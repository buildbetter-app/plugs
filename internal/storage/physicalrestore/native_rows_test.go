package physicalrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

var nativeProofIdentity = dumpformat.SourceIdentity{StoreID: "public-row-undo", Project: "disposable-native"}

// The original table-granular envelope, independent of reconstruction and of
// NativeTableBodyBytes. Source-native normalized rows are supplied by the test.
func nativeOldEnvelope(t *testing.T, source dumpformat.SourceIdentity, name, history string, table dumpformat.NativeTable) []byte {
	t.Helper()
	data, err := json.Marshal(struct {
		Source  dumpformat.SourceIdentity `json:"source_identity"`
		Table   string                    `json:"table"`
		History string                    `json:"history"`
		Columns []string                  `json:"columns"`
		Rows    []dumpformat.Row          `json:"rows"`
	}{source, name, history, table.Columns, table.Rows})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func nativeBinding(t *testing.T, source dumpformat.SourceIdentity, name, history, kind string, table dumpformat.NativeTable, records []driver.Record, physical string) NativeTableBinding {
	t.Helper()
	hash := sha256.Sum256(nativeOldEnvelope(t, source, name, history, table))
	refs := make([]driver.Ref, len(records))
	for i, r := range records {
		refs[i] = r.Ref
	}
	return NativeTableBinding{Source: source, Table: name, History: history, IdentityKind: kind, BodySHA256: hex.EncodeToString(hash[:]), PhysicalSHA256: physical, Refs: refs}
}

// Seed through the normal storage write boundary, then (for SQLite) close and
// reopen the file. This exercises actual Query snapshots, not a Get oracle.
func nativeRecordStore(t *testing.T, engine string, records []driver.Record) driver.Store {
	t.Helper()
	ctx := context.Background()
	var store driver.Store
	path := filepath.Join(t.TempDir(), "native-records.sqlite")
	openSQLite := func() driver.Store {
		s, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	if engine == "sqlite" {
		store = openSQLite()
	} else {
		store = memory.New()
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "public-native-undo", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	type entry struct {
		record driver.Record
		key    driver.LockKey
		order  string
	}
	entries := make([]entry, len(records))
	for i, r := range records {
		ref := r.Ref
		k := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		order, err := driver.EncodeLockKey(k)
		if err != nil {
			t.Fatal(err)
		}
		entries[i] = entry{r, k, order}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].order < entries[j].order })
	locks := make([]driver.LockRequest, len(entries))
	keys := make([]driver.LockKey, len(entries))
	changes := make([]model.ChangeRecord, len(entries))
	for i, e := range entries {
		locks[i] = driver.LockRequest{Key: e.key, Mode: driver.LockUpdate}
		keys[i] = e.key
		changes[i] = model.ChangeRecord{Ref: e.record.Ref, Op: model.OpCreate}
	}
	if _, err = tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err = tx.Put(ctx, e.record); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "public-native-undo"}, LockKeys: keys, Changes: changes}); err != nil {
		t.Fatal(err)
	}
	if err = lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if engine == "sqlite" {
		if err = store.Close(); err != nil {
			t.Fatal(err)
		}
		store = openSQLite()
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}
func nativeStoreReadback(t *testing.T, engine string, records []driver.Record, census []NativeTableBinding) NativeRowReadback {
	t.Helper()
	ctx := context.Background()
	store := nativeRecordStore(t, engine, records)
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	readback, readErr := ReadNativeRowTables(ctx, reader, census, nil)
	if err := errors.Join(readErr, reader.Close()); err != nil {
		t.Fatal(err)
	}
	return readback
}

func TestNativeRowReadbackBothStoresV1ExactCompletePagination(t *testing.T) {
	table := dumpformat.NativeTable{Columns: []string{"value"}, Rows: []dumpformat.Row{}}
	for i := 0; i < 310; i++ {
		table.Rows = append(table.Rows, dumpformat.Row{"value": fmt.Sprintf("row-%03d", i/2)})
	}
	records, err := dumpformat.NativeV1TableRecords(nativeProofIdentity, "public_keyless", dumpformat.FamilyVersioned, table)
	if err != nil {
		t.Fatal(err)
	}
	empty := dumpformat.NativeTable{Columns: []string{"value"}, Rows: []dumpformat.Row{}}
	empties, err := dumpformat.NativeV1TableRecords(nativeProofIdentity, "public_empty", dumpformat.FamilyUnversioned, empty)
	if err != nil {
		t.Fatal(err)
	}
	census := []NativeTableBinding{nativeBinding(t, nativeProofIdentity, "public_keyless", dumpformat.FamilyVersioned, dumpformat.NativeRowV1ContentBag, table, records, ""), nativeBinding(t, nativeProofIdentity, "public_empty", dumpformat.FamilyUnversioned, dumpformat.NativeRowV1ContentBag, empty, empties, "")}
	records = append(records, empties...)
	records = append(records, driver.Record{Ref: model.RefByID("ordinary-record", model.Natural64(0)), Kind: "unrelated", Body: []byte(`{}`)})
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			r := nativeStoreReadback(t, engine, records, census)
			bodies := r.TableBodies()
			for i, expected := range [][]byte{nativeOldEnvelope(t, nativeProofIdentity, "public_keyless", dumpformat.FamilyVersioned, table), nativeOldEnvelope(t, nativeProofIdentity, "public_empty", dumpformat.FamilyUnversioned, empty)} {
				if !bytes.Equal(bodies[i], expected) {
					t.Fatal("complete old table bytes differ")
				}
			}
			bodies[0][0] = '!'
			if r.TableBodies()[0][0] != '{' {
				t.Fatal("returned table body aliases retained readback")
			}
			t.Logf("NATIVE_ROW_V1_BYTE_PROOF engine=%s rows=310 duplicate_occurrences=155 empty_meta=1 terminal_pagination=true", engine)
		})
	}
}

type nativePageReader struct {
	driver.Reader
	view    driver.View
	records []driver.Record
	mutate  func(int, *driver.Page[driver.Record])
	calls   int
}

func (r *nativePageReader) Snapshot() driver.View {
	v := r.view
	if v.At != nil {
		at := *v.At
		v.At = &at
	}
	return v
}
func (r *nativePageReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	if err := ctx.Err(); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if q.Filter != nil || q.AsOf == nil || q.AsOf.String() != r.view.At.String() {
		return driver.Page[driver.Record]{}, errors.New("query must be unfiltered and snapshot-bound")
	}
	start := uint64(0)
	if q.Cursor != nil {
		start, _ = q.Cursor.Index.Uint64()
	}
	if start > uint64(len(r.records)) {
		return driver.Page[driver.Record]{}, errors.New("bad cursor")
	}
	end := start + 2
	if end > uint64(len(r.records)) {
		end = uint64(len(r.records))
	}
	p := driver.Page[driver.Record]{Items: append([]driver.Record{}, r.records[start:end]...), AsOf: *r.view.At}
	if end < uint64(len(r.records)) {
		d := sha256.Sum256([]byte("null"))
		p.NextCursor = &driver.Cursor{Incarnation: r.view.Incarnation, Revision: *r.view.At, Index: model.Natural64(end), Method: "query", FilterDigest: hex.EncodeToString(d[:])}
	}
	if r.mutate != nil {
		r.mutate(r.calls, &p)
	}
	r.calls++
	return p, nil
}
func nativeFake(records []driver.Record) *nativePageReader {
	at := model.Natural64(1)
	return &nativePageReader{view: driver.View{At: &at, Incarnation: "fixed-public-snapshot"}, records: records}
}

func TestNativeRowReadbackRefusesIncompleteForeignMalformedSnapshots(t *testing.T) {
	table := dumpformat.NativeTable{Columns: []string{"value"}, Rows: []dumpformat.Row{{"value": "a"}, {"value": "b"}, {"value": "c"}}}
	records, err := dumpformat.NativeV1TableRecords(nativeProofIdentity, "public_rows", dumpformat.FamilyVersioned, table)
	if err != nil {
		t.Fatal(err)
	}
	binding := nativeBinding(t, nativeProofIdentity, "public_rows", dumpformat.FamilyVersioned, dumpformat.NativeRowV1ContentBag, table, records, "")
	tests := []struct {
		name   string
		change func(*nativePageReader, *[]NativeTableBinding)
	}{
		{"missing_meta", func(r *nativePageReader, _ *[]NativeTableBinding) {
			for i, rec := range r.records {
				m, _, _ := dumpformat.DecodeNativeRowRecord(rec)
				if m != nil {
					r.records = append(r.records[:i:i], r.records[i+1:]...)
					break
				}
			}
		}},
		{"missing_row", func(r *nativePageReader, _ *[]NativeTableBinding) { r.records = r.records[:len(r.records)-1] }},
		{"duplicate_ref", func(r *nativePageReader, _ *[]NativeTableBinding) { r.records = append(r.records, r.records[0]) }},
		{"wrong_kind_extra_namespace", func(r *nativePageReader, _ *[]NativeTableBinding) {
			r.records = append(r.records, driver.Record{Ref: model.RefByID("source/native-current/unrecognized-extra", model.Natural64(0)), Kind: "hidden-by-kind-filter", Body: []byte(`{}`)})
		}},
		{"wrong_kind_known_ref", func(r *nativePageReader, _ *[]NativeTableBinding) { r.records[0].Kind = "wrong" }},
		{"malformed_body", func(r *nativePageReader, _ *[]NativeTableBinding) { r.records[0].Body = []byte(`{`) }},
		{"foreign_source", func(_ *nativePageReader, c *[]NativeTableBinding) { (*c)[0].Source.Project = "foreign" }},
		{"wrong_table_hash", func(_ *nativePageReader, c *[]NativeTableBinding) { (*c)[0].BodySHA256 = fmt.Sprintf("%064x", 1) }},
		{"wrong_identity_kind", func(_ *nativePageReader, c *[]NativeTableBinding) {
			(*c)[0].IdentityKind = dumpformat.NativeRowD3Physical
			(*c)[0].PhysicalSHA256 = fmt.Sprintf("%064x", 1)
		}},
		{"duplicate_expected_group", func(_ *nativePageReader, c *[]NativeTableBinding) { *c = append(*c, (*c)[0]) }},
		{"missing_ref_census", func(_ *nativePageReader, c *[]NativeTableBinding) { (*c)[0].Refs = nil }},
		{"duplicate_expected_ref", func(_ *nativePageReader, c *[]NativeTableBinding) {
			(*c)[0].Refs = append((*c)[0].Refs, (*c)[0].Refs[0])
		}},
		{"snapshot_drift", func(r *nativePageReader, _ *[]NativeTableBinding) {
			r.mutate = func(_ int, p *driver.Page[driver.Record]) { p.AsOf = model.Natural64(2) }
		}},
		{"reader_incarnation_drift", func(r *nativePageReader, _ *[]NativeTableBinding) {
			r.mutate = func(_ int, _ *driver.Page[driver.Record]) { r.view.Incarnation = "changed" }
		}},
		{"nonadvancing_cursor", func(r *nativePageReader, _ *[]NativeTableBinding) {
			r.mutate = func(_ int, p *driver.Page[driver.Record]) {
				if p.NextCursor != nil {
					p.NextCursor.Index = model.Natural64(0)
				}
			}
		}},
		{"cursor_skip", func(r *nativePageReader, _ *[]NativeTableBinding) {
			r.mutate = func(_ int, p *driver.Page[driver.Record]) {
				if p.NextCursor != nil {
					p.NextCursor.Index = model.Natural64(3)
				}
			}
		}},
		{"cursor_filter", func(r *nativePageReader, _ *[]NativeTableBinding) {
			r.mutate = func(_ int, p *driver.Page[driver.Record]) {
				if p.NextCursor != nil {
					p.NextCursor.FilterDigest = "kind-filtered"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := nativeFake(append([]driver.Record{}, records...))
			b := binding
			b.Refs = append([]driver.Ref{}, binding.Refs...)
			c := []NativeTableBinding{b}
			test.change(r, &c)
			if _, err := ReadNativeRowTables(context.Background(), r, c, nil); err == nil {
				t.Fatal("invalid complete-snapshot claim accepted")
			}
		})
	}
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := ReadNativeRowTables(ctx, nativeFake(records), []NativeTableBinding{binding}, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	})
}

func TestNativeUndoTaggedCellBoundary(t *testing.T) {
	tests := []struct {
		in   dumpformat.NativeDeltaCell
		want Cell
	}{
		{dumpformat.NativeDeltaCell{Kind: "sql-null"}, Cell{Type: "null"}},
		{dumpformat.NativeDeltaCell{Kind: "string", Value: "null"}, Cell{Type: "string", Data: "null"}},
		{dumpformat.NativeDeltaCell{Kind: "int64", Value: "9007199254740993"}, Cell{Type: "int64", Data: "9007199254740993"}},
		{dumpformat.NativeDeltaCell{Kind: "uint64", Value: "18446744073709551615"}, Cell{Type: "uint64", Data: "18446744073709551615"}},
		{dumpformat.NativeDeltaCell{Kind: "bytes", Value: "AP8="}, Cell{Type: "bytes", Data: "AP8="}},
		{dumpformat.NativeDeltaCell{Kind: "float32-bits", Value: "80000000"}, Cell{Type: "float32", Data: "-0"}},
		{dumpformat.NativeDeltaCell{Kind: "float64-bits", Value: "3ff8000000000000"}, Cell{Type: "float64", Data: "1.5"}},
	}
	for _, test := range tests {
		got, err := nativeUndoCell(test.in)
		if err != nil || got != test.want {
			t.Fatalf("%s: %+v %v", test.in.Kind, got, err)
		}
	}
	for _, cell := range []dumpformat.NativeDeltaCell{{Kind: "secret-reference", Value: "unavailable"}, {Kind: "string-bytes", Value: "/w=="}, {Kind: "int8", Value: "128"}, {Kind: "float64-bits", Value: "7ff0000000000000"}, {Kind: "float32-bits", Value: "7fc00000"}} {
		if _, err := nativeUndoCell(cell); err == nil {
			t.Fatalf("unsupported %s accepted", cell.Kind)
		}
	}
	// Keep the bit-level boundary explicit for signed zero.
	v, err := decodeCell(Cell{Type: "float64", Data: "-0"})
	if err != nil || !math.Signbit(v.(float64)) {
		t.Fatal("negative zero lost")
	}
}

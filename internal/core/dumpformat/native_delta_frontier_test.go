package dumpformat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestNativeDeltaPhysicalKeyOwnershipSurvivesValidationScratch(t *testing.T) {
	table := NativeDeltaTableSchema{Name: "items", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "bigint", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	root := NewNativeDeltaBudget(0)
	var owners []*NativeDeltaBudget
	var retained uint64
	for _, id := range []string{"1", "2"} {
		row := deltaTestRow(t, table, id, "exact", 0)
		owner := root.Scope()
		owners = append(owners, owner)
		fields, key, err := validateNativeDeltaRowAccounted(table, row, nil, owner)
		if err != nil || fields["value"] != "exact" || len(key) == 0 {
			t.Fatal("valid row changed", fields, key, err)
		}
		n := uint64(base64.StdEncoding.DecodedLen(len(row.Key)))
		retained += n
		if owner.ReservedBytes("physical_key_decode") != n {
			t.Fatal("returned key lost its owner", owner.Usage())
		}
		owner.ReleaseBytes("normalized_fields", owner.ReservedBytes("normalized_fields"))
		if root.Usage().CurrentAccountedDecodedBytes != retained {
			t.Fatal("previous/current key overlap is not charged", root.Usage(), retained)
		}
	}
	for _, owner := range owners {
		owner.Release()
	}
	if root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("key owners did not unwind", root.Usage())
	}
	// The string copy has a separate admission from the decoded byte buffer.
	key := []byte("physical-key")
	limited := NewNativeDeltaBudget(uint64(len(key) - 1))
	owner := limited.Scope()
	value, err := nativeDeltaMapKeyAccounted(key, owner)
	var refusal *NativeDeltaDecodedLimitError
	if value != "" || !errors.As(err, &refusal) || refusal.Phase != "decoded_row_map_keys" || refusal.MeasuredBytes != uint64(len(key)) || refusal.Overflow {
		t.Fatal("map key was not refused before conversion", value, err)
	}
	owner.Release()
	if limited.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("map key refusal retained a candidate")
	}
}

func TestNativeDeltaReconstructionDropsDecodedKeysAndMapReplacements(t *testing.T) {
	f := deltaReconstructionFixture(t)
	root := NewNativeDeltaBudget(0)
	var simultaneousKeys uint64
	root.Observe(func(event NativeDeltaAllocationEvent) {
		if n := root.ReservedBytes("physical_key_decode"); n > simultaneousKeys {
			simultaneousKeys = n
		}
	})
	_, _, err := reconstructNativeDelta(context.Background(), &f.stream, f.root, root)
	if err != nil {
		t.Fatal(err)
	}
	usage := root.Usage()
	if simultaneousKeys == 0 || usage.CumulativeCharges["decoded_row_map_keys"] == 0 || usage.CurrentCategories["physical_key_decode"] != 0 || usage.CurrentCategories["decoded_row_map_keys"] != 0 {
		t.Fatal("reconstruction key owners did not end with their tables", simultaneousKeys, usage)
	}
}

func TestNativeDeltaReconstructionReleasesRetiredRowsWithinTable(t *testing.T) {
	table := NativeDeltaTableSchema{Name: "items", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "bigint", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	encode := func(value any) []byte {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return append(raw, '\n')
	}
	key := func(row NativeDeltaRow) []byte {
		t.Helper()
		decoded, err := base64.StdEncoding.DecodeString(row.Key)
		if err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	// Ordered deletes retire most of a large baseline before a later update
	// and insert. Dead bodies must not compete with those live candidates.
	all := make([]NativeDeltaRow, 17)
	for i := range all {
		all[i] = deltaTestRow(t, table, strconv.Itoa(i+1), strings.Repeat("x", 8192), 0)
	}
	sort.Slice(all, func(i, j int) bool { return bytes.Compare(key(all[i]), key(all[j])) < 0 })
	var baseline, input bytes.Buffer
	type held struct{ baseline, operations, mapKeys uint64 }
	var live held
	for _, row := range all[:16] {
		raw := encode(row)
		baseline.Write(raw)
		live.baseline += uint64(len(raw))
		live.mapKeys += uint64(len(key(row)))
	}
	path := filepath.Join(t.TempDir(), "baseline.ndjson")
	if err := os.WriteFile(path, baseline.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	var expected []held
	for _, row := range all[:14] {
		d := NativeDeltaDelete{Type: "row", Op: "delete", Key: row.Key}
		raw := encode(d)
		input.Write(raw)
		expected = append(expected, held{live.baseline, live.operations + uint64(len(raw)), live.mapKeys})
		live.baseline -= uint64(len(encode(row)))
		live.mapKeys -= uint64(len(key(row)))
	}
	updated := deltaTestRow(t, table, all[14].Cells[0].Value, strings.Repeat("y", 8192), 0)
	updated.Op = "update"
	inserted := all[16]
	for i, row := range []NativeDeltaRow{updated, inserted} {
		raw := encode(row)
		input.Write(raw)
		expected = append(expected, held{live.baseline, live.operations + uint64(len(raw)), live.mapKeys})
		live.operations += uint64(len(raw))
		if i == 0 {
			live.baseline -= uint64(len(encode(all[14])))
		} else {
			live.mapKeys += uint64(len(key(row)))
		}
	}
	expected = append(expected, live) // At end_table admission, before table cleanup.
	final := []NativeDeltaRow{updated, all[15], inserted}
	hash, err := NativeDeltaTableSHA256(table, final)
	if err != nil {
		t.Fatal(err)
	}
	input.Write(encode(NativeDeltaEndTable{Type: "end_table", RowOperations: 16, Rows: 3, SHA256: hash}))
	open := NativeDeltaTable{Type: "table", Name: table.Name, TableSchemaID: NativeDeltaTableSchemaID(table), Mode: "delta", ExpectedRows: 3, ExpectedSHA256: hash}
	want := NativeTable{Columns: []string{"id", "value"}, Rows: []Row{}}
	for _, row := range final {
		raw, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
		if err != nil {
			t.Fatal(err)
		}
		var fields Row
		if err := decodeJSON(raw, &fields); err != nil {
			t.Fatal(err)
		}
		want.Rows = append(want.Rows, fields)
	}
	sort.Slice(want.Rows, func(i, j int) bool { return bytes.Compare(encode(want.Rows[i]), encode(want.Rows[j])) < 0 })
	run := func(limit uint64, cancelAt int) (*NativeDeltaBudget, []held, NativeTable, error) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root := NewNativeDeltaBudget(limit)
		owner := root.Scope()
		defer owner.Release()
		var samples []held
		root.Observe(func(event NativeDeltaAllocationEvent) {
			if event.Action == "reserve" && (event.Phase == "decoded_operations" || event.Phase == "json_typed_candidate" && len(samples) == 16) {
				samples = append(samples, held{root.ReservedBytes("decoded_baseline_rows"), root.ReservedBytes("decoded_operations"), root.ReservedBytes("decoded_row_map_keys")})
				if cancelAt != 0 && len(samples) == cancelAt {
					cancel()
				}
			}
		})
		reader := newNativeDeltaRecordReader(ctx, bytes.NewReader(input.Bytes()), owner)
		defer reader.Close()
		r := nativeDeltaReconstructor{ctx: ctx, input: reader, budget: owner, dir: t.TempDir()}
		_, actual, err := r.table(open, table, deltaStagedTable{schema: table, path: path}, false)
		return root, samples, actual, err
	}
	root, samples, actual, err := run(0, 0)
	if err != nil || !reflect.DeepEqual(actual, want) || !reflect.DeepEqual(samples, expected) {
		t.Fatal("in-table live targets or reconstructed bytes differ", err, samples, expected)
	}
	peak := root.Usage().PeakAccountedDecodedBytes
	bounded, samples, actual, err := run(peak, 0)
	if err != nil || !reflect.DeepEqual(actual, want) || !reflect.DeepEqual(samples, expected) {
		t.Fatal("live peak refused valid ordered deletes/update/insert", peak, err, samples)
	}
	refused, _, _, err := run(peak-1, 0)
	var refusal *NativeDeltaDecodedLimitError
	if !errors.As(err, &refusal) || refusal.MeasuredBytes != peak || refusal.Overflow {
		t.Fatal("simultaneous candidate boundary did not refuse exactly", peak, err)
	}
	cancelled, samples, _, err := run(0, 15)
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(samples, expected[:15]) {
		t.Fatal("mid-table cancellation did not preserve prior releases", err, samples)
	}
	for _, root := range []*NativeDeltaBudget{root, bounded, refused, cancelled} {
		if root.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("table exit retained a target after success/refusal/cancellation", root.Usage())
		}
	}
}

func TestNativeDeltaGraphFrontierReleasesOnlyLastUsedUnsharedFiles(t *testing.T) {
	directory := t.TempDir()
	path := func(name string) string {
		p := filepath.Join(directory, name)
		if err := os.WriteFile(p, []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	shared, old, previous := path("shared"), path("old"), path("previous")
	root := NewNativeDeltaBudget(100)
	owner := root.Scope()
	defer owner.Release()
	for i := 0; i < 2; i++ {
		if err := owner.ChargeBytes("staged_state_metadata", 10); err != nil {
			t.Fatal(err)
		}
	}
	r := nativeDeltaReconstructor{ctx: context.Background(), dir: directory, budget: owner,
		lastUse: map[string]uint64{"expired": 2, "future": 5}, stagedBytes: map[string]uint64{"expired": 10, "future": 10},
		states: map[string]deltaStagedState{
			"expired": {tables: map[string]deltaStagedTable{"shared": {path: shared}, "old": {path: old}}},
			"future":  {tables: map[string]deltaStagedTable{"shared": {path: shared}}},
		}, previousTables: map[string]deltaStagedTable{"previous": {path: previous}}}
	if err := r.releaseGraphFrontier(2); err != nil {
		t.Fatal(err)
	}
	if len(r.states) != 1 || owner.ReservedBytes("staged_state_metadata") != 10 {
		t.Fatal("state metadata survived last use")
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("superseded file retained", err)
	}
	for _, p := range []string{shared, previous} {
		if _, err := os.Stat(p); err != nil {
			t.Fatal("live shared/previous file dropped", err)
		}
	}
	r.previousTables = nil
	if err := r.releaseGraphFrontier(5); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) != 0 || root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("terminal frontier retained files/reservations", files, err, root.Usage())
	}
}

func TestNativeRowBudgetedDecodeMatchesPureCodecAndRollsBackOnRefusal(t *testing.T) {
	table := NativeTable{Columns: []string{"id", "value"}, Rows: []Row{{"id": "x", "value": "exact<>&\n"}}}
	records, err := NativeV1TableRecords(rowTestSource, "items", nativeRetainedHistory, table)
	if err != nil {
		t.Fatal(err)
	}
	root := NewNativeDeltaBudget(0)
	for _, record := range records {
		owner := root.Scope()
		archive := &Archive{nativeBudget: owner}
		meta, row, err := archive.DecodeNativeRowRecord(record)
		expectedMeta, expectedRow, pureErr := DecodeNativeRowRecord(record)
		if err != nil || pureErr != nil || !equalJSON(meta, expectedMeta) || !equalJSON(row, expectedRow) {
			t.Fatal("budget changed codec", err, pureErr)
		}
		owner.Release()
	}
	if root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("decode ownership leaked", root.Usage())
	}
	for _, limit := range []uint64{1, 64} {
		root := NewNativeDeltaBudget(limit)
		owner := root.Scope()
		_, _, err := (&Archive{nativeBudget: owner}).DecodeNativeRowRecord(records[0])
		if err == nil {
			t.Fatal("unadmitted decode accepted")
		}
		owner.Release()
		if root.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("decode failure leaked")
		}
	}
}

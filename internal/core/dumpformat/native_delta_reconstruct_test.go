package dumpformat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func deltaTestUint(n uint64) *uint64 { return &n }
func deltaTestText(s string) *string { return &s }

func TestNativeDeltaOpaqueCurrentKeepsPortableAndLegacyCensusExact(t *testing.T) {
	// This is a declared synthetic v1 input adapted only to exercise the v2
	// post-verification policy. It supplies no physical producer proof.
	a, err := Read(bytes.NewReader(nativeSyntheticFixture(t, "pl")))
	if err != nil {
		t.Fatal(err)
	}
	h := a.NativeSourceHistories()[0]
	h.deltaVerified = true
	h.Current["dolt_ignore"] = h.Ignore
	h.Current["dolt_nonlocal_tables"] = NativeTable{Columns: []string{"name"}, Rows: []Row{{"name": "synthetic_nonlocal"}}}
	h.Current["dolt_schemas"] = NativeTable{Columns: []string{"fragment", "name", "type"}, Rows: []Row{{"fragment": "SELECT 1 AS public_value", "name": "synthetic_public_view", "type": "view"}}}
	for i := range h.Images {
		if h.Images[i].CommitID == h.Head {
			h.Images[i].Tables["dolt_schemas"] = h.Current["dolt_schemas"]
		}
	}
	if err := validateNativeHistory(h); err != nil {
		t.Fatal("verified opaque current tables refused", err)
	}
	if err := bindNativeCurrent(h, a.byKind, false); err != nil {
		t.Fatal("opaque physical rows were treated as portable records", err)
	}
	copy := cloneNativeDeltaHistory(h)
	if !reflect.DeepEqual(copy.Current, h.Current) || !copy.OpaqueNativeCurrentFamily("dolt_schemas") {
		t.Fatal("opaque rows lost in detached source history")
	}
	legacy := cloneNativeDeltaHistory(h)
	legacy.deltaVerified = false
	if legacy.OpaqueNativeCurrentFamily("dolt_schemas") || validateNativeHistory(legacy) == nil {
		t.Fatal("opaque exception widened legacy portable census")
	}
	missing := cloneNativeDeltaHistory(h)
	delete(missing.Current, "config")
	if validateNativeHistory(missing) == nil {
		t.Fatal("opaque tables masked a missing required portable family")
	}
	unknown := cloneNativeDeltaHistory(h)
	unknown.Current["unknown_physical_table"] = NativeTable{Columns: []string{"value"}, Rows: []Row{}}
	if validateNativeHistory(unknown) == nil {
		t.Fatal("unknown physical current family accepted")
	}
	changed := cloneNativeDeltaHistory(h)
	changed.Current["dolt_ignore"] = NativeTable{Columns: []string{"ignored", "pattern"}, Rows: []Row{{"ignored": true, "pattern": "invented"}}}
	if validateNativeHistory(changed) == nil {
		t.Fatal("physical ignore rows diverged from authenticated header")
	}
	changed = cloneNativeDeltaHistory(h)
	delete(changed.Current, "dolt_schemas")
	if validateNativeHistory(changed) == nil {
		t.Fatal("versioned opaque family disappeared from current census")
	}
}

func deltaTestRow(t *testing.T, table NativeDeltaTableSchema, id, value string, occurrence uint64) NativeDeltaRow {
	t.Helper()
	cells := []NativeDeltaCell{{Kind: "string", Value: value}}
	fields := Row{"value": value}
	if len(table.Columns) == 2 {
		cells = []NativeDeltaCell{{Kind: "int64", Value: id}, {Kind: "string", Value: value}}
		fields["id"] = json.Number(id)
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	key, err := NativeDeltaPrimaryKey(table, cells)
	if err != nil {
		t.Fatal(err)
	}
	if key == nil {
		key = NativeDeltaBagKey(NativeDeltaRowPayload(cells, raw), occurrence)
	}
	return NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(raw)}
}

type deltaTestTransaction struct {
	table NativeDeltaTableSchema
	mode  string
	rows  []NativeDeltaRow
	ops   []any
}
type deltaTestFixture struct {
	t        *testing.T
	stream   bytes.Buffer
	root     NativeCaptureSet
	totals   NativeDeltaTotals
	lastRoot string
	expected []NativeHistoryImage
}

func (f *deltaTestFixture) write(value any) {
	f.t.Helper()
	if err := json.NewEncoder(&f.stream).Encode(value); err != nil {
		f.t.Fatal(err)
	}
}
func (f *deltaTestFixture) schema(s NativeDeltaSchema) { f.write(s); f.totals.Schemas++ }
func (f *deltaTestFixture) transaction(kind string, sequence uint64, id string, schema NativeDeltaSchema, parents []string, mode string, transactions []deltaTestTransaction) {
	f.t.Helper()
	digests := map[string]NativeDeltaTableDigest{}
	image := NativeHistoryImage{Sequence: sequence, CommitID: id, SchemaProfile: &schema.Profile, Tables: map[string]NativeTable{}}
	for _, tx := range transactions {
		hash, err := NativeDeltaTableSHA256(tx.table, tx.rows)
		if err != nil {
			f.t.Fatal(err)
		}
		digests[tx.table.Name] = NativeDeltaTableDigest{Rows: uint64(len(tx.rows)), SHA256: hash}
		columns := []string{}
		for _, c := range tx.table.Columns {
			columns = append(columns, c.Name)
		}
		sort.Strings(columns)
		table := NativeTable{Columns: columns, Rows: []Row{}}
		for _, row := range tx.rows {
			raw, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
			if err != nil {
				f.t.Fatal(err)
			}
			var fields Row
			if err := decodeJSON(raw, &fields); err != nil {
				f.t.Fatal(err)
			}
			table.Rows = append(table.Rows, fields)
		}
		sort.Slice(table.Rows, func(i, j int) bool {
			a, _ := json.Marshal(table.Rows[i])
			b, _ := json.Marshal(table.Rows[j])
			return bytes.Compare(a, b) < 0
		})
		image.Tables[tx.table.Name] = table
	}
	root, err := NativeDeltaRootSHA256(digests)
	if err != nil {
		f.t.Fatal(err)
	}
	if kind == "commit" {
		var first, base *string
		if len(parents) > 0 {
			first = deltaTestText(parents[0])
			if mode == "delta" {
				base = first
			}
		}
		f.write(NativeDeltaCommit{Type: kind, Sequence: sequence, CommitID: id, Parents: parents, FirstParent: first, Metadata: Row{"public": "fixture"}, SchemaID: schema.SchemaID, Mode: mode, BaseCommit: base, ExpectedRootSHA256: root})
	} else {
		f.write(NativeDeltaCheckpoint{Type: kind, CommitID: id, SchemaID: schema.SchemaID, ExpectedRootSHA256: root})
	}
	for _, tx := range transactions {
		d := digests[tx.table.Name]
		f.write(NativeDeltaTable{Type: "table", Name: tx.table.Name, TableSchemaID: NativeDeltaTableSchemaID(tx.table), Mode: tx.mode, ExpectedRows: d.Rows, ExpectedSHA256: d.SHA256})
		for _, op := range tx.ops {
			f.write(op)
		}
		f.write(NativeDeltaEndTable{Type: "end_table", RowOperations: uint64(len(tx.ops)), Rows: d.Rows, SHA256: d.SHA256})
		f.totals.TableTransactions++
		f.totals.RowOperations += uint64(len(tx.ops))
		f.totals.ReconstructedRows += d.Rows
		f.totals.ReconstructedCells += d.Rows * uint64(len(tx.table.Columns))
	}
	if kind == "commit" {
		f.write(NativeDeltaEndCommit{Type: "end_commit", Sequence: sequence, CommitID: id, TableCount: uint64(len(transactions)), RootSHA256: root})
		f.totals.Commits++
		raw, err := json.Marshal(image)
		if err != nil {
			f.t.Fatal(err)
		}
		f.totals.FullImageBytes += uint64(len(raw) + 1)
		f.expected = append(f.expected, image)
	} else {
		f.write(NativeDeltaEndCheckpoint{Type: "end_" + kind, TableCount: uint64(len(transactions)), RootSHA256: root})
	}
	f.lastRoot = root
}

// Four commits include two divergent branches and a merge whose actual first
// parent is the later branch. The expected third state deliberately differs
// from the previously walked second state. Keyless duplicates remain exact.
func deltaReconstructionFixture(t *testing.T) *deltaTestFixture {
	t.Helper()
	ids := []string{strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32), strings.Repeat("d", 32)}
	f := &deltaTestFixture{t: t, root: NativeCaptureSet{Format: NativeCaptureSetFormat, Contract: NativeDeltaFormat, StoreID: "public-store", Projects: []string{"public"}, Head: ids[3]}}
	items := NativeDeltaTableSchema{Name: "items", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "bigint", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	bag := NativeDeltaTableSchema{Name: "bag", Columns: []NativeDeltaColumn{{Name: "value", SQLType: "text"}}}
	schema := NativeDeltaSchema{Type: "schema", Profile: NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: NativeCommitSchemaScope, MainVersion: deltaTestUint(66), Absences: []NativeCommitSchemaAbsence{}}, Tables: []NativeDeltaTableSchema{bag, items}}
	schema.SchemaID = NativeDeltaSchemaID(schema)
	f.write(NativeDeltaHeader{Type: "header", Format: NativeDeltaFormat, StoreID: f.root.StoreID, Projects: f.root.Projects, Head: f.root.Head, CommitCount: 4, Ignore: NativeTable{Columns: []string{"ignored", "pattern"}, Rows: []Row{}}, ConfigPolicy: map[string]string{}})
	f.schema(schema)
	duplicate0 := deltaTestRow(t, bag, "", "same", 0)
	duplicate1 := deltaTestRow(t, bag, "", "same", 1)
	rootRow := deltaTestRow(t, items, "1", "root", 0)
	f.transaction("commit", 1, ids[0], schema, []string{}, "snapshot", []deltaTestTransaction{{bag, "replace", []NativeDeltaRow{duplicate0, duplicate1}, []any{duplicate0, duplicate1}}, {items, "replace", []NativeDeltaRow{rootRow}, []any{rootRow}}})
	left := deltaTestRow(t, items, "1", "left", 0)
	left.Op = "update"
	added := deltaTestRow(t, items, "2", "branch-only", 0)
	f.transaction("commit", 2, ids[1], schema, []string{ids[0]}, "delta", []deltaTestTransaction{{bag, "delta", []NativeDeltaRow{duplicate0, duplicate1}, []any{}}, {items, "delta", []NativeDeltaRow{left, added}, []any{left, added}}})
	right := deltaTestRow(t, items, "1", "right", 0)
	right.Op = "update"
	f.transaction("commit", 3, ids[2], schema, []string{ids[0]}, "delta", []deltaTestTransaction{{bag, "delta", []NativeDeltaRow{duplicate0}, []any{NativeDeltaDelete{Type: "row", Op: "delete", Key: duplicate1.Key}}}, {items, "delta", []NativeDeltaRow{right}, []any{right}}})
	merged := deltaTestRow(t, items, "2", "merged", 0)
	f.transaction("commit", 4, ids[3], schema, []string{ids[2], ids[1]}, "delta", []deltaTestTransaction{{bag, "delta", []NativeDeltaRow{duplicate0}, []any{}}, {items, "delta", []NativeDeltaRow{right, merged}, []any{merged}}})
	right.Op = "insert"
	for _, kind := range []string{"head-checkpoint", "current"} {
		f.transaction(kind, 0, ids[3], schema, nil, "", []deltaTestTransaction{{bag, "replace", []NativeDeltaRow{duplicate0}, []any{duplicate0}}, {items, "replace", []NativeDeltaRow{right, merged}, []any{right, merged}}})
	}
	f.write(NativeDeltaEnd{Type: "end", Totals: f.totals, HeadRootSHA256: f.lastRoot, CurrentRootSHA256: f.lastRoot})
	f.root.Totals = f.totals
	return f
}

func TestNativeDeltaReconstructsActualFirstParentAndDuplicateRows(t *testing.T) {
	f := deltaReconstructionFixture(t)
	h, proof, err := reconstructNativeDelta(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, NewNativeDeltaBudget(0))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(h.Images, f.expected) || proof.Totals != f.totals || len(proof.Commits) != 4 {
		t.Fatal("per-commit reconstruction/census differs")
	}
	if !reflect.DeepEqual(h.Commits[3].Parents, []string{strings.Repeat("c", 32), strings.Repeat("b", 32)}) {
		t.Fatal("actual full parent order changed")
	}
	var sum uint64
	for _, charge := range proof.Usage.Categories {
		sum += charge
	}
	if sum != proof.Usage.PeakAccountedDecodedBytes {
		t.Fatal("accounting cannot be reconciled")
	}
}

func TestNativeDeltaIntegrityAndDecodedLimitReturnNoHistory(t *testing.T) {
	f := deltaReconstructionFixture(t)
	bad := bytes.Replace(f.stream.Bytes(), []byte(`"value":"left"`), []byte(`"value":"mist"`), 1)
	if h, _, err := reconstructNativeDelta(context.Background(), bytes.NewReader(bad), f.root, NewNativeDeltaBudget(0)); err == nil || len(h.Images) != 0 {
		t.Fatal("changed typed cell published a history")
	}
	budget := NewNativeDeltaBudget(1)
	h, _, err := reconstructNativeDelta(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, budget)
	var limit *NativeDeltaDecodedLimitError
	if !errors.As(err, &limit) || len(h.Images) != 0 || limit.AdmittedBytes != 0 || limit.MeasuredBytes != limit.RejectedChargeBytes || budget.Usage().PeakAccountedDecodedBytes != 0 {
		t.Fatalf("refusal did not conserve state: %#v", err)
	}
}

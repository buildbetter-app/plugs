package dumpformat

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var rowTestSource = SourceIdentity{StoreID: "original/store", Project: "demo"}

func rowTestFields(t *testing.T, text string) Row {
	t.Helper()
	var row Row
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	if err := dec.Decode(&row); err != nil {
		t.Fatal(err)
	}
	return row
}

func rowTestTable(t *testing.T, rows ...Row) NativeTable {
	t.Helper()
	columns := []string{}
	for name := range rows[0] {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	result := NativeTable{Columns: columns, Rows: append([]Row{}, rows...)}
	sort.SliceStable(result.Rows, func(i, j int) bool {
		a, _ := json.Marshal(result.Rows[i])
		b, _ := json.Marshal(result.Rows[j])
		return bytes.Compare(a, b) < 0
	})
	return result
}

// Independent old-byte oracle: the ordered nativeTableBody envelope at exact
// 960619a8, not a call to the new codec or a row-multiset comparison.
func rowTestOldBytes(t *testing.T, source SourceIdentity, table, history string, value NativeTable) []byte {
	t.Helper()
	body, err := json.Marshal(struct {
		Source  SourceIdentity `json:"source_identity"`
		Table   string         `json:"table"`
		History string         `json:"history"`
		Columns []string       `json:"columns"`
		Rows    []Row          `json:"rows"`
	}{source, table, history, value.Columns, value.Rows})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func rowTestOracle(t *testing.T, source SourceIdentity, table, history string, records []driver.Record, expected NativeTable) NativeRowMeta {
	t.Helper()
	// Generic Ref order is not either table order. Also exercise arbitrary
	// input order rather than allowing reconstruction to depend on the encoder.
	reversed := append([]driver.Record{}, records...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	value, meta, err := ReconstructNativeRowTable(source, table, history, reversed, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := rowTestOldBytes(t, source, table, history, expected)
	got := rowTestOldBytes(t, source, table, history, value)
	if !bytes.Equal(got, want) {
		t.Fatalf("old table bytes differ\ngot %s\nwant %s", got, want)
	}
	serialized, err := NativeTableBodyBytes(source, table, history, value)
	if err != nil || !bytes.Equal(serialized, want) {
		t.Fatalf("production old-byte serializer differs: %v", err)
	}
	hash := sha256.Sum256(want)
	if meta.TableBodySHA256 != hex.EncodeToString(hash[:]) || meta.RowCount != uint64(len(expected.Rows)) {
		t.Fatal("wrong normalized commitment/census")
	}
	previous := ""
	for _, record := range records {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
		if err != nil || key <= previous {
			t.Fatalf("not in driver lock order: %v", err)
		}
		previous = key
	}
	return meta
}

func rowTestV1(t *testing.T, table NativeTable) []driver.Record {
	t.Helper()
	records, err := NativeV1TableRecords(rowTestSource, "issues", nativeRetainedHistory, table)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func rowTestPK(i uint64) *uint64 { return &i }

func rowTestPhysical(t *testing.T, schema NativeDeltaTableSchema, fields Row, cells []NativeDeltaCell, occurrence uint64) NativeDeltaRow {
	t.Helper()
	normalized, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	normalized = append(normalized, '\n')
	key, err := NativeDeltaPrimaryKey(schema, cells)
	if err != nil {
		t.Fatal(err)
	}
	if key == nil {
		key = NativeDeltaBagKey(NativeDeltaRowPayload(cells, normalized), occurrence)
	}
	return NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(normalized)}
}

func rowTestV2(t *testing.T, schema NativeDeltaTableSchema, rows ...NativeDeltaRow) []driver.Record {
	t.Helper()
	if rows == nil {
		rows = []NativeDeltaRow{}
	}
	records, err := NativeV2TableRecords(rowTestSource, schema, nativeRetainedHistory, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func rowTestRows(t *testing.T, records []driver.Record) map[model.Ref]driver.Record {
	t.Helper()
	rows := map[model.Ref]driver.Record{}
	for _, record := range records {
		_, row, err := DecodeNativeRowRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		if row != nil {
			rows[record.Ref] = record
		}
	}
	return rows
}

func TestNativeRowV1ExactTableOracleAndEmpty(t *testing.T) {
	a := rowTestFields(t, `{"id":9007199254740993,"note":"<>&\u2028","n":1e+03,"nullable":null,"payload":{"$bytes":"AP8="}}`)
	b := rowTestFields(t, `{"id":9007199254740994,"note":"日本語","n":1000.00,"nullable":"null","payload":{"$bytes":""}}`)
	value := rowTestTable(t, b, a, a)
	meta := rowTestOracle(t, rowTestSource, "issues", nativeRetainedHistory, rowTestV1(t, value), value)
	if meta.IdentityKind != NativeRowV1ContentBag || meta.PhysicalColumns != nil || meta.PhysicalTableSHA256 != nil {
		t.Fatal("v1 fabricated physical authority")
	}
	if len(rowTestRows(t, rowTestV1(t, value))) != 3 {
		t.Fatal("duplicate normalized rows collapsed")
	}
	empty := NativeTable{Columns: value.Columns, Rows: []Row{}}
	records := rowTestV1(t, empty)
	rowTestOracle(t, rowTestSource, "issues", nativeRetainedHistory, records, empty)
	if len(records) != 1 {
		t.Fatal("empty present table needs exactly one meta")
	}
	if _, _, err := ReconstructNativeRowTable(rowTestSource, "issues", nativeRetainedHistory, nil, nil); err == nil {
		t.Fatal("absent table accepted as empty present table")
	}
}

func TestNativeRowV2ExactTypedTableOracle(t *testing.T) {
	schema := NativeDeltaTableSchema{Name: "issues", Columns: []NativeDeltaColumn{
		{Name: "z", SQLType: "BIGINT", PrimaryKeyOrdinal: rowTestPK(0)},
		{Name: "a", SQLType: "BIGINT", PrimaryKeyOrdinal: rowTestPK(1)},
		{Name: "binary_data", SQLType: "BLOB"}, {Name: "json_data", SQLType: "JSON"},
		{Name: "nullable", SQLType: "TEXT"}, {Name: "observed", SQLType: "DATETIME"},
	}}
	fields := rowTestFields(t, `{"z":2,"a":9007199254740993,"binary_data":{"$bytes":"AP8="},"json_data":{"n":1e3},"nullable":null,"observed":"2026-11-11T01:30:00Z"}`)
	cells := []NativeDeltaCell{{Kind: "int16", Value: "2"}, {Kind: "int64", Value: "9007199254740993"}, {Kind: "bytes", Value: "AP8="}, {Kind: "string", Value: `{"n":1000}`}, {Kind: "sql-null", Value: ""}, {Kind: "time", Value: "2026-11-11T01:30:00Z"}}
	first := rowTestPhysical(t, schema, fields, cells, 0)
	otherFields := rowTestFields(t, `{"z":1,"a":9007199254740994,"binary_data":{"$bytes":""},"json_data":null,"nullable":"null","observed":"2026-11-11T01:30:01Z"}`)
	otherCells := []NativeDeltaCell{{Kind: "int16", Value: "1"}, {Kind: "int64", Value: "9007199254740994"}, {Kind: "bytes", Value: ""}, {Kind: "string", Value: "null"}, {Kind: "string", Value: "null"}, {Kind: "time", Value: "2026-11-11T01:30:01Z"}}
	second := rowTestPhysical(t, schema, otherFields, otherCells, 0)
	meta := rowTestOracle(t, rowTestSource, "issues", nativeRetainedHistory, rowTestV2(t, schema, first, second), rowTestTable(t, fields, otherFields))
	if meta.IdentityKind != NativeRowD3Physical || !reflect.DeepEqual(meta.PhysicalColumns, schema.Columns) {
		t.Fatal("physical column order/type/PK lost")
	}
	physicalRows := []NativeDeltaRow{first, second}
	sort.Slice(physicalRows, func(i, j int) bool {
		a, _ := base64.StdEncoding.DecodeString(physicalRows[i].Key)
		b, _ := base64.StdEncoding.DecodeString(physicalRows[j].Key)
		return bytes.Compare(a, b) < 0
	})
	want, err := NativeDeltaTableSHA256(schema, physicalRows)
	if err != nil || meta.PhysicalTableSHA256 == nil || *meta.PhysicalTableSHA256 != want {
		t.Fatal("D3 commitment changed")
	}
	rowTestOracle(t, rowTestSource, "issues", nativeRetainedHistory, rowTestV2(t, schema), NativeTable{Columns: rowTestTable(t, fields).Columns, Rows: []Row{}})
}

func TestNativeRowStableIdentityAndSchemaChanges(t *testing.T) {
	schema := NativeDeltaTableSchema{Name: "issues", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "BIGINT", PrimaryKeyOrdinal: rowTestPK(0)}, {Name: "note", SQLType: "TEXT"}}}
	makeRow := func(id, note string) NativeDeltaRow {
		fields := Row{"id": json.Number(id), "note": note}
		return rowTestPhysical(t, schema, fields, []NativeDeltaCell{{Kind: "int64", Value: id}, {Kind: "string", Value: note}}, 0)
	}
	original := rowTestRows(t, rowTestV2(t, schema, makeRow("1", "before")))
	updated := rowTestRows(t, rowTestV2(t, schema, makeRow("1", "after")))
	rekeyed := rowTestRows(t, rowTestV2(t, schema, makeRow("2", "after")))
	for ref, before := range original {
		after, ok := updated[ref]
		if !ok || bytes.Equal(before.Body, after.Body) {
			t.Fatal("PK-preserving update did not preserve Ref/change body")
		}
		if _, ok := rekeyed[ref]; ok {
			t.Fatal("PK edit kept old Ref")
		}
	}
	// A SQL-type-only schema edit changes meta, not an unchanged row body.
	schemaOnly := schema
	schemaOnly.Columns = append([]NativeDeltaColumn{}, schema.Columns...)
	schemaOnly.Columns[1].SQLType = "LONGTEXT"
	changedSchema := rowTestV2(t, schemaOnly, makeRow("1", "before"))
	if !reflect.DeepEqual(original, rowTestRows(t, changedSchema)) {
		t.Fatal("schema-only edit rewrote unchanged rows")
	}
	oldTable, _, _ := ReconstructNativeRowTable(rowTestSource, "issues", nativeRetainedHistory, rowTestV2(t, schema, makeRow("1", "before")), nil)
	newTable, newMeta, err := ReconstructNativeRowTable(rowTestSource, "issues", nativeRetainedHistory, changedSchema, nil)
	if err != nil || !reflect.DeepEqual(oldTable, newTable) || newMeta.SchemaID == NativeDeltaTableSchemaID(schema) {
		t.Fatal("schema-only reconstruction differs")
	}
	// V1 never guesses that a field named id is a primary key.
	v1Before := rowTestRows(t, rowTestV1(t, rowTestTable(t, Row{"id": json.Number("1"), "note": "before"})))
	v1After := rowTestRows(t, rowTestV1(t, rowTestTable(t, Row{"id": json.Number("1"), "note": "after"})))
	for ref := range v1Before {
		if _, exists := v1After[ref]; exists {
			t.Fatal("v1 content edit synthesized a PK update")
		}
	}
}

func TestNativeRowD3KeylessMultiplicityAndTypedPayload(t *testing.T) {
	schema := NativeDeltaTableSchema{Name: "events", Columns: []NativeDeltaColumn{{Name: "n", SQLType: "BIGINT"}}}
	fields := Row{"n": json.Number("7")}
	a := rowTestPhysical(t, schema, fields, []NativeDeltaCell{{Kind: "int8", Value: "7"}}, 0)
	b := rowTestPhysical(t, schema, fields, []NativeDeltaCell{{Kind: "int8", Value: "7"}}, 1)
	c := rowTestPhysical(t, schema, fields, []NativeDeltaCell{{Kind: "int64", Value: "7"}}, 0)
	base := rowTestV2(t, schema, a)
	grown := rowTestV2(t, schema, c, b, a)
	rowTestOracle(t, rowTestSource, "events", nativeRetainedHistory, grown, rowTestTable(t, fields, fields, fields))
	baseRows, grownRows := rowTestRows(t, base), rowTestRows(t, grown)
	if len(grownRows) != 3 {
		t.Fatal("equal normalized fields collapsed distinct typed payloads/occurrences")
	}
	for ref, record := range baseRows {
		if !bytes.Equal(record.Body, grownRows[ref].Body) {
			t.Fatal("keyless growth rewrote retained occurrence")
		}
	}
	if !reflect.DeepEqual(baseRows, rowTestRows(t, rowTestV2(t, schema, a))) {
		t.Fatal("keyless shrink changes retained occurrence")
	}
	for name, rows := range map[string][]NativeDeltaRow{"gap": {b}, "duplicate": {a, a}} {
		t.Run(name, func(t *testing.T) {
			if _, err := NativeV2TableRecords(rowTestSource, schema, nativeRetainedHistory, rows, nil); err == nil {
				t.Fatal("invalid keyless multiplicity accepted")
			}
		})
	}
	jsonSchema := NativeDeltaTableSchema{Name: "events", Columns: []NativeDeltaColumn{{Name: "j", SQLType: "JSON"}}}
	// SQL NULL and the JSON text null have equal normalized fields but distinct
	// tagged payloads and separate occurrence-zero identities.
	null := Row{"j": nil}
	sqlNull := rowTestPhysical(t, jsonSchema, null, []NativeDeltaCell{{Kind: "sql-null", Value: ""}}, 0)
	jsonNull := rowTestPhysical(t, jsonSchema, null, []NativeDeltaCell{{Kind: "string", Value: "null"}}, 0)
	rowTestOracle(t, rowTestSource, "events", nativeRetainedHistory, rowTestV2(t, jsonSchema, sqlNull, jsonNull), rowTestTable(t, null, null))
}

func rowTestChangeMeta(t *testing.T, records []driver.Record, change func(*NativeRowMeta)) []driver.Record {
	t.Helper()
	result := append([]driver.Record{}, records...)
	for i, record := range result {
		meta, _, err := DecodeNativeRowRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		if meta != nil {
			change(meta)
			replacement, err := NativeMetaRecord(*meta)
			if err != nil {
				t.Fatal(err)
			}
			result[i] = replacement
			return result
		}
	}
	t.Fatal("no metadata")
	return nil
}

func TestNativeRowCompleteGroupRejectsMalformedExtras(t *testing.T) {
	value := rowTestTable(t, Row{"id": json.Number("1")}, Row{"id": json.Number("2")})
	good := rowTestV1(t, value)
	rowIndex, metaIndex := -1, -1
	for i, r := range good {
		m, _, _ := DecodeNativeRowRecord(r)
		if m == nil {
			rowIndex = i
		} else {
			metaIndex = i
		}
	}
	alterRecord := func(index int, mutate func(*driver.Record)) []driver.Record {
		result := append([]driver.Record{}, good...)
		result[index].Body = append([]byte{}, result[index].Body...)
		mutate(&result[index])
		return result
	}
	foreign, err := NativeV1RowRecord(SourceIdentity{StoreID: "other", Project: "demo"}, "issues", nativeRetainedHistory, value.Rows[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	current, err := NativeV1RowRecord(rowTestSource, "issues", FamilyVersioned, value.Rows[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	wrongKind := good[rowIndex]
	wrongKind.Kind = "bead"
	wrongKind.Ref = model.RefByID(driver.NativeTableHistoryRecordPrefix+"row/extra-wrong-kind", model.Natural64(0))
	schema := NativeDeltaTableSchema{Name: "issues", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "BIGINT", PrimaryKeyOrdinal: rowTestPK(0)}}}
	physical := rowTestV2(t, schema, rowTestPhysical(t, schema, value.Rows[0], []NativeDeltaCell{{Kind: "int64", Value: "1"}}, 0))
	var mixed driver.Record
	for _, r := range physical {
		_, row, _ := DecodeNativeRowRecord(r)
		if row != nil {
			mixed = r
		}
	}
	cases := map[string][]driver.Record{
		"no meta":                     append(append([]driver.Record{}, good[:metaIndex]...), good[metaIndex+1:]...),
		"missing row":                 append(append([]driver.Record{}, good[:rowIndex]...), good[rowIndex+1:]...),
		"duplicate row":               append(append([]driver.Record{}, good...), good[rowIndex]),
		"extra foreign":               append(append([]driver.Record{}, good...), foreign),
		"extra current":               append(append([]driver.Record{}, good...), current),
		"all correct plus wrong Kind": append(append([]driver.Record{}, good...), wrongKind),
		"mixed identity":              append(append([]driver.Record{}, good...), mixed),
		"wrong count":                 rowTestChangeMeta(t, good, func(m *NativeRowMeta) { m.RowCount++ }),
		"wrong body hash":             rowTestChangeMeta(t, good, func(m *NativeRowMeta) { m.TableBodySHA256 = strings.Repeat("0", 64) }),
		"wrong schema": rowTestChangeMeta(t, good, func(m *NativeRowMeta) {
			m.Columns = []string{"note"}
			m.SchemaID = nativeV1ColumnsID(m.Table, m.Columns)
		}),
		"wrong ref":    alterRecord(rowIndex, func(r *driver.Record) { r.Ref.ID += "0" }),
		"wrong branch": alterRecord(rowIndex, func(r *driver.Record) { r.Ref.Branch = model.Natural64(1) }),
		"wrong Kind":   alterRecord(rowIndex, func(r *driver.Record) { r.Kind = "bead" }),
	}
	for name, records := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := ReconstructNativeRowTable(rowTestSource, "issues", nativeRetainedHistory, records, nil)
			var typed *NativeRowRecordError
			if err == nil || !errors.As(err, &typed) || !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("wanted typed refusal, got %v", err)
			}
		})
	}
}

func TestNativeRowCanonicalIdentityVectorAndPlanes(t *testing.T) {
	fields := Row{"id": json.Number("9007199254740993")}
	record, err := NativeV1RowRecord(rowTestSource, "issues", nativeRetainedHistory, fields, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Independently derived using struct.pack(">Q") and hashlib.sha256. This
	// pins U's width/endianness, domain lengths, source tuple and raw key hash.
	if record.Ref.ID != "source/native-history/row/0950b88fb55b92379f41d40b29f5ee68b88bd028eec4442ce096a515d11ea66c" {
		t.Fatal("row identity vector differs")
	}
	_, row, err := DecodeNativeRowRecord(record)
	if err != nil || hex.EncodeToString(row.Key) != "76312d626167002185812179ffd2b19c8154d2d409599d231fb75ef4968df59b7f02b435c094fa0000000000000000" {
		t.Fatal("canonical v1 key vector differs")
	}
	records := rowTestV1(t, rowTestTable(t, fields))
	for _, r := range records {
		meta, _, _ := DecodeNativeRowRecord(r)
		if meta != nil && r.Ref.ID != "source/native-history/meta/eeef3077e92322fc038c87d285b9e2f27a98e3f75b5210275949af92f1bf9025" {
			t.Fatal("meta identity vector differs")
		}
	}
	current, err := NativeV1RowRecord(rowTestSource, "issues", FamilyUnversioned, fields, 0)
	if err != nil || !strings.HasPrefix(current.Ref.ID, nativeCurrentPrefix) || current.Ref == record.Ref {
		t.Fatal("history/current planes alias")
	}
	first, err := NativeV1RowRecord(SourceIdentity{StoreID: "a/b", Project: "c"}, "issues", nativeRetainedHistory, fields, 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NativeV1RowRecord(SourceIdentity{StoreID: "a", Project: "b/c"}, "issues", nativeRetainedHistory, fields, 0)
	if err != nil || first.Ref == second.Ref {
		t.Fatal("length-prefixed source tuple aliases")
	}
	// Generic drivers may reconstruct Ref without the constructor's private
	// marker, and an omitted branch is still branch zero.
	literal := record
	literal.Ref = model.Ref{ID: record.Ref.ID}
	if _, _, err := DecodeNativeRowRecord(literal); err != nil {
		t.Fatal(err)
	}
	duplicate := append(append([]driver.Record{}, records...), literal)
	if _, _, err := ReconstructNativeRowTable(rowTestSource, "issues", nativeRetainedHistory, duplicate, nil); err == nil {
		t.Fatal("same driver identity accepted twice")
	}
	// These two individually invalid strings form valid UTF-8 only when
	// concatenated. JSON would otherwise replace each, breaking the bound Ref.
	if _, err := NativeV1RowRecord(SourceIdentity{StoreID: "\xc3", Project: "\xa9"}, "issues", nativeRetainedHistory, fields, 0); err == nil {
		t.Fatal("invalid individual source strings accepted")
	}
}

func TestNativeRowStrictCanonicalDecoder(t *testing.T) {
	record, err := NativeV1RowRecord(rowTestSource, "issues", nativeRetainedHistory, Row{"id": json.Number("1")}, 0)
	if err != nil {
		t.Fatal(err)
	}
	text := string(record.Body)
	cases := map[string]string{
		"unknown":                    strings.Replace(text, `"format":`, `"unknown":0,"format":`, 1),
		"duplicate":                  strings.Replace(text, `"format":`, `"format":"plug-native-row/1","format":`, 1),
		"case folded":                strings.Replace(text, `"format":`, `"Format":`, 1),
		"missing":                    strings.Replace(text, `"history":"retained-native-commit",`, "", 1),
		"null array":                 strings.TrimSuffix(text, "}") + `,"cells":null}`,
		"empty array":                strings.TrimSuffix(text, "}") + `,"cells":[]}`,
		"trailing":                   text + "\n",
		"trailing value":             text + "{}",
		"space":                      " " + text,
		"duplicate normalized field": strings.Replace(text, `"id":1`, `"id":0,"id":1`, 1),
		"lossy surrogate":            strings.Replace(text, `"id":1`, `"id":"\ud800"`, 1),
		"invalid UTF8":               strings.Replace(text, `"id":1`, "\"id\":\"\xff\"", 1),
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			copy := record
			copy.Body = []byte(text)
			if _, _, err := DecodeNativeRowRecord(copy); err == nil {
				t.Fatal("noncanonical row accepted")
			}
		})
	}
	value := NativeTable{Columns: []string{"id"}, Rows: []Row{}}
	metaRecord := rowTestV1(t, value)[0]
	for _, key := range []string{"row_count", "physical_columns", "physical_table_sha256"} {
		t.Run("missing meta "+key, func(t *testing.T) {
			var body map[string]json.RawMessage
			if err := json.Unmarshal(metaRecord.Body, &body); err != nil {
				t.Fatal(err)
			}
			delete(body, key)
			// Reorder through a map deliberately as well: closed canonical bytes
			// must refuse rather than silently filling omitted zero/null fields.
			copy := metaRecord
			copy.Body, _ = json.Marshal(body)
			if _, _, err := DecodeNativeRowRecord(copy); err == nil {
				t.Fatal("missing metadata field accepted")
			}
		})
	}
}

func TestNativeRowIdentitySchemaAndCoherenceRefusals(t *testing.T) {
	goodSchema := NativeDeltaTableSchema{Name: "issues", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "BIGINT", PrimaryKeyOrdinal: rowTestPK(0)}}}
	good := rowTestPhysical(t, goodSchema, Row{"id": json.Number("7")}, []NativeDeltaCell{{Kind: "int64", Value: "7"}}, 0)
	for name, mutate := range map[string]func(*NativeDeltaRow){
		"normalized differs":      func(r *NativeDeltaRow) { r.NormalizedJSON = base64.StdEncoding.EncodeToString([]byte("{\"id\":8}\n")) },
		"noncanonical normalized": func(r *NativeDeltaRow) { r.NormalizedJSON = base64.StdEncoding.EncodeToString([]byte("{ \"id\":7}\n")) },
		"no newline":              func(r *NativeDeltaRow) { r.NormalizedJSON = base64.StdEncoding.EncodeToString([]byte("{\"id\":7}")) },
		"PK differs":              func(r *NativeDeltaRow) { r.Key = base64.StdEncoding.EncodeToString([]byte("pk\x00bad")) },
		"invalid width":           func(r *NativeDeltaRow) { r.Cells = []NativeDeltaCell{{Kind: "int8", Value: "128"}} },
		"SQL NULL PK": func(r *NativeDeltaRow) {
			r.Cells = []NativeDeltaCell{{Kind: "sql-null", Value: ""}}
			r.NormalizedJSON = base64.StdEncoding.EncodeToString([]byte("{\"id\":null}\n"))
		},
		"missing cells": func(r *NativeDeltaRow) { r.Cells = nil },
	} {
		t.Run(name, func(t *testing.T) {
			copy := good
			mutate(&copy)
			if _, err := NativeV2RowRecord(rowTestSource, goodSchema, nativeRetainedHistory, copy, nil); err == nil {
				t.Fatal("invalid physical row accepted")
			}
		})
	}
	for name, schema := range map[string]NativeDeltaTableSchema{
		"noncontiguous PK": {Name: "issues", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "BIGINT", PrimaryKeyOrdinal: rowTestPK(1)}}},
		"duplicate column": {Name: "issues", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "BIGINT"}, {Name: "id", SQLType: "TEXT"}}},
		"missing SQL type": {Name: "issues", Columns: []NativeDeltaColumn{{Name: "id"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NativeV2TableRecords(rowTestSource, schema, nativeRetainedHistory, []NativeDeltaRow{}, nil); err == nil {
				t.Fatal("invalid physical schema accepted")
			}
		})
	}
	for name, fields := range map[string]Row{
		"raw credential":   {"password": "not-permitted"},
		"invalid text":     {"id": "\xff"},
		"non JSON number":  {"id": json.Number("NaN")},
		"empty number":     {"id": json.Number("")},
		"float conversion": {"id": float64(9007199254740992)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NativeV1RowRecord(rowTestSource, "issues", nativeRetainedHistory, fields, 0); err == nil {
				t.Fatal("invalid normalized row accepted")
			}
		})
	}
	// Dense v1 duplicate occurrences are checked against complete membership.
	fields := Row{"id": json.Number("7")}
	gap, err := NativeV1RowRecord(rowTestSource, "issues", nativeRetainedHistory, fields, 1)
	if err != nil {
		t.Fatal(err)
	}
	records := rowTestV1(t, rowTestTable(t, fields))
	for i, r := range records {
		_, row, _ := DecodeNativeRowRecord(r)
		if row != nil {
			records[i] = gap
		}
	}
	if _, _, err := ReconstructNativeRowTable(rowTestSource, "issues", nativeRetainedHistory, records, nil); err == nil {
		t.Fatal("v1 bag occurrence gap accepted")
	}
}

func TestNativeRowV2CurrentNullCredentialAndPolicy(t *testing.T) {
	schema := NativeDeltaTableSchema{Name: "federation_peers", Columns: []NativeDeltaColumn{{Name: "name", SQLType: "TEXT", PrimaryKeyOrdinal: rowTestPK(0)}, {Name: "password_encrypted", SQLType: "BLOB"}}}
	fields := Row{"name": "public-peer", "password_encrypted": nil}
	row := rowTestPhysical(t, schema, fields, []NativeDeltaCell{{Kind: "string", Value: "public-peer"}, {Kind: "sql-null", Value: ""}}, 0)
	records, err := NativeV2TableRecords(rowTestSource, schema, FamilyUnversioned, []NativeDeltaRow{row}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rowTestOracle(t, rowTestSource, schema.Name, FamilyUnversioned, records, rowTestTable(t, fields))
	// The narrowly permitted absent peer credential must not weaken nested or
	// nonempty credential validation in otherwise typed physical rows.
	bad := rowTestPhysical(t, schema, Row{"name": "public-peer", "password_encrypted": "plaintext"}, []NativeDeltaCell{{Kind: "string", Value: "public-peer"}, {Kind: "string", Value: "plaintext"}}, 0)
	if _, err := NativeV2RowRecord(rowTestSource, schema, FamilyUnversioned, bad, nil); err == nil {
		t.Fatal("nonempty raw peer credential accepted")
	}
	config := NativeDeltaTableSchema{Name: "config", Columns: []NativeDeltaColumn{{Name: "key", SQLType: "TEXT", PrimaryKeyOrdinal: rowTestPK(0)}, {Name: "value", SQLType: "TEXT"}}}
	secretFields := Row{"key": "ordinary", "value": map[string]any{"$secret": "config/ordinary"}}
	secret := rowTestPhysical(t, config, secretFields, []NativeDeltaCell{{Kind: "string", Value: "ordinary"}, {Kind: "secret-reference", Value: "config/ordinary"}}, 0)
	if _, err := NativeV2TableRecords(rowTestSource, config, FamilyVersioned, []NativeDeltaRow{secret}, nil); err == nil {
		t.Fatal("undeclared secret policy accepted")
	}
	policy := map[string]string{"config/ordinary": "secret"}
	secretRecords, err := NativeV2TableRecords(rowTestSource, config, FamilyVersioned, []NativeDeltaRow{secret}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReconstructNativeRowTable(rowTestSource, config.Name, FamilyVersioned, secretRecords, policy); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReconstructNativeRowTable(rowTestSource, config.Name, FamilyVersioned, secretRecords, nil); err == nil {
		t.Fatal("readback lost required source policy")
	}
}

func TestNativeRowOpaqueCurrentTableOracle(t *testing.T) {
	for _, name := range []string{"dolt_ignore", "dolt_nonlocal_tables", "dolt_schemas"} {
		t.Run(name, func(t *testing.T) {
			schema := NativeDeltaTableSchema{Name: name, Columns: []NativeDeltaColumn{{Name: "name", SQLType: "TEXT"}, {Name: "value", SQLType: "TEXT"}}}
			fields := Row{"name": "public-view", "value": "SELECT '<public>' AS value"}
			row := rowTestPhysical(t, schema, fields, []NativeDeltaCell{{Kind: "string", Value: "public-view"}, {Kind: "string", Value: "SELECT '<public>' AS value"}}, 0)
			records, err := NativeV2TableRecords(rowTestSource, schema, NativeRowOpaqueCurrentHistory, []NativeDeltaRow{row}, nil)
			if err != nil {
				t.Fatal(err)
			}
			meta := rowTestOracle(t, rowTestSource, name, NativeRowOpaqueCurrentHistory, records, rowTestTable(t, fields))
			if meta.History != "opaque-native-physical" || meta.IdentityKind != NativeRowD3Physical {
				t.Fatal("opaque current classification or typed identity changed")
			}
			for _, record := range records {
				if !strings.HasPrefix(record.Ref.ID, "source/native-current/") {
					t.Fatal("opaque current evidence crossed into history plane")
				}
			}
			// A normalized-only codec input still claims no physical types. The
			// importer, rather than this classification string, authenticates
			// whether that input belongs to the source's current table census.
			bag, err := NativeV1TableRecords(rowTestSource, name, NativeRowOpaqueCurrentHistory, rowTestTable(t, fields))
			if err != nil {
				t.Fatal(err)
			}
			bagMeta := rowTestOracle(t, rowTestSource, name, NativeRowOpaqueCurrentHistory, bag, rowTestTable(t, fields))
			if bagMeta.IdentityKind != NativeRowV1ContentBag || bagMeta.PhysicalColumns != nil || bagMeta.PhysicalTableSHA256 != nil {
				t.Fatal("opaque label invented typed provenance")
			}
			for _, history := range []string{"opaque", "opaque-native-physical-extra", "unknown"} {
				if _, err := NativeV2RowRecord(rowTestSource, schema, history, row, nil); err == nil {
					t.Fatal("unknown history class accepted")
				}
			}
		})
	}
}

func TestNativeRowConstructorsDetachBodies(t *testing.T) {
	fields := Row{"id": json.Number("7"), "json_data": map[string]any{"a": "before"}}
	records := rowTestV1(t, rowTestTable(t, fields))
	before := rowTestOldBytes(t, rowTestSource, "issues", nativeRetainedHistory, rowTestTable(t, fields))
	fields["json_data"].(map[string]any)["a"] = "after"
	got, _, err := ReconstructNativeRowTable(rowTestSource, "issues", nativeRetainedHistory, records, nil)
	if err != nil || !bytes.Equal(before, rowTestOldBytes(t, rowTestSource, "issues", nativeRetainedHistory, got)) {
		t.Fatal("input mutation changed detached record bodies")
	}
	for i := range records {
		for j := range records[i].Body {
			records[i].Body[j] = ' '
		}
	}
	if !bytes.Equal(before, rowTestOldBytes(t, rowTestSource, "issues", nativeRetainedHistory, got)) {
		t.Fatal("input Body mutation changed reconstructed owned data")
	}
}

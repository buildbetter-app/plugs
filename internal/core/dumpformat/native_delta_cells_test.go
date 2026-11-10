package dumpformat

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestNativeDeltaTypedCellsKeepExactIntegerBinaryNullAndPolicy(t *testing.T) {
	table := NativeDeltaTableSchema{Name: "items", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "BIGINT", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "blob", SQLType: "VARBINARY(8)"}, {Name: "value", SQLType: "JSON"}, {Name: "nullable", SQLType: "TEXT"}}}
	cells := []NativeDeltaCell{{Kind: "int64", Value: "9007199254740993"}, {Kind: "bytes", Value: "AP8="}, {Kind: "string", Value: `{"precise":9007199254740993}`}, {Kind: "sql-null", Value: ""}}
	fields := Row{"id": json.Number("9007199254740993"), "blob": map[string]any{"$bytes": "AP8="}, "value": map[string]any{"precise": json.Number("9007199254740993")}, "nullable": nil}
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	key, err := NativeDeltaPrimaryKey(table, cells)
	if err != nil {
		t.Fatal(err)
	}
	row := NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(raw)}
	if _, _, err := validateNativeDeltaRow(table, row, nil); err != nil {
		t.Fatal(err)
	}
	row.Cells[0].Value = "9007199254740992"
	if _, _, err := validateNativeDeltaRow(table, row, nil); err == nil {
		t.Fatal("integer rounded or typed/key mismatch admitted")
	}
	config := NativeDeltaTableSchema{Name: "config", Columns: []NativeDeltaColumn{{Name: "key", SQLType: "text", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	configCells := []NativeDeltaCell{{Kind: "string", Value: "ordinary"}, {Kind: "secret-reference", Value: "config/ordinary"}}
	configKey, err := NativeDeltaPrimaryKey(config, configCells)
	if err != nil {
		t.Fatal(err)
	}
	configRow := NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(configKey), Cells: configCells, NormalizedJSON: base64.StdEncoding.EncodeToString([]byte("{\"key\":\"ordinary\",\"value\":{\"$secret\":\"config/ordinary\"}}\n"))}
	if _, _, err := validateNativeDeltaRow(config, configRow, map[string]string{"config/ordinary": "secret"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := validateNativeDeltaRow(config, configRow, nil); err == nil {
		t.Fatal("secret authority inferred without explicit policy")
	}
}

func TestNativeDeltaNullableAndInheritedOmissionsStayExact(t *testing.T) {
	var header NativeDeltaHeader
	raw := []byte(`{"type":"header","format":"beads-native-delta/2","store_id":"public","projects":["public"],"head":"head","commit_count":1,"history_coverage":null,"ignore":{"columns":[],"rows":[]},"config_policy":{}}`)
	if err := decodeNativeDeltaJSON(raw, &header); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"family":"old","column":"","reason":"absent"}`,
		`{"family":"old","column":null,"reason":"absent"}`,
	} {
		var absence NativeCommitSchemaAbsence
		if err := decodeNativeDeltaJSON([]byte(raw), &absence); err == nil {
			t.Fatal("noncanonical inherited absence accepted")
		}
	}
	for _, tail := range []string{`,"commit_schema_version":null`, `,"sql_dolt_codes":[]`} {
		var boundary NativeHistoryBoundary
		raw := `{"commit_id":"boundary","child_commit_ids":["child"],"commit_ordinal_from_head":2,"reason":"missing_root_chunk"` + tail + `}`
		if err := decodeNativeDeltaJSON([]byte(raw), &boundary); err == nil {
			t.Fatal("noncanonical inherited boundary omission accepted")
		}
	}
}

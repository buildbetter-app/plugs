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

func TestNativeDeltaAbsentPeerPasswordKeepsExactAbsenceAndLegacyRules(t *testing.T) {
	table := NativeDeltaTableSchema{Name: "federation_peers", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "text", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "password_encrypted", SQLType: "VARBINARY(8)"}}}
	for _, absent := range []struct {
		cell  NativeDeltaCell
		value any
	}{{NativeDeltaCell{Kind: "sql-null"}, nil}, {NativeDeltaCell{Kind: "string"}, ""}, {NativeDeltaCell{Kind: "bytes"}, map[string]any{"$bytes": ""}}} {
		fields := Row{"id": "public-peer", "password_encrypted": absent.value}
		raw, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		cells := []NativeDeltaCell{{Kind: "string", Value: "public-peer"}, absent.cell}
		key, err := NativeDeltaPrimaryKey(table, cells)
		if err != nil {
			t.Fatal(err)
		}
		row := NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(append(raw, '\n'))}
		if _, _, err := validateNativeDeltaRow(table, row, nil); err != nil {
			t.Fatal("exact typed absence refused", err)
		}
		native := NativeTable{Columns: []string{"id", "password_encrypted"}, Rows: []Row{fields}}
		if err := validateNativeTablesPolicy(map[string]NativeTable{table.Name: native}, true); err != nil {
			t.Fatal("verified v2 normalized absence refused", err)
		}
		if err := validateNativeTables(map[string]NativeTable{table.Name: native}); err == nil {
			t.Fatal("legacy secret admission changed")
		}
	}
	// This exact SQL family/column exception does not authorize credentials,
	// additional envelope fields, nested secrets or an absent config password.
	for _, fields := range []map[string]any{
		{"password_encrypted": "public-placeholder"},
		{"password_encrypted": map[string]any{"$bytes": "AA=="}},
		{"password_encrypted": map[string]any{"$bytes": "", "extra": ""}},
		{"password_encrypted": nil, "details": map[string]any{"password_encrypted": nil}},
		{"password_encrypted": nil, "access_token": nil},
	} {
		if err := validateNativeDeltaSecretReferences("federation_peers", fields); err == nil {
			t.Fatal("peer absence exception escaped its exact field/value domain")
		}
	}
	if err := validateNativeDeltaSecretReferences("config", map[string]any{"password_encrypted": nil}); err == nil {
		t.Fatal("config secret rule changed")
	}
}

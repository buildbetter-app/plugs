package physicalcapture_test

import (
	"database/sql/driver"
	"encoding/json"
	"math"
	"testing"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

func TestPhysicalEvidencePreservesExactDetachedCellOccurrences(t *testing.T) {
	row := []driver.Value{nil, "null", "", []byte{}, []byte{0, 255}, int64(9007199254740993), uint64(18446744073709551615), float64(math.Copysign(0, -1)), "{\"n\":9007199254740992}", string([]byte{255, 0})}
	columns := []physicalcapture.Column{
		{Name: "sql_null", DatabaseTypeName: "JSON"}, {Name: "json_null", DatabaseTypeName: "JSON"},
		{Name: "empty_text", DatabaseTypeName: "LONGTEXT"}, {Name: "empty_bytes", DatabaseTypeName: "LONGBLOB"},
		{Name: "binary", DatabaseTypeName: "LONGBLOB"}, {Name: "signed", DatabaseTypeName: "BIGINT"},
		{Name: "unsigned", DatabaseTypeName: "BIGINT UNSIGNED"}, {Name: "negative_zero", DatabaseTypeName: "DOUBLE"},
		{Name: "native_json", DatabaseTypeName: "JSON"}, {Name: "opaque_text", DatabaseTypeName: "LONGTEXT"},
	}
	tables, err := physicalcapture.EvidenceTables([]physicalcapture.Table{{Name: "public_keyless", Columns: columns, Rows: [][]driver.Value{row, row}}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := physicalcapture.MarshalEvidence(physicalcapture.Evidence{Format: "beads-physical-evidence/1", Images: []physicalcapture.EvidenceImage{{Tables: tables}}})
	if err != nil {
		t.Fatal(err)
	}
	var readback physicalcapture.Evidence
	if err := json.Unmarshal(data, &readback); err != nil {
		t.Fatal(err)
	}
	got := readback.Images[0].Tables[0]
	if len(got.Rows) != 2 || len(got.Columns) != len(columns) {
		t.Fatal("keyless multiplicity or SQL schema lost")
	}
	expected := []physicalcapture.EvidenceCell{
		{Kind: "sql-null"}, {Kind: "string", Value: "null"}, {Kind: "string"}, {Kind: "bytes"},
		{Kind: "bytes", Value: "AP8="}, {Kind: "int64", Value: "9007199254740993"},
		{Kind: "uint64", Value: "18446744073709551615"}, {Kind: "float64-bits", Value: "8000000000000000"},
		{Kind: "string", Value: "{\"n\":9007199254740992}"}, {Kind: "string-bytes", Value: "/wA="},
	}
	for r, cells := range got.Rows {
		for i, cell := range cells {
			if cell != expected[i] || got.Columns[i].SQLType != columns[i].DatabaseTypeName {
				t.Fatalf("typed occurrence changed row=%d column=%d", r, i)
			}
		}
	}
}

func TestPhysicalEvidenceRefusesUnrepresentableOrRaggedInput(t *testing.T) {
	for _, row := range [][]driver.Value{{json.Number("9007199254740993")}, {nil, nil}} {
		if data, err := physicalcapture.EvidenceTables([]physicalcapture.Table{{Name: "public", Columns: []physicalcapture.Column{{Name: "cell", DatabaseTypeName: "JSON"}}, Rows: [][]driver.Value{row}}}); err == nil || data != nil {
			t.Fatal("non-driver type or ragged evidence accepted")
		}
	}
}

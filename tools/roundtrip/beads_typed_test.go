package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// These controls exercise only the adapter's changed boundary. They are not
// physical reader execution or evidence for any of the twelve native leaves.
func typedAdapterControl(t *testing.T) ([]byte, typedBinding) {
	t.Helper()
	tables := []typedTable{{Name: "issues", Columns: []typedColumn{{"id", "VARCHAR"}, {"wide", "BIGINT"}, {"document", "JSON"}, {"optional", "TEXT"}, {"binary", "BLOB"}}, Rows: [][]typedCell{{{"string", "demo-42v"}, {"int64", "9007199254740993"}, {"string", "null"}, {"sql-null", ""}, {"bytes", ""}}}}}
	evidence := typedEvidence{Format: "beads-physical-evidence/1", ArchiveSHA256: strings.Repeat("a", 64), StoreID: "original-store", Projects: []string{"demo"}, SQLDefaults: "not-captured", Working: typedWorking{Format: "beads-physical-working-root/1", Database: "demo", HeadCommitID: "head", WorkingRootHash: "root", ReadOnlyScope: true, Complete: true, Tables: tables}, Images: []typedImage{{CommitID: "head", ReadOnlyScope: true, Tables: tables}}}
	data, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	binding := typedBinding{Case: "memory/complete", Role: "intended-source", Workspace: "/original", Database: "demo", StoreID: "original-store", SourceSHA256: strings.Repeat("b", 64), LiveSHA256: strings.Repeat("c", 64), ImporterSHA256: strings.Repeat("d", 64), ImporterBuildRun: "1", ReaderCommit: typedReaderCommit, ReaderTree: typedReaderTree, ReaderSHA256: strings.Repeat("e", 64), ReaderBuildRun: "2"}
	return data, binding
}

func adaptControl(t *testing.T, data []byte, binding typedBinding) *typedManifest {
	t.Helper()
	got, err := adaptTypedManifest(data, binding, strings.Repeat("a", 64), "head", []string{"demo"})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestTypedAdapterPreservesObservedSQLTypesAndExactCellKinds(t *testing.T) {
	data, binding := typedAdapterControl(t)
	got := adaptControl(t, data, binding)
	row := got.Evidence.Working.Tables[0].Rows[0]
	if row[1].Kind != "int64" || row[1].Value != "9007199254740993" || row[2].Kind != "string" || row[2].Value != "null" || row[3].Kind != "sql-null" || row[4].Kind != "bytes" || row[4].Value != "" || got.Evidence.Working.Tables[0].Columns[1].SQLType != "BIGINT" || got.EvidenceSHA256 != hash(data) {
		t.Fatal("typed observations lost type, NULL or wide integer")
	}
	encoded, err := json.Marshal(got.Evidence)
	if err != nil || !sameJSON(json.RawMessage(data), json.RawMessage(encoded)) {
		t.Fatal("adapter changed observed typed evidence")
	}
}

func TestTypedAdapterRefusesIncompleteDuplicateWrongSourceAndUnsupportedInput(t *testing.T) {
	data, binding := typedAdapterControl(t)
	for _, test := range []struct {
		name string
		edit func(*typedEvidence, *typedBinding)
	}{
		{"wrong-store", func(e *typedEvidence, b *typedBinding) { e.StoreID = "other" }},
		{"wrong-database", func(e *typedEvidence, b *typedBinding) { e.Working.Database = "other" }},
		{"wrong-project", func(e *typedEvidence, b *typedBinding) { e.Projects = []string{"other"} }},
		{"missing-current", func(e *typedEvidence, b *typedBinding) { e.Working.Tables = nil }},
		{"missing-image", func(e *typedEvidence, b *typedBinding) { e.Images = nil }},
		{"duplicate-image", func(e *typedEvidence, b *typedBinding) { e.Images = append(e.Images, e.Images[0]) }},
		{"incomplete", func(e *typedEvidence, b *typedBinding) { e.Working.Complete = false }},
		{"write-scope", func(e *typedEvidence, b *typedBinding) { e.Working.ReadOnlyScope = false }},
		{"duplicate-family", func(e *typedEvidence, b *typedBinding) {
			e.Working.Tables = append(e.Working.Tables, e.Working.Tables[0])
		}},
		{"duplicate-column", func(e *typedEvidence, b *typedBinding) {
			e.Working.Tables[0].Columns[1] = e.Working.Tables[0].Columns[0]
		}},
		{"absent-sql-type", func(e *typedEvidence, b *typedBinding) { e.Working.Tables[0].Columns[0].SQLType = "" }},
		{"missing-cell", func(e *typedEvidence, b *typedBinding) { e.Working.Tables[0].Rows[0] = e.Working.Tables[0].Rows[0][:4] }},
		{"unsupported-kind", func(e *typedEvidence, b *typedBinding) { e.Working.Tables[0].Rows[0][1].Kind = "decimal-inferred" }},
		{"integer-overflow", func(e *typedEvidence, b *typedBinding) { e.Working.Tables[0].Rows[0][1].Kind = "int8" }},
		{"missing-import-binding", func(e *typedEvidence, b *typedBinding) { b.ImporterSHA256 = "" }},
		{"unsupported-reader", func(e *typedEvidence, b *typedBinding) { b.ReaderCommit = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var e typedEvidence
			if strictTypedJSON(data, &e) != nil {
				t.Fatal("control invalid")
			}
			b := binding
			test.edit(&e, &b)
			raw, _ := json.Marshal(e)
			if _, err := adaptTypedManifest(raw, b, strings.Repeat("a", 64), "head", []string{"demo"}); err == nil {
				t.Fatal("invalid typed input acquired a manifest")
			}
		})
	}
	for _, test := range []struct{ name, raw string }{
		{"duplicate-key", `{"kind":"sql-null","kind":"string"}`},
		{"sql-null-with-value", `{"kind":"sql-null","value":""}`},
		{"json-null-value", `{"kind":"string","value":null}`},
		{"unknown-cell-field", `{"kind":"string","inferred":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var cell typedCell
			if strictTypedJSON([]byte(test.raw), &cell) == nil {
				t.Fatal("invalid tagged cell accepted")
			}
		})
	}
}

func TestTypedComparisonCannotUseObservedTargetAsIntendedSourceOrBypassNativeRefusal(t *testing.T) {
	data, binding := typedAdapterControl(t)
	expected := adaptControl(t, data, binding)
	var targetEvidence typedEvidence
	if strictTypedJSON(data, &targetEvidence) != nil {
		t.Fatal("control invalid")
	}
	targetEvidence.StoreID = "target-store"
	targetData, _ := json.Marshal(targetEvidence)
	targetBinding := binding
	targetBinding.Role = "observed-target"
	targetBinding.Workspace = "/target"
	targetBinding.StoreID = "target-store"
	observed := adaptControl(t, targetData, targetBinding)
	if got := compareTypedManifests(binding.Case, expected, observed, true); got.Status != "PASS" || got.EqualityCredit != 1 {
		t.Fatalf("distinct synthetic control comparison: %+v", got)
	}
	for _, test := range []struct {
		name               string
		expected, observed *typedManifest
		accepted           bool
	}{
		{"original-missing", nil, observed, true}, {"target-missing", expected, nil, true}, {"self", observed, observed, true}, {"native-refused", expected, observed, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := compareTypedManifests(binding.Case, test.expected, test.observed, test.accepted)
			if got.Status != "GAP" || got.EqualityCredit != 0 || len(got.Gaps) == 0 {
				t.Fatal("missing or refused evidence gained equality")
			}
		})
	}
	for _, test := range []struct {
		name string
		edit func(*typedManifest)
	}{
		{"wide-integer", func(m *typedManifest) { m.Evidence.Working.Tables[0].Rows[0][1].Value = "9007199254740992" }},
		{"json-vs-sql-null", func(m *typedManifest) { m.Evidence.Working.Tables[0].Rows[0][2] = typedCell{Kind: "sql-null"} }},
		{"blob-vs-text", func(m *typedManifest) { m.Evidence.Working.Tables[0].Rows[0][4] = typedCell{Kind: "string"} }},
		{"absent-family", func(m *typedManifest) { m.Evidence.Working.Tables = nil }},
		{"sql-type", func(m *typedManifest) { m.Evidence.Working.Tables[0].Columns[1].SQLType = "TEXT" }},
		{"row-multiplicity", func(m *typedManifest) {
			table := &m.Evidence.Working.Tables[0]
			table.Rows = append(table.Rows, table.Rows[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := adaptControl(t, targetData, targetBinding)
			test.edit(m)
			got := compareTypedManifests(binding.Case, expected, m, true)
			if got.Status != "FAIL" || got.EqualityCredit != 0 || len(got.Differences) == 0 {
				t.Fatal("typed difference gained equality")
			}
		})
	}
}

func TestTypedCompanionCensusRefusesOmittedFamiliesRowsColumnsAndWrongImage(t *testing.T) {
	data, binding := typedAdapterControl(t)
	for _, test := range []struct {
		name string
		edit func(*typedArchiveHistory)
	}{
		{"exact", func(h *typedArchiveHistory) {}},
		{"family", func(h *typedArchiveHistory) { delete(h.Current, "issues") }},
		{"row", func(h *typedArchiveHistory) {
			table := h.Current["issues"]
			table.Rows = []map[string]json.RawMessage{}
			h.Current["issues"] = table
		}},
		{"column", func(h *typedArchiveHistory) {
			table := h.Current["issues"]
			table.Columns = table.Columns[:4]
			h.Current["issues"] = table
		}},
		{"head", func(h *typedArchiveHistory) { h.Head = "other" }},
		{"image", func(h *typedArchiveHistory) { h.Images[0].CommitID = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := adaptControl(t, data, binding)
			table := typedArchiveTable{Columns: []string{"id", "wide", "document", "optional", "binary"}, Rows: []map[string]json.RawMessage{{"id": json.RawMessage(`"demo-42v"`), "wide": json.RawMessage(`9007199254740993`), "document": json.RawMessage(`null`), "optional": json.RawMessage(`null`), "binary": json.RawMessage(`""`)}}}
			h := typedArchiveHistory{Format: "beads-native-tables/1", StoreID: binding.StoreID, Projects: []string{"demo"}, Head: "head", Current: map[string]typedArchiveTable{"issues": table}}
			h.Images = append(h.Images, struct {
				CommitID string                       `json:"commit_id"`
				Tables   map[string]typedArchiveTable `json:"tables"`
			}{"head", map[string]typedArchiveTable{"issues": table}})
			test.edit(&h)
			raw, _ := json.Marshal(h)
			var buffer bytes.Buffer
			writer := tar.NewWriter(&buffer)
			if err := writer.WriteHeader(&tar.Header{Name: "history/native-tables.json", Mode: 0600, Size: int64(len(raw)), Typeflag: tar.TypeReg}); err != nil {
				t.Fatal(err)
			}
			writer.Write(raw)
			writer.Close()
			m.Evidence.ArchiveSHA256 = hash(buffer.Bytes())
			err := bindTypedArchive(buffer.Bytes(), m)
			if (err == nil) != (test.name == "exact") {
				t.Fatalf("coverage refusal %s: %v", test.name, err)
			}
		})
	}
}

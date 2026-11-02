package s1b25b28_test

import (
	"bytes"
	"encoding/json"
	"testing"
)

const referenceCommit = "9d6fff760b7a314b476539198504d77ce0959656"

func decodeRows(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	rows := []map[string]any{}
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

func TestFreshReferenceRecordingsAreBoundToSourceAndExport(t *testing.T) {
	var bundle struct {
		SourceCommit, SourceTree, SourceArchiveSHA256, BinarySHA256, BuildInfoSHA256 string
		ProjectionSHA256, RecorderSHA256, InputSHA256, ExistingTargetInputSHA256     string
		Driver, SchemaEvidence                                                       string
		VerifiedSourceFiles                                                          int
		Migrations                                                                   map[string]struct {
			Count  int
			SHA256 string
		}
		ProjectionFields, Omitted []string
		Runs                      []struct{ SourceExportSHA256, TargetExportSHA256, CommandsSHA256 string }
	}
	if err := json.Unmarshal(sourceFile(t, "spec/s1-b25-b28/testdata/provenance.json"), &bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.SourceCommit != referenceCommit || bundle.SourceTree != "cde121d6e4119e454857ad1c3ca293e2791be840" || bundle.SourceArchiveSHA256 != "3942e1a021287e185f14b04adcc115b2a1f7c122fe7a16478ea0c5fe58fd7cba" || bundle.BinarySHA256 != "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3" || bundle.BuildInfoSHA256 != "2edc35ea3a2f0d50f708bda2271742494191f3288466ba246f6a8d0502f64d50" || len(bundle.Runs) != 2 {
		t.Fatalf("missing pinned reference provenance: %+v", bundle)
	}
	if bundle.VerifiedSourceFiles != 5087 || bundle.Driver != "embedded-dolt" || bundle.SchemaEvidence != "pinned source migration inventory; not a runtime SQL census" || bundle.Migrations["main"].Count != 72 || bundle.Migrations["ignored"].Count != 28 || len(bundle.Migrations["main"].SHA256) != 64 || len(bundle.Migrations["ignored"].SHA256) != 64 || len(bundle.Omitted) != 8 {
		t.Fatal("incomplete driver/schema/projection provenance")
	}
	for _, item := range []struct{ path, want string }{
		{"record_reference.py", bundle.RecorderSHA256},
		{"testdata/inputs.jsonl", bundle.InputSHA256},
		{"testdata/existing-target.jsonl", bundle.ExistingTargetInputSHA256},
		{"testdata/projection.json", bundle.ProjectionSHA256},
	} {
		if digest(sourceFile(t, "spec/s1-b25-b28/"+item.path)) != item.want {
			t.Fatalf("provenance mismatch: %s", item.path)
		}
	}
	var first []byte
	for i, run := range []string{"run1", "run2"} {
		source := sourceFile(t, "spec/s1-b25-b28/testdata/"+run+"/source-export.jsonl")
		target := sourceFile(t, "spec/s1-b25-b28/testdata/"+run+"/target-export.jsonl")
		if digest(source) != bundle.Runs[i].SourceExportSHA256 || digest(target) != bundle.Runs[i].TargetExportSHA256 {
			t.Fatal("raw recording digest mismatch")
		}
		commands := sourceFile(t, "spec/s1-b25-b28/testdata/"+run+"/commands.json")
		if digest(commands) != bundle.Runs[i].CommandsSHA256 {
			t.Fatal("raw command receipt digest mismatch")
		}
		var steps []struct {
			Label string
			Exit  int
			Argv  []string
		}
		if err := json.Unmarshal(commands, &steps); err != nil || len(steps) != 7 {
			t.Fatalf("command receipts: %v", err)
		}
		for i, label := range []string{"source-init", "source-import", "source-export", "target-init", "target-keep", "target-import", "target-export"} {
			if steps[i].Label != label || steps[i].Exit != 0 || len(steps[i].Argv) == 0 {
				t.Fatalf("unfinished reference step: %+v", steps[i])
			}
		}
		// This independently projects raw public export bytes. It does not
		// accept a copied authored projection in place of reference output.
		selectRows := func(data []byte) map[string]any {
			result := map[string]any{}
			for _, row := range decodeRows(t, data) {
				id, ok := row["id"].(string)
				if !ok || result[id] != nil {
					t.Fatal("missing or duplicate reference identity")
				}
				fields := map[string]any{}
				for _, field := range []string{"title", "description", "status", "issue_type", "priority", "metadata", "labels", "spec_id", "close_reason"} {
					fields[field] = row[field]
				}
				result[id] = fields
			}
			return result
		}
		projection, err := json.Marshal(map[string]any{"source": selectRows(source), "afterImportIntoNonempty": selectRows(target)})
		if err != nil {
			t.Fatal(err)
		}
		if digest(projection) != bundle.ProjectionSHA256 {
			t.Fatal("independent raw projection digest mismatch")
		}
		if i == 0 {
			first = projection
		} else if !bytes.Equal(first, projection) {
			t.Fatal("fresh reference projections differ")
		}
		from, to := selectRows(source), selectRows(target)
		if len(from) != 2 || len(to) != 3 || to["ref-keep"] == nil {
			t.Fatal("reference import lost existing or imported state")
		}
		for id, row := range from {
			left, _ := json.Marshal(row)
			right, _ := json.Marshal(to[id])
			if !bytes.Equal(left, right) {
				t.Fatalf("reference import changed exported fields: %s", id)
			}
		}
	}
}

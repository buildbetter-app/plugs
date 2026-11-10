package reverseconverter

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
)

// Move real fixture content behind an executable mapping so the composed
// exporter must execute it while preserving the original physical contracts.
func mappedPhysicalFixture(t *testing.T, dump *Dump) *Converter {
	t.Helper()
	record := dump.Records["memory"][0]
	record.Kind = "native.note"
	dump.Records[record.Kind] = []Record{record}
	delete(dump.Records, "memory")
	for i := range dump.Manifest.Kinds {
		if dump.Manifest.Kinds[i].Name == "memory" {
			dump.Manifest.Kinds[i] = Kind{Name: record.Kind, Version: 1, File: "records/" + record.Kind + ".ndjson", Count: 1}
		}
	}
	converter, err := NewConverter(RecordMapping{Kind: record.Kind, BeadsKind: "memory", Convert: func(record Record) (Record, error) {
		record.Kind = "memory"
		return record, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return converter
}

func TestRegisteredReverseMappingPreservesPhysicalSelectionAndDirectives(t *testing.T) {
	dump := unknownTypeDump("decision")
	dump.Manifest.SourcePlaneAddressContract = dumpformat.SourcePlaneAddressContract
	active := cloneMap(dump.Records["bead"][0].Source.Fields)
	active["id"], active["issue_type"] = "demo-active", "task"
	active["ephemeral"], active["no_history"], active["storage_class"] = false, true, "ephemeral"
	dump.Records["beads-archive"] = append(dump.Records["beads-archive"], Record{Kind: "beads-archive", ID: "active-origin", Project: "demo", Body: map[string]any{"read_only": true}, Source: Provenance{Table: dumpformat.EphemeralEntityTable, Fields: active}})
	dump.Records["bead"][1].Source.Fields["ephemeral"] = true
	for i := range dump.Manifest.Kinds {
		dump.Manifest.Kinds[i].Count = len(dump.Records[dump.Manifest.Kinds[i].Name])
	}
	converter := mappedPhysicalFixture(t, &dump)
	before := cloneDump(dump)
	directives := []ontologymigration.Directive{allowTypeDirective("decision")}
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprintf("all=%v", all), func(t *testing.T) {
			options := Options{All: all, Directives: directives}
			unmapped, err := ConvertWithOptions(dump, options)
			var refusal *Refusal
			if !errors.As(err, &refusal) || len(unmapped.JSONL) != 0 {
				t.Fatalf("default exporter admitted unmapped content: %v %s", err, unmapped.JSONL)
			}
			result, err := converter.ConvertWithOptions(dump, options)
			if err != nil {
				t.Fatal(err)
			}
			wantIssues, wantOmitted := 3, 1
			if all {
				wantIssues, wantOmitted = 4, 0
			}
			if len(result.Projection.Issues) != wantIssues || len(result.Report.OmittedEphemeralRows) != wantOmitted || bytes.Contains(result.JSONL, []byte(`"id":"demo-1"`)) != all {
				t.Fatalf("physical selection changed: %+v %+v %s", result.Projection, result.Report, result.JSONL)
			}
			if !bytes.Contains(result.JSONL, []byte(`"id":"demo-active"`)) || !bytes.Contains(result.JSONL, []byte(`"`+dumpformat.EphemeralPlaneWireField+`":true`)) || !bytes.Contains(result.JSONL, []byte(`"issue_type":"decision"`)) || len(result.Report.TypeMappings) != 1 {
				t.Fatalf("physical origin or source issue type changed: %+v %s", result.Report, result.JSONL)
			}
			if len(result.Projection.Memories) != 2 || !bytes.Contains(result.JSONL, []byte(`"key":"fixture","value":"memory value"`)) || result.Report.InputDigest != digestDump(before) || !reflect.DeepEqual(dump, before) {
				t.Fatal("mapping lost content or changed original input identity")
			}
			if !all {
				wrapped, err := converter.ConvertWithDirectives(dump, directives)
				if err != nil || !bytes.Equal(wrapped.JSONL, result.JSONL) {
					t.Fatalf("directives wrapper changed default selection: %v", err)
				}
			}
		})
	}
}

func TestRegisteredReverseMappingPreservesTypedEdgeVersions(t *testing.T) {
	const raw = " {\"large\":9007199254740993,\"literal\":1.0,\"null\":null} "
	for _, test := range []struct {
		version int
		kind    string
	}{
		{dumpformat.EdgeVersionTypedDependencies, "tracks"},
		{dumpformat.EdgeVersionLocalOpaque, "local-opaque"},
	} {
		t.Run(fmt.Sprintf("v%d", test.version), func(t *testing.T) {
			dump := versionedDependencyDump(test.kind, raw)
			for i := range dump.Manifest.Kinds {
				if dump.Manifest.Kinds[i].Name == "edge" {
					dump.Manifest.Kinds[i].Version = test.version
				}
			}
			converter := mappedPhysicalFixture(t, &dump)
			before := cloneDump(dump)
			result, err := converter.Convert(dump)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Projection.Edges) != 1 || result.Projection.Edges[0].Type != test.kind || result.Projection.Edges[0].To != "demo-1" || !bytes.Contains(result.JSONL, []byte(`"key":"fixture","value":"memory value"`)) {
				t.Fatalf("mapped export lost typed edge or native payload: %s", result.JSONL)
			}
			found := false
			for _, line := range bytes.Split(bytes.TrimSpace(result.JSONL), []byte{'\n'}) {
				var issue issueLine
				if err := json.Unmarshal(line, &issue); err != nil {
					t.Fatal(err)
				}
				if issue.ID != "demo-2" {
					continue
				}
				if len(issue.Dependencies) != 1 || issue.Dependencies[0].Type != test.kind || issue.Dependencies[0].ThreadID != "thread-1" {
					t.Fatalf("typed dependency identity changed: %+v", issue.Dependencies)
				}
				var envelope map[string]any
				if err := json.Unmarshal([]byte(issue.Dependencies[0].Metadata), &envelope); err != nil || envelope["_plug_edge_id"] != "edge-1" || envelope["_plug_metadata"] != raw {
					t.Fatalf("typed dependency metadata changed: %+v %v", envelope, err)
				}
				found = true
			}
			if !found || result.Report.InputDigest != digestDump(before) || !reflect.DeepEqual(dump, before) {
				t.Fatal("typed edge output omitted owner or mutated input")
			}
		})
	}
}

func TestRegisteredReverseMappingRetainsPhysicalCollisionRefusal(t *testing.T) {
	dump := fixtureDump()
	dump.Manifest.SourcePlaneAddressContract = dumpformat.SourcePlaneAddressContract
	active := cloneMap(dump.Records["bead"][0].Source.Fields)
	active["ephemeral"], active["no_history"], active["storage_class"] = true, true, "ephemeral"
	dump.Records["beads-archive"] = append(dump.Records["beads-archive"], Record{Kind: "beads-archive", ID: "active-origin", Project: "demo", Body: map[string]any{"read_only": true}, Source: Provenance{Table: dumpformat.EphemeralEntityTable, Fields: active}})
	for i := range dump.Manifest.Kinds {
		dump.Manifest.Kinds[i].Count = len(dump.Records[dump.Manifest.Kinds[i].Name])
	}
	converter := mappedPhysicalFixture(t, &dump)
	for _, all := range []bool{false, true} {
		result, err := converter.ConvertWithOptions(dump, Options{All: all})
		var refusal *Refusal
		if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "source-plane-contract") || len(result.JSONL) != 0 || refusal.Report.InputDigest != digestDump(dump) {
			t.Fatalf("all=%v mapped exporter lost collision refusal: %v %s", all, err, result.JSONL)
		}
	}
}

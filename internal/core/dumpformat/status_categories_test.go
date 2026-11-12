package dumpformat

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

func nativeProducerArchive(t *testing.T, name string) []byte {
	t.Helper()
	want := map[string]string{"reprocody": "fe136810685c5d7c438ea7ebc3b868caf130ace167be24f671816ef69f9889c8", "reprosignals": "09fa02c4f96ede9b2c4207064c9cfeed0c732958696d7c3841636a88f888d309"}
	data, err := os.ReadFile("testdata/producer-0f44-" + name + ".tar")
	if err != nil || digest(data) != want[name] {
		t.Fatal("actual public producer fixture identity changed", err)
	}
	return data
}

func TestNativeStatusProducerArchivesKeepExactCatalogAndNames(t *testing.T) {
	for name, category := range map[string]string{"reprocody": "active", "reprosignals": "wip"} {
		t.Run(name, func(t *testing.T) {
			data := nativeProducerArchive(t, name)
			archive, err := Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			manifest := archive.Manifest()
			project := manifest.Projects[0]
			if manifest.StatusCategoryContract != StatusCategoryContract || projectStatusCategory(project, "todo") != category {
				t.Fatal("native project status changed")
			}
			source, ok := archive.NativeStatusOntology()
			snapshot, problems := ontology.Compile(source.Base, source.Overlays, nil)
			if !ok || len(problems) != 0 {
				t.Fatalf("native ontology: %v", problems)
			}
			effective := snapshot.Effective(project.Prefix)
			for _, status := range project.Statuses {
				got, found := effective.Status(status.Name)
				if !found || got.Name != status.Name || string(got.Category) != status.Category {
					t.Fatalf("native name/category changed: %+v", got)
				}
			}
			if _, found := effective.Status("failed"); found {
				t.Fatal("undeclared legacy status entered native catalog")
			}
			for _, record := range archive.Records("bead") {
				status := stringValue(record.Source.Fields["status"])
				if record.Body["status_category"] != projectStatusCategory(project, status) {
					t.Fatal("record lost native category or status")
				}
			}
			history, found := archive.SourceHistory()
			if !found || history.Range.Status != "unverified" || history.Range.Reason != "head-image-not-proven" || history.Range.Complete {
				t.Fatal("diagnostic producer history was promoted")
			}
			project.Statuses[0].Category = "active"
			if archive.Manifest().Projects[0].Statuses[0].Category != "wip" {
				t.Fatal("catalog accessor aliases validated archive")
			}
		})
	}
}

func TestNativeStatusCatalogsRefuseDowngradeForgeryAndUnknownNames(t *testing.T) {
	data := nativeProducerArchive(t, "reprocody")
	for name, change := range map[string]func(map[string]any){
		"missing marker": func(m map[string]any) { delete(m, "status_category_contract") },
		"empty marker":   func(m map[string]any) { m["status_category_contract"] = "" },
		"null marker":    func(m map[string]any) { m["status_category_contract"] = nil },
		"alias marker": func(m map[string]any) {
			m["STATUS_CATEGORY_CONTRACT"] = m["status_category_contract"]
			delete(m, "status_category_contract")
		},
		"aliased status field": func(m map[string]any) {
			entry := m["projects"].([]any)[0].(map[string]any)["statuses"].([]any)[0].(map[string]any)
			entry["Category"] = entry["category"]
			delete(entry, "category")
		},
		"null statuses":   func(m map[string]any) { m["projects"].([]any)[0].(map[string]any)["statuses"] = nil },
		"missing catalog": func(m map[string]any) { delete(m["projects"].([]any)[0].(map[string]any), "statuses") },
		"forged category": func(m map[string]any) {
			m["projects"].([]any)[0].(map[string]any)["statuses"].([]any)[0].(map[string]any)["category"] = "active"
		},
	} {
		t.Run(name, func(t *testing.T) {
			members := sourcePlaneReaderMembers(t, data)
			var manifest map[string]any
			if err := json.Unmarshal(members["manifest.json"], &manifest); err != nil {
				t.Fatal(err)
			}
			change(manifest)
			members["manifest.json"] = fixtureJSON(t, manifest)
			if _, err := Read(bytes.NewReader(fixtureTar(t, members))); err == nil {
				t.Fatal("invalid native catalog admitted")
			}
		})
	}

}

func TestNativeStatusOntologyIsolatesSameNameAcrossProjects(t *testing.T) {
	left, right := nativeBuiltInStatuses(), nativeBuiltInStatuses()
	left = append(left, ProjectStatus{"todo", "active"})
	right = append(right, ProjectStatus{"todo", "wip"})
	archive := &Archive{manifest: Manifest{StatusCategoryContract: StatusCategoryContract, Projects: []Project{{Prefix: "cody-ops", Statuses: left}, {Prefix: "signals-v2", Statuses: right}}}}
	source, _ := archive.NativeStatusOntology()
	snapshot, problems := ontology.Compile(source.Base, source.Overlays, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if !snapshot.Effective("cody-ops").ReadyEligible("issue", "todo") || snapshot.Effective("signals-v2").ReadyEligible("issue", "todo") {
		t.Fatal("native category crossed project boundary")
	}
	if _, found := snapshot.Effective("other").Status("todo"); found {
		t.Fatal("undeclared project inherited native catalog")
	}
}

func TestNativeStatusRecordsRefuseUnknownNameAndForgedCategory(t *testing.T) {
	for _, policy := range []string{"unknown-name", "forged-body"} {
		t.Run(policy, func(t *testing.T) {
			members := sourcePlaneReaderMembers(t, nativeProducerArchive(t, "reprocody"))
			lines := bytes.Split(bytes.TrimSuffix(members["records/bead.ndjson"], []byte{'\n'}), []byte{'\n'})
			var record Record
			if err := decodeJSON(lines[0], &record); err != nil {
				t.Fatal(err)
			}
			if policy == "unknown-name" {
				record.Source.Fields["status"] = "unlisted"
			} else {
				record.Body["status_category"] = "frozen"
			}
			lines[0] = bytes.TrimSpace(fixtureJSON(t, record))
			members["records/bead.ndjson"] = append(bytes.Join(lines, []byte{'\n'}), '\n')
			var manifest Manifest
			if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
				t.Fatal(err)
			}
			raw := members["records/bead.ndjson"]
			manifest.Files["records/bead.ndjson"] = File{SHA256: digest(raw), Size: int64(len(raw))}
			members["manifest.json"] = fixtureJSON(t, manifest)
			_, err := Read(bytes.NewReader(fixtureTar(t, members)))
			if err == nil {
				t.Fatal("forged native status admitted")
			}
			var refusal *MigrationRefusal
			if policy == "unknown-name" && !errors.As(err, &refusal) {
				t.Fatalf("unknown status lacks typed refusal: %v", err)
			}
		})
	}
}

func TestNativeHistoricalCatalogNeedsIndependentStoredEvidence(t *testing.T) {
	a, err := Read(bytes.NewReader(nativeProducerArchive(t, "reprocody")))
	if err != nil {
		t.Fatal(err)
	}
	var records []Record
	for _, kind := range historyKinds {
		records = append(records, a.Records(kind)...)
	}
	if err := validateHistoryImage(records, a.Manifest()); err != nil {
		t.Fatal("stored catalog evidence refused", err)
	}
	for _, policy := range []string{"missing", "changed"} {
		t.Run(policy, func(t *testing.T) {
			var image []Record
			for _, record := range records {
				if record.Source.Table == "config" && record.Source.Fields["key"] == "status.custom" {
					if policy == "missing" {
						continue
					}
					if err := decodeJSON(fixtureJSON(t, record), &record); err != nil {
						t.Fatal(err)
					}
					record.Source.Fields["value"] = "todo:wip,done:done,parked:frozen"
					record.Body["value"] = record.Source.Fields["value"]
				}
				image = append(image, record)
			}
			if err := validateHistoryImage(image, a.Manifest()); err == nil {
				t.Fatal("head catalog authenticated an unproven or changed older image")
			}
		})
	}
}

func TestNativeLiteralFrozenRequiresAnExplicitCustomCatalogEntry(t *testing.T) {
	rows := map[string][]Record{"beads-archive": {{Kind: "beads-archive", Source: Provenance{Table: "custom_statuses", Fields: Row{"name": "frozen", "category": "frozen"}}}}}
	catalog, err := sourcePlaneCatalogCategoriesFor(rows, []byte("[]"), true)
	if err != nil || catalog["frozen"] != "frozen" {
		t.Fatal("explicit native frozen category refused", err)
	}
	if builtInSourceStatus("frozen") {
		t.Fatal("literal frozen was invented as a native builtin")
	}
}

func TestActualNativeProducerMergeRetainsDirtySourceAndIncompleteEvidence(t *testing.T) {
	var archives []*Archive
	for _, name := range []string{"reprocody", "reprosignals"} {
		archive, err := Read(bytes.NewReader(nativeProducerArchive(t, name)))
		if err != nil {
			t.Fatal(err)
		}
		history, ok := archive.SourceHistory()
		if !ok || history.Range.Status != "unverified" || history.Range.Complete || len(history.Range.Frames) != 0 || history.Range.Reason != "head-image-not-proven" {
			t.Fatal("actual producer's incomplete-history evidence changed")
		}
		archives = append(archives, archive)
	}
	data, err := MergeArchives(archives...)
	if data != nil || err == nil || err.Error() != `offline merge input 0: unmerged or dirty source family "dolt-conflicts"` {
		t.Fatalf("actual dirty producer must refuse before history qualification: %v", err)
	}
}

// These detached controls exercise the public reader's catalog/type contract.
// The unchanged B23 capture set supplies the separate native acceptance input.
func TestNativeGateReaderCatalogDirectiveBoundary(t *testing.T) {
	for _, test := range []struct {
		name, sourceType, targetType, refusal string
		explicit, extraSource, extraBody      bool
	}{
		{name: "literal", sourceType: "gate", targetType: "gate"},
		{name: "explicit-to-issue", sourceType: "gate", targetType: "issue", explicit: true},
		{name: "explicit-precedence", sourceType: "gate", targetType: "gate", explicit: true, refusal: "gate projection must honor its explicit ontology directive"},
		{name: "forged-source-type", sourceType: "task", targetType: "gate", refusal: "typed body/source mapping mismatch"},
		{name: "unknown-source-column", sourceType: "gate", targetType: "gate", extraSource: true, refusal: "source record schema drift"},
		{name: "unknown-body-member", sourceType: "gate", targetType: "gate", extraBody: true, refusal: "typed body/source mapping mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			members := sourcePlaneReaderMembers(t, nativeProducerArchive(t, "reprocody"))
			var manifest Manifest
			if err := decodeJSON(members["manifest.json"], &manifest); err != nil {
				t.Fatal(err)
			}
			lines := bytes.Split(bytes.TrimSuffix(members["records/bead.ndjson"], []byte{'\n'}), []byte{'\n'})
			var record Record
			index := -1
			for i, line := range lines {
				if err := decodeJSON(line, &record); err != nil {
					t.Fatal(err)
				}
				if builtInIssueType(stringValue(record.Source.Fields["issue_type"])) {
					index = i
					break
				}
			}
			if index < 0 {
				t.Fatal("control fixture has no builtin source type")
			}
			record.Source.Fields["issue_type"] = test.sourceType
			record.Body["source_type"], record.Body["type"] = test.sourceType, test.targetType
			if test.extraSource {
				record.Source.Fields["unknown_column"] = "unexpected"
			}
			if test.extraBody {
				record.Body["unknown_member"] = "unexpected"
			}
			if test.explicit {
				manifest.Ontology = append(manifest.Ontology, OntologyDirective{
					ID: "native-gate-explicit", Kind: "map_type",
					Target: OntologyTarget{Project: record.Project, Type: "gate"}, From: "gate", To: "issue",
				})
			}
			lines[index] = bytes.TrimSpace(fixtureJSON(t, record))
			raw := append(bytes.Join(lines, []byte{'\n'}), '\n')
			members["records/bead.ndjson"] = raw
			manifest.Files["records/bead.ndjson"] = File{SHA256: digest(raw), Size: int64(len(raw))}
			members["manifest.json"] = fixtureJSON(t, manifest)
			archive, err := Read(bytes.NewReader(fixtureTar(t, members)))
			if test.refusal != "" {
				if err == nil || !strings.Contains(err.Error(), test.refusal) {
					t.Fatalf("want refusal %q, got %v", test.refusal, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, got := range archive.Records("bead") {
				if got.ID == record.ID {
					found = true
					if !equalJSON(got, record) {
						t.Fatal("reader changed source cells or the declared body")
					}
				}
			}
			if !found {
				t.Fatal("reader lost projected record")
			}
			source, ok := archive.NativeStatusOntology()
			if !ok || source.Base.Types[ontology.GateType].Workflow != "issue" || source.Overlays[record.Project].Types[ontology.GateType].Workflow != "issue" {
				t.Fatal("native catalog omitted the literal type definition")
			}
			if _, problems := ontology.Compile(source.Base, source.Overlays, nil); len(problems) != 0 {
				t.Fatal("projected native catalog does not compile", problems)
			}
		})
	}
}

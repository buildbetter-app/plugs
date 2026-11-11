package dumpformat

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

// These are authored parser compatibility controls. They are not native
// recordings, historical imports, or substitutes for public runtime cases.
func TestB14GateNativeCatalogDirectiveBoundary(t *testing.T) {
	for _, test := range []struct {
		name, source, target string
		native, mapping      bool
		want                 bool
	}{
		{"native-implicit-gate", "gate", "gate", true, false, true},
		{"native-explicit-issue", "gate", "issue", true, true, true},
		{"native-no-silent-relabel", "gate", "issue", true, false, false},
		{"native-directive-wins", "gate", "gate", true, true, false},
		{"legacy-explicit-issue", "gate", "issue", false, true, true},
		{"legacy-still-needs-directive", "gate", "issue", false, false, false},
		{"legacy-no-implicit-gate", "gate", "gate", false, false, false},
		{"unknown-still-needs-directive", "decision", "issue", true, false, false},
		{"unknown-cannot-borrow-gate", "decision", "gate", true, false, false},
		{"unknown-explicit-issue", "decision", "issue", true, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMergeFixture(t, "pl")
			row := fixture.records["bead"][0]
			row.Source.Fields["issue_type"], row.Body["source_type"], row.Body["type"] = test.source, test.source, test.target
			if test.native {
				fixture.manifest.Projects[0].Statuses = nativeBuiltInStatuses()
				row.Body["status_category"] = "active"
			}
			var directives []OntologyDirective
			if test.mapping {
				directives = []OntologyDirective{{ID: "map-gate", Kind: "map_type", Target: OntologyTarget{Project: "pl", Type: test.source}, From: test.source, To: "issue"}}
			}
			for _, head := range []bool{false, true} {
				err := validateOntology(directives, fixture.manifest.Projects)
				if err == nil {
					err = validateRecord(row, fixture.manifest.Projects, map[string]int{})
				}
				if err == nil {
					err = validateIssueOntology(directives, fixture.manifest.Projects, []Record{row}, head)
				}
				if (err == nil) != test.want {
					t.Fatalf("head=%v accepted=%v want=%v: %v", head, err == nil, test.want, err)
				}
			}
		})
	}
}

func TestB14NativeGateUsesExactImportedWorkflow(t *testing.T) {
	doc := nativeStatusDocument(nativeBuiltInStatuses())
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	effective := snapshot.Effective("pl")
	initial, known := effective.InitialStatus("gate")
	closed, closeKnown := effective.SlotStatus("gate", ontology.Close)
	if !known || !closeKnown || initial != "open" || closed != "closed" || doc.Types["gate"].Workflow != doc.Types["issue"].Workflow {
		t.Fatal("native gate was assigned a different workflow or default status")
	}
	if _, present := doc.Statuses["done"]; present {
		t.Fatal("fresh runtime status leaked into imported native ontology")
	}
}

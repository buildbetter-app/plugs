package plugdump

import (
	"reflect"
	"testing"
)

// These are detached producer-contract controls. The shared actual native
// fixture is captured separately by its existing author; these rows earn no
// native source/history credit.
func TestB14NativeGateTypeDirectiveCatalogBoundary(t *testing.T) {
	legacy := Project{Prefix: "gate", IDMode: "sequential", ChildCounters: map[string]uint64{}}
	modern := legacy
	modern.Statuses = []ProjectStatus{{"blocked", "wip"}, {"closed", "done"}, {"deferred", "frozen"}, {"hooked", "wip"}, {"in_progress", "wip"}, {"open", "active"}, {"pinned", "frozen"}}
	rows := []Row{{"issue_type": "task"}, {"issue_type": "gate"}, {"issue_type": "custom"}, {"issue_type": "gate"}}
	for _, test := range []struct {
		Name    string
		Project Project
		Types   []string
	}{
		{"legacy", legacy, []string{"custom", "gate"}},
		{"modern", modern, []string{"custom"}},
	} {
		t.Run(test.Name, func(t *testing.T) {
			directives, err := nativeTypeDirectives(rows, test.Project)
			if err != nil || len(directives) != len(test.Types) {
				t.Fatal("wrong native gate directive inventory", directives, err)
			}
			for i, directive := range directives {
				if directive.Target.Type != test.Types[i] || directive.From != test.Types[i] || directive.To != "issue" || directive.Target.Project != "gate" {
					t.Fatal("explicit legacy/custom mapping was changed", directives)
				}
			}
		})
	}
	modern.Statuses = modern.Statuses[:1]
	if _, err := nativeTypeDirectives(rows, modern); err == nil {
		t.Fatal("incomplete status inventory admitted a modern gate")
	}
}

func TestB14GateProjectionAccountsExplicitBeforeDefault(t *testing.T) {
	for _, test := range []struct {
		Name, SourceType, Want   string
		Modern, Explicit, Refuse bool
	}{
		{"native-gate-default", "gate", "gate", true, false, false},
		{"native-gate-explicit", "gate", "issue", true, true, false},
		{"legacy-gate-explicit", "gate", "issue", false, true, false},
		{"legacy-gate-missing", "gate", "", false, false, true},
		{"modern-custom-missing", "custom", "", true, false, true},
		{"modern-custom-explicit", "custom", "issue", true, true, false},
	} {
		t.Run(test.Name, func(t *testing.T) {
			m := mapping{statusCategories: test.Modern, usedOntology: map[string]bool{}}
			if test.Explicit {
				m.ontology = []OntologyDirective{{ID: "explicit", Kind: "map_type", Target: OntologyTarget{Project: "gate", Type: test.SourceType}, From: test.SourceType, To: "issue"}}
				if _, err := copyOntology(m.ontology, []Project{{Prefix: "gate"}}); err != nil {
					t.Fatal("historical explicit gate directive was refused", err)
				}
			}
			got, err := m.ontologyDirective("gate", test.SourceType, "gate-1")
			if test.Refuse {
				if err == nil || got != "" || len(m.usedOntology) != 0 {
					t.Fatal("unmapped source type was admitted or accounted")
				}
				return
			}
			if err != nil || got != test.Want || m.usedOntology["explicit"] != test.Explicit {
				t.Fatal("projection lost exact default/explicit target or accounting", got, err, m.usedOntology)
			}
		})
	}
	// Duplicate explicit mappings do not silently fall through to the
	// implicit modern gate.
	d := OntologyDirective{ID: "first", Kind: "map_type", Target: OntologyTarget{Project: "gate", Type: "gate"}, From: "gate", To: "issue"}
	m := mapping{statusCategories: true, ontology: []OntologyDirective{d, d}, usedOntology: map[string]bool{}}
	if _, err := m.ontologyDirective("gate", "gate", "gate-1"); err == nil {
		t.Fatal("ambiguous explicit directive bypassed")
	}
	if !reflect.DeepEqual(m.usedOntology, map[string]bool{}) {
		t.Fatal("ambiguous directive changed used accounting")
	}
}

func TestB14GateBodyAndArchiveValidationUseTheSameCatalogBoundary(t *testing.T) {
	statuses := []ProjectStatus{{"blocked", "wip"}, {"closed", "done"}, {"deferred", "frozen"}, {"hooked", "wip"}, {"in_progress", "wip"}, {"open", "active"}, {"pinned", "frozen"}}
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "explicit"}[explicit], func(t *testing.T) {
			project := Project{Prefix: "gate", IDMode: "sequential", ChildCounters: map[string]uint64{}, Statuses: statuses}
			m := mapping{statusCategories: true, projects: []Project{project}, beads: map[string]string{}, records: map[string][]Record{}, usedOntology: map[string]bool{}}
			if explicit {
				m.ontology = []OntologyDirective{{ID: "legacy", Kind: "map_type", Target: OntologyTarget{Project: "gate", Type: "gate"}, From: "gate", To: "issue"}}
			}
			row := Row{"id": "gate-1", "title": "actual gate body projection", "status": "open", "priority": 2, "issue_type": "gate", "storage_class": "versioned"}
			for _, field := range opaqueIssueFields {
				row[field] = nil
			}
			row["await_type"], row["await_id"], row["timeout_ns"], row["waiters"] = "human", "opaque", 0, `["a","a",""]`
			if err := m.issues([]Row{row}); err != nil {
				t.Fatal(err)
			}
			records := m.records["bead"]
			want := "gate"
			if explicit {
				want = "issue"
			}
			if len(records) != 1 || records[0].Body["type"] != want || records[0].Body["source_type"] != "gate" || !reflect.DeepEqual(records[0].Source.Fields["waiters"], row["waiters"]) || m.usedOntology["legacy"] != explicit {
				t.Fatal("actual producer body/source/accounting differs", records)
			}
			if err := validateOntologyRecords(m.ontology, []Project{project}, records); err != nil {
				t.Fatal("producer refuses its own projected gate", err)
			}
			for _, typ := range []string{"custom", "task"} {
				forged := records[0]
				forged.Source.Fields = cloneRow(records[0].Source.Fields)
				forged.Body = cloneRow(records[0].Body)
				forged.Source.Fields["issue_type"], forged.Body["source_type"], forged.Body["type"] = typ, typ, "gate"
				if err := validateOntologyRecords(nil, []Project{project}, []Record{forged}); err == nil {
					t.Fatal("arbitrary gate body bypassed source type", typ)
				}
			}
			legacy := project
			legacy.Statuses = nil
			if !explicit {
				if err := validateOntologyRecords(nil, []Project{legacy}, records); err == nil {
					t.Fatal("legacy gate body bypassed directive")
				}
				broken := project
				broken.Statuses = statuses[:1]
				if err := validateOntologyRecords(nil, []Project{broken}, records); err == nil {
					t.Fatal("malformed native catalog admitted gate")
				}
			} else {
				if err := validateOntologyRecords(m.ontology, []Project{legacy}, records); err != nil {
					t.Fatal("old explicit gate archive was broadened/refused", err)
				}
				wrong := records[0]
				wrong.Body = cloneRow(records[0].Body)
				wrong.Body["type"] = "gate"
				if err := validateOntologyRecords(m.ontology, []Project{project}, []Record{wrong}); err == nil {
					t.Fatal("implicit gate overrode explicit directive")
				}
			}
		})
	}
}

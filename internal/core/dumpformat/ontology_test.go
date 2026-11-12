package dumpformat

import (
	"bytes"
	"testing"
)

func decisionFixture(t *testing.T) *mergeFixture {
	t.Helper()
	f := newMergeFixture(t, "pl")
	f.records["bead"][0].Source.Fields["issue_type"] = "decision"
	f.records["bead"][0].Body["source_type"] = "decision"
	f.manifest.Ontology = []OntologyDirective{{
		ID: "preserve-decision", Kind: "map_type",
		Target: OntologyTarget{Project: "pl", Type: "decision"}, From: "decision", To: "issue",
	}}
	f.manifest.Verification["ontology"] = explicitOntologyVerification
	return f
}

func TestReadExplicitSourceTypeDirective(t *testing.T) {
	for _, narrow := range []bool{false, true} {
		f := decisionFixture(t)
		if narrow {
			f.manifest.Ontology[0].Target.ID = f.records["bead"][0].ID
		}
		a, err := Read(bytes.NewReader(f.bytes(t, nil)))
		if err != nil {
			t.Fatal(err)
		}
		if got := a.Records("bead")[0]; got.Source.Fields["issue_type"] != "decision" || got.Body["source_type"] != "decision" {
			t.Fatal("directive lost the original source type")
		}
		manifest := a.Manifest()
		manifest.Ontology[0].Target.Type = "changed"
		if a.Manifest().Ontology[0].Target.Type != "decision" {
			t.Fatal("manifest accessor did not detach ontology directives")
		}
	}
}

func TestReadOldAndNewBuiltInOntologyDeclarations(t *testing.T) {
	for _, declaration := range []string{verificationLimits["ontology"], explicitOntologyVerification} {
		f := newMergeFixture(t, "pl")
		f.manifest.Verification["ontology"] = declaration
		if _, err := Read(bytes.NewReader(f.bytes(t, nil))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadRefusesUnaccountedTypeDirectives(t *testing.T) {
	cases := map[string]func(*mergeFixture){
		"missing":            func(f *mergeFixture) { f.manifest.Ontology = nil },
		"old disclosure":     func(f *mergeFixture) { f.manifest.Verification["ontology"] = verificationLimits["ontology"] },
		"unknown disclosure": func(f *mergeFixture) { f.manifest.Verification["ontology"] = "verified" },
		"missing id":         func(f *mergeFixture) { f.manifest.Ontology[0].ID = " " },
		"wrong directive":    func(f *mergeFixture) { f.manifest.Ontology[0].Kind = "rename_field" },
		"unknown project":    func(f *mergeFixture) { f.manifest.Ontology[0].Target.Project = "other" },
		"built in remap": func(f *mergeFixture) {
			f.manifest.Ontology[0].Target.Type = "task"
			f.manifest.Ontology[0].From = "task"
		},
		"wrong source":      func(f *mergeFixture) { f.manifest.Ontology[0].From = "other" },
		"wrong destination": func(f *mergeFixture) { f.manifest.Ontology[0].To = "task" },
		"unused": func(f *mergeFixture) {
			f.records["bead"][0].Source.Fields["issue_type"] = "task"
			f.records["bead"][0].Body["source_type"] = "task"
		},
		"wrong issue": func(f *mergeFixture) { f.manifest.Ontology[0].Target.ID = "pl-2" },
		"duplicate id": func(f *mergeFixture) {
			d := f.manifest.Ontology[0]
			d.Target.ID = "pl-2"
			f.manifest.Ontology = append(f.manifest.Ontology, d)
		},
		"overlap": func(f *mergeFixture) {
			d := f.manifest.Ontology[0]
			d.ID = "overlap"
			d.Target.ID = f.records["bead"][0].ID
			f.manifest.Ontology = append(f.manifest.Ontology, d)
		},
		"uncovered second row": func(f *mergeFixture) {
			f.manifest.Ontology[0].Target.ID = f.records["bead"][0].ID
			f.records["bead"][1].Source.Fields["issue_type"] = "decision"
			f.records["bead"][1].Body["source_type"] = "decision"
		},
		"empty source": func(f *mergeFixture) {
			f.records["bead"][0].Source.Fields["issue_type"] = ""
			f.records["bead"][0].Body["source_type"] = ""
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := decisionFixture(t)
			mutate(f)
			if _, err := Read(bytes.NewReader(f.bytes(t, nil))); err == nil {
				t.Fatal("invalid ontology was admitted")
			}
		})
	}
}

func TestTypeDirectiveWireShapeIsClosed(t *testing.T) {
	for _, data := range []string{
		`{"id":"d","kind":"map_type","target":{"project":"pl","type":"decision"},"from":"decision","to":"issue","field":"status"}`,
		`{"id":"d","kind":"map_type","target":{"project":"pl","type":"decision","extra":true},"from":"decision","to":"issue"}`,
		`{"id":"d","kind":"map_type","target":{"project":"pl","type":"decision","id":null},"from":"decision","to":"issue"}`,
		`{"id":"d","kind":"map_type","target":{"project":"pl"},"from":"decision","to":"issue"}`,
		`{"id":"d","kind":"map_type","target":{"project":"pl","type":"decision"},"to":"issue"}`,
	} {
		var directive OntologyDirective
		if err := decodeJSON([]byte(data), &directive); err == nil {
			t.Fatal("accepted malformed directive shape")
		}
	}
}

func TestHistoricalTypePolicyCanBeAppliedWithoutUnusedHeadDirectives(t *testing.T) {
	f := decisionFixture(t)
	counts := map[string]int{}
	if err := validateRecord(f.records["bead"][0], f.manifest.Projects, counts); err != nil {
		t.Fatal(err)
	}
	if err := validateIssueOntology(nil, f.manifest.Projects, f.records["bead"][:1], false); err == nil {
		t.Fatal("historical custom type admitted without a directive")
	}
	if err := validateIssueOntology(f.manifest.Ontology, f.manifest.Projects, f.records["bead"][:1], false); err != nil {
		t.Fatal(err)
	}
	if err := validateIssueOntology(f.manifest.Ontology, f.manifest.Projects, f.records["bead"][1:], false); err != nil {
		t.Fatal("a historical frame need not use every head directive")
	}
}

package ontology

import "testing"

func boolFact(value bool) *bool { return &value }

func TestDefaultLifecycle(t *testing.T) {
	e := Default()
	state, problems := Defaults(e, BeadState{ID: "one", Type: "issue", Fields: map[string]any{"title": "Ship"}})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if !e.ReadyEligible(state.Type, state.Status) {
		t.Fatal("initial issue is not ready eligible")
	}
	facts := GuardFacts{NoOpenBlockers: boolFact(true), NoOpenChildren: boolFact(true)}
	claimed, problems := Transition(e, state, Claim, facts)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if e.ReadyEligible(claimed.Type, claimed.Status) {
		t.Fatal("claimed issue is ready eligible")
	}
	closed, problems := Transition(e, claimed, Close, facts)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	s, ok := e.Status(closed.Status)
	if !ok || s.Category != Completed || s.Outcome != Success {
		t.Fatalf("wrong close outcome: %+v", s)
	}
	if state.Status == claimed.Status {
		t.Fatal("transition mutated input")
	}
}

func TestDirectStatusEditAndGuards(t *testing.T) {
	e := Default()
	before, _ := Defaults(e, BeadState{Type: "issue"})
	after := before
	after.Status, _ = e.SlotStatus(before.Type, Close)
	for _, method := range []string{"bead.update", "tx.apply", ""} {
		problems := Validate(e, &before, &after, method, GuardFacts{})
		if len(problems) != 1 || problems[0].Code != "ontology_use_slot_verb" {
			t.Fatalf("%s: %+v", method, problems)
		}
	}
	_, problems := Transition(e, before, Claim, GuardFacts{})
	if len(problems) != 1 || problems[0].Guard != NoOpenBlockers {
		t.Fatalf("missing fact accepted: %+v", problems)
	}
	_, problems = Transition(e, before, Close, GuardFacts{NoOpenBlockers: boolFact(true), NoOpenChildren: boolFact(false)})
	if len(problems) != 1 || problems[0].Guard != NoOpenChildren {
		t.Fatalf("open child accepted: %+v", problems)
	}
}

func TestOverlayIsWholeReplacementAndDigestStable(t *testing.T) {
	base := DefaultDocument()
	overlay := Document{Types: map[string]Type{"issue": {Name: "Project issue", Workflow: "issue", Fields: map[string]Field{"severity": {Type: "enum", Values: []string{"low", "high"}}}}}}
	first, problems := Compile(base, map[string]Document{"project": overlay}, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	second, problems := Compile(DefaultDocument(), map[string]Document{"project": overlay}, nil)
	if len(problems) != 0 || first.Digest() != second.Digest() {
		t.Fatalf("unstable digest: %s %s %+v", first.Digest(), second.Digest(), problems)
	}
	state, _ := Defaults(first.Effective("project"), BeadState{Type: "issue", Fields: map[string]any{"title": "removed by overlay"}})
	problems2 := Validate(first.Effective("project"), nil, &state, "bead.create", GuardFacts{})
	if len(problems2) != 1 || problems2[0].Code != "ontology_unknown_field" {
		t.Fatalf("overlay recursively merged fields: %+v", problems2)
	}
	if problems := Validate(first.Effective("other"), nil, &state, "bead.create", GuardFacts{}); len(problems) != 0 {
		t.Fatal(problems)
	}
}

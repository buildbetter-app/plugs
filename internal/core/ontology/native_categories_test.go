package ontology

import (
	"encoding/json"
	"testing"
)

func nativeCategoryDocument() Document {
	doc := DefaultDocument()
	doc.Statuses = map[string]Status{
		"todo":     {Name: "todo", Category: Active},
		"working":  {Name: "working", Category: WIP},
		"finished": {Name: "finished", Category: Done},
		"waiting":  {Name: "waiting", Category: Frozen},
	}
	doc.Workflows = map[string]Workflow{"issue": {
		Statuses: []string{"todo", "working", "finished", "waiting"}, Initial: "todo",
		Slots:       map[Slot]string{Claim: "working", Close: "finished", Reopen: "todo", Defer: "waiting"},
		Transitions: []Rule{{To: "working"}, {To: "finished"}, {To: "todo"}, {To: "waiting"}},
	}}
	return doc
}

func TestNativeCategoriesPreserveNamesAndDriveLifecycle(t *testing.T) {
	doc := nativeCategoryDocument()
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, problems := Compile(decoded, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	e := snapshot.Effective("native")
	for name, want := range map[string]Category{"todo": Active, "working": WIP, "finished": Done, "waiting": Frozen} {
		got, ok := e.Status(name)
		if !ok || got.Name != name || got.Category != want {
			t.Fatalf("native status changed: %s %+v", name, got)
		}
		if e.ReadyEligible("issue", name) != (want == Active) {
			t.Fatalf("readiness did not use category for %s", name)
		}
		if e.SatisfiesBlocks(name) != (want == Done) {
			t.Fatalf("terminal behavior did not use category for %s", name)
		}
	}
	state := BeadState{Project: "native", Type: "issue", Status: "todo"}
	for _, slot := range []Slot{Claim, Close, Reopen, Defer, Reopen} {
		before := state
		var violations []Violation
		state, violations = Transition(e, before, slot, GuardFacts{})
		if len(violations) != 0 {
			t.Fatalf("%s: %v", slot, violations)
		}
		if got := Validate(e, &before, &state, "bead."+string(slot), GuardFacts{}); len(got) != 0 {
			t.Fatalf("%s: %v", slot, got)
		}
	}
	if state.Status != "todo" {
		t.Fatalf("reopen renamed native status: %s", state.Status)
	}
	unknown := state
	unknown.Status = "absent"
	if got := Validate(e, &state, &unknown, "bead.update", GuardFacts{}); len(got) == 0 {
		t.Fatal("unknown status accepted")
	}
}

func TestNativeCategoryIsProjectScopedAndInvalidCategoriesRefuse(t *testing.T) {
	base := nativeCategoryDocument()
	other := nativeCategoryDocument()
	other.Statuses["todo"] = Status{Name: "todo", Category: Frozen}
	other.Statuses["waiting"] = Status{Name: "waiting", Category: Active}
	w := other.Workflows["issue"]
	w.Initial, w.Slots[Reopen], w.Slots[Defer] = "waiting", "waiting", "todo"
	other.Workflows["issue"] = w
	snapshot, problems := Compile(base, map[string]Document{"other": other}, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if !snapshot.Effective("native").ReadyEligible("issue", "todo") || snapshot.Effective("other").ReadyEligible("issue", "todo") {
		t.Fatal("project category leaked")
	}
	for _, category := range []Category{"", "unknown", "ACTIVE"} {
		doc := nativeCategoryDocument()
		doc.Statuses["todo"] = Status{Name: "todo", Category: category}
		if _, problems := Compile(doc, nil, nil); len(problems) == 0 {
			t.Fatalf("unknown category accepted: %q", category)
		}
	}
	doc := nativeCategoryDocument()
	doc.Statuses["finished"] = Status{Name: "finished", Category: Done, Outcome: Failure}
	if _, problems := Compile(doc, nil, nil); len(problems) == 0 {
		t.Fatal("native done accepted a fabricated failure outcome")
	}
}

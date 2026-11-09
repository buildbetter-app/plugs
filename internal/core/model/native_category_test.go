package model

import (
	"encoding/json"
	"testing"
)

func TestNativeCategoryConditionDecisionsRoundTripExactly(t *testing.T) {
	for _, category := range []string{"active", "wip", "done", "frozen"} {
		want := ConditionDecision{Status: "native_status", Category: category, Terminal: category == "done"}
		if want.Terminal {
			want.Outcome = "success"
		}
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got ConditionDecision
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("native decision changed: %+v != %+v", got, want)
		}
		roundtripComponent(t, "ConditionDecision", string(raw), &ConditionDecision{})
		if category == "done" {
			roundtripComponent(t, "ConditionUnmetDecision", string(raw), &ConditionDecision{})
		} else if err := checkComponent(t, "ConditionUnmetDecision", raw); err == nil {
			t.Fatalf("nonterminal native category fits unmet decision: %s", category)
		}
		bad := want
		bad.Terminal = !bad.Terminal
		if bad.Validate() == nil {
			t.Fatalf("inconsistent native decision accepted: %+v", bad)
		}
	}
	if err := (ConditionDecision{Status: "native_status", Category: "unknown"}).Validate(); err == nil {
		t.Fatal("unknown category accepted")
	}
}

func TestNativeCategoryDecisionSchemaRefusesContradictions(t *testing.T) {
	// Bypass the validating marshaler so the wire schema and decoder must both
	// reject these independently constructed contradictory decisions.
	type uncheckedDecision ConditionDecision
	for name, decision := range map[string]ConditionDecision{
		"unknown":             {Status: "todo", Category: "future-category"},
		"active terminal":     {Status: "todo", Category: "active", Terminal: true},
		"wip outcome":         {Status: "working", Category: "wip", Outcome: "success"},
		"frozen satisfied":    {Status: "waiting", Category: "frozen", Satisfied: true},
		"done nonterminal":    {Status: "finished", Category: "done", Outcome: "success"},
		"done missing result": {Status: "finished", Category: "done", Terminal: true},
		"done failure":        {Status: "finished", Category: "done", Outcome: "failure", Terminal: true, Satisfied: true},
		"done satisfied":      {Status: "finished", Category: "done", Outcome: "success", Terminal: true, Satisfied: true},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(uncheckedDecision(decision))
			if err != nil {
				t.Fatal(err)
			}
			rejectComponent(t, "ConditionDecision", string(raw), &ConditionDecision{})
			rejectComponent(t, "ConditionUnmetDecision", string(raw), &ConditionDecision{})
		})
	}
}

func TestNativeCategoryAuditNoticeConformsToSchema(t *testing.T) {
	changeset := auditFixture()
	details := changeset.Events[1].Details
	details.Before = ConditionDecision{Status: "native_working", Category: "wip"}
	details.After = ConditionDecision{Status: "native_finished", Category: "done", Outcome: "success", Terminal: true}
	raw, err := json.Marshal(changeset)
	if err != nil {
		t.Fatal(err)
	}
	roundtripComponent(t, "Changeset", string(raw), &Changeset{})
	raw, err = json.Marshal(changeset.Events[1])
	if err != nil {
		t.Fatal(err)
	}
	roundtripComponent(t, "ConditionUnmetDescriptor", string(raw), &EventDescriptor{})
	raw, err = json.Marshal(details)
	if err != nil {
		t.Fatal(err)
	}
	roundtripComponent(t, "ConditionUnmetDetails", string(raw), &ConditionUnmetDetails{})
}

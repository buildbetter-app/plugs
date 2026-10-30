package ontology

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestEachFixedGuardRequiresItsObservation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		guard   Guard
		state   BeadState
		yes, no GuardFacts
	}{
		{"field", Guard{Kind: FieldsRequired, Fields: []string{"description"}}, BeadState{}, GuardFacts{}, GuardFacts{}},
		{"evidence", Guard{Kind: EvidenceRequired, Kinds: []string{"link"}}, BeadState{}, GuardFacts{Evidence: map[string]bool{"link": true}}, GuardFacts{Evidence: map[string]bool{"commit": true}}},
		{"children", Guard{Kind: NoOpenChildren}, BeadState{}, GuardFacts{NoOpenChildren: boolFact(true)}, GuardFacts{NoOpenChildren: boolFact(false)}},
		{"blockers", Guard{Kind: NoOpenBlockers}, BeadState{}, GuardFacts{NoOpenBlockers: boolFact(true)}, GuardFacts{NoOpenBlockers: boolFact(false)}},
		{"template", Guard{Kind: NotTemplate}, BeadState{}, GuardFacts{}, GuardFacts{}},
		{"gates", Guard{Kind: GatesResolved, Kinds: []string{"human_approval"}}, BeadState{}, GuardFacts{Gates: map[string]bool{"human_approval": true}}, GuardFacts{Gates: map[string]bool{"different": true}}},
		{"permission", Guard{Kind: Permission}, BeadState{}, GuardFacts{AuthzInstalled: true, Permission: boolFact(true)}, GuardFacts{AuthzInstalled: true, Permission: boolFact(false)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := DefaultDocument()
			w := doc.Workflows["issue"]
			w.Transitions = []Rule{{To: w.Slots[Close], Guards: []Guard{tc.guard}}}
			doc.Workflows["issue"] = w
			e := compileForTest(t, doc, nil)
			before, _ := Defaults(e, BeadState{Type: "issue"})
			before.Fields["description"] = "evidence text"
			if _, violations := Transition(e, before, Close, tc.yes); len(violations) != 0 {
				t.Fatal(violations)
			}
			if tc.guard.Kind == FieldsRequired {
				delete(before.Fields, "description")
			}
			if tc.guard.Kind == NotTemplate {
				before.Fields["template"] = true
			}
			_, violations := Transition(e, before, Close, tc.no)
			if len(violations) != 1 || violations[0].Code != "ontology_guard_failed" || violations[0].Guard != tc.guard.Kind {
				t.Fatalf("guard did not fail: %+v", violations)
			}
			if tc.guard.Kind == NoOpenChildren || tc.guard.Kind == NoOpenBlockers || tc.guard.Kind == EvidenceRequired || tc.guard.Kind == GatesResolved || tc.guard.Kind == Permission {
				_, violations = Transition(e, before, Close, GuardFacts{AuthzInstalled: tc.guard.Kind == Permission})
				if len(violations) != 1 || violations[0].Guard != tc.guard.Kind {
					t.Fatalf("unobserved guard did not fail: %+v", violations)
				}
			}
		})
	}
}

func TestPermissionAllowsWithoutAuthorizationPlugin(t *testing.T) {
	doc := DefaultDocument()
	w := doc.Workflows["issue"]
	w.Transitions = []Rule{{To: w.Slots[Close], Guards: []Guard{{Kind: Permission}}}}
	doc.Workflows["issue"] = w
	e := compileForTest(t, doc, nil)
	before, _ := Defaults(e, BeadState{Type: "issue"})
	if _, violations := Transition(e, before, Close, GuardFacts{Permission: boolFact(false)}); len(violations) != 0 {
		t.Fatal(violations)
	}
}

func TestMatchingRulesAreConjunctive(t *testing.T) {
	doc := DefaultDocument()
	w := doc.Workflows["issue"]
	w.Transitions = []Rule{{To: w.Slots[Close]}, {From: []string{w.Initial}, To: w.Slots[Close], Guards: []Guard{{Kind: EvidenceRequired, Kinds: []string{"link"}}}}}
	doc.Workflows["issue"] = w
	e := compileForTest(t, doc, nil)
	before, _ := Defaults(e, BeadState{Type: "issue"})
	if _, violations := Transition(e, before, Close, GuardFacts{}); len(violations) != 1 || violations[0].Guard != EvidenceRequired {
		t.Fatalf("unguarded rule bypassed narrower guard: %+v", violations)
	}
	guards, ok := e.Guards("issue", before.Status, w.Slots[Close])
	if !ok || len(guards) != 1 {
		t.Fatal(guards)
	}
	guards[0].Kinds[0] = "changed copy"
	if _, violations := Transition(e, before, Close, GuardFacts{Evidence: map[string]bool{"link": true}}); len(violations) != 0 {
		t.Fatal("guard description aliased snapshot", violations)
	}
}

func TestOwnedFieldWritesAndDegradation(t *testing.T) {
	doc := DefaultDocument()
	doc.Types["issue"].Fields["severity"] = Field{Type: "enum", Values: []string{"low", "high"}, Owner: "x.triage/assign"}
	e := compileForTest(t, doc, PluginCatalog{"x.triage/assign": true})
	before, _ := Defaults(e, BeadState{Type: "issue", Fields: map[string]any{"severity": "low"}})
	after, _ := copyState(before)
	after.Fields["severity"] = "high"
	for _, facts := range []GuardFacts{{}, {SetBy: map[string]string{"fields.severity": "request"}}, {SetBy: map[string]string{"fields.severity": "x.other/assign"}}} {
		violations := Validate(e, &before, &after, "bead.update", facts)
		if len(violations) != 1 || violations[0].Code != "field_owned" {
			t.Fatal(violations)
		}
	}
	if violations := Validate(e, &before, &after, "bead.update", GuardFacts{SetBy: map[string]string{"fields.severity": "x.triage/assign"}}); len(violations) != 0 {
		t.Fatal(violations)
	}
	if violations := Validate(e, &before, &before, "bead.comment", GuardFacts{}); len(violations) != 0 {
		t.Fatal("unchanged owned field needs no new setter", violations)
	}
	delete(after.Fields, "severity")
	if violations := Validate(e, &before, &after, "bead.update", GuardFacts{}); len(violations) != 1 || violations[0].Code != "field_owned" {
		t.Fatal("owned field removed", violations)
	}
	degraded := compileForTest(t, doc, nil)
	if _, ok := degraded.SlotStatus("issue", Close); !ok {
		t.Fatal("static target lookup must remain independent of guard availability")
	}
	if result := Check(degraded, &before, &before, nil, "bead.comment", GuardFacts{}); len(result.Violations) != 1 || result.Violations[0].Code != "subscriber_unavailable" {
		t.Fatal("degraded type did not fail closed", result)
	}
}

func TestFieldValueDomainsAreLosslessAndTyped(t *testing.T) {
	doc := DefaultDocument()
	fields := doc.Types["issue"].Fields
	fields["count"] = Field{Type: "integer", Required: true, Default: json.Number("9007199254740993")}
	fields["ratio"] = Field{Type: "number"}
	fields["enabled"] = Field{Type: "boolean"}
	fields["tags"] = Field{Type: "strings"}
	fields["severity"] = Field{Type: "enum", Values: []string{"low", "high"}}
	e := compileForTest(t, doc, nil)
	for _, tc := range []struct {
		name  string
		value any
		valid bool
	}{
		{"count", json.Number("9007199254740993"), true}, {"count", json.Number("1e1000000000"), true},
		{"count", json.Number("-2.00e1"), true}, {"count", json.Number("0e-1000000000"), true},
		{"count", json.Number("-1e-1000000000"), false}, {"count", 1.5, false}, {"count", "1", false},
		{"ratio", json.Number("0.125"), true}, {"ratio", json.Number("1e-1000000000"), true},
		{"enabled", false, true}, {"enabled", "false", false},
		{"tags", []string{"one", "two"}, true}, {"tags", []any{"one", 2}, false}, {"tags", []string(nil), false},
		{"severity", "high", true}, {"severity", "urgent", false},
	} {
		t.Run(tc.name+"/"+stringValue(tc.value), func(t *testing.T) {
			state, violations := Defaults(e, BeadState{Type: "issue"})
			if len(violations) != 0 {
				t.Fatal(violations)
			}
			state.Fields[tc.name] = tc.value
			violations = Validate(e, nil, &state, "bead.create", GuardFacts{})
			if (len(violations) == 0) != tc.valid {
				t.Fatalf("value=%v valid=%t violations=%+v", tc.value, tc.valid, violations)
			}
		})
	}
	state, _ := Defaults(e, BeadState{Type: "issue"})
	if state.Fields["count"] != json.Number("9007199254740993") {
		t.Fatalf("large default narrowed: %v", state.Fields["count"])
	}
	delete(state.Fields, "count")
	if violations := Validate(e, nil, &state, "bead.create", GuardFacts{}); len(violations) != 1 || violations[0].Field != "fields.count" {
		t.Fatal(violations)
	}
}

func stringValue(value any) string { data, _ := json.Marshal(value); return string(data) }

func TestLifecycleChecksTrustedMethodAndActualSlot(t *testing.T) {
	e := Default()
	before, _ := Defaults(e, BeadState{ID: "one", Project: "p", Type: "issue"})
	facts := GuardFacts{NoOpenChildren: boolFact(true), NoOpenBlockers: boolFact(true)}
	claimed, violations := Transition(e, before, Claim, facts)
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	for _, tc := range []struct {
		name          string
		before, after BeadState
		method        string
	}{
		{"wrong slot", before, claimed, "bead.close"},
		{"already claimed", claimed, claimed, "bead.claim"},
		{"not a lifecycle method", before, claimed, "bead.update"},
		{"direct transaction", before, claimed, "tx.apply"},
		{"different project", before, BeadState{ID: "one", Project: "q", Type: claimed.Type, Status: claimed.Status}, "bead.claim"},
		{"different ID", before, BeadState{ID: "two", Project: "p", Type: claimed.Type, Status: claimed.Status}, "bead.claim"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if violations := Validate(e, &tc.before, &tc.after, model.WriteMeta{Method: tc.method}.Method, facts); len(violations) == 0 {
				t.Fatal("illegal transition accepted")
			}
		})
	}
	closed, violations := Transition(e, claimed, Close, facts)
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	if target, ok := e.SlotStatus("issue", Close); !ok || target != closed.Status {
		t.Fatal("static target failed for replay state")
	}
	if _, violations := Transition(e, closed, Claim, facts); len(violations) == 0 {
		t.Fatal("claim from terminal accepted")
	}
	if violations := Validate(e, nil, &closed, "bead.create", facts); len(violations) != 1 || violations[0].Code != "ontology_use_slot_verb" {
		t.Fatal("create bypassed lifecycle", violations)
	}
	if _, violations := Transition(e, before, Slot("unknown"), facts); len(violations) == 0 {
		t.Fatal("unknown slot accepted")
	}
}

func TestUntouchedDriftWarnsAndChangedViolationsFail(t *testing.T) {
	e := Default()
	before, _ := Defaults(e, BeadState{Type: "issue", Fields: map[string]any{"obsolete": true, "title": "original"}})
	after, _ := copyState(before)
	after.Fields["title"] = "updated"
	result := Check(e, &before, &after, model.FieldSet{"fields.title"}, "bead.update", GuardFacts{})
	if len(result.Violations) != 0 || len(result.Warnings) != 1 || result.Warnings[0].Code != "ontology_drifted" {
		t.Fatal(result)
	}
	after.Fields["obsolete"] = false
	result = Check(e, &before, &after, nil, "bead.update", GuardFacts{})
	if len(result.Violations) != 1 || result.Violations[0].Code != "ontology_unknown_field" {
		t.Fatal("incomplete touched set hid actual change", result)
	}
	result = Check(e, &before, &before, model.FieldSet{"status"}, "tx.apply", GuardFacts{})
	if len(result.Violations) != 1 || result.Violations[0].Code != "ontology_use_slot_verb" {
		t.Fatal("same-value direct status patch accepted", result)
	}
	if violations := Validate(e, &before, &before, "bead.update", GuardFacts{Touched: model.FieldSet{"fields.status"}}); len(violations) != 1 || violations[0].Code != "ontology_use_slot_verb" {
		t.Fatal("same-value record field patch accepted", violations)
	}
	before.Status = "old-status"
	after, _ = copyState(before)
	after.Fields["brand_new"] = true
	result = Check(e, &before, &after, nil, "bead.update", GuardFacts{})
	if len(result.Violations) != 1 || result.Violations[0].Field != "fields.brand_new" {
		t.Fatal("status drift hid a new field violation", result)
	}
	initial, _ := Defaults(e, BeadState{Type: "issue"})
	initial.Fields["priority"] = 5
	changed, _ := copyState(initial)
	changed.Fields["brand_new"] = true
	result = Check(e, &initial, &changed, nil, "bead.update", GuardFacts{})
	if len(result.Violations) != 1 || result.Violations[0].Field != "fields.brand_new" || len(result.Warnings) != 1 {
		t.Fatal("priority drift hid a new field violation", result)
	}
}

func TestFixedKernelPriority(t *testing.T) {
	e := Default()
	input := BeadState{Type: "issue"}
	state, violations := Defaults(e, input)
	if len(violations) != 0 || state.Fields["priority"] != json.Number("2") || input.Fields != nil {
		t.Fatal("priority default did not preserve the input", state, violations)
	}
	if value, violations := Priority(input); value != 2 || len(violations) != 0 {
		t.Fatal("missing priority must read as P2", value, violations)
	}
	for _, tc := range []struct {
		value any
		want  int
		valid bool
	}{
		{0, 0, true}, {4, 4, true}, {float64(2), 2, true}, {json.Number("2.00"), 2, true},
		{json.Number("40e-1"), 4, true}, {json.Number("-0e-1000000000"), 0, true},
		{-1, 0, false}, {5, 0, false}, {2.5, 0, false}, {"2", 0, false}, {false, 0, false}, {nil, 0, false},
		{json.Number("9007199254740993"), 0, false}, {json.Number("1e1000000000"), 0, false},
		{json.Number("1e-1000000000"), 0, false},
	} {
		t.Run(stringValue(tc.value), func(t *testing.T) {
			proposal := BeadState{Type: "issue", Fields: map[string]any{"priority": tc.value}}
			value, violations := Priority(proposal)
			if (len(violations) == 0) != tc.valid || (tc.valid && value != tc.want) {
				t.Fatal(value, violations)
			}
			state, violations := Defaults(e, proposal)
			if len(violations) != 0 {
				t.Fatal(violations)
			}
			violations = Validate(e, nil, &state, "bead.create", GuardFacts{})
			if tc.valid && len(violations) != 0 {
				t.Fatal(violations)
			}
			if !tc.valid && (len(violations) != 1 || violations[0].Field != "fields.priority" || violations[0].Code != "invalid_params") {
				t.Fatal("invalid priority was defaulted or treated as a custom field", violations)
			}
		})
	}
}

func TestFixedKernelTemplate(t *testing.T) {
	e := Default()
	for _, tc := range []struct {
		name  string
		state BeadState
		want  bool
		valid bool
	}{
		{"absent", BeadState{}, false, true},
		{"raw false", BeadState{Fields: map[string]any{"template": false}}, false, true},
		{"raw true", BeadState{Fields: map[string]any{"template": true}}, true, true},
		{"string", BeadState{Fields: map[string]any{"template": "false"}}, false, false},
		{"number", BeadState{Fields: map[string]any{"template": 0}}, false, false},
		{"null", BeadState{Fields: map[string]any{"template": nil}}, false, false},
		{"trusted view", BeadState{Template: true}, true, true},
		{"contradiction", BeadState{Template: true, Fields: map[string]any{"template": false}}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flag, violations := IsTemplate(tc.state)
			if (len(violations) == 0) != tc.valid || (tc.valid && flag != tc.want) {
				t.Fatal(flag, violations)
			}
			tc.state.Type = "issue"
			state, violations := Defaults(e, tc.state)
			if len(violations) != 0 {
				t.Fatal(violations)
			}
			violations = Validate(e, nil, &state, "bead.create", GuardFacts{})
			if (len(violations) == 0) != tc.valid || (!tc.valid && violations[0].Field != "fields.template") {
				t.Fatal("raw template value was discarded", violations)
			}
			_, violations = Transition(e, state, Claim, GuardFacts{NoOpenBlockers: boolFact(true)})
			if (len(violations) == 0) != (tc.valid && !tc.want) {
				t.Fatal("not_template did not use the raw flag", violations)
			}
		})
	}
}

func TestOwnedJSONValuesSurviveSlotLowering(t *testing.T) {
	doc := DefaultDocument()
	doc.Types["issue"].Fields["count"] = Field{Type: "integer", Owner: "x.triage/assign"}
	doc.Types["issue"].Fields["tags"] = Field{Type: "strings", Owner: "x.triage/assign"}
	e := compileForTest(t, doc, PluginCatalog{"x.triage/assign": true})
	before := BeadState{Type: "issue", Status: "todo", Fields: map[string]any{"count": 2, "tags": []string{"tag"}}}
	if _, violations := Transition(e, before, Claim, GuardFacts{NoOpenBlockers: boolFact(true)}); len(violations) != 0 {
		t.Fatal("JSON copying falsely charged an unchanged owned field", violations)
	}
}

func TestCreationRequiresInitialStatus(t *testing.T) {
	e := Default()
	for _, status := range []string{"in_progress", "done", "failed", "canceled", "backlog"} {
		for _, method := range []string{"bead.create", "tx.apply"} {
			state, violations := Defaults(e, BeadState{Type: "issue", Status: status})
			if len(violations) != 0 {
				t.Fatal(violations)
			}
			violations = Validate(e, nil, &state, method, GuardFacts{})
			if len(violations) != 1 || violations[0].Code != "ontology_use_slot_verb" {
				t.Fatal("create entered a slot without a lifecycle verb", status, method, violations)
			}
		}
	}
}

func TestCrossProjectBlockerSemantics(t *testing.T) {
	e := Default()
	for _, tc := range []struct {
		key                 string
		blocks, conditional bool
	}{
		{"todo", false, false}, {"in_progress", false, false}, {"backlog", false, false},
		{"done", true, false}, {"failed", true, true}, {"canceled", true, false}, {"unknown", false, false},
	} {
		if e.SatisfiesBlocks(tc.key) != tc.blocks || e.SatisfiesConditionalBlocks(tc.key) != tc.conditional {
			t.Fatal(tc)
		}
	}
	base := DefaultDocument()
	changed := base.Statuses["failed"]
	changed.Outcome = Success
	s, problems := Compile(base, map[string]Document{"other": {Statuses: map[string]Status{"failed": changed}, Workflows: map[string]Workflow{"issue": func() Workflow { w := DefaultDocument().Workflows["issue"]; delete(w.Slots, Fail); return w }()}}}, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if !s.Effective("base").SatisfiesConditionalBlocks("failed") || s.Effective("other").SatisfiesConditionalBlocks("failed") {
		t.Fatal("blocker was not judged by its own ontology")
	}
	if !reflect.DeepEqual(Default().Document(), Default().Document()) {
		t.Fatal("default is not deterministic")
	}
}

func TestPerTypeWorkflowDoesNotBorrowAnotherTypesStatuses(t *testing.T) {
	doc := DefaultDocument()
	doc.Statuses["new"] = Status{Name: "New", Category: Unstarted}
	doc.Statuses["reviewing"] = Status{Name: "Reviewing", Category: Started}
	doc.Statuses["accepted"] = Status{Name: "Accepted", Category: Completed, Outcome: Success}
	doc.Workflows["review"] = Workflow{Statuses: []string{"new", "reviewing", "accepted"}, Initial: "new", Slots: map[Slot]string{Claim: "reviewing", Close: "accepted", Reopen: "new"}, Transitions: []Rule{{To: "reviewing"}, {To: "accepted"}, {To: "new"}}}
	doc.Types["bug"] = Type{Name: "Bug", Workflow: "review", Fields: map[string]Field{"severity": {Type: "enum", Values: []string{"low", "high"}, Required: true}}}
	e := compileForTest(t, doc, nil)
	bug, violations := Defaults(e, BeadState{Type: "bug", Fields: map[string]any{"severity": "high"}})
	if len(violations) != 0 || !e.ReadyEligible("bug", bug.Status) || e.ReadyEligible("issue", bug.Status) {
		t.Fatal(bug, violations)
	}
	claimed, violations := Transition(e, bug, Claim, GuardFacts{})
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	closed, violations := Transition(e, claimed, Close, GuardFacts{})
	if len(violations) != 0 || closed.Status != "accepted" {
		t.Fatal(closed, violations)
	}
	issueSlot, _ := e.SlotStatus("issue", Close)
	bugSlot, _ := e.SlotStatus("bug", Close)
	if issueSlot == bugSlot {
		t.Fatal("types borrowed the same slot target")
	}
	bug.Status = "todo"
	if violations := Validate(e, nil, &bug, "bead.create", GuardFacts{}); len(violations) == 0 {
		t.Fatal("cross-type status was accepted")
	}
}

func TestCloseSettlementKeepsSchemaGuardsAndStrictOrdinaryTransitions(t *testing.T) {
	e := Default()
	target, _ := e.SlotStatus("issue", Close)
	before := BeadState{ID: "pl-settled", Project: "local", Type: "issue", Status: target, Fields: map[string]any{"title": "Closed"}}
	observed := true
	facts := GuardFacts{NoOpenChildren: &observed, NoOpenBlockers: &observed}
	if got := ValidateCloseSettlement(e, &before, &before, facts); len(got) != 0 {
		t.Fatal("valid close settlement refused", got)
	}
	if got := Validate(e, &before, &before, "bead.close", facts); len(got) == 0 {
		t.Fatal("ordinary same-slot transition was weakened")
	}
	for _, name := range []string{"missing-observation", "blocker", "template", "schema", "status-touch", "different-workflow", "different-status", "not-closed"} {
		t.Run(name, func(t *testing.T) {
			after, _ := copyState(before)
			prior, _ := copyState(before)
			f := facts
			code := "ontology_guard_failed"
			switch name {
			case "missing-observation":
				f.NoOpenChildren = nil
			case "blocker":
				blocked := false
				f.NoOpenBlockers = &blocked
			case "template":
				after.Fields["template"] = true
			case "schema":
				after.Fields["title"] = false
				code = "invalid_params"
			case "status-touch":
				f.Touched = []string{"fields.status"}
				code = "ontology_use_slot_verb"
			case "different-workflow":
				after.Project = "other"
				code = "invalid_params"
			case "different-status":
				after.Status, _ = e.InitialStatus("issue")
				code = "invalid_params"
			case "not-closed":
				prior.Status, _ = e.InitialStatus("issue")
				code = "invalid_params"
			}
			got := ValidateCloseSettlement(e, &prior, &after, f)
			if len(got) == 0 || string(got[0].Code) != code {
				t.Fatalf("wanted %s, got %+v", code, got)
			}
		})
	}
	doc := DefaultDocument()
	doc.Types["issue"].Fields["severity"] = Field{Type: "string", Owner: "triage"}
	owned := compileForTest(t, doc, PluginCatalog{"triage": true})
	after, _ := copyState(before)
	after.Fields["severity"] = "high"
	if got := ValidateCloseSettlement(owned, &before, &after, facts); len(got) == 0 || got[0].Code != "field_owned" {
		t.Fatal("settlement bypassed field owner", got)
	}
	facts.SetBy = map[string]string{"fields.severity": "triage"}
	if got := ValidateCloseSettlement(owned, &before, &after, facts); len(got) != 0 {
		t.Fatal("valid owned field update refused", got)
	}
}

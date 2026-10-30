package ontology

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestSchemaMatchesTypedFieldsAndTargetRequirements(t *testing.T) {
	doc := DefaultDocument()
	fields := doc.Types["issue"].Fields
	fields["title"] = Field{Type: "string", Required: true}
	fields["count"] = Field{Type: "integer", Default: json.Number("9007199254740993")}
	fields["severity"] = Field{Type: "enum", Values: []string{"low", "high"}, Owner: "x.triage/assign"}
	fields["tags"] = Field{Type: "strings", Default: []string{"base"}}
	w := doc.Workflows["issue"]
	w.Transitions = append(w.Transitions, Rule{To: w.Slots[Close], Guards: []Guard{{Kind: FieldsRequired, Fields: []string{"description"}}}})
	doc.Workflows["issue"] = w
	e := compileForTest(t, doc, PluginCatalog{"x.triage/assign": true})
	schema, violations := e.Schema("issue", w.Slots[Close])
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	properties := schema["properties"].(map[string]any)
	status := properties["status"].(map[string]any)
	if !reflect.DeepEqual(status["enum"], []any{w.Slots[Close]}) {
		t.Fatal(status)
	}
	fieldSchema := properties["fields"].(map[string]any)
	if fieldSchema["additionalProperties"] != false || !reflect.DeepEqual(fieldSchema["required"], []any{"description", "title"}) {
		t.Fatal(fieldSchema)
	}
	definitions := fieldSchema["properties"].(map[string]any)
	if !reflect.DeepEqual(definitions["priority"], map[string]any{"type": "integer", "minimum": json.Number("0"), "maximum": json.Number("4"), "default": json.Number("2")}) {
		t.Fatal("schema omitted the fixed kernel priority", definitions["priority"])
	}
	if !reflect.DeepEqual(definitions["template"], map[string]any{"type": "boolean", "default": false}) {
		t.Fatal("schema omitted the fixed kernel template flag", definitions["template"])
	}
	if definitions["severity"].(map[string]any)["readOnly"] != true || definitions["count"].(map[string]any)["default"] != json.Number("9007199254740993") {
		t.Fatal(definitions)
	}
	definitions["tags"].(map[string]any)["default"].([]any)[0] = "mutated schema"
	state, _ := Defaults(e, BeadState{Type: "issue"})
	if !reflect.DeepEqual(state.Fields["tags"], []any{"base"}) {
		t.Fatal("schema aliases compiled defaults")
	}
	if _, violations = e.Schema("unknown", ""); len(violations) == 0 {
		t.Fatal("unknown type schema accepted")
	}
	if _, violations = e.Schema("issue", "unknown"); len(violations) == 0 {
		t.Fatal("unknown status schema accepted")
	}
	initial, violations := e.Schema("issue", "")
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	if initial["properties"].(map[string]any)["status"].(map[string]any)["default"] != w.Initial {
		t.Fatal(initial)
	}
}

func TestCompilationPlanDigestStabilityAndBinding(t *testing.T) {
	base := DefaultDocument()
	left := map[string]Document{"b": {Types: map[string]Type{"issue": {Name: "B", Workflow: "issue"}}}, "a": {}}
	right := map[string]Document{"a": {}, "b": left["b"]}
	first, problems := Compile(base, left, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	second, problems := Compile(base, right, PluginCatalog{})
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	a, err := PlanCompilation(model.Revision("1.0e0"), first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PlanCompilation(model.Natural64(1), second)
	if err != nil {
		t.Fatal(err)
	}
	if a.Digest != b.Digest || first.Digest() != second.Digest() || a.Expect.String() != "1" {
		t.Fatalf("unstable canonical plan: %+v %+v", a, b)
	}
	newRevision, err := PlanCompilation(model.Natural64(2), first)
	if err != nil || a.Digest == newRevision.Digest {
		t.Fatal("plan does not bind expected revision")
	}
	base.Types["issue"].Fields["title"] = Field{Type: "string", Default: "different"}
	changed, problems := Compile(base, left, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	newDocument, err := PlanCompilation(model.Natural64(1), changed)
	if err != nil || newDocument.Digest == a.Digest {
		t.Fatal("plan does not bind candidate document")
	}
	if _, err := PlanCompilation(model.Revision("-1"), first); err == nil {
		t.Fatal("negative expected revision accepted")
	}
	if _, err := PlanCompilation(model.Natural64(1), Snapshot{}); err == nil {
		t.Fatal("invalid compilation can produce plan")
	}
	large, err := PlanCompilation(model.Revision("9007199254740993"), first)
	if err != nil || large.Expect.String() != "9007199254740993" {
		t.Fatal("expected revision narrowed", large, err)
	}
}

func TestOptionalSlotsUseCategoryAndOutcome(t *testing.T) {
	e := Default()
	initial, _ := Defaults(e, BeadState{Type: "issue"})
	facts := GuardFacts{NoOpenBlockers: boolFact(true), NoOpenChildren: boolFact(true)}
	for _, tc := range []struct {
		slot     Slot
		category Category
		outcome  Outcome
	}{
		{Fail, Completed, Failure}, {Cancel, Canceled, ""}, {Defer, Backlog, ""},
	} {
		state, violations := Transition(e, initial, tc.slot, facts)
		if len(violations) != 0 {
			t.Fatal(tc.slot, violations)
		}
		status, _ := e.Status(state.Status)
		if status.Category != tc.category || status.Outcome != tc.outcome {
			t.Fatal(tc, status)
		}
		reopened, violations := Transition(e, state, Reopen, facts)
		if len(violations) != 0 || !e.ReadyEligible(reopened.Type, reopened.Status) {
			t.Fatal("reopen did not enter unstarted slot", violations)
		}
	}
}

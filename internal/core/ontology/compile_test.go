package ontology

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func compileForTest(t *testing.T, doc Document, plugins PluginCatalog) Effective {
	t.Helper()
	snapshot, problems := Compile(doc, nil, plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	return snapshot.Effective("")
}

func TestCompileRejectsInvalidRules(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Document)
	}{
		{"category", func(d *Document) { s := d.Statuses["todo"]; s.Category = "unknown-category"; d.Statuses["todo"] = s }},
		{"outcome on nonterminal", func(d *Document) { s := d.Statuses["todo"]; s.Outcome = Success; d.Statuses["todo"] = s }},
		{"unknown outcome", func(d *Document) { s := d.Statuses["done"]; s.Outcome = "unknown"; d.Statuses["done"] = s }},
		{"wrong close outcome", func(d *Document) { d.Workflows["issue"].Slots[Close] = "failed" }},
		{"unknown slot", func(d *Document) { d.Workflows["issue"].Slots[Slot("finish")] = "done" }},
		{"missing claim", func(d *Document) { delete(d.Workflows["issue"].Slots, Claim) }},
		{"missing close", func(d *Document) { delete(d.Workflows["issue"].Slots, Close) }},
		{"foreign initial", func(d *Document) { w := d.Workflows["issue"]; w.Initial = "elsewhere"; d.Workflows["issue"] = w }},
		{"unknown transition target", func(d *Document) { d.Workflows["issue"].Transitions[0].To = "elsewhere" }},
		{"unknown transition source", func(d *Document) { d.Workflows["issue"].Transitions[0].From = []string{"elsewhere"} }},
		{"unknown guard", func(d *Document) { d.Workflows["issue"].Transitions[0].Guards = []Guard{{Kind: "expression"}} }},
		{"guard parameters", func(d *Document) {
			d.Workflows["issue"].Transitions[0].Guards = []Guard{{Kind: Permission, Fields: []string{"title"}}}
		}},
		{"missing guard kinds", func(d *Document) { d.Workflows["issue"].Transitions[0].Guards = []Guard{{Kind: EvidenceRequired}} }},
		{"unknown guard field", func(d *Document) {
			d.Workflows["issue"].Transitions[0].Guards = []Guard{{Kind: FieldsRequired, Fields: []string{"unknown"}}}
		}},
		{"unknown field type", func(d *Document) { d.Types["issue"].Fields["custom"] = Field{Type: "expression"} }},
		{"invalid default", func(d *Document) { d.Types["issue"].Fields["count"] = Field{Type: "integer", Default: 0.5} }},
		{"invalid JSON default", func(d *Document) { d.Types["issue"].Fields["count"] = Field{Type: "number", Default: math.NaN()} }},
		{"enum duplicate", func(d *Document) {
			d.Types["issue"].Fields["severity"] = Field{Type: "enum", Values: []string{"high", "high"}}
		}},
		{"fixed priority", func(d *Document) { d.Types["issue"].Fields["priority"] = Field{Type: "string"} }},
		{"fixed template", func(d *Document) { d.Types["issue"].Fields["template"] = Field{Type: "boolean"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := DefaultDocument()
			tc.edit(&doc)
			snapshot, problems := Compile(doc, nil, nil)
			if len(problems) == 0 || snapshot.Digest() != "" {
				t.Fatalf("invalid compilation was usable: %+v", problems)
			}
		})
	}
}

func TestDecodeRejectsUnknownAndAmbiguousConfiguration(t *testing.T) {
	for _, input := range []string{
		`null`, `[]`, `{}`, // the empty object decodes as an overlay, tested below
		`{"priorities":[0,1]}`, `{"dependency_types":{}}`, `{"templates":{}}`, `{"roles":{}}`,
		`{"Statuses":{}}`, `{"statuses":{},"statuses":{}}`,
		`{"statuses":{"a":{"name":"A","category":"unstarted","category":"completed"}}}`,
		`{"statuses":{"a":{"name":null,"category":"unstarted"}}}`,
		`{"types":{"issue":{"name":"Issue","workflow":"issue","permissions":{}}}}`,
		`{"workflows":{"issue":{"slots":{"close":"done"},"transitions":[{"to":"done","guards":[{"kind":"permission","condition":true}]}]}}}`,
		`{"statuses":null}`, `{"statuses":{}} {}`, `{"statuses":{"a":null}}`,
		`{"types":{"issue":{"workflow":"issue","fields":{"x":{"type":"string","default":null}}}}}`,
	} {
		_, err := Decode([]byte(input))
		if input == `{}` {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err == nil {
			t.Errorf("accepted %s", input)
		}
	}
	data, err := json.Marshal(DefaultDocument())
	if err != nil {
		t.Fatal(err)
	}
	doc, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	compileForTest(t, doc, nil)
}

func TestOverlayIsolationArchiveAndNoKeyReuse(t *testing.T) {
	base := DefaultDocument()
	base.Statuses["retired"] = Status{Name: "Retired", Category: Triage}
	base.Types["retired"] = Type{Name: "Old", Workflow: "issue", Archived: true}
	overlays := map[string]Document{
		"a": {Statuses: map[string]Status{"retired": {Archived: true}}},
		"b": {Types: map[string]Type{"issue": {Name: "B", Workflow: "issue"}}},
	}
	s, problems := Compile(base, overlays, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if status, _ := s.Effective("a").Status("retired"); !status.Archived || status.Category != Triage {
		t.Fatalf("archive changed definition: %+v", status)
	}
	if status, _ := s.Effective("b").Status("retired"); status.Archived {
		t.Fatal("archive leaked between projects")
	}
	if status, _ := s.Effective("unknown").Status("retired"); status.Archived {
		t.Fatal("archive leaked into base")
	}
	if len(s.Effective("b").document.Types["issue"].Fields) != 0 {
		t.Fatal("overlay kept base fields")
	}
	for _, overlay := range []Document{
		{Statuses: map[string]Status{"new": {Archived: true}}},
		{Types: map[string]Type{"retired": {Name: "Reused", Workflow: "issue"}}},
		{Statuses: map[string]Status{"todo": {Archived: true}}}, // active slot still targets it
		{Types: map[string]Type{"issue": {Name: "partial replacement"}}},
	} {
		if _, problems := Compile(base, map[string]Document{"a": overlay}, nil); len(problems) == 0 {
			t.Fatalf("accepted invalid overlay: %+v", overlay)
		}
	}
}

func TestCompiledSnapshotDoesNotAliasInputsOrDescriptions(t *testing.T) {
	doc := DefaultDocument()
	doc.Types["issue"].Fields["tags"] = Field{Type: "strings", Default: []string{"original"}}
	s, problems := Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	digest := s.Digest()
	delete(doc.Statuses, "todo")
	doc.Types["issue"].Fields["tags"].Default.([]string)[0] = "changed"
	description := s.Effective("").Document()
	description.Types["issue"].Fields["tags"].Default.([]any)[0] = "changed again"
	state, violations := Defaults(s.Effective(""), BeadState{Type: "issue"})
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	if !reflect.DeepEqual(state.Fields["tags"], []any{"original"}) || s.Digest() != digest {
		t.Fatalf("snapshot mutated: %+v", state)
	}
	state.Fields["tags"].([]any)[0] = "changed default"
	state, _ = Defaults(s.Effective(""), BeadState{Type: "issue"})
	if !reflect.DeepEqual(state.Fields["tags"], []any{"original"}) {
		t.Fatal("default output aliases snapshot")
	}
}

func TestNonReadyTypeDoesNotRequireClaim(t *testing.T) {
	doc := DefaultDocument()
	typ := doc.Types["issue"]
	typ.ReadyEligible = boolFact(false)
	doc.Types["issue"] = typ
	delete(doc.Workflows["issue"].Slots, Claim)
	w := doc.Workflows["issue"]
	w.Initial = ""
	doc.Workflows["issue"] = w
	e := compileForTest(t, doc, nil)
	initial, ok := e.InitialStatus("issue")
	if !ok || e.ReadyEligible("issue", initial) {
		t.Fatal("non-ready type became ready")
	}
	if _, ok := e.SlotStatus("issue", Claim); ok {
		t.Fatal("absent claim slot resolved")
	}
}

func TestCompilationProblemsAreDeterministicallyOrdered(t *testing.T) {
	base := DefaultDocument()
	first := map[string]Document{"z": {Types: map[string]Type{"bad": {Workflow: "missing"}}}, "a": {Statuses: map[string]Status{"new": {Archived: true}}}}
	second := map[string]Document{"a": first["a"], "z": first["z"]}
	_, a := Compile(base, first, nil)
	_, b := Compile(base, second, nil)
	if len(a) == 0 || !reflect.DeepEqual(a, b) {
		t.Fatalf("nondeterministic problems: %+v %+v", a, b)
	}
	if !strings.HasPrefix(a[0].Path, "projects.a") {
		t.Fatal(a)
	}
}

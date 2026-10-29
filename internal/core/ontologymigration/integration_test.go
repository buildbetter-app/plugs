package ontologymigration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

func TestProjectMappingsAndWholeKeyOverlayPreserveFields(t *testing.T) {
	current, candidate := ontology.DefaultDocument(), ontology.DefaultDocument()
	for _, doc := range []ontology.Document{current, candidate} {
		w := doc.Workflows["issue"]
		w.Transitions = nil
		doc.Workflows["issue"] = w
	}
	current.Types["legacy"] = ontology.Type{Name: "Legacy", Workflow: "issue", Fields: map[string]ontology.Field{
		"count": {Type: "string"}, "title": {Type: "string"}, "obsolete": {Type: "string"},
	}}
	candidate.Types["legacy"] = ontology.Type{Archived: true}
	candidate.Types["issue"] = ontology.Type{Name: "Issue", Workflow: "issue", Fields: map[string]ontology.Field{"count": {Type: "string"}, "title": {Type: "string"}}}
	candidate.Statuses["queued"] = ontology.Status{Name: "Queued", Category: ontology.Unstarted}
	w := candidate.Workflows["issue"]
	w.Statuses = []string{"queued", "in_progress", "done", "failed", "canceled", "backlog"}
	w.Initial = "queued"
	w.Slots = map[ontology.Slot]string{ontology.Claim: "in_progress", ontology.Close: "done", ontology.Reopen: "queued"}
	candidate.Workflows["project"] = w
	overlay := ontology.Document{Types: map[string]ontology.Type{"issue": {Name: "Project issue", Workflow: "project", Fields: map[string]ontology.Field{"count": {Type: "integer", Required: true}}}}}
	req := Request{Current: Version{Revision: "1", Snapshot: compiled(t, current, nil, nil)}, Candidate: Version{Revision: "2", Snapshot: compiled(t, candidate, map[string]ontology.Document{"P": overlay}, nil)}}
	pRow, qRow := bead("P-1", "P", map[string]any{"count": "9007199254740993", "title": "retain unless explicit", "obsolete": "old"}), bead("Q-1", "Q", map[string]any{"count": "17", "title": "kept", "obsolete": "old"})
	pRow.State.Type, qRow.State.Type = "legacy", "legacy"
	source := rowsFor(t, req, qRow, pRow)
	req.Directives = []Directive{
		{ID: "p-type", Kind: MapType, Target: Target{Project: "P", Type: "legacy"}, From: "legacy", To: "issue"},
		{ID: "q-type", Kind: MapType, Target: Target{Project: "Q", Type: "legacy"}, From: "legacy", To: "issue"},
		{ID: "p-count", Kind: ConvertField, Target: Target{Project: "P", Type: "legacy"}, Field: "count", Conversion: "string_to_integer"},
		{ID: "p-drop-old", Kind: DropField, Target: Target{Project: "P", Type: "legacy"}, Field: "obsolete"},
		{ID: "q-drop-old", Kind: DropField, Target: Target{Project: "Q", Type: "legacy"}, Field: "obsolete"},
	}
	p := build(t, req, source, noPre())
	if p.Ready() || len(p.Report().Rows[0].Findings) != 2 || p.Report().Rows[0].After.State.Fields["title"] != "retain unless explicit" {
		t.Fatalf("missing map and whole-key field removal must refuse without data loss: %+v", p.Report())
	}
	req.Directives = append(req.Directives,
		Directive{ID: "p-status", Kind: MapStatus, Target: Target{Project: "P", Type: "legacy"}, From: "todo", To: "queued"},
		Directive{ID: "q-status", Kind: MapStatus, Target: Target{Project: "Q", Type: "legacy"}, From: "todo", To: "backlog"},
		Directive{ID: "p-drop-title", Kind: DropField, Target: Target{Project: "P", Type: "legacy"}, Field: "title"})
	p = build(t, req, source, noPre())
	r := p.Report()
	if !p.Ready() || r.Affected != 2 || r.Rows[0].After.State.Status != "queued" || r.Rows[1].After.State.Status != "backlog" || r.Rows[1].After.State.Fields["count"] != "17" || r.Rows[1].After.State.Fields["title"] != "kept" {
		t.Fatalf("per-project mapping failed: %+v", r)
	}
	if r.Rows[0].After.State.Fields["count"] != json.Number("9007199254740993") || r.Rows[0].AfterStatus.Category != ontology.Unstarted || r.Rows[1].AfterStatus.Category != ontology.Backlog {
		t.Fatalf("lost exact number or global category semantics: %+v", r.Rows)
	}
	// Directives and enumeration order are not execution order.
	slices.Reverse(req.Directives)
	slices.Reverse(source.rows)
	if again := build(t, req, source, noPre()); again.Digest() != p.Digest() {
		t.Fatal("input ordering changed a deterministic executable plan")
	}
}

func TestOwnedFieldsRefuseAllFieldDirectivesAndOwnerRemoval(t *testing.T) {
	for _, kind := range []Kind{SetField, ConvertField, DropField} {
		t.Run(string(kind), func(t *testing.T) {
			current, candidate := ontology.DefaultDocument(), ontology.DefaultDocument()
			current.Types["issue"].Fields["owned"] = ontology.Field{Type: "string", Owner: "plugin/owner"}
			candidate.Types["issue"].Fields["owned"] = ontology.Field{Type: "integer", Default: 2, Required: true}
			req := Request{Current: Version{Revision: "1", Snapshot: compiled(t, current, nil, ontology.PluginCatalog{"plugin/owner": true})}, Candidate: Version{Revision: "2", Snapshot: compiled(t, candidate, nil, nil)}}
			d := Directive{ID: "owned", Kind: kind, Target: Target{Project: "P", Type: "issue"}, Field: "owned"}
			if kind == SetField {
				d.Value = 3
			}
			if kind == ConvertField {
				d.Conversion = "string_to_integer"
			}
			if kind == DropField {
				delete(candidate.Types["issue"].Fields, "owned")
				req.Candidate.Snapshot = compiled(t, candidate, nil, nil)
			}
			req.Directives = []Directive{d}
			p := build(t, req, rowsFor(t, req, bead("P-1", "P", map[string]any{"owned": "1"})), noPre())
			if p.Ready() || !hasRefusal(p, "field_owned") || p.Report().Rows[0].After.State.Fields["owned"] != "1" {
				t.Fatalf("owned directive permitted: %+v", p.Report())
			}
		})
	}
	// Candidate owners also prevent generic defaults from filling required data.
	current, candidate := ontology.DefaultDocument(), ontology.DefaultDocument()
	candidate.Types["issue"].Fields["owned"] = ontology.Field{Type: "string", Required: true, Default: "v", Owner: "plugin/owner"}
	req := request(t, current, candidate)
	req.Candidate.Snapshot = compiled(t, candidate, nil, ontology.PluginCatalog{"plugin/owner": true})
	p := build(t, req, rowsFor(t, req, bead("P-1", "P", nil)), noPre())
	if p.Ready() || p.Report().Rows[0].After.State.Fields["owned"] != nil || len(p.Report().Rows[0].Findings) != 1 {
		t.Fatalf("plugin-owned default was silently supplied: %+v", p.Report())
	}
}

func TestConversionsUseExactCanonicalFieldTypes(t *testing.T) {
	cases := []struct {
		from, to, name string
		value, want    any
		ready          bool
	}{
		{"string", "integer", "string_to_integer", "9007199254740993", json.Number("9007199254740993"), true},
		{"string", "integer", "string_to_integer", "-7", json.Number("-7"), true},
		{"string", "integer", "string_to_integer", "1.5", nil, false},
		{"string", "integer", "string_to_integer", "03", nil, false},
		{"string", "integer", "string_to_integer", " 3", nil, false},
		{"string", "number", "string_to_number", "1.25e30", json.Number("1.25e30"), true},
		{"integer", "string", "integer_to_string", json.Number("9007199254740993"), "9007199254740993", true},
		{"number", "string", "number_to_string", json.Number("1e1000000000"), "1e1000000000", true},
		{"string", "boolean", "string_to_boolean", "false", false, true},
		{"string", "boolean", "string_to_boolean", "yes", nil, false},
		{"boolean", "string", "boolean_to_string", true, "true", true},
		{"string", "integer", "coerce", "3", nil, false},
		{"strings", "string", "strings_to_string", []string{"a", "b"}, nil, false},
		{"string", "integer", "number_to_string", "3", nil, false},
	}
	for i, c := range cases {
		t.Run(fmt.Sprint(i)+"-"+c.name, func(t *testing.T) {
			current, candidate := ontology.DefaultDocument(), ontology.DefaultDocument()
			current.Types["issue"].Fields["f"] = ontology.Field{Type: c.from}
			candidate.Types["issue"].Fields["f"] = ontology.Field{Type: c.to}
			req := request(t, current, candidate)
			req.Directives = []Directive{{ID: "convert", Kind: ConvertField, Target: Target{Project: "P", Type: "issue"}, Field: "f", Conversion: c.name}}
			p := build(t, req, rowsFor(t, req, bead("P-1", "P", map[string]any{"f": c.value})), noPre())
			if p.Ready() != c.ready || (c.ready && !equal(p.Report().Rows[0].After.State.Fields["f"], c.want)) {
				t.Fatalf("conversion result ready=%v: %+v", p.Ready(), p.Report())
			}
		})
	}
}

func TestGrandfatherTracksExactViolationsAndCannotCoverFixedRules(t *testing.T) {
	current, candidate := ontology.DefaultDocument(), ontology.DefaultDocument()
	candidate.Types["issue"].Fields["required"] = ontology.Field{Type: "string", Required: true}
	req := request(t, current, candidate)
	source := rowsFor(t, req, bead("P-1", "P", nil), bead("P-2", "P", nil))
	p := build(t, req, source, noPre())
	for i, row := range p.Report().Rows {
		req.Directives = append(req.Directives, Directive{ID: fmt.Sprint(i), Kind: Grandfather, Target: Target{Project: "P", Type: "issue", ID: row.Before.State.ID}, Violation: row.Findings[0].ID, Reason: "retain existing data until its owner repairs it"})
	}
	p = build(t, req, source, noPre())
	if !p.Ready() || p.Report().Grandfathered != 2 || p.Report().Affected != 2 {
		t.Fatalf("grandfather accounting failed: %+v", p.Report())
	}
	// A disposition from a different row or candidate must never be transferable.
	req.Directives[0].Violation = req.Directives[1].Violation
	if p := build(t, req, source, noPre()); p.Ready() || !hasRefusal(p, "invalid_params") {
		t.Fatal("accepted a grandfather disposition for the wrong row")
	}
	req.Directives = nil
	source = rowsFor(t, req, bead("P-1", "P", nil))
	source.rows[0].State.Fields["priority"] = 5
	source.inventory.RowsDigest, _ = RowsDigest(source.rows)
	p = build(t, req, source, noPre())
	for i, f := range p.Report().Rows[0].Findings {
		req.Directives = append(req.Directives, Directive{ID: fmt.Sprint(i), Kind: Grandfather, Target: Target{Project: "P", Type: "issue", ID: "P-1"}, Violation: f.ID, Reason: "requested exception"})
	}
	if p = build(t, req, source, noPre()); p.Ready() || !hasRefusal(p, "invalid_params") {
		t.Fatal("grandfather allowed an invalid fixed priority")
	}
}

func TestPreCallbacksRunPerAffectedRowAndRejectionsRequireDisposition(t *testing.T) {
	current, candidate := ontology.DefaultDocument(), ontology.DefaultDocument()
	candidate.Types["issue"].Fields["new"] = ontology.Field{Type: "string", Default: "v"}
	req := request(t, current, candidate)
	source := rowsFor(t, req, bead("P-1", "P", nil), bead("P-2", "P", nil), bead("Q-1", "Q", nil))
	pre := &fixturePre{bindings: []Binding{
		{ID: "global", Type: "issue", Digest: strings.Repeat("a", 64)},
		{ID: "project", Project: "P", Type: "issue", Digest: strings.Repeat("b", 64)},
	}, run: func(_ context.Context, binding Binding, change Change) ([]ontology.Violation, error) {
		if change.After.State.Fields["new"] != "v" {
			t.Fatal("validating pre did not see the actual defaulted final image")
		}
		if binding.ID == "global" && change.After.State.ID == "P-1" {
			return []ontology.Violation{{Code: "ontology_guard_failed", Field: "fields.new", Message: "explicit review needed"}}, nil
		}
		return nil, nil
	}}
	p := build(t, req, source, pre)
	if p.Ready() || pre.calls != 5 || len(p.Report().Rows[0].PreRuns) != 2 || p.Report().Rows[0].Findings[0].Subscriber != "global" {
		t.Fatalf("pre callback/rejection was skipped: calls=%d %+v", pre.calls, p.Report())
	}
	req.Directives = []Directive{{ID: "retain", Kind: Grandfather, Target: Target{Project: "P", Type: "issue", ID: "P-1"}, Violation: p.Report().Rows[0].Findings[0].ID, Reason: "accepted migration exception"}}
	pre.calls = 0
	p = build(t, req, source, pre)
	if !p.Ready() || pre.calls != 5 || p.Report().Grandfathered != 1 || len(p.Report().Rows[0].PreRuns[0].Violations) != 1 {
		t.Fatalf("pre rejection evidence/disposition lost: %+v", p.Report())
	}
	ctx, cancel := context.WithCancel(context.Background())
	pre.run = func(context.Context, Binding, Change) ([]ontology.Violation, error) { cancel(); return nil, nil }
	p, err := Build(ctx, req, source, pre)
	if !errors.Is(err, context.Canceled) || p.Digest() != "" {
		t.Fatalf("cancellation inside a callback produced a plan: %v", err)
	}
	pre.run = func(context.Context, Binding, Change) ([]ontology.Violation, error) {
		return nil, errors.New("host unavailable")
	}
	if p, err := Build(context.Background(), req, source, pre); err == nil || p.Digest() != "" {
		t.Fatal("callback infrastructure failure must not be a grandfatherable rejection")
	}
}

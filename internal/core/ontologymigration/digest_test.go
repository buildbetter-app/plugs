package ontologymigration

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
)

func TestInputsReportsAndCallbackImagesAreDetached(t *testing.T) {
	current, candidate := ontology.DefaultDocument(), ontology.DefaultDocument()
	current.Types["issue"].Fields["tags"] = ontology.Field{Type: "strings"}
	candidate.Types["issue"].Fields["tags"] = ontology.Field{Type: "strings"}
	req := request(t, current, candidate)
	values := []any{"planned"}
	req.Directives = []Directive{{ID: "tags", Kind: SetField, Target: Target{Project: "P", Type: "issue"}, Field: "tags", Value: values}}
	source := rowsFor(t, req, bead("P-2", "P", map[string]any{"tags": []string{"old"}}), bead("P-1", "P", map[string]any{"tags": []string{"old"}}))
	pre := &fixturePre{bindings: []Binding{{ID: "validate", Type: "issue", Digest: strings.Repeat("a", 64)}}}
	var retained []Change
	pre.run = func(_ context.Context, _ Binding, change Change) ([]ontology.Violation, error) {
		retained = append(retained, change)
		change.After.State.Fields["tags"].([]any)[0] = "callback edit"
		change.Before.State.Fields["tags"].([]any)[0] = "callback edit"
		return nil, nil
	}
	p := build(t, req, source, pre)
	if !p.Ready() || pre.calls != 2 {
		t.Fatalf("plan did not execute both callbacks: %+v", p.Report())
	}
	original := p.Digest()
	r := p.Report()
	for _, row := range r.Rows {
		if row.Before.State.Fields["tags"].([]any)[0] != "old" || row.After.State.Fields["tags"].([]any)[0] != "planned" {
			t.Fatal("callback mutated retained plan images")
		}
	}
	values[0] = "caller changed directive"
	source.rows[0].State.Fields["tags"].([]string)[0] = "caller changed row"
	pre.bindings[0].Digest = strings.Repeat("b", 64)
	retained[0].After.State.Fields["tags"] = []any{"late callback edit"}
	r.Rows[0].After.State.Fields["tags"].([]any)[0] = "report edit"
	r.Directives[0].Value.([]any)[0] = "report directive edit"
	r.PreInventory.Bindings[0].Digest = strings.Repeat("c", 64)
	again := p.Report()
	if !p.Ready() || p.Digest() != original || again.Rows[0].After.State.Fields["tags"].([]any)[0] != "planned" || again.Rows[0].Before.State.Fields["tags"].([]any)[0] != "old" || again.Directives[0].Value.([]any)[0] != "planned" || again.PreInventory.Bindings[0].Digest != strings.Repeat("a", 64) {
		t.Fatal("external alias changed an already built plan")
	}
	again.PlanDigest = ""
	if digest(again) != original {
		t.Fatal("detached report cannot reproduce its content digest")
	}
}

func TestDigestBindsAllProvenanceAndActualDryRuns(t *testing.T) {
	baseline := func() (Request, *fixtureRows, *fixturePre) {
		req := request(t, ontology.DefaultDocument(), ontology.DefaultDocument())
		req.Directives = []Directive{{ID: "title", Kind: SetField, Target: Target{Project: "P", Type: "issue"}, Field: "title", Value: "new"}}
		return req, rowsFor(t, req, bead("P-1", "P", map[string]any{"title": "old"})), &fixturePre{bindings: []Binding{{ID: "b", Type: "issue", Digest: strings.Repeat("a", 64)}}}
	}
	req, source, pre := baseline()
	first := build(t, req, source, pre)
	if !first.Ready() {
		t.Fatal("baseline failed")
	}
	for _, change := range []string{"row image", "row revision", "storage revision", "ontology revision", "current digest", "candidate digest", "directive", "binding implementation", "actual callback rejection", "observation"} {
		t.Run(change, func(t *testing.T) {
			req, source, pre := baseline()
			switch change {
			case "row image":
				source.rows[0].State.Fields["description"] = "observed"
			case "row revision":
				source.rows[0].Revision = "13"
			case "storage revision":
				source.inventory.DataRevision = "51"
			case "ontology revision":
				req.Current.Revision, req.Candidate.Revision, source.inventory.Ontology.Revision = "8", "9", "8"
			case "current digest":
				doc := ontology.DefaultDocument()
				doc.Types["issue"].Fields["old_optional"] = ontology.Field{Type: "string"}
				req.Current.Snapshot = compiled(t, doc, nil, nil)
				source.inventory.Ontology.Digest = req.Current.Snapshot.Digest()
			case "candidate digest":
				doc := ontology.DefaultDocument()
				doc.Types["issue"].Fields["new_optional"] = ontology.Field{Type: "string"}
				req.Candidate.Snapshot = compiled(t, doc, nil, nil)
			case "directive":
				req.Directives[0].Value = "different"
			case "binding implementation":
				pre.bindings[0].Digest = strings.Repeat("b", 64)
			case "actual callback rejection":
				pre.run = func(context.Context, Binding, Change) ([]ontology.Violation, error) {
					return []ontology.Violation{{Code: "ontology_guard_failed", Message: "rejected by real callback"}}, nil
				}
			case "observation":
				source.observe = func(_ context.Context, _ Inventory, c Change) (Observation, error) {
					d, _ := RowDigest(c.After)
					allowed := true
					return Observation{Candidate: c.Candidate, RowDigest: d, AuthzInstalled: &allowed, Facts: ontology.GuardFacts{AuthzInstalled: true, Permission: &allowed}}, nil
				}
			}
			source.inventory.RowsDigest, _ = RowsDigest(source.rows)
			p := build(t, req, source, pre)
			if p.Digest() == first.Digest() || p.Digest() == "" {
				t.Fatalf("digest did not bind %s", change)
			}
		})
	}
}

func TestDeterministicBindingViolationAndRowOrder(t *testing.T) {
	req := request(t, ontology.DefaultDocument(), ontology.DefaultDocument())
	req.Directives = []Directive{{ID: "all", Kind: SetField, Target: Target{Project: "P", Type: "issue"}, Field: "title", Value: "new"}}
	source := rowsFor(t, req, bead("P-2", "P", nil), bead("P-1", "P", nil))
	violations := []ontology.Violation{{Code: "ontology_guard_failed", Field: "fields.title", Message: "second"}, {Code: "invalid_params", Field: "fields.description", Message: "first"}}
	pre := &fixturePre{bindings: []Binding{{ID: "b", Type: "issue", Digest: strings.Repeat("b", 64)}, {ID: "a", Type: "issue", Digest: strings.Repeat("a", 64)}}, run: func(context.Context, Binding, Change) ([]ontology.Violation, error) { return violations, nil }}
	p := build(t, req, source, pre)
	slices.Reverse(violations)
	slices.Reverse(pre.bindings)
	slices.Reverse(source.rows)
	again := build(t, req, source, pre)
	if p.Ready() || again.Ready() || p.Digest() != again.Digest() {
		t.Fatal("reordering complete inputs changed refused plan evidence")
	}
	// Different incomplete supplied images still bind distinct refusal digests.
	source.rows = source.rows[:1]
	omissionA := build(t, req, source, pre)
	source.rows[0] = bead("P-2", "P", nil)
	omissionB := build(t, req, source, pre)
	if omissionA.Ready() || omissionB.Ready() || omissionA.Digest() == omissionB.Digest() {
		t.Fatal("refused plans failed to bind the actually enumerated rows")
	}
}

func TestObservedEmptyFieldsAndLargeNumbersSurviveRowDigest(t *testing.T) {
	row := Row{Revision: "9007199254740993", State: ontology.BeadState{ID: "P-1", Project: "P", Type: "issue", Status: "todo", Fields: map[string]any{}}}
	d, err := RowsDigest([]Row{row})
	if err != nil || d == "" {
		t.Fatal("observed empty fields were lost", err)
	}
	copy, err := detached(row)
	if err != nil || copy.State.Fields == nil || copy.Revision != row.Revision {
		t.Fatal("empty field/revision copy lost", err)
	}
	row.State.Fields["large"] = json.Number("9007199254740993")
	copy, _ = detached(row)
	if copy.State.Fields["large"] != json.Number("9007199254740993") {
		t.Fatal("row copy rounded an integer")
	}
}

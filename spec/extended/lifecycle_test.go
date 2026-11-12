package extended

import (
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

// These are authored project configuration inputs, not a reimplementation of
// category, slot, readiness or guard evaluation.
func customDocument() ontology.Document {
	return ontology.Document{
		Statuses: map[string]ontology.Status{
			"queued":    {Name: "Queued", Category: ontology.Unstarted},
			"working":   {Name: "Working", Category: ontology.Started},
			"finished":  {Name: "Finished", Category: ontology.Completed, Outcome: ontology.Success},
			"broken":    {Name: "Broken", Category: ontology.Completed, Outcome: ontology.Failure},
			"abandoned": {Name: "Abandoned", Category: ontology.Canceled},
			"parked":    {Name: "Parked", Category: ontology.Backlog},
		},
		Workflows: map[string]ontology.Workflow{"issue": {
			Statuses: []string{"queued", "working", "finished", "broken", "abandoned", "parked"},
			Initial:  "queued",
			Slots: map[ontology.Slot]string{
				ontology.Claim: "working", ontology.Close: "finished", ontology.Fail: "broken",
				ontology.Cancel: "abandoned", ontology.Reopen: "queued", ontology.Defer: "parked",
			},
			Transitions: []ontology.Rule{
				{From: []string{"queued"}, To: "working", Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenBlockers}}},
				{To: "finished", Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.NoOpenChildren}}},
				{To: "broken"}, {To: "abandoned"}, {To: "queued"}, {To: "parked"},
			},
		}},
		Types: map[string]ontology.Type{"issue": {Name: "Issue", Workflow: "issue", Fields: map[string]ontology.Field{
			"title": {Type: "string"}, "description": {Type: "string"},
		}}},
	}
}

func TestB03ConfiguredSlotsAndStatusPatchRefusal(t *testing.T) {
	f := newFixture(t, []string{"pl-custom"}, compile(t, customDocument(), nil))
	f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "custom workflow"}})
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-custom")
	if got := f.image(t).document(t, "pl-custom").Fields["status"]; got != "queued" {
		t.Fatalf("configured initial status: %v", got)
	}
	for _, status := range []string{"queued", "working", "finished", "not-configured"} {
		t.Run("direct-"+status, func(t *testing.T) {
			before := f.image(t)
			label := "direct-" + status
			data := f.call(t, label, "bead.update", map[string]any{"id": wireRef("pl-custom"),
				"fields": map[string]any{"title": "must not land", "status": status}})
			if got := at[string](t, data, "/error/data/code"); got != "ontology_use_slot_verb" {
				t.Fatalf("status patch refusal: %s", data)
			}
			f.unchanged(t, before)
			f.noReceipt(t, label, "bead.update")
		})
	}
	f.success(t, "bead.claim", map[string]any{"id": wireRef("pl-custom")})
	pageIDs(t, f.success(t, "bead.ready", nil))
	if got := f.image(t).document(t, "pl-custom").Fields["status"]; got != "working" {
		t.Fatalf("claim ignored configured slot: %v", got)
	}
	f.success(t, "bead.close", map[string]any{"id": wireRef("pl-custom")})
	closed := f.image(t).document(t, "pl-custom")
	if closed.Fields["status"] != "finished" || closed.Lease != nil {
		t.Fatalf("close ignored configured slot/lease settlement: %+v", closed)
	}
	f.slot(t, "reopen", "bead.reopen", "pl-custom", nil)
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-custom")
	if got := f.image(t).document(t, "pl-custom").Fields["status"]; got != "queued" {
		t.Fatalf("reopen ignored configured slot: %v", got)
	}
}

func TestB03AllConfiguredCategoriesDriveReadyAndClaim(t *testing.T) {
	for _, tc := range []struct {
		status string
		ready  bool
	}{
		{"queued", true}, {"working", false}, {"finished", false},
		{"broken", false}, {"abandoned", false}, {"parked", false}, {"intake", false},
	} {
		t.Run(tc.status, func(t *testing.T) {
			base := customDocument()
			base.Statuses["intake"] = ontology.Status{Name: "Intake", Category: ontology.Triage}
			w := base.Workflows["issue"]
			w.Statuses = append(w.Statuses, "intake")
			w.Initial = tc.status
			base.Workflows["issue"] = w
			f := newFixture(t, []string{"pl-category"}, compile(t, base, nil))
			f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "category test"}})
			before := f.image(t)
			ready := f.success(t, "bead.ready", nil)
			if tc.ready {
				pageIDs(t, ready, "pl-category")
			} else {
				pageIDs(t, ready)
			}
			f.unchanged(t, before)
			data := f.call(t, "claim-category", "bead.claim", map[string]any{"id": wireRef("pl-category")})
			if tc.ready {
				if at[int](t, data, "/result/revision") != 2 || f.image(t).document(t, "pl-category").Fields["status"] != "working" {
					t.Fatalf("configured unstarted bead was not claimable: %s", data)
				}
			} else {
				if at[string](t, data, "/error/data/code") != "invalid_params" {
					t.Fatalf("non-unstarted claim: %s", data)
				}
				f.unchanged(t, before)
				f.noReceipt(t, "claim-category", "bead.claim")
			}
		})
	}
}

func TestB03CreateRefusesUnconfiguredAndNonInitialStatuses(t *testing.T) {
	for _, tc := range []struct {
		status string
		code   string
	}{
		{"not-configured", "invalid_params"}, {"finished", "ontology_use_slot_verb"},
	} {
		t.Run(tc.status, func(t *testing.T) {
			f := newFixture(t, []string{"pl-never"}, compile(t, customDocument(), nil))
			before := f.image(t)
			data := f.call(t, "bad-create", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"status": tc.status, "title": "must disappear"}})
			if at[string](t, data, "/error/data/code") != tc.code {
				t.Fatalf("invalid configured create: %s", data)
			}
			f.unchanged(t, before)
			f.noReceipt(t, "bad-create", "bead.create")
		})
	}
}

func TestB08TemplateCloseRefusesWithoutAppendingReason(t *testing.T) {
	f := newFixture(t, []string{"pl-template"}, nil)
	f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "template", "template": true}})
	before := f.image(t)
	data := f.call(t, "template-close", "bead.close", map[string]any{"id": wireRef("pl-template"), "reason": "must not append"})
	if at[string](t, data, "/error/data/code") != "ontology_guard_failed" {
		t.Fatalf("template close allowed: %s", data)
	}
	f.unchanged(t, before)
	f.noReceipt(t, "template-close", "bead.close")
	f.success(t, "bead.update", map[string]any{"id": wireRef("pl-template"), "fields": map[string]any{"template": false}})
	f.success(t, "bead.close", map[string]any{"id": wireRef("pl-template"), "reason": "now allowed"})
	after := f.image(t)
	doc := after.document(t, "pl-template")
	if len(doc.Comments) != 1 || doc.Comments[0].Body != "now allowed" || doc.Fields["status"] != "done" || len(after.Log) != 3 {
		t.Fatalf("template refusal leaked or lost settlement: %+v", after)
	}
}

func TestB03B08BlockerUsesItsOwnProjectCatalog(t *testing.T) {
	base := customDocument()
	// The dependent's catalog calls the same immutable key nonterminal.
	// The other project's replacement definition makes it successful terminal.
	base.Statuses["shared"] = ontology.Status{Name: "Waiting here", Category: ontology.Backlog}
	w := base.Workflows["issue"]
	w.Statuses = append(w.Statuses, "shared")
	base.Workflows["issue"] = w
	overlay := ontology.Document{Statuses: map[string]ontology.Status{
		"shared": {Name: "Finished there", Category: ontology.Completed, Outcome: ontology.Success},
	}}
	otherWorkflow := w
	otherWorkflow.Slots = map[ontology.Slot]string{
		ontology.Claim: "working", ontology.Close: "shared", ontology.Fail: "broken",
		ontology.Cancel: "abandoned", ontology.Reopen: "queued", ontology.Defer: "parked",
	}
	otherWorkflow.Transitions = append(append([]ontology.Rule{}, w.Transitions...), ontology.Rule{To: "shared"})
	overlay.Workflows = map[string]ontology.Workflow{"issue": otherWorkflow}
	f := newFixture(t, []string{"pl-blocker", "pl-ordinary", "pl-conditional"},
		compile(t, base, map[string]ontology.Document{"other": overlay}))
	f.apply(t, "nodes", create("blocker", "other", map[string]any{"title": "other catalog"}),
		create("ordinary", project, map[string]any{"title": "ordinary"}),
		create("conditional", project, map[string]any{"title": "conditional"}))
	f.apply(t, "dependencies", edge(model.OpLink, "pl-ordinary", "pl-blocker", "blocks"),
		edge(model.OpLink, "pl-conditional", "pl-blocker", "conditional-blocks"))
	pageIDs(t, f.success(t, "bead.ready", nil))
	before := f.image(t)
	data := f.call(t, "blocked", "bead.close", map[string]any{"id": wireRef("pl-ordinary"), "reason": "must not append"})
	if at[string](t, data, "/error/data/code") != "ontology_guard_failed" {
		t.Fatalf("live blocker allowed close: %s", data)
	}
	f.unchanged(t, before)
	f.noReceipt(t, "blocked", "bead.close")
	// Direct Writer composition is necessary for a different project's slot;
	// neither dependency insertion nor this setup adds a new RPC binding.
	f.slot(t, "finish-other", "bead.close", "pl-blocker", nil)
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-ordinary")
	f.success(t, "bead.close", map[string]any{"id": wireRef("pl-ordinary")})
	before = f.image(t)
	data = f.call(t, "success-is-not-failure", "bead.close", map[string]any{"id": wireRef("pl-conditional")})
	if at[string](t, data, "/error/data/code") != "ontology_guard_failed" {
		t.Fatalf("successful blocker satisfied failure condition: %s", data)
	}
	f.unchanged(t, before)
	f.slot(t, "reopen-other", "bead.reopen", "pl-blocker", nil)
	f.slot(t, "fail-other", "bead.fail", "pl-blocker", nil)
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-conditional")
	f.success(t, "bead.close", map[string]any{"id": wireRef("pl-conditional")})
}

func TestB08CloseGuardsReadTheFinalBatchImage(t *testing.T) {
	base := ontology.DefaultDocument()
	typ := base.Types["issue"]
	typ.Fields["resolution"] = ontology.Field{Type: "string"}
	base.Types["issue"] = typ
	w := base.Workflows["issue"]
	for i := range w.Transitions {
		if w.Transitions[i].To == w.Slots[ontology.Close] {
			w.Transitions[i].Guards = append(w.Transitions[i].Guards, ontology.Guard{Kind: ontology.FieldsRequired, Fields: []string{"resolution"}})
		}
	}
	base.Workflows["issue"] = w
	f := newFixture(t, []string{"pl-guard"}, compile(t, base, nil))
	f.apply(t, "template", create("bead", project, map[string]any{"title": "preserve", "template": true}))
	before := f.image(t)
	_, err := f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: []model.Op{
		patch("pl-guard", map[string]any{"template": false, "title": "must roll back"}),
		{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref("pl-guard"), Body: "must roll back too"}},
	}}, f.meta("missing-resolution", "bead.close"))
	refusal := typed(t, err, "ontology_guard_failed")
	violations, err := json.Marshal(refusal.Data.Details["violations"])
	if err != nil {
		t.Fatal(err)
	}
	var guards []ontology.Violation
	if err := json.Unmarshal(violations, &guards); err != nil {
		t.Fatal(err)
	}
	if len(guards) != 1 || guards[0].Guard != ontology.FieldsRequired {
		t.Fatalf("final image must fail the resolution guard, got %+v", guards)
	}
	f.unchanged(t, before)
	f.noReceipt(t, "missing-resolution", "bead.close")
	// The pre-image is a template with no resolution. The final image removes
	// the flag and supplies the required field in the very same close request.
	cs := model.ChangeSet{Ops: []model.Op{
		patch("pl-guard", map[string]any{"template": false, "resolution": "verified"}),
		{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref("pl-guard"), Body: "closed after verification"}},
	}}
	commit, err := f.writer.Apply(f.ctx, f.by, cs, f.meta("final-image", "bead.close"))
	if err != nil {
		t.Fatalf("final-image guard refused a valid atomic close: %v", err)
	}
	if commit.Revision.String() != "2" || len(commit.Results) != 2 {
		t.Fatalf("close must be one atomic revision: %+v", commit)
	}
	after := f.image(t)
	want := before.document(t, "pl-guard")
	want.Fields["template"], want.Fields["status"], want.Fields["resolution"] = false, "done", "verified"
	closedAt := f.inputs.Now().UTC()
	want.ClosedAt = &closedAt
	closeReason := "closed after verification"
	want.CloseReason = &closeReason
	want.Comments = []pipeline.Comment{{Body: "closed after verification", Principal: f.by, Time: f.inputs.Now()}}
	sameJSON(t, after.document(t, "pl-guard"), want)
	if len(after.Log) != 2 || len(after.Versions["pl-guard"]) != 2 || after.row(t, "pl-guard").Revision.String() != "2" {
		t.Fatalf("close minted partial or duplicate history: %+v", after)
	}
	for _, result := range commit.Results {
		if result.Revision.String() != "2" {
			t.Fatalf("operation has a partial revision: %+v", result)
		}
		sameJSON(t, json.RawMessage(result.Record), after.document(t, "pl-guard"))
	}
}

func TestB08CloseThenReopenReblocksAndCASRefuses(t *testing.T) {
	f := newFixture(t, []string{"pl-blocker", "pl-dependent"}, nil)
	f.apply(t, "nodes", create("blocker", project, map[string]any{"title": "blocker"}),
		create("dependent", project, map[string]any{"title": "dependent"}))
	f.apply(t, "dependency", edge(model.OpLink, "pl-dependent", "pl-blocker", "blocks"))
	f.success(t, "bead.close", map[string]any{"id": wireRef("pl-blocker")})
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-dependent")
	f.slot(t, "reopen", "bead.reopen", "pl-blocker", nil)
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-blocker")
	before := f.image(t)
	data := f.call(t, "stale-close", "bead.close", map[string]any{"id": wireRef("pl-blocker"),
		"reason": "must not append", "expect": []any{map[string]any{"ref": wireRef("pl-blocker"), "revision": 3}}})
	if at[string](t, data, "/error/data/code") != "precondition_failed" {
		t.Fatalf("stale close: %s", data)
	}
	f.unchanged(t, before)
	f.noReceipt(t, "stale-close", "bead.close")
	data = f.call(t, "reblocked-close", "bead.close", map[string]any{"id": wireRef("pl-dependent")})
	if at[string](t, data, "/error/data/code") != "ontology_guard_failed" {
		t.Fatalf("reopen failed to reblock close: %s", data)
	}
	f.unchanged(t, before)
}

func TestB08AlreadyClosedIsAnIdempotentNoop(t *testing.T) {
	f := newFixture(t, []string{"pl-closed"}, nil)
	f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "close once"}})
	f.success(t, "bead.close", map[string]any{"id": wireRef("pl-closed")})
	before := f.image(t)
	// A fresh request key distinguishes an already-closed no-op from replay.
	closed := f.success(t, "bead.close", map[string]any{"id": wireRef("pl-closed")})
	if at[int](t, closed, "/result/revision") != 2 || at[int](t, closed, "/result/results/0/revision") != 2 {
		t.Fatalf("already-closed request minted a revision: %s", closed)
	}
	f.unchanged(t, before)
}

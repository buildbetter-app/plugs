package b03catalog_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	queued    = "awaiting-work"
	started   = "work-running"
	completed = "work-delivered"
	failed    = "work-rejected"
	canceled  = "work-abandoned"
	parked    = "work-parked"
	intake    = "work-intake"
	handoff   = "handoff"
	retired   = "retired-key"
)

// These inputs describe Plug categories and workflow slots, not Beads' parser
// or compiler test vectors. Custom rows are never inserted by a fixture.
func catalogDocument() ontology.Document {
	doc := ontology.DefaultDocument()
	for key, value := range map[string]ontology.Status{
		queued:    {Name: "Ready for a person", Category: ontology.Unstarted},
		started:   {Name: "Being worked", Category: ontology.Started},
		completed: {Name: "Delivered", Category: ontology.Completed, Outcome: ontology.Success},
		failed:    {Name: "Delivery failed", Category: ontology.Completed, Outcome: ontology.Failure},
		canceled:  {Name: "Abandoned", Category: ontology.Canceled},
		parked:    {Name: "On ice", Category: ontology.Backlog},
		intake:    {Name: "Needs triage", Category: ontology.Triage},
		handoff:   {Name: "Base terminal handoff", Category: ontology.Completed, Outcome: ontology.Failure},
		retired:   {Name: "Retained catalog identity", Category: ontology.Completed, Outcome: ontology.Failure},
	} {
		doc.Statuses[key] = value
	}
	// A historical Beads maximum of 50 is not part of the adopted Plug
	// contract. The public producer must accept this otherwise valid catalog.
	for i := 0; i < 64; i++ {
		doc.Statuses[fmt.Sprintf("extra-%02d", i)] = ontology.Status{Name: fmt.Sprintf("Free text status %d", i), Category: ontology.Unstarted}
	}
	workflow := ontology.Workflow{
		Statuses: []string{queued, started, completed, failed, canceled, parked, intake, handoff}, Initial: queued,
		Slots: map[ontology.Slot]string{ontology.Claim: started, ontology.Close: completed, ontology.Reopen: queued, ontology.Fail: failed, ontology.Cancel: canceled, ontology.Defer: parked},
		Transitions: []ontology.Rule{
			{From: []string{queued, handoff}, To: started, Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}},
			{To: completed, Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}},
			{To: queued, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
			{To: failed, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
			{To: canceled, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
			{To: parked, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
		},
	}
	doc.Workflows["delivery"] = workflow
	workflow.Initial = handoff
	doc.Workflows["handoff-flow"] = workflow
	fields := map[string]ontology.Field{"title": {Type: "string"}, "catalog_stamp": {Type: "string", Default: "accepted"}}
	doc.Types["delivery"] = ontology.Type{Name: "Delivery", Workflow: "delivery", Fields: fields}
	doc.Types["handoff-item"] = ontology.Type{Name: "Handoff", Workflow: "handoff-flow", Fields: fields}
	typ := doc.Types["issue"]
	typ.Fields["catalog_stamp"] = ontology.Field{Type: "string", Default: "accepted"}
	doc.Types["issue"] = typ
	return doc
}

func slot(f *fixture, t *testing.T, project string, ref model.Ref, method, want string) model.Commit {
	t.Helper()
	var commit model.Commit
	f.result(t, method, f.params(method, project, map[string]any{"id": ref}), &commit)
	f.status(t, ref, want)
	return commit
}

func lifecycleDocument(t *testing.T, f *fixture, ref model.Ref) pipeline.Document {
	t.Helper()
	doc, err := pipeline.DecodeDocument(record(t, f.snapshot(t), ref).Body)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestInstalledCatalogPlanApplyLifecycleAndDurableReopen(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		builtin, originalRequest, originalResponse := f.create(t, "base-project", "issue")
		f.result(t, "bead.comment", f.params("bead.comment", "base-project", map[string]any{"id": builtin, "body": "retain through migration"}), &model.Commit{})
		before := f.snapshot(t)
		oldBody := record(t, before, builtin)
		plan, _ := f.install(t, "", catalogDocument())
		if len(plan.Changes.Migration.Rows) != 1 || plan.Changes.Migration.Rows[0].After.State.Fields["catalog_stamp"] != "accepted" {
			t.Fatal("catalog apply did not migrate the actual preexisting row")
		}
		var got ontologyops.Result
		f.result(t, "ontology.get", map[string]any{"project": "base-project"}, &got)
		if len(got.Document.Statuses) <= 50 || got.Document.Statuses[queued].Category != ontology.Unstarted || got.Document.Statuses[parked].Category != ontology.Backlog || got.Drifted {
			t.Fatalf("installed custom catalog differs from accepted producer input: %+v", got)
		}
		current, err := f.domain.Get(f.ctx, caller, service.GetRequest{ID: builtin})
		if err != nil || current.Bead.Fields["catalog_stamp"] != "accepted" || len(current.Comments) != 1 || current.Comments[0].Body != "retain through migration" {
			t.Fatalf("migration lost complete ordinary row: %+v %v", current, err)
		}
		// Historical storage preserves the exact serialized pre-migration body.
		r, err := f.store.Read(f.ctx, driver.View{Incarnation: before.Meta.Incarnation, At: &before.Meta.Head})
		if err != nil {
			t.Fatal(err)
		}
		rows, readErr := r.Get(f.ctx, []model.Ref{builtin})
		_ = r.Close()
		if readErr != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, oldBody.Body) {
			t.Fatalf("migration rewrote historical serialized bytes: %+v %v", rows, readErr)
		}

		ref, _, _ := f.create(t, "base-project", "delivery")
		f.status(t, ref, queued)
		f.ready(t, "base-project", ref, true)
		slot(f, t, "base-project", ref, "bead.claim", started)
		f.ready(t, "base-project", ref, false)
		closed := slot(f, t, "base-project", ref, "bead.close", completed)
		f.ready(t, "base-project", ref, false)
		f.refuse(t, "bead.claim", f.params("bead.claim", "base-project", map[string]any{"id": ref}), "invalid_params")
		slot(f, t, "base-project", ref, "bead.reopen", queued)
		f.ready(t, "base-project", ref, true)
		slot(f, t, "base-project", ref, "bead.defer", parked)
		f.ready(t, "base-project", ref, false)
		f.refuse(t, "bead.claim", f.params("bead.claim", "base-project", map[string]any{"id": ref}), "invalid_params")
		slot(f, t, "base-project", ref, "bead.reopen", queued)
		slot(f, t, "base-project", ref, "bead.fail", failed)
		f.ready(t, "base-project", ref, false)
		f.refuse(t, "bead.claim", f.params("bead.claim", "base-project", map[string]any{"id": ref}), "invalid_params")
		slot(f, t, "base-project", ref, "bead.reopen", queued)
		slot(f, t, "base-project", ref, "bead.cancel", canceled)
		f.ready(t, "base-project", ref, false)
		slot(f, t, "base-project", ref, "bead.reopen", queued)
		old, err := f.domain.Get(f.ctx, caller, service.GetRequest{ID: ref, AsOf: &service.AsOf{Revision: closed.Revision, Incarnation: before.Meta.Incarnation}})
		if err != nil || old.Bead.Status != completed {
			t.Fatalf("installed catalog lifecycle history: %+v %v", old, err)
		}
		var history ontologyops.HistoryResult
		f.result(t, "ontology.history", map[string]any{"project": "base-project", "after": 0, "limit": 1000}, &history)
		if len(history.Revisions) != 2 || history.Revisions[1].Revision != got.Revision {
			t.Fatalf("source revisions were conflated with ordinary lifecycle writes: %+v", history)
		}
		var diff ontologyops.DiffResult
		f.result(t, "ontology.diff", map[string]any{"project": "base-project", "fromRevision": 1, "toRevision": got.Revision}, &diff)
		if len(diff.Patch) != 1 || diff.Patch[0].Value == nil || diff.Patch[0].Value.Statuses[queued].Name == "" {
			t.Fatalf("public source history omitted accepted custom definition: %+v", diff)
		}
		if e.name != "memory" {
			durable := f.snapshot(t)
			f.reopen(t)
			equal(t, "real close/reopen preserves source, rows, history, receipts and installed ready views", durable, f.snapshot(t))
			f.status(t, ref, queued)
			f.ready(t, "base-project", ref, true)
			// Ordinary receipts survive host reconstruction even though retained
			// in-process ontology plans intentionally do not survive it.
			replayed := f.result(t, "bead.create", originalRequest, nil)
			if !replayed.Meta.Replayed || !bytes.Equal(replayed.Result, originalResponse.Result) {
				t.Fatal("ordinary durable receipt failed after reopen")
			}
			equal(t, "receipt replay after reopen adds no revision", durable, f.snapshot(t))
			slot(f, t, "base-project", ref, "bead.claim", started)
		}
	})
}

func TestInstalledCatalogLifecycleTimestampsPreserveStatusSpecificEntries(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		f.install(t, "", catalogDocument())
		ref, _, _ := f.create(t, "base-project", "delivery")
		created := lifecycleDocument(t, f, ref)
		if created.CreatedAt == nil || created.CreatedAt.IsZero() || created.StartedAt != nil || created.ClosedAt != nil {
			t.Fatalf("create lifecycle times=%+v", created)
		}

		slot(f, t, "base-project", ref, "bead.claim", started)
		claimed := lifecycleDocument(t, f, ref)
		if claimed.StartedAt == nil || claimed.StartedAt.IsZero() || claimed.ClosedAt != nil || !claimed.CreatedAt.Equal(*created.CreatedAt) {
			t.Fatalf("claim lifecycle times=%+v", claimed)
		}
		startedAt := *claimed.StartedAt

		slot(f, t, "base-project", ref, "bead.close", completed)
		closed := lifecycleDocument(t, f, ref)
		if closed.ClosedAt == nil || closed.ClosedAt.IsZero() || closed.StartedAt == nil || !closed.StartedAt.Equal(startedAt) {
			t.Fatalf("close lifecycle times=%+v", closed)
		}
		closedAt := *closed.ClosedAt

		// A same-status close may append a reason, but it must retain the first
		// terminal instant rather than stamping a second close time.
		f.result(t, "bead.close", f.params("bead.close", "base-project", map[string]any{"id": ref, "reason": "settlement note"}), nil)
		repeated := lifecycleDocument(t, f, ref)
		if repeated.ClosedAt == nil || !repeated.ClosedAt.Equal(closedAt) {
			t.Fatalf("repeated close rewrote closedAt: before=%s after=%+v", closedAt, repeated)
		}

		slot(f, t, "base-project", ref, "bead.reopen", queued)
		reopened := lifecycleDocument(t, f, ref)
		if reopened.ClosedAt != nil || reopened.StartedAt == nil || !reopened.StartedAt.Equal(startedAt) || !reopened.CreatedAt.Equal(*created.CreatedAt) {
			t.Fatalf("reopen lifecycle times=%+v", reopened)
		}
	})
}

func TestSameKeyProjectOverlaysReplaceWholeDefinitionsAndOlderReplay(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		f.install(t, "", catalogDocument())
		west := ontology.Document{Statuses: map[string]ontology.Status{handoff: {Name: "West ready handoff", Category: ontology.Unstarted}}}
		_, olderApply := f.install(t, "west", west)
		east := ontology.Document{Statuses: map[string]ontology.Status{handoff: {Name: "East parked handoff", Category: ontology.Backlog}}}
		f.install(t, "east", east)
		for _, tc := range []struct {
			project  string
			category ontology.Category
			outcome  ontology.Outcome
			ready    bool
		}{
			{"base-project", ontology.Completed, ontology.Failure, false},
			{"west", ontology.Unstarted, "", true},
			{"east", ontology.Backlog, "", false},
		} {
			var got ontologyops.Result
			f.result(t, "ontology.get", map[string]any{"project": tc.project}, &got)
			status := got.Document.Statuses[handoff]
			if status.Category != tc.category || status.Outcome != tc.outcome {
				t.Fatalf("same-key replacement inherited another project's fields: %s %+v", tc.project, status)
			}
			ref, _, _ := f.create(t, tc.project, "handoff-item")
			f.status(t, ref, handoff)
			f.ready(t, tc.project, ref, tc.ready)
			if tc.ready {
				slot(f, t, tc.project, ref, "bead.claim", started)
			} else {
				f.refuse(t, "bead.claim", f.params("bead.claim", tc.project, map[string]any{"id": ref}), "invalid_params")
			}
		}
		// B supersedes A. Replaying A's durable receipt must keep the latest
		// installed categories used by ordinary creates and claim decisions.
		west.Statuses[handoff] = ontology.Status{Name: "West now parked", Category: ontology.Backlog}
		f.install(t, "west", west)
		before := f.snapshot(t)
		replayed := f.result(t, "ontology.apply", olderApply, nil)
		if !replayed.Meta.Replayed {
			t.Fatal("older accepted public apply did not replay its receipt")
		}
		equal(t, "older apply receipt conserves latest durable catalog", before, f.snapshot(t))
		ref, _, _ := f.create(t, "west", "handoff-item")
		f.ready(t, "west", ref, false)
		f.refuse(t, "bead.claim", f.params("bead.claim", "west", map[string]any{"id": ref}), "invalid_params")
		if e.name != "memory" {
			before := f.snapshot(t)
			f.reopen(t)
			equal(t, "project overlays and their revisions survive close/open", before, f.snapshot(t))
			f.ready(t, "west", ref, false)
			f.refuse(t, "bead.claim", f.params("bead.claim", "west", map[string]any{"id": ref}), "invalid_params")
		}
	})
}

func TestMalformedCatalogAndMixedBatchRefuseAtomically(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		f.install(t, "", catalogDocument())
		ref, _, _ := f.create(t, "base-project", "delivery")
		for _, tc := range []struct {
			raw  string
			code model.ErrorCode
		}{
			{`{"statuses":{"bad":{"name":"Bad","category":"active"}}}`, "invalid_params"},
			{`{"statuses":{"bad":{"name":"Bad","category":"unstarted","unknown":true}}}`, "invalid_params"},
			// The shared byte-envelope decoder rejects duplicate object keys
			// before ontology.plan dispatch; preserve its exact error boundary.
			{`{"statuses":{"bad":{"name":"First","category":"unstarted"},"bad":{"name":"Second","category":"backlog"}}}`, "invalid_request"},
			{`{"statuses":null}`, "invalid_params"},
			{`{"priorities":{"highest":0}}`, "invalid_params"},
			{`{"directives":[]}`, "invalid_params"},
		} {
			f.refuse(t, "ontology.plan", map[string]any{"project": "", "document": json.RawMessage(tc.raw)}, tc.code)
		}
		f.refuse(t, "bead.update", f.params("bead.update", "base-project", map[string]any{"id": ref, "fields": map[string]any{"title": "must roll back", "status": "unconfigured-value"}}), "ontology_use_slot_verb")
		f.refuse(t, "tx.apply", f.params("tx.apply", "base-project", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
			{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": "valid prefix must roll back"}}},
			{Kind: model.OpCreate, Create: &model.CreateOp{Key: "new-row", Type: "delivery", Fields: map[string]any{"title": "created prefix must roll back"}}},
			{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref, Body: "comment prefix must roll back"}},
			{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"status": "unconfigured-value"}}},
		}}}), "ontology_use_slot_verb")

		// A valid retained plan cannot repair or overwrite corruption silently.
		plan := f.plan(t, "", catalogDocument())
		apply := f.applyParams(plan)
		f.seedSource(t, []byte(`{"base":{},"unsupported":true}`), model.OpPatch)
		f.refuse(t, "ontology.get", map[string]any{"project": "base-project"}, "invalid_params")
		f.refuse(t, "ontology.plan", map[string]any{"project": "", "document": catalogDocument()}, "invalid_params")
		f.refuse(t, "ontology.apply", apply, "invalid_params")
		before := f.snapshot(t)
		if err := f.bindStored(); err == nil {
			t.Fatal("connected host silently replaced malformed installed source with defaults")
		}
		equal(t, "refused source reload changes no durable state or prior runtime", before, f.snapshot(t))
	})
}

func TestCatalogApplySQLPublicationRollbackAndRetry(t *testing.T) {
	eachEngine(t, []string{"sqlite", "postgres"}, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		ref, _, _ := f.create(t, "base-project", "issue")
		plan := f.plan(t, "", catalogDocument())
		params := f.applyParams(plan)
		e.publicationFault(t, true)
		t.Cleanup(func() { e.publicationFault(t, false) })
		f.refuse(t, "ontology.apply", params, "internal_error")
		e.publicationFault(t, false)
		f.apply(t, plan, params)
		got, err := f.domain.Get(f.ctx, caller, service.GetRequest{ID: ref})
		if err != nil || got.Bead.Fields["catalog_stamp"] != "accepted" {
			t.Fatalf("retry failed to publish source and actual migrated row: %+v %v", got, err)
		}
		before := f.snapshot(t)
		f.reopen(t)
		equal(t, "successful exact retry remains durable", before, f.snapshot(t))
	})
}

// This is a public dry-run/commit boundary, not a helper-only descriptor test.
// The source counts toward the same complete atomic event cap as migrated rows.
func TestPublicCatalogApplyDryRunEventLimitParity(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		remaining := model.MaxChangesetEvents - 1
		for batch := 0; remaining > 0; batch++ {
			count := remaining
			if count > 200 {
				count = 200
			}
			ops := make([]model.Op, 0, count)
			for i := 0; i < count; i++ {
				ops = append(ops, model.Op{Kind: model.OpCreate, Create: &model.CreateOp{
					Key: fmt.Sprintf("limit-%d-%d", batch, i), Type: "issue", Fields: map[string]any{"title": "actual migration row"},
				}})
			}
			var created model.Commit
			f.result(t, "tx.apply", f.params("tx.apply", "base-project", map[string]any{"changeSet": model.ChangeSet{Ops: ops}}), &created)
			if len(created.Results) != count {
				t.Fatalf("public batch created %d rows, want %d", len(created.Results), count)
			}
			remaining -= count
		}

		document := ontology.DefaultDocument()
		issue := document.Types["issue"]
		issue.Fields["limit_stamp"] = ontology.Field{Type: "boolean", Default: false}
		document.Types["issue"] = issue
		plan := f.plan(t, "", document)
		params := f.applyParams(plan)
		meta := params["_meta"].(map[string]any)
		meta["dryRun"], meta["return"] = true, "minimal"
		before := f.snapshot(t)
		var preview model.Commit
		f.result(t, "ontology.apply", params, &preview)
		if !preview.WouldCommit || len(preview.Results) != model.MaxChangesetEvents {
			t.Fatalf("public boundary dry-run=%+v", preview)
		}
		equal(t, "successful dry-run publishes no events/revision/receipt or row changes", before, f.snapshot(t))
		delete(meta, "dryRun")
		f.apply(t, plan, params)
		after := f.snapshot(t)
		last := after.Log[len(after.Log)-1]
		if len(last.Events) != model.MaxChangesetEvents || last.ValidateEvents() != nil {
			t.Fatalf("boundary real apply lacks exact complete event plan: %+v", last)
		}

		noOp := f.applyParams(f.plan(t, "", document))
		noOp["_meta"].(map[string]any)["dryRun"] = true
		before = f.snapshot(t)
		preview = model.Commit{}
		f.result(t, "ontology.apply", noOp, &preview)
		if preview.WouldCommit || len(preview.Results) != 0 || preview.Revision != before.Meta.Head {
			t.Fatalf("no-op dry-run=%+v", preview)
		}
		equal(t, "no-op dry-run publishes no events/revision/receipt or row changes", before, f.snapshot(t))

		f.create(t, "base-project", "issue")
		issue.Fields["overflow_stamp"] = ontology.Field{Type: "boolean", Default: false}
		document.Types["issue"] = issue
		overflow := f.applyParams(f.plan(t, "", document))
		overflowMeta := overflow["_meta"].(map[string]any)
		overflowMeta["dryRun"], overflowMeta["return"] = true, "minimal"
		f.refuse(t, "ontology.apply", overflow, "scope_too_large")
		delete(overflowMeta, "dryRun")
		f.refuse(t, "ontology.apply", overflow, "scope_too_large")
	})
}

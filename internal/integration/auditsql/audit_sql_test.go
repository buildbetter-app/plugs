package auditsql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

func wire(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func equalJSON(t *testing.T, got, want any) {
	t.Helper()
	if !bytes.Equal(wire(t, got), wire(t, want)) {
		t.Fatalf("got %s\nwant %s", wire(t, got), wire(t, want))
	}
}

func assertClosePlan(t *testing.T, f *fixture, g graph, set model.Changeset, digest string) {
	t.Helper()
	if set.Revision.String() != "6" || set.Kind != model.ChangesetWrite || set.Incarnation != f.inc || set.Time.IsZero() || len(set.Changes) != 1 || len(set.Events) != 3 {
		t.Fatalf("incomplete close publication: %+v", set)
	}
	equalJSON(t, set.Principal, writerPrincipal)
	equalJSON(t, set.Changes[0], model.ChangeRecord{Ref: g.blocker, Op: model.OpPatch, FromRevision: model.Natural64(2), ChangedPaths: []string{"/closeReason", "/closedAt", "/comments", "/fields/status", "/lease"}})
	provenance := model.EventProvenance{Method: "bead.close", Slot: "close", SetBy: map[string]string{"closeReason": "request", "closedAt": "core", "fields.status": "request", "comments": "request", "lease": "request"}, Cause: model.EventCause{Root: key(6), Depth: model.Natural64(0), Chain: []string{}}, Chain: []string{}}
	for i, descriptor := range set.Events {
		if descriptor.Index.String() != model.Natural64(uint64(i)).String() || !descriptor.ChangeIndex.IsZero() || descriptor.Resource.Kind != "bead" {
			t.Fatalf("descriptor order/anchor=%+v", descriptor)
		}
		equalJSON(t, descriptor.Provenance, provenance)
		if i == 0 {
			if descriptor.Type != model.EventBeadClosed || descriptor.Details != nil {
				t.Fatalf("ordinary close missing: %+v", descriptor)
			}
			equalJSON(t, descriptor.Resource.Ref, g.blocker)
			continue
		}
		dependent := model.RefByID(g.dependents[i-1].ID, model.Natural64(0))
		if descriptor.Type != model.EventDependencyConditionUnmet {
			t.Fatalf("notice %d type=%s", i, descriptor.Type)
		}
		equalJSON(t, descriptor.Resource.Ref, dependent)
		project := model.Ref{ID: blockerProject}
		// Expected facts only: the real Writer is the sole descriptor producer.
		equalJSON(t, descriptor.Details, &model.ConditionUnmetDetails{
			Dependent:     model.EventImage{Ref: dependent, Revision: g.linked.Revision},
			BlockerBefore: model.EventImage{Ref: g.blocker, Revision: model.Natural64(2)},
			BlockerAfter:  model.EventImage{Ref: g.blocker, Revision: set.Revision},
			Edge:          model.ConditionalEdge{Dependent: dependent, Blocker: g.blocker, Kind: "conditional-blocks", Revision: g.linked.Revision},
			Ontology:      model.OntologyPin{Ref: f.sourceRef, Revision: model.Natural64(1), Digest: digest, Project: &project},
			Before:        model.ConditionDecision{Status: "todo", Category: "unstarted"},
			After:         model.ConditionDecision{Status: "done", Category: "completed", Outcome: "success", Terminal: true},
		})
	}
}

func assertDelivered(t *testing.T, events []model.Event, set model.Changeset) {
	t.Helper()
	if len(events) != len(set.Events) {
		t.Fatalf("delivered %d events, want %d", len(events), len(set.Events))
	}
	for i, item := range events {
		e, d := item.Durable, set.Events[i]
		if item.Category != model.EventDurable || e == nil || e.Incarnation != set.Incarnation || e.Revision.String() != set.Revision.String() || e.Index.String() != d.Index.String() || e.Type != d.Type || e.Resource != d.Resource.Kind+"/"+d.Resource.Ref.ID || e.Derived {
			t.Fatalf("event %d envelope=%+v", i, item)
		}
		equalJSON(t, e.Principal, set.Principal)
		equalJSON(t, e.Cause, d.Provenance.Cause)
		equalJSON(t, e.Chain, d.Provenance.Chain)
		equalJSON(t, e.SetBy, d.Provenance.SetBy)
		equalJSON(t, e.Details, d.Details)
		if i == 0 {
			if e.Before["fields"].(map[string]any)["status"] != "todo" || e.After["fields"].(map[string]any)["status"] != "done" {
				t.Fatalf("ordinary history images=%+v", e)
			}
			if _, present := e.Before["closeReason"]; present || e.After["closeReason"] != "SQL audited close" {
				t.Fatalf("first close reason history images=%+v", e)
			}
			equalJSON(t, e.Changed, set.Changes[0].ChangedPaths)
		} else if e.Changed == nil || len(e.Changed) != 0 || e.Before != nil || e.After != nil {
			t.Fatalf("notice invented a mutation/image: %+v", e)
		}
	}
}

type handlerFunc func(context.Context, coreevents.Delivery) error

func (h handlerFunc) Deliver(ctx context.Context, delivery coreevents.Delivery) error {
	return h(ctx, delivery)
}

func TestSQLAuditClosePersistsAndPostRecoveryReplaysAfterReopen(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		g := f.graph(t)
		dependents := f.histories(t, g.dependents)
		digest := f.compiled.Digest()
		first, err := f.close(g)
		if err != nil || first.Replayed {
			t.Fatalf("fresh service close=%+v err=%v", first, err)
		}
		set := f.set(t, first.Commit.Revision)
		assertClosePlan(t, f, g, set, digest)
		receipt := f.receipt(t)
		if receipt == nil || receipt.ResponsePending {
			t.Fatalf("close receipt=%+v", receipt)
		}
		equalJSON(t, receipt.Commit, first.Commit)
		equalJSON(t, receipt.Changeset, set)
		page, err := f.eventReader().Read(f.ctx, coreevents.ReadRequest{After: startPosition(f, g), Principal: readerPrincipal, Limit: 3})
		if err != nil {
			t.Fatal(err)
		}
		events := page.Events()
		assertDelivered(t, events, set)
		if !reflect.DeepEqual(dependents, f.histories(t, g.dependents)) {
			t.Fatal("condition notices changed dependent records or retained history")
		}
		sourceHistory := f.histories(t, []model.Ref{f.sourceRef})[f.sourceRef.ID]
		if len(sourceHistory.Versions) != 1 || !reflect.DeepEqual(sourceHistory.Versions[0].Principal, fixturePrincipal) {
			t.Fatalf("protected-source attribution was not retained: %+v", sourceHistory)
		}

		cp := f.checkpoints(t)
		var deliveries []coreevents.Delivery
		binding := coreevents.Binding{ID: "audit-sql-post", Class: coreevents.Post, Principal: readerPrincipal, Handler: handlerFunc(func(_ context.Context, item coreevents.Delivery) error {
			deliveries = append(deliveries, item)
			return nil
		})}
		if _, err := cp.Save(f.ctx, binding.ID, 0, coreevents.State{Position: startPosition(f, g), Class: binding.Class}); err != nil {
			t.Fatal(err)
		}
		e.checkpointFault(t, f.ctx, true)
		dispatch := &coreevents.Dispatcher{Reader: f.eventReader(), Checkpoints: cp, Active: f.lease}
		warnings := dispatch.RunPost(f.ctx, []coreevents.Binding{binding}, set.Revision)
		if len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" || warnings[0].Details["binding"] != binding.ID || len(deliveries) != 2 || deliveries[0].Late || deliveries[1].Late {
			t.Fatalf("post SQL checkpoint failure: warnings=%+v deliveries=%+v", warnings, deliveries)
		}
		state, generation, err := cp.Load(f.ctx, binding.ID)
		if err != nil || generation != 2 || state.Position.Revision.String() != "6" || !state.Position.Index.IsZero() {
			t.Fatalf("failed notice acknowledgement advanced SQL checkpoint: %+v gen=%d err=%v", state, generation, err)
		}

		// Change current ontology so done now satisfies the condition. Retained
		// notices must still use the original success decision and digest.
		f.source = service.OntologySource{Base: ontology.DefaultDocument()}
		f.source.Base.Statuses["done"] = f.source.Base.Statuses["failed"]
		f.source.Base.Statuses["new_success"] = ontology.Status{Name: "New success", Category: ontology.Completed, Outcome: ontology.Success}
		workflow := f.source.Base.Workflows["issue"]
		workflow.Statuses = append(workflow.Statuses, "new_success")
		workflow.Slots[ontology.Close] = "new_success"
		f.source.Base.Workflows["issue"] = workflow
		f.seedSource(t, model.OpPatch)
		f.reopen(t)
		if f.compiled.Digest() == digest || !f.compiled.Effective(blockerProject).SatisfiesConditionalBlocks("done") {
			t.Fatal("current ontology fixture did not change the historical decision")
		}
		equalJSON(t, f.receipt(t), receipt)
		equalJSON(t, f.set(t, set.Revision), set)
		cp = f.checkpoints(t)
		restored, restoredGeneration, err := cp.Load(f.ctx, binding.ID)
		if err != nil || restoredGeneration != generation || !reflect.DeepEqual(restored, state) {
			t.Fatalf("checkpoint lost on store reopen: %+v gen=%d err=%v", restored, restoredGeneration, err)
		}
		replayed, err := f.close(g)
		if err != nil || !replayed.Replayed {
			t.Fatalf("same-key service replay after reopen=%+v err=%v", replayed, err)
		}
		equalJSON(t, replayed.Commit, first.Commit)
		e.checkpointFault(t, f.ctx, false)
		dispatch = &coreevents.Dispatcher{Reader: f.eventReader(), Checkpoints: cp, Active: f.lease}
		progress, err := dispatch.Step(f.ctx, binding)
		if err != nil || progress.Delivered != 2 || len(deliveries) != 4 || !deliveries[2].Late || !deliveries[3].Late {
			t.Fatalf("late post recovery=%+v deliveries=%+v err=%v", progress, deliveries, err)
		}
		if deliveries[1].Key.String() != deliveries[2].Key.String() || deliveries[1].Key.Binding != binding.ID || deliveries[1].Key.Index.String() != "1" || deliveries[1].Key.Revision.String() != set.Revision.String() || deliveries[1].Key.Incarnation != f.inc {
			t.Fatalf("notice recovery changed delivery key: %+v -> %+v", deliveries[1].Key, deliveries[2].Key)
		}
		equalJSON(t, deliveries[1].Event, deliveries[2].Event)
		assertDelivered(t, []model.Event{deliveries[0].Event, deliveries[2].Event, deliveries[3].Event}, set)
		final, finalGeneration, err := cp.Load(f.ctx, binding.ID)
		if err != nil || finalGeneration != 4 || final.Position.Revision.String() != "6" || final.Position.Index.String() != "2" {
			t.Fatalf("recovered checkpoint=%+v gen=%d err=%v", final, finalGeneration, err)
		}
		f.reopen(t)
		cp = f.checkpoints(t)
		last, lastGeneration, err := cp.Load(f.ctx, binding.ID)
		if err != nil || lastGeneration != finalGeneration || !reflect.DeepEqual(last, final) {
			t.Fatalf("completed checkpoint did not survive reopen: %+v gen=%d err=%v", last, lastGeneration, err)
		}
		dispatch = &coreevents.Dispatcher{Reader: f.eventReader(), Checkpoints: cp, Active: f.lease}
		if warnings := dispatch.RunPost(f.ctx, []coreevents.Binding{binding}, set.Revision); len(warnings) != 0 || len(deliveries) != 4 {
			t.Fatalf("completed revision delivered again: warnings=%+v deliveries=%d", warnings, len(deliveries))
		}
		equalJSON(t, f.receipt(t), receipt)
		if f.meta(t).Head.String() != "7" || !reflect.DeepEqual(dependents, f.histories(t, g.dependents)) {
			t.Fatal("replay/recovery changed graph head or dependent history")
		}
		t.Logf("service->tracked ontology->Writer->%s->history->post: close r6 indexes 0,1,2; ontology@1 digest=%s; two store reopens; receipt exact; notice key=%s replayed with late=true; checkpoint generation=4 index=2; dependent history unchanged", e.name, digest, deliveries[2].Key.String())
	})
}

func TestSQLAuditFailureUsesBlockerProjectAndEmitsNoNotice(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		g := f.graph(t)
		before := f.histories(t, g.dependents)
		if !f.compiled.Effective(blockerProject).SatisfiesConditionalBlocks("failed") || f.compiled.Effective(dependentProject).SatisfiesConditionalBlocks("failed") {
			t.Fatal("own-project failure contrast fixture is ineffective")
		}
		// The admitted Service has no named Fail method. Exercise the actual
		// Writer's fail slot with the same real normalizer and tracked adapter.
		changes := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: g.blocker, Fields: map[string]any{}}}}}
		meta := model.WriteMeta{Method: "bead.fail", Project: blockerProject, IdempotencyKey: key(6)}
		outcome, err := f.writer.ApplyWithOutcome(f.ctx, writerPrincipal, changes, meta)
		if err != nil || outcome.Replayed {
			t.Fatalf("real Writer fail=%+v err=%v", outcome, err)
		}
		set := f.set(t, outcome.Commit.Revision)
		if len(set.Changes) != 1 || len(set.Events) != 1 || set.Events[0].Type != model.EventBeadFailed || set.Events[0].Details != nil {
			t.Fatalf("failure emitted a condition notice or lost ordinary event: %+v", set)
		}
		f.reopen(t)
		equalJSON(t, f.set(t, outcome.Commit.Revision), set)
		page, err := f.eventReader().Read(f.ctx, coreevents.ReadRequest{After: startPosition(f, g), Principal: readerPrincipal, Limit: 1000})
		if err != nil || len(page.Events()) != 1 || page.Events()[0].Durable.Type != model.EventBeadFailed || page.Events()[0].Durable.After["fields"].(map[string]any)["status"] != "failed" {
			t.Fatalf("failure SQL materialization=%+v err=%v", page.Events(), err)
		}
		replay, err := f.writer.ApplyWithOutcome(f.ctx, writerPrincipal, changes, meta)
		if err != nil || !replay.Replayed {
			t.Fatalf("failure replay after reopen=%+v err=%v", replay, err)
		}
		equalJSON(t, replay.Commit, outcome.Commit)
		if f.meta(t).Head.String() != "6" || !reflect.DeepEqual(before, f.histories(t, g.dependents)) {
			t.Fatal("failure suppression/replay changed dependent history or emitted another revision")
		}
		t.Log("direct real Writer fail slot: blocker-project completed/failure satisfies; dependent-project same key means success; SQL retains only bead.failed across reopen and exact receipt replay")
	})
}

func TestSQLAuditLateReceiptFailureRollsBackEveryPublishedFamily(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		g := f.graph(t)
		refs := append([]model.Ref{f.sourceRef, g.blocker}, g.dependents...)
		before := f.histories(t, refs)
		meta := f.meta(t)
		cp := f.checkpoints(t)
		state := coreevents.State{Position: startPosition(f, g), Class: coreevents.Post}
		if _, err := cp.Save(f.ctx, "rollback-post", 0, state); err != nil {
			t.Fatal(err)
		}
		counts := e.familyCounts(t, f.ctx)
		e.receiptFault(t, f.ctx, true)
		outcome, err := f.close(g)
		if err == nil || !strings.Contains(err.Error(), "audit late receipt failure") || !outcome.Commit.Revision.IsZero() || outcome.Replayed {
			t.Fatalf("late SQL failure did not abort after a real three-event close plan: outcome=%+v err=%v", outcome, err)
		}
		// The trigger only raises this error after seeing the actual persisted
		// three-event plan inside the uncommitted SQL transaction. A fresh
		// reader and a reopened store must see none of that transaction.
		for attempt := 0; attempt < 2; attempt++ {
			if !reflect.DeepEqual(f.meta(t), meta) || !reflect.DeepEqual(f.histories(t, refs), before) || !reflect.DeepEqual(e.familyCounts(t, f.ctx), counts) || f.receipt(t) != nil {
				t.Fatalf("late receipt failure leaked record/version/event/receipt/head state (reopen=%t)", attempt == 1)
			}
			page, err := f.eventReader().Read(f.ctx, coreevents.ReadRequest{After: startPosition(f, g), Principal: readerPrincipal, Limit: 1000})
			if err != nil || len(page.Events()) != 0 {
				t.Fatalf("rolled-back notices reached Reader: %+v err=%v", page.Events(), err)
			}
			got, generation, err := f.checkpoints(t).Load(f.ctx, "rollback-post")
			if err != nil || generation != 1 {
				t.Fatalf("rollback changed checkpoint: %+v gen=%d err=%v", got, generation, err)
			}
			equalJSON(t, got, state)
			if attempt == 0 {
				f.reopen(t)
			}
		}
		e.receiptFault(t, f.ctx, false)
		retry, err := f.close(g)
		if err != nil || retry.Replayed || retry.Commit.Revision.String() != "6" {
			t.Fatalf("same-key retry after rollback/reopen=%+v err=%v", retry, err)
		}
		assertClosePlan(t, f, g, f.set(t, retry.Commit.Revision), f.compiled.Digest())
		replay, err := f.close(g)
		if err != nil || !replay.Replayed {
			t.Fatalf("retry receipt was not retained: %+v err=%v", replay, err)
		}
		equalJSON(t, replay.Commit, retry.Commit)
		if f.meta(t).Head.String() != "6" {
			t.Fatal("rollback/retry consumed an extra revision")
		}
		t.Log("real SQL receipt-stage trigger observed ordinary+two notices, then aborted: records, versions, predicates, event log, receipts, head and checkpoint unchanged before/after reopen; same-key retry commits exactly r6")
	})
}

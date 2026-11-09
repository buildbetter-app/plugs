package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

var anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type fixture struct {
	svc    *Service
	store  *memory.Store
	writer *pipeline.Writer
	lease  driver.ActiveLease
	now    time.Time
}

func setup(t *testing.T, options ...pipeline.Option) *fixture {
	t.Helper()
	f := &fixture{store: memory.New(), now: time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)}
	t.Cleanup(func() { _ = f.store.Close() })
	var err error
	f.lease, err = f.store.Lease(context.Background(), "service-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.lease.Release(context.Background()) })
	o := NewOntology(nil)
	opts := []pipeline.Option{pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return f.now })}
	opts = append(opts, options...)
	f.writer = pipeline.New(f.store, f.lease, opts...)
	f.svc = New(f.store, f.lease, f.writer, WithOntology(o), WithClock(func() time.Time { return f.now }))
	return f
}

func meta(t *testing.T) model.WriteMeta {
	t.Helper()
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return model.WriteMeta{Project: "local", Agent: "test-agent", AgentLabel: "test-agent", IdempotencyKey: hex.EncodeToString(b[:])}
}

func create(t *testing.T, f *fixture, title string) (model.Ref, model.Commit) {
	t.Helper()
	v, err := f.svc.Create(context.Background(), anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": title}}, meta(t))
	if err != nil {
		t.Fatalf("create failed: %#v", err)
	}
	if len(v.Results) != 1 || v.Results[0].Ref.ID == "" {
		t.Fatalf("missing create result: %+v", v)
	}
	return v.Results[0].Ref, v
}

func wantCode(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != code {
		t.Fatalf("got %T %v, want %s", err, err, code)
	}
}

func TestMemoryPipelineServiceLifecycleAndHistory(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	empty, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
	if err != nil || len(empty.Items) != 0 {
		t.Fatalf("empty ready: %+v %v", empty, err)
	}
	id, created := create(t, f, "Original title")
	ready, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
	if err != nil || len(ready.Items) != 1 || ready.Items[0].ID != id.ID {
		t.Fatalf("ready: %+v %v", ready, err)
	}
	claimMeta := meta(t)
	claimMeta.Return = "minimal"
	claimed, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, claimMeta)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.LeaseTTL != 5*time.Minute || !claimed.LeaseExpiresAt.Equal(f.now.Add(5*time.Minute)) {
		t.Fatalf("claim lease: %+v", claimed)
	}
	read, err := f.svc.Get(ctx, anonymous, GetRequest{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	if read.Lease == nil || read.Lease.Epoch.String() != model.Natural64(f.lease.Epoch()).String() || read.Lease.TTL != 5*time.Minute || read.Lease.Agent != "test-agent" {
		t.Fatalf("durable lease: %+v", read.Lease)
	}
	replayed, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, claimMeta)
	if err != nil || replayed.Commit.Revision.String() != claimed.Commit.Revision.String() || !replayed.LeaseExpiresAt.Equal(claimed.LeaseExpiresAt) {
		t.Fatalf("claim replay: %+v %v", replayed, err)
	}
	expect := claimed.Commit.Revision
	updated, err := f.svc.Update(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"title": "Updated title"}, Expect: []model.Precondition{{Ref: id, Revision: &expect}}}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.Update(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"title": "stale"}, Expect: []model.Precondition{{Ref: id, Revision: &expect}}}, meta(t))
	wantCode(t, err, "precondition_failed")
	commented, err := f.svc.Comment(ctx, anonymous, CommentRequest{ID: id, Body: "Evidence recorded"}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	closeMeta := meta(t)
	closed, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: id, Reason: "Completed with evidence"}, closeMeta)
	if err != nil {
		t.Fatal(err)
	}
	closedReplay, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: id, Reason: "Completed with evidence"}, closeMeta)
	if err != nil || closedReplay.Revision.String() != closed.Revision.String() {
		t.Fatalf("close replay: %+v %v", closedReplay, err)
	}
	for _, v := range []model.Commit{created, claimed.Commit, updated, commented, closed} {
		if v.Revision.IsZero() {
			t.Fatal("write returned zero revision")
		}
	}
	current, err := f.svc.Get(ctx, anonymous, GetRequest{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := f.svc.ontology.effective("local").SlotStatus("issue", "close")
	if current.Bead.Status != target || current.Bead.Fields["title"] != "Updated title" {
		t.Fatalf("current: %+v", current)
	}
	history, err := f.svc.History(ctx, anonymous, HistoryRequest{Project: "local", AsOf: AsOf{Revision: created.Revision}, Filter: &Filter{Resources: []string{id.ID}}})
	if err != nil {
		t.Fatal(err)
	}
	initial, _ := f.svc.ontology.effective("local").InitialStatus("issue")
	if len(history.Items) != 1 || history.Items[0].Status != initial || history.Items[0].Fields["title"] != "Original title" {
		t.Fatalf("history: %+v", history)
	}
	ready, err = f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
	if err != nil || len(ready.Items) != 0 {
		t.Fatalf("closed ready: %+v %v", ready, err)
	}
	m, _ := f.store.Meta(ctx)
	r, err := f.store.Read(ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{id})
	if err != nil || len(rows) != 1 {
		t.Fatal(err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Comments) != 2 || doc.Comments[1].Body != "Completed with evidence" {
		t.Fatalf("comments not committed atomically: %+v", doc.Comments)
	}
	changes, err := r.Changesets(ctx, nil, 100)
	if err != nil || len(changes) != 5 {
		t.Fatalf("wanted five commits, got %d: %v", len(changes), err)
	}
}

func TestNamedLifecycleSlotsReplayRefuseIllegalMovesAndPreserveHistory(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id, created := create(t, f, "slot lifecycle")

	write := func(method, reason string) port.WriteOutcome {
		t.Helper()
		m := meta(t)
		var outcome port.WriteOutcome
		var err error
		switch method {
		case "bead.defer":
			outcome, err = f.svc.DeferWithOutcome(ctx, anonymous, DeferRequest{ID: id, Reason: reason}, m)
		case "bead.reopen":
			outcome, err = f.svc.ReopenWithOutcome(ctx, anonymous, ReopenRequest{ID: id, Reason: reason}, m)
		case "bead.claim":
			outcome, err = f.svc.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: id}, m)
		case "bead.fail":
			outcome, err = f.svc.FailWithOutcome(ctx, anonymous, FailRequest{ID: id, Reason: reason}, m)
		case "bead.cancel":
			outcome, err = f.svc.CancelWithOutcome(ctx, anonymous, CancelRequest{ID: id, Reason: reason}, m)
		case "bead.close":
			outcome, err = f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id, Reason: reason}, m)
		default:
			t.Fatalf("unknown lifecycle method %q", method)
		}
		if err != nil {
			t.Fatalf("%s failed: %v", method, err)
		}
		if outcome.Replayed {
			t.Fatalf("fresh %s was marked replay", method)
		}
		return outcome
	}

	steps := []struct {
		method string
		reason string
		status string
	}{
		{"bead.defer", "waiting", "backlog"},
		{"bead.reopen", "ready again", "todo"},
		{"bead.claim", "", "in_progress"},
		{"bead.fail", "failed validation", "failed"},
		{"bead.reopen", "retrying", "todo"},
		{"bead.cancel", "no longer needed", "canceled"},
		{"bead.reopen", "reconsidered", "todo"},
		{"bead.claim", "", "in_progress"},
		{"bead.close", "completed", "done"},
		{"bead.reopen", "follow-up", "todo"},
	}
	for i, step := range steps {
		outcome := write(step.method, step.reason)
		wantRevision := model.Natural64(uint64(i + 2))
		if outcome.Commit.Revision.String() != wantRevision.String() || len(outcome.Commit.Results) == 0 {
			t.Fatalf("%s commit=%+v want revision %s", step.method, outcome.Commit, wantRevision)
		}
		got, err := f.svc.Get(ctx, anonymous, GetRequest{ID: id})
		if err != nil || got.Bead.Status != step.status || got.Bead.Revision.String() != wantRevision.String() {
			t.Fatalf("%s state=%+v err=%v", step.method, got, err)
		}
	}

	// A second key for the same lifecycle request is a durable replay, not a
	// fresh transition; it must not append another comment or history row.
	replayKey := meta(t)
	first, err := f.svc.FailWithOutcome(ctx, anonymous, FailRequest{ID: id, Reason: "terminal failure"}, replayKey)
	if err != nil {
		t.Fatal(err)
	}
	afterFirst, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := f.svc.FailWithOutcome(ctx, anonymous, FailRequest{ID: id, Reason: "terminal failure"}, replayKey)
	if err != nil || !replay.Replayed || !bytes.Equal(mustJSON(t, first.Commit), mustJSON(t, replay.Commit)) {
		t.Fatalf("keyed lifecycle replay changed its result: first=%+v replay=%+v err=%v", first, replay, err)
	}
	afterReplay, err := f.store.Meta(ctx)
	if err != nil || afterReplay.Head.String() != afterFirst.Head.String() {
		t.Fatalf("keyed lifecycle replay advanced history: before=%+v after=%+v err=%v", afterFirst, afterReplay, err)
	}

	// Fail targets failed, so repeating it while already in failed is an illegal
	// transition and leaves the failed request without a receipt or revision.
	before, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.FailWithOutcome(ctx, anonymous, FailRequest{ID: id}, meta(t))
	wantCode(t, err, "invalid_params")
	after, err := f.store.Meta(ctx)
	if err != nil || after.Head.String() != before.Head.String() {
		t.Fatalf("illegal reopen changed head: before=%+v after=%+v err=%v", before, after, err)
	}

	// Every successful slot is represented in pinned history, including the
	// initial create and the final failed image.
	history, err := f.svc.History(ctx, anonymous, HistoryRequest{Project: "local", AsOf: AsOf{Revision: afterFirst.Head}, Filter: &Filter{Resources: []string{id.ID}}, Limit: 100})
	if err != nil || len(history.Items) != 1 || history.Items[0].Status != "failed" {
		t.Fatalf("lifecycle history=%+v err=%v", history, err)
	}
	initial, err := f.svc.History(ctx, anonymous, HistoryRequest{Project: "local", AsOf: AsOf{Revision: created.Revision}, Filter: &Filter{Resources: []string{id.ID}}})
	if err != nil || len(initial.Items) != 1 || initial.Items[0].Status != "todo" {
		t.Fatalf("initial lifecycle history=%+v err=%v", initial, err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReadinessAndSlotGuardUseLiveBlockers(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	blocker, _ := create(t, f, "Blocker")
	dependent, _ := create(t, f, "Dependent")
	m := meta(t)
	m.Method = "tx.apply"
	_, err := f.writer.Apply(ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: dependent, To: blocker, LinkKind: "blocks"}}}}, m)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
	if err != nil || len(ready.Items) != 1 || ready.Items[0].ID != blocker.ID {
		t.Fatalf("blocked readiness: %+v %v", ready, err)
	}
	before, _ := f.store.Meta(ctx)
	_, err = f.svc.Claim(ctx, anonymous, ClaimRequest{ID: dependent}, meta(t))
	wantCode(t, err, "ontology_guard_failed")
	_, err = f.svc.Close(ctx, anonymous, CloseRequest{ID: dependent}, meta(t))
	wantCode(t, err, "ontology_guard_failed")
	target, _ := f.svc.ontology.effective("local").SlotStatus("issue", "close")
	_, err = f.svc.Update(ctx, anonymous, UpdateRequest{ID: dependent, Fields: map[string]any{"status": target}}, meta(t))
	wantCode(t, err, "ontology_use_slot_verb")
	after, _ := f.store.Meta(ctx)
	if before.Head.String() != after.Head.String() {
		t.Fatal("guard refusal advanced head")
	}
	_, err = f.svc.Close(ctx, anonymous, CloseRequest{ID: blocker}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	ready, err = f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
	if err != nil || len(ready.Items) != 1 || ready.Items[0].ID != dependent.ID {
		t.Fatalf("unblocked readiness: %+v %v", ready, err)
	}
}

func TestConditionalBlockerFailureUnlocksReadyAndClose(t *testing.T) {
	for _, tc := range []struct {
		method string
		ready  bool
	}{{"bead.fail", true}, {"bead.close", false}, {"bead.cancel", false}} {
		t.Run(tc.method, func(t *testing.T) {
			f := setup(t)
			ctx := context.Background()
			blocker, _ := create(t, f, "Conditional blocker")
			dependent, _ := create(t, f, "Dependent")
			m := meta(t)
			m.Method = "tx.apply"
			_, err := f.writer.Apply(ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: dependent, To: blocker, LinkKind: "conditional-blocks"}}}}, m)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.svc.Close(ctx, anonymous, CloseRequest{ID: dependent}, meta(t))
			wantCode(t, err, "ontology_guard_failed")
			m = meta(t)
			m.Method = tc.method
			_, err = f.writer.Apply(ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: blocker, Fields: map[string]any{}}}}}, m)
			if err != nil {
				t.Fatal(err)
			}
			page, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
			wantCount := 0
			if tc.ready {
				wantCount = 1
			}
			if err != nil || len(page.Items) != wantCount || (len(page.Items) == 1 && page.Items[0].ID != dependent.ID) {
				t.Fatalf("conditional readiness: %+v %v", page, err)
			}
			before, _ := f.store.Meta(ctx)
			_, err = f.svc.Close(ctx, anonymous, CloseRequest{ID: dependent}, meta(t))
			if tc.ready {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				wantCode(t, err, "ontology_guard_failed")
				after, _ := f.store.Meta(ctx)
				if before.Head.String() != after.Head.String() {
					t.Fatal("conditional close refusal advanced head")
				}
			}
		})
	}
}

func TestConditionalBlockerUsesBlockerProjectOutcome(t *testing.T) {
	f := setup(t)
	doc := ontology.DefaultDocument()
	base := ontology.Default()
	closeStatus, _ := base.SlotStatus("issue", ontology.Close)
	failStatus, _ := base.SlotStatus("issue", ontology.Fail)
	status := doc.Statuses[closeStatus]
	status.Outcome = ontology.Failure
	doc.Statuses[closeStatus] = status
	status = doc.Statuses[failStatus]
	status.Outcome = ontology.Success
	doc.Statuses[failStatus] = status
	workflow := doc.Workflows["issue"]
	workflow.Slots[ontology.Close], workflow.Slots[ontology.Fail] = failStatus, closeStatus
	doc.Workflows["issue"] = workflow
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	o := NewOntology(func(project string) ontology.Effective {
		if project == "upstream" {
			return snapshot.Effective(project)
		}
		return base
	})
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(o))
	f.svc = New(f.store, f.lease, f.writer, WithOntology(o))
	ctx := context.Background()
	m := meta(t)
	m.Project = "upstream"
	created, err := f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": "Other project"}}, m)
	if err != nil {
		t.Fatal(err)
	}
	blocker := created.Results[0].Ref
	dependent, _ := create(t, f, "Local dependent")
	m = meta(t)
	m.Method = "tx.apply"
	_, err = f.writer.Apply(ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: dependent, To: blocker, LinkKind: "conditional-blocks"}}}}, m)
	if err != nil {
		t.Fatal(err)
	}
	m = meta(t)
	m.Project = "upstream"
	m.Method = "bead.fail"
	if _, err := f.writer.Apply(ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: blocker, Fields: map[string]any{}}}}}, m); err != nil {
		t.Fatal(err)
	}
	page, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != dependent.ID {
		t.Fatalf("used dependent's success outcome instead of blocker's failure outcome: %+v %v", page, err)
	}
}

type failedPost struct{ calls int }

func (p *failedPost) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls++
	return errors.New("unavailable post subscriber")
}

func TestCommittedWarningAndIdempotentReplay(t *testing.T) {
	post := &failedPost{}
	f := setup(t, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "test-post", Subscriber: post}))
	ctx := context.Background()
	m := meta(t)
	req := CreateRequest{Type: "issue", Fields: map[string]any{"title": "Keep committed result"}}
	a, err := f.svc.Create(ctx, anonymous, req, m)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Warnings) != 1 || a.Warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("lost warning: %+v", a)
	}
	b, err := f.svc.Create(ctx, anonymous, req, m)
	if err != nil {
		t.Fatal(err)
	}
	if b.Revision.String() != a.Revision.String() || len(b.Warnings) != 1 || post.calls != 1 {
		t.Fatalf("replay reran post or lost result: %+v calls %d", b, post.calls)
	}
	req.Fields = map[string]any{"title": "Different request"}
	_, err = f.svc.Create(ctx, anonymous, req, m)
	wantCode(t, err, "idempotency_key_reused")
}

func TestNamedWriteOutcomesUseActualReceipts(t *testing.T) {
	for _, method := range []string{"create", "update", "claim", "comment", "close"} {
		t.Run(method, func(t *testing.T) {
			post := &failedPost{}
			f := setup(t, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "outcome-post", Subscriber: post}))
			ctx := context.Background()
			id, _ := create(t, f, "Original")
			key := meta(t)
			call := func() (port.WriteOutcome, error) {
				switch method {
				case "create":
					return f.svc.CreateWithOutcome(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": "Outcome"}}, key)
				case "update":
					return f.svc.UpdateWithOutcome(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"title": "Updated"}}, key)
				case "claim":
					return f.svc.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: id}, key)
				case "comment":
					return f.svc.CommentWithOutcome(ctx, anonymous, CommentRequest{ID: id, Body: "Outcome"}, key)
				default:
					return f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id, Reason: "Outcome"}, key)
				}
			}
			first, err := call()
			if err != nil || first.Replayed || len(first.Commit.Warnings) != 1 {
				t.Fatalf("first finalized outcome: %+v %v", first, err)
			}
			// Advance the head, then replay the actual original receipt. A
			// revision comparison cannot distinguish these two executions.
			create(t, f, "Later unrelated write")
			before, _ := f.store.Meta(ctx)
			calls := post.calls
			replay, err := call()
			after, _ := f.store.Meta(ctx)
			a, marshalErr := json.Marshal(first.Commit)
			b, replayMarshalErr := json.Marshal(replay.Commit)
			if err != nil || marshalErr != nil || replayMarshalErr != nil || !replay.Replayed || !bytes.Equal(a, b) || post.calls != calls || before.Head.String() != after.Head.String() {
				t.Fatalf("stored result or replay outcome changed: first=%s replay=%s outcome=%+v errors=%v/%v/%v posts=%d/%d", a, b, replay, err, marshalErr, replayMarshalErr, calls, post.calls)
			}
		})
	}
}

func TestDependencyMethodsLowerToCanonicalGraphOperations(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	from, _ := create(t, f, "Dependent")
	to, _ := create(t, f, "Blocker")
	req := LinkRequest{From: from, To: to, Kind: "blocks"}
	firstKey := meta(t)
	first, err := f.svc.LinkWithOutcome(ctx, anonymous, req, firstKey)
	if err != nil || first.Replayed || first.Commit.Revision.String() != "3" {
		t.Fatalf("link outcome=%+v err=%v", first, err)
	}
	doc, err := pipeline.DecodeDocument(first.Commit.Results[0].Record)
	if err != nil || len(doc.Links) != 1 || doc.Links[0].To.ID != to.ID || doc.Links[0].Kind != "blocks" {
		t.Fatalf("link did not lower to graph edge: %+v err=%v", doc.Links, err)
	}

	replay, err := f.svc.LinkWithOutcome(ctx, anonymous, req, firstKey)
	if err != nil || !replay.Replayed || !bytes.Equal(replay.Commit.Results[0].Record, first.Commit.Results[0].Record) {
		t.Fatalf("link replay=%+v err=%v", replay, err)
	}
	duplicate, err := f.svc.LinkWithOutcome(ctx, anonymous, req, meta(t))
	if err != nil || duplicate.Replayed || duplicate.Commit.Revision.String() != "3" {
		t.Fatalf("duplicate link should be a fresh no-op: %+v err=%v", duplicate, err)
	}

	unlinked, err := f.svc.UnlinkWithOutcome(ctx, anonymous, req, meta(t))
	if err != nil || unlinked.Replayed || unlinked.Commit.Revision.String() != "4" {
		t.Fatalf("unlink outcome=%+v err=%v", unlinked, err)
	}
	doc, err = pipeline.DecodeDocument(unlinked.Commit.Results[0].Record)
	if err != nil || len(doc.Links) != 0 {
		t.Fatalf("unlink did not remove graph edge: %+v err=%v", doc.Links, err)
	}
	if _, err := f.svc.LinkWithOutcome(ctx, anonymous, LinkRequest{From: from, To: to}, meta(t)); err == nil {
		t.Fatal("empty link kind was accepted")
	} else {
		wantCode(t, err, "invalid_params")
	}
}

func TestFreshNoopAndDryRunOutcomesAreNotReplayed(t *testing.T) {
	post := &failedPost{}
	f := setup(t, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "outcome-post", Subscriber: post}))
	ctx := context.Background()
	id, original := create(t, f, "Unchanged")
	_, later := create(t, f, "Later")
	key := meta(t)
	req := UpdateRequest{ID: id, Fields: map[string]any{"title": "Unchanged"}}
	calls := post.calls
	fresh, err := f.svc.UpdateWithOutcome(ctx, anonymous, req, key)
	if err != nil || fresh.Replayed || fresh.Commit.Revision.String() != later.Revision.String() || fresh.Commit.Results[0].Revision.String() != original.Revision.String() || len(fresh.Commit.Warnings) != 0 || post.calls != calls {
		t.Fatalf("fresh no-op confused with replay: %+v %v", fresh, err)
	}
	replay, err := f.svc.UpdateWithOutcome(ctx, anonymous, req, key)
	if err != nil || !replay.Replayed || replay.Commit.Revision.String() != fresh.Commit.Revision.String() {
		t.Fatalf("no-op receipt replay: %+v %v", replay, err)
	}
	claimed, err := f.svc.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: id}, meta(t))
	if err != nil || claimed.Replayed {
		t.Fatalf("first claim: %+v %v", claimed, err)
	}
	f.now = f.now.Add(time.Minute)
	key = meta(t)
	calls = post.calls
	fresh, err = f.svc.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: id}, key)
	if err != nil || fresh.Replayed || !fresh.Commit.LeaseExpiresAt.Equal(*claimed.Commit.LeaseExpiresAt) || fresh.Commit.LeaseTtlMs.String() != claimed.Commit.LeaseTtlMs.String() || post.calls != calls {
		t.Fatalf("fresh same-holder claim changed durable lease or replay flag: %+v %v", fresh, err)
	}
	f.now = f.now.Add(6 * time.Minute)
	replay, err = f.svc.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: id}, key)
	if err != nil || !replay.Replayed || !replay.Commit.LeaseExpiresAt.Equal(*fresh.Commit.LeaseExpiresAt) || post.calls != calls {
		t.Fatalf("claim replay did not use original receipt: %+v %v", replay, err)
	}
	before, _ := f.store.Meta(ctx)
	key = meta(t)
	key.DryRun = true
	for range 2 {
		dry, err := f.svc.CreateWithOutcome(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": "Dry run"}}, key)
		if err != nil || dry.Replayed || !dry.Commit.WouldCommit || post.calls != calls {
			t.Fatalf("dry run became a replay or published posts: %+v %v", dry, err)
		}
	}
	after, _ := f.store.Meta(ctx)
	if before.Head.String() != after.Head.String() {
		t.Fatal("dry runs advanced storage head")
	}
}

type legacyOnlyWriter struct {
	model.Writer
	calls int
}

func (w *legacyOnlyWriter) Apply(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (model.Commit, error) {
	w.calls++
	return w.Writer.Apply(ctx, by, cs, meta)
}

func TestOutcomeMethodsRequireActualOutcomeWriterBeforeWriting(t *testing.T) {
	f := setup(t)
	legacy := &legacyOnlyWriter{Writer: f.writer}
	f.svc.writer = legacy
	req := CreateRequest{Type: "issue", Fields: map[string]any{"title": "Legacy"}}
	_, err := f.svc.CreateWithOutcome(context.Background(), anonymous, req, meta(t))
	wantCode(t, err, "internal_error")
	if legacy.calls != 0 {
		t.Fatal("outcome refusal happened after a legacy write")
	}
	if _, err := f.svc.Create(context.Background(), anonymous, req, meta(t)); err != nil || legacy.calls != 1 {
		t.Fatalf("legacy writer compatibility: %v calls=%d", err, legacy.calls)
	}
}

func TestPlainTransactionAndDependencyMethodsUseLegacyWriter(t *testing.T) {
	f := setup(t)
	from, _ := create(t, f, "Dependent")
	to, _ := create(t, f, "Blocker")
	legacy := &legacyOnlyWriter{Writer: f.writer}
	f.svc.writer = legacy
	if _, err := f.svc.Link(context.Background(), anonymous, LinkRequest{From: from, To: to, Kind: "relates-to"}, meta(t)); err != nil {
		t.Fatalf("legacy link: %v", err)
	}
	if _, err := f.svc.Apply(context.Background(), anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: from, Fields: map[string]any{"title": "updated"}}}}}, meta(t)); err != nil {
		t.Fatalf("legacy tx.apply: %v", err)
	}
	if _, err := f.svc.Unlink(context.Background(), anonymous, LinkRequest{From: from, To: to, Kind: "relates-to"}, meta(t)); err != nil {
		t.Fatalf("legacy unlink: %v", err)
	}
	if legacy.calls != 3 {
		t.Fatalf("legacy writer calls=%d, want 3", legacy.calls)
	}
}

func TestAlreadyClosedCloseUsesGuardedNoopReceipt(t *testing.T) {
	post := &failedPost{}
	f := setup(t, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "close-post", Subscriber: post}))
	ctx := context.Background()
	id, _ := create(t, f, "Close settlement")
	closed, err := f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	calls := post.calls
	key := meta(t)
	fresh, err := f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id}, key)
	if err != nil || fresh.Replayed || fresh.Commit.Revision.String() != closed.Commit.Revision.String() || fresh.Commit.Results[0].Revision.String() != closed.Commit.Results[0].Revision.String() || len(fresh.Commit.Warnings) != 0 || post.calls != calls {
		t.Fatalf("fresh close settlement did not retain no-op semantics: %+v %v posts=%d/%d", fresh, err, calls, post.calls)
	}
	replay, err := f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id}, key)
	a, _ := json.Marshal(fresh.Commit)
	b, _ := json.Marshal(replay.Commit)
	if err != nil || !replay.Replayed || !bytes.Equal(a, b) || post.calls != calls {
		t.Fatalf("close settlement replay: %+v %v", replay, err)
	}
	withReason, err := f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id, Reason: "Additional evidence"}, meta(t))
	if err != nil || withReason.Replayed || withReason.Commit.Revision.String() == closed.Commit.Revision.String() || post.calls != calls+1 {
		t.Fatalf("reason comment was lost as a no-op: %+v %v", withReason, err)
	}
	doc, err := pipeline.DecodeDocument(withReason.Commit.Results[0].Record)
	if err != nil || len(doc.Comments) != 1 || doc.Comments[0].Body != "Additional evidence" {
		t.Fatalf("reason comment was not committed: %+v %v", doc, err)
	}
	got, err := f.svc.Get(ctx, anonymous, GetRequest{ID: id})
	if err != nil || len(got.Bead.Fields) == 0 || got.Lease != nil {
		t.Fatalf("settled bead: %+v %v", got, err)
	}
	stale := closed.Commit.Revision
	_, err = f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id, Expect: []model.Precondition{{Ref: id, Revision: &stale}}}, meta(t))
	wantCode(t, err, "precondition_failed")
}

func TestAlreadyClosedCloseRechecksLiveGuards(t *testing.T) {
	for _, guard := range []string{"template", "blocker"} {
		t.Run(guard, func(t *testing.T) {
			f := setup(t)
			ctx := context.Background()
			id, _ := create(t, f, "Already closed")
			if _, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: id}, meta(t)); err != nil {
				t.Fatal(err)
			}
			if guard == "template" {
				if _, err := f.svc.Update(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"template": true}}, meta(t)); err != nil {
					t.Fatal(err)
				}
			} else {
				blocker, _ := create(t, f, "New blocker")
				m := meta(t)
				m.Method = "tx.apply"
				if _, err := f.writer.Apply(ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: id, To: blocker, LinkKind: "blocks"}}}}, m); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := f.store.Meta(ctx)
			_, err := f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id}, meta(t))
			wantCode(t, err, "ontology_guard_failed")
			after, _ := f.store.Meta(ctx)
			if before.Head.String() != after.Head.String() {
				t.Fatal("refused close settlement advanced head")
			}
		})
	}
}

type settledCloseMutator struct{ status bool }

func (m settledCloseMutator) Mutate(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
	if p.Meta.Method != "bead.close" || !hasDocument(p.Changes[0].Before) {
		return nil, nil
	}
	doc, err := pipeline.DecodeDocument(p.Changes[0].Before)
	if err != nil {
		return nil, err
	}
	target, _ := ontology.Default().SlotStatus(doc.Type, ontology.Close)
	if doc.Fields["status"] != target {
		return nil, nil
	}
	path, raw := "fields.description", json.RawMessage(`"Settlement synchronized"`)
	if m.status {
		path = "fields.status"
		raw, _ = json.Marshal(target)
	}
	return []port.Patch{{Resource: p.Changes[0].Resource, Paths: map[string]json.RawMessage{path: raw}}}, nil
}

func TestAlreadyClosedCloseValidatesMutatorsAndOriginalPatch(t *testing.T) {
	for _, status := range []bool{false, true} {
		t.Run(map[bool]string{false: "real-field-change", true: "same-status-touch"}[status], func(t *testing.T) {
			path := "fields.description"
			if status {
				path = "fields.status"
			}
			f := setup(t, pipeline.WithMutators(pipeline.MutatorBinding{ID: "close-sync", Owns: []string{path}, Mutator: settledCloseMutator{status: status}}))
			ctx := context.Background()
			id, _ := create(t, f, "Close callbacks")
			first, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: id}, meta(t))
			if err != nil {
				t.Fatal(err)
			}
			second, err := f.svc.CloseWithOutcome(ctx, anonymous, CloseRequest{ID: id}, meta(t))
			if status {
				wantCode(t, err, "ontology_use_slot_verb")
				return
			}
			if err != nil || second.Replayed || second.Commit.Revision.String() == first.Revision.String() {
				t.Fatalf("real close mutator change was lost: %+v %v", second, err)
			}
			got, err := f.svc.Get(ctx, anonymous, GetRequest{ID: id})
			if err != nil || got.Bead.Fields["description"] != "Settlement synchronized" {
				t.Fatalf("mutator change missing: %+v %v", got, err)
			}
		})
	}
	f := setup(t)
	id, _ := create(t, f, "Original patch")
	if _, err := f.svc.Close(context.Background(), anonymous, CloseRequest{ID: id}, meta(t)); err != nil {
		t.Fatal(err)
	}
	m := meta(t)
	m.Method = "bead.close"
	_, err := f.writer.Apply(context.Background(), anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: id, Fields: map[string]any{"title": "Not an empty slot patch"}}}}}, m)
	wantCode(t, err, "invalid_params")
}

func TestLeaseOwnershipExpiryAndLostStorageLease(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id, _ := create(t, f, "Leased")
	_, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	foreign := meta(t)
	foreign.Agent = "other-agent"
	foreign.AgentLabel = "other-agent"
	_, err = f.svc.Update(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"title": "foreign edit"}}, foreign)
	wantCode(t, err, "lease_not_held")
	f.now = f.now.Add(6 * time.Minute)
	_, err = f.svc.Update(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"title": "expired edit"}}, meta(t))
	wantCode(t, err, "lease_expired")
	if err := f.lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.Get(ctx, anonymous, GetRequest{ID: id})
	wantCode(t, err, "busy")
	_, err = f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
	wantCode(t, err, "busy")
}

func TestSameActorClaimAndSameValueUpdateDoNotAdvanceHead(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id, _ := create(t, f, "Unchanged")
	before, _ := f.store.Meta(ctx)
	_, err := f.svc.Update(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"title": "Unchanged"}}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	after, _ := f.store.Meta(ctx)
	if before.Head.String() != after.Head.String() {
		t.Fatal("same-value update advanced head")
	}
	claimed, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(time.Minute)
	key := meta(t)
	reclaimed, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, key)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.Commit.Revision.String() != claimed.Commit.Revision.String() || !reclaimed.LeaseExpiresAt.Equal(claimed.LeaseExpiresAt) || reclaimed.LeaseTTL != claimed.LeaseTTL {
		t.Fatalf("same-actor claim changed commit or lease: before=%+v after=%+v", claimed, reclaimed)
	}
	replayed, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, key)
	if err != nil || replayed.Commit.Revision.String() != reclaimed.Commit.Revision.String() {
		t.Fatalf("no-op claim replay: %+v %v", replayed, err)
	}
}

type claimMutator struct{ status bool }

func (m claimMutator) Mutate(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
	if !p.SameClaim {
		return nil, nil
	}
	path, raw := "fields.description", json.RawMessage(`"Synchronized"`)
	if m.status {
		doc, err := pipeline.DecodeDocument(p.Changes[0].After)
		if err != nil {
			return nil, err
		}
		path = "fields.status"
		raw, _ = json.Marshal(doc.Fields["status"])
	}
	return []port.Patch{{Resource: p.Changes[0].Resource, Paths: map[string]json.RawMessage{path: raw}}}, nil
}

func TestSameClaimStillValidatesMutatorsAndPreservesLease(t *testing.T) {
	for _, status := range []bool{false, true} {
		t.Run(map[bool]string{false: "real-field-change", true: "same-status-touch"}[status], func(t *testing.T) {
			path := "fields.description"
			if status {
				path = "fields.status"
			}
			f := setup(t, pipeline.WithMutators(pipeline.MutatorBinding{ID: "claim-sync", Owns: []string{path}, Mutator: claimMutator{status: status}}))
			ctx := context.Background()
			id, _ := create(t, f, "Claim callbacks")
			first, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, meta(t))
			if err != nil {
				t.Fatal(err)
			}
			f.now = f.now.Add(time.Minute)
			second, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, meta(t))
			if status {
				wantCode(t, err, "ontology_use_slot_verb")
				return
			}
			if err != nil || second.Commit.Revision.String() == first.Commit.Revision.String() || !second.LeaseExpiresAt.Equal(first.LeaseExpiresAt) {
				t.Fatalf("mutator write or durable lease: %+v %v", second, err)
			}
			got, err := f.svc.Get(ctx, anonymous, GetRequest{ID: id})
			if err != nil || got.Bead.Fields["description"] != "Synchronized" {
				t.Fatalf("mutator change missing: %+v %v", got, err)
			}
		})
	}
}

func TestUnsupportedReadsAndMalformedParentRefuseWithoutPartialSuccess(t *testing.T) {
	next := 0
	f := setup(t, pipeline.WithIDGenerator(func() (string, error) {
		ids := []string{"pl-a", "pl-b", "pl-c"}
		id := ids[next]
		next++
		return id, nil
	}))
	ctx := context.Background()
	create(t, f, "One")
	_, _ = create(t, f, "Two")
	first, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1})
	if err != nil || len(first.Items) != 1 || first.Next == nil {
		t.Fatalf("first page: %+v %v", first, err)
	}
	_, inserted := create(t, f, "Later insert")
	second, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1, Position: first.Next})
	if err != nil || len(second.Items) != 1 || second.Next == nil || second.Items[0].ID != "pl-b" || second.AsOf.Revision.String() != inserted.Revision.String() {
		t.Fatalf("live second page: %+v %v", second, err)
	}
	third, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1, Position: second.Next})
	if err != nil || len(third.Items) != 1 || third.Items[0].ID != "pl-c" || third.Next != nil {
		t.Fatalf("later insert missing from live page: %+v %v", third, err)
	}
	_, err = f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Cursor: "unbound"})
	wantCode(t, err, "cursor_invalid")
	_, err = f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Filter: &Filter{Query: "type=issue"}})
	wantCode(t, err, "invalid_params")
	_, err = f.svc.Ready(ctx, anonymous, ReadyRequest{})
	wantCode(t, err, "project_required")
	parentAlias := model.RefByKey("unresolved-parent")
	_, err = f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{}, Parent: &parentAlias}, meta(t))
	wantCode(t, err, "invalid_params")
	_, err = f.svc.History(ctx, anonymous, HistoryRequest{Project: "local", AsOf: AsOf{Revision: model.Natural64(1), Incarnation: "different"}})
	wantCode(t, err, "incarnation_changed")
}

func TestReadyPriorityThenIDAndPinnedHistoryPages(t *testing.T) {
	ids := []string{"pl-z", "pl-b", "pl-a", "pl-template", "pl-claimed", "pl-closed"}
	next := 0
	f := setup(t, pipeline.WithIDGenerator(func() (string, error) {
		id := ids[next]
		next++
		return id, nil
	}))
	ctx := context.Background()
	for i, priority := range []int{0, 2, 2, 0, 0, 0} {
		fields := map[string]any{"title": ids[i], "priority": priority}
		if i == 3 {
			fields["template"] = true
		}
		if _, err := f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: fields}, meta(t)); err != nil {
			t.Fatalf("create %s: %#v", ids[i], err)
		}
	}
	if _, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: model.Ref{ID: "pl-claimed"}}, meta(t)); err != nil {
		t.Fatal(err)
	}
	closed, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: model.Ref{ID: "pl-closed"}}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local"})
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, row := range page.Items {
		got = append(got, row.ID)
	}
	if !slices.Equal(got, []string{"pl-z", "pl-a", "pl-b"}) {
		t.Fatalf("priority/ID order or exclusion: %v", got)
	}
	first, err := f.svc.History(ctx, anonymous, HistoryRequest{Project: "local", AsOf: AsOf{Revision: closed.Revision}, Limit: 2})
	if err != nil || len(first.Items) != 2 || first.Next == nil {
		t.Fatalf("history first page: %+v %v", first, err)
	}
	_, err = f.svc.History(ctx, anonymous, HistoryRequest{Project: "local", AsOf: AsOf{Revision: model.Natural64(1)}, Position: first.Next})
	wantCode(t, err, "cursor_invalid")
	_, err = f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Position: &PagePosition{Offset: -1, AsOf: page.AsOf}})
	wantCode(t, err, "cursor_invalid")
	second, err := f.svc.History(ctx, anonymous, HistoryRequest{Project: "local", AsOf: first.AsOf, Limit: 2, Position: first.Next})
	if err != nil || len(second.Items) != 2 || second.Items[0].ID <= first.Items[1].ID {
		t.Fatalf("history second page: %+v %v", second, err)
	}
}

func TestClaimNextSelectionSpreadsStablePriorityTies(t *testing.T) {
	items := []Bead{
		{ID: "p0-a", Type: "issue", Project: "local", Fields: map[string]any{"priority": 0}},
		{ID: "p0-b", Type: "issue", Project: "local", Fields: map[string]any{"priority": 0}},
		{ID: "p0-c", Type: "issue", Project: "local", Fields: map[string]any{"priority": 0}},
		{ID: "p1-a", Type: "issue", Project: "local", Fields: map[string]any{"priority": 1}},
		{ID: "p1-b", Type: "issue", Project: "local", Fields: map[string]any{"priority": 1}},
	}
	seed := ClaimNextSelectionSeed("selector", "key")
	first, err := spreadClaimNextItems(items, seed)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := spreadClaimNextItems(items, seed)
	if err != nil {
		t.Fatal(err)
	}
	ids := func(rows []Bead) []string {
		out := make([]string, len(rows))
		for i, row := range rows {
			out[i] = row.ID
		}
		return out
	}
	if !slices.Equal(ids(first), ids(repeated)) {
		t.Fatalf("same seed changed candidate order: first=%v repeated=%v", ids(first), ids(repeated))
	}
	if slices.Equal(ids(first), ids(items)) {
		t.Fatalf("seed did not spread any equal-priority candidates: %v", ids(first))
	}
	for i, row := range first {
		priority, err := beadPriority(row)
		if err != nil {
			t.Fatal(err)
		}
		if i < 3 && priority != 0 || i >= 3 && priority != 1 {
			t.Fatalf("priority groups crossed: rows=%v", ids(first))
		}
	}
}

func TestClaimNextSelectionUsesLiteralSeededGroupRotations(t *testing.T) {
	items := []Bead{
		{ID: "p0-a", Type: "issue", Project: "local", Fields: map[string]any{"priority": 0}},
		{ID: "p0-b", Type: "issue", Project: "local", Fields: map[string]any{"priority": 0}},
		{ID: "p0-c", Type: "issue", Project: "local", Fields: map[string]any{"priority": 0}},
		{ID: "p0-d", Type: "issue", Project: "local", Fields: map[string]any{"priority": 0}},
		{ID: "p1-a", Type: "issue", Project: "local", Fields: map[string]any{"priority": 1}},
		{ID: "p1-b", Type: "issue", Project: "local", Fields: map[string]any{"priority": 1}},
		{ID: "p1-c", Type: "issue", Project: "local", Fields: map[string]any{"priority": 1}},
		{ID: "p2-a", Type: "issue", Project: "local", Fields: map[string]any{"priority": 2}},
		{ID: "p2-b", Type: "issue", Project: "local", Fields: map[string]any{"priority": 2}},
	}
	selectorA := ClaimNextRequest{
		ViewID: "queue-v1",
		Filter: &Filter{Resources: []string{"b", "a", "a"}, Types: []string{"task", "issue", "task"}, Events: []string{"bead.updated", "bead.created", "bead.updated"}},
	}
	selectorB := ClaimNextRequest{
		ViewID: "queue-v1",
		Filter: &Filter{Resources: []string{"a", "b"}, Types: []string{"issue", "task"}, Events: []string{"bead.created", "bead.updated"}},
	}
	identityA, err := ClaimNextRequestIdentity(selectorA)
	if err != nil {
		t.Fatal(err)
	}
	identityB, err := ClaimNextRequestIdentity(selectorB)
	if err != nil || identityA != identityB {
		t.Fatalf("selector set normalization changed identity: A=%q B=%q err=%v", identityA, identityB, err)
	}
	if identityA != `{"viewId":"queue-v1","filter":{"query":"","resources":["a","b"],"types":["issue","task"],"events":["bead.created","bead.updated"],"viewId":"","includeEphemeral":true}}` {
		t.Fatalf("unexpected canonical selector identity: %s", identityA)
	}

	// Ephemeral inclusion is part of the selector: the adopted default and
	// explicit inclusion are equivalent, while explicit exclusion is distinct.
	included, excluded := true, false
	explicit := selectorB
	explicitFilter := *selectorB.Filter
	explicitFilter.IncludeEphemeral = &included
	explicit.Filter = &explicitFilter
	if got, err := ClaimNextRequestIdentity(explicit); err != nil || got != identityA {
		t.Fatalf("explicit inclusion differs from the default: %q %v", got, err)
	}
	explicitFilter.IncludeEphemeral = &excluded
	excludedIdentity, err := ClaimNextRequestIdentity(explicit)
	if err != nil || excludedIdentity != `{"viewId":"queue-v1","filter":{"query":"","resources":["a","b"],"types":["issue","task"],"events":["bead.created","bead.updated"],"viewId":"","includeEphemeral":false}}` {
		t.Fatalf("unexpected exclusion selector identity: %q %v", excludedIdentity, err)
	}
	if excludedIdentity == identityA {
		t.Fatal("ephemeral inclusion and exclusion share a selector identity")
	}
	for _, filter := range []*Filter{nil, {}, {IncludeEphemeral: &included}} {
		got, err := ClaimNextRequestIdentity(ClaimNextRequest{Filter: filter})
		if err != nil || got != `{"viewId":"","filter":{"query":"","resources":null,"types":null,"events":null,"viewId":"","includeEphemeral":true}}` {
			t.Fatalf("nil, empty and explicit inclusion defaults differ: %q %v", got, err)
		}
	}

	original := append([]Bead(nil), items...)
	cases := []struct {
		name, key, seed, identity string
		want                      []string
	}{
		{
			name:     "first-key",
			identity: identityA,
			key:      "claim-key-a",
			seed:     "4511711ea73303056ddcb44df1c817971f821457a6754a3df7a056a4bfe2f3c1",
			want:     []string{"p0-a", "p0-b", "p0-c", "p0-d", "p1-a", "p1-b", "p1-c", "p2-a", "p2-b"},
		},
		{
			name:     "second-key",
			identity: identityA,
			key:      "claim-key-b",
			seed:     "5a8ebe422eafa62254588a67ecffbbed17a9103af885717bbfa8d2a896d59ee7",
			want:     []string{"p0-a", "p0-b", "p0-c", "p0-d", "p1-c", "p1-a", "p1-b", "p2-b", "p2-a"},
		},
		{
			name:     "excluded-first-key",
			identity: excludedIdentity,
			key:      "claim-key-a",
			seed:     "843e73462f8aaabf57a9c36259886700dfa7e0e1fe1139c216b23195ac31af77",
			want:     []string{"p0-b", "p0-c", "p0-d", "p0-a", "p1-b", "p1-c", "p1-a", "p2-a", "p2-b"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClaimNextSelectionSeed(tc.identity, tc.key); got != tc.seed {
				t.Fatalf("seed changed: got=%s want=%s", got, tc.seed)
			}
			got, err := spreadClaimNextItems(items, tc.seed)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, len(got))
			for i, row := range got {
				ids[i] = row.ID
			}
			if !slices.Equal(ids, tc.want) {
				t.Fatalf("literal seeded rotation changed: got=%v want=%v", ids, tc.want)
			}
			if !slices.Equal(idsOfBeads(items), idsOfBeads(original)) {
				t.Fatal("seeded rotation mutated its input slice")
			}
			if got[0].Fields["priority"] != 0 || got[4].Fields["priority"] != 1 || got[7].Fields["priority"] != 2 {
				t.Fatalf("priority groups crossed: %v", ids)
			}
		})
	}
}

func idsOfBeads(items []Bead) []string {
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.ID
	}
	return ids
}

func TestReadyContinuationReadsLiveRowsWhileHistoryStaysPinned(t *testing.T) {
	for _, action := range []string{"close", "claim", "update"} {
		t.Run(action, func(t *testing.T) {
			ids := []string{"pl-a", "pl-b", "pl-c", "pl-d"}
			next := 0
			f := setup(t, pipeline.WithIDGenerator(func() (string, error) {
				id := ids[next]
				next++
				return id, nil
			}))
			ctx := context.Background()
			for _, title := range ids {
				create(t, f, title)
			}
			first, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1})
			if err != nil || first.Next == nil || first.Items[0].ID != "pl-a" {
				t.Fatalf("first ready: %+v %v", first, err)
			}
			history, err := f.svc.History(ctx, anonymous, HistoryRequest{Project: "local", AsOf: first.AsOf, Limit: 1})
			if err != nil || history.Next == nil {
				t.Fatalf("first history: %+v %v", history, err)
			}
			pending := model.Ref{ID: "pl-b"}
			var changed model.Commit
			switch action {
			case "close":
				changed, err = f.svc.Close(ctx, anonymous, CloseRequest{ID: pending}, meta(t))
			case "claim":
				var claim ClaimResult
				claim, err = f.svc.Claim(ctx, anonymous, ClaimRequest{ID: pending}, meta(t))
				changed = claim.Commit
			case "update":
				changed, err = f.svc.Update(ctx, anonymous, UpdateRequest{ID: pending, Fields: map[string]any{"title": "Live title"}}, meta(t))
			}
			if err != nil {
				t.Fatal(err)
			}
			second, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1, Position: first.Next})
			wantID := "pl-c"
			if action == "update" {
				wantID = "pl-b"
			}
			if err != nil || len(second.Items) != 1 || second.Items[0].ID != wantID || second.AsOf.Revision.String() != changed.Revision.String() {
				t.Fatalf("live continuation: %+v %v; write revision %v", second, err, changed.Revision)
			}
			if action == "update" && second.Items[0].Fields["title"] != "Live title" {
				t.Fatalf("ready returned stale pending row: %+v", second.Items[0])
			}
			if second.Next == nil || second.Next.Offset <= first.Next.Offset || second.Next.AsOf.Revision.String() != second.AsOf.Revision.String() {
				t.Fatalf("continuation failed to advance: %+v", second)
			}
			old, err := f.svc.History(ctx, anonymous, HistoryRequest{Project: "local", AsOf: history.AsOf, Limit: 1, Position: history.Next})
			if err != nil || len(old.Items) != 1 || old.Items[0].ID != "pl-b" || old.Items[0].Fields["title"] != "pl-b" || old.AsOf.Revision.String() != first.AsOf.Revision.String() {
				t.Fatalf("history lost its pinned row: %+v %v", old, err)
			}
		})
	}
}

func TestReadyContinuationAllowsListShrinkAndRejectsOtherIncarnation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	var ids []model.Ref
	for _, title := range []string{"First", "Second", "Third"} {
		id, _ := create(t, f, title)
		ids = append(ids, id)
	}
	first, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Limit: 2})
	if err != nil || first.Next == nil {
		t.Fatalf("first page: %+v %v", first, err)
	}
	for _, id := range ids {
		if _, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: id}, meta(t)); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Limit: 2, Position: first.Next})
	if err != nil || len(page.Items) != 0 || page.Next != nil || page.AsOf.Revision.String() == first.AsOf.Revision.String() {
		t.Fatalf("shrunk live page: %+v %v", page, err)
	}
	position := *first.Next
	position.AsOf.Incarnation = "another-store"
	_, err = f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Position: &position})
	wantCode(t, err, "incarnation_changed")
}

func TestMemoryContinuationUsesLiveRevisionAndValidatesPosition(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	var ids []model.Ref
	for _, key := range []string{"alpha", "middle", "zeta"} {
		commit, err := f.svc.MemorySave(ctx, anonymous, MemorySaveRequest{Key: key, Content: key}, meta(t))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, commit.Results[0].Ref)
	}
	first, err := f.svc.MemoryList(ctx, anonymous, MemoryListRequest{Limit: 1})
	if err != nil || first.Next == nil || first.Items[0].Key != "alpha" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	updated, err := f.svc.MemorySave(ctx, anonymous, MemorySaveRequest{Key: "middle", Content: "updated"}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.svc.MemoryList(ctx, anonymous, MemoryListRequest{Limit: 1, Position: first.Next})
	if err != nil || len(second.Items) != 1 || second.Items[0].Key != "middle" || second.Items[0].Content != "updated" || second.AsOf.Revision.String() != updated.Revision.String() || second.Next == nil || second.Next.AsOf.Revision.String() != second.AsOf.Revision.String() {
		t.Fatalf("live page: %+v %v", second, err)
	}
	_, err = f.svc.MemoryList(ctx, anonymous, MemoryListRequest{Cursor: "unvalidated"})
	wantCode(t, err, "cursor_invalid")
	for _, offset := range []int{-1, scanLimit + 1} {
		_, err = f.svc.MemoryList(ctx, anonymous, MemoryListRequest{Position: &PagePosition{Offset: offset, AsOf: first.AsOf}})
		wantCode(t, err, "cursor_invalid")
	}
	position := *first.Next
	position.AsOf.Incarnation = "another-store"
	_, err = f.svc.MemoryList(ctx, anonymous, MemoryListRequest{Position: &position})
	wantCode(t, err, "incarnation_changed")
	for _, id := range ids {
		if _, err := f.svc.MemoryDelete(ctx, anonymous, MemoryDeleteRequest{ID: id}, meta(t)); err != nil {
			t.Fatal(err)
		}
	}
	last, err := f.svc.MemoryList(ctx, anonymous, MemoryListRequest{Position: second.Next})
	if err != nil || len(last.Items) != 0 || last.Next != nil || last.AsOf.Revision.String() == second.AsOf.Revision.String() {
		t.Fatalf("shrunk live page: %+v %v", last, err)
	}
}

func TestMemoryAuthorizationCapabilityConsistency(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	commit, err := f.svc.MemorySave(ctx, anonymous, MemorySaveRequest{Key: "private", Content: "private-memory-body"}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, installed := range []bool{false, true} {
		for _, hasPolicy := range []bool{false, true} {
			for _, by := range []model.PrincipalRef{anonymous, {Kind: "agent", ID: "worker"}} {
				var policy MemoryReadAuthorizer
				if hasPolicy {
					policy = MemoryReadAuthorizerFunc(func(context.Context, model.PrincipalRef, Memory) (bool, error) { return false, nil })
				}
				svc := New(f.store, f.lease, f.writer, WithMemoryReadAuthorizer(policy), WithAuthorizationInstalled(func() bool { return installed }))
				var code model.ErrorCode
				switch {
				case installed && !hasPolicy:
					code = "authz_unavailable"
				case !installed && by.Kind == "agent":
					code = "authz_not_installed"
				}
				got, getErr := svc.MemoryGet(ctx, by, MemoryGetRequest{ID: commit.Results[0].Ref})
				page, listErr := svc.MemoryList(ctx, by, MemoryListRequest{})
				brief, briefErr := svc.Brief(ctx, by, BriefRequest{})
				if code != "" {
					for _, err := range []error{getErr, listErr, briefErr} {
						wantCode(t, err, code)
					}
					if got.ID != "" || len(page.Items) != 0 || len(brief.Memories) != 0 {
						t.Fatal("inconsistent authorization returned memory data", got, page, brief)
					}
				} else if installed {
					wantCode(t, getErr, "not_found")
					if listErr != nil || briefErr != nil || len(page.Items) != 0 || len(brief.Memories) != 0 {
						t.Fatal("installed policy did not hide memory", page, brief, listErr, briefErr)
					}
				} else if getErr != nil || listErr != nil || briefErr != nil || got.Content != "private-memory-body" || len(page.Items) != 1 || len(brief.Memories) != 1 {
					t.Fatal("anonymous no-plugin behavior changed", got, page, brief, getErr, listErr, briefErr)
				}
			}
		}
	}
}

func TestClaimNextContinuationRestartsAfterFreshSnapshotRevision(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	firstID, _ := create(t, f, "First")
	secondID, _ := create(t, f, "Second")
	first, err := f.svc.ClaimNextReadyPage(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1})
	if err != nil || len(first.Items) != 1 || first.Next == nil {
		t.Fatalf("first claimNext page: %+v %v", first, err)
	}
	claimedFirst := model.RefByID(first.Items[0].ID, model.Natural64(0))
	remainingID := firstID
	if claimedFirst.ID == firstID.ID {
		remainingID = secondID
	}
	if _, err := f.svc.Close(ctx, anonymous, CloseRequest{ID: claimedFirst}, meta(t)); err != nil {
		t.Fatal(err)
	}
	second, err := f.svc.ClaimNextReadyPage(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1, Position: first.Next})
	if err != nil || len(second.Items) != 1 || second.Items[0].ID != remainingID.ID {
		t.Fatalf("claimNext continuation skipped after revision change: %+v %v", second, err)
	}
}

type unboundedQueryReader struct {
	records []driver.Record
}

func (r unboundedQueryReader) Query(_ context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	start := 0
	if q.Cursor != nil {
		value, err := q.Cursor.Index.Int64()
		if err != nil {
			return driver.Page[driver.Record]{}, err
		}
		start = int(value)
	}
	end := start + q.Limit
	if end > len(r.records) {
		end = len(r.records)
	}
	page := driver.Page[driver.Record]{Items: r.records[start:end]}
	if end < len(r.records) {
		page.NextCursor = &driver.Cursor{Index: model.Natural64(uint64(end))}
	}
	return page, nil
}

func TestScanBeadsTraversesBeyondTheFormerQueueCap(t *testing.T) {
	const count = 20001
	records := make([]driver.Record, 0, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("bead-%05d", i)
		doc := pipeline.Document{ID: id, Type: "issue", Project: "local", Fields: map[string]any{"status": "todo"}}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Body: body})
	}
	graph, err := scanBeads(context.Background(), unboundedQueryReader{records: records})
	if err != nil || len(graph) != count {
		t.Fatalf("uncapped graph walk returned %d/%d records: %v", len(graph), count, err)
	}
}

func TestClaimNextRaceOnlySkipsCandidateBusy(t *testing.T) {
	if !claimNextRace(&model.Error{Message: "bead already has a live claim", Data: model.ErrorData{Code: "busy"}}) {
		t.Fatal("candidate live-lease race was not skippable")
	}
	for _, err := range []error{
		&model.Error{Data: model.ErrorData{Code: "busy", Hint: "write pre phases exceeded their deadline"}},
		&model.Error{Data: model.ErrorData{Code: "busy", Hint: "active storage lease was lost"}},
		&model.Error{Message: "global storage failure", Data: model.ErrorData{Code: "write_conflict"}},
		&model.Error{Data: model.ErrorData{Code: "lease_not_held"}},
		&model.Error{Data: model.ErrorData{Code: "lease_expired"}},
	} {
		if claimNextRace(err) {
			t.Fatalf("global queue error was incorrectly skippable: %v", err)
		}
	}
	for _, err := range []error{
		&model.Error{Message: "snapshot changed during all four write attempts", Data: model.ErrorData{Code: "write_conflict"}},
		&model.Error{Data: model.ErrorData{Code: "write_conflict", Hint: "snapshot changed during all four write attempts"}},
	} {
		if !claimNextRace(err) {
			t.Fatalf("candidate write conflict was not skippable: %v", err)
		}
	}
}

// The hook commits through the real writer after selection/preparation and
// before the queue transaction opens. No candidate or receipt is fabricated.
type claimNextRaceStore struct {
	*memory.Store
	beforeWrite func()
	writeError  error
	writeErrors []error
	probes      int
	writes      int
	writePlan   func(int) error
}

func (s *claimNextRaceStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if hook := s.beforeWrite; hook != nil {
		s.beforeWrite = nil
		hook()
	}
	s.writes++
	if s.writePlan != nil {
		if err := s.writePlan(s.writes); err != nil {
			return nil, err
		}
	}
	if len(s.writeErrors) > 0 {
		err := s.writeErrors[0]
		s.writeErrors = s.writeErrors[1:]
		return nil, err
	}
	if s.writeError != nil {
		return nil, s.writeError
	}
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &claimNextRaceTx{WriteTx: tx, store: s}, nil
}

type claimNextRaceTx struct {
	driver.WriteTx
	store   *claimNextRaceStore
	hasHead bool
}

// The race wrapper observes Head and Query, but must preserve the optional
// atomic source writer of its real memory transaction. Hiding that capability
// would turn an otherwise valid certified bead update into an untyped Put.
func (tx *claimNextRaceTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	writer, ok := tx.WriteTx.(driver.GraphSourceWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	return writer.PutGraphRecord(ctx, record, parent, links)
}

func (tx *claimNextRaceTx) Head(ctx context.Context) (model.Revision, error) {
	head, err := tx.WriteTx.Head(ctx)
	tx.hasHead = err == nil
	return head, err
}

func (tx *claimNextRaceTx) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	if !tx.hasHead {
		return driver.Page[driver.Record]{}, errors.New("terminal ready probe ran before Head")
	}
	tx.store.probes++
	return tx.WriteTx.Query(ctx, q)
}

func claimNextRaceService(f *fixture, store *claimNextRaceStore) *Service {
	clock := func() time.Time { return f.now }
	o := f.svc.ontology
	w := pipeline.New(store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(clock))
	return New(store, f.lease, w, WithOntology(o), WithClock(clock))
}

func TestClaimNextAdvancesPastRealCandidateLeaseRace(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	create(t, f, "First")
	create(t, f, "Second")
	claimMeta := model.WriteMeta{Project: "local", Agent: "test-agent", AgentLabel: "test-agent", IdempotencyKey: "abababababababababababababababababababababababababababababababab"}
	claimReq := ClaimNextRequest{Project: "local"}
	identity, err := ClaimNextRequestIdentity(claimReq)
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.svc.ClaimNextReadyPage(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1000, ClaimNextSeed: ClaimNextSelectionSeed(identity, claimMeta.IdempotencyKey)})
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("ready candidates=%+v err=%v", page, err)
	}
	first := model.RefByID(page.Items[0].ID, model.Natural64(0))
	other := model.PrincipalRef{Kind: "user", ID: "competing-worker"}
	store := &claimNextRaceStore{Store: f.store, beforeWrite: func() {
		if _, err := f.svc.ClaimWithOutcome(ctx, other, ClaimRequest{ID: first}, meta(t)); err != nil {
			t.Fatal(err)
		}
	}}
	out, err := claimNextRaceService(f, store).ClaimNextWithOutcome(ctx, anonymous, claimReq, claimMeta)
	if err != nil || out == nil || out.NullResult || len(out.Commit.Results) != 1 || out.Commit.Results[0].Ref.ID != page.Items[1].ID {
		t.Fatalf("candidate busy did not advance to the next real claim: out=%+v err=%v", out, err)
	}
	got, err := f.svc.Get(ctx, anonymous, GetRequest{ID: first})
	if err != nil || got.Lease == nil || got.Lease.Principal.Kind != other.Kind || got.Lease.Principal.ID != other.ID {
		t.Fatalf("competing claim was overwritten: %+v err=%v", got, err)
	}
}

type claimNextAttemptWriter struct {
	*pipeline.Writer
	targets []string
}

func (w *claimNextAttemptWriter) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	if meta.Method == "bead.claimNext" && len(cs.Ops) == 1 && cs.Ops[0].Patch != nil {
		w.targets = append(w.targets, cs.Ops[0].Patch.Target.ID)
	}
	return w.Writer.ApplyWithOutcome(ctx, by, cs, meta)
}

func TestClaimNextRefreshesRacedPageBeforePreparingRetiredCandidates(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	for i := range 8 {
		create(t, f, fmt.Sprintf("Candidate %d", i))
	}
	m := meta(t)
	req := ClaimNextRequest{Project: "local"}
	identity, err := ClaimNextRequestIdentity(req)
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.svc.ClaimNextReadyPage(ctx, anonymous, ReadyRequest{Project: req.Project, Limit: 1000, ClaimNextSeed: ClaimNextSelectionSeed(identity, m.IdempotencyKey)})
	if err != nil || len(page.Items) != 8 {
		t.Fatalf("initial ready page: %+v %v", page, err)
	}
	other := model.PrincipalRef{Kind: "user", ID: "competing-worker"}
	store := &claimNextRaceStore{Store: f.store, beforeWrite: func() {
		for _, bead := range page.Items[:7] {
			if _, err := f.svc.ClaimWithOutcome(ctx, other, ClaimRequest{ID: model.RefByID(bead.ID, model.Natural64(0))}, meta(t)); err != nil {
				t.Fatal(err)
			}
		}
	}}
	svc := claimNextRaceService(f, store)
	w := &claimNextAttemptWriter{Writer: svc.writer.(*pipeline.Writer)}
	svc.writer = w
	out, err := svc.ClaimNextWithOutcome(ctx, anonymous, req, m)
	survivor := page.Items[7].ID
	if err != nil || out == nil || out.NullResult || len(out.Commit.Results) != 1 || out.Commit.Results[0].Ref.ID != survivor {
		t.Fatalf("fresh survivor was not claimed: %+v %v", out, err)
	}
	if !slices.Equal(w.targets, []string{page.Items[0].ID, survivor}) {
		t.Fatalf("retired candidates were prepared from the obsolete page: %v", w.targets)
	}
	if out.Commit.Revision.String() != "16" {
		t.Fatalf("competing claims and sole survivor write did not conserve the head: %+v", out.Commit)
	}
	replayed, err := svc.ClaimNextWithOutcome(ctx, anonymous, req, m)
	if err != nil || replayed == nil || !replayed.Replayed || replayed.Commit.Revision.String() != out.Commit.Revision.String() {
		t.Fatalf("survivor receipt did not replay: %+v %v", replayed, err)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: page.AsOf.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	principal, _ := driver.PrincipalKey(anonymous)
	receipt, err := reader.IdempotencyByKey(ctx, principal, m.AgentLabel, "bead.claimNext", m.IdempotencyKey)
	if err != nil || receipt == nil || len(receipt.Changeset.Changes) != 1 || receipt.Commit.Revision.String() != "16" || reader.Snapshot().At.String() != "16" {
		t.Fatalf("page refresh or replay fabricated a receipt or extra write: receipt=%+v head=%s err=%v", receipt, reader.Snapshot().At, err)
	}
}

func TestClaimNextAdvancesPastCandidateWriteConflict(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	create(t, f, "First")
	create(t, f, "Second")
	claimMeta := model.WriteMeta{Project: "local", Agent: "test-agent", AgentLabel: "test-agent", IdempotencyKey: "abababababababababababababababababababababababababababababababab"}
	claimReq := ClaimNextRequest{Project: "local"}
	identity, err := ClaimNextRequestIdentity(claimReq)
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.svc.ClaimNextReadyPage(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1000, ClaimNextSeed: ClaimNextSelectionSeed(identity, claimMeta.IdempotencyKey)})
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("ready candidates=%+v err=%v", page, err)
	}
	store := &claimNextRaceStore{Store: f.store, writeErrors: []error{
		&model.Error{Message: "snapshot changed during all four write attempts", Data: model.ErrorData{Code: "write_conflict", Retryable: true}},
	}}
	out, err := claimNextRaceService(f, store).ClaimNextWithOutcome(ctx, anonymous, claimReq, claimMeta)
	if err != nil || out == nil || out.NullResult || len(out.Commit.Results) != 1 || out.Commit.Results[0].Ref.ID != page.Items[1].ID {
		t.Fatalf("candidate write conflict did not advance to the next real claim: out=%+v err=%v", out, err)
	}
}

func TestClaimNextBoundsPersistentCandidateWriteConflict(t *testing.T) {
	f := setup(t)
	create(t, f, "First")
	create(t, f, "Second")
	conflict := &model.Error{Message: "snapshot changed during all four write attempts", Data: model.ErrorData{Code: "write_conflict", Retryable: true}}
	store := &claimNextRaceStore{Store: f.store, writePlan: func(n int) error {
		// Two complete walks visit two candidates each. Only the fifth write is
		// the first terminal probe; the tenth is the probe after the bounded
		// ready restart. Keep every candidate exhausted while allowing both
		// probes to observe the still-ready candidates.
		if n%5 != 0 {
			return conflict
		}
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	before, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m := meta(t)
	out, err := claimNextRaceService(f, store).ClaimNextWithOutcome(ctx, anonymous, ClaimNextRequest{Project: "local"}, m)
	if out != nil {
		t.Fatalf("persistent candidate exhaustion fabricated an outcome: %+v", out)
	}
	wantCode(t, err, "write_conflict")
	if !errors.Is(err, conflict) {
		t.Fatalf("persistent candidate exhaustion did not return the original conflict: %v", err)
	}
	if store.writes != 10 {
		t.Fatalf("persistent candidate exhaustion was not bounded after two terminal probes: writes=%d probes=%d", store.writes, store.probes)
	}
	after, err := f.store.Meta(ctx)
	if err != nil || after.Head.String() != before.Head.String() {
		t.Fatalf("persistent candidate exhaustion changed durable state: before=%s after=%s err=%v", before.Head, after.Head, err)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: after.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	principal, _ := driver.PrincipalKey(anonymous)
	receipt, err := reader.IdempotencyByKey(ctx, principal, m.AgentLabel, "bead.claimNext", m.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if receipt != nil {
		t.Fatalf("persistent candidate exhaustion persisted a receipt: %+v", receipt)
	}
}

func TestClaimNextRestartsWhenCandidateAppearsBeforeHead(t *testing.T) {
	f := setup(t)
	var created model.Ref
	store := &claimNextRaceStore{Store: f.store, beforeWrite: func() {
		created, _ = create(t, f, "Ready after the live walks")
	}}
	ctx := context.Background()
	m := meta(t)
	out, err := claimNextRaceService(f, store).ClaimNextWithOutcome(ctx, anonymous, ClaimNextRequest{Project: "local"}, m)
	if err != nil || out == nil || out.NullResult || len(out.Commit.Results) != 1 || out.Commit.Results[0].Ref.ID != created.ID || out.Commit.Revision.String() != "2" {
		t.Fatalf("post-head terminal probe missed a newly ready candidate: out=%+v err=%v", out, err)
	}
	if store.probes == 0 {
		t.Fatal("terminal path did not query through the acquired transaction head")
	}
	state, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: state.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	principal, _ := driver.PrincipalKey(anonymous)
	receipt, err := reader.IdempotencyByKey(ctx, principal, m.AgentLabel, "bead.claimNext", m.IdempotencyKey)
	if err != nil || receipt == nil || len(receipt.Changeset.Changes) != 1 || receipt.Commit.Revision.String() != "2" {
		t.Fatalf("stale empty result was committed: receipt=%+v err=%v", receipt, err)
	}
}

func TestClaimNextGlobalErrorsNeverBecomeNullReceipts(t *testing.T) {
	for _, candidates := range []bool{false, true} {
		for _, code := range []model.ErrorCode{"busy", "write_conflict", "lease_not_held", "lease_expired"} {
			t.Run(fmt.Sprintf("candidates=%t/%s", candidates, code), func(t *testing.T) {
				f := setup(t)
				if candidates {
					create(t, f, "Must remain ready")
				}
				ctx := context.Background()
				before, _ := f.store.Meta(ctx)
				failure := &model.Error{Data: model.ErrorData{Code: code, Hint: "global storage failure"}}
				store := &claimNextRaceStore{Store: f.store, writeError: failure}
				m := meta(t)
				out, err := claimNextRaceService(f, store).ClaimNextWithOutcome(ctx, anonymous, ClaimNextRequest{Project: "local"}, m)
				if out != nil || !errors.Is(err, failure) {
					t.Fatalf("global error changed to a terminal outcome: out=%+v err=%v", out, err)
				}
				after, _ := f.store.Meta(ctx)
				reader, err := f.store.Read(ctx, driver.View{Incarnation: after.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				principal, _ := driver.PrincipalKey(anonymous)
				receipt, err := reader.IdempotencyByKey(ctx, principal, m.AgentLabel, "bead.claimNext", m.IdempotencyKey)
				if err != nil || receipt != nil || after.Head.String() != before.Head.String() {
					t.Fatalf("failed queue attempt persisted a receipt/head: receipt=%+v before=%s after=%s err=%v", receipt, before.Head, after.Head, err)
				}
			})
		}
	}
}

func TestKernelFieldsAndSameValueStatusRefuseBeforeCommit(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id, _ := create(t, f, "Kernel validation")
	initial, _ := f.svc.ontology.effective("local").InitialStatus("issue")
	for _, fields := range []map[string]any{
		{"template": "false"}, {"priority": 5}, {"priority": -1},
		{"priority": json.Number("1.5")}, {"status": true},
	} {
		before, _ := f.store.Meta(ctx)
		_, err := f.svc.Update(ctx, anonymous, UpdateRequest{ID: id, Fields: fields}, meta(t))
		wantCode(t, err, "invalid_params")
		after, _ := f.store.Meta(ctx)
		if after.Head.String() != before.Head.String() {
			t.Fatal("malformed kernel field advanced head")
		}
	}
	_, err := f.svc.Update(ctx, anonymous, UpdateRequest{ID: id, Fields: map[string]any{"status": initial}}, meta(t))
	wantCode(t, err, "ontology_use_slot_verb")
	closeStatus, _ := f.svc.ontology.effective("local").SlotStatus("issue", ontology.Close)
	_, err = f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"status": closeStatus}}, meta(t))
	wantCode(t, err, "ontology_use_slot_verb")
	_, err = f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{"title": "Template", "template": true}}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
}

func TestGuardDoesNotIgnoreUnsupportedStoredParent(t *testing.T) {
	_, err := decodeDocument([]byte(`{"id":"pl-child","type":"issue","fields":{},"parent":"pl-parent"}`))
	wantCode(t, err, "invalid_params")
}

func TestHierarchyGraphNeededOnlyForParentRelationChanges(t *testing.T) {
	parent := model.RefByID("pl-parent", model.Natural64(0))
	before, err := json.Marshal(pipeline.Document{ID: "pl-child", Type: "issue", Parent: &parent, Fields: map[string]any{"title": "before"}})
	if err != nil {
		t.Fatal(err)
	}
	unchanged := append(json.RawMessage(nil), before...)
	other := model.RefByID("pl-other", model.Natural64(0))
	changedDoc := pipeline.Document{ID: "pl-child", Type: "issue", Parent: &other, Fields: map[string]any{"title": "after"}}
	changed, err := json.Marshal(changedDoc)
	if err != nil {
		t.Fatal(err)
	}
	root, err := json.Marshal(pipeline.Document{ID: "pl-root", Type: "issue", Fields: map[string]any{"title": "root", "status": "todo"}})
	if err != nil {
		t.Fatal(err)
	}
	statusBefore, err := json.Marshal(pipeline.Document{ID: "pl-status", Type: "issue", Parent: &parent, Fields: map[string]any{"title": "status", "status": "todo"}})
	if err != nil {
		t.Fatal(err)
	}
	statusAfter, err := json.Marshal(pipeline.Document{ID: "pl-status", Type: "issue", Parent: &parent, Fields: map[string]any{"title": "status", "status": "in_progress"}})
	if err != nil {
		t.Fatal(err)
	}
	projectAfter, err := json.Marshal(pipeline.Document{ID: "pl-child", Project: "other", Type: "issue", Parent: &parent, Fields: map[string]any{"title": "project"}})
	if err != nil {
		t.Fatal(err)
	}
	typeAfter, err := json.Marshal(pipeline.Document{ID: "pl-child", Type: "other", Parent: &parent, Fields: map[string]any{"title": "type"}})
	if err != nil {
		t.Fatal(err)
	}
	childCreate, err := json.Marshal(pipeline.Document{ID: "pl-new", Type: "issue", Parent: &parent, Fields: map[string]any{"title": "new", "status": "todo"}})
	if err != nil {
		t.Fatal(err)
	}
	bead := func(op model.OpKind, old, next []byte) port.Change {
		return port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: model.RefByID("pl-child", model.Natural64(0))}, Op: model.Op{Kind: op}, Before: old, After: next}
	}
	for name, tc := range map[string]struct {
		change port.Change
		want   bool
		setBy  map[string]string
	}{
		"ordinary-parented-field-update": {change: bead(model.OpPatch, before, unchanged), want: false},
		"actual-parent-edit":             {change: bead(model.OpPatch, before, changed), want: true},
		"parented-create":                {change: bead(model.OpCreate, nil, childCreate), want: true},
		"parented-create-status-touch":   {change: bead(model.OpCreate, nil, childCreate), want: true, setBy: map[string]string{"fields.status": "request"}},
		"root-create":                    {change: bead(model.OpCreate, nil, root), want: false},
		"root-create-status-touch":       {change: bead(model.OpCreate, nil, root), want: false, setBy: map[string]string{"fields.status": "request"}},
		"parented-delete":                {change: bead(model.OpDelete, before, nil), want: true},
		"root-delete":                    {change: bead(model.OpDelete, root, nil), want: false},
		"existing-status-drift":          {change: bead(model.OpPatch, statusBefore, statusAfter), want: true},
		"existing-project-drift":         {change: bead(model.OpPatch, before, projectAfter), want: true},
		"existing-type-drift":            {change: bead(model.OpPatch, before, typeAfter), want: true},
	} {
		t.Run(name, func(t *testing.T) {
			tc.change.SetBy = tc.setBy
			got, err := hierarchyGraphNeeded(port.ProposedChange{Changes: []port.Change{tc.change}})
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("hierarchyGraphNeeded=%v, want %v", got, tc.want)
			}
		})
	}
	_, err = hierarchyGraphNeeded(port.ProposedChange{Changes: []port.Change{bead(model.OpPatch, before, []byte(`{"not":"a bead"}`))}})
	wantCode(t, err, "invalid_params")
	for _, op := range []model.OpKind{model.OpReparent, model.OpLink, model.OpUnlink} {
		_, err = hierarchyGraphNeeded(port.ProposedChange{Changes: []port.Change{bead(op, before, []byte(`{"not":"a bead"}`))}})
		wantCode(t, err, "invalid_params")
	}
	statusTouch := bead(model.OpPatch, before, unchanged)
	statusTouch.SetBy = map[string]string{"fields.status": "request"}
	got, err := hierarchyGraphNeeded(port.ProposedChange{Changes: []port.Change{statusTouch}})
	if err != nil || !got {
		t.Fatalf("same-value status touch lost lifecycle graph validation: needed=%v err=%v", got, err)
	}
	linkChange := bead(model.OpLink, before, unchanged)
	got, err = hierarchyGraphNeeded(port.ProposedChange{Changes: []port.Change{linkChange}})
	if err != nil || !got {
		t.Fatalf("link mutation lost relational graph validation: needed=%v err=%v", got, err)
	}
	got, err = hierarchyGraphNeeded(port.ProposedChange{Changes: []port.Change{{Resource: model.ResourceRef{Kind: "bead"}, Op: model.Op{Kind: model.OpReparent}, Before: unchanged, After: unchanged}}})
	if err != nil || !got {
		t.Fatal("explicit reparent must retain graph validation even when the final parent is unchanged")
	}
}

func TestSlotSelectionUsesOntologyKeys(t *testing.T) {
	f := setup(t)
	doc := ontology.DefaultDocument()
	w := doc.Workflows["issue"]
	status := doc.Statuses[w.Slots[ontology.Claim]]
	old := w.Slots[ontology.Claim]
	delete(doc.Statuses, old)
	doc.Statuses["working"] = status
	w.Slots[ontology.Claim] = "working"
	for i, value := range w.Statuses {
		if value == old {
			w.Statuses[i] = "working"
		}
	}
	for i := range w.Transitions {
		if w.Transitions[i].To == old {
			w.Transitions[i].To = "working"
		}
	}
	doc.Workflows["issue"] = w
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	o := NewOntology(snapshot.Effective)
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(o))
	f.svc = New(f.store, f.lease, f.writer, WithOntology(o))
	id, _ := create(t, f, "Custom slot")
	if _, err := f.svc.Claim(context.Background(), anonymous, ClaimRequest{ID: id}, meta(t)); err != nil {
		t.Fatal(err)
	}
	got, err := f.svc.Get(context.Background(), anonymous, GetRequest{ID: id})
	if err != nil || got.Bead.Status != "working" {
		t.Fatalf("hardcoded claim status: %+v %v", got, err)
	}
}

// Storage fixtures exercise the real strict read decoder, not import admission.
func TestGetCommentSnapshotsAndDetachedProvenance(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	ref, created := create(t, f, "Comments")
	initial, err := f.svc.Get(ctx, anonymous, GetRequest{ID: ref})
	if err != nil || initial.Comments != nil {
		t.Fatalf("old empty read: %+v %v", initial, err)
	}
	doc := pipeline.Document{ID: ref.ID, Project: "local", Type: "issue", Fields: map[string]any{"title": "Comments", "status": "todo"}}
	source := f.now.Add(-time.Hour)
	doc.Comments = []pipeline.Comment{{Body: "legacy", Principal: anonymous, Time: f.now}, {Body: "imported", Principal: model.PrincipalRef{Kind: "user", ID: "restore-admin", Caveats: map[string]any{"nested": map[string]any{"value": "original"}}}, Time: f.now, Origin: "import", SourceAuthor: "original author", SourceTime: &source}}
	seeded, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpPatch, Body: mustJSON(t, doc), Graph: &conformance.FixtureGraphSource{}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.svc.Get(ctx, anonymous, GetRequest{ID: ref})
	if err != nil || len(got.Comments) != 2 {
		t.Fatalf("comments: %+v %v", got, err)
	}
	if got.Comments[1].Principal.ID != "restore-admin" || got.Comments[1].SourceAuthor != "original author" || !got.Comments[1].SourceTime.Equal(source) {
		t.Fatal("actor/source provenance changed")
	}
	*got.Comments[1].SourceTime = f.now
	got.Comments[1].Principal.Caveats["nested"].(map[string]any)["value"] = "mutated"
	again, err := f.svc.Get(ctx, anonymous, GetRequest{ID: ref, AsOf: &AsOf{Revision: seeded.Revision}})
	if err != nil || !again.Comments[1].SourceTime.Equal(source) || again.Comments[1].Principal.Caveats["nested"].(map[string]any)["value"] != "original" {
		t.Fatalf("result alias: %+v %v", again, err)
	}
	old, err := f.svc.Get(ctx, anonymous, GetRequest{ID: ref, AsOf: &AsOf{Revision: created.Revision}})
	if err != nil || old.Comments != nil {
		t.Fatalf("later comments leaked: %+v %v", old, err)
	}
}

func TestGetRefusesMalformedStoredCommentUnion(t *testing.T) {
	base := map[string]any{"body": "secret", "principal": anonymous, "time": "2026-10-30T10:00:00Z"}
	cases := []map[string]any{{"origin": ""}, {"sourceAuthor": ""}, {"sourceTime": nil}, {"origin": "import", "sourceTime": "2026-10-29T10:00:00Z", "sourceAuthor": nil}, {"origin": "import"}, {"origin": "import", "sourceTime": nil}, {"origin": "import", "sourceTime": "2026-10-29T10:00:00Z", "sourceAuthor": "bad\nauthor"}, {"unexpected": true}, {"Origin": ""}, {"sourceauthor": ""}, {"Time": "2026-10-30T10:00:00Z"}}
	for i, extra := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			f := setup(t)
			ref, _ := create(t, f, "Malformed")
			comment := map[string]any{}
			for k, v := range base {
				comment[k] = v
			}
			for k, v := range extra {
				comment[k] = v
			}
			doc := map[string]any{"id": ref.ID, "project": "local", "type": "issue", "fields": map[string]any{"title": "Malformed", "status": "todo"}, "comments": []any{comment}}
			_, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpPatch, Body: mustJSON(t, doc), Graph: &conformance.FixtureGraphSource{}}})
			if err != nil {
				t.Fatal(err)
			}
			got, err := f.svc.Get(context.Background(), anonymous, GetRequest{ID: ref})
			wantCode(t, err, "internal_error")
			if got.Comments != nil || got.Bead.ID != "" {
				t.Fatal("refused document leaked")
			}
		})
	}
}

// The shared read codec can decode imported/stored canonical Parent refs even
// before the hierarchy writer/child-status layer is installed. Lifecycle guards
// must not turn that newly supported source shape into a false child absence.
func TestCanonicalParentCannotSatisfyNoOpenChildren(t *testing.T) {
	for _, method := range []string{"bead.close", "bead.fail"} {
		t.Run(method, func(t *testing.T) {
			f := setup(t)
			ctx := context.Background()
			parent, _ := create(t, f, "Parent")
			child, _ := create(t, f, "Open child")
			unrelated, _ := create(t, f, "Unrelated root")
			body, err := json.Marshal(pipeline.Document{ID: child.ID, Type: "issue", Project: "local", Parent: &parent, Fields: map[string]any{"title": "Open child", "status": "todo", "priority": 0}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: child, Kind: "bead", Op: model.OpPatch, Body: body, Graph: &conformance.FixtureGraphSource{Parent: &parent}}}); err != nil {
				t.Fatal(err)
			}
			before, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			m := meta(t)
			if method == "bead.close" {
				_, err = f.svc.Close(ctx, anonymous, CloseRequest{ID: parent}, m)
			} else {
				_, err = f.svc.Fail(ctx, anonymous, FailRequest{ID: parent}, m)
			}
			wantCode(t, err, "ontology_guard_failed")
			after, err := f.store.Meta(ctx)
			if err != nil || after.Head.String() != before.Head.String() {
				t.Fatal("refused parent lifecycle published a write", err)
			}
			reader, err := f.store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			principal, err := driver.PrincipalKey(anonymous)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := reader.IdempotencyByKey(ctx, principal, m.AgentLabel, method, m.IdempotencyKey)
			if err != nil || receipt != nil {
				t.Fatal("refused parent lifecycle published a receipt", err)
			}
			// This restriction is local to the actual direct parent, not a global
			// refusal whenever any relation exists in the graph.
			if method == "bead.close" {
				_, err = f.svc.Close(ctx, anonymous, CloseRequest{ID: unrelated}, meta(t))
			} else {
				_, err = f.svc.Fail(ctx, anonymous, FailRequest{ID: unrelated}, meta(t))
			}
			if err != nil {
				t.Fatal("unrelated root lifecycle refused", err)
			}
		})
	}
}

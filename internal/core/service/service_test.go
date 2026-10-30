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

func TestUnsupportedReadsAndParentRefuseWithoutPartialSuccess(t *testing.T) {
	next := 0
	f := setup(t, pipeline.WithIDGenerator(func() (string, error) {
		ids := []string{"pl-a", "pl-b", "pl-c"}
		id := ids[next]
		next++
		return id, nil
	}))
	ctx := context.Background()
	id, _ := create(t, f, "One")
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
	_, err = f.svc.Create(ctx, anonymous, CreateRequest{Type: "issue", Fields: map[string]any{}, Parent: &id}, meta(t))
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
	if !claimNextRace(&model.Error{Data: model.ErrorData{Code: "busy", Hint: "bead already has a live claim"}}) {
		t.Fatal("candidate live-lease race was not skippable")
	}
	for _, err := range []error{
		&model.Error{Data: model.ErrorData{Code: "busy", Hint: "write pre phases exceeded their deadline"}},
		&model.Error{Data: model.ErrorData{Code: "busy", Hint: "active storage lease was lost"}},
		&model.Error{Data: model.ErrorData{Code: "write_conflict"}},
		&model.Error{Data: model.ErrorData{Code: "lease_not_held"}},
		&model.Error{Data: model.ErrorData{Code: "lease_expired"}},
	} {
		if claimNextRace(err) {
			t.Fatalf("global queue error was incorrectly skippable: %v", err)
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

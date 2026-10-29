package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
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

func TestUnsupportedReadsAndParentRefuseWithoutPartialSuccess(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id, _ := create(t, f, "One")
	_, _ = create(t, f, "Two")
	first, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1})
	if err != nil || len(first.Items) != 1 || first.Next == nil {
		t.Fatalf("first page: %+v %v", first, err)
	}
	_, _ = create(t, f, "Later insert")
	second, err := f.svc.Ready(ctx, anonymous, ReadyRequest{Project: "local", Limit: 1, Position: first.Next})
	if err != nil || len(second.Items) != 1 || second.Next != nil || second.Items[0].ID == first.Items[0].ID || second.AsOf.Revision.String() != first.AsOf.Revision.String() {
		t.Fatalf("pinned second page: %+v %v", second, err)
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

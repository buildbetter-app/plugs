package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAuthorizationPortNeverNamesUndeliveredResource(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	f.manager.config.Events.Reader.Authorization = f.auth
	sub := f.open(t, f.request("unseen"))
	_ = nextFrame(t, sub, time.Second)
	f.commit(t, "never-exposed", "private payload", model.OpCreate)
	waitQueued(t, sub)
	f.auth.SetAllowed(false)
	frame := nextFrame(t, sub, time.Second)
	if len(frame.Events) != 0 || !frame.Terminal() {
		t.Fatalf("undelivered resource leaked: %+v", frame)
	}
	if _, err := sub.Next(f.ctx); !errors.Is(err, ErrResetRequired) {
		t.Fatalf("reset resumed: %v", err)
	}
}

type bindingFunc func(context.Context, Request) (Binding, error)

func (fn bindingFunc) Current(ctx context.Context, req Request) (Binding, error) { return fn(ctx, req) }

// boundaryHeadLog keeps the real memory log and materializer in the service
// path while presenting one of the reader's actual cursor-boundary heads. The
// service must still scan the returned head to construct its signed gap cursor.
type boundaryHeadLog struct {
	inner coreevents.Log
	head  coreevents.Head
}

func (l boundaryHeadLog) Head(context.Context) (coreevents.Head, error) { return l.head, nil }

func (l boundaryHeadLog) Scan(ctx context.Context, after coreevents.Position, limit int) ([]model.Changeset, error) {
	if limit > 0 && after.Incarnation == l.head.Incarnation && after.Revision.String() == l.head.Revision.String() && !l.head.Revision.IsZero() {
		return []model.Changeset{{Incarnation: l.head.Incarnation, Revision: l.head.Revision}}, nil
	}
	return l.inner.Scan(ctx, after, limit)
}

func TestGapBoundariesRemainMandatoryCanonicalTerminalFrames(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		from   string
		after  model.Revision
		head   coreevents.Head
	}{
		{name: "incarnation", reason: "failover", from: "old-incarnation", after: "1"},
		{name: "history-floor", reason: "history_floor", from: "watch-incarnation-0001", after: "1", head: coreevents.Head{Incarnation: "watch-incarnation-0001", Revision: "2", Floor: "2"}},
		{name: "checkpoint", reason: "checkpoint", from: "watch-incarnation-0001", after: "2", head: coreevents.Head{Incarnation: "watch-incarnation-0001", Revision: "1", Floor: "0"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
			meta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			head := tc.head
			if head.Incarnation == "" {
				head = coreevents.Head{Incarnation: meta.Incarnation, Revision: meta.Head, Floor: meta.HistoryFloor}
			}
			f.manager.config.Events.Reader.Log = boundaryHeadLog{inner: f.manager.config.Events.Reader.Log, head: head}
			req := f.request("gap-" + tc.name)
			binding, err := f.manager.binding(f.ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			cursorBinding, err := serverevents.BindingFor(string(req.Method), binding.Filter, binding.Principal, binding.Ontology, binding.PolicyVersion)
			if err != nil {
				t.Fatal(err)
			}
			req.From, err = f.manager.config.Events.Cursors.Encode(coreevents.Position{Incarnation: tc.from, Revision: tc.after}, cursorBinding)
			if err != nil {
				t.Fatal(err)
			}
			sub := f.open(t, req)
			frame := nextFrame(t, sub, time.Second)
			if frame.Kind != FrameReset || frame.Reason != "" || len(frame.Events) != 1 || frame.Events[0].Control == nil || frame.Events[0].Control.Type != model.EventGap {
				t.Fatalf("gap frame=%+v", frame)
			}
			details, ok := frame.Events[0].Control.Details.(model.EventGapDetails)
			if !ok || details.Reason != tc.reason {
				t.Fatalf("gap details=%+v want reason %q", frame.Events[0].Control.Details, tc.reason)
			}
			wire := frame.WireEvents()
			if len(wire) != 1 || wire[0].Control == nil || wire[0].Control.Type != model.EventGap {
				t.Fatalf("gap wire=%+v", wire)
			}
			if _, err := json.Marshal(frame); err != nil {
				t.Fatalf("gap frame marshal=%v", err)
			}
			if _, err := sub.Next(f.ctx); !errors.Is(err, ErrResetRequired) {
				t.Fatalf("gap stream resumed: %v", err)
			}
		})
	}
}

func TestTerminalResetDropsQueuedDurableImagesAtAuthorizationBoundary(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	f.manager.config.Events.Reader.Authorization = f.auth
	f.commit(t, "before-reset", "must be omitted", model.OpCreate)
	source := service.OntologySource{Base: ontology.DefaultDocument()}
	body, err := service.MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "user", ID: "admin"}}, []conformance.FixtureChange{{Ref: model.RefByID("ontology/current", "0"), Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	f.commit(t, "after-reset", "must not be queued", model.OpCreate)
	sub := f.open(t, f.request("reset-authz"))
	waitQueued(t, sub)
	sub.watch.queueMu.Lock()
	queued := <-sub.watch.queue
	sub.watch.queue <- queued
	sub.watch.queueMu.Unlock()
	if queued.Kind != FrameReset || queued.Reason != "ontology_changed" {
		t.Fatalf("queued terminal frame=%+v", queued)
	}
	for _, event := range queued.Events {
		if event.Durable != nil {
			t.Fatalf("queued terminal frame retained durable image=%+v", event)
		}
	}
	f.auth.SetAllowed(false)
	frame := nextFrame(t, sub, time.Second)
	if frame.Kind != FrameReset || frame.Reason != "ontology_changed" {
		t.Fatalf("delivered terminal frame=%+v", frame)
	}
	for _, event := range frame.Events {
		if event.Durable != nil {
			t.Fatalf("delivered terminal frame leaked durable image=%+v", event)
		}
	}
	if _, err := json.Marshal(frame); err != nil {
		t.Fatalf("terminal frame marshal=%v", err)
	}
}

func TestBindingPortChangesResetBeforeQueuedPayloadAndRequireReopen(t *testing.T) {
	for _, kind := range []string{"policy", "ontology"} {
		t.Run(kind, func(t *testing.T) {
			f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
			var changed atomic.Bool
			f.manager.config.Bindings = bindingFunc(func(_ context.Context, r Request) (Binding, error) {
				b := Binding{Principal: r.Principal, Filter: r.Filter, Ontology: "1", PolicyVersion: "p1"}
				if changed.Load() {
					if kind == "policy" {
						b.PolicyVersion = "p2"
					} else {
						b.Ontology = "2"
					}
				}
				return b, nil
			})
			sub := f.open(t, f.request("binding"))
			_ = nextFrame(t, sub, time.Second)
			f.commit(t, "queued", "stale image", model.OpCreate)
			waitQueued(t, sub)
			changed.Store(true)
			frame := nextFrame(t, sub, time.Second)
			if frame.Kind != FrameReset || frame.Reason != kind+"_changed" || len(frame.Events) != 0 {
				t.Fatalf("binding reset=%+v", frame)
			}
			if _, err := sub.Next(f.ctx); !errors.Is(err, ErrResetRequired) {
				t.Fatalf("stream resumed under old generation: %v", err)
			}
			// The reset token is bound to the current generation, and is an explicit
			// reopen marker rather than an acknowledgement of the discarded image.
			binding, _ := f.manager.binding(f.ctx, f.request("binding"))
			expected, _ := sub.watch.cursorBinding(binding)
			if _, err := f.manager.config.Events.Cursors.Decode(frame.Cursor, expected); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestActualOntologyBoundaryStopsBeforeLaterServiceWriteEvenWhenFiltered(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	// This source record is actual stored configuration; subsequent bead events
	// are produced by service.Create, not a fabricated log/materializer.
	source := service.OntologySource{Base: ontology.DefaultDocument()}
	body, err := service.MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "user", ID: "admin"}}, []conformance.FixtureChange{{Ref: model.RefByID("ontology/current", "0"), Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	f.commit(t, "after-boundary", "must not arrive", model.OpCreate)
	req := f.request("boundary")
	req.Filter = coreevents.Filter{Types: []string{"bead.created"}}
	sub := f.open(t, req)
	frame := nextFrame(t, sub, time.Second)
	if frame.Reason != "ontology_changed" || len(frame.Events) != 0 || !frame.Terminal() {
		t.Fatalf("filtered boundary=%+v", frame)
	}
	if _, err := sub.Next(f.ctx); !errors.Is(err, ErrResetRequired) {
		t.Fatalf("later event delivered after boundary: %v", err)
	}
}

func TestActualServiceReplayPagesPreserveRevisionAndLogicalIndexOrder(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	f.commit(t, "ordered", "initial", model.OpCreate)
	// Two real creates in one pipeline commit exercise logical-index replay.
	ids := []string{"ordered-a", "ordered-b"}
	at := 0
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithIDGenerator(func() (string, error) { id := ids[at]; at++; return id, nil }))
	commit, err := writer.Apply(f.ctx, model.PrincipalRef{Kind: "user", ID: "writer"}, model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: "local", Fields: map[string]any{"title": "a"}}},
		{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: "local", Fields: map[string]any{"title": "b"}}},
	}}, model.WriteMeta{Method: "changeset.apply", Project: "local", IdempotencyKey: fmt.Sprintf("%032x", 100)})
	if err != nil {
		t.Fatal(err)
	}
	f.manager.config.PageLimit = 1
	sub := f.open(t, f.request("ordered"))
	first := nextFrame(t, sub, time.Second)
	second := nextFrame(t, sub, time.Second)
	third := nextFrame(t, sub, time.Second)
	if len(first.Events) != 1 || len(second.Events) != 1 || len(third.Events) != 1 || first.Events[0].Durable.Type != model.EventBeadCreated || second.Events[0].Durable.Type != model.EventBeadCreated || third.Events[0].Durable.Type != model.EventBeadCreated {
		t.Fatalf("ordered pages: %+v %+v %+v", first.Events, second.Events, third.Events)
	}
	if second.Events[0].Durable.Revision.String() != commit.Revision.String() || third.Events[0].Durable.Index.String() != "1" || second.Cursor != third.FromCursor {
		t.Fatal("lost revision/index continuity")
	}
	req := f.request("index-replay")
	req.From = second.Cursor
	replay := f.open(t, req)
	frame := nextFrame(t, replay, time.Second)
	if len(frame.Events) != 1 || frame.Events[0].Durable.Resource != "bead/ordered-b" || frame.Cursor != third.Cursor {
		t.Fatalf("exact index replay=%+v", frame)
	}
}

func TestMethodPrincipalAndFilterCursorBindings(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	f.commit(t, "bound-bead", "image", model.OpCreate)
	req := f.request("bead")
	req.Method = MethodBeadWatch
	req.Resource = "bead/bound-bead"
	sub := f.open(t, req)
	frame := nextFrame(t, sub, time.Second)
	for _, mutation := range []string{"method", "principal", "filter"} {
		t.Run(mutation, func(t *testing.T) {
			r := req
			r.SubscriptionID = "wrong-" + mutation
			r.From = frame.Cursor
			switch mutation {
			case "method":
				r.Method = MethodEventSubscribe
				r.Filter = coreevents.Filter{Resources: []string{req.Resource}}
			case "principal":
				r.Principal.ID = "another"
			case "filter":
				r.Resource = "bead/another"
			}
			if _, err := f.manager.Open(f.ctx, r); !errors.Is(err, coreevents.ErrCursorInvalid) {
				t.Fatalf("cursor %s accepted: %v", mutation, err)
			}
		})
	}
	binding, _ := serverevents.BindingFor("bead.watch", coreevents.Filter{Resources: []string{req.Resource}}, req.Principal, req.Ontology, req.PolicyVersion)
	if _, err := f.manager.config.Events.Cursors.Decode(frame.Cursor, binding); err != nil {
		t.Fatal(err)
	}
}

func TestSingleStreamOwnerAndOwnedUnsubscribe(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	opened := f.open(t, f.request("one-consumer"))
	if _, err := f.manager.AttachSession(f.ctx, opened.ID(), "other-session", f.request("").Principal); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("cross-session attach=%v", err)
	}
	attached, err := f.manager.AttachSession(f.ctx, opened.ID(), "session-1", f.request("").Principal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.manager.Attach(f.ctx, opened.ID(), f.request("").Principal); !errors.Is(err, ErrAlreadyAttached) {
		t.Fatalf("second attach=%v", err)
	}
	if _, err = opened.Next(f.ctx); !errors.Is(err, ErrAlreadyAttached) {
		t.Fatalf("original handle stole stream=%v", err)
	}
	_ = nextFrame(t, attached, time.Second)
	if err = f.manager.CancelOwned(f.ctx, opened.ID(), model.PrincipalRef{Kind: "user", ID: "other"}); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("foreign unsubscribe=%v", err)
	}
	if err = f.manager.CancelOwned(f.ctx, opened.ID(), f.request("").Principal); err != nil {
		t.Fatal(err)
	}
}

func TestCancellationNeverDrainsBufferedPayloadAndConcurrentCloseCompletes(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	f.commit(t, "buffered", "payload", model.OpCreate)
	lifetime, cancel := context.WithCancel(f.ctx)
	sub, err := f.manager.Open(lifetime, f.request("cancel-buffered"))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	<-sub.watch.ctx.Done()
	if _, err := sub.Next(f.ctx); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("canceled watch delivered buffered frame: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.manager.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent manager close did not release all loops")
	}
	if count := f.manager.WatchCount("session-1"); count != 0 {
		t.Fatalf("retained watches=%d", count)
	}
}

// The controlled resource port is used only to prove delivery-time callback
// coverage; it does not certify installed fixed-permission host integration.
type resourcePolicy struct {
	mu       sync.Mutex
	denied   string
	redacted bool
}

func (p *resourcePolicy) Read(_ context.Context, _ model.PrincipalRef, e model.DurableEvent) (coreevents.Visibility, error) {
	return coreevents.Visibility{Readable: true, Before: e.Before, After: e.After}, nil
}
func (p *resourcePolicy) ReadResource(_ context.Context, _ model.PrincipalRef, r model.AuditResource) (coreevents.ResourceVisibility, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hit := p.denied == r.Kind+"/"+r.Ref.ID
	return coreevents.ResourceVisibility{Readable: !hit || p.redacted, Redacted: hit && p.redacted}, nil
}

func TestAuthorizationPortRechecksEveryNoticeResourceBeforeDelivery(t *testing.T) {
	for _, resource := range []string{"bead/dependent", "bead/blocker", "ontology/ontology/source", "project/local"} {
		for _, redacted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/redacted=%v", resource, redacted), func(t *testing.T) {
				f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
				policy := &resourcePolicy{}
				f.manager.config.Events.Reader.Authorization = policy
				sourceRef := model.RefByID("ontology/source", "0")
				source := service.OntologySource{Base: ontology.DefaultDocument()}
				body, err := service.MarshalOntologySource(source)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "user", ID: "writer"}}, []conformance.FixtureChange{{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
					t.Fatal(err)
				}
				initial := f.open(t, f.request("source-boundary"))
				boundary := nextFrame(t, initial, time.Second)
				if boundary.Reason != "ontology_changed" {
					t.Fatalf("source boundary=%+v", boundary)
				}
				_ = initial.Close()
				normalizer := service.NewOntology(nil)
				f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(normalizer), pipeline.WithOntologyAudit(service.NewOntologyAudit(service.StaticOntologySource(sourceRef))), pipeline.WithIDGenerator(func() (string, error) { return f.nextID, nil }))
				f.svc = service.New(f.store, f.lease, f.writer, service.WithOntology(normalizer))
				f.commit(t, "blocker", "blocker", model.OpCreate)
				f.commit(t, "dependent", "dependent", model.OpCreate)
				f.key++
				_, err = f.writer.Apply(f.ctx, model.PrincipalRef{Kind: "user", ID: "writer"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: model.RefByID("dependent", "0"), To: model.RefByID("blocker", "0"), LinkKind: "conditional-blocks"}}}}, model.WriteMeta{Method: "bead.update", Project: "local", IdempotencyKey: fmt.Sprintf("%032x", f.key)})
				if err != nil {
					t.Fatal(err)
				}
				req := f.request("notice")
				req.From = boundary.Cursor
				sub := f.open(t, req)
				snapshot := nextFrame(t, sub, time.Second)
				if len(snapshot.Events) < 2 {
					t.Fatalf("real graph snapshot=%+v", snapshot)
				}
				f.key++
				_, err = f.svc.Close(f.ctx, model.PrincipalRef{Kind: "user", ID: "writer"}, service.CloseRequest{ID: model.RefByID("blocker", "0")}, model.WriteMeta{Project: "local", IdempotencyKey: fmt.Sprintf("%032x", f.key)})
				if err != nil {
					t.Fatal(err)
				}
				waitQueued(t, sub)
				// Prove this is testing a queued ACTUAL condition notice, not an empty log.
				sub.watch.queueMu.Lock()
				queued := <-sub.watch.queue
				sub.watch.queue <- queued
				sub.watch.queueMu.Unlock()
				if len(queued.Events) != 2 || queued.Events[1].Durable == nil || queued.Events[1].Durable.Type != model.EventDependencyConditionUnmet {
					t.Fatalf("service did not create condition notice: %+v", queued.Events)
				}
				policy.mu.Lock()
				policy.denied = resource
				policy.redacted = redacted
				policy.mu.Unlock()
				frame := nextFrame(t, sub, time.Second)
				if len(frame.Events) != 1 || frame.Events[0].Durable == nil || frame.Events[0].Durable.Type != model.EventBeadClosed || frame.Reason != "" {
					t.Fatalf("notice denial leaked notice or marker: %+v", frame)
				}
			})
		}
	}
}

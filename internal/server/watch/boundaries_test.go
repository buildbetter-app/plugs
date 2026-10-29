package watch

import (
	"context"
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
	attached, err := f.manager.Attach(f.ctx, opened.ID(), f.request("").Principal)
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

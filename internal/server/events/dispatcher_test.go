package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type handlerFunc func(context.Context, core.Delivery) error

func (fn handlerFunc) Deliver(ctx context.Context, item core.Delivery) error { return fn(ctx, item) }

type projectorFunc func(context.Context, core.ChangeDelivery) error

func (fn projectorFunc) Apply(ctx context.Context, group core.ChangeDelivery) error {
	return fn(ctx, group)
}

type adminFunc func(context.Context, string, string, model.PrincipalRef) error

func (fn adminFunc) Authorize(ctx context.Context, action, binding string, p model.PrincipalRef) error {
	return fn(ctx, action, binding, p)
}
func admin() core.Administration {
	return adminFunc(func(_ context.Context, _ string, _ string, p model.PrincipalRef) error {
		if p.Kind != "user" || p.ID != "admin" {
			return errors.New("forbidden")
		}
		return nil
	})
}
func adminUser() model.PrincipalRef { return model.PrincipalRef{Kind: "user", ID: "admin"} }

type notices struct {
	mu     sync.Mutex
	values []core.Notice
}

func (n *notices) Notify(_ context.Context, notice core.Notice) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.values = append(n.values, notice)
}

type lostAck struct {
	core.Checkpoints
	fail bool
}

func (c *lostAck) Save(ctx context.Context, key string, generation uint64, state core.State) (uint64, error) {
	if c.fail {
		c.fail = false
		return generation, errors.New("simulated checkpoint write failure")
	}
	return c.Checkpoints.Save(ctx, key, generation, state)
}

func TestReplayAfterSideEffectBeforeCheckpointHasIdenticalKey(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-a", "a"))
	var keys []string
	binding := core.Binding{ID: "listener", Class: core.Listener, Principal: request().Principal, Handler: handlerFunc(func(_ context.Context, item core.Delivery) error {
		keys = append(keys, item.Key.String())
		if item.Partition != "bead/pl-a" {
			t.Error("wrong partition")
		}
		return nil
	})}
	first := f.dispatcher(t)
	first.Checkpoints = &lostAck{Checkpoints: first.Checkpoints, fail: true}
	if _, err := first.Step(f.ctx, binding); err == nil {
		t.Fatal("checkpoint failure became success")
	}
	state, generation, err := f.checkpoints(t).Load(f.ctx, binding.ID)
	if err != nil || generation != 0 {
		t.Fatalf("unpublished checkpoint=%+v %d %v", state, generation, err)
	}
	// Reconstruct every event adapter/dispatcher, retaining ONLY actual store
	// metadata. No in-process delivery state is carried over.
	restarted := f.dispatcher(t)
	if progress, err := restarted.Step(f.ctx, binding); err != nil || !progress.Advanced {
		t.Fatalf("replay=%+v %v", progress, err)
	}
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("at-least-once keys=%v", keys)
	}
	if _, err = restarted.Step(f.ctx, binding); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatal("acknowledged event replayed without a crash")
	}
	state, generation, err = f.checkpoints(t).Load(f.ctx, binding.ID)
	if err != nil || generation != 1 || state.Position.Revision.String() != "1" {
		t.Fatalf("persisted state=%+v %d %v", state, generation, err)
	}
	meta, _ := f.store.Meta(f.ctx)
	if meta.Head.String() != "1" {
		t.Fatal("checkpoint advanced graph head")
	}
}

func TestPostFailureWarnsCommittedResultAndPersistsLateReplay(t *testing.T) {
	f := newFixture(t)
	set := f.commit(t, time.Time{}, bead("pl-a", "committed"))
	observer := &notices{}
	d := f.dispatcher(t)
	d.Observer = observer
	var firstKey string
	b := core.Binding{ID: "post", Class: core.Post, Principal: request().Principal, Handler: handlerFunc(func(_ context.Context, item core.Delivery) error {
		firstKey = item.Key.String()
		if item.Late {
			t.Error("fresh post call marked late")
		}
		return errors.New("external destination failed")
	})}
	warnings := d.RunPost(f.ctx, []core.Binding{b}, set.Revision)
	if len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("warnings=%+v", warnings)
	}
	state, _, err := f.checkpoints(t).Load(f.ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Failure == nil || !state.Failure.DeadLetter || state.Failure.Attempts != 1 || !state.Position.Revision.IsZero() {
		t.Fatalf("failed checkpoint=%+v", state)
	}
	if len(observer.values) != 1 || observer.values[0].Reason != "delivery_failed" {
		t.Fatal("missing alert/breaker fault notice")
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "committed") || strings.Contains(string(data), "before") || strings.Contains(string(data), "after") {
		t.Fatal("dead letter retained record values")
	}
	meta, _ := f.store.Meta(f.ctx)
	if meta.Head.String() != "1" {
		t.Fatal("post failure undid commit")
	}
	late := false
	b.Handler = handlerFunc(func(_ context.Context, item core.Delivery) error {
		late = item.Late
		if item.Key.String() != firstKey {
			t.Error("post replay key changed")
		}
		return nil
	})
	if _, err = f.dispatcher(t).Step(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	if !late {
		t.Fatal("recovered post callback missing late marker")
	}
	state, _, err = f.checkpoints(t).Load(f.ctx, b.ID)
	if err != nil || state.Failure != nil || state.Position.Revision.String() != "1" {
		t.Fatalf("recovered=%+v %v", state, err)
	}
}

func TestPostBindingsRunInParallelAndTimeoutDeadLetters(t *testing.T) {
	f := newFixture(t)
	set := f.commit(t, time.Time{}, bead("pl-a", "a"))
	started := make(chan string, 2)
	release := make(chan struct{})
	done := make(chan []model.Warning, 1)
	bindings := []core.Binding{}
	for _, id := range []string{"post-a", "post-b"} {
		bindings = append(bindings, core.Binding{ID: id, Class: core.Post, Principal: request().Principal, Handler: handlerFunc(func(ctx context.Context, item core.Delivery) error {
			started <- item.Key.Binding
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})})
	}
	d := f.dispatcher(t)
	go func() { done <- d.RunPost(f.ctx, bindings, set.Revision) }()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-f.ctx.Done():
			t.Fatal("post bindings did not start in parallel")
		}
	}
	close(release)
	if warnings := <-done; len(warnings) != 0 {
		t.Fatalf("parallel post=%+v", warnings)
	}
	slow := core.Binding{ID: "post-timeout", Class: core.Post, Principal: request().Principal, Timeout: 5 * time.Millisecond, Handler: handlerFunc(func(ctx context.Context, _ core.Delivery) error { <-ctx.Done(); return ctx.Err() })}
	if warnings := d.RunPost(f.ctx, []core.Binding{slow}, set.Revision); len(warnings) != 1 {
		t.Fatal("timeout missing committed warning")
	}
	state, _, err := f.checkpoints(t).Load(f.ctx, slow.ID)
	if err != nil || state.Failure == nil || !state.Failure.DeadLetter {
		t.Fatalf("timeout dead letter=%+v %v", state, err)
	}
}

func TestPoisonListenerRequiresAuditedAuthorizedSkip(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-a", "a"), bead("pl-b", "b"))
	calls := 0
	b := core.Binding{ID: "poison-listener", Class: core.Listener, Principal: request().Principal, MaxAttempts: 2, Handler: handlerFunc(func(_ context.Context, item core.Delivery) error {
		calls++
		if item.Partition == "bead/pl-a" {
			return errors.New("poison")
		}
		return nil
	})}
	d := f.dispatcher(t)
	for i := 0; i < 2; i++ {
		if _, err := d.Step(f.ctx, b); err == nil {
			t.Fatal("poison callback succeeded")
		}
	}
	if _, err := f.dispatcher(t).Step(f.ctx, b); !errors.Is(err, core.ErrHalted) {
		t.Fatalf("persisted halt=%v", err)
	}
	state, _, err := f.checkpoints(t).Load(f.ctx, b.ID)
	if err != nil || state.Failure == nil || state.Failure.Attempts != 2 || !state.Failure.DeadLetter || !state.Position.Revision.IsZero() {
		t.Fatalf("poison state=%+v %v", state, err)
	}
	if err = d.Skip(f.ctx, b, adminUser(), "reviewed poison"); !errors.Is(err, core.ErrSkipForbidden) {
		t.Fatal("skip without admin port allowed")
	}
	d.Admin = admin()
	if err = d.Skip(f.ctx, b, request().Principal, "reviewed poison"); err == nil {
		t.Fatal("unauthorized skip allowed")
	}
	if err = d.Skip(f.ctx, b, adminUser(), "reviewed poison"); err != nil {
		t.Fatal(err)
	}
	state, _, err = f.checkpoints(t).Load(f.ctx, b.ID)
	if err != nil || len(state.Skips) != 1 || state.Skips[0].Principal.ID != "admin" || state.Skips[0].Key.Index.String() != "0" {
		t.Fatalf("skip audit=%+v %v", state, err)
	}
	if _, err = d.Step(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("calls=%d want poison twice then b", calls)
	}
	state, _, _ = f.checkpoints(t).Load(f.ctx, b.ID)
	if state.Position.Index.String() != "1" || state.Halted {
		t.Fatal("remaining changeset event lost")
	}
}

func TestProjectionWholeChangesetHaltsAndNeverSkips(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-b", "b"), bead("pl-a", "a"))
	f.commit(t, time.Time{}, bead("pl-c", "c"))
	var batches []core.ChangeDelivery
	fail := true
	b := core.Binding{ID: "projection", Class: core.Projection, Principal: request().Principal, Projector: projectorFunc(func(_ context.Context, group core.ChangeDelivery) error {
		batches = append(batches, group)
		if fail {
			return errors.New("projection broken")
		}
		return nil
	})}
	d := f.dispatcher(t)
	d.Admin = admin()
	if _, err := d.Step(f.ctx, b); err == nil {
		t.Fatal("projection failure became success")
	}
	if len(batches) != 1 || len(batches[0].Events) != 2 || batches[0].Events[0].Key.Index.String() != "0" || batches[0].Events[1].Key.Index.String() != "1" {
		t.Fatalf("changeset batch=%+v", batches)
	}
	state, _, err := f.checkpoints(t).Load(f.ctx, b.ID)
	if err != nil || !state.Halted || !state.Position.Revision.IsZero() {
		t.Fatalf("projection checkpoint=%+v %v", state, err)
	}
	if err = d.Skip(f.ctx, b, adminUser(), "skip poison"); !errors.Is(err, core.ErrSkipForbidden) {
		t.Fatal("projection skip allowed")
	}
	if _, err = f.dispatcher(t).Step(f.ctx, b); !errors.Is(err, core.ErrHalted) {
		t.Fatal("projection failed to retain halt")
	}
	if len(batches) != 1 {
		t.Fatal("halted projection continued")
	}
	if err = d.Resume(f.ctx, b, adminUser()); err != nil {
		t.Fatal(err)
	}
	fail = false
	if _, err = d.Step(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || !batches[1].Late || batches[1].Events[0].Key.String() != batches[0].Events[0].Key.String() {
		t.Fatal("projection did not replay whole failed changeset")
	}
	if _, err = d.Step(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 3 || batches[2].Revision.String() != "2" {
		t.Fatal("projection violated total changeset order")
	}
}

func TestProjectionCannotPassPoisonMaterialization(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-good", "good"))
	f.commit(t, time.Time{}, change{id: "unknown", kind: "unregistered", op: model.OpCreate, fields: map[string]any{"value": "poison"}})
	f.commit(t, time.Time{}, bead("pl-later", "later"))
	calls := 0
	b := core.Binding{ID: "projection-poison", Class: core.Projection, Principal: request().Principal, Projector: projectorFunc(func(context.Context, core.ChangeDelivery) error { calls++; return nil })}
	d := f.dispatcher(t)
	if _, err := d.Step(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Step(f.ctx, b); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("poison=%v", err)
	}
	if _, err := f.dispatcher(t).Step(f.ctx, b); !errors.Is(err, core.ErrHalted) {
		t.Fatalf("persistent poison halt=%v", err)
	}
	state, _, err := f.checkpoints(t).Load(f.ctx, b.ID)
	if err != nil || state.Position.Revision.String() != "1" || calls != 1 {
		t.Fatalf("poison skipped=%+v calls=%d %v", state, calls, err)
	}
}

func TestListenerAndProjectionLagStallsPersistUntilOperatorActs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		class  core.Class
		age    time.Duration
		stalls bool
	}{
		{"listener-before", core.Listener, 7*24*time.Hour - time.Second, false},
		{"listener-at", core.Listener, 7 * 24 * time.Hour, true},
		{"projection-before", core.Projection, 30*24*time.Hour - time.Second, false},
		{"projection-at", core.Projection, 30 * 24 * time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			now := time.Now().UTC()
			f.commit(t, now.Add(-tc.age), bead("pl-old", "old"))
			calls := 0
			b := core.Binding{ID: "lag", Class: tc.class, Principal: request().Principal, Handler: handlerFunc(func(context.Context, core.Delivery) error { calls++; return nil }), Projector: projectorFunc(func(context.Context, core.ChangeDelivery) error { calls++; return nil })}
			d := f.dispatcher(t)
			d.Now = func() time.Time { return now }
			d.Admin = admin()
			_, err := d.Step(f.ctx, b)
			if !tc.stalls {
				if err != nil || calls != 1 {
					t.Fatalf("before threshold calls=%d %v", calls, err)
				}
				return
			}
			if !errors.Is(err, core.ErrStalled) || calls != 0 {
				t.Fatalf("stall calls=%d %v", calls, err)
			}
			restarted := f.dispatcher(t)
			if _, err = restarted.Step(f.ctx, b); !errors.Is(err, core.ErrStalled) {
				t.Fatal("stall did not persist")
			}
			if err = d.Resume(f.ctx, b, adminUser()); err != nil {
				t.Fatal(err)
			}
			if _, err = d.Step(f.ctx, b); err != nil || calls != 1 {
				t.Fatalf("operator resume=%d %v", calls, err)
			}
		})
	}
}

func TestLostLeaseCannotAcknowledgeSideEffect(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-a", "a"))
	oldLease := f.lease
	var replacement driver.ActiveLease
	var keys []string
	b := core.Binding{ID: "lease-listener", Class: core.Listener, Principal: request().Principal, Handler: handlerFunc(func(ctx context.Context, item core.Delivery) error {
		keys = append(keys, item.Key.String())
		if len(keys) == 1 {
			if err := oldLease.Release(ctx); err != nil {
				return err
			}
			var err error
			replacement, err = f.store.Lease(ctx, "replacement", driver.LeaseTTL)
			return err
		}
		return nil
	})}
	d := f.dispatcher(t)
	if _, err := d.Step(f.ctx, b); err == nil {
		t.Fatal("lost lease acknowledged callback")
	}
	state, generation, err := f.checkpoints(t).Load(f.ctx, b.ID)
	if err != nil || generation != 0 {
		t.Fatalf("stale acknowledgement=%+v %d %v", state, generation, err)
	}
	_, err = f.checkpoints(t).Save(f.ctx, b.ID, 0, core.State{Class: core.Listener, Position: core.Position{Incarnation: f.incarnation, Revision: "1"}})
	var fenced driver.ErrFenced
	if !errors.As(err, &fenced) {
		t.Fatalf("old lease Save=%v", err)
	}
	f.lease = replacement
	if _, err = f.dispatcher(t).Step(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatal("lease takeover did not replay stable key")
	}
}

func TestCheckpointCapabilityCASAndCorruptionFailExplicitly(t *testing.T) {
	f := newFixture(t)
	if _, err := NewCheckpoints(struct{ ReadStore }{f.store}, f.lease); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("missing capability=%v", err)
	}
	cp := f.checkpoints(t)
	state := core.State{Class: core.Listener, Position: core.Position{Incarnation: f.incarnation}}
	if _, err := cp.Save(f.ctx, "cas", 0, state); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.Save(f.ctx, "cas", 0, state); !errors.Is(err, driver.ErrCheckpointConflict) {
		t.Fatalf("CAS=%v", err)
	}
	if _, err := f.store.SaveCheckpoint(f.ctx, f.lease, "events:corrupt", 0, []byte(`{"not":"a checkpoint"}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cp.Load(f.ctx, "corrupt"); err == nil {
		t.Fatal("corrupt persisted checkpoint became initial state")
	}
	if _, _, err := cp.Load(f.ctx, " bad-key "); err == nil {
		t.Fatal("noncanonical key accepted")
	}
}

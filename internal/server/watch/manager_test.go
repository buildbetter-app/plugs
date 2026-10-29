package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type watchFixture struct {
	ctx         context.Context
	store       *memory.Store
	lease       driver.ActiveLease
	incarnation string
	auth        *toggleAuthorization
	manager     *Manager
}

func newWatchFixture(t *testing.T, queue int, coalesce, maxLag time.Duration) *watchFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	store, err := memory.NewWithOptions(memory.Options{Incarnation: "watch-incarnation-0001"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "watch-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	auth := &toggleAuthorization{allowed: true}
	reader := &coreevents.Reader{Log: serverevents.Log{Store: store}, Materializer: serverevents.HistoryMaterializer{Store: store}, Authorization: auth}
	codec, err := serverevents.NewCursorCodec([]byte(strings.Repeat("watch-test-key-", 3)))
	if err != nil {
		t.Fatal(err)
	}
	service := &serverevents.Service{Reader: reader, Cursors: codec}
	manager, err := New(ctx, Config{Events: service, QueueSize: queue, Coalesce: coalesce, MaxLag: maxLag})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return &watchFixture{ctx: ctx, store: store, lease: lease, incarnation: meta.Incarnation, auth: auth, manager: manager}
}

func (f *watchFixture) request(id string) Request {
	return Request{SessionID: "session-1", SubscriptionID: id, Method: MethodEventSubscribe, Principal: model.PrincipalRef{Kind: "user", ID: "reader"}, Ontology: "1", PolicyVersion: "policy-1"}
}

func (f *watchFixture) open(t *testing.T, request Request) *Subscription {
	t.Helper()
	subscription, err := f.manager.Open(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	return subscription
}

func (f *watchFixture) commit(t *testing.T, id, title string, op model.OpKind) model.Changeset {
	t.Helper()
	body, err := json.Marshal(map[string]any{"title": title})
	if err != nil {
		t.Fatal(err)
	}
	set, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Incarnation: f.incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "user", ID: "writer"}}, []conformance.FixtureChange{{Ref: model.RefByID(id, "0"), Kind: "bead", Op: op, Body: body, ChangedPaths: []string{"/title"}}})
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func nextFrame(t *testing.T, subscription *Subscription, timeout time.Duration) Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	frame, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestSnapshotThenDeltaUsesActualMemoryLogAndExactCursor(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	subscription := f.open(t, f.request("snapshot-delta"))
	snapshot := nextFrame(t, subscription, time.Second)
	if snapshot.Kind != FrameSnapshot || snapshot.Cursor == "" || len(snapshot.Events) != 0 {
		t.Fatalf("initial snapshot=%+v", snapshot)
	}
	set := f.commit(t, "bead-race", "created", model.OpCreate)
	delta := nextFrame(t, subscription, time.Second)
	if delta.Kind != FrameDelta || delta.FromCursor != snapshot.Cursor || delta.Cursor == snapshot.Cursor || len(delta.Events) != 1 {
		t.Fatalf("delta=%+v snapshot=%+v", delta, snapshot)
	}
	if delta.Events[0].Durable == nil || delta.Events[0].Durable.Resource != "bead/bead-race" || delta.Events[0].Durable.Revision.String() != set.Revision.String() {
		t.Fatalf("actual committed event=%+v", delta.Events[0])
	}

	if err := subscription.Close(); err != nil {
		t.Fatal(err)
	}
	replay := f.request("replay")
	replay.From = snapshot.Cursor
	replayed := f.open(t, replay)
	replaySnapshot := nextFrame(t, replayed, time.Second)
	if replaySnapshot.Kind != FrameSnapshot || replaySnapshot.Cursor != delta.Cursor || len(replaySnapshot.Events) != 1 || replaySnapshot.Events[0].Durable.Resource != "bead/bead-race" {
		t.Fatalf("exact cursor replay=%+v want cursor %q", replaySnapshot, delta.Cursor)
	}
}

func TestSnapshotWriteRaceIsCoveredByReturnedCursor(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	gate := &scanGate{inner: f.manager.config.Events.Reader.Log, started: make(chan struct{}), release: make(chan struct{})}
	f.manager.config.Events.Reader.Log = gate
	opened := make(chan struct {
		sub *Subscription
		err error
	}, 1)
	go func() {
		sub, err := f.manager.Open(f.ctx, f.request("scan-race"))
		opened <- struct {
			sub *Subscription
			err error
		}{sub: sub, err: err}
	}()
	select {
	case <-gate.started:
	case <-time.After(time.Second):
		t.Fatal("snapshot scan did not reach the gate")
	}
	set := f.commit(t, "bead-scan-race", "raced", model.OpCreate)
	close(gate.release)
	result := <-opened
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.sub.Close()
	frame := nextFrame(t, result.sub, time.Second)
	if frame.Kind != FrameSnapshot || len(frame.Events) != 1 || frame.Events[0].Durable == nil || frame.Events[0].Durable.Revision.String() != set.Revision.String() {
		t.Fatalf("snapshot race frame=%+v", frame)
	}
}

func TestAuthorizationRevokeRevalidatesQueuedDelta(t *testing.T) {
	f := newWatchFixture(t, 8, 5*time.Millisecond, time.Second)
	subscription := f.open(t, f.request("revoke"))
	_ = nextFrame(t, subscription, time.Second)
	f.commit(t, "bead-secret", "secret", model.OpCreate)
	// Let the actual reader enqueue the delta, then revoke before delivery.
	time.Sleep(30 * time.Millisecond)
	f.auth.SetAllowed(false)
	frame := nextFrame(t, subscription, time.Second)
	if frame.Kind != FrameDelta || frame.Reason != "visibility_changed" || len(frame.Events) != 1 || frame.Events[0].Control == nil || frame.Events[0].Control.Type != model.EventVisibilityLost {
		t.Fatalf("revoked queued frame=%+v", frame)
	}
	if frame.Events[0].Durable != nil || frame.Events[0].Control.Resource != "bead/bead-secret" {
		t.Fatalf("revocation leaked image=%+v", frame.Events[0])
	}
}

func TestOverflowEmitsResetAtNewestCursor(t *testing.T) {
	f := newWatchFixture(t, 1, 5*time.Millisecond, time.Second)
	subscription := f.open(t, f.request("overflow"))
	_ = nextFrame(t, subscription, time.Second)
	for i := 0; i < 4; i++ {
		f.commit(t, fmt.Sprintf("bead-overflow-%d", i), fmt.Sprintf("v-%d", i), model.OpCreate)
		time.Sleep(20 * time.Millisecond)
	}
	frame := nextFrame(t, subscription, time.Second)
	if frame.Kind != FrameReset || frame.Reason != "overflow" || frame.Cursor == "" || frame.position.Validate() != nil {
		t.Fatalf("overflow frame=%+v", frame)
	}
	if frame.Cursor != subscription.Cursor() {
		t.Fatalf("reset cursor=%q subscription cursor=%q", frame.Cursor, subscription.Cursor())
	}
}

func TestLagEmitsBoundedResetAndCancellationReleasesSession(t *testing.T) {
	f := newWatchFixture(t, 4, 5*time.Millisecond, 10*time.Millisecond)
	subscription := f.open(t, f.request("lag"))
	time.Sleep(30 * time.Millisecond)
	frame := nextFrame(t, subscription, time.Second)
	if frame.Kind != FrameReset || frame.Reason != "lag" {
		t.Fatalf("lag frame=%+v", frame)
	}
	if got := f.manager.WatchCount("session-1"); got != 1 {
		t.Fatalf("watch count before close=%d", got)
	}
	if err := f.manager.CloseSession(context.Background(), "session-1"); err != nil {
		t.Fatal(err)
	}
	if got := f.manager.WatchCount("session-1"); got != 0 {
		t.Fatalf("watch count after close=%d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := subscription.Next(ctx); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("closed subscription err=%v", err)
	}
	if err := f.manager.CloseSession(nil, "session-1"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil close context err=%v", err)
	}
}

func TestSessionBoundAndViewCheckpointRefusal(t *testing.T) {
	f := newWatchFixture(t, 64, 5*time.Millisecond, time.Second)
	opened := make([]*Subscription, 0, MaxWatchesPerSession)
	for i := 0; i < MaxWatchesPerSession; i++ {
		request := f.request(fmt.Sprintf("bound-%d", i))
		request.Resource = fmt.Sprintf("bead/bound-%d", i)
		request.Method = MethodBeadWatch
		opened = append(opened, f.open(t, request))
	}
	tooMany := f.request("bound-too-many")
	tooMany.Resource = "bead/bound-too-many"
	tooMany.Method = MethodBeadWatch
	if _, err := f.manager.Open(f.ctx, tooMany); !errors.Is(err, ErrTooManyWatches) {
		t.Fatalf("33rd watch err=%v", err)
	}
	for _, subscription := range opened {
		_ = subscription.Close()
	}
	view := f.request("view-refused")
	view.Method = MethodViewWatch
	view.ViewID = "view-1"
	if _, err := f.manager.Open(f.ctx, view); !errors.Is(err, ErrViewCheckpointRequired) {
		t.Fatalf("view without checkpoint err=%v", err)
	}
	if got := f.manager.WatchCount("session-1"); got != 0 {
		t.Fatalf("view refusal leaked watch=%d", got)
	}
}

type toggleAuthorization struct {
	mu      sync.Mutex
	allowed bool
}

func (a *toggleAuthorization) SetAllowed(allowed bool) {
	a.mu.Lock()
	a.allowed = allowed
	a.mu.Unlock()
}

func (a *toggleAuthorization) Read(_ context.Context, _ model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	a.mu.Lock()
	allowed := a.allowed
	a.mu.Unlock()
	if !allowed {
		return coreevents.Visibility{}, nil
	}
	return coreevents.Visibility{Readable: true, Before: event.Before, After: event.After}, nil
}

func (a *toggleAuthorization) ReadResource(context.Context, model.PrincipalRef, model.AuditResource) (coreevents.ResourceVisibility, error) {
	a.mu.Lock()
	allowed := a.allowed
	a.mu.Unlock()
	return coreevents.ResourceVisibility{Readable: allowed}, nil
}

var _ coreevents.Authorization = (*toggleAuthorization)(nil)
var _ coreevents.ResourceAuthorization = (*toggleAuthorization)(nil)

type scanGate struct {
	inner   coreevents.Log
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *scanGate) Head(ctx context.Context) (coreevents.Head, error) {
	return g.inner.Head(ctx)
}

func (g *scanGate) Scan(ctx context.Context, after coreevents.Position, limit int) ([]model.Changeset, error) {
	g.once.Do(func() { close(g.started) })
	select {
	case <-g.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.inner.Scan(ctx, after, limit)
}

var _ coreevents.Log = (*scanGate)(nil)

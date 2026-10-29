package postphase

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
)

func TestPartialNativePostFailurePersistsExactWarningWithoutRepeatingEffects(t *testing.T) {
	f := newFixture(t)
	var successes, failures atomic.Int32
	host := f.host(t,
		nativePost("success", func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
			successes.Add(1)
			return nil
		}),
		nativePost("failed", func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
			failures.Add(1)
			return errors.New("destination unavailable")
		}),
	)
	writer := f.writer(host, f.store, f.lease)
	meta := writeMeta(20)
	cs := create("warning", "warning")
	first := mustApply(t, f, writer, cs, meta)
	if len(first.Warnings) != 1 || first.Warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("partial failure commit=%+v", first)
	}
	assertJSONEqual(t, map[string]any{"binding": "post-failed", "revision": first.Revision}, first.Warnings[0].Details)
	entry := receipt(t, f.store, f.principal, meta)
	if entry.ResponsePending {
		t.Fatal("warning receipt remained pending")
	}
	assertJSONEqual(t, first, entry.Commit)
	assertJSONEqual(t, first, mustApply(t, f, writer, cs, meta))
	if successes.Load() != 1 || failures.Load() != 1 || head(t, f.store).String() != "1" {
		t.Fatalf("replay repeated side effects: success=%d failed=%d", successes.Load(), failures.Load())
	}
	if err := assertTitle(entry.Commit.Results[0].Record, "warning"); err != nil {
		t.Fatal(err)
	}
	// The direct native post adapter currently creates no delivery checkpoint.
	// Its durable warning must not be mistaken for a wired durable native queue.
	checkpoint, err := f.store.LoadCheckpoint(f.ctx, "events:post-failed")
	if err != nil || checkpoint.Generation != 0 {
		t.Fatalf("unexpected native delivery checkpoint=%+v err=%v", checkpoint, err)
	}
	t.Log("partial-failure proof: graph committed; only failed binding warned; warning bytes replay exactly; neither binding repeats; native checkpoint composition absent")
}

// This wrapper observes a real durable receipt read. It adds no receipt state,
// outcome, polling policy or finalization behavior.
type pendingReadStore struct {
	*memory.Store
	seen chan struct{}
	once sync.Once
}

func (s *pendingReadStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &pendingReadReader{Reader: r, owner: s}, nil
}

type pendingReadReader struct {
	driver.Reader
	owner *pendingReadStore
}

func (r *pendingReadReader) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	entry, err := r.Reader.IdempotencyByKey(ctx, principal, agent, method, key)
	if err == nil && entry != nil && entry.ResponsePending {
		r.owner.once.Do(func() { close(r.owner.seen) })
	}
	return entry, err
}

func TestPendingDuplicatesWaitForFinalReceiptAndCancelledReplayReturnsCommittedBusy(t *testing.T) {
	f := newFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var calls atomic.Int32
	host := f.host(t, nativePost("pending", func(ctx context.Context, _ pluginapi.Host, _ pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return errors.New("actual pending callback failure")
		case <-ctx.Done():
			return ctx.Err()
		}
	}))
	writer := f.writer(host, f.store, f.lease)
	meta := writeMeta(21)
	cs := create("pending", "pending")
	original := applyAsync(f.ctx, writer, f.principal, cs, meta)
	await(t, f.ctx, started)
	pending := receipt(t, f.store, f.principal, meta)
	if !pending.ResponsePending || len(pending.Commit.Warnings) != 0 {
		t.Fatalf("initial response should be pending=%+v", pending)
	}
	retained := openReader(t, f)
	watch := &pendingReadStore{Store: f.store, seen: make(chan struct{})}
	duplicate := applyAsync(f.ctx, f.writer(host, watch, f.lease), f.principal, cs, meta)
	await(t, f.ctx, watch.seen)
	select {
	case got := <-duplicate:
		t.Fatalf("pending duplicate returned before callback finished: %+v", got)
	default:
	}
	cancelWatch := &pendingReadStore{Store: f.store, seen: make(chan struct{})}
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	cancellable := applyAsync(ctx, f.writer(host, cancelWatch, f.lease), f.principal, cs, meta)
	await(t, f.ctx, cancelWatch.seen)
	cancel()
	assertPendingError(t, await(t, f.ctx, cancellable).err, meta, pending.Commit.Revision)
	if _, err := writer.Apply(f.ctx, f.principal, create("different", "pending"), meta); modelCode(err) != "idempotency_key_reused" {
		t.Fatalf("different request reused pending key: %v", err)
	}
	if calls.Load() != 1 || head(t, f.store).String() != "1" || !receipt(t, f.store, f.principal, meta).ResponsePending {
		t.Fatal("pending duplicate changed graph, repeated callback or finalized early")
	}
	once.Do(func() { close(release) })
	first, second := await(t, f.ctx, original), await(t, f.ctx, duplicate)
	if first.err != nil || second.err != nil || len(first.commit.Warnings) != 1 || calls.Load() != 1 {
		t.Fatalf("final responses first=%+v duplicate=%+v calls=%d", first, second, calls.Load())
	}
	entry := receipt(t, f.store, f.principal, meta)
	if entry.ResponsePending {
		t.Fatal("fresh receipt did not finalize")
	}
	assertJSONEqual(t, entry.Commit, first.commit)
	assertJSONEqual(t, entry.Commit, second.commit)
	assertJSONEqual(t, entry.Commit, mustApply(t, f, writer, cs, meta))
	principal, err := driver.PrincipalKey(f.principal)
	if err != nil {
		t.Fatal(err)
	}
	old, err := retained.IdempotencyByKey(f.ctx, principal, "", meta.Method, meta.IdempotencyKey)
	if err != nil || old == nil || !old.ResponsePending {
		t.Fatalf("retained pending snapshot changed: %+v err=%v", old, err)
	}
	t.Log("pending proof: waiting duplicate reads real pending receipt; cancelled duplicate returns retryable busy with committed identity; original/waiter/final replay bytes identical; one callback")
}

type finalizationAttempt struct {
	principal, agent, method, key string
	revision                      model.Revision
	warnings                      json.RawMessage
}

// Faults occur at the real finalizer boundary; every recovery outcome below
// is captured from Writer rather than invented by the fixture.
type faultyFinalizerStore struct {
	*memory.Store
	failBefore bool
	loseAck    bool
	mu         sync.Mutex
	attempts   []finalizationAttempt
}

func (s *faultyFinalizerStore) FinalizeResponse(ctx context.Context, lease driver.ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error {
	encoded, err := json.Marshal(warnings)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.attempts = append(s.attempts, finalizationAttempt{principal, agent, method, key, revision, encoded})
	s.mu.Unlock()
	if s.failBefore {
		return errors.New("injected failure before response CAS")
	}
	if err := s.Store.FinalizeResponse(ctx, lease, principal, agent, method, key, revision, warnings); err != nil {
		return err
	}
	if s.loseAck {
		return errors.New("injected lost response CAS acknowledgement")
	}
	return nil
}

func TestFinalizerTakeoverUsesCapturedNativeOutcomeAndFencesOldLease(t *testing.T) {
	f := newFixture(t)
	faulty := &faultyFinalizerStore{Store: f.store, failBefore: true}
	var posts atomic.Int32
	host := f.host(t, nativePost("takeover", func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
		posts.Add(1)
		return errors.New("actual destination failure")
	}))
	meta, cs := writeMeta(22), create("takeover", "takeover")
	_, err := f.writer(host, faulty, f.lease).Apply(f.ctx, f.principal, cs, meta)
	pending := receipt(t, f.store, f.principal, meta)
	assertPendingError(t, err, meta, pending.Commit.Revision)
	if !pending.ResponsePending || posts.Load() != 1 || len(pending.Commit.Warnings) != 0 {
		t.Fatalf("unresolved finalizer receipt=%+v posts=%d", pending, posts.Load())
	}
	if len(faulty.attempts) != 4 {
		t.Fatalf("expected bounded retry of known outcome: attempts=%d", len(faulty.attempts))
	}
	actual := faulty.attempts[0]
	for _, attempt := range faulty.attempts {
		if !reflect.DeepEqual(actual, attempt) {
			t.Fatalf("finalizer retry changed captured outcome: first=%+v attempt=%+v", actual, attempt)
		}
	}
	var warnings []model.Warning
	if err := json.Unmarshal(actual.warnings, &warnings); err != nil || len(warnings) != 1 || warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("captured actual outcome=%s err=%v", actual.warnings, err)
	}
	assertJSONEqual(t, map[string]any{"binding": "post-takeover", "revision": pending.Commit.Revision}, warnings[0].Details)
	retained := openReader(t, f)
	old := f.lease
	if err := old.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := f.store.Lease(f.ctx, "takeover-owner", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	f.lease = lease
	if lease.Epoch() <= old.Epoch() {
		t.Fatal("takeover did not increase epoch")
	}
	if err := f.store.FinalizeResponse(f.ctx, old, actual.principal, actual.agent, actual.method, actual.key, actual.revision, warnings); err == nil {
		t.Fatal("old epoch finalized receipt")
	} else {
		var fenced driver.ErrFenced
		if !errors.As(err, &fenced) {
			t.Fatalf("old epoch error=%v", err)
		}
	}
	assertJSONEqual(t, pending, receipt(t, f.store, f.principal, meta))
	if err := f.store.FinalizeResponse(f.ctx, lease, actual.principal, actual.agent, actual.method, actual.key, actual.revision, warnings); err != nil {
		t.Fatal(err)
	}
	entry := receipt(t, f.store, f.principal, meta)
	assertJSONEqual(t, warnings, entry.Commit.Warnings)
	assertJSONEqual(t, pending.Changeset, entry.Changeset)
	assertJSONEqual(t, pending.Commit.Results, entry.Commit.Results)
	if entry.ResponsePending || posts.Load() != 1 || head(t, f.store).String() != "1" {
		t.Fatalf("takeover repeated write/effect or remained pending=%+v", entry)
	}
	assertJSONEqual(t, entry.Commit, mustApply(t, f, f.writer(host, f.store, lease), cs, meta))
	ref := entry.Commit.Results[0].Ref
	advanced := mustApply(t, f, f.writer(nil, f.store, lease), patch(ref, "after-takeover"), writeMeta(24))
	if advanced.Revision.String() != "2" || retained.Snapshot().At.String() != "1" {
		t.Fatalf("takeover changed retained snapshot=%+v current=%s", retained.Snapshot(), advanced.Revision)
	}
	rows, err := retained.Get(f.ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 || rows[0].Revision.String() != "1" {
		t.Fatalf("retained reader after takeover=%+v err=%v", rows, err)
	}
	if err := assertTitle(rows[0].Body, "takeover"); err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, entry, receipt(t, f.store, f.principal, meta))
	t.Logf("finalizer proof: %d failed CAS attempts with identical captured outcome; epoch %d fenced by %d; recovery preserves receipt/log/results; native posts=%d", len(faulty.attempts), old.Epoch(), lease.Epoch(), posts.Load())
}

func TestFinalizerLostAcknowledgementReloadsDurableNativeWarning(t *testing.T) {
	f := newFixture(t)
	faulty := &faultyFinalizerStore{Store: f.store, loseAck: true}
	var posts atomic.Int32
	host := f.host(t, nativePost("lostack", func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
		posts.Add(1)
		return errors.New("actual post failure before lost finalizer acknowledgement")
	}))
	meta := writeMeta(23)
	commit := mustApply(t, f, f.writer(host, faulty, f.lease), create("lostack", "lostack"), meta)
	entry := receipt(t, f.store, f.principal, meta)
	if len(faulty.attempts) != 1 || entry.ResponsePending || len(commit.Warnings) != 1 || posts.Load() != 1 {
		t.Fatalf("lost ack attempts=%d receipt=%+v posts=%d", len(faulty.attempts), entry, posts.Load())
	}
	assertJSONEqual(t, entry.Commit, commit)
	t.Log("lost finalizer ack proof: persisted warning reloaded before success; one callback and one response CAS")
}

var _ driver.ResponseFinalizer = (*faultyFinalizerStore)(nil)

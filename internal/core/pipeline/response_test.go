package pipeline

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
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type applyResult struct {
	commit model.Commit
	err    error
}

func awaitResult(t *testing.T, result <-chan applyResult) applyResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("write did not finish within its bounded window")
		return applyResult{}
	}
}

func assertSameResponses(t *testing.T, results ...applyResult) {
	t.Helper()
	want, _ := json.Marshal(results[0].commit)
	for _, result := range results {
		got, _ := json.Marshal(result.commit)
		if result.err != nil || string(got) != string(want) {
			t.Fatalf("response=%s err=%v want=%s", got, result.err, want)
		}
	}
}

func receipt(t *testing.T, h *harness, m model.WriteMeta) *driver.IdempotencyRecord {
	t.Helper()
	principal, _ := driver.PrincipalKey(by)
	entry, err := h.read(t).IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey)
	if err != nil || entry == nil {
		t.Fatalf("receipt=%+v err=%v", entry, err)
	}
	return entry
}

func assertSingleGraphWrite(t *testing.T, h *harness, commit model.Commit) {
	t.Helper()
	r := h.read(t)
	log, err := r.Changesets(context.Background(), nil, 100)
	if err != nil || len(log) != 1 || r.Snapshot().At.String() != "1" {
		t.Fatalf("head=%s log=%+v err=%v", r.Snapshot().At, log, err)
	}
	versions, err := r.Versions(context.Background(), commit.Results[0].Ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 1 {
		t.Fatalf("versions=%+v err=%v", versions, err)
	}
}

func TestPendingPostReplayWaitsForDurableFailureWarning(t *testing.T) {
	h := setup(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var posts atomic.Int32
	w := h.writer(WithPostSubscribers(PostBinding{ID: "post", Subscriber: postFunc(func(ctx context.Context, _ port.CommittedChange, _ port.SnapshotReader) error {
		posts.Add(1)
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return errors.New("held failing post")
	})}))
	cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)
	original := make(chan applyResult, 1)
	go func() { c, e := w.Apply(context.Background(), by, cs, m); original <- applyResult{c, e} }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("post did not start")
	}
	initial := receipt(t, h, m)
	if !initial.ResponsePending || len(initial.Commit.Warnings) != 0 {
		t.Fatalf("post was not published pending: %+v", initial)
	}
	duplicate := make(chan applyResult, 1)
	go func() { c, e := h.writer().Apply(context.Background(), by, cs, m); duplicate <- applyResult{c, e} }()
	select {
	case got := <-duplicate:
		t.Fatalf("duplicate returned before held post outcome was durable: %+v", got)
	case <-time.After(40 * time.Millisecond):
	}
	changed := model.ChangeSet{Ops: []model.Op{create("")}}
	changed.Ops[0].Create.Fields["title"] = "different request"
	_, err := h.writer().Apply(context.Background(), by, changed, m)
	code(t, err, "idempotency_key_reused")
	// A caller can cancel its wait without running any callbacks or losing the
	// durable graph outcome; the original caller still finishes normally.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = h.writer().Apply(ctx, by, cs, m)
	code(t, err, "busy")
	once.Do(func() { close(release) })
	a, b := awaitResult(t, original), awaitResult(t, duplicate)
	c, err := h.writer().Apply(context.Background(), by, cs, m)
	assertSameResponses(t, a, b, applyResult{c, err})
	if len(a.commit.Warnings) != 1 || a.commit.Warnings[0].Code != "post_subscriber_failed" || posts.Load() != 1 || receipt(t, h, m).ResponsePending {
		t.Fatalf("final=%+v posts=%d", a, posts.Load())
	}
	assertSingleGraphWrite(t, h, a.commit)
}

// This fault boundary delegates every graph/read operation to actual memory.
// It injects a transport failure before or after the real finalization CAS.
type finalizerFault struct {
	*memory.Store
	calls   atomic.Int32
	persist bool
	always  bool
	post    []model.Warning
}

func (s *finalizerFault) FinalizeResponse(ctx context.Context, lease driver.ActiveLease, principal, agent, method, key string, rev model.Revision, warnings []model.Warning) error {
	n := s.calls.Add(1)
	s.post, _ = clone(warnings)
	if n == 1 || s.always {
		if s.persist {
			if err := s.Store.FinalizeResponse(ctx, lease, principal, agent, method, key, rev, warnings); err != nil {
				return err
			}
		}
		return errors.New("finalization transport failure")
	}
	return s.Store.FinalizeResponse(ctx, lease, principal, agent, method, key, rev, warnings)
}

func TestFinalizerFailureAfterCommitReturnsOnlyDurableResponse(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure-before-CAS", true: "lost-CAS-acknowledgment"}[persist], func(t *testing.T) {
			h := setup(t)
			fault := &finalizerFault{Store: h.store, persist: persist}
			var posts atomic.Int32
			w := h.writer(WithPostSubscribers(PostBinding{ID: "post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
				posts.Add(1)
				return errors.New("post failed")
			})}))
			w.store = fault
			cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)
			first, err := w.Apply(context.Background(), by, cs, m)
			replayed, replayErr := h.writer().Apply(context.Background(), by, cs, m)
			assertSameResponses(t, applyResult{first, err}, applyResult{replayed, replayErr})
			if len(first.Warnings) != 1 || posts.Load() != 1 || !persist && fault.calls.Load() < 2 || receipt(t, h, m).ResponsePending {
				t.Fatalf("response=%+v posts=%d finalizations=%d", first, posts.Load(), fault.calls.Load())
			}
			assertSingleGraphWrite(t, h, first)
		})
	}
}

func TestUnresolvedFinalizerFailureRetainsRecoverableReceipt(t *testing.T) {
	h := setup(t)
	fault := &finalizerFault{Store: h.store, always: true}
	var posts atomic.Int32
	w := h.writer(WithPostSubscribers(PostBinding{ID: "post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
		posts.Add(1)
		return errors.New("post failed")
	})}))
	w.store = fault
	cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)
	result, err := w.Apply(context.Background(), by, cs, m)
	code(t, err, "busy")
	if !reflect.DeepEqual(result, model.Commit{}) {
		t.Fatalf("failed finalization returned a success result: %+v", result)
	}
	entry := receipt(t, h, m)
	if !entry.ResponsePending || len(entry.Commit.Warnings) != 0 || posts.Load() != 1 || fault.calls.Load() > 4 {
		t.Fatalf("unresolved receipt=%+v posts=%d finalizations=%d", entry, posts.Load(), fault.calls.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err = h.writer().Apply(ctx, by, cs, m)
	code(t, err, "busy")
	if posts.Load() != 1 {
		t.Fatal("pending replay reran the callback")
	}
	// An active holder can retry the same known post outcome after lease
	// takeover. This is receipt recovery, not a second graph write or callback.
	if err := h.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := h.store.Lease(context.Background(), "recovery", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	principal, _ := driver.PrincipalKey(by)
	if err := h.store.FinalizeResponse(context.Background(), lease, principal, "", m.Method, m.IdempotencyKey, entry.Commit.Revision, fault.post); err != nil {
		t.Fatal(err)
	}
	recovered := New(h.store, lease, WithOntology(testOntology{}))
	a, ae := recovered.Apply(context.Background(), by, cs, m)
	b, be := recovered.Apply(context.Background(), by, cs, m)
	assertSameResponses(t, applyResult{a, ae}, applyResult{b, be})
	if len(a.Warnings) != 1 || posts.Load() != 1 || receipt(t, h, m).ResponsePending {
		t.Fatalf("recovery=%+v posts=%d", a, posts.Load())
	}
	assertSingleGraphWrite(t, h, a)
}

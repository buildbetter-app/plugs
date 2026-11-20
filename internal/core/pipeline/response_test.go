package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
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
	originalReplay := make(chan bool, 1)
	go func() {
		outcome, err := w.ApplyWithOutcome(context.Background(), by, cs, m)
		originalReplay <- outcome.Replayed
		original <- applyResult{outcome.Commit, err}
	}()
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
	duplicateReplay := make(chan bool, 1)
	go func() {
		outcome, err := h.writer().ApplyWithOutcome(context.Background(), by, cs, m)
		duplicateReplay <- outcome.Replayed
		duplicate <- applyResult{outcome.Commit, err}
	}()
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
	if <-originalReplay || !<-duplicateReplay {
		t.Fatal("own finalization must be fresh; waiting receipt replay must be marked")
	}
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
	ctx, cancel := context.WithCancel(context.Background())
	ctx = withResponsePauseHook(ctx, cancel)
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

func TestWriteOutcomeDistinguishesNoopDryRunAndStoredReplay(t *testing.T) {
	h := setup(t)
	w := h.writer()
	cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)
	first, err := w.ApplyWithOutcome(context.Background(), by, cs, m)
	if err != nil || first.Replayed || first.Commit.Revision.String() != "1" {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	replay, err := h.writer().ApplyWithOutcome(context.Background(), by, cs, m)
	assertSameResponses(t, applyResult{first.Commit, nil}, applyResult{replay.Commit, err})
	if !replay.Replayed {
		t.Fatal("stored graph response was not marked replayed")
	}

	noop := model.ChangeSet{Ops: []model.Op{patch(first.Commit.Results[0].Ref, map[string]any{"title": "initial"})}}
	unchanged, err := w.ApplyWithOutcome(context.Background(), by, noop, meta(2))
	if err != nil || unchanged.Replayed || unchanged.Commit.Revision.String() != "1" || receipt(t, h, meta(2)).ResponsePending {
		t.Fatalf("fresh same-revision noop=%+v err=%v", unchanged, err)
	}
	apply(t, w, cs, meta(3))
	noopReplay, err := h.writer().ApplyWithOutcome(context.Background(), by, noop, meta(2))
	assertSameResponses(t, applyResult{unchanged.Commit, nil}, applyResult{noopReplay.Commit, err})
	if !noopReplay.Replayed {
		t.Fatal("stored noop response was not marked replayed after head advanced")
	}

	dryMeta := meta(4)
	dryMeta.DryRun = true
	for i := 0; i < 2; i++ {
		dry, err := w.ApplyWithOutcome(context.Background(), by, cs, dryMeta)
		if err != nil || dry.Replayed || !dry.Commit.WouldCommit || dry.Commit.Revision.String() != "2" {
			t.Fatalf("dry run=%+v err=%v", dry, err)
		}
	}
	principal, _ := driver.PrincipalKey(by)
	if entry, err := h.read(t).IdempotencyByKey(context.Background(), principal, "", dryMeta.Method, dryMeta.IdempotencyKey); err != nil || entry != nil {
		t.Fatalf("dry run stored receipt=%+v err=%v", entry, err)
	}
	// A dry run never claims the key or creates a graph revision. The actual
	// execution is fresh even though it uses that same request key.
	dryMeta.DryRun = false
	actual, err := w.ApplyWithOutcome(context.Background(), by, cs, dryMeta)
	if err != nil || actual.Replayed || actual.Commit.Revision.String() != "3" {
		t.Fatalf("execution after dry run=%+v err=%v", actual, err)
	}
	log, err := h.read(t).Changesets(context.Background(), nil, 100)
	if err != nil || len(log) != 3 {
		t.Fatalf("noop/dry-run published graph entries=%+v err=%v", log, err)
	}
}

func TestLeaseLossDuringPostLeavesRecoverablePendingResponse(t *testing.T) {
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
			return errors.New("post failed after lease takeover")
		case <-ctx.Done():
			return ctx.Err()
		}
	})}))
	cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)
	result := make(chan applyResult, 1)
	go func() { c, e := w.Apply(context.Background(), by, cs, m); result <- applyResult{c, e} }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("post did not start")
	}
	if err := h.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := h.store.Lease(context.Background(), "takeover", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	got := awaitResult(t, result)
	code(t, got.err, "busy")
	var failure *model.Error
	if !errors.As(got.err, &failure) || !failure.Data.Retryable || failure.Data.Details["responsePending"] != true || failure.Data.Details["idempotencyKey"] != m.IdempotencyKey || !reflect.DeepEqual(got.commit, model.Commit{}) {
		t.Fatalf("fenced response=%+v", got)
	}
	entry := receipt(t, h, m)
	if !entry.ResponsePending || len(entry.Commit.Warnings) != 0 || failure.Data.Details["committedRevision"] != entry.Commit.Revision {
		t.Fatalf("fenced receipt=%+v error=%+v", entry, failure)
	}
	// The active holder knows the completed post outcome and can finalize that
	// same receipt. Recovery never re-executes a callback or graph transaction.
	warnings := []model.Warning{{Code: "post_subscriber_failed", Message: "post subscriber failed after commit", Details: map[string]any{"binding": "post", "revision": entry.Commit.Revision}}}
	principal, _ := driver.PrincipalKey(by)
	if err := h.store.FinalizeResponse(context.Background(), lease, principal, "", m.Method, m.IdempotencyKey, entry.Commit.Revision, warnings); err != nil {
		t.Fatal(err)
	}
	recovered := New(h.store, lease, WithOntology(testOntology{}))
	a, ae := recovered.ApplyWithOutcome(context.Background(), by, cs, m)
	b, be := recovered.ApplyWithOutcome(context.Background(), by, cs, m)
	assertSameResponses(t, applyResult{a.Commit, ae}, applyResult{b.Commit, be})
	if !a.Replayed || !b.Replayed || len(a.Commit.Warnings) != 1 || posts.Load() != 1 || receipt(t, h, m).ResponsePending {
		t.Fatalf("takeover recovery=%+v posts=%d", a, posts.Load())
	}
	assertSingleGraphWrite(t, h, a.Commit)
}

// Report an uncertain outcome only after the real driver durably publishes.
// Cancelling the transaction context also proves resolution does not reuse it.
type indeterminateCommitStore struct {
	driver.Store
	calls             int
	abortThen         func()
	resolutionBounded bool
}

func (s *indeterminateCommitStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	if s.calls > 0 {
		deadline, ok := ctx.Deadline()
		s.resolutionBounded = ok && time.Until(deadline) <= 2*time.Second && ctx.Err() == nil
	}
	return s.Store.Read(ctx, view)
}

func (s *indeterminateCommitStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	txCtx, cancel := context.WithCancel(ctx)
	tx, err := s.Store.Write(txCtx, lease)
	if err != nil {
		cancel()
		return nil, err
	}
	return &indeterminateCommitTx{WriteTx: tx, store: s, cancel: cancel}, nil
}

type indeterminateCommitTx struct {
	driver.WriteTx
	store  *indeterminateCommitStore
	cancel context.CancelFunc
}

func (tx *indeterminateCommitTx) Close() error {
	defer tx.cancel()
	return tx.WriteTx.Close()
}

func (tx *indeterminateCommitTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	if tx.store.abortThen != nil {
		if err := tx.WriteTx.Rollback(ctx); err != nil {
			return model.Changeset{}, err
		}
		tx.store.calls++
		tx.store.abortThen()
		return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: header.IdempotencyKey}
	}
	_, err := tx.WriteTx.Commit(ctx, header)
	if err != nil {
		return model.Changeset{}, err
	}
	tx.store.calls++
	tx.cancel()
	return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: header.IdempotencyKey}
}

func (tx *indeterminateCommitTx) CommitNoop(ctx context.Context, header driver.ChangesetHeader) (model.Commit, error) {
	noop, ok := tx.WriteTx.(driver.NoopCommitter)
	if !ok {
		return model.Commit{}, driver.ErrUnsupported
	}
	_, err := noop.CommitNoop(ctx, header)
	if err != nil {
		return model.Commit{}, err
	}
	tx.store.calls++
	tx.cancel()
	return model.Commit{}, driver.ErrIndeterminate{IdempotencyKey: header.IdempotencyKey}
}

func TestIndeterminateReceiptOwnership(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, mode := range []string{"own", "concurrent", "concurrent-legacy", "prior", "legacy", "prior-post", "own-noop"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				h := setup(t)
				var store driver.Store = h.store
				lease := h.lease
				if backend == "sqlite" {
					var err error
					store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "receipt.db")})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					lease, err = store.Lease(context.Background(), "receipt-test", driver.LeaseTTL)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = lease.Release(context.Background()) })
				}
				winner := h.writer()
				if mode == "prior-post" {
					winner = h.writer(WithPostSubscribers(PostBinding{ID: "post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error { return nil })}))
				}
				winner.store, winner.lease = store, lease
				cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(901)
				var expected model.Commit
				if mode == "own-noop" {
					seed, err := winner.Apply(context.Background(), by, cs, meta(900))
					if err != nil {
						t.Fatal(err)
					}
					cs = model.ChangeSet{Ops: []model.Op{patch(seed.Results[0].Ref, map[string]any{"title": "initial"})}}
				}
				if mode == "prior" || mode == "legacy" || mode == "prior-post" {
					if mode == "legacy" {
						winner.store = &legacyReceiptStore{Store: store}
					}
					var err error
					expected, err = winner.Apply(context.Background(), by, cs, m)
					if err != nil {
						t.Fatal(err)
					}
				}
				fault := &indeterminateCommitStore{Store: store}
				if mode == "concurrent" || mode == "concurrent-legacy" {
					if mode == "concurrent-legacy" {
						winner.store = &legacyReceiptStore{Store: store}
					}
					fault.abortThen = func() {
						var err error
						expected, err = winner.Apply(context.Background(), by, cs, m)
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				w := h.writer()
				w.store, w.lease = fault, lease
				outcome, err := w.ApplyWithOutcome(context.Background(), by, cs, m)
				if err != nil || outcome.Replayed != (mode != "own" && mode != "own-noop") {
					t.Fatalf("outcome=%+v err=%v", outcome, err)
				}
				wantCalls := 1
				if mode == "prior" || mode == "legacy" || mode == "prior-post" {
					wantCalls = 0
				}
				if fault.calls != wantCalls || (wantCalls > 0 && !fault.resolutionBounded) {
					t.Fatalf("calls=%d bounded=%v", fault.calls, fault.resolutionBounded)
				}
				reader, err := w.reader(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				principal, _ := driver.PrincipalKey(by)
				entry, err := reader.IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey)
				if err != nil || entry == nil {
					t.Fatalf("receipt=%+v err=%v", entry, err)
				}
				if entry.ResponsePending || !reflect.DeepEqual(outcome.Commit, entry.Commit) || len(outcome.Commit.Results) != 1 || len(outcome.Commit.Results[0].Record) == 0 {
					t.Fatalf("outcome=%+v receipt=%+v", outcome, entry)
				}
				if mode != "own" && mode != "own-noop" && !reflect.DeepEqual(outcome.Commit, expected) {
					t.Fatalf("outcome=%+v winner=%+v", outcome.Commit, expected)
				}
				if (entry.AttemptID == "") != (mode == "legacy" || mode == "concurrent-legacy") {
					t.Fatalf("attemptID=%q mode=%s", entry.AttemptID, mode)
				}
				log, err := reader.Changesets(context.Background(), nil, 100)
				if err != nil || len(log) != 1 || outcome.Commit.Revision.String() != "1" {
					t.Fatalf("log=%+v err=%v", log, err)
				}
			})
		}
	}
}

// Emulate a receipt persisted by a binary predating invocation metadata.
type legacyReceiptStore struct{ driver.Store }

func (s *legacyReceiptStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &legacyReceiptTx{tx}, nil
}

type legacyReceiptTx struct{ driver.WriteTx }

func (tx *legacyReceiptTx) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	h.AttemptID = ""
	return tx.WriteTx.Commit(ctx, h)
}

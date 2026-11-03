package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

func TestPostFailureIsCommittedAndDurablyReplayed(t *testing.T) {
	h := setup(t)
	var pre, posts atomic.Int32
	w := h.writer(WithValidators(ValidatorBinding{ID: "pre", Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
		pre.Add(1)
		return port.Verdict{Allow: true}, nil
	})}), WithPostSubscribers(PostBinding{ID: "external", Subscriber: postFunc(func(ctx context.Context, change port.CommittedChange, r port.SnapshotReader) error {
		posts.Add(1)
		if r.Snapshot().At.String() != change.Revision.String() {
			return errors.New("post reader not pinned")
		}
		records, err := r.Get(ctx, []model.Ref{change.Changes[0].Resource.Ref})
		if err != nil || len(records) != 1 {
			return errors.New("post did not see committed data")
		}
		return errors.New("external sink unavailable")
	})}))
	cs := model.ChangeSet{Ops: []model.Op{create("new")}}
	result := apply(t, w, cs, meta(1))
	if result.Revision.String() != "1" || len(result.Warnings) != 1 || result.Warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("result=%+v", result)
	}
	// A new Writer proves replay uses durable storage, not a callback cache.
	again := apply(t, h.writer(), cs, meta(1))
	if !reflect.DeepEqual(result, again) || pre.Load() != 1 || posts.Load() != 1 {
		t.Fatalf("replay=%+v counters=%d/%d", again, pre.Load(), posts.Load())
	}
	if h.read(t).Snapshot().At.String() != "1" {
		t.Fatal("post failure/finalization advanced graph head")
	}
	key := PostDeadLetterKey("pipeline-test", result.Revision, "external")
	stored, err := h.store.LoadCheckpoint(context.Background(), key)
	var dead PostDeadLetter
	if err != nil || stored.Generation != 1 || json.Unmarshal(stored.Payload, &dead) != nil || dead.Binding != "external" || dead.Revision.String() != "1" || dead.Incarnation != "pipeline-test" || dead.Reason != "post_subscriber_failed" || !dead.FailedAt.Equal(h.now) {
		t.Fatalf("failed callback has no durable dead letter: %+v entry=%+v err=%v", stored, dead, err)
	}
}

type postMetadataFault struct {
	*memory.Store
	persist bool
	calls   atomic.Int32
}

type panicTimedPost struct{ calls *atomic.Int32 }

func (p *panicTimedPost) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	return errors.New("legacy timed post path used")
}

func (p *panicTimedPost) AfterWithTimeout(context.Context, time.Duration, port.CommittedChange, port.SnapshotReader) error {
	p.calls.Add(1)
	panic("controlled timed post panic")
}

func TestTimedPostPanicBecomesWarningAndDurableDeadLetter(t *testing.T) {
	h := setup(t)
	var calls atomic.Int32
	w := h.writer(WithPostSubscribers(PostBinding{ID: "timed-panic", Subscriber: &panicTimedPost{calls: &calls}}))
	cs, m := model.ChangeSet{Ops: []model.Op{create("panic")}}, meta(992)
	first, err := w.Apply(context.Background(), by, cs, m)
	if err != nil || first.Revision.String() != "1" || len(first.Warnings) != 1 || first.Warnings[0].Code != "post_subscriber_failed" {
		t.Fatalf("timed panic was not committed as a warning: commit=%+v err=%v", first, err)
	}
	deadKey := PostDeadLetterKey("pipeline-test", first.Revision, "timed-panic")
	stored, err := h.store.LoadCheckpoint(context.Background(), deadKey)
	var dead PostDeadLetter
	if err != nil || stored.Generation != 1 || json.Unmarshal(stored.Payload, &dead) != nil || dead.Binding != "timed-panic" || dead.Revision.String() != "1" || dead.Reason != "post_subscriber_failed" {
		t.Fatalf("timed panic did not persist a sanitized dead letter: checkpoint=%+v dead=%+v err=%v", stored, dead, err)
	}
	replay, err := h.writer().Apply(context.Background(), by, cs, m)
	if err != nil || !reflect.DeepEqual(first, replay) || calls.Load() != 1 {
		t.Fatalf("timed panic replay reran post or changed response: first=%+v replay=%+v calls=%d err=%v", first, replay, calls.Load(), err)
	}
}

func (s *postMetadataFault) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, generation uint64, payload []byte) (driver.Checkpoint, error) {
	s.calls.Add(1)
	if s.persist {
		if _, err := s.Store.SaveCheckpoint(ctx, lease, key, generation, payload); err != nil {
			return driver.Checkpoint{}, err
		}
	}
	return driver.Checkpoint{}, errors.New("injected post metadata acknowledgement failure")
}

func TestPostDeadLetterPersistenceFailureKeepsReceiptPendingAndLostAckReloads(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(fmt.Sprintf("persist_%v", persist), func(t *testing.T) {
			h := setup(t)
			store := &postMetadataFault{Store: h.store, persist: persist}
			var posts atomic.Int32
			w := New(store, h.lease, WithOntology(testOntology{}), WithPostSubscribers(PostBinding{ID: "failed", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
				posts.Add(1)
				return errors.New("private sink detail must not enter metadata")
			})}))
			cs, m := model.ChangeSet{Ops: []model.Op{create("new")}}, meta(991)
			commit, err := w.Apply(context.Background(), by, cs, m)
			entry := receipt(t, h, m)
			if entry == nil || posts.Load() != 1 || h.read(t).Snapshot().At.String() != "1" {
				t.Fatalf("metadata failure repeated or lost the graph write: entry=%+v posts=%d", entry, posts.Load())
			}
			wantCalls := int32(4)
			if persist {
				wantCalls = 1
				if err != nil || entry.ResponsePending || len(commit.Warnings) != 1 {
					t.Fatalf("lost metadata ack did not reload durable outcome: commit=%+v entry=%+v err=%v", commit, entry, err)
				}
			} else {
				code(t, err, "busy")
				if !entry.ResponsePending || len(entry.Commit.Warnings) != 0 {
					t.Fatalf("missing dead letter returned unpersisted success: %+v", entry)
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, _ = w.Apply(ctx, by, cs, m)
			}
			if store.calls.Load() != wantCalls || posts.Load() != 1 {
				t.Fatalf("metadata retries reran callback: saves=%d posts=%d", store.calls.Load(), posts.Load())
			}
		})
	}
}

func TestMutatorDataflowOwnershipAndFailOpen(t *testing.T) {
	h := setup(t)
	producer := MutatorBinding{ID: "z-producer", Owns: []string{"fields.source"}, Mutator: mutatorFunc(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
		return []port.Patch{{Resource: p.Changes[0].Resource, Paths: map[string]json.RawMessage{"fields.source": json.RawMessage(`"derived"`)}}}, nil
	})}
	consumer := MutatorBinding{ID: "a-consumer", Reads: []string{"fields.source"}, Owns: []string{"fields.dest"}, Mutator: mutatorFunc(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
		doc, err := DecodeDocument(p.Changes[0].After)
		if err != nil {
			return nil, err
		}
		if doc.Fields["source"] != "derived" {
			return nil, errors.New("dataflow order wrong")
		}
		// Attempting to mutate the isolated proposal cannot change stored input.
		p.Changes[0].Op.Create.Fields["title"] = "callback wrote its argument"
		return []port.Patch{{Resource: p.Changes[0].Resource, Paths: map[string]json.RawMessage{"fields.dest": json.RawMessage(`"ok"`)}}}, nil
	})}
	skip := ValidatorBinding{ID: "offline", FailOpen: true, Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
		return port.Verdict{}, errors.New("offline")
	})}
	w := h.writer(WithMutators(consumer, producer), WithValidators(skip))
	result := apply(t, w, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
	doc, err := DecodeDocument(result.Results[0].Record)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Fields["dest"] != "ok" || doc.Fields["title"] != "initial" || result.Results[0].SetBy["fields.source"] != "z-producer" || len(result.Warnings) != 1 || result.Warnings[0].Code != "subscriber_skipped" {
		t.Fatalf("body=%+v result=%+v", doc, result)
	}
	bad := producer
	bad.ID = "unowned"
	bad.Owns = nil
	_, err = h.writer(WithMutators(bad)).Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("other")}}, meta(2))
	code(t, err, "plugin_patch_out_of_scope")
	conflict := producer
	conflict.ID = "second-owner"
	_, err = h.writer(WithMutators(producer, conflict)).Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("other")}}, meta(3))
	code(t, err, "invalid_params")
}

type queuedTimedMutator struct{}

func (queuedTimedMutator) Mutate(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
	return nil, errors.New("legacy callback path used")
}

func (queuedTimedMutator) MutateWithTimeout(ctx context.Context, timeout time.Duration, _ port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
	// Model a bounded pool queue wait followed by a callback that uses its full
	// execution budget. The queue delay must not consume the callback budget.
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	execution, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-time.After(200 * time.Millisecond):
		return nil, nil
	case <-execution.Done():
		return nil, execution.Err()
	}
}

func TestTimedMutatorStartsBudgetAfterPoolQueue(t *testing.T) {
	h := setup(t)
	w := h.writer(WithMutators(MutatorBinding{ID: "queued", Mutator: queuedTimedMutator{}}))
	started := time.Now()
	if _, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("queued")}}, meta(1)); err != nil {
		t.Fatalf("timed callback failed after bounded queue wait: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 290*time.Millisecond {
		t.Fatalf("timed callback did not exercise queue plus execution windows: elapsed=%s", elapsed)
	}
}

type timedPoolBusyError struct{}

func (timedPoolBusyError) Error() string      { return "runtime pool queue is full" }
func (timedPoolBusyError) CallbackBusy() bool { return true }

type busyTimedMutator struct{}

func (busyTimedMutator) Mutate(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
	return nil, errors.New("legacy callback path used")
}

func (busyTimedMutator) MutateWithTimeout(context.Context, time.Duration, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
	return nil, timedPoolBusyError{}
}

func TestTimedMutatorPoolBusyMapsToRetryableBusy(t *testing.T) {
	h := setup(t)
	w := h.writer(WithMutators(MutatorBinding{ID: "busy", Mutator: busyTimedMutator{}}))
	_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("busy")}}, meta(1))
	code(t, err, "busy")
	var typed *model.Error
	if !errors.As(err, &typed) || !typed.Data.Retryable {
		t.Fatalf("pool busy error was not retryable: %v", err)
	}
}

type authorizerFunc func(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error

func (f authorizerFunc) Authorize(ctx context.Context, s port.AuthorizationStage, p port.ProposedChange, r port.SnapshotReader) error {
	return f(ctx, s, p, r)
}

func TestPostPrincipalInputsAreIsolated(t *testing.T) {
	h := setup(t)
	mutated := make(chan struct{})
	w := h.writer(WithPostSubscribers(
		PostBinding{ID: "mutates-own-input", Subscriber: postFunc(func(_ context.Context, change port.CommittedChange, _ port.SnapshotReader) error {
			change.Principal.Caveats["label"] = "changed"
			change.Principal.Sponsor.Caveats["label"] = "changed"
			close(mutated)
			return nil
		})},
		PostBinding{ID: "observes-own-input", Subscriber: postFunc(func(ctx context.Context, change port.CommittedChange, _ port.SnapshotReader) error {
			select {
			case <-mutated:
			case <-ctx.Done():
				return ctx.Err()
			}
			if change.Principal.Caveats["label"] != "original" || change.Principal.Sponsor.Caveats["label"] != "original" {
				return errors.New("another post subscriber mutated the principal")
			}
			return nil
		})},
	))
	principal := model.PrincipalRef{Kind: "agent", ID: "test-agent", Caveats: map[string]any{"label": "original"}, Sponsor: &model.PrincipalRef{Kind: "user", ID: "test-user", Caveats: map[string]any{"label": "original"}}}
	result, err := w.Apply(context.Background(), principal, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	if err != nil || len(result.Warnings) != 0 || principal.Caveats["label"] != "original" || principal.Sponsor.Caveats["label"] != "original" {
		t.Fatalf("post isolation result=%+v principal=%+v err=%v", result, principal, err)
	}
}

func TestReplayReauthorizesAndDifferentParamsRefuse(t *testing.T) {
	h := setup(t)
	cs := model.ChangeSet{Ops: []model.Op{create("new")}}
	w := h.writer()
	apply(t, w, cs, meta(1))
	var stages []port.AuthorizationStage
	denied := h.writer(WithAuthorizer(authorizerFunc(func(_ context.Context, s port.AuthorizationStage, _ port.ProposedChange, _ port.SnapshotReader) error {
		stages = append(stages, s)
		return failure("forbidden", "current authority revoked")
	})))
	_, err := denied.Apply(context.Background(), by, cs, meta(1))
	code(t, err, "forbidden")
	if !reflect.DeepEqual(stages, []port.AuthorizationStage{port.AuthorizeReplay}) {
		t.Fatalf("stages=%v", stages)
	}
	cs.Ops[0].Create.Fields["title"] = "different"
	_, err = w.Apply(context.Background(), by, cs, meta(1))
	code(t, err, "idempotency_key_reused")
	if h.read(t).Snapshot().At.String() != "1" {
		t.Fatal("replay refusal wrote graph")
	}
}

func TestConcurrentSameKeyRunsPostOnce(t *testing.T) {
	h := setup(t)
	var posts atomic.Int32
	w := h.writer(WithPostSubscribers(PostBinding{ID: "post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error { posts.Add(1); return nil })}))
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]model.Commit, 8)
	errs := make([]error, 8)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("call%d: %v", i, err)
		}
		if results[i].Revision.String() != "1" || results[i].Results[0].Ref.ID != results[0].Results[0].Ref.ID {
			t.Fatalf("different durable receipts: %+v", results)
		}
	}
	if posts.Load() != 1 || h.read(t).Snapshot().At.String() != "1" {
		t.Fatalf("posts=%d head=%s", posts.Load(), h.read(t).Snapshot().At)
	}
}

func TestConcurrentRevisionCASHasOneWinner(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("new")}}, meta(1))
	ref := seed.Results[0].Ref
	cs := model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "winner"})}, Expect: []model.Precondition{{Ref: ref, Revision: &seed.Revision}}}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) { <-start; _, err := w.Apply(context.Background(), by, cs, meta(i+2)); errs <- err }(i)
	}
	close(start)
	a, b := <-errs, <-errs
	if a == nil && b == nil || a != nil && b != nil {
		t.Fatalf("want one winner: %v %v", a, b)
	}
	if a != nil {
		code(t, a, "precondition_failed")
	}
	if b != nil {
		code(t, b, "precondition_failed")
	}
	r := h.read(t)
	versions, err := r.Versions(context.Background(), ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 2 || r.Snapshot().At.String() != "2" {
		t.Fatalf("versions=%+v head=%s err=%v", versions, r.Snapshot().At, err)
	}
}

func TestStaleCASReleasesRevisionOnlyAfterExpectationAuthorization(t *testing.T) {
	h := setup(t)
	seed := apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{create("write-target"), create("expectation-only")}}, meta(40))
	target, expectationOnly := seed.Results[0].Ref, seed.Results[1].Ref
	stale := model.Natural64(0)
	cs := model.ChangeSet{
		Ops:    []model.Op{patch(target, map[string]any{"title": "must-not-land"})},
		Expect: []model.Precondition{{Ref: expectationOnly, Revision: &stale}},
	}

	var deniedCalls, deniedMutations atomic.Int32
	denied := h.writer(
		WithMutators(MutatorBinding{ID: "must-not-run", Mutator: mutatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
			deniedMutations.Add(1)
			return nil, nil
		})}),
		WithAuthorizer(authorizerFunc(func(_ context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, _ port.SnapshotReader) error {
			if stage != port.AuthorizeBefore {
				return failure("forbidden", "unexpected authorization stage")
			}
			deniedCalls.Add(1)
			if len(proposal.Expect) != 1 || proposal.Expect[0].Ref.ID != expectationOnly.ID {
				return failure("forbidden", "expectation-only target was not authorized")
			}
			// This models a hidden pre-image: the operation target may be
			// visible, but the expectation-only record is outside the caller's
			// permitted write scope.
			return failure("forbidden", "expectation target is hidden")
		})),
	)
	_, err := denied.Apply(context.Background(), by, cs, meta(41))
	code(t, err, "forbidden")
	if deniedCalls.Load() != 1 || deniedMutations.Load() != 0 {
		t.Fatalf("denied stale CAS crossed the callback boundary: authz=%d mutators=%d", deniedCalls.Load(), deniedMutations.Load())
	}
	var deniedModel *model.Error
	if !errors.As(err, &deniedModel) || deniedModel.Data.Details != nil || strings.Contains(err.Error(), "currentRevision") {
		t.Fatalf("hidden refusal disclosed stale revision: %v", err)
	}
	if head := h.read(t).Snapshot().At.String(); head != "1" {
		t.Fatalf("denied stale CAS changed head to %s", head)
	}

	var allowedCalls, allowedMutations atomic.Int32
	allowed := h.writer(
		WithMutators(MutatorBinding{ID: "must-not-run-after-allow", Mutator: mutatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error) {
			allowedMutations.Add(1)
			return nil, nil
		})}),
		WithAuthorizer(authorizerFunc(func(_ context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, _ port.SnapshotReader) error {
			if stage != port.AuthorizeBefore {
				return failure("forbidden", "unexpected authorization stage")
			}
			allowedCalls.Add(1)
			if len(proposal.Expect) != 1 || proposal.Expect[0].Ref.ID != expectationOnly.ID {
				return failure("forbidden", "expectation-only target was not carried")
			}
			return nil
		})),
	)
	_, err = allowed.Apply(context.Background(), by, cs, meta(42))
	code(t, err, "precondition_failed")
	var staleError *model.Error
	if !errors.As(err, &staleError) || staleError.Data.Details["reason"] != "revision" || staleError.Data.Details["currentRevision"] == nil {
		t.Fatalf("allowed stale CAS did not release its typed revision refusal: %v", err)
	}
	if allowedCalls.Load() != 1 || allowedMutations.Load() != 0 {
		t.Fatalf("stale refusal crossed callbacks after allow: authz=%d mutators=%d", allowedCalls.Load(), allowedMutations.Load())
	}
	if head := h.read(t).Snapshot().At.String(); head != "1" {
		t.Fatalf("allowed stale CAS changed head to %s", head)
	}
}

func TestMetadataCASPreparedWritersHaveOneWinner(t *testing.T) {
	h := setup(t)
	ref := apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
	apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{metadataOp(ref, "compare", `1.0`)}}, meta(2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan int, 2)
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var posts atomic.Int32
	type outcome struct {
		index int
		value port.WriteOutcome
		err   error
	}
	finished := make(chan outcome, 2)
	for i := range 2 {
		w := h.writer(WithValidators(ValidatorBinding{ID: "prepared", Validator: validatorFunc(func(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
			if p.BaseRevision.String() != "2" || r.Snapshot().At.String() != "2" {
				return port.Verdict{}, errors.New("writers did not prepare on the same pinned pre-image")
			}
			ready <- i
			select {
			case <-release[i]:
				return port.Verdict{Allow: true}, nil
			case <-ctx.Done():
				return port.Verdict{}, ctx.Err()
			}
		})}), WithPostSubscribers(PostBinding{ID: "winner", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error { posts.Add(1); return nil })}))
		cs := model.ChangeSet{Ops: []model.Op{metadataOp(ref, "compare", fmt.Sprint(i+2)), patch(ref, map[string]any{"title": fmt.Sprint(i)})}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.compare", metadataDigest(`1.0`))}}
		go func() { out, err := w.ApplyWithOutcome(ctx, by, cs, meta(10+i)); finished <- outcome{i, out, err} }()
	}
	for range 2 {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("real writers did not both pass metadata preparation")
		}
	}
	close(release[0])
	winner := <-finished
	if winner.index != 0 || winner.err != nil || winner.value.Commit.Revision.String() != "3" {
		t.Fatalf("first prepared writer=%+v", winner)
	}
	winnerState := metadataGraph(t, h)
	close(release[1])
	loser := <-finished
	code(t, loser.err, "precondition_failed")
	if loser.index != 1 || posts.Load() != 1 || metadataReceipt(t, h, meta(11)) != nil || !reflect.DeepEqual(winnerState, metadataGraph(t, h)) {
		t.Fatalf("stale metadata writer published after the winner: %+v posts=%d", loser, posts.Load())
	}
	if len(winnerState.Log) != 3 || len(winnerState.Versions[ref.ID]) != 3 {
		t.Fatal("metadata CAS winner did not publish exactly one version/changeset")
	}
}

func TestMetadataExpectationReadOnlyDependencyRestartsOnFreshSnapshot(t *testing.T) {
	for _, selectedValueChanges := range []bool{true, false} {
		t.Run(fmt.Sprint("selected-value-changes=", selectedValueChanges), func(t *testing.T) {
			h := setup(t)
			seed := apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{create("guard"), create("target")}}, meta(1))
			guard, target := seed.Results[0].Ref, seed.Results[1].Ref
			apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{metadataOp(guard, "compare", `1.0`)}}, meta(2))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			prepared, resume := make(chan struct{}), make(chan struct{})
			var calls, posts atomic.Int32
			w := h.writer(WithValidators(ValidatorBinding{ID: "pause-prepared", Validator: validatorFunc(func(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
				if calls.Add(1) == 1 {
					if p.BaseRevision.String() != "2" || r.Snapshot().At.String() != "2" {
						return port.Verdict{}, errors.New("unexpected preparation snapshot")
					}
					close(prepared)
					select {
					case <-resume:
					case <-ctx.Done():
						return port.Verdict{}, ctx.Err()
					}
				} else if r.Snapshot().At.String() != "3" {
					return port.Verdict{}, errors.New("retry reused the stale snapshot")
				}
				return port.Verdict{Allow: true}, nil
			})}), WithPostSubscribers(PostBinding{ID: "post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error { posts.Add(1); return nil })}))
			result := make(chan error, 1)
			cs := model.ChangeSet{Ops: []model.Op{patch(target, map[string]any{"title": "dependent"})}, Expect: []model.Precondition{model.PreconditionByHash(guard, "metadata.compare", metadataDigest(`1.0`))}}
			go func() { _, err := w.Apply(ctx, by, cs, meta(10)); result <- err }()
			select {
			case <-prepared:
			case <-time.After(time.Second):
				t.Fatal("metadata-dependent writer did not prepare")
			}
			change := patch(guard, map[string]any{"title": "disjoint edit"})
			if selectedValueChanges {
				change = metadataOp(guard, "compare", `2`)
			}
			apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{change}}, meta(11))
			beforeResume := metadataGraph(t, h)
			close(resume)
			err := <-result
			if selectedValueChanges {
				code(t, err, "precondition_failed")
				if !reflect.DeepEqual(beforeResume, metadataGraph(t, h)) || metadataReceipt(t, h, meta(10)) != nil || posts.Load() != 0 {
					t.Fatal("read-only metadata dependency was absent from the shared commit read set")
				}
			} else if err != nil || calls.Load() != 2 || posts.Load() != 1 || metadataReceipt(t, h, meta(10)) == nil || h.read(t).Snapshot().At.String() != "4" {
				t.Fatalf("equal selected value failed to restart and commit: calls=%d posts=%d err=%v", calls.Load(), posts.Load(), err)
			}
		})
	}
}

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

// Every operation except the selected failure delegates to actual storage.
// An inner SQL/preparation deadline must not become an opaque wire error.
type admissionDeadlineStore struct {
	*memory.Store
	stage          string
	failure        error
	lateCallback   bool
	opened, closed int
}

type sharedPreparationDeadlineStore struct {
	*memory.Store
	observed    time.Time
	failure     error
	returnEarly bool
}

func (s *sharedPreparationDeadlineStore) Meta(ctx context.Context) (driver.Meta, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return driver.Meta{}, errors.New("preparation omitted its deadline")
	}
	s.observed = deadline
	if !s.returnEarly {
		<-ctx.Done()
	}
	if s.failure != nil {
		return driver.Meta{}, s.failure
	}
	return driver.Meta{}, ctx.Err()
}

func TestPreparationConnectTimeoutRequiresExpiredBudgetAndPreservesRetry(t *testing.T) {
	connectTimeout := fmt.Errorf("connection setup: %w", &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded})
	unrelated := errors.New("unrelated storage failure")
	for _, test := range []struct {
		name        string
		failure     error
		returnEarly bool
		wantBusy    bool
	}{
		{name: "expired connection timeout", failure: connectTimeout, wantBusy: true},
		{name: "early connection timeout", failure: connectTimeout, returnEarly: true},
		{name: "unrelated error after expiry", failure: unrelated},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := setup(t)
			w := h.writer()
			w.store = &sharedPreparationDeadlineStore{Store: h.store, failure: test.failure, returnEarly: test.returnEarly}
			caller, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			ctx := WithPreparationDeadline(caller, time.Now().Add(100*time.Millisecond))
			cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(711)
			_, err := w.Apply(ctx, by, cs, m)
			if test.wantBusy {
				code(t, err, "busy")
			} else if !errors.Is(err, test.failure) {
				t.Fatalf("preparation changed an unrelated or early error: got=%v want=%v", err, test.failure)
			}
			if caller.Err() != nil {
				t.Fatalf("preparation consumed response budget: %v", caller.Err())
			}
			r := h.read(t)
			changes, readErr := r.Changesets(caller, nil, 100)
			principal, _ := driver.PrincipalKey(by)
			entry, receiptErr := r.IdempotencyByKey(caller, principal, "", m.Method, m.IdempotencyKey)
			if readErr != nil || receiptErr != nil || !r.Snapshot().At.IsZero() || len(changes) != 0 || entry != nil {
				t.Fatalf("connection failure published state: head=%s changes=%v receipt=%v errors=%v/%v", r.Snapshot().At, changes, entry, readErr, receiptErr)
			}
			_ = r.Close()
			w.store = h.store
			first := apply(t, w, cs, m)
			replayed := apply(t, w, cs, m)
			assertSameResponses(t, applyResult{commit: first}, applyResult{commit: replayed})
			assertSingleGraphWrite(t, h, first)
		})
	}
}

func TestSharedPreparationDeadlinePreservesCallerAndRetry(t *testing.T) {
	h := setup(t)
	w := h.writer()
	store := &sharedPreparationDeadlineStore{Store: h.store}
	w.store = store
	caller, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	deadline := time.Now().Add(time.Second)
	ctx := WithPreparationDeadline(caller, deadline)
	ctx = WithPreparationDeadline(ctx, deadline.Add(time.Hour))
	callerDeadline, _ := caller.Deadline()
	if got, _ := ctx.Deadline(); !got.Equal(callerDeadline) {
		t.Fatal("preparation annotation changed the caller response deadline")
	}
	cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(710)
	_, err := w.Apply(ctx, by, cs, m)
	code(t, err, "busy")
	if !store.observed.Equal(deadline) || caller.Err() != nil {
		t.Fatalf("preparation restarted or consumed caller budget: observed=%s shared=%s caller=%v", store.observed, deadline, caller.Err())
	}
	r := h.read(t)
	changes, readErr := r.Changesets(caller, nil, 100)
	principal, _ := driver.PrincipalKey(by)
	entry, receiptErr := r.IdempotencyByKey(caller, principal, "", m.Method, m.IdempotencyKey)
	if readErr != nil || receiptErr != nil || !r.Snapshot().At.IsZero() || len(changes) != 0 || entry != nil {
		t.Fatalf("shared-budget expiry published state: head=%s changes=%v receipt=%v errors=%v/%v", r.Snapshot().At, changes, entry, readErr, receiptErr)
	}
	_ = r.Close()
	w.store = h.store
	first := apply(t, w, cs, m)
	replayed := apply(t, w, cs, m)
	assertSameResponses(t, applyResult{commit: first}, applyResult{commit: replayed})
	assertSingleGraphWrite(t, h, first)
}

func (s *admissionDeadlineStore) Meta(ctx context.Context) (driver.Meta, error) {
	if s.stage == "metadata" {
		return driver.Meta{}, fmt.Errorf("metadata admission: %w", s.failure)
	}
	return s.Store.Meta(ctx)
}

func (s *admissionDeadlineStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	if s.stage == "snapshot" {
		return nil, fmt.Errorf("snapshot admission: %w", s.failure)
	}
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	s.opened++
	tracked := &admissionDeadlineReader{Reader: r, store: s}
	if s.lateCallback {
		deadline, ok := ctx.Deadline()
		if !ok {
			_ = tracked.Close()
			return nil, errors.New("preparation reader omitted its deadline")
		}
		timer := time.NewTimer(time.Until(deadline) - 100*time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			_ = tracked.Close()
			return nil, ctx.Err()
		}
	}
	return tracked, nil
}

func (s *admissionDeadlineStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if s.stage == "write" {
		return nil, fmt.Errorf("write admission: %w", s.failure)
	}
	return s.Store.Write(ctx, lease)
}

type admissionDeadlineReader struct {
	driver.Reader
	store  *admissionDeadlineStore
	closed bool
}

func (r *admissionDeadlineReader) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	if r.store.stage == "replay" {
		return nil, fmt.Errorf("receipt admission: %w", r.store.failure)
	}
	return r.Reader.IdempotencyByKey(ctx, principal, agent, method, key)
}

func (r *admissionDeadlineReader) Close() error {
	if !r.closed {
		r.closed = true
		r.store.closed++
	}
	return r.Reader.Close()
}

func TestInnerAdmissionDeadlinesAreRetryableWithoutPublication(t *testing.T) {
	for _, stage := range []string{"metadata", "snapshot", "replay", "write"} {
		t.Run(stage, func(t *testing.T) {
			h := setup(t)
			store := &admissionDeadlineStore{Store: h.store, stage: stage, failure: context.DeadlineExceeded}
			w := h.writer()
			w.store = store
			cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(704)
			_, err := w.Apply(context.Background(), by, cs, m)
			code(t, err, "busy")
			r := h.read(t)
			changes, readErr := r.Changesets(context.Background(), nil, 100)
			principal, _ := driver.PrincipalKey(by)
			entry, receiptErr := r.IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey)
			if readErr != nil || receiptErr != nil || r.Snapshot().At.String() != "0" || len(changes) != 0 || entry != nil || store.opened != store.closed {
				t.Fatalf("deadline published or retained a reader: head=%s changes=%v receipt=%v readers=%d/%d errors=%v/%v", r.Snapshot().At, changes, entry, store.opened, store.closed, readErr, receiptErr)
			}
			store.stage = ""
			first := apply(t, w, cs, m)
			replayed := apply(t, w, cs, m)
			assertSameResponses(t, applyResult{commit: first}, applyResult{commit: replayed})
			assertSingleGraphWrite(t, h, first)
		})
	}
}

func TestAdmissionFailurePreservesCancellationAndOtherErrors(t *testing.T) {
	for _, failure := range []error{context.Canceled, errors.New("private storage failure")} {
		h := setup(t)
		w := h.writer()
		w.store = &admissionDeadlineStore{Store: h.store, stage: "metadata", failure: failure}
		_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("")}}, meta(705))
		if !errors.Is(err, failure) {
			t.Fatalf("admission changed non-deadline failure: got=%v want=%v", err, failure)
		}
	}
}

func TestCallbackParentDeadlineIsRetryableWithoutPublication(t *testing.T) {
	for _, kind := range []string{"mutator", "validator"} {
		t.Run(kind, func(t *testing.T) {
			h := setup(t)
			store := &admissionDeadlineStore{Store: h.store, lateCallback: true}
			var calls atomic.Int32
			var option Option
			if kind == "mutator" {
				option = WithMutators(MutatorBinding{ID: "late", Mutator: mutatorFunc(func(ctx context.Context, _ port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
					calls.Add(1)
					<-ctx.Done()
					return nil, ctx.Err()
				})})
			} else {
				option = WithValidators(ValidatorBinding{ID: "late", Validator: validatorFunc(func(ctx context.Context, _ port.ProposedChange, _ port.SnapshotReader) (port.Verdict, error) {
					calls.Add(1)
					<-ctx.Done()
					return port.Verdict{}, ctx.Err()
				})})
			}
			w := h.writer(option)
			w.store = store
			cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(706)
			_, err := w.Apply(context.Background(), by, cs, m)
			code(t, err, "busy")
			r := h.read(t)
			changes, readErr := r.Changesets(context.Background(), nil, 100)
			principal, _ := driver.PrincipalKey(by)
			entry, receiptErr := r.IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey)
			if calls.Load() != 1 || readErr != nil || receiptErr != nil || r.Snapshot().At.String() != "0" || len(changes) != 0 || entry != nil || store.opened != store.closed {
				t.Fatalf("expired callback published or retained a reader: calls=%d head=%s changes=%v receipt=%v readers=%d/%d errors=%v/%v", calls.Load(), r.Snapshot().At, changes, entry, store.opened, store.closed, readErr, receiptErr)
			}
			first := apply(t, h.writer(), cs, m)
			replayed := apply(t, h.writer(), cs, m)
			assertSameResponses(t, applyResult{commit: first}, applyResult{commit: replayed})
			assertSingleGraphWrite(t, h, first)
		})
	}
}

func TestCallbackFailurePreservesIndependentDeadlineAndEstablishedCancellationMapping(t *testing.T) {
	for _, kind := range []string{"mutator", "validator"} {
		code(t, callbackFailure(context.Background(), kind, "test", context.DeadlineExceeded), "subscriber_unavailable")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		code(t, callbackFailure(ctx, kind, "test", context.Canceled), "subscriber_unavailable")
		code(t, callbackFailure(context.Background(), kind, "test", context.Canceled), "subscriber_unavailable")
	}
}

type publicationAdmissionStore struct {
	*memory.Store
	writes, readers  atomic.Int32
	entered, release chan struct{}
	readerClosed     chan struct{}
	admission        chan struct{}
}

func (s *publicationAdmissionStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	s.readers.Add(1)
	return &publicationAdmissionReader{Reader: r, store: s}, nil
}

func (s *publicationAdmissionStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	// Model storage-owned admission, keeping the original cancellation and
	// publication assertions independent of any Writer-local queue.
	select {
	case s.admission <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.admission }()
	if s.writes.Add(1) == 1 {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.Store.Write(ctx, lease)
}

type publicationAdmissionReader struct {
	driver.Reader
	store *publicationAdmissionStore
	once  sync.Once
}

func (r *publicationAdmissionReader) Close() error {
	err := r.Reader.Close()
	r.once.Do(func() {
		r.store.readers.Add(-1)
		r.store.readerClosed <- struct{}{}
	})
	return err
}

func TestPublicationAdmissionCancellationClosesReadersAndCannotPublish(t *testing.T) {
	h := setup(t)
	store := &publicationAdmissionStore{Store: h.store, entered: make(chan struct{}), release: make(chan struct{}), readerClosed: make(chan struct{}, 16), admission: make(chan struct{}, 1)}
	w := h.writer()
	w.store = store
	first := make(chan applyResult, 1)
	go func() {
		commit, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("")}}, meta(707))
		first <- applyResult{commit, err}
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("first publication did not reach storage")
	}
	<-store.readerClosed
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := make(chan applyResult, 1)
	cs, m := model.ChangeSet{Ops: []model.Op{create("")}}, meta(708)
	go func() {
		commit, err := w.Apply(ctx, by, cs, m)
		second <- applyResult{commit, err}
	}()
	select {
	case <-store.readerClosed:
	case <-time.After(time.Second):
		t.Fatal("queued publication retained its preparation reader")
	}
	select {
	case result := <-second:
		t.Fatalf("queued publication bypassed the held admission: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	result := awaitResult(t, second)
	r := h.read(t)
	principal, _ := driver.PrincipalKey(by)
	entry, receiptErr := r.IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey)
	if !errors.Is(result.err, context.Canceled) || store.writes.Load() != 1 || store.readers.Load() != 0 || r.Snapshot().At.String() != "0" || entry != nil || receiptErr != nil {
		t.Fatalf("canceled admission entered storage or published: result=%+v writes=%d readers=%d receipt=%v error=%v", result, store.writes.Load(), store.readers.Load(), entry, receiptErr)
	}
	close(store.release)
	winner := awaitResult(t, first)
	if winner.err != nil || winner.commit.Revision.String() != "1" {
		t.Fatalf("held publication failed: %+v", winner)
	}
	retried := apply(t, w, cs, m)
	replayed := apply(t, w, cs, m)
	assertSameResponses(t, applyResult{commit: retried}, applyResult{commit: replayed})
	if retried.Revision.String() != "2" || h.read(t).Snapshot().At.String() != "2" || store.writes.Load() != 2 || store.readers.Load() != 0 {
		t.Fatalf("admission did not release or retry/replay republished: commit=%+v writes=%d readers=%d", retried, store.writes.Load(), store.readers.Load())
	}
}

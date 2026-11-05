package pipeline

import (
	"context"
	"errors"
	"fmt"
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

func TestCallbackFailurePreservesIndependentDeadlineAndParentCancellation(t *testing.T) {
	for _, kind := range []string{"mutator", "validator"} {
		code(t, callbackFailure(context.Background(), kind, "test", context.DeadlineExceeded), "subscriber_unavailable")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := callbackFailure(ctx, kind, "test", context.Canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("callback changed parent cancellation: %v", err)
		}
	}
}

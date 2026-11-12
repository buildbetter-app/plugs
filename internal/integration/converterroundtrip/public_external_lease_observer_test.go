package converterroundtrip

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Only HoldRestoreLease sees the observed lease. Operational calls retain the
// original store and concrete lease, including the memory driver's type check.
func publicProofHoldObservedLease(t *testing.T, ctx context.Context, store driver.Store, owner, output string) (context.Context, driver.ActiveLease, func() error, error) {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(output, "lease-renewals.ndjson"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal("lease observation output", err)
	}
	observer := &publicProofLeaseObserver{start: time.Now(), owner: owner, events: make(chan publicProofLeaseEvent, 4096), done: make(chan struct{})}
	go observer.write(file)
	observed := &publicProofObservedLeaseStore{Store: store, observer: observer}
	op := ctx
	t.Cleanup(func() {
		var cause error
		if op != nil {
			cause = context.Cause(op)
		}
		observer.record("cleanup", op, observed.raw, cause)
		close(observer.events)
		<-observer.done
		if n := observer.dropped.Load(); n != 0 {
			t.Errorf("lease observer dropped %d events", n)
		}
		if observer.writeErr != nil {
			t.Errorf("lease observer output: %v", observer.writeErr)
		}
	})
	op, _, release, err := HoldRestoreLease(ctx, observed, owner)
	return op, observed.raw, release, err
}

type publicProofLeaseFence struct {
	ExpectedEpoch uint64 `json:"expected_epoch"`
	ObservedEpoch uint64 `json:"observed_epoch"`
}

func publicProofLeaseFenceOf(err error) *publicProofLeaseFence {
	var value driver.ErrFenced
	if errors.As(err, &value) {
		return &publicProofLeaseFence{ExpectedEpoch: value.ExpectedEpoch, ObservedEpoch: value.ObservedEpoch}
	}
	var pointer *driver.ErrFenced
	if errors.As(err, &pointer) && pointer != nil {
		return &publicProofLeaseFence{ExpectedEpoch: pointer.ExpectedEpoch, ObservedEpoch: pointer.ObservedEpoch}
	}
	return nil
}

// Keep the original error/refusal path; expose the fields hidden by Error().
func publicProofLogLeaseFence(t *testing.T, stage string, err error) {
	t.Helper()
	if fence := publicProofLeaseFenceOf(err); fence != nil {
		t.Logf("public_external_lease_fence stage=%s unix_ns=%d expected_epoch=%d observed_epoch=%d", stage, time.Now().UnixNano(), fence.ExpectedEpoch, fence.ObservedEpoch)
	}
}

type publicProofLeaseEvent struct {
	Kind         string                 `json:"kind"`
	UnixNS       int64                  `json:"unix_ns"`
	ElapsedNS    int64                  `json:"elapsed_ns"`
	Owner        string                 `json:"owner"`
	Epoch        uint64                 `json:"epoch"`
	LostClosed   bool                   `json:"lost_closed"`
	Error        string                 `json:"error,omitempty"`
	Fence        *publicProofLeaseFence `json:"fence,omitempty"`
	ContextError string                 `json:"context_error,omitempty"`
	ContextCause string                 `json:"context_cause,omitempty"`
	CauseFence   *publicProofLeaseFence `json:"cause_fence,omitempty"`
}

type publicProofLeaseObserver struct {
	start    time.Time
	owner    string
	events   chan publicProofLeaseEvent
	done     chan struct{}
	dropped  atomic.Uint64
	writeErr error // Read only after done closes.
}

// No disk I/O or waiting for the observer is allowed on the renewal goroutine.
// Bounded telemetry is part of process RSS, separate from decoded accounting.
func (o *publicProofLeaseObserver) record(kind string, ctx context.Context, lease driver.ActiveLease, err error) {
	now := time.Now()
	event := publicProofLeaseEvent{Kind: kind, UnixNS: now.UnixNano(), ElapsedNS: now.Sub(o.start).Nanoseconds(), Owner: o.owner}
	if lease != nil {
		event.Epoch = lease.Epoch()
		select {
		case <-lease.Lost():
			event.LostClosed = true
		default:
		}
	}
	if err != nil {
		event.Error, event.Fence = err.Error(), publicProofLeaseFenceOf(err)
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			event.ContextError = err.Error()
		}
		if cause := context.Cause(ctx); cause != nil {
			event.ContextCause, event.CauseFence = cause.Error(), publicProofLeaseFenceOf(cause)
		}
	}
	select {
	case o.events <- event:
	default:
		o.dropped.Add(1)
	}
}

func (o *publicProofLeaseObserver) write(file *os.File) {
	defer close(o.done)
	encoder := json.NewEncoder(file)
	for event := range o.events {
		if o.writeErr == nil {
			o.writeErr = encoder.Encode(event)
		}
	}
	if o.writeErr == nil {
		o.writeErr = encoder.Encode(map[string]any{"kind": "observer-end", "unix_ns": time.Now().UnixNano(), "dropped_events": o.dropped.Load(), "lease_ttl_ns": driver.LeaseTTL.Nanoseconds(), "renew_interval_ns": (driver.LeaseTTL / 3).Nanoseconds()})
	}
	o.writeErr = errors.Join(o.writeErr, file.Sync(), file.Close())
}

type publicProofObservedLeaseStore struct {
	driver.Store
	observer *publicProofLeaseObserver
	raw      driver.ActiveLease
}

func (s *publicProofObservedLeaseStore) Lease(ctx context.Context, owner string, ttl time.Duration) (driver.ActiveLease, error) {
	s.observer.record("acquire-enter", ctx, nil, nil)
	lease, err := s.Store.Lease(ctx, owner, ttl)
	s.raw = lease
	s.observer.record("acquire-return", ctx, lease, err)
	if err != nil {
		return nil, err
	}
	return &publicProofObservedLease{ActiveLease: lease, observer: s.observer}, nil
}

type publicProofObservedLease struct {
	driver.ActiveLease
	observer *publicProofLeaseObserver
}

func (l *publicProofObservedLease) Renew(ctx context.Context) error {
	l.observer.record("renew-enter", ctx, l.ActiveLease, nil)
	err := l.ActiveLease.Renew(ctx)
	l.observer.record("renew-return", ctx, l.ActiveLease, err)
	return err
}

func (l *publicProofObservedLease) Release(ctx context.Context) error {
	l.observer.record("release-enter", ctx, l.ActiveLease, nil)
	err := l.ActiveLease.Release(ctx)
	l.observer.record("release-return", ctx, l.ActiveLease, err)
	return err
}

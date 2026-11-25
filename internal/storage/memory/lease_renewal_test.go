package memory

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

func TestMemoryRestoreChecksCancellationAtPublicationFence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var armed atomic.Bool
	now := time.Now()
	s, err := NewWithOptions(Options{Clock: func() time.Time {
		if armed.CompareAndSwap(true, false) {
			cancel()
		}
		return now
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lease, err := s.Lease(ctx, "restore-publication-cancel", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	published := false
	s.mu.Lock()
	armed.Store(true)
	err = s.withLeasePublicationContext(ctx, lease, func() { published = true })
	s.mu.Unlock()
	if err != context.Canceled || published {
		t.Fatalf("interruption during final lease fence: publication=%t err=%v", published, err)
	}
}

func TestMemoryLeaseRenewalDuringSlowCommit(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 11, 1, 4, 29, 59, 999999999, time.UTC))
	s, err := NewWithOptions(Options{Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lease, err := s.Lease(ctx, "slow-commit", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	s.beforeCommitHook = func() {
		close(entered)
		<-release
	}
	commitDone := make(chan error, 1)
	go func() {
		_, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{Method: "memory.slow", IdempotencyKey: "slow", RequestHash: "slow"}, []conformance.FixtureChange{{
			Ref: model.RefByID("pl-slow-commit", model.Natural64(0)), Kind: "test", Op: model.OpCreate, Body: []byte(`{"slow":true}`),
		}})
		commitDone <- err
	}()
	<-entered

	// The commit is paused after its detached plan is prepared. The renewal
	// must complete while the data mutex remains held by the commit goroutine.
	// Move halfway through the original lease first, so the heartbeat extends
	// the deadline beyond the original expiry rather than resetting it to t0.
	clock.Advance(t, driver.LeaseTTL/2)
	renewDone := make(chan error, 1)
	go func() { renewDone <- lease.Renew(ctx) }()
	select {
	case err := <-renewDone:
		if err != nil {
			t.Fatalf("renewal during paused commit: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("renewal waited on the paused commit")
	}
	// The commit now runs past its original TTL, but the accepted heartbeat
	// extends the same epoch and keeps the final publication valid.
	clock.Advance(t, driver.LeaseTTL/2+time.Nanosecond)
	close(release)
	if err := <-commitDone; err != nil {
		t.Fatalf("renewed slow commit: %v", err)
	}

	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Head != model.Natural64(1) {
		t.Fatalf("renewed commit head=%s, want 1", meta.Head)
	}
}

func TestMemoryExpiredLeaseRefusesCommitPublication(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 11, 1, 4, 29, 59, 999999999, time.UTC))
	s, err := NewWithOptions(Options{Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lease, err := s.Lease(ctx, "expired-commit", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID("pl-expired-commit", model.Natural64(0))
	key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err := tx.Lock(ctx, []driver.LockRequest{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"expired":true}`)}); err != nil {
		t.Fatal(err)
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(t, driver.LeaseTTL)
	_, err = tx.Commit(ctx, driver.ChangesetHeader{
		Incarnation: meta.Incarnation,
		Kind:        model.ChangesetWrite,
		Principal:   model.PrincipalRef{Kind: "system", ID: "memory-expired"},
		LockKeys:    []driver.LockKey{key},
		Changes:     []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}},
	})
	var fenced driver.ErrFenced
	if !errors.As(err, &fenced) {
		t.Fatalf("expired commit=%v, want ErrFenced", err)
	}
	if meta, err := s.Meta(ctx); err != nil || !meta.Head.IsZero() {
		t.Fatalf("expired commit published head=%s err=%v", meta.Head, err)
	}
}

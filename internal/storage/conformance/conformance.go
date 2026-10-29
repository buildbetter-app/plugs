// Package conformance exercises the shared storage contract against real stores.
package conformance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Factory returns a fresh empty store; the caller arranges driver cleanup.
// SQL drivers can reuse these cases without importing the memory driver.
type Factory func(*testing.T) driver.Store

func Run(t *testing.T, factory Factory) {
	t.Run("lifetime_close_rolls_back_and_releases", func(t *testing.T) { lifetimes(t, factory) })
	t.Run("G1_concurrent_gapless_revisions", func(t *testing.T) { concurrentGapless(t, factory) })
	t.Run("G2_locks_latest_and_conflicting", func(t *testing.T) { locksLatest(t, factory) })
	t.Run("G2_atomic_lock_set_and_skip_locked", func(t *testing.T) { atomicLockSet(t, factory) })
	t.Run("G3_atomic_rollback_and_snapshots", func(t *testing.T) { atomicPublication(t, factory) })
	t.Run("G3_derived_predicate_consistency", func(t *testing.T) { derivedPredicates(t, factory) })
	t.Run("G4_idempotency_and_responses", func(t *testing.T) { idempotency(t, factory) })
	t.Run("G4_noop_receipts_without_graph_revisions", func(t *testing.T) { noopReceipts(t, factory) })
	t.Run("G4_pending_response_publication", func(t *testing.T) { pendingPublication(t, factory) })
	t.Run("G4_final_response_is_immutable", func(t *testing.T) { finalResponses(t, factory) })
	t.Run("G5_response_finalization_CAS", func(t *testing.T) { finalizationRace(t, factory) })
	t.Run("G5_lease_fencing_and_incarnation", func(t *testing.T) { fencing(t, factory) })
	t.Run("G5_takeover_races_commit", func(t *testing.T) { takeoverRace(t, factory) })
	t.Run("G5_checkpoint_CAS_and_isolation", func(t *testing.T) { checkpoints(t, factory) })
	t.Run("history_replay_and_pagination", func(t *testing.T) { history(t, factory) })
	t.Run("history_scheduled_delete_and_recreation", func(t *testing.T) { historyRecreation(t, factory) })
}

func setup(t *testing.T, factory Factory) (context.Context, driver.Store, driver.ActiveLease, driver.Meta) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	s := factory(t)
	t.Cleanup(func() { _ = s.Close() })
	l, err := s.Lease(ctx, "conformance", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Release(context.Background()) })
	m, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, s, l, m
}

func concurrentGapless(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	const writers = 16
	start := make(chan struct{})
	errs := make(chan error, writers)
	var group sync.WaitGroup
	for i := 0; i < writers; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			tx, err := s.Write(ctx, lease)
			if err != nil {
				errs <- err
				return
			}
			defer tx.Rollback(context.Background())
			ref := model.RefByID(fmt.Sprintf("pl-concurrent-%02d", index), model.Natural64(0))
			key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
			if _, err = tx.Lock(ctx, []driver.LockRequest{{Key: key, Mode: driver.LockUpdate}}); err != nil {
				errs <- err
				return
			}
			if _, err = tx.Head(ctx); err != nil {
				errs <- err
				return
			}
			if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(fmt.Sprintf(`{"writer":%d}`, index))}); err != nil {
				errs <- err
				return
			}
			if index%4 == 0 {
				errs <- tx.Rollback(ctx)
				return
			}
			_, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "conformance"}, LockKeys: []driver.LockKey{key}, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}})
			errs <- err
		}(i)
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	log, err := r.Changesets(ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(log) != 12 {
		t.Fatalf("commits=%d, want 12", len(log))
	}
	for i, entry := range log {
		if entry.Revision.String() != model.Natural64(uint64(i+1)).String() {
			t.Fatalf("gap at log[%d]: %s", i, entry.Revision)
		}
	}
	page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}})
	if err != nil || len(page.Items) != 12 {
		t.Fatalf("records=%d error=%v", len(page.Items), err)
	}
}

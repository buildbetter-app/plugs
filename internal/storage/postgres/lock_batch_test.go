package postgres

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestBatchedLocksKeepOpaqueRefIdentityAndOwnership(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	requests := driver.LockSet{}
	for _, value := range []string{"raw:\xff", "raw:\xfe", "raw:\ufffd", "raw:\x00:tail", ""} {
		ref := model.RefByKey(value)
		requests = append(requests, driver.LockRequest{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate})
	}
	ref := model.RefByID("raw-\xff", model.Natural64(0))
	requests = append(requests, driver.LockRequest{Key: testLock(ref), Mode: driver.LockUpdate})
	sort.Slice(requests, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(requests[i].Key)
		b, _ := driver.EncodeLockKey(requests[j].Key)
		return a < b
	})
	keys := make([]driver.LockKey, len(requests))
	ids := make([]string, len(requests))
	for i, request := range requests {
		// Keep an independent value so mutations below cannot change the oracle.
		ref := *request.Key.Ref
		keys[i] = driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		var err error
		ids[i], err = driver.EncodeLockKey(keys[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	states, err := tx.Lock(ctx, requests)
	if err != nil || len(states) != len(keys) {
		t.Fatalf("opaque lock set: states=%d want=%d error=%v", len(states), len(keys), err)
	}
	r := testRead(t, s, nil)
	observed, err := r.Observe(ctx, keys)
	if err != nil || len(observed) != len(keys) {
		t.Fatalf("opaque observations: states=%d want=%d error=%v", len(observed), len(keys), err)
	}
	for i, id := range ids {
		var rows int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.locks WHERE key=$1`, []byte(id)).Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("cold lock added a durable placeholder: key=%x rows=%d error=%v", id, rows, err)
		}
		for _, state := range []driver.LockState{states[i], observed[i]} {
			got, err := driver.EncodeLockKey(state.Key)
			if err != nil || got != id || state.Present || !state.Revision.IsZero() {
				t.Fatalf("opaque state: want=%x got=%x present=%v revision=%v error=%v", id, got, state.Present, state.Revision, err)
			}
		}
		*requests[i].Key.Ref = model.RefByKey("changed-input")
		*states[i].Key.Ref = model.RefByKey("changed-lock-result")
		got, err := driver.EncodeLockKey(tx.(*transaction).acquired[i].Key)
		if err != nil || got != id {
			t.Fatalf("opaque acquired key aliases caller: want=%x got=%x error=%v", id, got, err)
		}
		*observed[i].Key.Ref = model.RefByKey("changed-observe-result")
		got, err = driver.EncodeLockKey(keys[i])
		if err != nil || got != id {
			t.Fatalf("opaque observation aliases input: want=%x got=%x error=%v", id, got, err)
		}
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	// Distinct raw encodings must remain independently lockable after the
	// first writer closes, including invalid UTF-8 and its replacement rune.
	owner, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if _, err := owner.Lock(ctx, driver.LockSet{{Key: keys[0], Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	peer, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	probeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := peer.Lock(probeCtx, driver.LockSet{{Key: keys[1], Mode: driver.LockUpdate}}); err != nil {
		t.Fatalf("distinct opaque key falsely conflicted: %v", err)
	}
	t.Log("OPAQUE_LOCK_IDENTITY raw_keys_distinct=true lock_and_observe_order=true caller_owned=true no_placeholder_rows=true")
}

func TestColdShareHoldersCoexistWithoutPlaceholderRows(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	key := testLock(testRef("cold-shared"))
	share := driver.LockSet{{Key: key, Mode: driver.LockShare}}
	first, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := first.Lock(ctx, share); err != nil {
		t.Fatal(err)
	}
	second, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := second.Lock(bounded, share); err != nil {
		t.Fatalf("compatible cold share waited for first writer: %v", err)
	}
	skipped, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer skipped.Close()
	states, err := skipped.Lock(bounded, driver.LockSet{{Key: key, Mode: driver.LockSkipLocked}})
	if err != nil || len(states) != 0 {
		t.Fatalf("cold update passed shared holders: states=%v error=%v", states, err)
	}
	if err := first.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	update, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer update.Close()
	if _, err := update.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatalf("cold update remained blocked after shared rollback: %v", err)
	}
	var rows int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.locks`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("cold lock left coordination rows: rows=%d error=%v", rows, err)
	}
}

func TestNativeRestoreBarrierCoversDisjointColdKeysWithoutRows(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	fence, err := driver.NativeAllocatorRestoreLock()
	if err != nil {
		t.Fatal(err)
	}
	restoreKey := testLock(testRef("restore-cold-key"))
	ordinaryKey := testLock(testRef("unrelated-cold-key"))
	restoreSet := driver.LockSet{{Key: fence, Mode: driver.LockUpdate}, {Key: restoreKey, Mode: driver.LockUpdate}}
	sort.Slice(restoreSet, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(restoreSet[i].Key)
		b, _ := driver.EncodeLockKey(restoreSet[j].Key)
		return a < b
	})
	restore, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer restore.Close()
	states, err := restore.Lock(ctx, restoreSet)
	if err != nil || len(states) != len(restoreSet) {
		t.Fatalf("restore did not acquire its complete set: states=%d err=%v", len(states), err)
	}
	skipped, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer skipped.Close()
	states, err = skipped.Lock(ctx, driver.LockSet{{Key: ordinaryKey, Mode: driver.LockSkipLocked}})
	if err != nil || len(states) != 0 {
		t.Fatalf("ordinary cold key escaped restore barrier: states=%d err=%v", len(states), err)
	}
	waiting, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Close()
	deadline, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = waiting.Lock(deadline, driver.LockSet{{Key: ordinaryKey, Mode: driver.LockUpdate}})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ordinary update did not wait for restore: %v", err)
	}
	if err := restore.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	ordinary, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Close()
	states, err = ordinary.Lock(ctx, driver.LockSet{{Key: ordinaryKey, Mode: driver.LockShare}})
	if err != nil || len(states) != 1 || states[0].Present {
		t.Fatalf("ordinary cold share after restore: states=%+v err=%v", states, err)
	}
	blocked, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.Close()
	deadline, cancel = context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = blocked.Lock(deadline, restoreSet)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("restore did not wait for an ordinary share: %v", err)
	}
	if err := ordinary.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.locks`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("barrier left durable coordination rows: rows=%d err=%v", rows, err)
	}
}

func TestBatchedLocksPreserveMixedModesAndSkippedSubset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	owner, _ := beginTest(t, s, lease, testLock(testRef("mixed-f")))
	requests := driver.LockSet{}
	modes := []driver.LockMode{driver.LockShare, driver.LockShare, driver.LockUpdate, driver.LockUpdate, driver.LockSkipLocked, driver.LockSkipLocked, driver.LockSkipLocked, driver.LockShare, driver.LockUpdate}
	for i, mode := range modes {
		requests = append(requests, driver.LockRequest{Key: testLock(testRef("mixed-" + string(rune('a'+i)))), Mode: mode})
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	states, err := tx.Lock(ctx, requests)
	if err != nil || len(states) != len(requests)-1 {
		t.Fatalf("mixed lock subset: %+v %v", states, err)
	}
	index := 0
	for i, request := range requests {
		if i == 5 {
			continue
		}
		if states[index].Key.Ref.ID != request.Key.Ref.ID || states[index].Present || !states[index].Revision.IsZero() {
			t.Fatalf("missing-key state/order changed at %d: %+v", i, states[index])
		}
		index++
	}
	for _, i := range []int{0, 1, 5, 7} {
		if err := tx.Put(ctx, driver.Record{Ref: *requests[i].Key.Ref, Kind: "test", Body: []byte(`{}`)}); err == nil {
			t.Fatalf("share/skipped key %d authorized write", i)
		}
	}
	for _, i := range []int{2, 3, 4, 6, 8} {
		if err := tx.Put(ctx, driver.Record{Ref: *requests[i].Key.Ref, Kind: "test", Body: []byte(`{}`)}); err != nil {
			t.Fatalf("acquired update key %d: %v", i, err)
		}
	}
	// Both input requests and returned observations remain caller-owned.
	requests[0].Key.Ref.ID = "changed-input"
	states[0].Key.Ref.ID = "changed-result"
	if got := tx.(*transaction).acquired[0].Key.Ref.ID; got != "mixed-a" {
		t.Fatalf("acquired key aliases caller memory: %s", got)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBatchedLocksCancelWithoutRetainingMixedModePrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	owner, _ := beginTest(t, s, lease, testLock(testRef("wait-d")))
	requests := driver.LockSet{
		{Key: testLock(testRef("wait-a")), Mode: driver.LockShare},
		{Key: testLock(testRef("wait-b")), Mode: driver.LockShare},
		{Key: testLock(testRef("wait-c")), Mode: driver.LockUpdate},
		{Key: testLock(testRef("wait-d")), Mode: driver.LockUpdate},
	}
	waiter, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer waiter.Close()
	waitCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { _, err := waiter.Lock(waitCtx, requests); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("conflicting mixed-mode set completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	probe, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	probeCtx, stopProbe := context.WithTimeout(ctx, time.Second)
	defer stopProbe()
	if _, err := probe.Lock(probeCtx, driver.LockSet{{Key: requests[0].Key, Mode: driver.LockUpdate}, {Key: requests[1].Key, Mode: driver.LockUpdate}, {Key: requests[2].Key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatalf("failed acquisition retained a mixed-mode prefix: %v", err)
	}
	stop()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation result: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("mixed-mode acquisition ignored cancellation")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}

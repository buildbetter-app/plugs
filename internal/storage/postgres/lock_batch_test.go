package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

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

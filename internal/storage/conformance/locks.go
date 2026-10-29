package conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func locksLatest(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	put(t, ctx, s, lease, meta, "pl-lock", `{"value":1}`)
	pinned := read(t, ctx, s, meta, nil)
	requests := update(recordLock("pl-lock"))
	first := begin(t, ctx, s, lease, requests)
	if _, err := first.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Put(ctx, driver.Record{Ref: ref("pl-lock"), Kind: "test", Body: []byte(`{"value":2}`)}); err != nil {
		t.Fatal(err)
	}
	second, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback(ctx)
	type result struct {
		states []driver.LockState
		err    error
	}
	done := make(chan result, 1)
	started := make(chan struct{})
	go func() { close(started); states, err := second.Lock(ctx, requests); done <- result{states, err} }()
	<-started
	select {
	case value := <-done:
		t.Fatalf("conflicting lock returned before commit: %+v", value)
	case <-time.After(20 * time.Millisecond):
	}
	// Read must complete while the first writer owns both row and head locks.
	current := read(t, ctx, s, meta, nil)
	records, err := current.Get(ctx, []driver.Ref{ref("pl-lock")})
	if err != nil || len(records) != 1 || string(records[0].Body) != `{"value":1}` {
		t.Fatalf("dirty or blocked read: %v %v", records, err)
	}
	if _, err = first.Commit(ctx, header(meta, requests, model.ChangeRecord{Ref: ref("pl-lock"), Op: model.OpPatch, FromRevision: model.Natural64(1)})); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-done:
		if value.err != nil || len(value.states) != 1 || value.states[0].Revision.String() != "2" || value.states[0].Record == nil || string(value.states[0].Record.Body) != `{"value":2}` {
			t.Fatalf("lock did not return latest committed state: %+v", value)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	old, err := pinned.Get(ctx, []driver.Ref{ref("pl-lock")})
	if err != nil || string(old[0].Body) != `{"value":1}` {
		t.Fatalf("snapshot changed: %v %v", old, err)
	}
}

func atomicLockSet(t *testing.T, factory Factory) {
	ctx, s, lease, _ := setup(t, factory)
	owner := begin(t, ctx, s, lease, update(recordLock("pl-b")))
	waiter, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer waiter.Rollback(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := waiter.Lock(waitCtx, update(recordLock("pl-a"), recordLock("pl-b"))); done <- err }()
	// A failed/waiting complete-set acquisition must not retain its first key.
	probe := begin(t, ctx, s, lease, update(recordLock("pl-a")))
	if err := probe.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error=%v", err)
	}
	skipped, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer skipped.Rollback(ctx)
	requests := update(recordLock("pl-a"), recordLock("pl-b"))
	for i := range requests {
		requests[i].Mode = driver.LockSkipLocked
	}
	states, err := skipped.Lock(ctx, requests)
	if err != nil || len(states) != 1 || states[0].Key.Ref.ID != "pl-a" {
		t.Fatalf("skipLocked=%v %v", states, err)
	}
	if err := skipped.Put(ctx, driver.Record{Ref: ref("pl-b"), Kind: "test", Body: []byte(`{}`)}); err == nil {
		t.Fatal("write under skipped key succeeded")
	}
	if _, err := skipped.Lock(ctx, nil); err == nil {
		t.Fatal("late second lock set accepted")
	}
	if err := owner.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	shareRequests := []driver.LockRequest{{Key: recordLock("pl-b"), Mode: driver.LockShare}}
	share1 := begin(t, ctx, s, lease, shareRequests)
	share2 := begin(t, ctx, s, lease, shareRequests)
	if err := share1.Put(ctx, driver.Record{Ref: ref("pl-b"), Kind: "test", Body: []byte(`{}`)}); err == nil {
		t.Fatal("share lock authorized write")
	}
	_ = share1.Rollback(ctx)
	_ = share2.Rollback(ctx)
	invalid, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer invalid.Rollback(ctx)
	unsorted := []driver.LockRequest{{Key: recordLock("pl-b"), Mode: driver.LockUpdate}, {Key: recordLock("pl-a"), Mode: driver.LockUpdate}}
	if _, err := invalid.Lock(ctx, unsorted); err == nil {
		t.Fatal("unsorted locks accepted")
	}
	if _, err := invalid.Lock(ctx, []driver.LockRequest{unsorted[0], unsorted[0]}); err == nil {
		t.Fatal("duplicate locks accepted")
	}
}

package conformance

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func checkpoints(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	store, ok := s.(driver.CheckpointStore)
	if !ok {
		t.Fatal("CheckpointStore capability missing")
	}
	value, err := store.LoadCheckpoint(ctx, "delivery")
	if err != nil || value.Generation != 0 || value.Payload != nil {
		t.Fatalf("missing checkpoint=%+v %v", value, err)
	}
	input := []byte("initial")
	value, err = store.SaveCheckpoint(ctx, lease, "delivery", 0, input)
	if err != nil || value.Generation != 1 {
		t.Fatalf("save=%+v %v", value, err)
	}
	input[0] = '!'
	value.Payload[0] = '?'
	value, err = store.LoadCheckpoint(ctx, "delivery")
	if err != nil || string(value.Payload) != "initial" {
		t.Fatal("input/save-return alias leaked")
	}
	value.Payload[0] = '!'
	value, err = store.LoadCheckpoint(ctx, "delivery")
	if err != nil || string(value.Payload) != "initial" {
		t.Fatal("load alias leaked")
	}
	const writers = 12
	start := make(chan struct{})
	errs := make(chan error, writers)
	var group sync.WaitGroup
	for i := 0; i < writers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, err := store.SaveCheckpoint(ctx, lease, "delivery", 1, []byte("winner"))
			errs <- err
		}()
	}
	close(start)
	group.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else if !errors.Is(err, driver.ErrCheckpointConflict) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatalf("CAS winners=%d", wins)
	}
	value, err = store.LoadCheckpoint(ctx, "delivery")
	if err != nil || value.Generation != 2 || string(value.Payload) != "winner" {
		t.Fatalf("CAS publication=%+v %v", value, err)
	}
	if _, err := store.SaveCheckpoint(ctx, lease, "delivery", 1, []byte("loser")); !errors.Is(err, driver.ErrCheckpointConflict) {
		t.Fatalf("CAS mismatch=%v", err)
	}
	for _, key := range []string{"", " leading", "trailing ", "nul\x00key"} {
		if _, err := store.SaveCheckpoint(ctx, lease, key, 0, []byte("bad")); err == nil {
			t.Fatalf("invalid key %q accepted", key)
		}
		if _, err := store.LoadCheckpoint(ctx, key); err == nil {
			t.Fatalf("invalid load key %q accepted", key)
		}
	}
	foreignStore := factory(t)
	foreign, err := foreignStore.Lease(ctx, "foreign", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Release(context.Background())
	_, err = store.SaveCheckpoint(ctx, foreign, "delivery", 2, []byte("foreign"))
	requireFenced(t, err)
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := s.Lease(ctx, "next", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release(ctx)
	_, err = store.SaveCheckpoint(ctx, lease, "delivery", 2, []byte("stale"))
	requireFenced(t, err)
	value, err = store.LoadCheckpoint(ctx, "delivery")
	if err != nil || value.Generation != 2 || string(value.Payload) != "winner" {
		t.Fatalf("failed saves changed checkpoint=%+v %v", value, err)
	}
	after, err := s.Meta(ctx)
	if err != nil || after.Head.String() != meta.Head.String() {
		t.Fatalf("checkpoint advanced graph=%+v %v", after, err)
	}
	log, err := read(t, ctx, s, meta, nil).Changesets(ctx, nil, 100)
	if err != nil || len(log) != 0 {
		t.Fatalf("checkpoint polluted log=%v %v", log, err)
	}
}

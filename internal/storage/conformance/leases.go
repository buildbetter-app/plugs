package conformance

import (
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func requireFenced(t *testing.T, err error) {
	t.Helper()
	var fenced driver.ErrFenced
	if !errors.As(err, &fenced) {
		t.Fatalf("want ErrFenced, got %v", err)
	}
}

func fencing(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	requests := update(recordLock("pl-fenced"))
	stale := begin(t, ctx, s, lease, requests)
	if _, err := stale.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := stale.Put(ctx, driver.Record{Ref: ref("pl-fenced"), Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lease.Lost():
	default:
		t.Fatal("release did not signal loss")
	}
	next, err := s.Lease(ctx, "takeover", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release(ctx)
	if next.Epoch() <= lease.Epoch() {
		t.Fatal("epoch did not increase")
	}
	_, err = stale.Commit(ctx, header(meta, requests, model.ChangeRecord{Ref: ref("pl-fenced"), Op: model.OpCreate}))
	requireFenced(t, err)
	requireFenced(t, lease.Renew(ctx))
	_, err = s.Write(ctx, lease)
	requireFenced(t, err)
	other := factory(t)
	foreign, err := other.Lease(ctx, "foreign", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Release(ctx)
	_, err = s.Write(ctx, foreign)
	requireFenced(t, err)
	put(t, ctx, s, next, meta, "pl-live", `{"value":1}`)
	rows, err := read(t, ctx, s, meta, nil).Get(ctx, []driver.Ref{ref("pl-fenced"), ref("pl-live")})
	if err != nil || len(rows) != 1 || rows[0].Ref.ID != "pl-live" {
		t.Fatalf("stale publication: %v %v", rows, err)
	}
	_, err = s.Read(ctx, driver.View{Incarnation: "old-incarnation"})
	var mismatch driver.ErrIncarnationChanged
	if !errors.As(err, &mismatch) {
		t.Fatalf("view mismatch=%v", err)
	}
	r := read(t, ctx, s, meta, nil)
	_, err = r.Changesets(ctx, &driver.Cursor{Incarnation: "old-incarnation"}, 100)
	if !errors.As(err, &mismatch) {
		t.Fatalf("cursor mismatch=%v", err)
	}
	if _, err = r.Changesets(ctx, &driver.Cursor{Incarnation: meta.Incarnation, Index: model.Natural64(1)}, 100); err == nil {
		t.Fatal("partial changeset cursor accepted")
	}
	// Actual expiry must close Lost without another API call and cannot renew.
	_ = next.Release(ctx)
	short, err := s.Lease(ctx, "short", 25*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-short.Lost():
	case <-time.After(time.Second):
		t.Fatal("lease expiry was not signalled")
	}
	requireFenced(t, short.Renew(ctx))
	fresh, err := s.Lease(ctx, "fresh", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release(ctx)
	if fresh.Epoch() <= short.Epoch() {
		t.Fatal("expiry takeover reused epoch")
	}
}

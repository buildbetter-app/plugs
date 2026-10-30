package conformance

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func lifetimes(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	r := read(t, ctx, s, meta, nil)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(ctx, nil); err == nil {
		t.Fatal("closed reader Get succeeded")
	}
	if _, err := r.Query(ctx, driver.Query{}); err == nil {
		t.Fatal("closed reader Query succeeded")
	}
	if _, err := r.Observe(ctx, nil); err == nil {
		t.Fatal("closed reader Observe succeeded")
	}
	if _, err := r.Versions(ctx, ref("pl-close"), driver.PageReq{}); err == nil {
		t.Fatal("closed reader Versions succeeded")
	}
	if _, err := r.Changesets(ctx, nil, 100); err == nil {
		t.Fatal("closed reader Changesets succeeded")
	}
	if _, err := r.Blob(ctx, "absent"); err == nil {
		t.Fatal("closed reader Blob succeeded")
	}
	if _, err := r.IdempotencyByKey(ctx, "system:test", "", "test", "key"); err == nil {
		t.Fatal("closed reader IdempotencyByKey succeeded")
	}
	if _, err := r.ChangesetByKey(ctx, "system:test", "", "test", "key"); err == nil {
		t.Fatal("closed reader ChangesetByKey succeeded")
	}
	requests := update(recordLock("pl-close"))
	tx := begin(t, ctx, s, lease, requests)
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, driver.Record{Ref: ref("pl-close"), Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
		t.Fatal(err)
	}
	hash, err := tx.PutBlob(ctx, []byte("unpublished"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, header(meta, requests, model.ChangeRecord{Ref: ref("pl-close"), Op: model.OpCreate})); err == nil {
		t.Fatal("close committed staged data")
	}
	current := read(t, ctx, s, meta, nil)
	records, err := current.Get(ctx, []driver.Ref{ref("pl-close")})
	if err != nil || len(records) != 0 {
		t.Fatalf("closed tx published record=%v %v", records, err)
	}
	if _, err := current.Blob(ctx, hash); err == nil {
		t.Fatal("closed tx published blob")
	}
	log, err := current.Changesets(ctx, nil, 100)
	if err != nil || len(log) != 0 || !current.Snapshot().At.IsZero() {
		t.Fatalf("close consumed revision=%v %v", log, err)
	}
	_ = current.Close()
	// Reacquiring the same row/head proves Close released both locks.
	put(t, ctx, s, lease, meta, "pl-close", `{"value":2}`)
	completed := begin(t, ctx, s, lease, nil)
	if _, err := completed.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := completed.Commit(ctx, header(meta, nil)); err != nil {
		t.Fatal(err)
	}
	if err := completed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := completed.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

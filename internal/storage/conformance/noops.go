package conformance

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func noopReceipts(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	put(t, ctx, s, lease, meta, "pl-noop", `{"value":1}`)
	sentinel, err := driver.NewBeadSentinel("pl-noop")
	if err != nil {
		t.Fatal(err)
	}
	requests := update(recordLock("pl-noop"), driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	h := header(meta, requests)
	h.Method, h.IdempotencyKey, h.RequestHash = "bead.update", "noop", "digest"
	h.Response = model.Commit{Results: []model.OpResult{{Ref: ref("pl-noop"), Revision: model.Natural64(1)}}, Warnings: []model.Warning{}}
	commit := func(h driver.ChangesetHeader) (model.Commit, error) {
		tx := begin(t, ctx, s, lease, requests)
		defer tx.Close()
		if _, err := tx.Head(ctx); err != nil {
			return model.Commit{}, err
		}
		committer, ok := tx.(driver.NoopCommitter)
		if !ok {
			t.Fatal("NoopCommitter capability missing")
		}
		response, err := committer.CommitNoop(ctx, h)
		if err == nil {
			if _, err := tx.Get(ctx, nil); err == nil {
				t.Fatal("successful no-op left transaction open")
			}
		}
		return response, err
	}
	pinned := read(t, ctx, s, meta, nil)
	first, err := commit(h)
	if err != nil || first.Revision.String() != "1" || first.Results == nil || first.Warnings == nil {
		t.Fatalf("no-op response=%+v %v", first, err)
	}
	if cached, err := pinned.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey); err != nil || cached != nil {
		t.Fatal("no-op changed pinned metadata snapshot")
	}
	r := read(t, ctx, s, meta, nil)
	cached, err := r.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
	if err != nil || cached == nil || cached.RequestHash != h.RequestHash || !reflect.DeepEqual(cached.Commit, first) || !reflect.DeepEqual(cached.Changeset, model.Changeset{}) {
		t.Fatalf("receipt=%+v %v", cached, err)
	}
	if entry, err := r.ChangesetByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey); err != nil || entry != nil {
		t.Fatalf("fabricated no-op changeset=%+v %v", entry, err)
	}
	// Advance an unrelated row. Replay retains the original response while a
	// fresh key uses the current head and preserves the old row revision.
	put(t, ctx, s, lease, meta, "pl-unrelated", `{}`)
	replayed, err := commit(h)
	if err != nil || !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replay=%+v %v", replayed, err)
	}
	conflict := h
	conflict.RequestHash = "changed"
	if _, err := commit(conflict); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("no-op digest conflict=%v", err)
	}
	fresh := h
	fresh.IdempotencyKey = "fresh"
	response, err := commit(fresh)
	if err != nil || response.Revision.String() != "2" || response.Results[0].Revision.String() != "1" {
		t.Fatalf("fresh no-op=%+v %v", response, err)
	}
	unkeyed := h
	unkeyed.IdempotencyKey, unkeyed.Method, unkeyed.RequestHash = "", "", ""
	if _, err := commit(unkeyed); err != nil {
		t.Fatalf("unkeyed no-op=%v", err)
	}
	after := read(t, ctx, s, meta, nil)
	versions, err := after.Versions(ctx, ref("pl-noop"), driver.PageReq{})
	if err != nil || len(versions.Items) != 1 {
		t.Fatalf("no-op changed versions=%v %v", versions, err)
	}
	log, err := after.Changesets(ctx, nil, 100)
	if err != nil || len(log) != 2 || after.Snapshot().At.String() != "2" {
		t.Fatalf("no-op changed log/head=%v %v", log, err)
	}
	states, err := after.Observe(ctx, []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: sentinel}})
	if err != nil || len(states) != 1 || states[0].Present || !states[0].Revision.IsZero() {
		t.Fatalf("no-op bumped guard sentinel=%v %v", states, err)
	}
	finalizer, ok := s.(driver.ResponseFinalizer)
	if !ok {
		t.Fatal("ResponseFinalizer capability missing")
	}
	warnings := []model.Warning{{Code: "post_subscriber_failed", Message: "post"}}
	if err := finalizer.FinalizeResponse(ctx, lease, "system:conformance", "", h.Method, h.IdempotencyKey, first.Revision, warnings); err != nil {
		t.Fatal(err)
	}
	final, err := commit(h)
	if err != nil || len(final.Warnings) != 1 || final.Revision.String() != "1" {
		t.Fatalf("finalized no-op replay=%+v %v", final, err)
	}
	// A deposed transaction must not publish even receipt-only metadata.
	stale := begin(t, ctx, s, lease, requests)
	if _, err := stale.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	staleHeader := h
	staleHeader.IdempotencyKey = "stale"
	var fenced driver.ErrFenced
	if _, err := stale.(driver.NoopCommitter).CommitNoop(ctx, staleHeader); !errors.As(err, &fenced) {
		t.Fatalf("stale no-op=%v", err)
	}
	latest := read(t, context.Background(), s, meta, nil)
	if entry, err := latest.IdempotencyByKey(ctx, "system:conformance", "", h.Method, "stale"); err != nil || entry != nil {
		t.Fatal("stale no-op published a receipt")
	}
}

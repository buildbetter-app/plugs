package conformance

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func idempotency(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	commit := func(id, agent, method, key, hash string) (model.Changeset, error) {
		requests := update(recordLock(id))
		tx := begin(t, ctx, s, lease, requests)
		defer tx.Rollback(context.Background())
		if _, err := tx.Head(ctx); err != nil {
			return model.Changeset{}, err
		}
		if err := tx.Put(ctx, driver.Record{Ref: ref(id), Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
			return model.Changeset{}, err
		}
		h := header(meta, requests, model.ChangeRecord{Ref: ref(id), Op: model.OpCreate})
		h.Principal = model.PrincipalRef{Kind: "anonymous", ID: ""}
		h.AgentLabel = agent
		h.Method = method
		h.IdempotencyKey = key
		h.RequestHash = hash
		h.ResponsePending = true
		h.Response = model.Commit{Results: []model.OpResult{{Ref: ref(id)}}, Warnings: []model.Warning{}}
		return tx.Commit(ctx, h)
	}
	first, err := commit("pl-first", "a", "bead.create", "key", "request")
	if err != nil {
		t.Fatal(err)
	}
	pinned := read(t, ctx, s, meta, nil)
	replayed, err := commit("pl-duplicate", "a", "bead.create", "key", "request")
	if err != nil || !reflect.DeepEqual(first, replayed) {
		t.Fatalf("replay=%+v %v", replayed, err)
	}
	if _, err := commit("pl-conflict", "a", "bead.create", "key", "different"); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("changed request replay=%v", err)
	}
	r := read(t, ctx, s, meta, nil)
	rows, err := r.Query(ctx, driver.Query{})
	if err != nil || len(rows.Items) != 1 {
		t.Fatalf("replayed graph mutation=%v %v", rows, err)
	}
	entry, err := r.IdempotencyByKey(ctx, "anonymous:", "a", "bead.create", "key")
	if err != nil || entry == nil || !entry.ResponsePending || entry.RequestHash != "request" || entry.Commit.Revision.String() != "1" || len(entry.Commit.Results) != 1 || entry.Commit.Results[0].Ref.ID != "pl-first" {
		t.Fatalf("response replay=%+v %v", entry, err)
	}
	entry.Commit.Results[0].Ref = ref("corrupted")
	entry.Changeset.Changes[0].ChangedPaths = []string{"corrupted"}
	entry, err = r.IdempotencyByKey(ctx, "anonymous:", "a", "bead.create", "key")
	if err != nil || entry.Commit.Results[0].Ref.ID != "pl-first" {
		t.Fatal("response alias leaked")
	}
	if absent, err := r.IdempotencyByKey(ctx, "anonymous:", "b", "bead.create", "key"); err != nil || absent != nil {
		t.Fatalf("agent isolation=%+v %v", absent, err)
	}
	if _, err := commit("pl-agent-b", "b", "bead.create", "key", "request"); err != nil {
		t.Fatal(err)
	}
	if _, err := commit("pl-method", "a", "other.write", "key", "request"); err != nil {
		t.Fatal(err)
	}
	finalizer, ok := s.(driver.ResponseFinalizer)
	if !ok {
		t.Fatal("ResponseFinalizer capability missing")
	}
	warnings := []model.Warning{{Code: "post_subscriber_failed", Message: "retry pending", Details: map[string]any{"binding": "test"}}}
	if err := finalizer.FinalizeResponse(ctx, lease, "anonymous:", "a", "bead.create", "key", first.Revision, warnings); err != nil {
		t.Fatal(err)
	}
	if err := finalizer.FinalizeResponse(ctx, lease, "anonymous:", "a", "bead.create", "key", first.Revision, warnings); err != nil {
		t.Fatal(err)
	}
	warnings[0].Details["binding"] = "mutated"
	if err := finalizer.FinalizeResponse(ctx, lease, "anonymous:", "a", "bead.create", "key", first.Revision, warnings); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("different post outcome=%v", err)
	}
	if err := finalizer.FinalizeResponse(ctx, lease, "anonymous:", "a", "bead.create", "key", model.Natural64(2), nil); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("wrong response revision=%v", err)
	}
	after := read(t, ctx, s, meta, nil)
	entry, err = after.IdempotencyByKey(ctx, "anonymous:", "a", "bead.create", "key")
	if err != nil || entry.ResponsePending || len(entry.Commit.Warnings) != 1 || entry.Commit.Warnings[0].Details["binding"] != "test" {
		t.Fatalf("final warnings=%+v %v", entry, err)
	}
	old, err := pinned.IdempotencyByKey(ctx, "anonymous:", "a", "bead.create", "key")
	if err != nil || !old.ResponsePending || len(old.Commit.Warnings) != 0 {
		t.Fatal("finalization changed pinned snapshot")
	}
	log, err := after.Changesets(ctx, nil, 100)
	if err != nil || len(log) != 3 || after.Snapshot().At.String() != "3" {
		t.Fatalf("finalization advanced graph: %v %v", log, err)
	}
}

package conformance

import (
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func pendingPublication(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "conformance"}, Method: "bead.create", IdempotencyKey: "pending", RequestHash: "digest", ResponsePending: true}
	changes := []FixtureChange{{Ref: ref("pl-pending"), Kind: "test", Op: model.OpCreate, Body: []byte(`{"value":1}`)}}
	if err := SeedRollback(ctx, s, lease, h, changes); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"unkeyed", "invalid_response"} {
		bad := h
		if invalid == "unkeyed" {
			bad.IdempotencyKey = ""
		} else {
			bad.Response.Warnings = []model.Warning{{Code: "post_subscriber_failed", Details: map[string]any{"invalid": make(chan int)}}}
		}
		if _, err := Seed(ctx, s, lease, bad, changes); err == nil {
			t.Fatalf("%s pending write succeeded", invalid)
		}
		r := read(t, ctx, s, meta, nil)
		cached, err := r.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
		if err != nil || cached != nil || !r.Snapshot().At.IsZero() {
			t.Fatalf("failed/rolled-back pending write published receipt/head: %+v %v", cached, err)
		}
		rows, err := r.Get(ctx, []driver.Ref{changes[0].Ref})
		if err != nil || len(rows) != 0 {
			t.Fatalf("failed pending write published record: %+v %v", rows, err)
		}
		log, err := r.Changesets(ctx, nil, 100)
		if err != nil || len(log) != 0 {
			t.Fatalf("failed pending write published log: %+v %v", log, err)
		}
	}
	entry, err := Seed(ctx, s, lease, h, changes)
	if err != nil {
		t.Fatal(err)
	}
	r := read(t, ctx, s, meta, nil)
	cached, err := r.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
	if err != nil || cached == nil || !cached.ResponsePending || cached.Commit.Revision.String() != "1" || !reflect.DeepEqual(cached.Changeset, entry) {
		t.Fatalf("pending graph/receipt publication: %+v %v", cached, err)
	}
	// A same-scope no-op may not turn the initial pending response into success.
	tx := begin(t, ctx, s, lease, nil)
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	h.Incarnation, h.Kind, h.ResponsePending = meta.Incarnation, model.ChangesetWrite, false
	committer, ok := tx.(driver.NoopCommitter)
	if !ok {
		t.Fatal("NoopCommitter capability missing")
	}
	if _, err := committer.CommitNoop(ctx, h); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("no-op exposed pending response: %v", err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	after := read(t, ctx, s, meta, nil)
	unchanged, err := after.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
	if err != nil || !reflect.DeepEqual(cached, unchanged) || after.Snapshot().At.String() != "1" {
		t.Fatalf("pending no-op refusal changed receipt/head: %+v %v", unchanged, err)
	}
}

func finalResponses(t *testing.T, factory Factory) {
	for _, pending := range []bool{false, true} {
		name := "initially_final"
		if pending {
			name = "empty_post_outcome"
		}
		t.Run(name, func(t *testing.T) {
			ctx, s, lease, meta := setup(t, factory)
			h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "conformance"}, Method: "bead.create", IdempotencyKey: "final", RequestHash: "digest", ResponsePending: pending}
			entry, err := Seed(ctx, s, lease, h, nil)
			if err != nil {
				t.Fatal(err)
			}
			finalizer, ok := s.(driver.ResponseFinalizer)
			if !ok {
				t.Fatal("ResponseFinalizer capability missing")
			}
			pinned := read(t, ctx, s, meta, nil)
			for _, empty := range [][]model.Warning{nil, {}, nil} {
				if err := finalizer.FinalizeResponse(ctx, lease, "system:conformance", "", h.Method, h.IdempotencyKey, entry.Revision, empty); err != nil {
					t.Fatalf("nil/empty finalization: %v", err)
				}
			}
			if err := finalizer.FinalizeResponse(ctx, lease, "system:conformance", "", h.Method, h.IdempotencyKey, entry.Revision, []model.Warning{{Code: "post_subscriber_failed"}}); !errors.Is(err, driver.ErrConflict) {
				t.Fatalf("final response accepted new outcome: %v", err)
			}
			old, err := pinned.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
			if err != nil || old == nil || old.ResponsePending != pending {
				t.Fatalf("changed pinned receipt: %+v %v", old, err)
			}
			fresh := read(t, ctx, s, meta, nil)
			cached, err := fresh.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
			if err != nil || cached == nil || cached.ResponsePending || cached.Commit.Results == nil || cached.Commit.Warnings == nil || len(cached.Commit.Warnings) != 0 {
				t.Fatalf("empty finalized receipt: %+v %v", cached, err)
			}
		})
	}
}

func finalizationRace(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "conformance"}, Method: "bead.create", IdempotencyKey: "race", RequestHash: "digest", ResponsePending: true, Response: model.Commit{
		Results:  []model.OpResult{{Ref: ref("pl-finalize"), Revision: model.Natural64(1)}},
		Warnings: []model.Warning{{Code: "subscriber_skipped", Message: "initial"}},
	}}
	entry, err := Seed(ctx, s, lease, h, []FixtureChange{{Ref: ref("pl-finalize"), Kind: "test", Op: model.OpCreate, Body: []byte(`{"value":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	put(t, ctx, s, lease, meta, "pl-finalize-head", `{}`)
	pinned := read(t, ctx, s, meta, nil)
	finalizer, ok := s.(driver.ResponseFinalizer)
	if !ok {
		t.Fatal("ResponseFinalizer capability missing")
	}
	warnings := [][]model.Warning{
		{{Code: "post_subscriber_failed", Message: "first", Details: map[string]any{"binding": "first"}}},
		{{Code: "post_subscriber_failed", Message: "second", Details: map[string]any{"binding": "second"}}},
	}
	start := make(chan struct{})
	results := make(chan struct {
		index int
		err   error
	}, 2)
	for i := range warnings {
		go func(i int) {
			<-start
			err := finalizer.FinalizeResponse(ctx, lease, "system:conformance", "", h.Method, h.IdempotencyKey, entry.Revision, warnings[i])
			results <- struct {
				index int
				err   error
			}{i, err}
		}(i)
	}
	close(start)
	winner := -1
	for range warnings {
		result := <-results
		if result.err == nil {
			if winner != -1 {
				t.Fatal("different final outcomes both succeeded")
			}
			winner = result.index
		} else if !errors.Is(result.err, driver.ErrConflict) {
			t.Fatalf("finalization race: %v", result.err)
		}
	}
	if winner == -1 {
		t.Fatal("neither final outcome committed")
	}
	if err := finalizer.FinalizeResponse(ctx, lease, "system:conformance", "", h.Method, h.IdempotencyKey, entry.Revision, warnings[winner]); err != nil {
		t.Fatalf("same-outcome retry: %v", err)
	}
	fresh := read(t, ctx, s, meta, nil)
	cached, err := fresh.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
	if err != nil || cached == nil || cached.ResponsePending || cached.RequestHash != h.RequestHash || cached.Commit.Revision.String() != "1" || !reflect.DeepEqual(cached.Commit.Results, h.Response.Results) || !reflect.DeepEqual(cached.Changeset, entry) || len(cached.Commit.Warnings) != 2 || cached.Commit.Warnings[0].Message != "initial" || cached.Commit.Warnings[1].Message != warnings[winner][0].Message {
		t.Fatalf("final response CAS: %+v %v", cached, err)
	}
	old, err := pinned.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
	if err != nil || old == nil || !old.ResponsePending || len(old.Commit.Warnings) != 1 {
		t.Fatalf("finalization changed pinned pending receipt: %+v %v", old, err)
	}
	message := warnings[winner][0].Message
	warnings[winner][0].Details["binding"] = "input alias"
	cached.Commit.Warnings[1].Details["binding"] = "lookup alias"
	cached.Commit.Results[0].Ref = ref("pl-corrupted")
	isolated, err := fresh.IdempotencyByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
	if err != nil || isolated.Commit.Warnings[1].Details["binding"] != message || isolated.Commit.Results[0].Ref.ID != "pl-finalize" {
		t.Fatalf("finalization copy isolation: %+v %v", isolated, err)
	}
	versions, err := fresh.Versions(ctx, ref("pl-finalize"), driver.PageReq{})
	if err != nil || len(versions.Items) != 1 || fresh.Snapshot().At.String() != "2" {
		t.Fatalf("finalization changed versions/head: %+v %v", versions, err)
	}
	log, err := fresh.Changesets(ctx, nil, 100)
	if err != nil || len(log) != 2 {
		t.Fatalf("finalization changed log: %+v %v", log, err)
	}
}

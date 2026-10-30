package conformance

import (
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func poisonAudit(events []model.EventDescriptor) {
	for i := range events {
		events[i].Provenance.Method = "mutated"
		events[i].Provenance.SetBy["status"] = "mutated"
		events[i].Provenance.Chain[0] = "mutated"
		events[i].Provenance.Cause.Chain[0] = "mutated"
		if d := events[i].Details; d != nil {
			d.Dependent.Ref.ID = "mutated"
			d.Edge.Kind = "mutated"
			d.Ontology.Project.ID = "mutated"
			d.After.Status = "mutated"
		}
	}
}

func auditIsolationAndReceipts(t *testing.T, factory Factory) {
	f := newAuditFixture(t, factory, 2)
	h := f.header
	h.ResponsePending = true
	h.Response.Warnings = []model.Warning{{Code: "subscriber_skipped", Message: "initial"}}
	wantResults := append([]model.OpResult(nil), h.Response.Results...)
	entry, err := Seed(f.ctx, f.store, f.lease, h, f.changes)
	if err != nil {
		t.Fatal(err)
	}
	want := entry
	want.Events = model.CloneEventDescriptors(entry.Events)
	poisonAudit(h.Events)
	poisonAudit(entry.Events)
	h.Response.Results[0].Revision = "999"
	r := read(t, f.ctx, f.store, f.meta, nil)
	assertStored := func(reader driver.Reader) *driver.IdempotencyRecord {
		t.Helper()
		log, err := reader.Changesets(f.ctx, &driver.Cursor{Incarnation: f.meta.Incarnation, Revision: "1"}, 100)
		if err != nil || len(log) != 1 || !reflect.DeepEqual(log[0], want) {
			t.Fatalf("log alias leaked: %+v %v", log, err)
		}
		poisonAudit(log[0].Events)
		lookup, err := reader.ChangesetByKey(f.ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
		if err != nil || lookup == nil || !reflect.DeepEqual(*lookup, want) {
			t.Fatalf("lookup alias leaked: %+v %v", lookup, err)
		}
		poisonAudit(lookup.Events)
		cached, err := reader.IdempotencyByKey(f.ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
		if err != nil || cached == nil || !reflect.DeepEqual(cached.Changeset, want) || !reflect.DeepEqual(cached.Commit.Results, wantResults) || cached.Commit.Revision.String() != want.Revision.String() {
			t.Fatalf("receipt alias or per-result revision changed: %+v %v", cached, err)
		}
		return cached
	}
	cached := assertStored(r)
	poisonAudit(cached.Changeset.Events)
	cached.Commit.Results[0].Ref.ID = "mutated"
	assertStored(r)
	tx := begin(t, f.ctx, f.store, f.lease, nil)
	assertStored(tx)
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	// Retry goes through the real driver's G4 path. It returns the original
	// plan even though this legacy caller supplies no replacement plan.
	retry := h
	retry.Events = nil
	replayed, err := Seed(f.ctx, f.store, f.lease, retry, f.changes)
	if err != nil || !reflect.DeepEqual(replayed, want) {
		t.Fatalf("receipt replay replaced plan: %+v %v", replayed, err)
	}
	poisonAudit(replayed.Events)
	assertStored(r)
	conflict := retry
	conflict.RequestHash = "different-digest"
	if _, err := Seed(f.ctx, f.store, f.lease, conflict, f.changes); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("digest conflict=%v", err)
	}
	finalizer, ok := f.store.(driver.ResponseFinalizer)
	if !ok {
		t.Fatal("ResponseFinalizer missing")
	}
	post := [][]model.Warning{
		{{Code: "post_subscriber_failed", Message: "first", Details: map[string]any{"binding": "first"}}},
		{{Code: "post_subscriber_failed", Message: "second", Details: map[string]any{"binding": "second"}}},
	}
	start := make(chan struct{})
	results := make(chan struct {
		index int
		err   error
	}, 2)
	for i := range post {
		go func(i int) {
			<-start
			err := finalizer.FinalizeResponse(f.ctx, f.lease, "system:conformance", "", h.Method, h.IdempotencyKey, want.Revision, post[i])
			results <- struct {
				index int
				err   error
			}{i, err}
		}(i)
	}
	close(start)
	winner := -1
	for range post {
		result := <-results
		if result.err == nil {
			if winner != -1 {
				t.Fatal("two different post outcomes won CAS")
			}
			winner = result.index
		} else if !errors.Is(result.err, driver.ErrConflict) {
			t.Fatal(result.err)
		}
	}
	if winner == -1 {
		t.Fatal("no post outcome won CAS")
	}
	if err := finalizer.FinalizeResponse(f.ctx, f.lease, "system:conformance", "", h.Method, h.IdempotencyKey, want.Revision, post[winner]); err != nil {
		t.Fatal(err)
	}
	fresh := read(t, f.ctx, f.store, f.meta, nil)
	final := assertStored(fresh)
	if final.ResponsePending || final.RequestHash != h.RequestHash || len(final.Commit.Warnings) != 2 || final.Commit.Warnings[1].Message != post[winner][0].Message {
		t.Fatalf("plan/receipt CAS split: %+v", final)
	}
	if old := assertStored(r); !old.ResponsePending || len(old.Commit.Warnings) != 1 {
		t.Fatal("finalization changed pinned pending receipt")
	}
	post[winner][0].Details["binding"] = "mutated"
	poisonAudit(final.Changeset.Events)
	if isolated := assertStored(fresh); isolated.Commit.Warnings[1].Details["binding"] != isolated.Commit.Warnings[1].Message {
		t.Fatal("post input aliased receipt")
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil || meta.Head.String() != want.Revision.String() {
		t.Fatal("replay or finalization consumed a graph revision", err)
	}
	// Later real mutations must not change the stored plan or its source pins.
	later := []FixtureChange{
		{Ref: ref("pl-blocker"), Kind: "bead", Op: model.OpPatch, Body: []byte(`{"status":"reopened"}`)},
		{Ref: ref("pl-dependent-0000"), Kind: "bead", Op: model.OpPatch, Body: []byte(`{"status":"changed-later"}`)},
		{Ref: ref("pl-ontology"), Kind: "ontology", Op: model.OpPatch, Body: []byte(`{"changed":"later"}`)},
	}
	if _, err := Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, later); err != nil {
		t.Fatal(err)
	}
	past := read(t, f.ctx, f.store, f.meta, &want.Revision)
	assertStored(past)
	for _, pin := range []model.EventImage{want.Events[1].Details.Dependent, want.Events[1].Details.BlockerBefore, want.Events[1].Details.BlockerAfter, {Ref: want.Events[1].Details.Ontology.Ref, Revision: want.Events[1].Details.Ontology.Revision}} {
		source := read(t, f.ctx, f.store, f.meta, &pin.Revision)
		rows, err := source.Get(f.ctx, []driver.Ref{pin.Ref})
		if err != nil || len(rows) != 1 || rows[0].Revision.String() != pin.Revision.String() {
			t.Fatalf("lost pinned source: %+v %v", rows, err)
		}
		if pin.Revision.String() == "1" && string(rows[0].Body) == `{"status":"changed-later"}` {
			t.Fatal("source pin resolved to current image")
		}
	}
}

package conformance

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// RunStagedReadOverlay proves that a transaction's Reader view includes its
// own records and derived-row mutations before publication. The committed
// revision remains the optimistic token while the staged image is visible.
func RunStagedReadOverlay(t *testing.T, factory Factory) {
	t.Helper()
	ctx, s, lease, meta := setup(t, factory)
	a, b := ref("staged-a"), ref("staged-b")
	derived := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "staged"}}
	requests := update(recordLock(a.ID), recordLock(b.ID), derived)
	keys := lockKeys(requests)
	row := driver.DerivedRow{Key: "staged", Value: []byte("before")}

	tx := begin(t, ctx, s, lease, requests)
	if err := tx.Put(ctx, driver.Record{Ref: a, Kind: "test", Body: []byte(`{"value":"a"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, driver.Record{Ref: b, Kind: "test", Body: []byte(`{"value":"b"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	assertStaged := func(states []driver.LockState, revision, bodyA, bodyB string, derivedPresent bool, derivedRevision, derivedValue string) {
		t.Helper()
		if len(states) != len(keys) {
			t.Fatalf("observation count=%d, want %d", len(states), len(keys))
		}
		for i, state := range states {
			if !reflect.DeepEqual(state.Key, keys[i]) {
				t.Fatalf("observation order changed at %d: %+v", i, state)
			}
			switch state.Key.Kind {
			case driver.LockRecord:
				wantBody := bodyA
				if state.Key.Ref.ID == b.ID {
					wantBody = bodyB
				}
				wantPresent := wantBody != ""
				if state.Present != wantPresent || state.Revision.String() != revision {
					t.Fatalf("record observation=%+v, want present=%t revision=%s", state, wantPresent, revision)
				}
				if wantBody != "" && (state.Record == nil || string(state.Record.Body) != wantBody || state.Record.Revision.String() != revision) {
					t.Fatalf("staged record=%+v, want body %q", state.Record, wantBody)
				}
			case driver.LockDerived:
				if state.Present != derivedPresent || state.Revision.String() != derivedRevision {
					t.Fatalf("derived observation=%+v", state)
				}
				if derivedPresent && (state.Derived == nil || string(state.Derived.Value) != derivedValue) {
					t.Fatalf("staged derived=%+v, want %q", state.Derived, derivedValue)
				}
			}
		}
	}
	observe := func(r driver.Reader) []driver.LockState {
		t.Helper()
		states, err := r.Observe(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		return states
	}
	states := observe(tx)
	// New rows have no committed revision yet. Both staged rows and the
	// derived row must nevertheless be visible to the transaction reader.
	assertStaged(states, "0", `{"value":"a"}`, `{"value":"b"}`, true, "0", "before")
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{
		Incarnation: meta.Incarnation, Kind: model.ChangesetWrite,
		Principal: model.PrincipalRef{Kind: "system", ID: "staged-read"},
		LockKeys:  keys,
		Changes:   []model.ChangeRecord{{Ref: a, Op: model.OpCreate}, {Ref: b, Op: model.OpCreate}},
	}); err != nil {
		t.Fatal(err)
	}

	// A second transaction must preserve the committed revision token while
	// exposing its staged deletion, replacement and derived-row removal.
	tx = begin(t, ctx, s, lease, requests)
	if err := tx.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, driver.Record{Ref: b, Kind: "test", Body: []byte(`{"value":"updated"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, []driver.DerivedRow{row}, nil); err != nil {
		t.Fatal(err)
	}
	states = observe(tx)
	assertStaged(states, "1", "", `{"value":"updated"}`, false, "1", "")
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	entry, err := tx.Commit(ctx, header(meta, requests,
		model.ChangeRecord{Ref: a, Op: model.OpDelete, FromRevision: model.Natural64(1)},
		model.ChangeRecord{Ref: b, Op: model.OpPatch, FromRevision: model.Natural64(1)},
	))
	if err != nil || entry.Revision.String() != "2" {
		t.Fatalf("replacement commit=%+v error=%v", entry, err)
	}
	committed := observe(read(t, ctx, s, meta, nil))
	assertStaged(committed, "2", "", `{"value":"updated"}`, false, "2", "")

	// Replacement must leave revision-one bytes intact. Returned observation
	// bodies and derived values belong to the caller, including historical reads.
	at := model.Natural64(1)
	historical := read(t, ctx, s, meta, &at)
	states = observe(historical)
	assertStaged(states, "1", `{"value":"a"}`, `{"value":"b"}`, true, "1", "before")
	for _, state := range states {
		if state.Record != nil {
			state.Record.Body[0] = '!'
		}
		if state.Derived != nil {
			state.Derived.Value[0] = '!'
		}
	}
	assertStaged(observe(historical), "1", `{"value":"a"}`, `{"value":"b"}`, true, "1", "before")

	// Recreating a deleted key and replacing a live key/derived tombstone
	// retain revision two while staged, and rollback publishes none of them.
	tx = begin(t, ctx, s, lease, requests)
	if err := tx.Put(ctx, driver.Record{Ref: a, Kind: "test", Body: []byte(`{"value":"rollback-create"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, driver.Record{Ref: b, Kind: "test", Body: []byte(`{"value":"rollback-update"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{{Key: row.Key, Value: []byte("rollback-derived")}}); err != nil {
		t.Fatal(err)
	}
	assertStaged(observe(tx), "2", `{"value":"rollback-create"}`, `{"value":"rollback-update"}`, true, "2", "rollback-derived")
	if head, err := tx.Head(ctx); err != nil || head.String() != "2" {
		t.Fatalf("head before rollback=%s error=%v", head, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	after := read(t, ctx, s, meta, nil)
	if states := observe(after); !reflect.DeepEqual(states, committed) {
		t.Fatalf("rollback changed committed observations: got %+v, want %+v", states, committed)
	}
	log, err := after.Changesets(ctx, nil, 100)
	if err != nil || len(log) != 2 || after.Snapshot().At.String() != "2" {
		t.Fatalf("rollback consumed revision: log=%+v snapshot=%+v error=%v", log, after.Snapshot(), err)
	}
	for _, target := range []driver.Ref{a, b} {
		versions, err := after.Versions(ctx, target, driver.PageReq{Limit: 10})
		if err != nil || len(versions.Items) != 2 || versions.NextCursor != nil {
			t.Fatalf("rollback changed versions for %s: %+v error=%v", target.ID, versions, err)
		}
	}
	assertStaged(observe(read(t, ctx, s, meta, &at)), "1", `{"value":"a"}`, `{"value":"b"}`, true, "1", "before")
}

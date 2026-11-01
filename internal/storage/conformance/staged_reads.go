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
	assertStaged := func(states []driver.LockState, revision, bodyA, bodyB string, derivedPresent bool, derivedValue string) {
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
				if wantBody != "" && (state.Record == nil || string(state.Record.Body) != wantBody) {
					t.Fatalf("staged record=%+v, want body %q", state.Record, wantBody)
				}
			case driver.LockDerived:
				if state.Present != derivedPresent || state.Revision.String() != revision {
					t.Fatalf("derived observation=%+v", state)
				}
				if derivedPresent && (state.Derived == nil || string(state.Derived.Value) != derivedValue) {
					t.Fatalf("staged derived=%+v, want %q", state.Derived, derivedValue)
				}
			}
		}
	}
	states, err := tx.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	// New rows have no committed revision yet. Both staged rows and the
	// derived row must nevertheless be visible to the transaction reader.
	assertStaged(states, "0", `{"value":"a"}`, `{"value":"b"}`, true, "before")
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
	states, err = tx.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	assertStaged(states, "1", "", `{"value":"updated"}`, false, "")
}

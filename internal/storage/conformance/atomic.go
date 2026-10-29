package conformance

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func atomicPublication(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	fact, _ := driver.NewReadFact("guard/test")
	derived := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "rank"}}
	requests := update(recordLock("pl-a"), recordLock("pl-b"), driver.LockKey{Kind: driver.LockReadFact, Fact: fact}, driver.OntologySentinelLock, derived)
	pinned := read(t, ctx, s, meta, nil)
	stage := func() (driver.WriteTx, driver.Hash) {
		tx := begin(t, ctx, s, lease, requests)
		body := []byte(`{"value":1}`)
		if err := tx.Put(ctx, driver.Record{Ref: ref("pl-a"), Kind: "test", Body: body}); err != nil {
			t.Fatal(err)
		}
		body[0] = '!'
		if err := tx.Put(ctx, driver.Record{Ref: ref("pl-b"), Kind: "test", Body: []byte(`{"value":2}`)}); err != nil {
			t.Fatal(err)
		}
		data := []byte("blob payload")
		hash, err := tx.PutBlob(ctx, data)
		if err != nil {
			t.Fatal(err)
		}
		data[0] = '!'
		row := driver.DerivedRow{Key: "rank", Value: []byte("rank-value")}
		if err := tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
		row.Value[0] = '!'
		if err := tx.BumpReadFacts(ctx, []driver.ReadFact{fact}); err != nil {
			t.Fatal(err)
		}
		if err := tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelOntology}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Head(ctx); err != nil {
			t.Fatal(err)
		}
		staged, err := tx.Get(ctx, []driver.Ref{ref("pl-a"), ref("pl-b")})
		if err != nil || len(staged) != 2 {
			t.Fatalf("read own writes=%v %v", staged, err)
		}
		return tx, hash
	}
	first, hash := stage()
	before := read(t, ctx, s, meta, nil)
	assertEmpty := func(r driver.Reader) {
		t.Helper()
		rows, err := r.Query(ctx, driver.Query{})
		if err != nil || len(rows.Items) != 0 {
			t.Fatalf("uncommitted records=%v %v", rows, err)
		}
		log, err := r.Changesets(ctx, nil, 100)
		if err != nil || len(log) != 0 {
			t.Fatalf("uncommitted log=%v %v", log, err)
		}
		if _, err := r.Blob(ctx, hash); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("uncommitted blob=%v", err)
		}
		states, err := r.Observe(ctx, lockKeys(requests))
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range states {
			if v.Present || !v.Revision.IsZero() {
				t.Fatalf("uncommitted key=%+v", v)
			}
		}
	}
	assertEmpty(before)
	if err := first.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Put(ctx, driver.Record{}); !errors.Is(err, driver.ErrTxClosed) {
		t.Fatalf("closed write=%v", err)
	}
	assertEmpty(read(t, ctx, s, meta, nil))
	second, _ := stage()
	changes := []model.ChangeRecord{{Ref: ref("pl-a"), Op: model.OpCreate}, {Ref: ref("pl-b"), Op: model.OpCreate}}
	bad := header(meta, requests, changes...)
	bad.LockKeys = bad.LockKeys[:len(bad.LockKeys)-1]
	if _, err := second.Commit(ctx, bad); err == nil {
		t.Fatal("incomplete lock header accepted")
	}
	assertEmpty(read(t, ctx, s, meta, nil))
	entry, err := second.Commit(ctx, header(meta, requests, changes...))
	if err != nil || entry.Revision.String() != "1" {
		t.Fatalf("commit=%+v %v", entry, err)
	}
	assertEmpty(pinned)
	r := read(t, ctx, s, meta, nil)
	states, err := r.Observe(ctx, lockKeys(requests))
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if !state.Present || state.Revision.String() != "1" {
			t.Fatalf("atomic key=%+v", state)
		}
	}
	if states[len(states)-1].Derived == nil || string(states[len(states)-1].Derived.Value) != "rank-value" {
		t.Fatalf("derived state=%+v", states[len(states)-1])
	}
	states[len(states)-1].Derived.Value[0] = '!'
	rows, err := r.Get(ctx, []driver.Ref{ref("pl-a"), ref("pl-b")})
	if err != nil || len(rows) != 2 || string(rows[0].Body) != `{"value":1}` {
		t.Fatalf("atomic records=%v %v", rows, err)
	}
	rows[0].Body[0] = '!'
	data, err := r.Blob(ctx, hash)
	if err != nil || string(data) != "blob payload" {
		t.Fatalf("blob=%s %v", data, err)
	}
	data[0] = '!'
	fresh := read(t, ctx, s, meta, nil)
	again, err := fresh.Get(ctx, []driver.Ref{ref("pl-a")})
	if err != nil || string(again[0].Body) != `{"value":1}` {
		t.Fatalf("record alias leaked: %v %v", again, err)
	}
	data, err = fresh.Blob(ctx, hash)
	if err != nil || string(data) != "blob payload" {
		t.Fatal("blob alias leaked")
	}
	states, err = fresh.Observe(ctx, lockKeys(requests))
	if err != nil || string(states[len(states)-1].Derived.Value) != "rank-value" {
		t.Fatal("derived alias leaked")
	}
	latest := begin(t, ctx, s, lease, update(driver.LockKey{Kind: driver.LockReadFact, Fact: fact}))
	observed, err := latest.Observe(ctx, []driver.LockKey{{Kind: driver.LockReadFact, Fact: fact}})
	if err != nil || observed[0].Revision.String() != "1" {
		t.Fatalf("read fact inert: %v %v", observed, err)
	}
	_ = latest.Rollback(ctx)
	missing := begin(t, ctx, s, lease, nil)
	if err := missing.BumpReadFacts(ctx, []driver.ReadFact{fact}); err == nil {
		t.Fatal("unlocked read-fact bump accepted")
	}
}

func derivedPredicates(t *testing.T, factory Factory) {
	ready, _ := driver.NewReadySentinel("project", "task")
	closure, _ := driver.NewClosureSentinel("pl-scope")
	grant, _ := driver.NewGrantSentinel("scope")
	for _, test := range []struct {
		table    driver.DerivedTable
		sentinel driver.Sentinel
	}{
		{driver.DerivedReadyState, ready}, {driver.DerivedAncestorClosure, closure}, {driver.DerivedCutClosure, closure}, {driver.DerivedAttribute, closure}, {driver.DerivedGrant, grant}, {driver.DerivedOntology, driver.SentinelOntology}, {driver.DerivedPlugins, driver.SentinelPlugins},
	} {
		t.Run(string(test.table), func(t *testing.T) {
			ctx, s, lease, meta := setup(t, factory)
			key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: test.table, Key: "row"}}
			requests := update(key)
			tx := begin(t, ctx, s, lease, requests)
			if err := tx.PutDerived(ctx, test.table, nil, []driver.DerivedRow{{Key: "row", Value: []byte("opaque")}}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(ctx, header(meta, requests)); err == nil {
				t.Fatal("predicate-bearing row committed without sentinel family")
			}
			_ = tx.Rollback(ctx)
			requests = update(key, driver.LockKey{Kind: driver.LockSentinel, Sentinel: test.sentinel})
			tx = begin(t, ctx, s, lease, requests)
			if err := tx.PutDerived(ctx, test.table, nil, []driver.DerivedRow{{Key: "row", Value: []byte("opaque")}}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(ctx, header(meta, requests)); err == nil {
				t.Fatal("update sentinel was not required to bump")
			}
			if err := tx.BumpSentinels(ctx, []driver.Sentinel{test.sentinel}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(ctx, header(meta, requests)); err != nil {
				t.Fatal(err)
			}
			states, err := read(t, ctx, s, meta, nil).Observe(ctx, lockKeys(requests))
			if err != nil {
				t.Fatal(err)
			}
			if len(states) != 2 || states[0].Revision.String() != "1" || states[1].Revision.String() != "1" {
				t.Fatalf("non-atomic derived/predicate: %+v", states)
			}
			unlocked := begin(t, ctx, s, lease, nil)
			if err := unlocked.PutDerived(ctx, test.table, nil, []driver.DerivedRow{{Key: "row"}}); err == nil {
				t.Fatal("unlocked derived write accepted")
			}
			_ = unlocked.Rollback(context.Background())
		})
	}
}

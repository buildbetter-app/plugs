package conformance

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Exercise the same deny-only query contract against each real driver, at
// the current head, in retained history and over a transaction's own writes.
func queryRewrite(t *testing.T, factory Factory) {
	ctx, store, lease, meta := setup(t, factory)
	a, b, c, d := ref("rewrite-a"), ref("rewrite-b"), ref("rewrite-c"), ref("rewrite-d")
	oldBody, newBody := []byte("{ \"value\" : 1 }\n"), []byte("{ \"value\" : 2 }\n")
	first, err := Seed(ctx, store, lease, driver.ChangesetHeader{}, []FixtureChange{
		{Ref: a, Kind: "test", Op: model.OpCreate, Body: oldBody},
		{Ref: b, Kind: "test", Op: model.OpCreate, Body: oldBody},
		{Ref: c, Kind: "test", Op: model.OpCreate, Body: oldBody},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = Seed(ctx, store, lease, driver.ChangesetHeader{}, []FixtureChange{
		{Ref: a, Kind: "test", Op: model.OpPatch, Body: newBody, ChangedPaths: []string{"/value"}},
		{Ref: b, Kind: "test", Op: model.OpDelete},
		{Ref: d, Kind: "test", Op: model.OpCreate, Body: newBody},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertPages := func(t *testing.T, r driver.Reader, filter map[string]any, rewrite *driver.QueryRewrite, expected []string) {
		t.Helper()
		var got []string
		var cursor *driver.Cursor
		for n := 0; ; n++ {
			if n > 4 {
				t.Fatal("query continuation did not terminate")
			}
			page, err := r.Query(ctx, driver.Query{Filter: filter, Rewrite: rewrite, Limit: 1, Cursor: cursor})
			if err != nil || page.AsOf.String() != r.Snapshot().At.String() {
				t.Fatalf("query snapshot/page=%+v err=%v", page, err)
			}
			for _, row := range page.Items {
				got = append(got, row.Ref.ID)
				if !bytes.Equal(row.Body, oldBody) && !bytes.Equal(row.Body, newBody) {
					t.Fatalf("query changed raw body bytes: %q", row.Body)
				}
			}
			cursor = page.NextCursor
			if cursor == nil {
				break
			}
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("query IDs=%v want=%v", got, expected)
		}
	}
	for _, historical := range []bool{false, true} {
		name := "current"
		view := driver.View{Incarnation: meta.Incarnation}
		all := []string{a.ID, c.ID, d.ID}
		selected := []string{a.ID, c.ID, d.ID}
		if historical {
			name = "history"
			view.At = &first.Revision
			all = []string{a.ID, b.ID, c.ID}
			selected = []string{a.ID, c.ID}
		}
		t.Run(name, func(t *testing.T) {
			r, err := store.Read(ctx, view)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			// No filter must continue on both SQL engines, including the
			// PostgreSQL base predicate that already contains WHERE NOT deleted.
			assertPages(t, r, nil, nil, all)
			assertPages(t, r, map[string]any{"kind": "test"}, &driver.QueryRewrite{IDs: []string{d.ID, c.ID, a.ID, c.ID}}, selected)
			assertPages(t, r, nil, &driver.QueryRewrite{}, all)
			assertPages(t, r, nil, &driver.QueryRewrite{IDs: []string{}}, nil)
			assertPages(t, r, map[string]any{"kind": "other"}, &driver.QueryRewrite{IDs: all}, nil)

			firstPage, err := r.Query(ctx, driver.Query{Rewrite: &driver.QueryRewrite{IDs: all}, Limit: 1})
			if err != nil || firstPage.NextCursor == nil {
				t.Fatalf("rewrite cursor not created: page=%+v err=%v", firstPage, err)
			}
			for _, changed := range []*driver.QueryRewrite{nil, {}, {IDs: []string{}}, {IDs: []string{a.ID}}} {
				if _, err := r.Query(ctx, driver.Query{Rewrite: changed, Limit: 1, Cursor: firstPage.NextCursor}); err == nil || !strings.Contains(err.Error(), "cursor binding mismatch") {
					t.Fatalf("cursor accepted changed rewrite %+v: %v", changed, err)
				}
			}
			noNarrowing, err := r.Query(ctx, driver.Query{Rewrite: &driver.QueryRewrite{}, Limit: 1})
			if err != nil || noNarrowing.NextCursor == nil {
				t.Fatalf("nil-ID rewrite cursor not created: page=%+v err=%v", noNarrowing, err)
			}
			if _, err := r.Query(ctx, driver.Query{Rewrite: &driver.QueryRewrite{IDs: []string{}}, Limit: 1, Cursor: noNarrowing.NextCursor}); err == nil || !strings.Contains(err.Error(), "cursor binding mismatch") {
				t.Fatalf("nil and empty rewrite IDs share a cursor binding: %v", err)
			}
		})
	}
	tx := begin(t, ctx, store, lease, update(recordLock(a.ID), recordLock(c.ID), recordLock(d.ID)))
	if err := tx.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, driver.Record{Ref: c, Kind: "test", Body: newBody}); err != nil {
		t.Fatal(err)
	}
	assertPages(t, tx, nil, &driver.QueryRewrite{IDs: []string{a.ID, c.ID}}, []string{c.ID})
	rows, err := tx.Get(ctx, []driver.Ref{d, a, c, c, ref("rewrite-missing")})
	if err != nil || len(rows) != 3 || rows[0].Ref.ID != d.ID || rows[1].Ref.ID != c.ID || rows[2].Ref.ID != c.ID || !bytes.Equal(rows[1].Body, newBody) {
		t.Fatalf("staged batch ordering/duplicates/tombstones/body=%+v err=%v", rows, err)
	}
}

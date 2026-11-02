package postgres

import (
	"bytes"
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestCurrentReadIndexPagesPinnedBodiesAndRebuildsOnOpen(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	s := openTest(t, dsn)
	lease := testLease(t, s)
	for _, id := range []string{"index-a", "index-b", "index-c"} {
		seedTest(t, s, lease, id, `{"number":1e0,"nested":{"z":2,"a":1}}`)
	}
	other, otherHeader := beginTest(t, s, lease, testLock(testRef("index-b-other")))
	if err := other.Put(ctx, driver.Record{Ref: testRef("index-b-other"), Kind: "other", Body: []byte(`{"number":1e0}`)}); err != nil {
		t.Fatal(err)
	}
	otherHeader.Changes = []model.ChangeRecord{{Ref: testRef("index-b-other"), Op: model.OpCreate}}
	if _, err := other.Commit(ctx, otherHeader); err != nil {
		t.Fatal(err)
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pinned := testRead(t, s, nil)
	before, err := pinned.Get(ctx, []model.Ref{testRef("index-b")})
	if err != nil || len(before) != 1 {
		t.Fatalf("missing deletion preimage: %v %v", before, err)
	}
	tx, header := beginTest(t, s, lease, testLock(testRef("index-b")))
	if err := tx.Delete(ctx, testRef("index-b")); err != nil {
		t.Fatal(err)
	}
	header.Changes = []model.ChangeRecord{{Ref: testRef("index-b"), Op: model.OpDelete, FromRevision: before[0].Revision}}
	if _, err := tx.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
	currentMeta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	current := testRead(t, s, nil)
	historical := testRead(t, s, &meta.Head)
	check := func(r driver.Reader, allIDs, testIDs []string) {
		t.Helper()
		for _, filter := range []map[string]any{nil, {"kind": "test"}} {
			ids := allIDs
			if filter != nil {
				ids = testIDs
			}
			var cursor *driver.Cursor
			for i, id := range ids {
				page, err := r.Query(ctx, driver.Query{Filter: filter, Limit: 1, Cursor: cursor})
				if err != nil || len(page.Items) != 1 || page.Items[0].Ref.ID != id || page.AsOf.String() != r.Snapshot().At.String() {
					t.Fatalf("page%d lost order/snapshot: %+v %v", i, page, err)
				}
				point, err := r.Get(ctx, []model.Ref{testRef(id)})
				if err != nil || len(point) != 1 || !bytes.Equal(page.Items[0].Body, point[0].Body) {
					t.Fatalf("page changed raw body: %+v %v", page, err)
				}
				if i == 0 {
					bad := *page.NextCursor
					bad.Index = model.Natural64(uint64(len(ids)))
					end, err := r.Query(ctx, driver.Query{Filter: filter, Limit: 1, Cursor: &bad})
					if err != nil || len(end.Items) != 0 || end.NextCursor != nil {
						t.Fatalf("exact-end cursor did not return empty tail: %+v %v", end, err)
					}
					bad.Index = model.Natural64(99)
					if _, err := r.Query(ctx, driver.Query{Filter: filter, Limit: 1, Cursor: &bad}); err == nil {
						t.Fatal("out-of-range cursor accepted")
					}
				}
				cursor = page.NextCursor
				if (cursor != nil) != (i+1 < len(ids)) {
					t.Fatal("page cursor invented or lost tail")
				}
			}
		}
	}
	// The contract orders encoded keys, including the branch suffix: the '-'
	// in index-b-other precedes the ':' in index-b:0.
	check(pinned, []string{"index-a", "index-b-other", "index-b", "index-c"}, []string{"index-a", "index-b", "index-c"})
	check(historical, []string{"index-a", "index-b-other", "index-b", "index-c"}, []string{"index-a", "index-b", "index-c"})
	check(current, []string{"index-a", "index-b-other", "index-c"}, []string{"index-a", "index-c"})
	for _, r := range []driver.Reader{pinned, historical, current} {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `DROP INDEX plug_postgres.records_live_kind_key`); err != nil {
		t.Fatal(err)
	}
	reopened := openTest(t, dsn)
	if got, err := reopened.Meta(ctx); err != nil || got != currentMeta {
		t.Fatalf("index rebuild changed store metadata: %+v %v", got, err)
	}
	var present bool
	if err := reopened.db.QueryRowContext(ctx, `SELECT to_regclass('plug_postgres.records_live_kind_key') IS NOT NULL`).Scan(&present); err != nil || !present {
		t.Fatalf("read index not rebuilt: %v", err)
	}
	check(testRead(t, reopened, nil), []string{"index-a", "index-b-other", "index-c"}, []string{"index-a", "index-c"})
}

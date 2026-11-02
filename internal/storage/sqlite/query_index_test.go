package sqlite

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestCurrentReadIndexPagesPinnedBodiesAndRebuildsOnOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "query-index.db")
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	for _, id := range []string{"index-a", "index-b", "index-c"} {
		seedRecord(t, ctx, s, lease, id, `{"number":1e0,"nested":{"z":2,"a":1}}`)
	}
	other, otherHeader := beginRecord(t, ctx, s, lease, "index-b-other")
	if err := other.Put(ctx, driver.Record{Ref: testRef("index-b-other"), Kind: "other", Body: []byte(`{"number":1e0}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Commit(ctx, otherHeader); err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, s)
	pinned := getReader(t, ctx, s, meta, nil)
	tx, header := beginRecord(t, ctx, s, lease, "index-b")
	if err := tx.Delete(ctx, testRef("index-b")); err != nil {
		t.Fatal(err)
	}
	header.Changes[0].Op = model.OpDelete
	if _, err := tx.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
	currentMeta := getMeta(t, ctx, s)
	current := getReader(t, ctx, s, currentMeta, nil)
	historical := getReader(t, ctx, s, currentMeta, &meta.Head)
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
	check(pinned, []string{"index-a", "index-b", "index-b-other", "index-c"}, []string{"index-a", "index-b", "index-c"})
	check(historical, []string{"index-a", "index-b", "index-b-other", "index-c"}, []string{"index-a", "index-b", "index-c"})
	check(current, []string{"index-a", "index-b-other", "index-c"}, []string{"index-a", "index-c"})
	for _, r := range []driver.Reader{pinned, historical, current} {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate a supported existing database lacking this additive physical
	// index. Open rebuilds it from the source without changing data or head.
	if _, err := s.db.ExecContext(ctx, `DROP INDEX objects_kind_key`); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, ctx, path)
	if got := getMeta(t, ctx, reopened); got != currentMeta {
		t.Fatalf("index rebuild changed store metadata: %+v", got)
	}
	var present bool
	if err := reopened.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='objects_kind_key')`).Scan(&present); err != nil || !present {
		t.Fatalf("read index not rebuilt: %v", err)
	}
	check(getReader(t, ctx, reopened, currentMeta, nil), []string{"index-a", "index-b-other", "index-c"}, []string{"index-a", "index-c"})
}

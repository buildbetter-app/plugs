package sqlite

import (
	"math"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Pin the existing current-main contract while independently checking the
// cursor regression found in the candidate current-query optimization.
func TestCurrentQueryCursorRejectsOutOfRangeAndPreservesExactEnd(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "query-cursor.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	for _, id := range []string{"cursor-a", "cursor-b", "cursor-c"} {
		seedRecord(t, ctx, s, lease, id, `{}`)
	}
	meta := getMeta(t, ctx, s)
	r := getReader(t, ctx, s, meta, nil)
	filter := map[string]any{"kind": "test"}
	first, err := r.Query(ctx, driver.Query{Filter: filter, Limit: 1})
	if err != nil || len(first.Items) != 1 || first.Items[0].Ref.ID != "cursor-a" || first.NextCursor == nil {
		t.Fatalf("first current page: %+v, %v", first, err)
	}
	for _, tc := range []struct {
		name  string
		index model.Natural
	}{
		{"past_end", model.Natural64(100)},
		{"uint64_max", model.Natural64(math.MaxUint64)},
		{"beyond_uint64", model.Natural("1e1000000000")},
		{"fractional", model.Natural("1.5")},
		{"negative", model.Natural("-1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := *first.NextCursor
			bad.Index = tc.index
			page, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: &bad, Limit: 1})
			if err == nil || len(page.Items) != 0 || page.NextCursor != nil {
				t.Fatalf("invalid current cursor returned a page: %+v, %v", page, err)
			}
		})
	}
	exactEnd := *first.NextCursor
	exactEnd.Index = model.Natural64(3)
	page, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: &exactEnd, Limit: 1})
	if err != nil || len(page.Items) != 0 || page.NextCursor != nil || page.AsOf.String() != first.AsOf.String() {
		t.Fatalf("exact-end cursor lost its empty pinned page: %+v, %v", page, err)
	}
	second, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: first.NextCursor, Limit: 1})
	if err != nil || len(second.Items) != 1 || second.Items[0].Ref.ID != "cursor-b" || second.NextCursor == nil {
		t.Fatalf("invalid cursors changed valid continuation: %+v, %v", second, err)
	}
	last, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: second.NextCursor, Limit: 1})
	if err != nil || len(last.Items) != 1 || last.Items[0].Ref.ID != "cursor-c" || last.NextCursor != nil {
		t.Fatalf("valid final page changed: %+v, %v", last, err)
	}
	if after := getMeta(t, ctx, s); after.Head.String() != meta.Head.String() || after.Incarnation != meta.Incarnation {
		t.Fatal("read/refused cursors changed store state")
	}
}

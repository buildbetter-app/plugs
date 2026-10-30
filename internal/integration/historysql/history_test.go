package historysql_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestHistorySQLPinnedPagesAcrossWritesAndReopen(t *testing.T) {
	for _, b := range []backend{sqliteBackend, postgresBackend} {
		t.Run(b.String(), func(t *testing.T) {
			f := newFixture(t, b)
			want := map[string]bead{}
			refs := make([]model.Ref, 0, 4)
			for _, title := range []string{"first", "second", "third", "fourth"} {
				ref, row := f.create(title, "writer-a")
				refs = append(refs, ref)
				want[ref.ID] = row
			}
			at := asOf{Revision: f.meta().Head, Incarnation: f.meta().Incarnation}
			params := map[string]any{
				"asOf": at, "limit": 1,
				"_meta": map[string]any{"agent": "reader-a"},
			}
			var page beadPage
			f.success("history.query", params, &page)
			if len(page.Items) != 1 || page.NextCursor == "" || !reflect.DeepEqual(page.AsOf, at) {
				t.Fatalf("first history page did not pin asOf: %+v", page)
			}
			firstCursor := page.NextCursor
			seen := append([]bead(nil), page.Items...)
			for _, ref := range refs {
				current := f.get(ref, nil)
				f.update(ref, current.Revision, "later", "writer-b")
			}
			late, _ := f.create("outside snapshot", "writer-b")
			f.reopen()
			for page.NextCursor != "" {
				params["cursor"] = page.NextCursor
				page = beadPage{}
				f.success("history.query", params, &page)
				if len(page.Items) != 1 || !reflect.DeepEqual(page.AsOf, at) {
					t.Fatalf("history page moved snapshot after reopen: %+v", page)
				}
				seen = append(seen, page.Items...)
				if len(seen) > len(want) {
					t.Fatal("history cursor returned too many rows")
				}
			}
			ids := make([]string, 0, len(seen))
			for _, row := range seen {
				expected, ok := want[row.ID]
				if !ok || !reflect.DeepEqual(row, expected) {
					t.Fatalf("history returned a changed, duplicate or late row: %+v", row)
				}
				ids = append(ids, row.ID)
				delete(want, row.ID)
			}
			ordered := append([]string(nil), ids...)
			sort.Strings(ordered)
			if len(want) != 0 || len(ids) != 4 || !reflect.DeepEqual(ids, ordered) {
				t.Fatalf("history rows missing after reopen: ids=%v missing=%v", ids, want)
			}
			f.expectCode("bead.get", map[string]any{"id": late, "asOf": at}, "not_found")
			current := f.meta()
			head, err := current.Head.Uint64()
			if err != nil {
				t.Fatal(err)
			}
			future := model.Natural64(head + 1)
			f.expectCode("history.get", map[string]any{
				"ref":  late,
				"asOf": asOf{Revision: future, Incarnation: current.Incarnation},
			}, "not_found")
			f.expectCode("history.diff", map[string]any{
				"ref":          late,
				"fromRevision": at.Revision,
				"toRevision":   future,
			}, "not_found")

			// The signed cursor binds every caller-visible selector. These calls
			// must fail before a second page can read or mutate the SQL store.
			for _, bad := range map[string]map[string]any{
				"caller":      {"asOf": at, "limit": 1, "cursor": firstCursor, "_meta": map[string]any{"agent": "reader-b"}},
				"project":     {"asOf": at, "limit": 1, "cursor": firstCursor, "_meta": map[string]any{"agent": "reader-a", "project": "other-project"}},
				"filter":      {"asOf": at, "limit": 1, "cursor": firstCursor, "filter": map[string]any{"types": []string{"issue"}}, "_meta": map[string]any{"agent": "reader-a"}},
				"incarnation": {"asOf": asOf{Revision: at.Revision, Incarnation: "different-incarnation"}, "limit": 1, "cursor": firstCursor, "_meta": map[string]any{"agent": "reader-a"}},
				"malformed":   {"asOf": at, "limit": 1, "cursor": "!" + firstCursor, "_meta": map[string]any{"agent": "reader-a"}},
			} {
				f.expectCode("history.query", bad, "cursor_invalid")
			}
			if rows := f.changesets(); len(rows) != 9 {
				t.Fatalf("unexpected durable changeset count: %d", len(rows))
			} else {
				for _, row := range rows {
					if row.Principal.Kind != "anonymous" || row.Principal.ID != "anonymous" {
						t.Fatalf("changeset lost the actual HTTP caller identity: %+v", row)
					}
				}
			}
			t.Logf("%s retained four historical HTTP/RPC rows across writes and reopen; cursor bindings rejected caller, project, filter, incarnation and malformed tokens", b)
		})
	}
}

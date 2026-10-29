package cursorhttp_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestRealHTTPCursorsReadyAndHistoryOnMemoryAndSQLite(t *testing.T) {
	for _, kind := range []backend{memoryBackend, sqliteBackend} {
		t.Run(string(kind), func(t *testing.T) {
			f := newFixture(t, kind)

			// Writes are admitted through real API keys. Alternating the principals
			// makes the cursor binding and durable changeset identity observable.
			refs := make([]map[string]any, 0, 3)
			wantTitles := map[string]string{}
			for i, title := range []string{"alpha", "bravo", "charlie"} {
				token := f.userKey.Token
				if i%2 == 1 {
					token = f.workerKey.Token
				}
				result := success(t, f.call(t, token, "bead.create", writeParams(t, cursorProject, map[string]any{
					"type": "issue", "fields": map[string]any{"title": title},
				})))
				ref := createdRef(t, result)
				refs = append(refs, ref)
				wantTitles[refID(t, ref)] = title
			}
			seed := f.state(t)
			asOf := seed.Meta.Head
			incarnation := seed.Meta.Incarnation
			asOfWire := map[string]any{"revision": asOf, "incarnation": incarnation}

			// History starts at the real current head, then remains pinned while
			// ordinary HTTP writes advance the live store.
			historyParams := readParams(cursorProject, map[string]any{"asOf": asOfWire, "limit": model.Natural64(1)})
			firstHistory := success(t, f.call(t, f.userKey.Token, "history.query", historyParams))
			firstItems := pageItems(t, firstHistory)
			if len(firstItems) != 1 {
				t.Fatalf("history first page length=%d", len(firstItems))
			}
			assertRevision(t, firstHistory, asOf)
			firstHistoryCursor := cursor(t, firstHistory)

			// Interleaved create and patch calls are ordinary production writes;
			// neither may move the already-issued history snapshot.
			lateResult := success(t, f.call(t, f.workerKey.Token, "bead.create", writeParams(t, cursorProject, map[string]any{
				"type": "issue", "fields": map[string]any{"title": "late"},
			})))
			lateID := refID(t, createdRef(t, lateResult))
			updated := writeParams(t, cursorProject, map[string]any{
				"id": refs[0], "fields": map[string]any{"title": "changed-after-snapshot"},
			})
			success(t, f.call(t, f.userKey.Token, "bead.update", updated))

			seen := map[string]string{}
			for _, item := range firstItems {
				id, ok := item["id"].(string)
				if !ok {
					t.Fatal("history item omitted id")
				}
				seen[id] = fmt.Sprint(item["fields"].(map[string]any)["title"])
			}
			cursorValue := firstHistoryCursor
			for pages := 1; ; pages++ {
				if pages > len(wantTitles) {
					t.Fatal("history cursor did not reach a terminal page")
				}
				params := readParams(cursorProject, map[string]any{
					"asOf": asOfWire, "limit": model.Natural64(1), "cursor": cursorValue,
				})
				page := success(t, f.call(t, f.userKey.Token, "history.query", params))
				items := pageItems(t, page)
				if len(items) != 1 {
					t.Fatalf("history page %d length=%d", pages, len(items))
				}
				assertRevision(t, page, asOf)
				item := items[0]
				id, ok := item["id"].(string)
				if !ok || id == lateID {
					t.Fatalf("history page exposed post-snapshot row: %#v", item)
				}
				fields, ok := item["fields"].(map[string]any)
				if !ok || fmt.Sprint(fields["title"]) != wantTitles[id] {
					t.Fatalf("history page lost fixed body for %s: %#v", id, item)
				}
				seen[id] = fmt.Sprint(fields["title"])
				next, hasNext := page["nextCursor"].(string)
				if !hasNext || next == "" {
					break
				}
				cursorValue = next
			}
			if !reflect.DeepEqual(seen, wantTitles) {
				t.Fatalf("history pages did not return exactly the fixed snapshot: seen=%v want=%v", seen, wantTitles)
			}

			// Empty history filters are successful terminal pages, not errors.
			emptyHistory := success(t, f.call(t, f.userKey.Token, "history.query", readParams(cursorProject, map[string]any{
				"asOf": asOfWire, "limit": model.Natural64(1), "filter": map[string]any{"resources": []string{"missing-bead"}},
			})))
			if len(pageItems(t, emptyHistory)) != 0 {
				t.Fatal("empty history filter returned an item")
			}
			assertNoNextCursor(t, emptyHistory)
			assertRevision(t, emptyHistory, asOf)

			// The same real server exposes live ready pages. A post-page write is
			// visible to the live scan, and closing every ready row makes the
			// already-issued offset produce the documented empty terminal page.
			ready := success(t, f.call(t, f.workerKey.Token, "bead.ready", readParams(cursorProject, map[string]any{
				"limit": model.Natural64(2),
			})))
			readyItems := pageItems(t, ready)
			if len(readyItems) != 2 {
				t.Fatalf("ready first page length=%d", len(readyItems))
			}
			readyCursor := cursor(t, ready)
			success(t, f.call(t, f.userKey.Token, "bead.create", writeParams(t, cursorProject, map[string]any{
				"type": "issue", "fields": map[string]any{"title": "ready-interleave"},
			})))
			for _, ref := range refs {
				success(t, f.call(t, f.workerKey.Token, "bead.close", writeParams(t, cursorProject, map[string]any{"id": ref})))
			}
			readyEnd := success(t, f.call(t, f.workerKey.Token, "bead.ready", readParams(cursorProject, map[string]any{
				"limit": model.Natural64(2), "cursor": readyCursor,
			})))
			if len(pageItems(t, readyEnd)) != 0 {
				t.Fatal("ready continuation did not return an empty end page")
			}
			assertNoNextCursor(t, readyEnd)

			// Every refusal below is a canonical RPC error and leaves all durable
			// records and history untouched.
			beforeRefusals := f.state(t)
			errorCode(t, f.call(t, f.userKey.Token, "bead.ready", readParams(cursorProject, map[string]any{
				"limit": model.Natural64(1), "cursor": "!malformed",
			})), "cursor_invalid")
			errorCode(t, f.call(t, f.workerKey.Token, "history.query", readParams(cursorProject, map[string]any{
				"asOf": asOfWire, "limit": model.Natural64(1), "cursor": firstHistoryCursor,
			})), "cursor_invalid")
			wrongIncarnation := map[string]any{"revision": asOf, "incarnation": "foreign-store-incarnation"}
			errorCode(t, f.call(t, f.userKey.Token, "history.query", readParams(cursorProject, map[string]any{
				"asOf": wrongIncarnation, "limit": model.Natural64(1),
			})), "incarnation_changed")
			errorCode(t, f.call(t, f.userKey.Token, "history.query", readParams(cursorProject, map[string]any{
				"asOf": asOfWire, "limit": model.Natural64(1), "cursor": firstHistoryCursor,
				"filter": map[string]any{"types": []string{"issue"}},
			})), "cursor_invalid")
			requireSameState(t, beforeRefusals, f.state(t))
		})
	}
}

func TestCursorHistoryOrderIsCanonical(t *testing.T) {
	// Keep a small deterministic check separate from the two-engine acceptance:
	// the opaque pages are sorted by the service's stable ID order, not write
	// arrival order. This catches accidental fixture-order assertions.
	f := newFixture(t, memoryBackend)
	for _, title := range []string{"zulu", "alpha", "mike"} {
		success(t, f.call(t, f.userKey.Token, "bead.create", writeParams(t, cursorProject, map[string]any{
			"type": "issue", "fields": map[string]any{"title": title},
		})))
	}
	meta := f.state(t).Meta
	result := success(t, f.call(t, f.userKey.Token, "history.query", readParams(cursorProject, map[string]any{
		"asOf": map[string]any{"revision": meta.Head, "incarnation": meta.Incarnation}, "limit": model.Natural64(100),
	})))
	items := pageItems(t, result)
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, fmt.Sprint(item["id"]))
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatalf("history order was not canonical: %v", ids)
	}
}

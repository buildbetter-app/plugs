package extended

import (
	"encoding/json"
	"testing"
)

// The prefix remains stable deliberately: this distinguishes live membership
// from a pinned snapshot without imposing an uncontracted seek/keyset scheme.
func TestB03B24ReadyContinuationIsLiveWhileHistoryStaysPinned(t *testing.T) {
	f := newFixture(t, []string{"pl-a", "pl-b", "pl-c", "pl-d"}, nil)
	for _, title := range []string{"a", "b", "c"} {
		f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}})
	}
	first := f.success(t, "bead.ready", map[string]any{"limit": 1})
	pageIDs(t, first, "pl-a")
	liveCursor := at[string](t, first, "/result/nextCursor")
	if liveCursor == "" {
		t.Fatal("ready omitted a required continuation")
	}
	asOf := map[string]any{"revision": 3, "incarnation": incarnation}
	historical := f.success(t, "history.query", map[string]any{"asOf": asOf, "limit": 1})
	pageIDs(t, historical, "pl-a")
	historyCursor := at[string](t, historical, "/result/nextCursor")
	if historyCursor == "" {
		t.Fatal("history omitted a required continuation")
	}
	f.success(t, "bead.close", map[string]any{"id": wireRef("pl-b")})
	f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "d arrived after page one"}})
	beforeReads := f.image(t)
	live := f.success(t, "bead.ready", map[string]any{"limit": 2, "cursor": liveCursor})
	pageIDs(t, live, "pl-c", "pl-d")
	if at[int](t, live, "/result/asOf/revision") != 5 {
		t.Fatalf("ready continuation retained an old snapshot: %s", live)
	}
	old := f.success(t, "history.query", map[string]any{"asOf": asOf, "limit": 2, "cursor": historyCursor})
	pageIDs(t, old, "pl-b", "pl-c")
	if at[int](t, old, "/result/asOf/revision") != 3 || at[string](t, old, "/result/items/0/status") != "todo" ||
		at[int](t, old, "/result/items/0/revision") != 2 {
		t.Fatalf("history continuation lost its requested historical image: %s", old)
	}
	// Reusing a cursor with a changed filter is a typed refusal, not a live
	// request silently interpreted under the cursor's old scope.
	bad := f.call(t, "changed-filter", "bead.ready", map[string]any{"limit": 2, "cursor": liveCursor,
		"filter": map[string]any{"resources": []string{"pl-d"}}})
	if at[string](t, bad, "/error/data/code") != "cursor_invalid" {
		t.Fatalf("cursor scope drift was accepted: %s", bad)
	}
	f.unchanged(t, beforeReads)
}

func TestB08CloseReplayPreservesResultAndMarksOnlyTheResponse(t *testing.T) {
	f := newFixture(t, []string{"pl-replay", "pl-later"}, nil)
	f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "one close"}})
	params := map[string]any{"id": wireRef("pl-replay"), "reason": "only once"}
	first := f.call(t, "stable-close-key", "bead.close", params)
	if at[int](t, first, "/result/revision") != 2 {
		t.Fatalf("first close: %s", first)
	}
	receipt := f.receipt(t, "stable-close-key", "bead.close")
	if receipt == nil || receipt.ResponsePending || receipt.Commit.Revision.String() != "2" {
		t.Fatalf("first response is not a durable finalized receipt: %+v", receipt)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(first, &envelope); err != nil {
		t.Fatal(err)
	}
	if meta, ok := envelope["_meta"]; ok {
		var flags struct {
			Replayed bool `json:"replayed"`
		}
		if err := json.Unmarshal(meta, &flags); err != nil || flags.Replayed {
			t.Fatalf("first response claims replay: %s", first)
		}
	}
	f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "advance head"}})
	before := f.image(t)
	replay := f.call(t, "stable-close-key", "bead.close", params)
	sameJSON(t, at[json.RawMessage](t, replay, "/result"), at[json.RawMessage](t, first, "/result"))
	if !at[bool](t, replay, "/_meta/replayed") {
		t.Fatalf("true replay lacks its response-only marker: %s", replay)
	}
	f.unchanged(t, before)
	sameJSON(t, f.receipt(t, "stable-close-key", "bead.close"), receipt)
	// Complete stored log/record comparison also proves the marker did not
	// mutate the durable result, comments, history, or later head.
}

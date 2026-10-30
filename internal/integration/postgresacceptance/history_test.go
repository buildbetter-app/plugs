package postgresacceptance

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type bead struct {
	ID       string         `json:"id"`
	Revision model.Revision `json:"revision"`
	Status   string         `json:"status"`
	Fields   map[string]any `json:"fields"`
}

type beadPage struct {
	Items []bead `json:"items"`
	AsOf  struct {
		Revision    model.Revision `json:"revision"`
		Incarnation string         `json:"incarnation"`
	} `json:"asOf"`
	NextCursor string `json:"nextCursor"`
}

func page(t *testing.T, r reply, revision uint64, names ...string) beadPage {
	t.Helper()
	if r.Error != nil || r.replayed() {
		t.Fatalf("expected page: %+v", r)
	}
	out := decode[beadPage](t, r.Result)
	got := make([]string, 0, len(out.Items))
	for _, item := range out.Items {
		got = append(got, item.ID)
	}
	if out.AsOf.Revision.String() != fmt.Sprint(revision) || out.AsOf.Incarnation == "" || !reflect.DeepEqual(got, append([]string{}, names...)) {
		t.Fatalf("page revision/order: %s want revision=%d ids=%v", r.Result, revision, names)
	}
	return out
}

func get(t *testing.T, h *host, id string, asOf *model.Revision) bead {
	t.Helper()
	params := map[string]any{"id": ref(id)}
	if asOf != nil {
		params["asOf"] = map[string]any{"revision": *asOf}
	}
	r := h.call(t, "bead.get", params, "")
	if r.Error != nil {
		t.Fatal(r.Error)
	}
	return decode[struct {
		Bead bead `json:"bead"`
	}](t, r.Result).Bead
}

func TestB01B18LiveReadyAndPinnedHistoryContinuationSurviveReopen(t *testing.T) {
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs("pl-a", "pl-b", "pl-c", "pl-d")
	h := startHost(t, openStore(t, dsn), sequence, trace)
	for i, title := range []string{"a", "b", "c"} {
		create(t, h, "page-"+title, title, uint64(i+1))
	}
	ready := page(t, h.call(t, "bead.ready", map[string]any{"limit": 1}, ""), 3, "pl-a")
	asOf := map[string]any{"revision": model.Natural64(3), "incarnation": ready.AsOf.Incarnation}
	history := page(t, h.call(t, "history.query", map[string]any{"asOf": asOf, "limit": 1}, ""), 3, "pl-a")
	if ready.NextCursor == "" || history.NextCursor == "" || ready.NextCursor == history.NextCursor {
		t.Fatal("real RPC did not issue distinct method-bound continuations")
	}
	committed(t, h.call(t, "bead.close", map[string]any{"id": ref("pl-b")}, key("page-close-b")), 4)
	create(t, h, "page-d", "d", 5)
	before := graph(t, h.store)
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startHost(t, openStore(t, dsn), sequence, trace)
	// Ready resumes its signed offset in the live order [a,c,d]. History
	// resumes the same old snapshot [a,b,c], including b's old status/title.
	live := page(t, h.call(t, "bead.ready", map[string]any{"limit": 2, "cursor": ready.NextCursor}, ""), 5, "pl-c", "pl-d")
	pinned := page(t, h.call(t, "history.query", map[string]any{"asOf": asOf, "limit": 2, "cursor": history.NextCursor}, ""), 3, "pl-b", "pl-c")
	if live.NextCursor != "" || pinned.NextCursor != "" || pinned.Items[0].Status != "todo" || pinned.Items[0].Fields["title"] != "b" || pinned.Items[0].Revision.String() != "2" {
		t.Fatalf("pinned continuation substituted current data: %+v", pinned)
	}
	if got := get(t, h, "pl-b", nil); got.Status != "done" || got.Revision.String() != "4" {
		t.Fatalf("current state did not retain the intervening close: %+v", got)
	}
	for _, params := range []map[string]any{
		{"limit": 2, "cursor": history.NextCursor},
		{"limit": 2, "cursor": ready.NextCursor, "filter": map[string]any{"resources": []string{"pl-a"}}},
		{"limit": 2, "cursor": ready.NextCursor + "x"},
	} {
		refused(t, h.call(t, "bead.ready", params, ""), "cursor_invalid")
	}
	refused(t, h.call(t, "history.query", map[string]any{"asOf": map[string]any{"revision": 5}, "limit": 2, "cursor": history.NextCursor}, ""), "cursor_invalid")
	foreign, err := h.exchange("bead.ready", map[string]any{"limit": 2, "cursor": ready.NextCursor}, "", "another-anonymous-agent")
	if err != nil {
		t.Fatal(err)
	}
	refused(t, foreign, "cursor_invalid")
	unchanged(t, before, graph(t, h.store))
	// A live list can shrink below the old offset. This is an empty live tail,
	// not a historical snapshot or an invalid cursor.
	shortening := page(t, h.call(t, "bead.ready", map[string]any{"limit": 2}, ""), 5, "pl-a", "pl-c")
	if shortening.NextCursor == "" {
		t.Fatal("live list did not expose its remaining tail")
	}
	committed(t, h.call(t, "bead.close", map[string]any{"id": ref("pl-c")}, key("page-close-c")), 6)
	committed(t, h.call(t, "bead.close", map[string]any{"id": ref("pl-d")}, key("page-close-d")), 7)
	tail := page(t, h.call(t, "bead.ready", map[string]any{"limit": 2, "cursor": shortening.NextCursor}, ""), 7)
	if tail.NextCursor != "" {
		t.Fatal("empty live tail generated a non-advancing continuation")
	}
	ledger(t, graph(t, h.store), 7, map[string]int{"pl-a": 1, "pl-b": 2, "pl-c": 2, "pl-d": 2})
	t.Log("B01/B18 bounded: loopback HTTP cursors survive clean SQL reopen; Ready is live, history pins revision 3; method/filter/agent/asOf/tampering refuse and shrinking live tails terminate")
}

func TestB18DiffReplayAcrossStoredBaseBoundaryAndReopen(t *testing.T) {
	dsn, trace, sequence := freshDatabase(t), newTrace(t), newIDs("pl-a")
	h := startHost(t, openStore(t, dsn), sequence, trace)
	create(t, h, "diff-create", "version-1", 1)
	for revision := uint64(2); revision <= 34; revision++ {
		committed(t, h.call(t, "bead.update", map[string]any{"id": ref("pl-a"), "fields": map[string]any{"title": fmt.Sprintf("version-%d", revision)}}, key(fmt.Sprintf("diff-%d", revision))), revision)
	}
	before := graph(t, h.store)
	ledger(t, before, 34, map[string]int{"pl-a": 34})
	for i, version := range before.Versions["pl-a"] {
		wantBase := i == 0 || i == 32
		if version.Revision.String() != fmt.Sprint(i+1) || version.Base != wantBase || version.Deleted || wantBase && len(version.Body) == 0 || !wantBase && len(version.Patch) == 0 {
			t.Fatalf("base/diff boundary version %d: %+v", i+1, version)
		}
	}
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	h = startHost(t, openStore(t, dsn), sequence, trace)
	for _, revision := range []uint64{1, 2, 31, 32, 33, 34} {
		at := model.Natural64(revision)
		got := get(t, h, "pl-a", &at)
		if got.Revision.String() != fmt.Sprint(revision) || got.Fields["title"] != fmt.Sprintf("version-%d", revision) || got.Status != "todo" {
			t.Fatalf("as-of %d substituted the wrong version: %+v", revision, got)
		}
	}
	zero := model.Natural64(0)
	refused(t, h.call(t, "bead.get", map[string]any{"id": ref("pl-a"), "asOf": map[string]any{"revision": zero}}, ""), "not_found")
	unchanged(t, before, graph(t, h.store))
	t.Log("B18 bounded: all 34 real HTTP writes persisted; creation/base at positions 1/33 and forward patches reconstruct exact revisions on both sides after all-pool reopen; pre-creation never substitutes head")
}

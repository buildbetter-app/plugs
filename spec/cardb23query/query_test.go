package cardb20

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/viewops"
)

func TestB23CombinedPublicPredicateCountsPagingAndReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := open(t, engine)
			seedQuery(t, f)
			meta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			at := viewops.AsOf{Revision: meta.Head, Incarnation: meta.Incarnation}
			for _, phase := range []string{"committed", "reopened"} {
				if phase == "reopened" {
					f.reopen()
				}
				for _, c := range queryCases() {
					// Membership vectors above are ID-ordered. Exercise the saved existing
					// lexical ID order at budgets that actually force multiple item pages.
					for _, budget := range []int{1, 1000} {
						d := queryDefinition(c, budget)
						d["sort"] = []string{"id"}
						label := fmt.Sprintf("%s-%s-%d", phase, c.name, budget)
						queryPages(t, f, label, d, &at, c.want)
						queryCount(t, f, label+"-count", d, &at, len(c.want))
					}
				}
				// The two equal titles cross the page boundary; byte-order ID tie-break
				// is fixed by the admitted view contract, which keeps sorting unchanged.
				tie := queryCase{query: "title=Needle AND status=todo AND type=issue"}
				d := queryDefinition(tie, 1)
				queryPages(t, f, phase+"-title-tie", d, &at, []string{queryIDs[4], queryIDs[1], queryIDs[0]})
				for i, c := range []struct{ query, code string }{{"status=todo OR status=done", "invalid_params"}, {"NOT status=todo", "invalid_params"}, {"plane=ephemeral", "unknown_field"}} {
					f.refusal("view.run", fmt.Sprintf("%s-closed-grammar-%d", phase, i), c.code, map[string]any{"countOnly": true, "definition": queryDefinition(queryCase{query: c.query}, 1)})
				}
			}
			// An old content window is selected with current fixed authority. All
			// denied membership must vanish before both page and count construction.
			f.denied = queryIDs[1]
			d := queryDefinition(queryCases()[0], 1)
			d["sort"] = []string{"id"}
			queryPages(t, f, "current-denial-old-content", d, &at, []string{queryIDs[0]})
			queryCount(t, f, "current-denial-old-count", d, &at, 1)
			f.denied = ""
			queryCount(t, f, "current-regrant-old-count", d, &at, 2)
			after, err := f.store.Meta(f.ctx)
			if err != nil || after.Head != meta.Head || after.Incarnation != meta.Incarnation {
				t.Fatal("read-only acceptance mutated store", after, err)
			}
		})
	}
}

func readyCount(t *testing.T, f *fixture, label string, filter map[string]any, want int) {
	t.Helper()
	for _, limit := range []int{1, 1000} {
		meta, err := f.store.Meta(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		raw := f.call("bead.ready", fmt.Sprintf("%s-count-%d", label, limit), map[string]any{"filter": filter, "limit": limit, "countOnly": true})
		var body map[string]json.RawMessage
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := json.Unmarshal(body["count"], &count); err != nil || count != want || string(body["items"]) != "[]" {
			t.Fatalf("ready count=%s want=%d %v", raw, want, err)
		}
		var at viewops.AsOf
		if err := json.Unmarshal(body["asOf"], &at); err != nil || at.Revision != meta.Head || at.Incarnation != meta.Incarnation {
			t.Fatalf("ready snapshot=%s want=%+v: %v", raw, meta, err)
		}
		if _, ok := body["nextCursor"]; ok {
			t.Fatalf("ready count emitted cursor: %s", raw)
		}
	}
}
func TestB23PublicReadyCountDependencyLifecycleAndReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := open(t, engine)
			a := f.mutation("bead.create", "ready-a", map[string]any{"type": "issue", "fields": map[string]any{"title": "blocker"}}).Results[0].Ref
			b := f.mutation("bead.create", "ready-b", map[string]any{"type": "issue", "fields": map[string]any{"title": "dependent"}}).Results[0].Ref
			filter := map[string]any{"resources": []string{a.ID, b.ID}}
			readyCount(t, f, "independent", filter, 2)
			f.mutation("bead.link", "ready-link", map[string]any{"from": b, "to": a, "kind": "blocks"})
			readyCount(t, f, "blocked", filter, 1)
			f.mutation("bead.close", "ready-close-blocker", map[string]any{"id": a})
			readyCount(t, f, "unblocked", filter, 1)
			f.mutation("bead.close", "ready-close-dependent", map[string]any{"id": b})
			readyCount(t, f, "empty", filter, 0)
			f.mutation("bead.reopen", "ready-reopen-dependent", map[string]any{"id": b})
			readyCount(t, f, "reopened-dependent", filter, 1)
			f.reopen()
			readyCount(t, f, "physical-reopen", filter, 1)
			for i, p := range []map[string]any{{"countOnly": true, "cursor": ""}, {"countOnly": true, "cursor": "bad"}, {"countOnly": true, "limit": 1001}, {"countOnly": true, "limit": 0}, {"countOnly": nil}, {"countOnly": "true"}} {
				f.refusal("bead.ready", fmt.Sprintf("ready-refuse-%d", i), "invalid_params", p)
			}
			for i, p := range []map[string]any{{"limit": 1}, {"limit": 1, "countOnly": false}} {
				var body map[string]json.RawMessage
				raw := f.call("bead.ready", fmt.Sprintf("ordinary-ready-%d", i), p)
				if err := json.Unmarshal(raw, &body); err != nil {
					t.Fatal(err)
				}
				if _, ok := body["count"]; ok {
					t.Fatalf("ordinary ready gained count: %s", raw)
				}
			}
		})
	}
}

package cardb20

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
)

// Expectations are literal membership/order vectors, not a second copy of
// viewops' matcher or a sorted derivative of the actual result.
var queryIDs = []string{"b23-query.2", "b23-query.10", "b23-query.3", "b23-query.11", "b23-query.20", "b23-query.1"}

type queryCase struct {
	name, query            string
	resources, types, want []string
}

func queryCases() []queryCase {
	return []queryCase{
		{name: "all-intersection", query: "title=Needle AND label=Alpha AND label=shared AND status=todo AND type=issue", want: []string{queryIDs[1], queryIDs[0]}},
		{name: "any-and-exclude", query: "title=Needle AND (label=Alpha OR label=gamma) AND NOT label=gamma AND status=todo AND type=issue", want: []string{queryIDs[0]}},
		{name: "any-with-status", query: "title=Needle AND (label=Alpha OR label=gamma) AND status=todo AND type=issue", want: []string{queryIDs[1], queryIDs[0], queryIDs[4]}},
		{name: "case-exact-label", query: "title=needle AND label=alpha AND status=todo AND type=issue", want: []string{queryIDs[2]}},
		{name: "closed", query: "title=Needle AND label=Alpha AND status=done AND type=issue", want: []string{queryIDs[3]}},
		{name: "resource-intersection", query: "title=Needle AND label=shared", resources: []string{queryIDs[0], queryIDs[2], queryIDs[5]}, types: []string{"issue"}, want: []string{queryIDs[0]}},
		{name: "unknown-status", query: "status=not-a-status", want: []string{}},
		{name: "unknown-type", query: "type=not-a-type", want: []string{}},
		{name: "unknown-label", query: "label=not-a-label", want: []string{}},
		{name: "unknown-resource", resources: []string{"absent"}, want: []string{}},
		{name: "types-intersect-query", query: "type=issue", types: []string{"absent"}, want: []string{}},
		{name: "empty", want: []string{queryIDs[5], queryIDs[1], queryIDs[3], queryIDs[0], queryIDs[4], queryIDs[2]}},
		{name: "blank", query: " \t ", want: []string{queryIDs[5], queryIDs[1], queryIDs[3], queryIDs[0], queryIDs[4], queryIDs[2]}},
	}
}
func queryDefinition(c queryCase, budget int) map[string]any {
	return map[string]any{"filter": map[string]any{"query": c.query, "resources": c.resources, "types": c.types}, "fields": []string{"id", "type", "status", "labels", "fields.title"}, "sort": []string{"title"}, "budgetTokens": budget}
}
func queryCount(t *testing.T, f *fixture, label string, definition map[string]any, at *viewops.AsOf, want int) {
	t.Helper()
	p := map[string]any{"definition": definition, "countOnly": true}
	if at != nil {
		p["asOf"] = at
	}
	raw := f.call("view.run", label, p)
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := json.Unmarshal(body["count"], &count); err != nil || count != want {
		t.Fatalf("count %s want=%d: %v", raw, want, err)
	}
	if string(body["items"]) != "[]" {
		t.Fatalf("count items: %s", raw)
	}
	for _, field := range []string{"groups", "elisions", "nextCursor"} {
		if _, ok := body[field]; ok {
			t.Fatalf("count emitted %s: %s", field, raw)
		}
	}
	if at != nil {
		var got viewops.AsOf
		if err := json.Unmarshal(body["asOf"], &got); err != nil || got != *at {
			t.Fatalf("count snapshot: %s", raw)
		}
	}
}
func queryPages(t *testing.T, f *fixture, label string, definition map[string]any, at *viewops.AsOf, want []string) {
	t.Helper()
	got := []string{}
	seen := map[string]bool{}
	cursor := ""
	cursors := map[string]bool{}
	var pinned viewops.AsOf
	for page := 0; page <= len(want); page++ {
		p := map[string]any{"definition": definition}
		if cursor != "" {
			p["cursor"] = cursor
		}
		if at != nil {
			p["asOf"] = at
		}
		raw := f.call("view.run", fmt.Sprintf("%s-%d", label, page), p)
		var result struct {
			Items      []struct{ ID string }
			AsOf       viewops.AsOf
			NextCursor string
		}
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		if page == 0 {
			pinned = result.AsOf
		}
		if result.AsOf != pinned || at != nil && result.AsOf != *at {
			t.Fatalf("paging changed snapshot: %s", raw)
		}
		for _, item := range result.Items {
			if seen[item.ID] {
				t.Fatalf("repeated id %s", item.ID)
			}
			seen[item.ID] = true
			got = append(got, item.ID)
		}
		if result.NextCursor == "" {
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("ordered sequence=%v want=%v", got, want)
			}
			return
		}
		if len(result.Items) == 0 || cursors[result.NextCursor] || result.NextCursor == cursor {
			t.Fatalf("nonadvancing cursor: %s", raw)
		}
		cursors[result.NextCursor] = true
		cursor = result.NextCursor
	}
	t.Fatal("continuation did not terminate")
}
func seedQuery(t *testing.T, f *fixture) []model.Ref {
	t.Helper()
	f.ids = append([]string(nil), queryIDs...)
	rows := []struct {
		title  string
		labels []string
	}{
		{"Needle twin", []string{"Alpha", "shared"}},
		{"Needle twin", []string{"Alpha", "gamma", "shared"}},
		{"needle lower", []string{"alpha", "shared"}},
		{"Needle closed", []string{"Alpha", "shared"}},
		{"Needle other", []string{"gamma"}},
		{"ordinary", []string{"Alpha", "shared"}},
	}
	refs := []model.Ref{}
	for i, row := range rows {
		c := f.mutation("bead.create", fmt.Sprintf("seed-%d", i), map[string]any{"type": "issue", "fields": map[string]any{"title": row.title, "labels": row.labels}})
		if c.Results[0].Ref.ID != queryIDs[i] {
			t.Fatal("fixture identity drift")
		}
		refs = append(refs, c.Results[0].Ref)
		var read struct {
			ID     string
			Fields map[string]any
			Labels []string
		}
		raw := f.call("bead.get", fmt.Sprintf("seed-read-%d", i), map[string]any{"id": c.Results[0].Ref})
		if err := json.Unmarshal(raw, &read); err != nil || read.ID != queryIDs[i] || read.Fields["title"] != row.title || !reflect.DeepEqual(read.Labels, row.labels) {
			t.Fatalf("committed readback=%s want=%+v %v", raw, row, err)
		}
	}
	closed := f.mutation("bead.close", "seed-close", map[string]any{"id": refs[3]})
	replay := f.mutation("bead.close", "seed-close", map[string]any{"id": refs[3]})
	if !reflect.DeepEqual(closed, replay) {
		t.Fatal("winning public request/result replay changed")
	}
	return refs
}
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
				tie := queryCase{query: "title=Needle AND status=todo"}
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
		raw := f.call("bead.ready", fmt.Sprintf("%s-count-%d", label, limit), map[string]any{"filter": filter, "limit": limit, "countOnly": true})
		var body map[string]json.RawMessage
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := json.Unmarshal(body["count"], &count); err != nil || count != want || string(body["items"]) != "[]" {
			t.Fatalf("ready count=%s want=%d %v", raw, want, err)
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

package s1b21b24

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const (
	revisionNine   = "b23-order-z-nine"
	revisionTenZ   = "b23-order-z-ten"
	revisionTenA   = "b23-order-a-ten"
	revisionEleven = "b23-order-a-eleven"
)

var expectedRevisions = map[string]string{
	revisionNine: "9", revisionTenZ: "10", revisionTenA: "10", revisionEleven: "11",
}

type revisionPage struct {
	Items []struct {
		ID       string         `json:"id"`
		Revision model.Revision `json:"revision"`
	} `json:"items"`
	Count      *int   `json:"count"`
	NextCursor string `json:"nextCursor"`
	AsOf       struct {
		Revision    model.Revision `json:"revision"`
		Incarnation string         `json:"incarnation"`
	} `json:"asOf"`
}

func seedRevisionOrder(t *testing.T, f *fixture) {
	t.Helper()
	if f.idIndex != 0 {
		t.Fatal("revision fixture already allocated an ID")
	}
	f.ids = []string{revisionNine, revisionTenZ, revisionTenA, revisionEleven}
	batch := func(label, want string, ops ...any) model.Commit {
		c := commit(t, f.call("tx.apply", label, map[string]any{"changeSet": map[string]any{"ops": ops}}))
		if c.Revision.String() != want {
			t.Fatalf("%s committed revision=%s want=%s", label, c.Revision.String(), want)
		}
		return c
	}
	create := func(key, title string) map[string]any {
		return map[string]any{"kind": "create", "key": key, "type": "issue", "fields": map[string]any{"title": title}}
	}
	created := batch("revision-create", "1", create("nine", "same"), create("ten-z", "alpha"), create("ten-a", "zeta"), create("eleven", "same"))
	if len(created.Results) != len(f.ids) {
		t.Fatalf("revision fixture create results=%+v", created.Results)
	}
	for i, result := range created.Results {
		if result.Ref.ID != f.ids[i] {
			t.Fatalf("created ID[%d]=%s want=%s", i, result.Ref.ID, f.ids[i])
		}
	}
	patch := func(id, description string) map[string]any {
		return map[string]any{"kind": "patch", "target": model.RefByID(id, model.Natural64(0)), "fields": map[string]any{"description": description}}
	}
	for revision := 2; revision <= 9; revision++ {
		label := fmt.Sprintf("revision-write-%d", revision)
		batch(label, fmt.Sprint(revision), patch(revisionNine, label))
	}
	// These two records receive the same real committed revision, with IDs and
	// titles deliberately disagreeing about their order.
	batch("revision-write-10", "10", patch(revisionTenZ, "ten"), patch(revisionTenA, "ten"))
	batch("revision-write-11", "11", patch(revisionEleven, "eleven"))
}

func revisionDefinition(sortKeys []string, budget int) map[string]any {
	return map[string]any{
		"filter":       map[string]any{"query": "type=issue"},
		"fields":       []string{"id", "revision"},
		"sort":         sortKeys,
		"budgetTokens": budget,
	}
}

func readRevisionPage(t *testing.T, f *fixture, label string, definition map[string]any, cursor string, countOnly bool, incarnation string) revisionPage {
	t.Helper()
	params := map[string]any{"definition": definition}
	if cursor != "" {
		params["cursor"] = cursor
	}
	if countOnly {
		params["countOnly"] = true
	}
	got := f.call("view.run", label, params)
	if got.Error != nil || got.Meta.Replayed {
		t.Fatalf("%s view refusal/replay: %+v", label, got)
	}
	var page revisionPage
	if err := json.Unmarshal(got.Result, &page); err != nil {
		t.Fatal(err)
	}
	if page.AsOf.Revision.String() != "11" || page.AsOf.Incarnation != incarnation {
		t.Fatalf("%s lost pinned revision/incarnation: %+v", label, page.AsOf)
	}
	if f.casReceipt(t, "view.run", label) != nil {
		t.Fatalf("%s read-only view stored an idempotency receipt", label)
	}
	return page
}

func requireRevisionItems(t *testing.T, page revisionPage, want []string) {
	t.Helper()
	if page.Count != nil {
		t.Fatalf("ordinary revision page emitted count=%d", *page.Count)
	}
	ids := make([]string, len(page.Items))
	for i, item := range page.Items {
		ids[i] = item.ID
		if wantRevision, ok := expectedRevisions[item.ID]; !ok || item.Revision.String() != wantRevision {
			t.Fatalf("item %s revision=%s want=%s", item.ID, item.Revision.String(), wantRevision)
		}
	}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("revision page IDs=%v want=%v", ids, want)
	}
}

func assertRevisionPages(t *testing.T, f *fixture, phase, incarnation string) {
	t.Helper()
	for _, tc := range []struct {
		name string
		sort []string
		want []string
	}{
		// These expectations are literal sequences, never computed by the
		// production comparator or copied from the unpaged response.
		{"ascending", []string{"revision"}, []string{revisionNine, revisionTenA, revisionTenZ, revisionEleven}},
		{"descending", []string{"-revision"}, []string{revisionEleven, revisionTenA, revisionTenZ, revisionNine}},
		{"revision_then_title", []string{"revision", "title"}, []string{revisionNine, revisionTenZ, revisionTenA, revisionEleven}},
		{"title_then_revision", []string{"title", "-revision"}, []string{revisionTenZ, revisionEleven, revisionNine, revisionTenA}},
	} {
		label := "revision-" + phase + "-" + tc.name
		unpaged := readRevisionPage(t, f, label+"-unpaged", revisionDefinition(tc.sort, 100), "", false, incarnation)
		requireRevisionItems(t, unpaged, tc.want)
		if unpaged.NextCursor != "" {
			t.Fatalf("%s unpaged response has a cursor", label)
		}
		definition := revisionDefinition(tc.sort, 2)
		count := readRevisionPage(t, f, label+"-count", definition, "", true, incarnation)
		if count.Count == nil || *count.Count != 4 || count.Items == nil || len(count.Items) != 0 || count.NextCursor != "" {
			t.Fatalf("%s countOnly must return exactly four without an item page: %+v", label, count)
		}
		var joined []string
		seen := make(map[string]bool)
		cursor := ""
		for start := 0; start < len(tc.want); start += 2 {
			page := readRevisionPage(t, f, fmt.Sprintf("%s-page-%d", label, start/2), definition, cursor, false, incarnation)
			end := min(start+2, len(tc.want))
			requireRevisionItems(t, page, tc.want[start:end])
			for _, item := range page.Items {
				if seen[item.ID] {
					t.Fatalf("%s repeated item %s", label, item.ID)
				}
				seen[item.ID] = true
				joined = append(joined, item.ID)
			}
			if (page.NextCursor != "") != (end < len(tc.want)) {
				t.Fatalf("%s wrong continuation after item %d", label, end)
			}
			cursor = page.NextCursor
		}
		if !reflect.DeepEqual(joined, tc.want) || len(seen) != 4 {
			t.Fatalf("%s concatenated pages=%v want=%v", label, joined, tc.want)
		}
	}
	t.Logf("B23 revision pages engine=%s phase=%s revisions=9,10,10,11 exact_count=4", f.engine, phase)
}

func TestS1B23RevisionOrderingPagesAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seedRevisionOrder(t, f)
			before := f.state(t)
			if before.Meta.Head.String() != "11" || len(before.Records) != 4 {
				t.Fatalf("revision fixture persisted unexpected state: %+v", before)
			}
			for _, row := range before.Records {
				if want, ok := expectedRevisions[row.Ref.ID]; !ok || row.Revision.String() != want {
					t.Fatalf("persisted row %s revision=%s want=%s", row.Ref.ID, row.Revision.String(), want)
				}
			}
			assertRevisionPages(t, f, "initial", before.Meta.Incarnation)
			sameState(t, before, f.state(t))
			if engine == "memory" {
				return
			}
			definition := revisionDefinition([]string{"revision"}, 2)
			first := readRevisionPage(t, f, "revision-before-reopen", definition, "", false, before.Meta.Incarnation)
			requireRevisionItems(t, first, []string{revisionNine, revisionTenA})
			if first.NextCursor == "" {
				t.Fatal("missing cursor for SQL reopen continuation")
			}
			// Reopen both SQL drivers through the existing real-store harness;
			// fixture.reopen intentionally covers only older SQLite scenarios.
			f.close(t)
			f.open(t)
			sameState(t, before, f.state(t))
			continued := readRevisionPage(t, f, "revision-after-reopen", definition, first.NextCursor, false, before.Meta.Incarnation)
			requireRevisionItems(t, continued, []string{revisionTenZ, revisionEleven})
			if continued.NextCursor != "" {
				t.Fatal("SQL reopen continuation did not finish the page sequence")
			}
			assertRevisionPages(t, f, "reopened", before.Meta.Incarnation)
			sameState(t, before, f.state(t))
		})
	}
}

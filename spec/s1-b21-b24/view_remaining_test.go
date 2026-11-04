package s1b21b24

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func viewDefinition(query string) map[string]any {
	return map[string]any{
		"filter":       map[string]any{"query": query},
		"fields":       []string{"id", "type", "status", "labels"},
		"sort":         []string{"id"},
		"budgetTokens": 100,
	}
}

func createViewBead(t *testing.T, f *fixture, label, typ string, labels ...string) model.Ref {
	t.Helper()
	fields := map[string]any{"title": label}
	if len(labels) != 0 {
		fields["labels"] = labels
	}
	got := f.call("bead.create", label, map[string]any{"type": typ, "fields": fields})
	c := commit(t, got)
	if len(c.Results) != 1 {
		t.Fatalf("create %s result=%+v", label, c)
	}
	return c.Results[0].Ref
}

func viewResultIDs(t *testing.T, got reply) []string {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("view.run: %+v", got.Error)
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(got.Result, &page); err != nil {
		t.Fatalf("view result %s: %v", got.Result, err)
	}
	ids := make([]string, 0, len(page.Items))
	for _, item := range page.Items {
		ids = append(ids, item.ID)
	}
	return ids
}

func viewCount(t *testing.T, got reply) int {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("view count: %+v", got.Error)
	}
	var result struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(got.Result, &result); err != nil {
		t.Fatalf("view count result %s: %v", got.Result, err)
	}
	return result.Count
}

func assertViewListCount(t *testing.T, f *fixture, query string, want ...string) {
	t.Helper()
	got := viewResultIDs(t, f.call("view.run", "b23-list-"+query, map[string]any{"definition": viewDefinition(query)}))
	// Keep an explicit empty expected list non-nil so an unknown-type query
	// proves a real empty result rather than failing on slice representation.
	want = append([]string{}, want...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("query %q list=%v want=%v", query, got, want)
	}
	count := viewCount(t, f.call("view.run", "b23-count-"+query, map[string]any{
		"countOnly": true, "definition": viewDefinition(query),
	}))
	if count != len(want) {
		t.Fatalf("query %q count=%d list=%d", query, count, len(want))
	}
}

// This is an ordinary Writer/public view exercise. It deliberately uses all
// three configured stores: seeded driver rows cannot establish the public
// label grammar, workspace vocabulary, or the ready blocker projection.
func TestS1B23WriterLabelsVocabularyListCountAndReadyAcrossStores(t *testing.T) {
	for _, engine := range append(engines(), "postgres") {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			upper := createViewBead(t, f, "b23-upper", "issue", "Alpha", "beta")
			lower := createViewBead(t, f, "b23-lower", "issue", "alpha", "beta")
			gamma := createViewBead(t, f, "b23-gamma", "issue", "gamma")
			plain := createViewBead(t, f, "b23-plain", "issue")
			// The no-plugin ontology advertises only the configured issue type.
			// Keep the unsupported vocabulary boundary explicit instead of
			// fabricating a bug row that the public Writer must refuse.
			unknown := f.call("bead.create", "b23-unknown-type", map[string]any{
				"type": "bug", "fields": map[string]any{"title": "b23-unknown"},
			})
			if unknown.Error == nil || unknown.Error.Data.Code != "invalid_params" {
				t.Fatalf("unknown type was not refused: %+v", unknown)
			}
			closed := createViewBead(t, f, "b23-closed", "issue", "Alpha")
			if got := f.call("bead.close", "b23-close", map[string]any{"id": closed}); got.Error != nil {
				t.Fatalf("close: %+v", got.Error)
			}

			assertViewListCount(t, f, "label=Alpha AND label=beta", upper.ID)
			assertViewListCount(t, f, "label=Alpha OR label=gamma", upper.ID, gamma.ID, closed.ID)
			assertViewListCount(t, f, "NOT label=gamma", upper.ID, lower.ID, plain.ID, closed.ID)
			// Label matching is exact and case-sensitive: lower-case alpha does
			// not match the separately persisted Alpha vocabulary member.
			assertViewListCount(t, f, "label=alpha", lower.ID)
			assertViewListCount(t, f, "type=bug")
			assertViewListCount(t, f, "type=issue AND status=todo", upper.ID, lower.ID, gamma.ID, plain.ID)
			assertViewListCount(t, f, "status=done", closed.ID)

			blocker := createViewBead(t, f, "b23-blocker", "issue")
			dependent := createViewBead(t, f, "b23-dependent", "issue")
			if got := f.call("bead.link", "b23-block", map[string]any{"from": dependent, "to": blocker, "kind": "blocks"}); got.Error != nil {
				t.Fatalf("link blocker: %+v", got.Error)
			}
			ready := viewResultIDs(t, f.call("bead.ready", "b23-ready-blocked", map[string]any{
				"limit": 100, "filter": map[string]any{"resources": []string{blocker.ID, dependent.ID}},
			}))
			if !reflect.DeepEqual(ready, []string{blocker.ID}) {
				t.Fatalf("blocked ready=%v want=[%s]", ready, blocker.ID)
			}
			if got := f.call("bead.close", "b23-close-blocker", map[string]any{"id": blocker}); got.Error != nil {
				t.Fatalf("close blocker: %+v", got.Error)
			}
			ready = viewResultIDs(t, f.call("bead.ready", "b23-ready-unblocked", map[string]any{
				"limit": 100, "filter": map[string]any{"resources": []string{blocker.ID, dependent.ID}},
			}))
			if !reflect.DeepEqual(ready, []string{dependent.ID}) {
				t.Fatalf("unblocked ready=%v want=[%s]", ready, dependent.ID)
			}
		})
	}
}

func TestS1B23CurrentAuthorizationChangesListAndCount(t *testing.T) {
	for _, engine := range append(engines(), "postgres") {
		t.Run(engine, func(t *testing.T) {
			denied := map[string]bool{}
			f := newFixtureWithConfig(t, engine, nil, func(_ context.Context, _ model.PrincipalRef, ref model.ResourceRef, _ string) error {
				if denied[ref.Ref.ID] {
					return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
				}
				return nil
			})
			first := createViewBead(t, f, "b23-auth-first", "issue", "auth")
			second := createViewBead(t, f, "b23-auth-second", "issue", "auth")
			before := f.state(t)
			assertViewListCount(t, f, "label=auth", first.ID, second.ID)
			denied[second.ID] = true
			assertViewListCount(t, f, "label=auth", first.ID)
			if after := f.state(t); !reflect.DeepEqual(before, after) {
				t.Fatalf("authorization-only list/count changed durable state")
			}
		})
	}
}

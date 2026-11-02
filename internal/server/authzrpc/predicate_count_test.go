package authzrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// This fixture reuses the real fixed-permission composition, HTTP admission,
// and owned PostgreSQL engine. Only disposable source rows are seeded through
// conformance; count assertions call the schema-checked public JSON-RPC wire.
func ordinaryCountFixture(t *testing.T, open storeFactory) *fixture {
	t.Helper()
	f := newFixtureOn(t, open(t), nil)
	f.base.Projects = append(f.base.Projects, authzfixed.Project{Ref: model.RefByID("other", model.Natural64(0)), Name: "other", InstallationID: "fixed", Active: true})
	if err := f.facts.Replace(f.base); err != nil {
		t.Fatal(err)
	}
	var changes []conformance.FixtureChange
	for _, row := range []struct {
		n                      int
		title, status, project string
	}{
		{2, "needle-alpha", "todo", "project"},
		{3, "needle-zeta", "todo", "project"},
		{4, "needle-secret", "todo", "project"},
		{5, "needle-done", "done", "project"},
		{6, "needle-other", "todo", "other"},
		{7, "Needle-case", "todo", "project"},
		{8, "needle-parent-secret", "todo", "project"},
	} {
		body := map[string]any{"id": fixtureRef(row.n).ID, "type": "issue", "project": row.project, "fields": map[string]any{"title": row.title, "status": row.status}}
		if row.n == 2 {
			body["parent"] = fixtureRef(8)
			body["links"] = []map[string]any{{"to": fixtureRef(4), "kind": "blocks"}}
		}
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: fixtureRef(row.n), Kind: "bead", Op: model.OpCreate, Body: raw})
	}
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	parents, err := viewrpc.NewParentProjectionConfig(f.server.Checker)
	if err != nil {
		t.Fatal(err)
	}
	views, err := viewops.New(f.store, f.lease, viewops.Config{
		OntologyRevision: model.Natural64(1), ParentProjection: parents,
		AuthorizeReadBatch: f.server.Service.AuthorizeReadBatch,
		Views:              ordinarySavedView{},
		AuthorizeRead: func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
			_, err := f.server.Service.Get(ctx, by, service.GetRequest{ID: resource.Ref})
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.server.Service); err != nil {
		t.Fatal(err)
	}
	if err := viewrpc.BindWithConfig(registry, views, viewrpc.Config{CursorKey: []byte("b23-disposable-count-cursor-key-0123456789")}); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, AuthMode: "authenticated", DefaultProject: "project", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{Provider: f.keys, AuthorizationInstalled: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	f.server.Handler, err = httpjson.New(server, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

type ordinarySavedView struct{}

func (ordinarySavedView) GetView(_ context.Context, _ model.PrincipalRef, id string) (viewops.Definition, error) {
	if id != "ordinary-count" {
		return viewops.Definition{}, &model.Error{Data: model.ErrorData{Code: "not_found"}}
	}
	budget := model.Natural64(1)
	return viewops.Definition{Filter: &viewops.Filter{Query: "title=needle AND status=todo"}, Sort: []string{"title"}, BudgetTokens: &budget}, nil
}

func countDrivers(t *testing.T, run func(*testing.T, storeFactory)) {
	t.Helper()
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var open storeFactory
			switch engine {
			case "memory":
				open = func(*testing.T) driver.Store { return memory.New() }
			case "sqlite":
				open = func(t *testing.T) driver.Store {
					store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: t.TempDir() + "/predicate-count.db"})
					if err != nil {
						t.Fatal(err)
					}
					return store
				}
			case "postgres":
				open = postgresFactory(t)
			}
			run(t, open)
		})
	}
}

func countReader() model.PrincipalRef {
	return model.PrincipalRef{Kind: "user", ID: "user:count-reader", Caveats: map[string]any{"refs": []model.Ref{fixtureRef(1), fixtureRef(2), fixtureRef(3), fixtureRef(5), fixtureRef(6), fixtureRef(7)}}}
}

type ordinaryCountPage struct {
	Items []struct {
		ID             string          `json:"id"`
		Parent         json.RawMessage `json:"parent"`
		HiddenBlockers int             `json:"hiddenBlockers"`
	} `json:"items"`
	Count      *int            `json:"count"`
	NextCursor string          `json:"nextCursor"`
	AsOf       map[string]any  `json:"asOf"`
	Groups     json.RawMessage `json:"groups"`
}

func ordinaryPage(t *testing.T, got reply) ordinaryCountPage {
	t.Helper()
	success(t, got)
	var page ordinaryCountPage
	if err := json.Unmarshal(got.Result, &page); err != nil {
		t.Fatal(err)
	}
	if page.Items == nil || page.AsOf["revision"] == nil || page.AsOf["incarnation"] == nil || len(page.Groups) != 0 {
		t.Fatalf("ordinary wire shape = %s", got.Result)
	}
	for _, hidden := range []string{fixtureRef(4).ID, fixtureRef(8).ID, "needle-secret", "needle-parent-secret"} {
		if strings.Contains(string(got.Result), hidden) {
			t.Fatalf("hidden source escaped ordinary view: %s", got.Result)
		}
	}
	t.Logf("ordinary public wire: %s", got.Result)
	return page
}

func TestB23OrdinaryPredicateFoundationAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		for _, tc := range []struct {
			name, query string
			resources   []string
			types, sort []string
			want        []int
		}{
			{name: "empty_query", want: []int{1, 2, 3, 5, 7}},
			{name: "blank_query", query: " \t\n ", want: []int{1, 2, 3, 5, 7}},
			{name: "composed_title_status_type", query: "title=needle AND status=todo and type=issue", sort: []string{"title"}, want: []int{2, 3}},
			{name: "reverse_title_sort", query: "title=needle AND status=todo", sort: []string{"-title"}, want: []int{3, 2}},
			{name: "title_collation", query: "title=Needle", want: []int{7}},
			{name: "unknown_status", query: "status=absent"},
			{name: "unknown_type", query: "type=absent"},
			{name: "no_match", query: "title=absent"},
			{name: "duplicate_resources_intersection", query: "title=needle AND status=todo", resources: []string{fixtureRef(2).ID, fixtureRef(2).ID, fixtureRef(3).ID, fixtureRef(4).ID}, types: []string{"issue"}, want: []int{2, 3}},
			{name: "hidden_resource", resources: []string{fixtureRef(4).ID}},
			{name: "project_scope", query: "title=needle", resources: []string{fixtureRef(6).ID}},
			{name: "type_intersection", query: "type=issue", types: []string{"absent"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				filter := map[string]any{"query": tc.query}
				if tc.resources != nil {
					filter["resources"] = tc.resources
				}
				if tc.types != nil {
					filter["types"] = tc.types
				}
				definition := map[string]any{"filter": filter, "budgetTokens": 1}
				if tc.sort != nil {
					definition["sort"] = tc.sort
				}
				params := map[string]any{"definition": definition}
				var ids []string
				seen := map[string]bool{}
				for i := 0; ; i++ {
					if i > 10 {
						t.Fatal("ordinary pagination did not terminate")
					}
					page := ordinaryPage(t, f.call(t, countReader(), "view.run", params, 12000+i))
					if len(page.Items) > 1 {
						t.Fatal("output budget was ignored")
					}
					for _, item := range page.Items {
						if seen[item.ID] {
							t.Fatal("ordinary page repeated a row")
						}
						seen[item.ID] = true
						ids = append(ids, item.ID)
						if item.ID == fixtureRef(2).ID && (string(item.Parent) != `{"exists":true}` || item.HiddenBlockers != 1) {
							t.Fatalf("parent/blocker redaction = %+v", item)
						}
					}
					if page.NextCursor == "" {
						break
					}
					params["cursor"] = page.NextCursor
				}
				var want []string
				for _, n := range tc.want {
					want = append(want, fixtureRef(n).ID)
				}
				if !reflect.DeepEqual(ids, want) {
					t.Fatalf("predicate members=%v want=%v", ids, want)
				}
			})
		}
		for i, query := range []string{"AND", "type=", "type=issue AND", "type=issue OR status=todo", "type=issue && status=todo", "title", "unknown=value"} {
			t.Run(fmt.Sprintf("malformed_%d", i), func(t *testing.T) {
				got := f.call(t, countReader(), "view.run", map[string]any{"definition": map[string]any{"filter": map[string]any{"query": query}}}, 13000+i)
				want := model.ErrorCode("invalid_params")
				if query == "unknown=value" {
					want = "unknown_field"
				}
				if got.Error == nil || got.Error.Data.Code != want || len(got.Result) != 0 {
					t.Fatalf("malformed predicate response=%+v", got)
				}
			})
		}
	})
}

func TestB23OrdinaryCurrentSnapshotAndCurrentVisibilityAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		definition := map[string]any{"filter": map[string]any{"query": "status=todo AND title=needle"}, "sort": []string{"title"}}
		first := ordinaryPage(t, f.call(t, countReader(), "view.run", map[string]any{"definition": definition}, 14000))
		if len(first.Items) != 2 || first.Items[0].ID != fixtureRef(2).ID || first.Items[1].ID != fixtureRef(3).ID {
			t.Fatalf("initial full predicate=%+v", first)
		}
		// A real public Writer update changes the next unpinned request. It
		// cannot change membership in the explicitly selected old content.
		success(t, f.call(t, worker("user:operator"), "bead.update", map[string]any{"id": fixtureRef(3), "fields": map[string]any{"title": "changed"}}, 14001))
		fresh := ordinaryPage(t, f.call(t, countReader(), "view.run", map[string]any{"definition": definition}, 14002))
		if len(fresh.Items) != 1 || fresh.Items[0].ID != fixtureRef(2).ID || reflect.DeepEqual(fresh.AsOf, first.AsOf) {
			t.Fatalf("fresh request reused old membership: first=%+v fresh=%+v", first, fresh)
		}
		old := ordinaryPage(t, f.call(t, countReader(), "view.run", map[string]any{"definition": definition, "asOf": first.AsOf}, 14003))
		if len(old.Items) != 2 || !reflect.DeepEqual(old.AsOf, first.AsOf) {
			t.Fatalf("historical membership drifted: first=%+v old=%+v", first, old)
		}
		// Explicit old content never restores a broader caller caveat. The
		// installed finite read policy applies to every candidate again.
		narrow := countReader()
		narrow.Caveats = map[string]any{"refs": []model.Ref{fixtureRef(2)}}
		restricted := ordinaryPage(t, f.call(t, narrow, "view.run", map[string]any{"definition": definition, "asOf": first.AsOf}, 14004))
		if len(restricted.Items) != 1 || restricted.Items[0].ID != fixtureRef(2).ID {
			t.Fatalf("historical content bypassed current caveats: %+v", restricted)
		}
		for i, by := range []model.PrincipalRef{
			{Kind: "user", ID: "user:empty-count", Caveats: map[string]any{"refs": []model.Ref{}}},
			worker("user:operator"),
		} {
			denied := ordinaryPage(t, f.call(t, by, "view.run", map[string]any{"definition": definition}, 14005+i))
			if len(denied.Items) != 0 {
				t.Fatalf("non-readable candidates escaped: %+v", denied)
			}
		}
	})
}

func TestB23OrdinarySavedViewPredicateAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		params := map[string]any{"viewId": "ordinary-count"}
		first := ordinaryPage(t, f.call(t, countReader(), "view.run", params, 15000))
		if len(first.Items) != 1 || first.Items[0].ID != fixtureRef(2).ID || first.NextCursor == "" {
			t.Fatalf("saved predicate first page=%+v", first)
		}
		params["cursor"] = first.NextCursor
		second := ordinaryPage(t, f.call(t, countReader(), "view.run", params, 15001))
		if len(second.Items) != 1 || second.Items[0].ID != fixtureRef(3).ID || second.NextCursor != "" {
			t.Fatalf("saved predicate second page=%+v", second)
		}
		// A narrower current caller cannot reuse the broader cursor binding.
		narrow := countReader()
		narrow.Caveats = map[string]any{"refs": []model.Ref{fixtureRef(2)}}
		got := f.call(t, narrow, "view.run", params, 15002)
		if got.Error == nil || got.Error.Data.Code != "cursor_invalid" || len(got.Result) != 0 {
			t.Fatalf("cursor accepted a changed caller: %+v", got)
		}
	})
}

func exactCountPage(t *testing.T, got reply, want int) ordinaryCountPage {
	t.Helper()
	page := ordinaryPage(t, got)
	if page.Count == nil || *page.Count != want || len(page.Items) != 0 || page.NextCursor != "" {
		t.Fatalf("exact ordinary count=%s want count=%d and no item page", got.Result, want)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(got.Result, &members); err != nil {
		t.Fatal(err)
	}
	for name := range members {
		if name != "items" && name != "asOf" && name != "count" && name != "projection" {
			t.Fatalf("count-only returned payload member %q: %s", name, got.Result)
		}
	}
	return page
}

func TestB23OrdinaryExactPredicateCountWireAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		for i, tc := range []struct {
			name       string
			definition map[string]any
			want       int
		}{
			{"empty_query_default_fields", map[string]any{"filter": map[string]any{}}, 5},
			{"blank_query", map[string]any{"filter": map[string]any{"query": " \t\n "}}, 5},
			{"full_predicate_before_page_budget", map[string]any{"filter": map[string]any{"query": "title=needle AND status=todo and type=issue"}, "budgetTokens": 1}, 2},
			{"zero_is_present", map[string]any{"filter": map[string]any{"query": "title=absent"}}, 0},
			{"unknown_status", map[string]any{"filter": map[string]any{"query": "status=absent"}}, 0},
			{"unknown_type", map[string]any{"filter": map[string]any{"query": "type=absent"}}, 0},
			{"case_sensitive_title", map[string]any{"filter": map[string]any{"query": "title=Needle"}}, 1},
			{"unique_resource_type_query_intersection", map[string]any{"filter": map[string]any{"query": "title=needle AND status=todo", "resources": []string{fixtureRef(2).ID, fixtureRef(2).ID, fixtureRef(3).ID, fixtureRef(4).ID}, "types": []string{"issue"}}}, 2},
			{"hidden_resource_zero", map[string]any{"filter": map[string]any{"resources": []string{fixtureRef(4).ID}}}, 0},
			{"project_intersection_zero", map[string]any{"filter": map[string]any{"resources": []string{fixtureRef(6).ID}}}, 0},
			{"type_intersection_zero", map[string]any{"filter": map[string]any{"query": "type=issue", "types": []string{"absent"}}}, 0},
			{"fields_sort_budget_independent", map[string]any{"filter": map[string]any{"query": "title=needle AND status=todo"}, "fields": []string{"id", "parent", "fields.title"}, "sort": []string{"-title", "id"}, "budgetTokens": 1}, 2},
			{"explicit_empty_fields_is_ordinary", map[string]any{"filter": map[string]any{"query": "title=needle AND status=todo"}, "fields": []string{}, "groupBy": []string{}, "budgetTokens": 0}, 2},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exactCountPage(t, f.call(t, countReader(), "view.run", map[string]any{"countOnly": true, "definition": tc.definition}, 16000+i), tc.want)
			})
		}
		t.Run("saved_definition_same_predicate", func(t *testing.T) {
			exactCountPage(t, f.call(t, countReader(), "view.run", map[string]any{"countOnly": true, "viewId": "ordinary-count"}, 16100), 2)
		})
		for i, by := range []model.PrincipalRef{
			{Kind: "user", ID: "user:empty-count", Caveats: map[string]any{"refs": []model.Ref{}}},
			worker("user:operator"),
		} {
			t.Run(fmt.Sprintf("current_read_permission_zero_%d", i), func(t *testing.T) {
				exactCountPage(t, f.call(t, by, "view.run", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{}}}, 16101+i), 0)
			})
		}
	})
}

func TestB23OrdinaryCountOptInCompatibilityAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		for i, source := range []map[string]any{
			{"definition": map[string]any{"filter": map[string]any{"query": "title=needle AND status=todo"}, "budgetTokens": 1}},
			{"viewId": "ordinary-count"},
		} {
			first := f.call(t, countReader(), "view.run", source, 17000+i*10)
			page := ordinaryPage(t, first)
			if page.Count != nil || len(page.Items) != 1 || page.NextCursor == "" {
				t.Fatalf("existing page changed: %s", first.Result)
			}
			source["countOnly"] = false
			explicitFalse := f.call(t, countReader(), "view.run", source, 17001+i*10)
			ordinaryPage(t, explicitFalse)
			if string(first.Result) != string(explicitFalse.Result) {
				t.Fatalf("absent and false differ: absent=%s false=%s", first.Result, explicitFalse.Result)
			}
			source["cursor"] = page.NextCursor
			last := ordinaryPage(t, f.call(t, countReader(), "view.run", source, 17002+i*10))
			if last.Count != nil || len(last.Items) != 1 || last.Items[0].ID != fixtureRef(3).ID || last.NextCursor != "" {
				t.Fatalf("false changed existing continuation: %+v", last)
			}
		}
		grouped := map[string]any{"definition": map[string]any{"filter": map[string]any{}, "fields": []string{}, "groupBy": []string{"type"}}}
		first := f.call(t, countReader(), "view.run", grouped, 17100)
		success(t, first)
		grouped["countOnly"] = false
		second := f.call(t, countReader(), "view.run", grouped, 17101)
		success(t, second)
		if string(first.Result) != string(second.Result) {
			t.Fatalf("false changed existing grouped response: absent=%s false=%s", first.Result, second.Result)
		}
	})
}

func TestB23OrdinaryExactCountCurrentAsOfAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		definition := map[string]any{"filter": map[string]any{"query": "status=todo AND title=needle"}, "budgetTokens": 1}
		first := exactCountPage(t, f.call(t, countReader(), "view.run", map[string]any{"countOnly": true, "definition": definition}, 18000), 2)
		success(t, f.call(t, worker("user:operator"), "bead.update", map[string]any{"id": fixtureRef(3), "fields": map[string]any{"title": "changed"}}, 18001))
		fresh := exactCountPage(t, f.call(t, countReader(), "view.run", map[string]any{"countOnly": true, "definition": definition}, 18002), 1)
		if reflect.DeepEqual(fresh.AsOf, first.AsOf) {
			t.Fatal("fresh count reused the old snapshot")
		}
		old := exactCountPage(t, f.call(t, countReader(), "view.run", map[string]any{"countOnly": true, "definition": definition, "asOf": first.AsOf}, 18003), 2)
		if !reflect.DeepEqual(old.AsOf, first.AsOf) {
			t.Fatal("historical count changed the selected content snapshot")
		}
		narrow := countReader()
		narrow.Caveats = map[string]any{"refs": []model.Ref{fixtureRef(2)}}
		exactCountPage(t, f.call(t, narrow, "view.run", map[string]any{"countOnly": true, "definition": definition, "asOf": first.AsOf}, 18004), 1)
		// CP7 user reads do not depend on project activity. Preserve that
		// adopted rule; current caller caveats still constrain old content.
		f.base.Projects[0].Active = false
		if err := f.facts.Replace(f.base); err != nil {
			t.Fatal(err)
		}
		exactCountPage(t, f.call(t, countReader(), "view.run", map[string]any{"countOnly": true, "definition": definition, "asOf": first.AsOf}, 18005), 2)
		narrow.Caveats = map[string]any{"refs": []model.Ref{}}
		exactCountPage(t, f.call(t, narrow, "view.run", map[string]any{"countOnly": true, "definition": definition, "asOf": first.AsOf}, 18006), 0)
	})
}

func TestB23OrdinaryCountWireValidationAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		page := ordinaryPage(t, f.call(t, countReader(), "view.run", map[string]any{"viewId": "ordinary-count"}, 19000))
		for i, tc := range []struct {
			name   string
			params map[string]any
			code   model.ErrorCode
		}{
			{"cursor_valid", map[string]any{"countOnly": true, "viewId": "ordinary-count", "cursor": page.NextCursor}, "invalid_params"},
			{"cursor_malformed", map[string]any{"countOnly": true, "viewId": "ordinary-count", "cursor": "not-a-cursor"}, "invalid_params"},
			{"cursor_empty", map[string]any{"countOnly": true, "viewId": "ordinary-count", "cursor": ""}, "invalid_params"},
			{"group_type_no_cap_exception", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{}, "fields": []string{}, "groupBy": []string{"type"}}}, "invalid_params"},
			{"group_epic_progress", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{"query": "parent=" + fixtureRef(2).ID}, "fields": []string{}, "groupBy": []string{"epic-progress"}}}, "invalid_params"},
			{"flag_string", map[string]any{"countOnly": "true", "viewId": "ordinary-count"}, "invalid_params"},
			{"flag_number", map[string]any{"countOnly": 1, "viewId": "ordinary-count"}, "invalid_params"},
			{"flag_null", map[string]any{"countOnly": nil, "viewId": "ordinary-count"}, "invalid_params"},
			{"flag_object", map[string]any{"countOnly": map[string]any{}, "viewId": "ordinary-count"}, "invalid_params"},
			{"flag_in_definition_refused", map[string]any{"definition": map[string]any{"filter": map[string]any{}, "countOnly": true}}, "invalid_params"},
			{"missing_definition", map[string]any{"countOnly": true}, "invalid_params"},
			{"two_definitions", map[string]any{"countOnly": true, "viewId": "ordinary-count", "definition": map[string]any{"filter": map[string]any{}}}, "invalid_params"},
			{"saved_missing", map[string]any{"countOnly": true, "viewId": "missing"}, "not_found"},
			{"malformed_query", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{"query": "title=needle AND"}}}, "invalid_params"},
			{"unsupported_query", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{"query": "unknown=value"}}}, "unknown_field"},
			{"invalid_fields", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{}, "fields": []string{"unknown"}}}, "unknown_field"},
			{"invalid_sort", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{}, "sort": []string{"unknown"}}}, "invalid_params"},
			{"invalid_budget", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{}, "budgetTokens": -1}}, "invalid_params"},
			{"unrepresentable_page_budget", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{}, "budgetTokens": "18446744073709551615"}}, "invalid_params"},
			{"overflow_budget", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{}, "budgetTokens": "18446744073709551616"}}, "invalid_params"},
			{"conflicting_project", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{"query": "project=other"}}}, "invalid_params"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got := f.call(t, countReader(), "view.run", tc.params, 19001+i)
				if got.Error == nil || got.Error.Data.Code != tc.code || len(got.Result) != 0 {
					t.Fatalf("count refusal=%+v want=%s, no partial count", got, tc.code)
				}
				t.Logf("count typed refusal: %s", got.Error.Data.Code)
			})
		}
	})
}

// Keep the large disposable fixture's lease at the production renewal cadence.
// A renewal failure is a test failure; this neither extends the lease TTL nor
// retries a failed operation, and changes no shared driver/test lifecycle.
func renewCountFixtureLease(t *testing.T, lease driver.ActiveLease) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	failed := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil {
					if ctx.Err() == nil {
						failed <- err
					}
					return
				}
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		select {
		case err := <-failed:
			t.Errorf("count fixture lease renewal failed: %v", err)
		default:
		}
	})
}

func TestB23OrdinaryExactCountCapAndRefusalAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		renewCountFixtureLease(t, f.lease)
		for start := 0; start <= viewops.OrdinaryCandidateCap; start += 1000 {
			end := min(start+1000, viewops.OrdinaryCandidateCap+1)
			changes := make([]conformance.FixtureChange, 0, end-start)
			for i := start; i < end; i++ {
				ref := fixtureRef(100 + i)
				status := "todo"
				if i == viewops.OrdinaryCandidateCap {
					status = "done"
				}
				body, err := json.Marshal(map[string]any{"id": ref.ID, "type": "issue", "project": "project", "fields": map[string]any{"title": "countcap", "status": status}})
				if err != nil {
					t.Fatal(err)
				}
				changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body})
			}
			if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
				t.Fatal(err)
			}
		}
		reader := model.PrincipalRef{Kind: "user", ID: "user:cap-reader"}
		t.Run("at_cap_exact_before_budget", func(t *testing.T) {
			exactCountPage(t, f.call(t, reader, "view.run", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{"query": "title=countcap AND status=todo"}, "budgetTokens": 1}}, 20000), viewops.OrdinaryCandidateCap)
		})
		for i, fields := range []any{nil, []string{}} {
			t.Run(fmt.Sprintf("over_cap_refused_%d", i), func(t *testing.T) {
				definition := map[string]any{"filter": map[string]any{"query": "title=countcap"}, "budgetTokens": 1}
				if fields != nil {
					definition["fields"] = fields
				}
				got := f.call(t, reader, "view.run", map[string]any{"countOnly": true, "definition": definition}, 20001+i)
				if got.Error == nil || got.Error.Data.Code != "scope_too_large" || len(got.Result) != 0 || fmt.Sprint(got.Error.Data.Details["cap"]) != "20000" {
					t.Fatalf("ordinary count cap returned a partial/incorrect result: %+v", got)
				}
				t.Logf("count typed refusal: scope_too_large cap=%v", got.Error.Data.Details["cap"])
			})
		}
		t.Run("denied_rows_do_not_consume_cap", func(t *testing.T) {
			exactCountPage(t, f.call(t, countReader(), "view.run", map[string]any{"countOnly": true, "definition": map[string]any{"filter": map[string]any{"query": "title=countcap"}}}, 20003), 0)
		})
	})
}

func TestB23CountBatchAuthorizationMatchesGuardedGetAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		refs := []model.ResourceRef{}
		for _, n := range []int{1, 2, 3, 4, 5, 6, 7, 8, 999, 2} {
			refs = append(refs, model.ResourceRef{Kind: "bead", Ref: fixtureRef(n)})
		}
		for i, by := range []model.PrincipalRef{
			countReader(),
			{Kind: "user", ID: "user:all-reader"},
			{Kind: "user", ID: "user:empty-reader", Caveats: map[string]any{"refs": []model.Ref{}}},
			{Kind: "user", ID: "user:project-reader", Caveats: map[string]any{"projects": []model.Ref{model.RefByID("other", model.Natural64(0))}}},
			worker("user:operator"),
		} {
			t.Run(fmt.Sprintf("principal_%d", i), func(t *testing.T) {
				allowed, err := f.server.Service.AuthorizeReadBatch(context.Background(), by, refs)
				if err != nil || len(allowed) != len(refs) {
					t.Fatalf("batch result=%v err=%v", allowed, err)
				}
				for j, resource := range refs {
					got := f.call(t, by, "bead.get", map[string]any{"id": resource.Ref}, 21000+i*100+j)
					if got.Error != nil && got.Error.Data.Code != "not_found" {
						t.Fatalf("point read failed unexpectedly: %+v", got.Error.Data)
					}
					if allowed[j] != (got.Error == nil) {
						t.Fatalf("batch/guarded Get differ: ref=%s batch=%t point=%+v", resource.Ref.ID, allowed[j], got)
					}
				}
			})
		}
		for _, resource := range []model.ResourceRef{{Kind: "view", Ref: fixtureRef(1)}, {Kind: "bead", Ref: model.RefByID("", model.Natural64(0))}} {
			allowed, err := f.server.Service.AuthorizeReadBatch(context.Background(), countReader(), []model.ResourceRef{resource})
			if err == nil || allowed != nil {
				t.Fatalf("invalid batch resource admitted: %v %v", allowed, err)
			}
		}
	})
}

// These hooks inject source failures and an actual authority-generation change
// at the adapter boundary; they do not replace either driver or policy logic.
type countAuthorityStore struct {
	driver.Store
	failKey    string
	reads      int
	beforeRead func(int) error
}

func (s *countAuthorityStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	s.reads++
	if s.beforeRead != nil {
		if err := s.beforeRead(s.reads); err != nil {
			return nil, err
		}
	}
	reader, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &countAuthorityReader{Reader: reader, store: s}, nil
}

type countAuthorityReader struct {
	driver.Reader
	store *countAuthorityStore
}

func (r *countAuthorityReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	for _, key := range keys {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return nil, err
		}
		if encoded == r.store.failKey {
			return nil, errors.New("count test dependency unavailable")
		}
	}
	return r.Reader.Observe(ctx, keys)
}

func TestB23CountBatchCurrentAuthorityGuardsAcrossDrivers(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		store := &countAuthorityStore{Store: open(t)}
		f := ordinaryCountFixture(t, func(*testing.T) driver.Store { return store })
		resources := []model.ResourceRef{{Kind: "bead", Ref: fixtureRef(2)}}
		for _, dep := range []authz.Dependency{
			authz.ContextDependency(f.server.Namespace), authz.ACLDependency(f.server.Namespace),
			{Kind: "sentinel", Key: "plugins"}, {Kind: "sentinel", Key: "ontology"},
		} {
			t.Run(dep.Kind+"_"+dep.Key, func(t *testing.T) {
				var key driver.LockKey
				if dep.Kind == "read_fact" {
					fact, err := driver.NewReadFact(dep.Key)
					if err != nil {
						t.Fatal(err)
					}
					key = driver.LockKey{Kind: driver.LockReadFact, Fact: fact}
				} else {
					sentinel, err := driver.NewSentinel(dep.Key)
					if err != nil {
						t.Fatal(err)
					}
					key = driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
				}
				var err error
				store.failKey, err = driver.EncodeLockKey(key)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { store.failKey = "" }()
				allowed, err := f.server.Service.AuthorizeReadBatch(context.Background(), countReader(), resources)
				if allowed != nil || err == nil {
					t.Fatalf("source failure returned decisions: %v %v", allowed, err)
				}
			})
		}
		t.Run("canceled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			allowed, err := f.server.Service.AuthorizeReadBatch(ctx, countReader(), resources)
			if allowed != nil || err == nil {
				t.Fatalf("canceled batch returned decisions: %v %v", allowed, err)
			}
		})
		t.Run("fresh_final_authority", func(t *testing.T) {
			store.reads = 0
			changed := false
			store.beforeRead = func(n int) error {
				if n != 2 {
					return nil
				}
				store.beforeRead = nil
				next := f.base
				next.Grants = nil
				changed = true
				return f.facts.Replace(next)
			}
			defer func() { store.beforeRead = nil }()
			allowed, err := f.server.Service.AuthorizeReadBatch(context.Background(), countReader(), resources)
			if !changed || allowed != nil || err == nil {
				t.Fatalf("changed current authority returned decisions: changed=%t %v %v", changed, allowed, err)
			}
		})
	})
}

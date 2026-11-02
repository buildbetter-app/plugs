package authzrpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

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
// conformance; every assertion calls the schema-checked public JSON-RPC wire.
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
		Views: ordinarySavedView{},
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

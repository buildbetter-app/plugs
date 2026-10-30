package viewpermissions_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/integration/permissions"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type savedViews struct{ values map[string]viewops.Definition }

func (v *savedViews) GetView(_ context.Context, _ model.PrincipalRef, id string) (viewops.Definition, error) {
	definition, ok := v.values[id]
	if !ok {
		return viewops.Definition{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "saved view is absent"}}
	}
	return definition, nil
}

type fixture struct {
	store  *memory.Store
	lease  driver.ActiveLease
	keys   *authnapikey.Provider
	facts  *authzrpc.MutableFacts
	ns     authz.Namespace
	views  *savedViews
	url    string
	client *http.Client
	server *httpjson.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "viewpermissions-integration", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{
		StoreID: string(meta.StoreID), Incarnation: meta.Incarnation,
		InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0)),
	}
	team := model.RefByID("team", model.Natural64(0))
	projectOne := model.RefByID("project-one", model.Natural64(0))
	projectTwo := model.RefByID("project-two", model.Natural64(0))
	operator := authz.SubjectRef{Kind: "user", ID: "user:operator"}
	base := authzfixed.Facts{
		Namespace: ns,
		Teams:     []authzfixed.Team{{Ref: team, InstallationID: "fixed", Active: true}},
		Projects: []authzfixed.Project{
			{Ref: projectOne, InstallationID: "fixed", Name: "project-one", Active: true},
			{Ref: projectTwo, InstallationID: "fixed", Name: "project-two", Active: true},
		},
		Grants: []authz.Grant{
			{Subject: operator, Scope: authz.Scope{Kind: authz.TeamScope, Ref: team}, Role: "member"},
			{Subject: authz.SubjectRef{Kind: "team", ID: "team"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: projectOne}, Role: "involved"},
			{Subject: authz.SubjectRef{Kind: "team", ID: "team"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: projectTwo}, Role: "involved"},
		},
	}
	facts, err := authzrpc.NewMutableFacts(base)
	if err != nil {
		t.Fatalf("facts: %v", err)
	}

	// Seed through the real service and Writer before installing the guarded
	// composition. These are ordinary durable bead rows, not view fixtures.
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	for i, project := range []string{"project-one", "project-two", "project-one"} {
		_, err := domain.Create(ctx, model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.CreateRequest{
			Type: "issue", Fields: map[string]any{"title": fmt.Sprintf("view-row-%d", i)},
		}, model.WriteMeta{
			Project: project, Agent: "seed", AgentLabel: "seed",
			IdempotencyKey: fmt.Sprintf("%032x", i+1), Method: "bead.create", Return: "full",
		})
		if err != nil {
			t.Fatalf("seed %s: %v", project, err)
		}
	}

	keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	composition, err := permissions.New(ctx, permissions.Config{
		Store: store, Lease: lease, Keys: keys, Facts: facts, Namespace: ns,
	})
	if err != nil {
		t.Fatalf("permissions: %v", err)
	}
	views := &savedViews{values: map[string]viewops.Definition{
		"issues": {Filter: &viewops.Filter{Types: []string{"issue"}}, BudgetTokens: natural(1)},
	}}
	core, err := viewops.New(store, lease, viewops.Config{
		Views:            views,
		OntologyRevision: model.Natural64(1),
		AuthorizeRead: func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
			if resource.Kind != "bead" {
				return errors.New("unexpected view resource kind")
			}
			_, err := composition.Service.Get(ctx, by, service.GetRequest{ID: resource.Ref})
			return err
		},
	})
	if err != nil {
		t.Fatalf("view core: %v", err)
	}
	key := []byte("view-permissions-cursor-key-0123456789")
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatalf("view bind: %v", err)
	}
	if err := viewrpc.BindWithConfig(registry, core, viewrpc.Config{CursorKey: key}); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{
		Incarnation: meta.Incarnation, AuthMode: "authenticated", OntologyRevision: model.Natural64(1),
		Visible: composition.Checker.Visible, CursorKey: key,
	})
	if err != nil {
		t.Fatalf("rpc: %v", err)
	}
	admission, err := authn.New(authn.Config{Provider: keys, AuthorizationInstalled: func() bool { return true }})
	if err != nil {
		t.Fatalf("admission: %v", err)
	}
	handler, err := httpjson.New(server, admission, httpjson.Config{RequestTimeout: 60 * time.Second})
	if err != nil {
		t.Fatalf("http handler: %v", err)
	}
	listener, err := handler.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("http listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- listener.Serve() }()
	f := &fixture{
		store: store, lease: lease, keys: keys, facts: facts, ns: ns,
		views: views, url: "http://" + listener.Addr().String(), client: &http.Client{Timeout: 65 * time.Second, Transport: &http.Transport{Proxy: nil}}, server: listener,
	}
	t.Cleanup(func() {
		f.client.CloseIdleConnections()
		if err := f.server.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("view permissions HTTP server did not stop")
		}
	})
	return f
}

type reply struct {
	status int
	body   []byte
	value  map[string]any
}

func (f *fixture) issue(t *testing.T, principal model.PrincipalRef) authnapikey.IssuedKey {
	t.Helper()
	key, err := f.keys.Issue(principal, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (f *fixture) call(t *testing.T, token, method string, params map[string]any) reply {
	t.Helper()
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "view-permissions", "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, f.url+httpjson.Path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	value := map[string]any{}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("response was not JSON: %q", body)
	}
	return reply{status: response.StatusCode, body: body, value: value}
}

func result(t *testing.T, response reply) map[string]any {
	t.Helper()
	value, ok := response.value["result"].(map[string]any)
	if response.status != http.StatusOK || response.value["error"] != nil || !ok {
		t.Fatalf("expected view success, got HTTP %d: %s", response.status, response.body)
	}
	return value
}

func requireCode(t *testing.T, response reply, want string) {
	t.Helper()
	errorObject, ok := response.value["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected RPC error %s, got HTTP %d: %s", want, response.status, response.body)
	}
	data, _ := errorObject["data"].(map[string]any)
	if data["code"] != want {
		t.Fatalf("expected RPC error %s, got HTTP %d: %s", want, response.status, response.body)
	}
}

func TestRealHTTPViewsFilterFixedScopedRowsAndExplain(t *testing.T) {
	f := newFixture(t)
	projectOne := model.RefByID("project-one", model.Natural64(0))
	key := f.issue(t, model.PrincipalRef{
		Kind: "user", ID: "user:operator",
		Caveats: map[string]any{"projects": []model.Ref{projectOne}},
	})
	definition := map[string]any{
		"filter": map[string]any{"types": []string{"issue"}},
		"fields": []string{"id", "project"}, "groupBy": []string{"type"}, "budgetTokens": 1,
	}
	first := result(t, f.call(t, key.Token, "view.run", map[string]any{"definition": definition}))
	groups := first["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("expected one authorized type group, got %v", first)
	}
	group := groups[0].(map[string]any)
	if count, ok := group["count"].(json.Number); !ok || count.String() != "2" {
		t.Fatalf("hidden project row affected count: %v", group)
	}
	if first["nextCursor"] == nil || group["elided"] == nil {
		t.Fatalf("authorized output lost bounded continuation/elision: %v", first)
	}
	if strings.Contains(string(firstJSON(t, first)), "project-two") {
		t.Fatal("denied project row leaked through items or elision")
	}
	continuation := group["elided"].(map[string]any)["continueWith"].(map[string]any)
	last := result(t, f.call(t, key.Token, "view.run", continuation["params"].(map[string]any)))
	if last["nextCursor"] != nil || len(last["groups"].([]any)) != 1 {
		t.Fatalf("authorized continuation did not finish: %v", last)
	}

	explain := result(t, f.call(t, key.Token, "view.explain", map[string]any{
		"viewId": "issues", "itemId": findID(t, first),
	}))
	if explain["included"] != true || explain["viewId"] != "issues" {
		t.Fatalf("authorized explain did not include row: %v", explain)
	}
	// The second durable row is in project-two. Explain must not disclose its
	// filter status or existence through the real fixed authorization callback.
	// The hidden ID is selected from the durable memory snapshot, never from a
	// fixture decision or a synthetic authorization result.
	hidden := hiddenID(t, f.store)
	requireCode(t, f.call(t, key.Token, "view.explain", map[string]any{"viewId": "issues", "itemId": hidden}), "not_found")
}

func TestViewCursorDeniedAfterRealPermissionAndDefinitionChanges(t *testing.T) {
	f := newFixture(t)
	key := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:operator"})
	definition := map[string]any{"filter": map[string]any{"types": []string{"issue"}}, "fields": []string{"id"}, "budgetTokens": 1}
	first := result(t, f.call(t, key.Token, "view.run", map[string]any{"definition": definition}))
	cursor, ok := first["nextCursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("first page did not return a cursor: %v", first)
	}
	f.keys.Revoke(key.ID)
	revoked := f.call(t, key.Token, "view.run", map[string]any{"definition": definition, "cursor": cursor})
	if revoked.status != http.StatusUnauthorized {
		t.Fatalf("revoked credential was accepted on continuation: HTTP %d %s", revoked.status, revoked.body)
	}
	requireCode(t, revoked, "unauthenticated")

	// A fresh credential gets a new signed first page. Changing the trusted
	// saved definition then invalidates its bound cursor by digest.
	fresh := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:operator"})
	f.views.values["issues"] = viewops.Definition{Filter: &viewops.Filter{Types: []string{"issue"}}, Sort: []string{"-id"}, BudgetTokens: natural(1)}
	firstSaved := result(t, f.call(t, fresh.Token, "view.run", map[string]any{"viewId": "issues"}))
	savedCursor, ok := firstSaved["nextCursor"].(string)
	if !ok || savedCursor == "" {
		t.Fatalf("saved view did not return a cursor: %v", firstSaved)
	}
	f.views.values["issues"] = viewops.Definition{Filter: &viewops.Filter{Types: []string{"issue"}}, Sort: []string{"id"}, BudgetTokens: natural(1)}
	requireCode(t, f.call(t, fresh.Token, "view.run", map[string]any{"viewId": "issues", "cursor": savedCursor}), "cursor_invalid")
}

func TestViewCursorDeniedWhenFixedFactsSourceChanges(t *testing.T) {
	f := newFixture(t)
	key := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:operator"})
	definition := map[string]any{"filter": map[string]any{"types": []string{"issue"}}, "fields": []string{"id"}, "budgetTokens": 1}
	first := result(t, f.call(t, key.Token, "view.run", map[string]any{"definition": definition}))
	cursor, ok := first["nextCursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("first page did not return a cursor: %v", first)
	}
	// The source is replaced as one complete trusted snapshot. A namespace
	// drift is rejected by the real Checker, so a continuation cannot reuse a
	// cursor under an untrusted fixed-permission source.
	changed := authzfixed.Facts{Namespace: authz.Namespace{
		StoreID: f.ns.StoreID, Incarnation: f.ns.Incarnation,
		InstallationID: "replaced", Authority: f.ns.Authority,
	}}
	if err := f.facts.Replace(changed); err != nil {
		t.Fatal(err)
	}
	requireCode(t, f.call(t, key.Token, "view.run", map[string]any{"definition": definition, "cursor": cursor}), "authz_unavailable")
}

func TestCandidateCapAppliesAfterRealFixedReadAuthorization(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	renewCtx, stopRenew := context.WithCancel(ctx)
	defer stopRenew()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = f.lease.Renew(ctx)
			case <-renewCtx.Done():
				return
			}
		}
	}()
	for start := 0; start < viewops.OrdinaryCandidateCap+1; start += 1000 {
		end := start + 1000
		if end > viewops.OrdinaryCandidateCap+1 {
			end = viewops.OrdinaryCandidateCap + 1
		}
		changes := make([]conformance.FixtureChange, 0, end-start)
		for i := start; i < end; i++ {
			id := fmt.Sprintf("cap-%05d", i)
			body, err := json.Marshal(pipeline.Document{ID: id, Type: "issue", Project: "project-one", Fields: map[string]any{"title": id}})
			if err != nil {
				t.Fatal(err)
			}
			changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body})
		}
		if _, err := conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
			t.Fatal(err)
		}
	}
	sponsor := model.PrincipalRef{Kind: "user", ID: "user:operator"}
	agent := f.issue(t, model.PrincipalRef{Kind: "agent", ID: "agent:viewer", Sponsor: &sponsor})
	// The real fixed component denies Agent bead reads. If the view cap were
	// applied before current authorization, these 20,001 rows would refuse;
	// after authorization they produce a bounded empty result.
	got := result(t, f.call(t, agent.Token, "view.run", map[string]any{
		"definition": map[string]any{"filter": map[string]any{}, "fields": []string{"id"}},
	}))
	if len(got["items"].([]any)) != 0 || got["nextCursor"] != nil {
		t.Fatalf("denied candidates escaped post-authorization cap: %v", got)
	}
}

func firstJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func natural(value uint64) *model.Natural {
	n := model.Natural64(value)
	return &n
}

func findID(t *testing.T, result map[string]any) string {
	t.Helper()
	groups := result["groups"].([]any)
	items := groups[0].(map[string]any)["items"].([]any)
	if len(items) == 0 {
		t.Fatal("first page did not contain an authorized row")
	}
	return items[0].(map[string]any)["id"].(string)
}

func hiddenID(t *testing.T, store *memory.Store) string {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Query(context.Background(), driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range page.Items {
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			t.Fatal(err)
		}
		if doc.Project == "project-two" {
			return doc.ID
		}
	}
	t.Fatal("hidden project row was not found in the durable memory snapshot")
	return ""
}

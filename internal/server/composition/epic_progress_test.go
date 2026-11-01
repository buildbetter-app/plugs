package composition

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

func epicRef(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

type epicFixture struct {
	*runtimeFixture
	key      string
	facts    *authzrpc.MutableFacts
	base     authzfixed.Facts
	adapter  *viewops.EpicProgressConfig
	opt      *viewops.EpicProgressConfig
	revision model.Revision
}

func newEpicFixture(t *testing.T, enabled bool) *epicFixture {
	t.Helper()
	f := &epicFixture{}
	keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	var opt *viewops.EpicProgressConfig
	if enabled {
		opt = &viewops.EpicProgressConfig{
			OntologySource: service.StaticOntologySource(epicRef("ontology")),
			ValidateAuthority: func(ctx context.Context, by model.PrincipalRef, expected string) error {
				return f.adapter.ValidateAuthority(ctx, by, expected)
			},
			AuthorizeScope: func(ctx context.Context, by model.PrincipalRef, r driver.Reader, parent model.Ref, children []model.Ref) (viewops.EpicProgressAuthority, error) {
				return f.opt.AuthorizeScope(ctx, by, r, parent, children)
			},
		}
	}
	f.runtimeFixture = startRuntime(t, Config{Driver: "memory", Project: "parent-project", Address: DefaultAddress,
		AuthMode: "authenticated", Auth: authn.Config{Provider: keys, AuthorizationInstalled: func() bool { return true }}, EpicProgress: opt})
	ctx := context.Background()
	meta, err := f.r.Store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: epicRef("authority")}
	f.base.Namespace = ns
	for _, name := range []string{"parent-project", "child-project", "secret-project"} {
		f.base.Projects = append(f.base.Projects, authzfixed.Project{Ref: epicRef(name), Name: name, InstallationID: "fixed", Active: true})
		if name != "secret-project" {
			f.base.Grants = append(f.base.Grants, authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: epicRef(name)}, Role: "involved"})
		}
	}
	f.facts, err = authzrpc.NewMutableFacts(f.base)
	if err != nil {
		t.Fatal(err)
	}
	app, err := authzrpc.New(ctx, authzrpc.Config{Store: f.r.Store, Lease: f.r.Lease, Provider: keys, Namespace: ns, Facts: f.facts, DefaultProject: "parent-project"})
	if err != nil {
		t.Fatal(err)
	}
	f.adapter, err = viewrpc.NewEpicProgressConfig(app.Checker, service.StaticOntologySource(epicRef("ontology")))
	if err != nil {
		t.Fatal(err)
	}
	f.opt = f.adapter
	issued, err := keys.Issue(model.PrincipalRef{Kind: "agent", ID: "agent:epic-worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"}}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	f.key = issued.Token
	source, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	changes := []conformance.FixtureChange{{Ref: epicRef("ontology"), Kind: "ontology", Op: model.OpCreate, Body: source}}
	for _, row := range []struct{ id, project, parent, status string }{
		{"epic", "parent-project", "", "todo"},
		{"visible-one", "child-project", "epic", "todo"},
		{"visible-two", "parent-project", "epic", "done"},
		{"hidden-child", "secret-project", "epic", "secret-status-must-not-be-bucketed"},
		{"grandchild", "child-project", "visible-one", "in_progress"},
		{"unrelated", "child-project", "", "in_progress"},
	} {
		doc := pipeline.Document{ID: row.id, Type: "issue", Project: row.project, Fields: map[string]any{"title": row.id, "status": row.status, "priority": 0}}
		if row.parent != "" {
			parent := epicRef(row.parent)
			doc.Parent = &parent
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: epicRef(row.id), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	cs, err := conformance.Seed(ctx, f.r.Store, f.r.Lease, driver.ChangesetHeader{}, changes)
	if err != nil {
		t.Fatal(err)
	}
	f.revision = cs.Revision
	return f
}

func epicParams(parent string) map[string]any {
	return map[string]any{"definition": map[string]any{"filter": map[string]any{"query": "parent=" + parent}, "fields": []string{}, "groupBy": []string{"epic-progress"}}, "_meta": map[string]any{"project": "parent-project", "agent": "epic-client"}}
}

func (f *epicFixture) call(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "epic-test", "method": "view.run", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+f.server.Addr().String()+"/rpc", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+f.key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("epic RPC status %d", resp.StatusCode)
	}
	var response map[string]any
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func epicResult(t *testing.T, response map[string]any) map[string]any {
	t.Helper()
	if response["error"] != nil {
		t.Fatalf("epic RPC error: %v", response["error"])
	}
	result, ok := response["result"].(map[string]any)
	if !ok {
		t.Fatal("missing epic result")
	}
	if len(result["items"].([]any)) != 0 || result["nextCursor"] != nil {
		t.Fatalf("count-only result paged/emitted items: %v", result)
	}
	return result
}

func epicCode(t *testing.T, response map[string]any, want string) {
	t.Helper()
	if response["result"] != nil {
		t.Fatalf("refusal published partial result: %v", response)
	}
	err, ok := response["error"].(map[string]any)
	if !ok || err["data"].(map[string]any)["code"] != want {
		t.Fatalf("want %s: %v", want, response)
	}
}

func epicGroups(t *testing.T, result map[string]any) map[string]map[string]any {
	t.Helper()
	groups := map[string]map[string]any{}
	for _, raw := range result["groups"].([]any) {
		group := raw.(map[string]any)
		if len(group["items"].([]any)) != 0 {
			t.Fatal("epic group emitted rows")
		}
		groups[group["key"].(string)] = group
	}
	return groups
}

func TestAuthenticatedEpicProgressDirectChildrenAndRefresh(t *testing.T) {
	f := newEpicFixture(t, true)
	result := epicResult(t, f.call(t, epicParams("epic")))
	groups := epicGroups(t, result)
	if len(groups) != 3 || groups["total"]["count"] != float64(2) || groups["status:todo"]["count"] != float64(1) || groups["status:done"]["count"] != float64(1) {
		t.Fatalf("wrong direct-child counts: %v", groups)
	}
	elided := groups["total"]["elided"].(map[string]any)
	if elided["count"] != float64(1) || elided["reason"] != "authorization" {
		t.Fatalf("hidden remainder: %v", elided)
	}
	raw, _ := json.Marshal(result)
	for _, secret := range []string{"hidden-child", "secret-status", "secret-project", "grandchild", "unrelated", "in_progress"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("nonchild/hidden content leaked: %s", secret)
		}
	}
	refresh := elided["continueWith"].(map[string]any)
	params := refresh["params"].(map[string]any)
	if refresh["method"] != "view.run" || params["cursor"] != nil || params["asOf"] != nil || params["_meta"].(map[string]any)["agent"] != "epic-client" {
		t.Fatalf("refresh binding: %v", refresh)
	}
	if params["definition"].(map[string]any)["fields"] == nil {
		t.Fatal("refresh lost explicit fields=[]")
	}
	if got := epicGroups(t, epicResult(t, f.call(t, params))); got["total"]["count"] != float64(2) {
		t.Fatalf("refresh changed counts: %v", got)
	}
}

func TestEpicProgressHistoricalContentUsesCurrentAuthority(t *testing.T) {
	f := newEpicFixture(t, true)
	facts := f.base
	facts.Grants = append([]authz.Grant(nil), f.base.Grants[:1]...)
	if err := f.facts.Replace(facts); err != nil {
		t.Fatal(err)
	}
	params := epicParams("epic")
	params["asOf"] = map[string]any{"revision": f.revision}
	result := epicResult(t, f.call(t, params))
	groups := epicGroups(t, result)
	if len(groups) != 2 || groups["total"]["count"] != float64(1) || groups["total"]["elided"].(map[string]any)["count"] != float64(2) || groups["status:todo"] != nil {
		t.Fatalf("historical grants reused: %v", groups)
	}
	refresh := groups["total"]["elided"].(map[string]any)["continueWith"].(map[string]any)["params"].(map[string]any)
	if refresh["asOf"] != nil {
		t.Fatal("authorization refresh retained historical snapshot")
	}
	facts.Grants = nil
	if err := f.facts.Replace(facts); err != nil {
		t.Fatal(err)
	}
	epicCode(t, f.call(t, params), "not_found")
	epicCode(t, f.call(t, epicParams("missing")), "not_found")
}

func TestEpicProgressRefusesChangedAuthorityAndUntrustedScope(t *testing.T) {
	for _, mode := range []string{"changed", "outside", "duplicate", "incomplete", "failure"} {
		t.Run(mode, func(t *testing.T) {
			f := newEpicFixture(t, true)
			copy := *f.adapter
			copy.AuthorizeScope = func(ctx context.Context, by model.PrincipalRef, r driver.Reader, parent model.Ref, refs []model.Ref) (viewops.EpicProgressAuthority, error) {
				value, err := f.adapter.AuthorizeScope(ctx, by, r, parent, refs)
				if err != nil {
					return value, err
				}
				switch mode {
				case "changed":
					facts := f.base
					facts.Grants = nil
					if err := f.facts.Replace(facts); err != nil {
						return value, err
					}
				case "outside":
					value.Visible = []model.Ref{epicRef("unrelated")}
				case "duplicate":
					value.Visible = append(value.Visible, value.Visible[0])
				case "incomplete":
					value.Hidden = model.Natural64(0)
				case "failure":
					return value, &model.Error{Data: model.ErrorData{Code: "authz_unavailable"}}
				}
				return value, nil
			}
			f.opt = &copy
			epicCode(t, f.call(t, epicParams("epic")), "authz_unavailable")
		})
	}
}

func TestEpicProgressWireShapeAndCapabilityRefusals(t *testing.T) {
	f := newEpicFixture(t, true)
	for _, mode := range []string{"cursor", "budget", "fields-absent", "fields-nonempty", "extra-filter", "malformed-parent", "project", "unknown-member"} {
		t.Run(mode, func(t *testing.T) {
			p := epicParams("epic")
			d := p["definition"].(map[string]any)
			want := "invalid_params"
			switch mode {
			case "cursor":
				p["cursor"] = "not-a-view-cursor"
				want = "cursor_invalid"
			case "budget":
				d["budgetTokens"] = 1
			case "fields-absent":
				delete(d, "fields")
			case "fields-nonempty":
				d["fields"] = []string{"id"}
			case "extra-filter":
				d["filter"].(map[string]any)["types"] = []string{"issue"}
			case "malformed-parent":
				d["filter"].(map[string]any)["query"] = "parent=epic AND status=todo"
			case "project":
				p["_meta"].(map[string]any)["project"] = "child-project"
				want = "not_found"
			case "unknown-member":
				d["parent"] = "epic"
			}
			epicCode(t, f.call(t, p), want)
		})
	}
	p := epicParams("epic")
	p["definition"].(map[string]any)["budgetTokens"] = 0
	epicResult(t, f.call(t, p))
	disabled := newEpicFixture(t, false)
	response := disabled.call(t, epicParams("epic"))
	epicCode(t, response, "invalid_params")
	details := response["error"].(map[string]any)["data"].(map[string]any)["details"].(map[string]any)
	if details["capability"] != "epic-progress" || details["available"] != false {
		t.Fatal("missing unavailable capability signal")
	}
	if _, err := viewrpc.NewEpicProgressConfig(nil, nil); err == nil {
		t.Fatal("incomplete trusted adapter admitted")
	}
}

func TestEpicProgressOrdinaryInputCap(t *testing.T) {
	f := newEpicFixture(t, true)
	changes := make([]conformance.FixtureChange, 0, viewops.OrdinaryCandidateCap)
	for i := 0; i < viewops.OrdinaryCandidateCap-2; i++ {
		id := fmt.Sprintf("cap-child-%05d", i)
		parent := epicRef("epic")
		body, err := json.Marshal(pipeline.Document{ID: id, Type: "issue", Project: "child-project", Parent: &parent, Fields: map[string]any{"title": id, "status": "todo", "priority": 0}})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: epicRef(id), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	if _, err := conformance.Seed(context.Background(), f.r.Store, f.r.Lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	groups := epicGroups(t, epicResult(t, f.call(t, epicParams("epic"))))
	if groups["total"]["count"] != float64(viewops.OrdinaryCandidateCap) {
		t.Fatal("ordinary cap boundary was reduced")
	}
	last := changes[0]
	last.Ref = epicRef("one-over-cap")
	var doc pipeline.Document
	if err := json.Unmarshal(last.Body, &doc); err != nil {
		t.Fatal(err)
	}
	doc.ID = last.Ref.ID
	last.Body, _ = json.Marshal(doc)
	if _, err := conformance.Seed(context.Background(), f.r.Store, f.r.Lease, driver.ChangesetHeader{}, []conformance.FixtureChange{last}); err != nil {
		t.Fatal(err)
	}
	epicCode(t, f.call(t, epicParams("epic")), "scope_too_large")
}

func TestAnonymousEpicProgressRequiresExplicitNoPluginComposition(t *testing.T) {
	store := memory.New()
	opt, err := viewrpc.NewAnonymousEpicProgressConfig(store, service.StaticOntologySource(epicRef("ontology")))
	if err != nil {
		t.Fatal(err)
	}
	f := startRuntime(t, Config{Driver: "memory", Project: "parent-project", Address: DefaultAddress, EpicProgress: opt, StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil }})
	source, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	parent := epicRef("epic")
	parentBody, _ := json.Marshal(pipeline.Document{ID: "epic", Type: "issue", Project: "parent-project", Fields: map[string]any{"title": "epic", "status": "todo", "priority": 0}})
	childBody, _ := json.Marshal(pipeline.Document{ID: "child", Type: "issue", Project: "other-project", Parent: &parent, Fields: map[string]any{"title": "child", "status": "todo", "priority": 0}})
	if _, err := conformance.Seed(context.Background(), f.r.Store, f.r.Lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: epicRef("ontology"), Kind: "ontology", Op: model.OpCreate, Body: source},
		{Ref: parent, Kind: "bead", Op: model.OpCreate, Body: parentBody},
		{Ref: epicRef("child"), Kind: "bead", Op: model.OpCreate, Body: childBody},
	}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "anonymous-progress", "method": "view.run", "params": epicParams("epic")})
	request, err := http.NewRequest(http.MethodPost, "http://"+f.server.Addr().String()+"/rpc", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var object map[string]any
	if err := json.NewDecoder(response.Body).Decode(&object); err != nil {
		t.Fatal(err)
	}
	groups := epicGroups(t, epicResult(t, object))
	if groups["total"]["count"] != float64(1) || groups["total"]["elided"] != nil {
		t.Fatal("anonymous no-plugin counts were not complete")
	}
	keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	for _, auth := range []authn.Config{{Provider: keys}, {AuthorizationInstalled: func() bool { return true }}} {
		if r, err := Open(context.Background(), Config{Driver: "memory", Project: "parent-project", Address: DefaultAddress, EpicProgress: opt, Auth: auth, AuthMode: "authenticated"}); err == nil {
			_ = r.Close(context.Background())
			t.Fatal("anonymous mode silently bypassed authenticated/policy composition")
		}
	}
}

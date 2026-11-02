package batchsql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	b01Child  = "b01-child-project"
	b01Epic   = "b01-epic-project"
	b01Old    = "b01-old-project"
	b01New    = "b01-new-project"
	b01Hidden = "b01-hidden-project"
)

var b01Projects = []string{b01Child, b01Epic, b01Old, b01New, b01Hidden}

func b01Ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func b01Agent(id string) model.PrincipalRef {
	return model.PrincipalRef{Kind: "agent", ID: "agent:" + id, Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:" + id}}
}

type b01Reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

// These endpoints use real loopback HTTP, API-key admission, the installed
// fixed checker and the ordinary public RPC lowering. Only the protected
// ontology is seeded through conformance; every bead/parent/status is a write.
type b01Public struct {
	f     *fixture
	api   *httptest.Server
	slots *httptest.Server
	keys  *authnapikey.Provider
	facts *authzrpc.MutableFacts
	base  authzfixed.Facts
	owner string
	app   *authzrpc.Composition
}

func newB01Public(t *testing.T, f *fixture) *b01Public {
	t.Helper()
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: b01Ref("ontology/b01"), Kind: "ontology", Op: model.OpCreate, Body: body}})
	if err != nil {
		t.Fatal(err)
	}
	h := &b01Public{f: f}
	h.open(t)
	return h
}

func (h *b01Public) open(t *testing.T) {
	t.Helper()
	f := h.f
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: b01Ref("authority")}
	h.base = authzfixed.Facts{Namespace: ns}
	for _, project := range b01Projects {
		h.base.Projects = append(h.base.Projects, authzfixed.Project{Ref: b01Ref(project), Name: project, InstallationID: "fixed", Active: true})
	}
	for _, role := range []struct {
		user     string
		projects []string
	}{{"owner", b01Projects}, {"mover", []string{b01Child, b01New}}, {"child-only", []string{b01Child}}, {"parent-only", []string{b01Epic}}} {
		for _, project := range role.projects {
			h.base.Grants = append(h.base.Grants, authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: "user:" + role.user}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: b01Ref(project)}, Role: "involved"})
		}
	}
	h.facts, err = authzrpc.NewMutableFacts(h.base)
	if err != nil {
		t.Fatal(err)
	}
	h.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	h.app, err = authzrpc.New(f.ctx, authzrpc.Config{Store: f.store, Lease: f.lease, Provider: h.keys, Facts: h.facts, Namespace: ns, DefaultProject: b01Child, Hierarchy: true})
	if err != nil {
		t.Fatal(err)
	}
	epic, err := viewrpc.NewEpicProgressConfig(h.app.Checker, service.StaticOntologySource(b01Ref("ontology/b01")))
	if err != nil {
		t.Fatal(err)
	}
	views, err := viewops.New(f.store, f.lease, viewops.Config{OntologyRevision: model.Natural64(1), EpicProgress: epic, AuthorizeRead: func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
		_, err := h.app.Service.Get(ctx, by, service.GetRequest{ID: resource.Ref})
		return err
	}})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, h.app.Service); err != nil {
		t.Fatal(err)
	}
	if err := viewrpc.BindWithConfig(registry, views, viewrpc.Config{CursorKey: []byte("b01-public-disposable-cursor-0123456789")}); err != nil {
		t.Fatal(err)
	}
	h.api = h.listen(t, registry, authn.Config{Provider: h.keys, AuthorizationInstalled: func() bool { return true }}, "authenticated")
	// Fixed policy still refuses lifecycle verbs. This separate supported
	// no-plugin host supplies real lifecycle observations, never a new permit.
	registry, err = rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, service.New(f.store, f.lease, f.writer, service.WithOntology(f.ontology), service.WithClock(f.now))); err != nil {
		t.Fatal(err)
	}
	h.slots = h.listen(t, registry, authn.Config{}, "anonymous")
	h.owner = h.issue(t, b01Agent("owner")).Token
}

func (h *b01Public) listen(t *testing.T, registry *rpc.Registry, config authn.Config, mode string) *httptest.Server {
	t.Helper()
	meta, err := h.f.store.Meta(h.f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: b01Child, AuthMode: mode, OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(config)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := httptest.NewServer(handler)
	t.Cleanup(endpoint.Close)
	return endpoint
}

func (h *b01Public) issue(t *testing.T, by model.PrincipalRef) authnapikey.IssuedKey {
	t.Helper()
	key, err := h.keys.Issue(by, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (h *b01Public) viewer(t *testing.T, projects ...string) string {
	t.Helper()
	refs := make([]model.Ref, len(projects))
	for i, project := range projects {
		refs[i] = b01Ref(project)
	}
	return h.issue(t, model.PrincipalRef{Kind: "user", ID: "user:b01-reader", Caveats: map[string]any{"projects": refs}}).Token
}

func (h *b01Public) call(t *testing.T, endpoint *httptest.Server, token, method string, params map[string]any) b01Reply {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "b01", "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(h.f.ctx, http.MethodPost, endpoint.URL+"/rpc/v1", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := endpoint.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		t.Fatal(err)
	}
	var reply b01Reply
	if err := json.Unmarshal(raw, &reply); err != nil || response.StatusCode != http.StatusOK && reply.Error == nil {
		t.Fatalf("invalid public RPC response: HTTP %d %s (%v)", response.StatusCode, raw, err)
	}
	return reply
}

func (h *b01Public) write(t *testing.T, token, method, project string, params map[string]any) b01Reply {
	t.Helper()
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": h.f.meta("b01").IdempotencyKey}
	return h.call(t, h.api, token, method, params)
}

func b01Result(t *testing.T, reply b01Reply) map[string]any {
	t.Helper()
	if reply.Error != nil {
		t.Fatalf("public RPC refused: %+v", reply.Error.Data)
	}
	var result map[string]any
	if err := json.Unmarshal(reply.Result, &result); err != nil || result == nil {
		t.Fatalf("missing RPC result: %s (%v)", reply.Result, err)
	}
	return result
}

func b01Committed(t *testing.T, reply b01Reply) model.Commit {
	t.Helper()
	b01Result(t, reply)
	var commit model.Commit
	if err := json.Unmarshal(reply.Result, &commit); err != nil || len(commit.Results) == 0 {
		t.Fatalf("missing public write commit: %s (%v)", reply.Result, err)
	}
	return commit
}

func b01Code(t *testing.T, reply b01Reply, code model.ErrorCode) {
	t.Helper()
	if reply.Error == nil || reply.Error.Data.Code != code || len(reply.Result) != 0 || reply.Meta.Replayed {
		t.Fatalf("want pure %s refusal, got %+v result=%s", code, reply.Error, reply.Result)
	}
}

func (h *b01Public) create(t *testing.T, project, title string, parent *model.Ref) model.Ref {
	t.Helper()
	params := map[string]any{"type": "issue", "fields": map[string]any{"title": title}}
	if parent != nil {
		params["parent"] = parent
	}
	return b01Committed(t, h.write(t, h.owner, "bead.create", project, params)).Results[0].Ref
}

func (h *b01Public) move(t *testing.T, token string, child model.Ref, parent *model.Ref) b01Reply {
	t.Helper()
	return h.write(t, token, "tx.apply", b01Child, map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{hierarchyMove(child, parent)}}})
}

func (h *b01Public) slot(t *testing.T, method, project string, target model.Ref) {
	t.Helper()
	before := h.f.state(t)
	b01Code(t, h.write(t, h.owner, method, project, map[string]any{"id": target}), "forbidden")
	requireSameState(t, before, h.f.state(t))
	params := map[string]any{"id": target, "_meta": map[string]any{"project": project, "idempotencyKey": h.f.meta("slot").IdempotencyKey}}
	b01Committed(t, h.call(t, h.slots, "", method, params))
}

func (h *b01Public) ready(t *testing.T, token string, want ...model.Ref) {
	t.Helper()
	reply := h.call(t, h.api, token, "bead.ready", map[string]any{"limit": 1000, "_meta": map[string]any{"project": b01Child}})
	result := b01Result(t, reply)
	if result["nextCursor"] != nil {
		t.Fatal("test ready set was truncated")
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(reply.Result, &page); err != nil {
		t.Fatal(err)
	}
	got, expected := []string{}, []string{}
	for _, item := range page.Items {
		got = append(got, item.ID)
	}
	for _, ref := range want {
		expected = append(expected, ref.ID)
	}
	sort.Strings(got)
	sort.Strings(expected)
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("public ready membership=%v, want %v", got, expected)
	}
}

func (h *b01Public) get(t *testing.T, token string, child model.Ref, asOf any) map[string]any {
	t.Helper()
	params := map[string]any{"id": child, "_meta": map[string]any{"project": b01Child}}
	if asOf != nil {
		params["asOf"] = asOf
	}
	return b01Result(t, h.call(t, h.api, token, "bead.get", params))["bead"].(map[string]any)
}

func (h *b01Public) unchanged(t *testing.T, action func() b01Reply, code model.ErrorCode) {
	t.Helper()
	before := h.f.state(t)
	rows, predicates := hierarchyRows(t, h.f), hierarchyFacts(t, h.f)
	b01Code(t, action(), code)
	requireSameState(t, before, h.f.state(t))
	if !reflect.DeepEqual(rows, hierarchyRows(t, h.f)) || !reflect.DeepEqual(predicates, hierarchyFacts(t, h.f)) {
		t.Fatal("refused public write changed closure/predicate state")
	}
}

func (h *b01Public) progress(t *testing.T, token string, parent model.Ref, asOf any, want map[string]int, hidden int) map[string]any {
	t.Helper()
	params := map[string]any{"definition": map[string]any{"filter": map[string]any{"query": "parent=" + parent.ID}, "fields": []string{}, "groupBy": []string{"epic-progress"}}, "_meta": map[string]any{"project": b01Epic}}
	if asOf != nil {
		params["asOf"] = asOf
	}
	reply := h.call(t, h.api, token, "view.run", params)
	result := b01Result(t, reply)
	if result["nextCursor"] != nil || len(result["items"].([]any)) != 0 {
		t.Fatal("atomic epic counts returned rows or a cursor")
	}
	counts, remainder := map[string]int{}, 0
	for _, raw := range result["groups"].([]any) {
		group := raw.(map[string]any)
		key := group["key"].(string)
		if _, duplicate := counts[key]; duplicate || len(group["items"].([]any)) != 0 {
			t.Fatal("duplicate/row-bearing epic count group")
		}
		counts[key] = int(group["count"].(float64))
		if raw, exists := group["elided"]; exists {
			elided := raw.(map[string]any)
			if key != "total" || elided["reason"] != "authorization" {
				t.Fatal("hidden child contributed a status bucket")
			}
			remainder = int(elided["count"].(float64))
		}
	}
	if !reflect.DeepEqual(counts, want) || remainder != hidden {
		t.Fatalf("public progress=%v hidden=%d, want %v hidden=%d", counts, remainder, want, hidden)
	}
	return result["asOf"].(map[string]any)
}

func TestB01PublicHierarchyFivePermissionRulesAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			h := newB01Public(t, f)
			parent := h.create(t, b01Epic, "epic", nil)
			old := h.create(t, b01Old, "private old epic", nil)
			next := h.create(t, b01New, "new epic", nil)
			secret := h.create(t, b01Hidden, "hidden parent canary", nil)
			child := h.create(t, b01Child, "child keeps its project", &old)
			original := f.state(t).Meta
			asOf := map[string]any{"revision": original.Head, "incarnation": original.Incarnation}
			t.Run("rule1-attach-needs-child-and-epic-edit", func(t *testing.T) {
				for _, who := range []string{"child-only", "parent-only"} {
					t.Run(who, func(t *testing.T) {
						token := h.issue(t, b01Agent(who)).Token
						h.unchanged(t, func() b01Reply { return h.move(t, token, child, &parent) }, "forbidden")
						h.unchanged(t, func() b01Reply {
							return h.write(t, token, "bead.create", b01Child, map[string]any{"type": "issue", "fields": map[string]any{"title": "must not exist"}, "parent": parent})
						}, "forbidden")
					})
				}
			})
			t.Run("rules4-and5-move-keeps-project-without-old-epic-grant", func(t *testing.T) {
				mover := h.issue(t, b01Agent("mover")).Token
				b01Committed(t, h.move(t, mover, child, &next))
				bead := h.get(t, h.viewer(t, b01Projects...), child, nil)
				if bead["project"] != b01Child || bead["parent"] != next.ID {
					t.Fatalf("move changed child ownership/final parent: %v", bead)
				}
				h.unchanged(t, func() b01Reply { return h.move(t, mover, child, &secret) }, "forbidden")
			})
			t.Run("rule2-current-and-history-parent-existence-only", func(t *testing.T) {
				viewer := h.viewer(t, b01Child, b01New)
				if got := h.get(t, viewer, child, nil)["parent"]; got != next.ID {
					t.Fatalf("current readable parent missing: %v", got)
				}
				bead := h.get(t, viewer, child, asOf)
				if !reflect.DeepEqual(bead["parent"], map[string]any{"exists": true}) || bead["project"] != b01Child {
					t.Fatalf("historical hidden parent disclosed content: %v", bead)
				}
				raw, _ := json.Marshal(bead)
				for _, canary := range []string{old.ID, b01Old, "private old epic"} {
					if bytes.Contains(raw, []byte(canary)) {
						t.Fatalf("hidden parent canary escaped: %s", canary)
					}
				}
				key := h.issue(t, model.PrincipalRef{Kind: "user", ID: "user:revoked"})
				h.get(t, key.Token, child, asOf)
				if !h.keys.Revoke(key.ID) {
					t.Fatal("credential was not revoked")
				}
				b01Code(t, h.call(t, h.api, key.Token, "bead.get", map[string]any{"id": child, "asOf": asOf}), "unauthenticated")
			})
			t.Run("ordered-aliases-final-postimage-and-atomic-refusals", func(t *testing.T) {
				batch := model.ChangeSet{Ops: []model.Op{
					hierarchyCreate("epic", nil, b01New),
					hierarchyCreate("task", hierarchyRef("epic"), b01Child),
					hierarchyMove(model.RefByKey("task"), &parent),
				}}
				commit := b01Committed(t, h.write(t, h.owner, "tx.apply", b01Child, map[string]any{"changeSet": batch}))
				bead := h.get(t, h.viewer(t, b01Projects...), commit.Results[1].Ref, nil)
				if bead["parent"] != parent.ID || bead["project"] != b01Child {
					t.Fatalf("ordered create/reparent lost final image: %v", bead)
				}
				mover := h.issue(t, b01Agent("mover")).Token
				batch.Ops = []model.Op{hierarchyCreate("earlier", &next, b01Child), hierarchyMove(child, &parent)}
				h.unchanged(t, func() b01Reply {
					return h.write(t, mover, "tx.apply", b01Child, map[string]any{"changeSet": batch})
				}, "forbidden")
				h.unchanged(t, func() b01Reply {
					return h.write(t, h.owner, "bead.create", b01Child, map[string]any{"type": "issue", "parent": model.RefByKey("unresolved")})
				}, "invalid_params")
				h.unchanged(t, func() b01Reply { return h.move(t, h.owner, child, &child) }, "ontology_guard_failed")
				op := hierarchyMove(child, &parent)
				op.Reparent.Project = b01Hidden
				h.unchanged(t, func() b01Reply {
					return h.write(t, h.owner, "tx.apply", b01Child, map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{op}}})
				}, "invalid_params")
				verifyHierarchy(t, f)
			})
			t.Run("rule3-visible-counts-hidden-remainder-without-status", func(t *testing.T) {
				visible := h.create(t, b01Child, "visible child", &parent)
				hidden := h.create(t, b01Hidden, "hidden child canary", &parent)
				h.create(t, b01Child, "grandchild is not direct", &visible)
				viewer := h.viewer(t, b01Child, b01Epic)
				want := map[string]int{"total": 2, "status:todo": 2}
				pinned := h.progress(t, viewer, parent, nil, want, 1)
				h.slot(t, "bead.defer", b01Hidden, hidden)
				h.progress(t, viewer, parent, nil, want, 1)
				h.progress(t, viewer, parent, pinned, want, 1)
				h.progress(t, h.viewer(t, b01Projects...), parent, nil, map[string]int{"total": 3, "status:todo": 2, "status:backlog": 1}, 0)
			})
			t.Run("stored-receipt-rechecks-current-child-and-parent-permission", func(t *testing.T) {
				params := map[string]any{"type": "issue", "parent": parent}
				accepted := h.write(t, h.owner, "bead.create", b01Child, params)
				b01Committed(t, accepted)
				replayed := h.call(t, h.api, h.owner, "bead.create", params)
				if replayed.Error != nil || !replayed.Meta.Replayed || !bytes.Equal(accepted.Result, replayed.Result) {
					t.Fatal("public hierarchy create did not replay exactly")
				}
				for _, missing := range []string{b01Child, b01Epic} {
					revoked := h.base
					revoked.Grants = nil
					for _, grant := range h.base.Grants {
						if grant.Subject.ID != "user:owner" || grant.Scope.Ref.ID != missing {
							revoked.Grants = append(revoked.Grants, grant)
						}
					}
					if err := h.facts.Replace(revoked); err != nil {
						t.Fatal(err)
					}
					h.unchanged(t, func() b01Reply { return h.call(t, h.api, h.owner, "bead.create", params) }, "forbidden")
				}
				if err := h.facts.Replace(h.base); err != nil {
					t.Fatal(err)
				}
				replayed = h.call(t, h.api, h.owner, "bead.create", params)
				if replayed.Error != nil || !replayed.Meta.Replayed || !bytes.Equal(accepted.Result, replayed.Result) {
					t.Fatal("revoked replay altered its original receipt")
				}
			})
		})
	}
}

func TestB01PublicDeferredAncestorsAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			h := newB01Public(t, f)
			parent := h.create(t, b01Epic, "deferred ancestor", nil)
			other := h.create(t, b01New, "open destination", nil)
			child := h.create(t, b01Child, "direct child", &parent)
			grandchild := h.create(t, b01Child, "transitive descendant", &child)
			free := h.create(t, b01Child, "unrelated ready work", nil)
			viewer := h.viewer(t, b01Child)
			t.Run("open-parent-does-not-block", func(t *testing.T) {
				h.ready(t, viewer, child, grandchild, free)
			})
			h.slot(t, "bead.defer", b01Epic, parent)
			deferred := f.state(t).Meta
			asOf := map[string]any{"revision": deferred.Head, "incarnation": deferred.Incarnation}
			t.Run("deferred-parent-suppresses-direct-and-transitive-descendants", func(t *testing.T) {
				h.ready(t, viewer, free)
				bead := h.get(t, viewer, child, nil)
				if !reflect.DeepEqual(bead["parent"], map[string]any{"exists": true}) {
					t.Fatalf("readiness disclosed hidden parent content: %v", bead)
				}
			})
			t.Run("direct-claim-refuses-atomically", func(t *testing.T) {
				before := f.state(t)
				params := map[string]any{"id": grandchild, "_meta": map[string]any{"project": b01Child, "idempotencyKey": f.meta("claim").IdempotencyKey}}
				b01Code(t, h.call(t, h.slots, "", "bead.claim", params), "ontology_guard_failed")
				requireSameState(t, before, f.state(t))
			})
			t.Run("claim-next-fresh-probe-excludes-deferred-tree", func(t *testing.T) {
				before := f.state(t)
				params := map[string]any{"filter": map[string]any{"resources": []string{child.ID, grandchild.ID}}, "_meta": map[string]any{"project": b01Child, "idempotencyKey": f.meta("claim-next").IdempotencyKey}}
				reply := h.call(t, h.slots, "", "bead.claimNext", params)
				if reply.Error != nil || string(reply.Result) != "null" {
					t.Fatalf("claimNext selected deferred descendant: result=%s error=%+v", reply.Result, reply.Error)
				}
				requireSameState(t, before, f.state(t))
			})
			t.Run("public-reparent-releases-and-resuppresses-entire-subtree", func(t *testing.T) {
				b01Committed(t, h.move(t, h.owner, child, &other))
				h.ready(t, viewer, child, grandchild, free)
				historic := h.get(t, h.viewer(t, b01Projects...), child, asOf)
				current := h.get(t, h.viewer(t, b01Projects...), child, nil)
				if historic["parent"] != parent.ID || current["parent"] != other.ID || current["project"] != b01Child {
					t.Fatalf("reparent lost content snapshot or ownership: historic=%v current=%v", historic, current)
				}
				b01Committed(t, h.move(t, h.owner, child, &parent))
				h.ready(t, viewer, free)
			})
			var aliasChild, aliasGrandchild model.Ref
			t.Run("parented-alias-create-uses-final-deferred-ancestor", func(t *testing.T) {
				batch := model.ChangeSet{Ops: []model.Op{hierarchyCreate("child", &parent, b01Child), hierarchyCreate("grandchild", hierarchyRef("child"), b01Child)}}
				commit := b01Committed(t, h.write(t, h.owner, "tx.apply", b01Child, map[string]any{"changeSet": batch}))
				aliasChild, aliasGrandchild = commit.Results[0].Ref, commit.Results[1].Ref
				h.ready(t, viewer, free)
				verifyHierarchy(t, f)
			})
			t.Run("public-reopen-releases-deferred-descendants", func(t *testing.T) {
				h.slot(t, "bead.reopen", b01Epic, parent)
				h.ready(t, viewer, child, grandchild, free, aliasChild, aliasGrandchild)
			})
			if engine != "memory" {
				t.Run("sql-reopen-preserves-suppression-and-history", func(t *testing.T) {
					h.slot(t, "bead.defer", b01Epic, parent)
					h.api.Close()
					h.slots.Close()
					f.reopen(t)
					h.open(t)
					h.ready(t, h.viewer(t, b01Child), free)
					if got := h.get(t, h.viewer(t, b01Projects...), child, asOf)["parent"]; got != parent.ID {
						t.Fatalf("SQL reopen lost pinned parent: %v", got)
					}
					verifyHierarchy(t, f)
				})
			}
		})
	}
}

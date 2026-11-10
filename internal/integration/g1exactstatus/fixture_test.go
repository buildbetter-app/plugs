package g1exactstatus_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const project = "g1-native"
const otherProject = "g1-other"
const nativeType = "native-work-item"

var source = model.RefByID("ontology/g1-exact-status", model.Natural64(0))
var user = model.PrincipalRef{Kind: "user", ID: "user:g1-owner"}
var sponsoredAgent = model.PrincipalRef{Kind: "agent", ID: "agent:g1-worker", Sponsor: &user}
var outsider = model.PrincipalRef{Kind: "agent", ID: "agent:g1-outsider", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:g1-outsider"}}

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
func key(label string) string { sum := sha256.Sum256([]byte(label)); return hex.EncodeToString(sum[:]) }

// Only protected ontology/authority configuration is seeded. Every bead,
// status transition, edge, reason and receipt is written through actual HTTP
// RPC, the production typed service, Writer and ontology. The installed-policy
// controls additionally use the production fixed authorizer.
func nativeDocument() ontology.Document {
	d := ontology.DefaultDocument()
	statuses := map[string]ontology.Status{
		"native-open":           {Name: "Native open", Category: ontology.Active},
		"native-ready-alt":      {Name: "Native alternate ready", Category: ontology.Active},
		"native-ready-no-route": {Name: "No reopen transition", Category: ontology.Active},
		"native-working":        {Name: "Native working", Category: ontology.WIP},
		"native-frozen":         {Name: "Native frozen", Category: ontology.Frozen},
		"native-done":           {Name: "Native done", Category: ontology.Done, Outcome: ontology.Success},
		"native-done-alt":       {Name: "Native alternate done", Category: ontology.Done, Outcome: ontology.Success},
		"native-done-no-route":  {Name: "No close transition", Category: ontology.Done, Outcome: ontology.Success},
		"native-failed":         {Name: "Failure", Category: ontology.Completed, Outcome: ontology.Failure},
		"native-archived":       {Name: "Archived", Category: ontology.Active, Archived: true},
		"foreign-active":        {Name: "Different workflow", Category: ontology.Active},
	}
	names := []string{}
	for name, s := range statuses {
		d.Statuses[name] = s
		if name != "foreign-active" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	closeGuards := []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}
	d.Workflows["native-flow"] = ontology.Workflow{Statuses: names, Initial: "native-open", Slots: map[ontology.Slot]string{ontology.Claim: "native-working", ontology.Close: "native-done", ontology.Reopen: "native-open"}, Transitions: []ontology.Rule{
		{From: []string{"native-open"}, To: "native-working", Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}},
		{To: "native-frozen", Guards: []ontology.Guard{{Kind: ontology.Permission}}},
		{To: "native-open", Guards: []ontology.Guard{{Kind: ontology.Permission}}},
		{From: []string{"native-open", "native-done", "native-done-alt"}, To: "native-ready-alt", Guards: []ontology.Guard{{Kind: ontology.Permission}}},
		{To: "native-done", Guards: closeGuards},
		{To: "native-done-alt", Guards: append(append([]ontology.Guard{}, closeGuards...), ontology.Guard{Kind: ontology.FieldsRequired, Fields: []string{"proof"}})},
	}}
	d.Types[nativeType] = ontology.Type{Name: "Native work item", Workflow: "native-flow", Fields: map[string]ontology.Field{"title": {Type: "string"}, "proof": {Type: "string"}, "labels": {Type: "strings"}}}
	return d
}

func compiled(t *testing.T) (ontology.Snapshot, service.OntologySource) {
	t.Helper()
	d := nativeDocument()
	// The same exact status is frozen in a second project. A target selector
	// must use the bead's bound project, never a global or caller status table.
	overlay := ontology.Document{Statuses: map[string]ontology.Status{"native-ready-alt": {Name: "Other project frozen", Category: ontology.Frozen}}}
	src := service.OntologySource{Base: d, Overlays: map[string]ontology.Document{otherProject: overlay}}
	snap, problems := ontology.Compile(src.Base, src.Overlays, nil)
	if len(problems) != 0 {
		t.Fatalf("native ontology: %+v", problems)
	}
	return snap, src
}

type response struct {
	Result json.RawMessage `json:"result"`
	Error  *wire.Error     `json:"error"`
	Meta   struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

type fixture struct {
	t           *testing.T
	ctx         context.Context
	engine      engine
	store       driver.Store
	lease       driver.ActiveLease
	api         *httptest.Server
	authAPI     *httptest.Server
	actor       model.PrincipalRef
	keys        *authnapikey.Provider
	snapshot    ontology.Snapshot
	facts       authzfixed.Facts
	policy      *model.Ref
	seq, nextID int
}

func eachFixture(t *testing.T, run func(*testing.T, *fixture)) {
	t.Helper()
	eachConfiguredFixture(t, "local", run)
}

func eachConfiguredFixture(t *testing.T, mode string, run func(*testing.T, *fixture)) {
	t.Helper()
	// The default product configuration has no permission plugin. Every exact
	// status workload runs there; installed-policy controls use a separate User
	// configuration and assert the policy's actual supported/refused actions.
	t.Run(mode, func(t *testing.T) {
		engines(t, func(t *testing.T, e engine) {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			t.Cleanup(cancel)
			snap, src := compiled(t)
			f := &fixture{t: t, ctx: ctx, engine: e, snapshot: snap, actor: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}}
			f.open()
			t.Cleanup(f.close)
			body, err := service.MarshalOntologySource(src)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "g1-ontology-fixture"}}, []conformance.FixtureChange{{Ref: source, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
				t.Fatal(err)
			}
			m, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ns := authz.Namespace{StoreID: string(m.StoreID), Incarnation: m.Incarnation, InstallationID: "fixed", Authority: ref("authority")}
			f.facts = authzfixed.Facts{Namespace: ns}
			for _, p := range []string{project, otherProject} {
				f.facts.Projects = append(f.facts.Projects, authzfixed.Project{Ref: ref(p), Name: p, InstallationID: "fixed", Active: true})
				f.facts.Grants = append(f.facts.Grants, authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: "user:g1-owner"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: ref(p)}, Role: "involved"})
			}
			f.bind()
			run(t, f)
		})
	})
}

func (f *fixture) open() {
	f.t.Helper()
	var err error
	f.store, err = f.engine.driver.Open(f.ctx, driver.Config{DSN: f.engine.dsn})
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "g1-exact-status", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
}
func (f *fixture) bind() {
	f.t.Helper()
	f.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	facts, err := authzrpc.NewMutableFacts(f.facts)
	if err != nil {
		f.t.Fatal(err)
	}
	audit := service.NewTrackedOntologyAudit(service.StaticOntologySource(source))
	var checker *authzrpc.Checker
	options := []service.OntologyOption{}
	config := authzrpc.AuthorityConfig{Facts: facts, Namespace: f.facts.Namespace, Hierarchy: true}
	if f.policy != nil {
		config.LifecyclePolicy = &authzrpc.LifecyclePolicyConfig{Ref: *f.policy}
		options = append(options, service.WithTransitionPermissionObserver(func(ctx context.Context, q service.TransitionPermissionRequest, reader port.SnapshotReader) (service.TransitionPermissionObservation, error) {
			return checker.ObserveTransitionPermission(ctx, q, reader)
		}))
	}
	o := service.NewOntology(f.snapshot.Effective, options...)
	checker, err = authzrpc.NewChecker(f.ctx, f.store, f.lease, o, config)
	if err != nil {
		f.t.Fatal(err)
	}
	// The real Writer and guarded read service share the same Checker and
	// installed custom ontology. No replacement authorizer or fixture rule runs.
	w := pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithOntologyAudit(audit), pipeline.WithAuthorizer(checker), pipeline.WithEventContext(checker), pipeline.WithIDGenerator(func() (string, error) { f.nextID++; return fmt.Sprintf("g1-%06d", f.nextID), nil }))
	domain := service.New(f.store, f.lease, w, service.WithOntology(o), service.WithAuthorizationInstalled(func() bool { return true }))
	f.authAPI = f.bindHTTP(checker.GuardService(domain), true)
	f.api = f.authAPI
	if f.actor.Kind == "anonymous" {
		// No installed policy is the supported local production mode. Keep the
		// real signed endpoint above for every permission-refusal control.
		localWriter := pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithOntologyAudit(audit), pipeline.WithIDGenerator(func() (string, error) { f.nextID++; return fmt.Sprintf("g1-%06d", f.nextID), nil }))
		local := service.New(f.store, f.lease, localWriter, service.WithOntology(o))
		f.api = f.bindHTTP(local, false)
	}
}

func (f *fixture) bindHTTP(domain rpc.Service, authenticated bool) *httptest.Server {
	f.t.Helper()
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err = rpc.BindService(registry, domain); err != nil {
		f.t.Fatal(err)
	}
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	mode := "anonymous"
	admission := authn.Config{}
	if authenticated {
		mode = "authenticated"
		admission = authn.Config{Provider: f.keys, AuthorizationInstalled: func() bool { return true }}
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: m.Incarnation, DefaultProject: project, AuthMode: mode})
	if err != nil {
		f.t.Fatal(err)
	}
	authenticator, err := authn.New(admission)
	if err != nil {
		f.t.Fatal(err)
	}
	handler, err := httpjson.New(server, authenticator, httpjson.Config{})
	if err != nil {
		f.t.Fatal(err)
	}
	return httptest.NewServer(handler)
}
func (f *fixture) close() {
	f.closeHTTP()
	if f.lease != nil {
		if err := f.lease.Release(f.ctx); err != nil {
			f.t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) closeHTTP() {
	if f.api != nil && f.api != f.authAPI {
		f.api.Close()
	}
	if f.authAPI != nil {
		f.authAPI.Close()
	}
	f.api, f.authAPI = nil, nil
}

func (f *fixture) installUserPolicy(grants []authzrpc.LifecycleGrant) {
	f.t.Helper()
	f.closeHTTP()
	policy := ref("lifecycle-policy/g1-user-close")
	body, err := json.Marshal(authzrpc.LifecyclePolicy{Namespace: f.facts.Namespace, Grants: grants})
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "g1-authority-fixture"}}, []conformance.FixtureChange{{Ref: policy, Kind: "lifecycle_policy", Op: model.OpCreate, Body: body}}); err != nil {
		f.t.Fatal(err)
	}
	f.policy, f.actor = &policy, user
	f.bind()
}
func (f *fixture) reopen() {
	f.t.Helper()
	if f.engine.name == "memory" {
		f.t.Fatal("physical reopen requires durable store")
	}
	f.close()
	f.open()
	f.bind()
}
func (f *fixture) call(by model.PrincipalRef, p, method string, params map[string]any, label string) response {
	f.t.Helper()
	api := f.api
	token := ""
	if by.Kind != "anonymous" {
		issued, err := f.keys.Issue(by, time.Now().Add(time.Hour))
		if err != nil {
			f.t.Fatal(err)
		}
		token, api = issued.Token, f.authAPI
	}
	values := map[string]any{}
	for k, v := range params {
		values[k] = v
	}
	meta := map[string]any{"project": p}
	if label != "" {
		meta["idempotencyKey"] = key(label)
		meta["return"] = "full"
	}
	values["_meta"] = meta
	f.seq++
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": f.seq, "method": method, "params": values})
	if err != nil {
		f.t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, api.URL+httpjson.Path, bytes.NewReader(raw))
	if err != nil {
		f.t.Fatal(err)
	}
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := api.Client().Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	var r response
	if err = json.Unmarshal(body, &r); err != nil {
		f.t.Fatalf("HTTP %d %s: %s: %v", res.StatusCode, method, body, err)
	}
	if res.StatusCode != http.StatusOK && r.Error == nil {
		f.t.Fatalf("HTTP %d: %s", res.StatusCode, body)
	}
	return r
}
func accepted(t *testing.T, r response, replay bool) model.Commit {
	t.Helper()
	if r.Error != nil || r.Meta.Replayed != replay || len(r.Result) == 0 {
		t.Fatalf("want commit replay=%v; error=%+v result=%s replay=%v", replay, r.Error, r.Result, r.Meta.Replayed)
	}
	var c model.Commit
	if err := json.Unmarshal(r.Result, &c); err != nil {
		t.Fatal(err)
	}
	return c
}
func (f *fixture) write(method string, params map[string]any, label string) model.Commit {
	f.t.Helper()
	return accepted(f.t, f.call(f.actor, project, method, params, label), false)
}
func (f *fixture) create(p, target, label string) model.Ref {
	f.t.Helper()
	params := map[string]any{"type": nativeType, "fields": map[string]any{"title": label, "proof": "checked"}}
	if target != "" {
		params["targetStatus"] = target
	}
	c := accepted(f.t, f.call(f.actor, p, "bead.create", params, label), false)
	if len(c.Results) != 1 {
		f.t.Fatal(c)
	}
	return c.Results[0].Ref
}
func (f *fixture) get(id model.Ref) service.Bead {
	f.t.Helper()
	r := f.call(f.actor, project, "bead.get", map[string]any{"id": id}, "")
	if r.Error != nil {
		f.t.Fatalf("get: %+v", r.Error)
	}
	var out struct {
		Bead service.Bead `json:"bead"`
	}
	if err := json.Unmarshal(r.Result, &out); err != nil {
		f.t.Fatal(err)
	}
	return out.Bead
}
func (f *fixture) capture(by model.PrincipalRef, method, label string) string {
	f.t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil || rows.NextCursor != nil {
		f.t.Fatalf("rows: %+v %v", rows, err)
	}
	versions := []driver.Page[driver.Version]{}
	for _, row := range rows.Items {
		v, err := r.Versions(f.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || v.NextCursor != nil {
			f.t.Fatalf("versions: %+v %v", v, err)
		}
		versions = append(versions, v)
	}
	log, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		f.t.Fatal(err)
	}
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		f.t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(f.ctx, principal, "", method, key(label))
	if err != nil {
		f.t.Fatal(err)
	}
	locks := []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock}
	sort.Slice(locks, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(locks[i])
		b, _ := driver.EncodeLockKey(locks[j])
		return a < b
	})
	state, err := r.Observe(f.ctx, locks)
	if err != nil {
		f.t.Fatal(err)
	}
	raw, err := json.Marshal([]any{m, r.Snapshot(), rows, versions, log, receipt, state})
	if err != nil {
		f.t.Fatal(err)
	}
	return string(raw)
}
func (f *fixture) refuse(by model.PrincipalRef, p, method string, params map[string]any, label, code string) {
	f.t.Helper()
	before := f.capture(by, method, label)
	r := f.call(by, p, method, params, label)
	if r.Error == nil || r.Error.Data.Code != code || len(r.Result) != 0 || r.Meta.Replayed {
		f.t.Fatalf("want %s: error=%+v result=%s replay=%v", code, r.Error, r.Result, r.Meta.Replayed)
	}
	if after := f.capture(by, method, label); after != before {
		f.t.Fatal("refusal changed rows, versions, graph predicates, head/history or receipt")
	}
}

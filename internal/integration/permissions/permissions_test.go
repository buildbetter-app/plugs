package permissions_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/permissions"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type fixture struct {
	store  *memory.Store
	lease  driver.ActiveLease
	seed   *service.Service
	keys   *authnapikey.Provider
	facts  *authzrpc.MutableFacts
	url    string
	client *http.Client
	nextID atomic.Uint64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	f := &fixture{store: memory.New()}
	t.Cleanup(func() { _ = f.store.Close() })
	lease, err := f.store.Lease(ctx, "permissions-integration", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	f.lease = lease
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	project := model.RefByID("project", model.Natural64(0))
	team := model.RefByID("team", model.Natural64(0))
	operator := authz.SubjectRef{Kind: "user", ID: "user:operator"}
	base := authzfixed.Facts{
		Namespace: ns,
		Projects:  []authzfixed.Project{{Ref: project, InstallationID: "fixed", Name: "project", Active: true}},
		Teams:     []authzfixed.Team{{Ref: team, InstallationID: "fixed", Active: true}},
		Grants: []authz.Grant{
			{Subject: operator, Scope: authz.Scope{Kind: authz.TeamScope, Ref: team}, Role: "member"},
			{Subject: authz.SubjectRef{Kind: "team", ID: "team"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"},
		},
	}
	facts, err := authzrpc.NewMutableFacts(base)
	if err != nil {
		t.Fatal(err)
	}
	f.facts = facts

	// Seed ordinary beads before installing the fixed chain. This is the
	// disposable host fixture path; no protected authorization record is
	// written and no request can use it as a bypass.
	o := service.NewOntology(nil)
	seed := pipeline.New(f.store, lease, pipeline.WithOntology(o), pipeline.WithIDGenerator(func() (string, error) {
		n := f.nextID.Add(1)
		return fmt.Sprintf("00000000-0000-4000-8000-%012d", n), nil
	}))
	domain := service.New(f.store, lease, seed, service.WithOntology(o))
	f.seed = domain
	for i := 0; i < 3; i++ {
		_, err := domain.Create(ctx, model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": fmt.Sprintf("seed-%d", i), "priority": i}}, model.WriteMeta{Project: "project", Agent: "seed", AgentLabel: "seed", IdempotencyKey: fmt.Sprintf("%032x", i+1), Method: "bead.create", Return: "full"})
		if err != nil {
			t.Fatal(err)
		}
	}

	f.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	composition, err := permissions.New(ctx, permissions.Config{Store: f.store, Lease: lease, Keys: f.keys, Facts: facts, Namespace: ns, DefaultProject: "project"})
	if err != nil {
		t.Fatal(err)
	}
	server, err := composition.Handler.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.url = "http://" + server.Addr().String()
	f.client = &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	t.Cleanup(func() {
		f.client.CloseIdleConnections()
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("permissions HTTP server did not stop")
		}
	})
	return f
}

type reply struct {
	status int
	body   []byte
	value  map[string]any
}

func (f *fixture) issue(t *testing.T, p model.PrincipalRef) authnapikey.IssuedKey {
	t.Helper()
	key, err := f.keys.Issue(p, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (f *fixture) call(t *testing.T, token, method string, params map[string]any) reply {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequest(http.MethodPost, f.url+httpjson.Path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Plug-RPC", "1")
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := f.client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	value := map[string]any{}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		t.Fatalf("response was not JSON: %q", body)
	}
	return reply{status: response.StatusCode, body: body, value: value}
}

func result(t *testing.T, r reply) map[string]any {
	t.Helper()
	v, ok := r.value["result"].(map[string]any)
	if r.status != http.StatusOK || r.value["error"] != nil || !ok {
		t.Fatalf("expected success, got HTTP %d: %s", r.status, r.body)
	}
	return v
}

func wantCode(t *testing.T, r reply, want string) {
	t.Helper()
	e, ok := r.value["error"].(map[string]any)
	if r.status != http.StatusOK || !ok || r.value["result"] != nil {
		t.Fatalf("expected RPC error %s, got HTTP %d: %s", want, r.status, r.body)
	}
	d, ok := e["data"].(map[string]any)
	if !ok || d["code"] != want {
		t.Fatalf("expected RPC error %s, got %s", want, r.body)
	}
}

func writeMeta(n int) map[string]any {
	return map[string]any{"idempotencyKey": hex.EncodeToString([]byte(fmt.Sprintf("permission-key-%020d", n))), "agent": "rpc-test"}
}

func TestRealHTTPFixedPermissionsFilterListsAndAuthorizeWrites(t *testing.T) {
	f := newFixture(t)
	user := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:operator"})
	agent := f.issue(t, model.PrincipalRef{Kind: "agent", ID: "agent:worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"}, Caveats: map[string]any{"actions": []string{"modify"}, "projects": []model.Ref{model.RefByID("project", model.Natural64(0))}}})
	// User point reads are allowed by CP7, while an Agent with modification
	// authority receives no record existence signal.
	ready := result(t, f.call(t, user.Token, "bead.ready", map[string]any{"limit": 1, "_meta": map[string]any{"project": "project"}}))
	items := ready["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("user list was not populated: %s", f.call(t, user.Token, "bead.ready", nil).body)
	}
	cursor, ok := ready["nextCursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("first page did not return a signed continuation cursor: %s", f.call(t, user.Token, "bead.ready", nil).body)
	}
	second := result(t, f.call(t, user.Token, "bead.ready", map[string]any{"limit": 1, "cursor": cursor, "_meta": map[string]any{"project": "project"}}))
	secondItems := second["items"].([]any)
	if len(secondItems) != 1 || secondItems[0].(map[string]any)["id"] == items[0].(map[string]any)["id"] {
		t.Fatalf("authorized continuation repeated or hid a bead: %s", f.call(t, user.Token, "bead.ready", nil).body)
	}
	ref := items[0].(map[string]any)["id"].(string)
	got := result(t, f.call(t, user.Token, "bead.get", map[string]any{"id": map[string]any{"id": ref}}))
	if got["bead"].(map[string]any)["id"] != ref {
		t.Fatal("user point read returned the wrong bead")
	}
	wantCode(t, f.call(t, agent.Token, "bead.get", map[string]any{"id": map[string]any{"id": ref}}), "not_found")
	agentReady := result(t, f.call(t, agent.Token, "bead.ready", map[string]any{"limit": 10, "_meta": map[string]any{"project": "project"}}))
	if len(agentReady["items"].([]any)) != 0 {
		t.Fatal("Agent list leaked denied beads")
	}

	params := map[string]any{"id": map[string]any{"id": ref}, "fields": map[string]any{"title": "authorized update"}, "_meta": writeMeta(100)}
	first := result(t, f.call(t, agent.Token, "bead.update", params))
	head := first["revision"]
	replay := f.call(t, agent.Token, "bead.update", params)
	replayResult := result(t, replay)
	if replay.value["_meta"].(map[string]any)["replayed"] != true || !reflect.DeepEqual(first, replayResult) {
		t.Fatal("authorized replay changed the stored result")
	}
	if got = result(t, f.call(t, user.Token, "bead.get", map[string]any{"id": map[string]any{"id": ref}})); got["bead"].(map[string]any)["fields"].(map[string]any)["title"] != "authorized update" {
		t.Fatal("authorized write did not persist through the real store")
	}
	if head == nil {
		t.Fatal("write result omitted its revision")
	}
	// The fixed component has no create permission. The real pipeline refuses
	// the request before it can advance the store or mint an idempotency receipt.
	wantCode(t, f.call(t, agent.Token, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "unsupported"}, "_meta": writeMeta(102)}), "forbidden")
	wantCode(t, f.call(t, agent.Token, "view.run", map[string]any{"_meta": map[string]any{"project": "project"}}), "method_not_found")

	// Revoking the team-to-project edge changes the real source facts. The
	// same idempotency receipt is reauthorized and cannot be replayed after
	// revocation; a new request is denied without advancing the store head.
	metaBefore, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	project := model.RefByID("project", model.Natural64(0))
	team := model.RefByID("team", model.Natural64(0))
	updatedFacts := authzfixed.Facts{Namespace: authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}, Projects: []authzfixed.Project{{Ref: project, InstallationID: "fixed", Name: "project", Active: true}}, Teams: []authzfixed.Team{{Ref: team, InstallationID: "fixed", Active: true}}, Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.TeamScope, Ref: team}, Role: "member"}}}
	if err := f.facts.Replace(updatedFacts); err != nil {
		t.Fatal(err)
	}
	revoked, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantCode(t, f.call(t, agent.Token, "bead.update", map[string]any{"id": map[string]any{"id": ref}, "fields": map[string]any{"title": "revoked"}, "_meta": writeMeta(101)}), "forbidden")
	wantCode(t, f.call(t, agent.Token, "bead.update", params), "forbidden")
	metaAfter, err := f.store.Meta(context.Background())
	if err != nil || metaAfter.Head.String() != revoked.Head.String() || revoked.Head.String() == metaBefore.Head.String() {
		t.Fatal("denied/revoked write advanced the real store")
	}
}

func TestRealHTTPClaimNextAndPinnedFactsInvalidation(t *testing.T) {
	f := newFixture(t)
	agent := f.issue(t, model.PrincipalRef{Kind: "agent", ID: "agent:worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"}, Caveats: map[string]any{"actions": []string{"modify"}, "projects": []model.Ref{model.RefByID("project", model.Natural64(0))}}})
	claimed := result(t, f.call(t, agent.Token, "bead.claimNext", map[string]any{"_meta": writeMeta(200)}))
	if claimed["leaseTtlMs"] == nil || claimed["leaseExpiresAt"] == nil || claimed["revision"] == nil {
		t.Fatalf("claimNext did not return the cataloged claim result: %s", f.call(t, agent.Token, "bead.claimNext", nil).body)
	}
	for i := 0; i < 2; i++ {
		if got := f.call(t, agent.Token, "bead.claimNext", map[string]any{"_meta": writeMeta(202 + i)}); got.status != http.StatusOK || got.value["error"] != nil || got.value["result"] == nil {
			t.Fatalf("claimNext did not drain candidate %d: %s", i, got.body)
		}
	}
	emptyParams := map[string]any{"filter": map[string]any{"types": []string{"issue"}}, "_meta": writeMeta(201)}
	empty := f.call(t, agent.Token, "bead.claimNext", emptyParams)
	if empty.status != http.StatusOK || empty.value["error"] != nil || empty.value["result"] != nil {
		t.Fatalf("claimNext returned a non-null result after its candidates were claimed: %s", empty.body)
	}
	// The null is a durable terminal receipt. It remains null after a later
	// ready bead appears, while changing either selector component conflicts
	// with the original request identity.
	if retry := f.call(t, agent.Token, "bead.claimNext", emptyParams); retry.status != http.StatusOK || retry.value["error"] != nil || retry.value["result"] != nil {
		t.Fatalf("same empty claimNext key did not replay null: %s", retry.body)
	}
	if _, err := f.seed.Create(context.Background(), model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "appeared-after-empty", "priority": 0}}, model.WriteMeta{Project: "project", Agent: "seed", AgentLabel: "seed", IdempotencyKey: fmt.Sprintf("%032x", 99), Method: "bead.create", Return: "full"}); err != nil {
		t.Fatal(err)
	}
	if retry := f.call(t, agent.Token, "bead.claimNext", emptyParams); retry.status != http.StatusOK || retry.value["error"] != nil || retry.value["result"] != nil {
		t.Fatalf("durable empty claimNext receipt was not replayed after a new bead: %s", retry.body)
	}
	changedFilter := map[string]any{"filter": map[string]any{"resources": []string{"different"}}, "_meta": writeMeta(201)}
	wantCode(t, f.call(t, agent.Token, "bead.claimNext", changedFilter), "idempotency_key_reused")
	changedView := map[string]any{"viewId": "different", "_meta": writeMeta(201)}
	wantCode(t, f.call(t, agent.Token, "bead.claimNext", changedView), "idempotency_key_reused")

	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.facts.Load(context.Background(), reader); err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	updated := authzfixed.Facts{Namespace: authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}, Projects: []authzfixed.Project{{Ref: model.RefByID("project", model.Natural64(0)), InstallationID: "fixed", Name: "project", Active: true}}, Teams: []authzfixed.Team{{Ref: model.RefByID("team", model.Natural64(0)), InstallationID: "fixed", Active: true}}}
	if err := f.facts.Replace(updated); err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	if _, err := f.facts.Load(context.Background(), reader); err == nil {
		_ = reader.Close()
		t.Fatal("pinned authorization snapshot ignored the ACL read-fact bump")
	}
	_ = reader.Close()
}

func TestFixedPermissionCompositionRequiresRealInputs(t *testing.T) {
	store := memory.New()
	defer store.Close()
	for _, cfg := range []permissions.Config{{}, {Store: store, Keys: authnapikey.New(authnapikey.Config{})}} {
		composition, err := permissions.New(context.Background(), cfg)
		if composition != nil || err == nil {
			t.Fatal("incomplete permission composition was accepted")
		}
	}
}

func TestFactsSourceRejectsNamespaceDrift(t *testing.T) {
	f := newFixture(t)
	user := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:operator"})
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bad := authzfixed.Facts{Namespace: authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "other", Authority: model.RefByID("authority", model.Natural64(0))}}
	if err := f.facts.Replace(bad); err != nil {
		t.Fatal(err)
	}
	wantCode(t, f.call(t, user.Token, "bead.ready", map[string]any{"limit": 1, "_meta": map[string]any{"project": "project"}}), "authz_unavailable")
}

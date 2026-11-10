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
	store   *memory.Store
	lease   driver.ActiveLease
	seed    *service.Service
	guard   *authzrpc.GuardedService
	checker *authzrpc.Checker
	keys    *authnapikey.Provider
	facts   *authzrpc.MutableFacts
	url     string
	client  *http.Client
	nextID  atomic.Uint64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return newFixtureWithStore(t, nil)
}

func newFixtureWithStore(t *testing.T, wrap func(*memory.Store) driver.Store) *fixture {
	t.Helper()
	return newFixtureWithClaimPolicy(t, wrap, nil)
}

func newFixtureWithClaimPolicy(t *testing.T, wrap func(*memory.Store) driver.Store, claimPolicy *authzrpc.ClaimPolicyConfig) *fixture {
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
	var store driver.Store = f.store
	if wrap != nil {
		store = wrap(f.store)
	}
	composition, err := permissions.New(ctx, permissions.Config{Store: store, Lease: lease, Keys: f.keys, Facts: facts, Namespace: ns, DefaultProject: "project", ClaimPolicy: claimPolicy})
	if err != nil {
		t.Fatal(err)
	}
	f.guard = composition.Service
	f.checker = composition.Checker
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
	f := newFixtureWithClaimPolicy(t, nil, &authzrpc.ClaimPolicyConfig{UseImmutableDefault: true})
	agent := f.issue(t, claimPermissionWorker("agent:worker"))
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
	beforeEmpty, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	empty := f.call(t, agent.Token, "bead.claimNext", emptyParams)
	if empty.status != http.StatusOK || empty.value["error"] != nil || empty.value["result"] != nil {
		t.Fatalf("claimNext returned a non-null result after its candidates were claimed: %s", empty.body)
	}
	afterEmpty, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	beforeRevision, err := beforeEmpty.Head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	afterRevision, err := afterEmpty.Head.Uint64()
	if err != nil || afterRevision != beforeRevision+1 {
		t.Fatalf("empty claimNext did not publish exactly one H+1 receipt: before=%s after=%s", beforeEmpty.Head, afterEmpty.Head)
	}
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "agent", ID: "agent:worker"})
	if err != nil {
		t.Fatal(err)
	}
	receiptReader, err := f.store.Read(context.Background(), driver.View{Incarnation: afterEmpty.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	key := emptyParams["_meta"].(map[string]any)["idempotencyKey"].(string)
	receipt, err := receiptReader.IdempotencyByKey(context.Background(), principal, "", "bead.claimNext", key)
	_ = receiptReader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if receipt == nil || receipt.Commit.Revision.String() != afterEmpty.Head.String() || receipt.Changeset.Kind != model.ChangesetWrite || len(receipt.Changeset.Changes) != 0 || len(receipt.Changeset.Events) != 0 {
		t.Fatalf("empty claimNext receipt was not durable H+1 zero-change state: %+v", receipt)
	}
	// The null is a durable terminal receipt. It remains null after a later
	// ready bead appears, while changing either selector component conflicts
	// with the original request identity.
	if retry := f.call(t, agent.Token, "bead.claimNext", emptyParams); func() bool {
		meta, ok := retry.value["_meta"].(map[string]any)
		return retry.status != http.StatusOK || retry.value["error"] != nil || retry.value["result"] != nil || !ok || meta["replayed"] != true
	}() {
		t.Fatalf("same empty claimNext key did not replay null: %s", retry.body)
	}
	if _, err := f.seed.Create(context.Background(), model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "appeared-after-empty", "priority": 0}}, model.WriteMeta{Project: "project", Agent: "seed", AgentLabel: "seed", IdempotencyKey: fmt.Sprintf("%032x", 99), Method: "bead.create", Return: "full"}); err != nil {
		t.Fatal(err)
	}
	if retry := f.call(t, agent.Token, "bead.claimNext", emptyParams); func() bool {
		meta, ok := retry.value["_meta"].(map[string]any)
		return retry.status != http.StatusOK || retry.value["error"] != nil || retry.value["result"] != nil || !ok || meta["replayed"] != true
	}() {
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

func TestRealHTTPCursorBindsAuthorizationFacts(t *testing.T) {
	f := newFixture(t)
	user := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:operator"})
	first := result(t, f.call(t, user.Token, "bead.ready", map[string]any{
		"limit": 1, "_meta": map[string]any{"project": "project"},
	}))
	cursor, ok := first["nextCursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("ready page did not return a continuation cursor: %s", f.call(t, user.Token, "bead.ready", nil).body)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	updated := authzfixed.Facts{
		Namespace: authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))},
		Projects:  []authzfixed.Project{{Ref: model.RefByID("project", model.Natural64(0)), InstallationID: "fixed", Name: "project", Active: true}},
		Teams:     []authzfixed.Team{{Ref: model.RefByID("team", model.Natural64(0)), InstallationID: "fixed", Active: true}},
	}
	if err := f.facts.Replace(updated); err != nil {
		t.Fatal(err)
	}
	wantCode(t, f.call(t, user.Token, "bead.ready", map[string]any{
		"limit": 1, "cursor": cursor, "_meta": map[string]any{"project": "project"},
	}), "cursor_invalid")
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
	if err := f.facts.Replace(bad); err == nil {
		t.Fatal("installed source accepted a different namespace")
	}
	// Refuse drift at its source without replacing the last valid facts or
	// turning subsequent authorized reads into an unavailable installation.
	result(t, f.call(t, user.Token, "bead.ready", map[string]any{"limit": 1, "_meta": map[string]any{"project": "project"}}))
}

// The hook makes a real authorized claim after the outer request has selected
// its candidate, before its transaction starts. The store never fabricates a
// candidate error or a committed receipt.
type guardedClaimRaceStore struct {
	*memory.Store
	beforeWrite atomic.Pointer[func(context.Context) error]
	writeError  error
	writePlan   func(int32) error
	writes      atomic.Int32
}

func (s *guardedClaimRaceStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if hook := s.beforeWrite.Swap(nil); hook != nil {
		if err := (*hook)(ctx); err != nil {
			return nil, err
		}
	}
	n := s.writes.Add(1)
	if s.writePlan != nil {
		if err := s.writePlan(n); err != nil {
			return nil, err
		}
	}
	if s.writeError != nil {
		return nil, s.writeError
	}
	return s.Store.Write(ctx, lease)
}

func permissionWorker(id string) model.PrincipalRef {
	return model.PrincipalRef{Kind: "agent", ID: id,
		Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"},
		Caveats: map[string]any{"actions": []string{"modify"}, "projects": []model.Ref{model.RefByID("project", model.Natural64(0))}},
	}
}

func claimPermissionWorker(id string) model.PrincipalRef {
	by := permissionWorker(id)
	by.Caveats["actions"] = []string{"modify", "transition"}
	return by
}

type claimSourceRefReader struct {
	driver.Reader
	ref model.Ref
}

func (r claimSourceRefReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	rows, err := r.Reader.Get(ctx, refs)
	if err != nil {
		return nil, err
	}
	rows = append([]driver.Record(nil), rows...)
	for i := range rows {
		rows[i].Ref = r.ref
	}
	return rows, nil
}

func TestClaimPolicyBindsCanonicalBranchZeroSourceReferences(t *testing.T) {
	f := newFixtureWithClaimPolicy(t, nil, &authzrpc.ClaimPolicyConfig{UseImmutableDefault: true})
	ctx := context.Background()
	ready, err := f.seed.Ready(ctx, model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.ReadyRequest{Project: "project"})
	if err != nil || len(ready.Items) != 3 {
		t.Fatalf("real seeded candidates: %+v %v", ready, err)
	}
	id := ready.Items[0].ID
	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, tc := range []struct {
		name    string
		stored  model.Ref
		request model.Ref
		allowed bool
	}{
		{"literal-source-wire-request", model.Ref{ID: id}, model.RefByID(id, model.Natural64(0)), true},
		{"wire-source-literal-request", model.RefByID(id, model.Natural64(0)), model.Ref{ID: id}, true},
		{"different-source-id", model.RefByID("different", model.Natural64(0)), model.RefByID(id, model.Natural64(0)), false},
		{"source-key-arm", model.RefByKey(id), model.RefByID(id, model.Natural64(0)), false},
		{"source-nonzero-branch", model.RefByID(id, model.Natural64(1)), model.RefByID(id, model.Natural64(0)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observation, err := f.checker.ObserveTransitionPermission(ctx, service.TransitionPermissionRequest{
				Principal: claimPermissionWorker("agent:reference-bounds"), Resource: model.ResourceRef{Kind: "bead", Ref: tc.request},
				Project: "project", Type: "issue", FromStatus: "todo", ToStatus: "in_progress", Method: "bead.claimNext", Action: "transition",
			}, claimSourceRefReader{Reader: r, ref: tc.stored})
			if err != nil || !observation.AuthzInstalled || observation.Permission == nil || *observation.Permission != tc.allowed {
				t.Fatalf("source reference permission allowed=%t: observation=%+v err=%v", tc.allowed, observation, err)
			}
		})
	}
}

func TestRealHTTPClaimTransitionInstallationAndCaveatBounds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		policy     *authzrpc.ClaimPolicyConfig
		transition bool
		allowed    bool
	}{
		{name: "omitted-policy", transition: true},
		{name: "installed-policy-without-transition-caveat", policy: &authzrpc.ClaimPolicyConfig{UseImmutableDefault: true}},
		{name: "installed-policy-with-transition-caveat", policy: &authzrpc.ClaimPolicyConfig{UseImmutableDefault: true}, transition: true, allowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixtureWithClaimPolicy(t, nil, tc.policy)
			ctx := context.Background()
			by := permissionWorker("agent:installation-bounds")
			if tc.transition {
				by = claimPermissionWorker(by.ID)
			}
			key := f.issue(t, by)
			ready, err := f.seed.Ready(ctx, model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.ReadyRequest{Project: "project"})
			if err != nil || len(ready.Items) != 3 {
				t.Fatalf("real seeded candidates: %+v %v", ready, err)
			}
			refs := make([]model.Ref, len(ready.Items))
			for i, bead := range ready.Items {
				refs[i] = model.RefByID(bead.ID, model.Natural64(0))
			}
			read := func() (driver.Meta, []driver.Record) {
				t.Helper()
				meta, err := f.store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				r, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				rows, err := r.Get(ctx, refs)
				if err != nil || len(rows) != len(refs) {
					t.Fatalf("complete candidate image: rows=%d err=%v", len(rows), err)
				}
				return meta, rows
			}
			before, rows := read()
			params := map[string]any{"_meta": writeMeta(1100)}
			got := f.call(t, key.Token, "bead.claimNext", params)
			if got.status != http.StatusOK || got.value["error"] != nil {
				t.Fatalf("claim admission: %s", got.body)
			}
			if tc.allowed {
				value, ok := got.value["result"].(map[string]any)
				if !ok || value["leaseTtlMs"] == nil {
					t.Fatalf("installed claim did not produce a lease: response=%s first_candidate=%s", got.body, rows[0].Body)
				}
			} else if value, ok := got.value["result"]; !ok || value != nil {
				t.Fatalf("missing transition authority claimed a candidate: %s", got.body)
			}
			after, afterRows := read()
			beforeHead, _ := before.Head.Uint64()
			afterHead, _ := after.Head.Uint64()
			if afterHead != beforeHead+1 || (!tc.allowed && !reflect.DeepEqual(rows, afterRows)) {
				t.Fatal("claim failed to publish exactly one receipt or changed a denied candidate")
			}
			replay := f.call(t, key.Token, "bead.claimNext", params)
			metadata, ok := replay.value["_meta"].(map[string]any)
			if replay.value["error"] != nil || !reflect.DeepEqual(got.value["result"], replay.value["result"]) || !ok || metadata["replayed"] != true {
				t.Fatalf("receipt replay changed: %s", replay.body)
			}
			replayed, replayRows := read()
			if replayed.Head.String() != after.Head.String() || !reflect.DeepEqual(afterRows, replayRows) {
				t.Fatal("receipt replay published candidate state")
			}
			r, err := f.store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			facts, err := f.facts.Load(ctx, r)
			_ = r.Close()
			if err != nil {
				t.Fatal(err)
			}
			facts.Grants = nil
			if err := f.facts.Replace(facts); err != nil {
				t.Fatal(err)
			}
			// Replacing installed facts is itself a real ACL publication. Keep
			// that commit distinct from the denied replay's state boundary.
			revoked, revokedRows := read()
			revokedHead, _ := revoked.Head.Uint64()
			if revokedHead != afterHead+1 || !reflect.DeepEqual(afterRows, revokedRows) {
				t.Fatal("grant revocation did not publish exactly one ACL-only revision")
			}
			wantCode(t, f.call(t, key.Token, "bead.claimNext", params), "forbidden")
			final, finalRows := read()
			if final.Head.String() != revoked.Head.String() || !reflect.DeepEqual(revokedRows, finalRows) {
				t.Fatal("revoked replay published candidate state")
			}
		})
	}
}

func TestRealHTTPGuardedClaimNextSkipsCompetingClaim(t *testing.T) {
	var raced *guardedClaimRaceStore
	f := newFixtureWithClaimPolicy(t, func(store *memory.Store) driver.Store {
		raced = &guardedClaimRaceStore{Store: store}
		return raced
	}, &authzrpc.ClaimPolicyConfig{UseImmutableDefault: true})
	ctx := context.Background()
	page, err := f.seed.Ready(ctx, model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.ReadyRequest{Project: "project"})
	if err != nil || len(page.Items) != 3 {
		t.Fatalf("real ready candidates: %+v %v", page, err)
	}
	first := model.RefByID(page.Items[0].ID, model.Natural64(0))
	competitor := claimPermissionWorker("agent:competitor")
	var claims atomic.Int32
	hook := func(ctx context.Context) error {
		_, err := f.guard.ClaimWithOutcome(ctx, competitor, service.ClaimRequest{ID: first}, model.WriteMeta{
			Project: "project", IdempotencyKey: fmt.Sprintf("%064x", 901), Return: "full",
		})
		if err == nil {
			claims.Add(1)
		}
		return err
	}
	raced.beforeWrite.Store(&hook)
	worker := f.issue(t, claimPermissionWorker("agent:queue-worker"))
	params := map[string]any{"_meta": writeMeta(900)}
	response := f.call(t, worker.Token, "bead.claimNext", params)
	got := result(t, response)
	results, ok := got["results"].([]any)
	if !ok || len(results) != 1 || claims.Load() != 1 {
		t.Fatalf("guarded queue did not commit after the real competing claim: %s claims=%d", response.body, claims.Load())
	}
	ref := results[0].(map[string]any)["ref"].(map[string]any)["id"]
	if ref != page.Items[1].ID || got["leaseTtlMs"] == nil {
		t.Fatalf("guarded queue did not advance to the next real candidate: %s", response.body)
	}
	stored, err := f.seed.Get(ctx, model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.GetRequest{ID: first})
	if err != nil || stored.Lease == nil || stored.Lease.Principal.ID != competitor.ID {
		t.Fatalf("competing authorized lease was overwritten: %+v %v", stored, err)
	}
	retry := f.call(t, worker.Token, "bead.claimNext", params)
	if !reflect.DeepEqual(got, result(t, retry)) || retry.value["_meta"].(map[string]any)["replayed"] != true {
		t.Fatalf("guarded claim replay changed the winning receipt: %s", retry.body)
	}
}

func TestRealHTTPGuardedClaimNextBoundsPersistentCandidateWriteConflict(t *testing.T) {
	var raced *guardedClaimRaceStore
	conflict := &model.Error{Message: "snapshot changed during all four write attempts", Data: model.ErrorData{Code: "write_conflict", Retryable: true}}
	f := newFixtureWithClaimPolicy(t, func(store *memory.Store) driver.Store {
		raced = &guardedClaimRaceStore{Store: store, writePlan: func(n int32) error {
			// Two complete walks visit three authorized candidates each. Only the
			// seventh write is the first terminal probe; the fourteenth is the
			// probe after the bounded ready restart.
			if n%7 != 0 {
				return conflict
			}
			return nil
		}}
		return raced
	}, &authzrpc.ClaimPolicyConfig{UseImmutableDefault: true})
	worker := f.issue(t, claimPermissionWorker("agent:persistent-conflict"))
	ctx := context.Background()
	before, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"_meta": writeMeta(903)}
	response := f.call(t, worker.Token, "bead.claimNext", params)
	wantCode(t, response, "write_conflict")
	if got := raced.writes.Load(); got != 14 {
		t.Fatalf("persistent guarded candidate exhaustion was not bounded after two terminal probes: writes=%d", got)
	}
	after, err := f.store.Meta(ctx)
	if err != nil || after.Head.String() != before.Head.String() {
		t.Fatalf("persistent guarded candidate exhaustion changed durable state: before=%s after=%s err=%v", before.Head, after.Head, err)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: after.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	principal := claimPermissionWorker("agent:persistent-conflict")
	principalKey, err := driver.PrincipalKey(principal)
	if err != nil {
		t.Fatal(err)
	}
	key := params["_meta"].(map[string]any)["idempotencyKey"].(string)
	receipt, err := reader.IdempotencyByKey(ctx, principalKey, "", "bead.claimNext", key)
	if err != nil {
		t.Fatal(err)
	}
	if receipt != nil {
		t.Fatalf("persistent guarded candidate exhaustion persisted a receipt: %+v", receipt)
	}
}

func TestRealHTTPGuardedClaimNextPreservesGlobalErrors(t *testing.T) {
	for _, code := range []model.ErrorCode{"busy", "write_conflict", "lease_not_held", "lease_expired"} {
		t.Run(string(code), func(t *testing.T) {
			// Even a candidate-looking Hint cannot replace the actual pipeline
			// Message discriminator and turn a global storage error into null.
			refusal := &model.Error{Message: "global storage failure", Data: model.ErrorData{Code: code, Hint: "bead already has a live claim"}}
			f := newFixtureWithStore(t, func(store *memory.Store) driver.Store {
				return &guardedClaimRaceStore{Store: store, writeError: refusal}
			})
			workerPrincipal := permissionWorker("agent:queue-worker")
			worker := f.issue(t, workerPrincipal)
			ctx := context.Background()
			before, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			params := map[string]any{"_meta": writeMeta(902)}
			wantCode(t, f.call(t, worker.Token, "bead.claimNext", params), string(code))
			after, err := f.store.Meta(ctx)
			if err != nil || after.Head.String() != before.Head.String() {
				t.Fatalf("global refusal advanced head: before=%+v after=%+v err=%v", before, after, err)
			}
			reader, err := f.store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			principal, _ := driver.PrincipalKey(workerPrincipal)
			key := params["_meta"].(map[string]any)["idempotencyKey"].(string)
			receipt, err := reader.IdempotencyByKey(ctx, principal, "", "bead.claimNext", key)
			if err != nil || receipt != nil {
				t.Fatalf("global refusal persisted a terminal receipt: %+v %v", receipt, err)
			}
		})
	}
}

func TestRealHTTPGuardedOutcomeAdaptersPreserveFixedPolicy(t *testing.T) {
	for _, method := range []string{"tx.apply", "bead.link", "bead.unlink"} {
		t.Run(method, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			workerPrincipal := permissionWorker("agent:worker")
			worker := f.issue(t, workerPrincipal)
			page, err := f.seed.Ready(ctx, model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.ReadyRequest{Project: "project"})
			if err != nil || len(page.Items) != 3 {
				t.Fatalf("ready fixture: %+v %v", page, err)
			}
			first := model.RefByID(page.Items[0].ID, model.Natural64(0))
			second := model.RefByID(page.Items[1].ID, model.Natural64(0))
			allowed := map[string]any{"id": first, "fields": map[string]any{"title": "permitted named update"}, "_meta": writeMeta(910)}
			committed := result(t, f.call(t, worker.Token, "bead.update", allowed))
			before, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := f.store.Read(ctx, driver.View{Incarnation: before.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			rows, err := reader.Get(ctx, []model.Ref{first, second})
			if err != nil {
				t.Fatal(err)
			}
			changes, err := reader.Changesets(ctx, nil, 100)
			if err != nil {
				t.Fatal(err)
			}
			versions, err := reader.Versions(ctx, first, driver.PageReq{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			params := map[string]any{"from": first, "to": second, "kind": "relates-to", "_meta": writeMeta(911)}
			if method == "tx.apply" {
				// Both patches would be permitted as named updates. The batch
				// adapter must retain tx.apply's unsupported fixed-policy identity.
				params = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: first, Fields: map[string]any{"title": "denied first"}}},
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: second, Fields: map[string]any{"title": "denied second"}}},
				}}, "_meta": writeMeta(911)}
			}
			wantCode(t, f.call(t, worker.Token, method, params), "forbidden")
			wantCode(t, f.call(t, worker.Token, method, params), "forbidden")
			after, err := f.store.Meta(ctx)
			if err != nil || after.Head.String() != before.Head.String() {
				t.Fatalf("denied %s advanced head: before=%+v after=%+v err=%v", method, before, after, err)
			}
			current, err := f.store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			gotRows, err := current.Get(ctx, []model.Ref{first, second})
			if err != nil || !reflect.DeepEqual(rows, gotRows) {
				t.Fatalf("denied %s changed records: %+v %v", method, gotRows, err)
			}
			gotChanges, err := current.Changesets(ctx, nil, 100)
			if err != nil || !reflect.DeepEqual(changes, gotChanges) {
				t.Fatalf("denied %s changed the journal: %+v %v", method, gotChanges, err)
			}
			gotVersions, err := current.Versions(ctx, first, driver.PageReq{Limit: 100})
			if err != nil || !reflect.DeepEqual(versions, gotVersions) {
				t.Fatalf("denied %s changed versions: %+v %v", method, gotVersions, err)
			}
			principal, err := driver.PrincipalKey(workerPrincipal)
			if err != nil {
				t.Fatal(err)
			}
			key := params["_meta"].(map[string]any)["idempotencyKey"].(string)
			receipt, err := current.IdempotencyByKey(ctx, principal, "", method, key)
			if err != nil || receipt != nil {
				t.Fatalf("denied %s persisted a receipt: %+v %v", method, receipt, err)
			}
			retry := f.call(t, worker.Token, "bead.update", allowed)
			if !reflect.DeepEqual(committed, result(t, retry)) || retry.value["_meta"].(map[string]any)["replayed"] != true {
				t.Fatalf("guarded adapter changed permitted outcome/replay: %s", retry.body)
			}
		})
	}
}

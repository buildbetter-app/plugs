package oidcpermissions_test

import (
	"bytes"
	"context"
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
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/permissions"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authnoidc"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type observedService struct {
	rpc.Service
	sessionProof         atomic.Int64
	apiKeyNonInteractive atomic.Int64
}

func (s *observedService) UpdateWithOutcome(ctx context.Context, by model.PrincipalRef, req service.UpdateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	switch err := authn.RequireInteractiveUser(ctx, by); {
	case err == nil:
		s.sessionProof.Add(1)
	case errors.Is(err, authn.ErrHumanPresenceRequired):
		s.apiKeyNonInteractive.Add(1)
	}
	return s.Service.UpdateWithOutcome(ctx, by, req, meta)
}

type fixture struct {
	idp      *mockIDP
	provider *authnoidc.Provider
	auth     *authn.Authenticator
	keys     *authnapikey.Provider
	store    *memory.Store
	lease    driver.ActiveLease
	facts    *authzrpc.MutableFacts
	base     authzfixed.Facts
	service  *observedService
	url      string
	client   *http.Client
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	idp := newIDP(t)
	provider, err := authnoidc.New(idp.config())
	if err != nil {
		t.Fatal("could not configure OIDC provider")
	}
	store := memory.New()
	lease, err := store.Lease(ctx, "oidc-permissions", driver.LeaseTTL)
	if err != nil {
		t.Fatal("could not acquire fixture lease")
	}
	f := &fixture{idp: idp, provider: provider, store: store, lease: lease}
	t.Cleanup(func() {
		if err := lease.Release(ctx); err != nil {
			t.Error("fixture lease did not release")
		}
		if err := store.Close(); err != nil {
			t.Error("fixture store did not close")
		}
	})

	// Seed ordinary beads before installing the fixed authorization chain. This
	// is the trusted fixture setup boundary; no request can use it as a bypass.
	otype := service.NewOntology(nil)
	seed := pipeline.New(store, lease, pipeline.WithOntology(otype))
	domain := service.New(store, lease, seed, service.WithOntology(otype))
	for i := 0; i < 3; i++ {
		_, err := domain.Create(ctx, model.PrincipalRef{Kind: "user", ID: "user:seed"}, service.CreateRequest{
			Type: "issue", Fields: map[string]any{"title": fmt.Sprintf("seed-%d", i), "priority": i},
		}, model.WriteMeta{Project: "project", Agent: "seed", AgentLabel: "seed", IdempotencyKey: fmt.Sprintf("%032x", i+1), Method: "bead.create", Return: "full"})
		if err != nil {
			t.Fatal("could not seed the real service")
		}
	}

	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal("could not read fixture metadata")
	}
	ns := authz.Namespace{
		StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed",
		Authority: model.RefByID("authority", model.Natural64(0)),
	}
	project := model.RefByID("project", model.Natural64(0))
	team := model.RefByID("team", model.Natural64(0))
	f.base = authzfixed.Facts{
		Namespace: ns,
		Projects:  []authzfixed.Project{{Ref: project, InstallationID: "fixed", Name: "project", Active: true}},
		Teams:     []authzfixed.Team{{Ref: team, InstallationID: "fixed", Active: true}},
		Grants: []authz.Grant{
			{Subject: authz.SubjectRef{Kind: "user", ID: "user:operator"}, Scope: authz.Scope{Kind: authz.TeamScope, Ref: team}, Role: "member"},
			{Subject: authz.SubjectRef{Kind: "team", ID: "team"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: project}, Role: "involved"},
		},
	}
	facts, err := authzrpc.NewMutableFacts(f.base)
	if err != nil {
		t.Fatal("could not configure fixed permission facts")
	}
	f.facts = facts
	f.keys = authnapikey.New(authnapikey.Config{Now: idp.clock, AuthorizationInstalled: func() bool { return true }})
	permissionComposition, err := permissions.New(ctx, permissions.Config{
		Store: store, Lease: lease, Keys: f.keys, Facts: facts, Namespace: ns, DefaultProject: "project",
	})
	if err != nil {
		t.Fatal("could not compose the admitted fixed permission adapter")
	}

	// Rebind the actual GuardedService and Checker to an HTTP server whose
	// transport authenticator admits core OIDC sessions. The dependency's own
	// API-key handler is not used as a stand-in: these requests traverse the
	// same service, checker, writer, RPC registry and HTTP framing with the
	// live session authenticator below.
	f.service = &observedService{Service: permissionComposition.Service}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal("could not create the RPC registry")
	}
	if err := rpc.BindService(registry, f.service); err != nil {
		t.Fatal("could not bind the guarded service")
	}
	server, err := rpc.New(registry, rpc.Config{
		Incarnation: meta.Incarnation, DefaultProject: "project", AuthMode: "authenticated",
		OntologyRevision: model.Natural64(1), Visible: permissionComposition.Checker.Visible,
	})
	if err != nil {
		t.Fatal("could not create the RPC server")
	}
	f.auth, err = authn.New(authn.Config{
		Provider: f.keys, CoreSessions: provider,
		AuthorizationInstalled: func() bool { return true },
	})
	if err != nil {
		t.Fatal("could not configure the combined authenticator")
	}
	handler, err := httpjson.New(server, f.auth, httpjson.Config{})
	if err != nil {
		t.Fatal("could not create the HTTP JSON-RPC handler")
	}
	carried, err := authnoidc.CarryIdentity(f.auth, handler)
	if err != nil {
		t.Fatal("could not carry the live OIDC identity")
	}
	listener, err := f.auth.Listen(ctx, "127.0.0.1:0", idp.server.TLS)
	if err != nil {
		t.Fatal("could not listen on the disposable TLS endpoint")
	}
	httpServer := &http.Server{Handler: carried, ReadHeaderTimeout: time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()
	f.url = "https://" + listener.Addr().String()
	f.client = idp.server.Client()
	f.client.Timeout = 5 * time.Second
	t.Cleanup(func() {
		f.client.CloseIdleConnections()
		if err := httpServer.Close(); err != nil {
			t.Error("OIDC permissions HTTP server did not close")
		}
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Error("OIDC permissions HTTP server returned an unexpected error")
			}
		case <-time.After(3 * time.Second):
			t.Error("OIDC permissions HTTP server did not stop")
		}
	})
	return f
}

func (f *fixture) issue(t *testing.T, principal model.PrincipalRef) authnapikey.IssuedKey {
	t.Helper()
	key, err := f.keys.Issue(principal, f.idp.clock().Add(time.Hour))
	if err != nil {
		t.Fatal("could not provision the fixture API key")
	}
	return key
}

func (f *fixture) issueAgent(t *testing.T) authnapikey.IssuedKey {
	t.Helper()
	return f.issue(t, model.PrincipalRef{
		Kind: "agent", ID: "agent:worker",
		Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"},
		Caveats: map[string]any{
			"actions":  []string{"modify"},
			"projects": []model.Ref{model.RefByID("project", model.Natural64(0))},
		},
	})
}

func (f *fixture) replaceFacts(t *testing.T, mutate func(*authzfixed.Facts)) {
	t.Helper()
	next := f.base
	next.Projects = append([]authzfixed.Project(nil), f.base.Projects...)
	next.Teams = append([]authzfixed.Team(nil), f.base.Teams...)
	next.Grants = append([]authz.Grant(nil), f.base.Grants...)
	mutate(&next)
	if err := f.facts.Replace(next); err != nil {
		t.Fatal("could not replace fixed permission facts")
	}
}

type reply struct {
	status int
	body   []byte
	value  map[string]any
}

func (f *fixture) call(t *testing.T, token, method string, params map[string]any) reply {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal("could not encode RPC request")
	}
	r, err := http.NewRequest(http.MethodPost, f.url+httpjson.Path, bytes.NewReader(data))
	if err != nil {
		t.Fatal("could not construct HTTP request")
	}
	r.Header.Set("Plug-RPC", "1")
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := f.client.Do(r)
	if err != nil {
		t.Fatal("actual TLS HTTP request failed")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal("could not read HTTP response")
	}
	value := map[string]any{}
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatal("HTTP response was not a JSON-RPC object")
	}
	return reply{status: response.StatusCode, body: body, value: value}
}

func result(t *testing.T, r reply) map[string]any {
	t.Helper()
	value, ok := r.value["result"].(map[string]any)
	if r.status != http.StatusOK || r.value["error"] != nil || !ok {
		t.Fatalf("expected a successful JSON-RPC result (HTTP %d)", r.status)
	}
	return value
}

func code(t *testing.T, r reply, status int, want string) {
	t.Helper()
	errorValue, ok := r.value["error"].(map[string]any)
	data, dataOK := errorValue["data"].(map[string]any)
	if r.status != status || !ok || !dataOK || data["code"] != want || r.value["result"] != nil {
		t.Fatalf("expected JSON-RPC error %q at HTTP %d", want, status)
	}
}

func updateParams(id string, n int) map[string]any {
	return map[string]any{
		"id":     map[string]any{"id": id},
		"fields": map[string]any{"title": fmt.Sprintf("OIDC permission update %d", n)},
		"_meta":  map[string]any{"idempotencyKey": fmt.Sprintf("%032x", n), "agent": "oidc-permissions"},
	}
}

func beadID(t *testing.T, page map[string]any) string {
	t.Helper()
	items, ok := page["items"].([]any)
	if !ok || len(items) == 0 {
		t.Fatal("authorized User list was empty")
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatal("authorized User list item was malformed")
	}
	id, ok := item["id"].(string)
	if !ok || id == "" {
		t.Fatal("authorized User list item had no id")
	}
	return id
}

func assertCredentialFree(t *testing.T, body []byte, values ...string) {
	t.Helper()
	for _, value := range values {
		if value != "" && bytes.Contains(body, []byte(value)) {
			t.Fatal("HTTP diagnostics exposed credential material")
		}
	}
}

func TestVerifiedOIDCSessionRunsFixedPermissionsOverRealHTTP(t *testing.T) {
	f := newFixture(t)
	session := login(t, f.idp, f.provider)
	page := result(t, f.call(t, session.Token, "bead.ready", map[string]any{"limit": 10, "_meta": map[string]any{"project": "project"}}))
	id := beadID(t, page)
	got := result(t, f.call(t, session.Token, "bead.get", map[string]any{"id": map[string]any{"id": id}}))
	if got["bead"].(map[string]any)["id"] != id {
		t.Fatal("verified OIDC session did not read through the fixed service")
	}

	// The live OIDC User proof reaches the service, but the fixed CP7 slice
	// reserves modification of existing beads for an assigned Agent.
	code(t, f.call(t, session.Token, "bead.update", updateParams(id, 1)), http.StatusOK, "forbidden")
	if f.service.sessionProof.Load() != 1 {
		t.Fatal("live OIDC session proof was not preserved to the service boundary")
	}

	agent := f.issueAgent(t)
	result(t, f.call(t, agent.Token, "bead.update", updateParams(id, 2)))
	if f.service.apiKeyNonInteractive.Load() != 1 {
		t.Fatal("ordinary API-key Agent was treated as an interactive session")
	}

	f.replaceFacts(t, func(next *authzfixed.Facts) {
		// Remove the explicit team-to-project edge. The User session remains a
		// valid reader, while the Agent's fixed modification grant is revoked.
		next.Grants = next.Grants[:1]
	})
	metaAfterFacts, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal("could not read metadata after team revocation")
	}
	code(t, f.call(t, agent.Token, "bead.update", updateParams(id, 3)), http.StatusOK, "forbidden")
	metaAfter, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal("could not read metadata after team revocation")
	}
	if metaAfter.Head != metaAfterFacts.Head {
		t.Fatal("denied fixed-permission update advanced the real store")
	}
	// Current User reads are independent of project membership under CP7.
	result(t, f.call(t, session.Token, "bead.get", map[string]any{"id": map[string]any{"id": id}}))

	f.replaceFacts(t, func(next *authzfixed.Facts) {
		next.Projects[0].Active = false
	})
	code(t, f.call(t, agent.Token, "bead.update", updateParams(id, 4)), http.StatusOK, "forbidden")
	if f.service.apiKeyNonInteractive.Load() != 3 {
		t.Fatal("fixed project change did not reach the real authorization service")
	}
}

func TestOIDCLiveSessionRevocationExpiryAndTransportRefusalFailClosed(t *testing.T) {
	f := newFixture(t)
	session := login(t, f.idp, f.provider)
	page := result(t, f.call(t, session.Token, "bead.ready", map[string]any{"limit": 1, "_meta": map[string]any{"project": "project"}}))
	id := beadID(t, page)

	if !f.provider.Revoke(session.ID) {
		t.Fatal("verified session was not revoked")
	}
	revoked := f.call(t, session.Token, "bead.get", map[string]any{"id": map[string]any{"id": id}})
	code(t, revoked, http.StatusUnauthorized, "unauthenticated")
	assertCredentialFree(t, revoked.body, session.Token, session.ID, "user:operator", f.idp.server.URL)

	missing := f.call(t, "", "bead.get", map[string]any{"id": map[string]any{"id": id}})
	code(t, missing, http.StatusUnauthorized, "unauthenticated")
	assertCredentialFree(t, missing.body, "anonymous", "user:operator", f.idp.server.URL)
	if f.service.sessionProof.Load() != 0 || f.service.apiKeyNonInteractive.Load() != 0 {
		t.Fatal("refused authentication reached the guarded service")
	}

	// A fresh flow crosses the exact half-open session expiry boundary. The
	// provider's live session reference and transport admission both recheck it.
	fresh := login(t, f.idp, f.provider)
	f.idp.now.Store(fresh.ExpiresAt.Unix())
	expired := f.call(t, fresh.Token, "bead.get", map[string]any{"id": map[string]any{"id": id}})
	code(t, expired, http.StatusUnauthorized, "token_expired")
	assertCredentialFree(t, expired.body, fresh.Token, fresh.ID, "user:operator", f.idp.server.URL)

	malformed := f.call(t, "not-a-session", "bead.get", map[string]any{"id": map[string]any{"id": id}})
	code(t, malformed, http.StatusUnauthorized, "unauthenticated")
	assertCredentialFree(t, malformed.body, "not-a-session", "user:operator", f.idp.server.URL)
}

func TestAPIKeyUserRemainsNoninteractiveAlongsideOIDC(t *testing.T) {
	f := newFixture(t)
	session := login(t, f.idp, f.provider)
	page := result(t, f.call(t, session.Token, "bead.ready", map[string]any{"limit": 1, "_meta": map[string]any{"project": "project"}}))
	id := beadID(t, page)
	key := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:api-client"})
	got := f.call(t, key.Token, "bead.get", map[string]any{"id": map[string]any{"id": id}})
	result(t, got)
	assertCredentialFree(t, got.body, key.Token)
	code(t, f.call(t, key.Token, "bead.update", updateParams(id, 5)), http.StatusOK, "forbidden")
	if f.service.sessionProof.Load() != 0 || f.service.apiKeyNonInteractive.Load() != 1 {
		t.Fatal("API-key User supplied interactive proof to the fixed service")
	}
	identity, err := f.auth.Admit(context.Background(), key.Token)
	if err != nil || !reflect.DeepEqual(identity.Principal(), model.PrincipalRef{Kind: "user", ID: "user:api-client"}) || !errors.Is(identity.RequireInteractiveUser(), authn.ErrHumanPresenceRequired) {
		t.Fatal("API-key User admission did not remain noninteractive")
	}
}

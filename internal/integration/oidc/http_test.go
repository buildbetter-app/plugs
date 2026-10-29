package oidc_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authnoidc"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type observedDomain struct {
	rpc.Service
	sessionCreates atomic.Int64
	keyCreates     atomic.Int64
}

func (s *observedDomain) CreateWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CreateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	// Observe the real opaque proof at the domain boundary, then execute the
	// ordinary real write. This is not a substitute/fake ontology.apply method.
	err := authn.RequireInteractiveUser(ctx, by)
	if err == nil {
		s.sessionCreates.Add(1)
	} else if errors.Is(err, authn.ErrHumanPresenceRequired) {
		s.keyCreates.Add(1)
	}
	return s.Service.CreateWithOutcome(ctx, by, req, meta)
}

type httpFixture struct {
	provider *authnoidc.Provider
	keys     *authnapikey.Provider
	auth     *authn.Authenticator
	store    *memory.Store
	domain   *observedDomain
	url      string
	client   *http.Client
}

func realHTTP(t *testing.T, idp *mockIDP) *httpFixture {
	t.Helper()
	p, err := authnoidc.New(idp.config())
	if err != nil {
		t.Fatal(err)
	}
	f := &httpFixture{provider: p, keys: authnapikey.New(authnapikey.Config{Now: idp.clock, AuthorizationInstalled: func() bool { return true }}), store: memory.New()}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	lease, err := f.store.Lease(context.Background(), "oidc-http-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, lease, pipeline.WithOntology(ontology))
	f.domain = &observedDomain{Service: service.New(f.store, lease, writer, service.WithOntology(ontology))}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{AuthMode: "authenticated", DefaultProject: "oidc-test", Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	f.auth, err = authn.New(authn.Config{CoreSessions: p, Provider: f.keys})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, f.auth, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	carried, err := authnoidc.CarryIdentity(f.auth, handler)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := f.auth.Listen(context.Background(), "127.0.0.1:0", idp.server.TLS)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: carried, ReadHeaderTimeout: time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()
	f.url, f.client = "https://"+listener.Addr().String(), idp.server.Client()
	f.client.Timeout = 5 * time.Second
	t.Cleanup(func() {
		f.client.CloseIdleConnections()
		if err := httpServer.Close(); err != nil {
			t.Error(err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Error("HTTP listener did not stop cleanly")
		}
	})
	return f
}

type reply struct {
	status int
	value  map[string]any
	raw    []byte
}

func (f *httpFixture) call(t *testing.T, token, method string, params map[string]any, change func(*http.Request)) reply {
	t.Helper()
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequest(http.MethodPost, f.url+httpjson.Path, bytes.NewReader(data))
	if err != nil {
		t.Fatal("could not construct local RPC request")
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Plug-RPC", "1")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if change != nil {
		change(r)
	}
	response, err := f.client.Do(r)
	if err != nil {
		t.Fatal("local admitted HTTP request failed")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal("could not read local response")
	}
	var value map[string]any
	if json.Unmarshal(raw, &value) != nil {
		t.Fatal("response was not JSON-RPC")
	}
	if token != "" && bytes.Contains(raw, []byte(token)) {
		t.Fatal("RPC response exposed bearer credential")
	}
	return reply{status: response.StatusCode, value: value, raw: raw}
}

func writeParams(t *testing.T) map[string]any {
	t.Helper()
	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal("could not generate idempotency key")
	}
	return map[string]any{"type": "issue", "fields": map[string]any{"title": "OIDC authenticated write"}, "_meta": map[string]any{"idempotencyKey": hex.EncodeToString(key[:])}}
}

func result(t *testing.T, r reply) map[string]any {
	t.Helper()
	if r.status != http.StatusOK || r.value["error"] != nil {
		t.Fatal("actual HTTP/RPC/core call failed")
	}
	v, ok := r.value["result"].(map[string]any)
	if !ok {
		t.Fatal("missing RPC result")
	}
	return v
}

func expectCode(t *testing.T, r reply, status int, code string) {
	t.Helper()
	e, _ := r.value["error"].(map[string]any)
	d, _ := e["data"].(map[string]any)
	if r.status != status || d["code"] != code || r.value["result"] != nil {
		t.Fatalf("expected %s at HTTP %d", code, status)
	}
}

func TestRealHTTPPlugSessionAndAPIKeyStampCoreWithoutSharingHumanProof(t *testing.T) {
	idp := newIDP(t)
	f := realHTTP(t, idp)
	session := login(t, idp, f.provider)
	key, err := f.keys.Issue(model.PrincipalRef{Kind: "user", ID: "user:api-client"}, idp.clock().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	created := result(t, f.call(t, session.Token, "bead.create", writeParams(t), nil))
	ref := created["results"].([]any)[0].(map[string]any)["ref"]
	result(t, f.call(t, key.Token, "bead.create", writeParams(t), nil))
	result(t, f.call(t, session.Token, "bead.get", map[string]any{"id": ref}, nil))
	if f.domain.sessionCreates.Load() != 1 || f.domain.keyCreates.Load() != 1 {
		t.Fatal("domain boundary lost live proof or gave API-key User human authority")
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	changes, err := reader.Changesets(context.Background(), nil, 10)
	if err != nil || len(changes) != 2 || changes[0].Principal.ID != "user:nikhil" || changes[1].Principal.ID != "user:api-client" {
		t.Fatal("persistent core changesets did not retain session/API-key identities")
	}
	encoded, err := json.Marshal(changes)
	if err != nil || bytes.Contains(encoded, []byte(session.Token)) || bytes.Contains(encoded, []byte(key.Token)) || bytes.Contains(encoded, []byte(idp.lastJWT)) {
		t.Fatal("core persisted credential material")
	}
	spoof := writeParams(t)
	spoof["_meta"].(map[string]any)["interactive"] = true
	expectCode(t, f.call(t, key.Token, "bead.create", spoof, nil), http.StatusOK, "invalid_params")
	expectCode(t, f.call(t, session.Token, "ontology.apply", map[string]any{}, nil), http.StatusOK, "method_not_found")
}

func TestRealHTTPRefusesRevokedExpiredIssuerAndWrongSessionTokens(t *testing.T) {
	idp := newIDP(t)
	f := realHTTP(t, idp)
	session := login(t, idp, f.provider)
	for _, token := range []string{"", idp.lastJWT, "plg_s_" + session.ID + "_" + strings.Repeat("A", 43)} {
		expectCode(t, f.call(t, token, "bead.create", writeParams(t), nil), http.StatusUnauthorized, "unauthenticated")
	}
	if !f.provider.Revoke(session.ID) {
		t.Fatal("revocation failed")
	}
	expectCode(t, f.call(t, session.Token, "bead.create", writeParams(t), nil), http.StatusUnauthorized, "unauthenticated")
	other := login(t, idp, f.provider)
	idp.now.Store(other.ExpiresAt.Unix())
	expectCode(t, f.call(t, other.Token, "bead.create", writeParams(t), nil), http.StatusUnauthorized, "token_expired")
	meta, err := f.store.Meta(context.Background())
	if err != nil || !meta.Head.IsZero() || f.domain.sessionCreates.Load() != 0 || f.domain.keyCreates.Load() != 0 {
		t.Fatal("refused credentials reached core storage")
	}
}

func TestRealHTTPSessionKeepsOriginHostCookieAndScopedAgentRestrictions(t *testing.T) {
	idp := newIDP(t)
	f := realHTTP(t, idp)
	session := login(t, idp, f.provider)
	for _, tc := range []struct {
		name   string
		change func(*http.Request)
	}{
		{"foreign origin", func(r *http.Request) { r.Header.Set("Origin", "https://other.invalid") }},
		{"sibling origin", func(r *http.Request) { r.Header.Set("Origin", "https://localhost") }},
		{"rebound host", func(r *http.Request) { r.Host = "rebound.invalid" }},
		{"missing marker", func(r *http.Request) { r.Header.Del("Plug-RPC") }},
		{"cookie", func(r *http.Request) { r.Header.Set("Cookie", "plug_session=unsupported") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectCode(t, f.call(t, session.Token, "bead.create", writeParams(t), tc.change), http.StatusForbidden, "forbidden")
		})
	}
	key, err := f.keys.Issue(model.PrincipalRef{Kind: "agent", ID: "agent:scoped", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:nikhil"}}, idp.clock().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	expectCode(t, f.call(t, key.Token, "bead.create", writeParams(t), nil), http.StatusForbidden, "authz_not_installed")
	result(t, f.call(t, session.Token, "bead.create", writeParams(t), func(r *http.Request) { r.Header.Set("Origin", f.url) }))
}

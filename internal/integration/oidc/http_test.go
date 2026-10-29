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
	idp      *mockIDP
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
	config := idp.config()
	config.AllowedOrigins = []string{"https://app.invalid"}
	p, err := authnoidc.New(config)
	if err != nil {
		t.Fatal(err)
	}
	f := &httpFixture{idp: idp, provider: p, keys: authnapikey.New(authnapikey.Config{Now: idp.clock, AuthorizationInstalled: func() bool { return true }}), store: memory.New()}
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
	f.auth, err = authn.New(authn.Config{CoreSessions: p, Provider: f.keys, CrossOriginAuthorizer: p.AllowOrigin})
	if err != nil {
		t.Fatal(err)
	}
	rpcHandler, err := httpjson.New(server, f.auth, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := p.HTTPHandler(rpcHandler)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := f.auth.Listen(context.Background(), "127.0.0.1:0", idp.server.TLS)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
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

type loginEnvelope struct {
	AuthorizationURL string    `json:"authorizationUrl"`
	State            string    `json:"state"`
	Binding          string    `json:"binding"`
	ExpiresAt        time.Time `json:"expiresAt"`
}

type sessionEnvelope struct {
	AccessToken string    `json:"access_token"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func loginOverHTTP(t *testing.T, f *httpFixture, origin string) sessionEnvelope {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, f.url+authnoidc.LoginPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", origin)
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal("HTTP login start failed")
	}
	var login loginEnvelope
	data, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK || json.Unmarshal(data, &login) != nil || login.AuthorizationURL == "" || login.State == "" || login.Binding == "" {
		t.Fatalf("HTTP login start failed: status=%d body=%s", response.StatusCode, data)
	}
	state, code := f.idp.authorize(authnoidc.Login{AuthorizationURL: login.AuthorizationURL, State: login.State, Binding: login.Binding, ExpiresAt: login.ExpiresAt})
	body, err := json.Marshal(map[string]string{"state": state, "binding": login.Binding, "code": code})
	if err != nil {
		t.Fatal(err)
	}
	callback, err := http.NewRequest(http.MethodPost, f.url+authnoidc.CallbackPath, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	callback.Header.Set("Origin", origin)
	callback.Header.Set("Content-Type", "application/json")
	response, err = f.client.Do(callback)
	if err != nil {
		t.Fatal("HTTP login callback failed")
	}
	data, readErr = io.ReadAll(response.Body)
	_ = response.Body.Close()
	var session sessionEnvelope
	if readErr != nil || response.StatusCode != http.StatusOK || json.Unmarshal(data, &session) != nil || session.AccessToken == "" {
		t.Fatalf("HTTP login callback failed: status=%d body=%s", response.StatusCode, data)
	}
	return session
}

type reply struct {
	status  int
	value   map[string]any
	raw     []byte
	headers http.Header
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
	return reply{status: response.StatusCode, value: value, raw: raw, headers: response.Header.Clone()}
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

func expectHTTPError(t *testing.T, response *http.Response, status int, code string) {
	t.Helper()
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	var value map[string]string
	if err != nil || response.StatusCode != status || json.Unmarshal(data, &value) != nil || value["error"] != code {
		t.Fatalf("expected HTTP %d/%s, got %d/%s", status, code, response.StatusCode, data)
	}
}

func TestRealHTTPLoginAdapterIssuesBoundSessionAndCORS(t *testing.T) {
	idp := newIDP(t)
	f := realHTTP(t, idp)
	session := loginOverHTTP(t, f, "https://app.invalid")
	if !session.ExpiresAt.After(idp.clock()) {
		t.Fatal("HTTP login returned an expired session")
	}
	created := f.call(t, session.AccessToken, "bead.create", writeParams(t), func(r *http.Request) {
		r.Header.Set("Origin", "https://app.invalid")
	})
	result(t, created)
	if created.status != http.StatusOK || created.value["error"] != nil {
		t.Fatal("allowlisted OIDC bearer did not reach the real RPC boundary")
	}
	if created.headers.Get("Access-Control-Allow-Origin") != "https://app.invalid" || created.headers.Get("Access-Control-Allow-Credentials") != "" {
		t.Fatal("allowlisted response did not carry non-credentialed CORS")
	}
	if got := created.raw; !bytes.Contains(got, []byte(`"result"`)) {
		t.Fatal("real RPC response was not returned")
	}
}

func TestRealHTTPLoginAdapterPreflightUsesOriginAllowlist(t *testing.T) {
	idp := newIDP(t)
	f := realHTTP(t, idp)
	request, err := http.NewRequest(http.MethodOptions, f.url+httpjson.Path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", "https://app.invalid")
	request.Header.Set("Access-Control-Request-Method", http.MethodPost)
	request.Header.Set("Access-Control-Request-Headers", "Authorization, Content-Type, Plug-RPC")
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNoContent || response.Header.Get("Access-Control-Allow-Origin") != "https://app.invalid" || response.Header.Get("Access-Control-Allow-Credentials") != "" {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("allowlisted preflight was not admitted: status=%d body=%s", response.StatusCode, data)
	}
	response.Body.Close()

	request, err = http.NewRequest(http.MethodOptions, f.url+httpjson.Path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", "https://evil.invalid")
	request.Header.Set("Access-Control-Request-Method", http.MethodPost)
	request.Header.Set("Access-Control-Request-Headers", "Authorization")
	response, err = f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	expectHTTPError(t, response, http.StatusForbidden, "forbidden")
}

func TestRealHTTPLoginAdapterRefusesWrongStateNonceAudienceAndExpiry(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]any, int64)
		state  func(string) string
		want   int
		code   string
	}{
		{name: "wrong state", state: func(state string) string { return strings.Repeat("A", len(state)) }, want: http.StatusUnauthorized, code: "unauthenticated"},
		{name: "wrong nonce", mutate: func(claims map[string]any, _ int64) { claims["nonce"] = "wrong-nonce" }, want: http.StatusUnauthorized, code: "unauthenticated"},
		{name: "wrong audience", mutate: func(claims map[string]any, _ int64) { claims["aud"] = "other-client" }, want: http.StatusUnauthorized, code: "unauthenticated"},
		{name: "expired token", mutate: func(claims map[string]any, now int64) {
			claims["exp"] = now
			claims["iat"] = now - 1
			claims["auth_time"] = now - 1
		}, want: http.StatusUnauthorized, code: "token_expired"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idp := newIDP(t)
			f := realHTTP(t, idp)
			if tc.mutate != nil {
				idp.mu.Lock()
				idp.mutate = func(claims map[string]any) { tc.mutate(claims, idp.clock().Unix()) }
				idp.mu.Unlock()
			}
			request, err := http.NewRequest(http.MethodGet, f.url+authnoidc.LoginPath, nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Origin", "https://app.invalid")
			response, err := f.client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			var login loginEnvelope
			data, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode != http.StatusOK || json.Unmarshal(data, &login) != nil {
				t.Fatalf("login start failed: status=%d body=%s", response.StatusCode, data)
			}
			state, code := f.idp.authorize(authnoidc.Login{AuthorizationURL: login.AuthorizationURL, State: login.State, Binding: login.Binding, ExpiresAt: login.ExpiresAt})
			if tc.state != nil {
				state = tc.state(state)
			}
			body, err := json.Marshal(map[string]string{"state": state, "binding": login.Binding, "code": code})
			if err != nil {
				t.Fatal(err)
			}
			callback, err := http.NewRequest(http.MethodPost, f.url+authnoidc.CallbackPath, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			callback.Header.Set("Origin", "https://app.invalid")
			callback.Header.Set("Content-Type", "application/json")
			response, err = f.client.Do(callback)
			if err != nil {
				t.Fatal(err)
			}
			expectHTTPError(t, response, tc.want, tc.code)
		})
	}
}

func TestRealHTTPLoginAdapterRefusesDisallowedOriginsAndAPIKeys(t *testing.T) {
	idp := newIDP(t)
	f := realHTTP(t, idp)
	request, err := http.NewRequest(http.MethodGet, f.url+authnoidc.LoginPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", "https://evil.invalid")
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	expectHTTPError(t, response, http.StatusForbidden, "forbidden")

	session := loginOverHTTP(t, f, "https://app.invalid")
	foreign := f.call(t, session.AccessToken, "bead.create", writeParams(t), func(r *http.Request) {
		r.Header.Set("Origin", "https://evil.invalid")
	})
	expectCode(t, foreign, http.StatusForbidden, "forbidden")
	key, err := f.keys.Issue(model.PrincipalRef{Kind: "user", ID: "user:api-client"}, idp.clock().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	apiKey := f.call(t, key.Token, "bead.create", writeParams(t), func(r *http.Request) {
		r.Header.Set("Origin", "https://app.invalid")
	})
	expectCode(t, apiKey, http.StatusForbidden, "forbidden")
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

func TestRealHTTPSessionReconnectsWithSameCredentialUntilExpiry(t *testing.T) {
	idp := newIDP(t)
	f := realHTTP(t, idp)
	session := login(t, idp, f.provider)
	first := result(t, f.call(t, session.Token, "bead.create", writeParams(t), nil))
	ref := first["results"].([]any)[0].(map[string]any)["ref"]

	// A reconnect is a fresh HTTP client using the same opaque Plug session;
	// it must re-admit the same User and read the existing durable state.
	reconnected := &http.Client{Transport: f.client.Transport, Timeout: f.client.Timeout}
	defer reconnected.CloseIdleConnections()
	params := map[string]any{"jsonrpc": "2.0", "id": 2, "method": "bead.get", "params": map[string]any{"id": ref}}
	payload, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, f.url+httpjson.Path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Authorization", "Bearer "+session.Token)
	response, err := reconnected.Do(req)
	if err != nil {
		t.Fatal("reconnected HTTP request failed", err)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("reconnected request status=%d body=%s", response.StatusCode, data)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil || value["error"] != nil {
		t.Fatalf("reconnected session was not admitted: %s", data)
	}
	if got := value["result"].(map[string]any)["bead"].(map[string]any)["id"]; got != ref.(map[string]any)["id"] {
		t.Fatalf("reconnected request read a different bead: %v", got)
	}

	// The same reconnect credential fails closed at the half-open expiry
	// boundary; it must not fall back to anonymous or reach the writer.
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	idp.now.Store(session.ExpiresAt.Unix())
	expired := f.call(t, session.Token, "bead.create", writeParams(t), nil)
	expectCode(t, expired, http.StatusUnauthorized, "token_expired")
	after, err := f.store.Meta(context.Background())
	if err != nil || after.Head != meta.Head || f.domain.sessionCreates.Load() != 1 {
		t.Fatalf("expired reconnect changed core state: before=%s after=%s sessionCreates=%d", meta.Head, after.Head, f.domain.sessionCreates.Load())
	}
}

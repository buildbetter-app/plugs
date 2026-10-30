package oidc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authnoidc"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// Observe transported proof at the real storage boundary without replacing
// any canonical RPC method, service, writer or identity provider.
type mcpSessionProofStore struct {
	driver.Store
	interactiveReads atomic.Int64
}

func (s *mcpSessionProofStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	for _, id := range []string{"user:nikhil", "user:api-client"} {
		if authn.RequireInteractiveUser(ctx, model.PrincipalRef{Kind: "user", ID: id}) == nil {
			s.interactiveReads.Add(1)
		}
	}
	return s.Store.Read(ctx, view)
}

func compositionMCP(t *testing.T, f *httpFixture, token, session, method string, params map[string]any) (reply, http.Header) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, f.url+mcp.Path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal("could not construct composed MCP request")
	}
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if session != "" {
		req.Header.Set("MCP-Session-Id", session)
	}
	response, err := f.client.Do(req)
	if err != nil {
		t.Fatal("local composed MCP request failed")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal("could not read composed MCP response")
	}
	var value map[string]any
	if json.Unmarshal(raw, &value) != nil {
		t.Fatal("composed MCP response was not JSON")
	}
	if token != "" && bytes.Contains(raw, []byte(token)) {
		t.Fatal("composed MCP response exposed a bearer credential")
	}
	return reply{status: response.StatusCode, value: value, raw: raw}, response.Header
}

func compositionMCPTool(t *testing.T, f *httpFixture, token, session, method string, params map[string]any) map[string]any {
	t.Helper()
	reply, _ := compositionMCP(t, f, token, session, "tools/call", map[string]any{
		"name": "plug_call", "arguments": map[string]any{"method": method, "params": params},
	})
	tool := result(t, reply)
	if tool["isError"] == true {
		t.Fatal("composed MCP canonical call failed")
	}
	content, ok := tool["content"].([]any)
	if !ok || len(content) == 0 {
		t.Fatal("composed MCP omitted canonical result")
	}
	text, _ := content[0].(map[string]any)["text"].(string)
	var value map[string]any
	if json.Unmarshal([]byte(text), &value) != nil {
		t.Fatal("composed MCP result was not canonical JSON")
	}
	return value
}

func TestComposedMCPAndRPCShareOIDCProofPrincipalAndRefusalRules(t *testing.T) {
	idp := newIDP(t)
	provider, err := authnoidc.New(idp.config())
	if err != nil {
		t.Fatal(err)
	}
	keys := authnapikey.New(authnapikey.Config{Now: idp.clock, AuthorizationInstalled: func() bool { return true }})
	var observed *mcpSessionProofStore
	runtime, err := composition.Open(context.Background(), composition.Config{
		Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "mcp-auth.db"), Project: "mcp-auth",
		Address: composition.DefaultAddress, AuthMode: "authenticated",
		Auth: authn.Config{Provider: keys, CoreSessions: provider},
		StoreFactory: func(ctx context.Context, _ string, config driver.Config) (driver.Store, error) {
			store, err := (sqlite.Driver{}).Open(ctx, config)
			if err != nil {
				return nil, err
			}
			observed = &mcpSessionProofStore{Store: store}
			return observed, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	server, err := runtime.Listen(context.Background(), composition.DefaultAddress, idp.server.TLS)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	client := *idp.server.Client()
	client.Timeout = 3 * time.Second
	f := &httpFixture{provider: provider, keys: keys, client: &client, url: "https://" + server.Addr().String()}
	t.Cleanup(func() { client.CloseIdleConnections() })

	session := login(t, idp, provider)
	key, err := keys.Issue(model.PrincipalRef{Kind: "user", ID: "user:api-client"}, idp.clock().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var firstSession string
	for _, credential := range []struct {
		name, token string
		interactive bool
	}{{"OIDC", session.Token, true}, {"API key", key.Token, false}} {
		t.Run(credential.name, func(t *testing.T) {
			init, headers := compositionMCP(t, f, credential.token, "", "initialize", map[string]any{
				"protocolVersion": mcp.DefaultProtocolVersion, "capabilities": map[string]any{},
				"clientInfo": map[string]any{"name": "composition-auth-test", "version": "1"},
			})
			result(t, init)
			mcpSession := headers.Get("MCP-Session-Id")
			if mcpSession == "" {
				t.Fatal("composed MCP did not issue a session")
			}
			if credential.interactive {
				firstSession = mcpSession
			}
			before := observed.interactiveReads.Load()
			created := result(t, f.call(t, credential.token, "bead.create", writeParams(t), nil))
			proofs := []int64{before, observed.interactiveReads.Load()}
			ref := created["results"].([]any)[0].(map[string]any)["ref"]
			compositionMCPTool(t, f, credential.token, mcpSession, "bead.get", map[string]any{"id": ref})
			proofs = append(proofs, observed.interactiveReads.Load())
			created = compositionMCPTool(t, f, credential.token, mcpSession, "bead.create", writeParams(t))
			proofs = append(proofs, observed.interactiveReads.Load())
			ref = created["results"].([]any)[0].(map[string]any)["ref"]
			result(t, f.call(t, credential.token, "bead.get", map[string]any{"id": ref}, nil))
			after := observed.interactiveReads.Load()
			proofs = append(proofs, after)
			for i := 1; i < len(proofs); i++ {
				if credential.interactive && proofs[i] == proofs[i-1] {
					t.Fatal("composed transport lost interactive core-session proof")
				}
			}
			if !credential.interactive && after != before {
				t.Fatal("composed transport gave an API key interactive authority")
			}
		})
	}
	foreign, _ := compositionMCP(t, f, key.Token, firstSession, "ping", map[string]any{})
	expectCode(t, foreign, http.StatusNotFound, "not_found")
	meta, err := runtime.Store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := runtime.Store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := reader.Changesets(context.Background(), nil, 10)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || len(changes) != 4 {
		t.Fatal("composed transports did not share durable changesets")
	}
	for i, want := range []string{"user:nikhil", "user:nikhil", "user:api-client", "user:api-client"} {
		if changes[i].Principal.Kind != "user" || changes[i].Principal.ID != want {
			t.Fatal("composed transport stamped the wrong principal")
		}
	}
	encoded, err := json.Marshal(changes)
	if err != nil || bytes.Contains(encoded, []byte(session.Token)) || bytes.Contains(encoded, []byte(key.Token)) || bytes.Contains(encoded, []byte(idp.lastJWT)) {
		t.Fatal("composed core persisted bearer material")
	}
	if !provider.Revoke(session.ID) {
		t.Fatal("could not revoke composed core session")
	}
	expiring := login(t, idp, provider)
	scoped, err := keys.Issue(model.PrincipalRef{Kind: "agent", ID: "agent:scoped", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:nikhil"}}, idp.clock().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	idp.now.Store(expiring.ExpiresAt.Unix())
	for _, refusal := range []struct {
		name, token, code string
		status            int
	}{
		{"missing", "", "unauthenticated", http.StatusUnauthorized},
		{"unknown", "unknown-bearer", "unauthenticated", http.StatusUnauthorized},
		{"issuer token", idp.lastJWT, "unauthenticated", http.StatusUnauthorized},
		{"revoked", session.Token, "unauthenticated", http.StatusUnauthorized},
		{"expired", expiring.Token, "token_expired", http.StatusUnauthorized},
		{"scoped without authorizer", scoped.Token, "authz_not_installed", http.StatusForbidden},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			expectCode(t, f.call(t, refusal.token, "bead.create", writeParams(t), nil), refusal.status, refusal.code)
			// Even a previously issued MCP session never replaces fresh bearer
			// admission after revocation, expiry or a credential change.
			reply, _ := compositionMCP(t, f, refusal.token, firstSession, "tools/call", map[string]any{
				"name": "bead_create", "arguments": writeParams(t),
			})
			code := refusal.code
			if code == "authz_not_installed" {
				// The existing adapter conservatively maps this host admission
				// failure to forbidden; both routes still refuse before dispatch.
				code = "forbidden"
			}
			expectCode(t, reply, refusal.status, code)
		})
	}
	after, err := runtime.Store.Meta(context.Background())
	if err != nil || after.Head != meta.Head {
		t.Fatal("refused composed calls advanced durable state")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			t.Fatal("composed authenticated listener did not stop")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("composed authenticated listener retained work")
	}
}

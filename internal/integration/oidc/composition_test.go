package oidc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
)

type composedFixture struct {
	runtime  *composition.Runtime
	server   *httpjson.Server
	done     chan error
	endpoint string
	client   *http.Client
}

func startComposed(t *testing.T, config composition.Config) *composedFixture {
	t.Helper()
	runtime, err := composition.Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	server, err := runtime.Listen(context.Background(), config.Address, nil)
	if err != nil {
		_ = runtime.Close(context.Background())
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	f := &composedFixture{
		runtime: runtime, server: server, done: done,
		endpoint: "http://" + server.Addr().String(), client: &http.Client{Timeout: 5 * time.Second},
	}
	t.Cleanup(func() { f.stop(t) })
	return f
}

func (f *composedFixture) stop(t *testing.T) {
	t.Helper()
	if f.runtime == nil {
		return
	}
	f.client.CloseIdleConnections()
	if err := f.runtime.Close(context.Background()); err != nil {
		t.Error(err)
	}
	select {
	case err := <-f.done:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	case <-time.After(3 * time.Second):
		t.Error("composed HTTP server did not stop")
	}
	f.runtime = nil
}

func (f *composedFixture) call(t *testing.T, token, method string, params map[string]any) reply {
	t.Helper()
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, f.endpoint+httpjson.Path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Plug-RPC", "1")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("response=%s: %v", raw, err)
	}
	return reply{status: response.StatusCode, value: value, raw: raw}
}

func composedConfig(path string, idp *mockIDP, keys *authnapikey.Provider) composition.Config {
	oidc := idp.config()
	return composition.Config{
		Driver: "sqlite", DSN: path, Project: "composition", Address: composition.DefaultAddress,
		AuthMode: "authenticated", Auth: authn.Config{Provider: keys}, OIDC: &oidc,
	}
}

func TestComposedSQLiteOIDCSessionSurvivesRestartAndRevocation(t *testing.T) {
	idp := newIDP(t)
	path := filepath.Join(t.TempDir(), "composition.db")
	keys := authnapikey.New(authnapikey.Config{Now: idp.clock})
	apiKey, err := keys.Issue(model.PrincipalRef{Kind: "user", ID: "user:api"}, idp.clock().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	first := startComposed(t, composedConfig(path, idp, keys))
	if first.runtime.OIDC == nil {
		t.Fatal("composition did not construct OIDC provider")
	}
	session := login(t, idp, first.runtime.OIDC)
	created := result(t, first.call(t, session.Token, "bead.create", writeParams(t)))
	ref := created["results"].([]any)[0].(map[string]any)["ref"]
	apiIdentity, err := first.runtime.Authenticator.Admit(context.Background(), apiKey.Token)
	if err != nil {
		t.Fatalf("API-key credential was not admitted: %v", err)
	}
	if !errors.Is(apiIdentity.RequireInteractiveUser(), authn.ErrHumanPresenceRequired) {
		t.Fatal("API-key credential acquired interactive user proof")
	}
	first.stop(t)

	second := startComposed(t, composedConfig(path, idp, keys))
	if _, err := second.runtime.Authenticator.Admit(context.Background(), session.Token); err != nil {
		t.Fatalf("session did not survive composition restart: %v", err)
	}
	result(t, second.call(t, session.Token, "bead.get", map[string]any{"id": ref}))
	retained, err := second.runtime.Authenticator.Admit(context.Background(), session.Token)
	if err != nil {
		t.Fatal(err)
	}
	if !second.runtime.OIDC.Revoke(session.ID) {
		t.Fatal("durable session revocation was not acknowledged")
	}
	if !errors.Is(retained.RequireInteractiveUser(), authn.ErrUnauthenticated) {
		t.Fatal("retained composition identity survived revocation")
	}
	second.stop(t)

	third := startComposed(t, composedConfig(path, idp, keys))
	expectCode(t, third.call(t, session.Token, "bead.get", map[string]any{"id": ref}), http.StatusUnauthorized, "unauthenticated")
	expiring := login(t, idp, third.runtime.OIDC)
	idp.now.Store(expiring.ExpiresAt.Unix())
	expectCode(t, third.call(t, expiring.Token, "bead.get", map[string]any{"id": ref}), http.StatusUnauthorized, "token_expired")
}

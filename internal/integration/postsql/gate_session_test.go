package postsql_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
)

type approvalHTTPResponse struct {
	Result port.GateResult `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func approvalHTTP(t *testing.T, f *approvalFixture, authenticator *authn.Authenticator) *httptest.Server {
	t.Helper()
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: f.meta(t).Incarnation, DefaultProject: "post-sql", AuthMode: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, authenticator, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)
	return httpServer
}

func callApprovalHTTP(t *testing.T, server *httptest.Server, token string, ref model.Ref, m model.WriteMeta) (int, approvalHTTPResponse) {
	t.Helper()
	body := encoded(t, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "gate.approve", "params": map[string]any{
		"id": ref, "_meta": map[string]any{"project": m.Project, "idempotencyKey": m.IdempotencyKey},
	}})
	req, err := http.NewRequest(http.MethodPost, server.URL+httpjson.Path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	response, err := client.Do(req)
	if err != nil {
		t.Fatal("approval HTTP round trip failed")
	}
	defer response.Body.Close()
	var out approvalHTTPResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&out); err != nil {
		t.Fatal("invalid approval HTTP response")
	}
	return response.StatusCode, out
}

func TestApprovalGateCredentialsAndLiveSessionRefusalsAcrossDrivers(t *testing.T) {
	approvalEngines(t, func(t *testing.T, e engine) {
		f := newApprovalFixture(t, e)
		ref, _ := f.createGate(t, false)
		idp := newApprovalIDP(t)
		provider := idp.provider(t, f.store)
		keys := authnapikey.New(authnapikey.Config{Now: idp.clock})
		newAuthenticator := func() *authn.Authenticator {
			a, err := authn.New(authn.Config{Provider: keys, CoreSessions: provider})
			if err != nil {
				t.Fatal(err)
			}
			return a
		}
		authenticator := newAuthenticator()
		server := approvalHTTP(t, f, authenticator)
		issue := func() authnapikey.IssuedKey {
			key, err := keys.Issue(approvalUser("alice"), idp.clock().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			return key
		}
		alice, aliceOther := issue(), issue()
		identity, err := authenticator.Admit(f.ctx, alice.Token)
		if err != nil || !errors.Is(identity.RequireInteractiveUser(), authn.ErrHumanPresenceRequired) {
			t.Fatal("ordinary API key acquired D32 interactive authority")
		}
		status, first := callApprovalHTTP(t, server, alice.Token, ref, approvalMeta(1041))
		if status != http.StatusOK || first.Error != nil {
			t.Fatal("valid User API key could not approve the ordinary gate")
		}
		assertApproval(t, port.GateOutcome{Result: first.Result}, "pending", 1)
		f.approvalEvents(t, port.GateOutcome{Result: first.Result}, model.EventGateApproved)
		before := f.state(t, ref)
		status, duplicate := callApprovalHTTP(t, server, aliceOther.Token, ref, approvalMeta(1042))
		if status != http.StatusOK || duplicate.Error != nil {
			t.Fatal("second token of the same User was refused")
		}
		assertApproval(t, port.GateOutcome{Result: duplicate.Result}, "pending", 1)
		same(t, before, f.state(t, ref))

		// Real PKCE logins issue opaque sessions through the production provider.
		// SQL uses this same driver's CoreSessionStore; memory uses the provider's
		// supported process-local sessions and claims no session durability.
		revoked := idp.login(t, provider)
		liveRevoked, err := authenticator.Admit(f.ctx, revoked.Token)
		if err != nil || liveRevoked.RequireInteractiveUser() != nil {
			t.Fatal("fresh login was not interactive")
		}
		if !provider.Revoke(revoked.ID) {
			t.Fatal("session revocation failed")
		}
		if !errors.Is(liveRevoked.RequireInteractiveUser(), authn.ErrUnauthenticated) {
			t.Fatal("retained identity survived revocation")
		}
		expired := idp.login(t, provider)
		liveExpired, err := authenticator.Admit(f.ctx, expired.Token)
		if err != nil {
			t.Fatal("fresh expiry-control login was refused")
		}
		idp.now.Store(expired.ExpiresAt.Unix())
		if !errors.Is(liveExpired.RequireInteractiveUser(), authn.ErrTokenExpired) {
			t.Fatal("retained identity survived half-open expiry")
		}
		if e.name != "memory" {
			server.Close()
			f.reopen(t)
			f.bind()
			provider = idp.provider(t, f.store)
			authenticator = newAuthenticator()
			server = approvalHTTP(t, f, authenticator)
		}
		for i, tc := range []struct {
			name, token string
			code        model.ErrorCode
		}{
			{"revoked-session", revoked.Token, "unauthenticated"},
			{"expired-session", expired.Token, "token_expired"},
			{"missing-credential", "", "unauthenticated"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				m := approvalMeta(1043 + i)
				status, out := callApprovalHTTP(t, server, tc.token, ref, m)
				if status != http.StatusUnauthorized || out.Error == nil || out.Error.Data.Code != tc.code {
					t.Fatalf("session admission status=%d want code=%s", status, tc.code)
				}
				same(t, before, f.state(t, ref))
				f.noApprovalReceipt(t, approvalUser("bob"), m)
			})
		}
		fresh := idp.login(t, provider)
		status, second := callApprovalHTTP(t, server, fresh.Token, ref, approvalMeta(1046))
		if status != http.StatusOK || second.Error != nil {
			t.Fatal("fresh distinct-human session could not resolve the gate")
		}
		assertApproval(t, port.GateOutcome{Result: second.Result}, "resolved", 2)
		f.approvalEvents(t, port.GateOutcome{Result: second.Result}, model.EventBeadClosed, model.EventGateApproved)
		entry := f.approvalReceipt(t, approvalUser("bob"), approvalMeta(1046))
		if entry == nil {
			t.Fatal("valid session resolution left no receipt")
		}
		same(t, second.Result.Commit, entry.Commit)
		resolved := f.state(t, ref)
		status, replay := callApprovalHTTP(t, server, fresh.Token, ref, approvalMeta(1046))
		if status != http.StatusOK || replay.Error != nil || !replay.Meta.Replayed {
			t.Fatal("valid session could not replay terminal receipt")
		}
		same(t, second.Result, replay.Result)
		if !provider.Revoke(fresh.ID) {
			t.Fatal("terminal session revocation failed")
		}
		status, deniedReplay := callApprovalHTTP(t, server, fresh.Token, ref, approvalMeta(1046))
		if status != http.StatusUnauthorized || deniedReplay.Error == nil || deniedReplay.Error.Data.Code != "unauthenticated" {
			t.Fatal("revoked session replay bypassed admission")
		}
		if !keys.Revoke(alice.ID) {
			t.Fatal("API key revocation failed")
		}
		status, deniedKey := callApprovalHTTP(t, server, alice.Token, ref, approvalMeta(1041))
		if status != http.StatusUnauthorized || deniedKey.Error == nil || deniedKey.Error.Data.Code != "unauthenticated" {
			t.Fatal("revoked API key replay bypassed admission")
		}
		same(t, resolved, f.state(t, ref))
	})
}

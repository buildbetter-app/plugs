package oidc_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/authn"
)

func TestPrivateBindingStateAndReplayAreEnforcedBeforeExchange(t *testing.T) {
	idp := newIDP(t)
	p, _ := configured(t, idp.config())
	first, err := p.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	url1, _ := url.Parse(first.AuthorizationURL)
	url2, _ := url.Parse(second.AuthorizationURL)
	seen := map[string]bool{}
	for _, secret := range []string{url1.Query().Get("state"), url1.Query().Get("nonce"), url1.Query().Get("code_challenge"), first.Binding,
		url2.Query().Get("state"), url2.Query().Get("nonce"), url2.Query().Get("code_challenge"), second.Binding} {
		if len(secret) != 43 || seen[secret] {
			t.Fatal("flow secrets were not independent high-entropy values")
		}
		seen[secret] = true
	}
	state, code := idp.authorize(first)
	for _, input := range [][3]string{
		{state, second.Binding, code}, {url2.Query().Get("state"), first.Binding, code}, {"bad-state", first.Binding, code},
	} {
		if _, err := p.Complete(context.Background(), input[0], input[1], input[2]); !errors.Is(err, authn.ErrUnauthenticated) {
			t.Fatal("unbound callback was accepted")
		}
	}
	if idp.tokens != 0 {
		t.Fatal("unbound callback performed token egress")
	}
	if _, err := p.Complete(context.Background(), state, first.Binding, code); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Complete(context.Background(), state, first.Binding, code); !errors.Is(err, authn.ErrUnauthenticated) {
		t.Fatal("consumed flow was replayed")
	}
	if idp.tokens != 1 {
		t.Fatal("consumed flow repeated token egress")
	}
}

func TestConcurrentCallbackConsumesFlowOnce(t *testing.T) {
	idp := newIDP(t)
	p, _ := configured(t, idp.config())
	flow, err := p.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state, code := idp.authorize(flow)
	var successes atomic.Int64
	var workers sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		workers.Go(func() {
			<-start
			if _, err := p.Complete(context.Background(), state, flow.Binding, code); err == nil {
				successes.Add(1)
			}
		})
	}
	close(start)
	workers.Wait()
	if successes.Load() != 1 || idp.tokens != 1 {
		t.Fatal("concurrent callback minted multiple sessions")
	}
}

func TestBadPKCECodeConsumesFlowAndDoesNotMintSession(t *testing.T) {
	idp := newIDP(t)
	p, _ := configured(t, idp.config())
	flow, err := p.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state, code := idp.authorize(flow)
	idp.mu.Lock()
	idp.codes[code].Set("code_challenge", strings.Repeat("A", 43))
	idp.mu.Unlock()
	if _, err := p.Complete(context.Background(), state, flow.Binding, code); !errors.Is(err, authn.ErrUnauthenticated) {
		t.Fatal("mock IdP did not refuse the mismatched actual PKCE verifier")
	}
	if _, err := p.Complete(context.Background(), state, flow.Binding, code); !errors.Is(err, authn.ErrUnauthenticated) || idp.tokens != 1 {
		t.Fatal("failed exchange left flow replayable")
	}
}

func TestSignedIDTokenClaimAndKeyRefusals(t *testing.T) {
	idp := newIDP(t)
	wrongKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal("could not generate disposable second key")
	}
	now := idp.clock().Unix()
	for _, tc := range []struct {
		name   string
		claims func(map[string]any)
		header func(map[string]any)
		key    func(map[string]any)
		sign   *rsa.PrivateKey
		want   error
	}{
		{name: "issuer", claims: func(c map[string]any) { c["iss"] = "https://other.invalid" }},
		{name: "audience", claims: func(c map[string]any) { c["aud"] = "other-client" }},
		{name: "authorized party", claims: func(c map[string]any) { c["azp"] = "other-client" }},
		{name: "multiple audience without party", claims: func(c map[string]any) { c["aud"] = []string{"plug-client", "other"} }},
		{name: "nonce", claims: func(c map[string]any) { c["nonce"] = "different-nonce" }},
		{name: "missing nonce", claims: func(c map[string]any) { delete(c, "nonce") }},
		{name: "unknown subject", claims: func(c map[string]any) { c["sub"] = "unknown-subject"; c["interactive"] = true; c["admin"] = true }},
		{name: "missing expiry", claims: func(c map[string]any) { delete(c, "exp") }},
		{name: "exact expired", claims: func(c map[string]any) { c["exp"] = now; c["iat"] = now - 1; c["auth_time"] = now - 1 }, want: authn.ErrTokenExpired},
		{name: "quoted time", claims: func(c map[string]any) { c["exp"] = strconv.FormatInt(now+60, 10) }},
		{name: "fractional time", claims: func(c map[string]any) { c["exp"] = float64(now) + 1.5 }},
		{name: "future issued", claims: func(c map[string]any) { c["iat"] = now + 1 }},
		{name: "stale issued", claims: func(c map[string]any) { c["iat"] = now - 301; c["auth_time"] = now - 301 }},
		{name: "future not before", claims: func(c map[string]any) { c["nbf"] = now + 1 }},
		{name: "missing fresh login", claims: func(c map[string]any) { delete(c, "auth_time") }},
		{name: "stale login", claims: func(c map[string]any) { c["auth_time"] = now - 61 }},
		{name: "unsigned algorithm", header: func(h map[string]any) { h["alg"] = "none" }},
		{name: "HMAC algorithm", header: func(h map[string]any) { h["alg"] = "HS256" }},
		{name: "unknown key", header: func(h map[string]any) { h["kid"] = "unknown-key" }},
		{name: "critical extension", header: func(h map[string]any) { h["crit"] = []string{"unhandled"} }},
		{name: "token key URL", header: func(h map[string]any) { h["jku"] = "https://other.invalid/keys" }},
		{name: "wrong signature", sign: wrongKey},
		{name: "key use", key: func(k map[string]any) { k["use"] = "enc" }},
		{name: "key operations", key: func(k map[string]any) { k["key_ops"] = []string{"sign"} }},
		{name: "weak modulus", key: func(k map[string]any) { k["n"] = "AQAB" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idp.mu.Lock()
			idp.mutate, idp.jwtHeader, idp.keySet, idp.signKey = tc.claims, tc.header, tc.key, tc.sign
			idp.mu.Unlock()
			p, a := configured(t, idp.config())
			flow, err := p.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			state, code := idp.authorize(flow)
			session, err := p.Complete(context.Background(), state, flow.Binding, code)
			want := tc.want
			if want == nil {
				want = authn.ErrUnauthenticated
			}
			if !errors.Is(err, want) || session.Token != "" || err.Error() != want.Error() {
				t.Fatalf("signed invalid token did not fail closed with canonical error: %v", err)
			}
			if _, err := a.Admit(context.Background(), idp.lastJWT); !errors.Is(err, authn.ErrUnauthenticated) {
				t.Fatal("raw issuer JWT replaced a revocable Plug session")
			}
		})
	}
}

func TestBoundedFlowSessionCapacityAndIdTokenExpiry(t *testing.T) {
	idp := newIDP(t)
	config := idp.config()
	config.MaxFlows, config.MaxSessions, config.FlowTTL = 1, 1, time.Minute
	p, a := configured(t, config)
	flow, err := p.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state, code := idp.authorize(flow)
	if _, err := p.Begin(context.Background()); !errors.Is(err, authn.ErrUnavailable) {
		t.Fatal("flow capacity was unbounded")
	}
	idp.now.Store(flow.ExpiresAt.Unix())
	if _, err := p.Complete(context.Background(), state, flow.Binding, code); !errors.Is(err, authn.ErrTokenExpired) || idp.tokens != 0 {
		t.Fatal("expired flow performed token exchange")
	}
	idp.mu.Lock()
	idp.mutate = func(c map[string]any) { c["exp"] = idp.clock().Unix() + 30 }
	idp.mu.Unlock()
	first := login(t, idp, p)
	if !first.ExpiresAt.Equal(idp.clock().Add(30 * time.Second)) {
		t.Fatal("session outlived ID token")
	}
	second, err := p.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state, code = idp.authorize(second)
	if _, err := p.Complete(context.Background(), state, second.Binding, code); !errors.Is(err, authn.ErrUnavailable) {
		t.Fatal("session capacity was unbounded")
	}
	other, _ := configured(t, config)
	if _, err := other.VerifyCoreSession(context.Background(), first.Token); !errors.Is(err, authn.ErrUnauthenticated) {
		t.Fatal("process-local token was portable to another authority")
	}
	idp.now.Store(first.ExpiresAt.Unix())
	if _, err := a.Admit(context.Background(), first.Token); !errors.Is(err, authn.ErrTokenExpired) {
		t.Fatal("session expiry bound was ignored")
	}
	_ = login(t, idp, p) // Expired capacity is reclaimed without evicting live sessions.
}

func TestTokenResponseBoundsAndRedirectsFailClosed(t *testing.T) {
	idp := newIDP(t)
	var redirected atomic.Int64
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1); w.WriteHeader(http.StatusOK) }))
	defer other.Close()
	for _, tc := range []struct{ name, body, redirect string }{
		{name: "duplicate fields", body: `{"id_token":"one","id_token":"two"}`},
		{name: "provider error", body: `{"error":"private-provider-detail"}`},
		{name: "oversized response", body: `{"id_token":"` + strings.Repeat("A", 65536) + `"}`},
		{name: "redirect", redirect: other.URL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idp.mu.Lock()
			idp.tokenRaw, idp.redirect = tc.body, tc.redirect
			idp.mu.Unlock()
			p, _ := configured(t, idp.config())
			flow, err := p.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			state, code := idp.authorize(flow)
			if _, err := p.Complete(context.Background(), state, flow.Binding, code); !errors.Is(err, authn.ErrUnauthenticated) || err.Error() != "unauthenticated" {
				t.Fatalf("invalid provider response escaped sanitized boundary: %v", err)
			}
		})
	}
	if redirected.Load() != 0 {
		t.Fatal("token request followed a redirect")
	}
}

func TestActualTokenRequestCancellationAndFlowDeadline(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "expired during exchange"}[expired], func(t *testing.T) {
			idp := newIDP(t)
			config := idp.config()
			config.FlowTTL = time.Minute
			p, _ := configured(t, config)
			flow, err := p.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			state, code := idp.authorize(flow)
			started, resume := make(chan struct{}, 1), make(chan struct{})
			idp.mu.Lock()
			idp.started, idp.resume = started, resume
			idp.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := p.Complete(ctx, state, flow.Binding, code); done <- err }()
			select {
			case <-started:
			case <-ctx.Done():
				close(resume)
				t.Fatal("actual token request did not reach mock IdP")
			}
			want := authn.ErrUnavailable
			if expired {
				idp.now.Store(flow.ExpiresAt.Unix())
				want = authn.ErrTokenExpired
			} else {
				cancel()
			}
			close(resume)
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("interrupted exchange did not fail closed: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("interrupted actual HTTP request retained work")
			}
			if _, err := p.Complete(context.Background(), state, flow.Binding, code); !errors.Is(err, authn.ErrUnauthenticated) {
				t.Fatal("interrupted exchange remained replayable")
			}
		})
	}
}

func TestIdPServerFailureIsCredentialFreeAndRetryable(t *testing.T) {
	idp := newIDP(t)
	idp.status = http.StatusServiceUnavailable
	p, _ := configured(t, idp.config())
	flow, err := p.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state, code := idp.authorize(flow)
	if _, err := p.Complete(context.Background(), state, flow.Binding, code); !errors.Is(err, authn.ErrUnavailable) || err.Error() != "authn_unavailable" {
		t.Fatal("IdP failure was not sanitized as unavailable")
	}
}

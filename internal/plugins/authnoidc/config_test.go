package authnoidc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/authn"
)

func validConfig() Config {
	return Config{Issuer: "https://idp.invalid", ClientID: "plug", RedirectURI: "https://plug.invalid/callback",
		AuthorizationEndpoint: "https://idp.invalid/authorize", TokenEndpoint: "https://idp.invalid/token",
		JWKSURL: "https://idp.invalid/keys", Users: map[string]string{"subject": "user:one"}}
}

func TestConfigurationRefusesUnsupportedTrustAndBounds(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.Issuer = "http://127.0.0.1" },
		func(c *Config) { c.TokenEndpoint = "https://idp.invalid/token?client=other" },
		func(c *Config) { c.JWKSURL = "https://user:pass@idp.invalid/keys" },
		func(c *Config) { c.RedirectURI = "https://plug.invalid/callback#fragment" },
		func(c *Config) { c.ClientID = "" },
		func(c *Config) { c.Users = nil },
		func(c *Config) { c.Users = map[string]string{"subject": "agent:one"} },
		func(c *Config) { c.FlowTTL = 6 * time.Minute },
		func(c *Config) { c.SessionTTL = 2 * time.Hour },
		func(c *Config) { c.MaxFlows = -1 },
		func(c *Config) { c.MaxSessions = 65537 },
	} {
		config := validConfig()
		change(&config)
		if _, err := New(config); err == nil {
			t.Fatal("unsupported configuration was accepted")
		}
	}
	config := validConfig()
	config.HTTPClient = &http.Client{}
	p, err := New(config)
	if err != nil || p.client.Timeout <= 0 || p.client.CheckRedirect == nil {
		t.Fatal("IdP egress was not bounded")
	}
	config.Users["subject"] = "user:changed"
	if p.config.Users["subject"] != "user:one" {
		t.Fatal("caller mutated provisioned identity map")
	}
}

func TestOriginAllowlistCanonicalizesAndRejectsUnsafeValues(t *testing.T) {
	config := validConfig()
	config.AllowedOrigins = []string{"HTTPS://APP.INVALID:443", "http://localhost:8080", "https://[::1]:443"}
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, origin := range []string{"https://app.invalid", "https://app.invalid:443", "http://localhost:8080", "https://[::1]"} {
		if !p.OriginAllowed(origin) {
			t.Fatalf("allowlisted origin was not canonicalized: %q", origin)
		}
	}
	for _, origin := range []string{"https://app.invalid:0", "http://example.invalid", "https://app.invalid/path", "https://app.invalid?query", "null"} {
		if p.OriginAllowed(origin) {
			t.Fatalf("unsafe origin was admitted: %q", origin)
		}
	}
	for _, origins := range [][]string{
		{"https://app.invalid", "HTTPS://APP.INVALID:443"},
		{"http://example.invalid"},
		{"https://app.invalid:0"},
	} {
		bad := validConfig()
		bad.AllowedOrigins = origins
		if _, err := New(bad); err == nil {
			t.Fatalf("unsafe origin configuration was accepted: %v", origins)
		}
	}
}

func TestCanceledFlowAndInvalidCredentialsNeverUseIdP(t *testing.T) {
	p, err := New(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Begin(ctx); !errors.Is(err, authn.ErrUnavailable) {
		t.Fatal("canceled login was accepted")
	}
	if _, err := p.Complete(ctx, "invalid", "invalid", "code"); !errors.Is(err, authn.ErrUnavailable) {
		t.Fatal("canceled callback was accepted")
	}
	for _, token := range []string{"issuer.jwt.signature", "", "plg_s_invalid", "plg_k_unrelated"} {
		if _, err := p.VerifyCoreSession(context.Background(), token); !errors.Is(err, authn.ErrUnauthenticated) {
			t.Fatal("unissued session was accepted")
		}
	}
}

func TestAudienceRequiresClientAndAuthorizedParty(t *testing.T) {
	for _, tc := range []struct {
		aud, party string
		want       bool
	}{
		{`"plug"`, "", true}, {`["plug"]`, "", true}, {`["plug","other"]`, "plug", true},
		{`["plug","other"]`, "", false}, {`"other"`, "", false}, {`"plug"`, "other", false},
		{`["plug","plug"]`, "plug", false}, {`null`, "", false}, {`[]`, "", false},
	} {
		if validAudience(json.RawMessage(tc.aud), tc.party, "plug") != tc.want {
			t.Fatal("incorrect audience/authorized party decision")
		}
	}
}

func TestSessionLookupHonorsContextWhileContended(t *testing.T) {
	p, err := New(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := p.VerifyCoreSession(ctx, "plg_s_"+strings.Repeat("a", 32)+"_"+strings.Repeat("A", 43))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, authn.ErrUnavailable) {
			t.Fatal("contended session admission did not fail closed on cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("session lookup waited for a held lock past its deadline")
	}
}

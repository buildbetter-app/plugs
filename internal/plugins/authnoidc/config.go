// Package authnoidc verifies a bounded public-client OIDC code flow and issues
// revocable, opaque Plug core sessions. It never admits issuer JWTs as bearer
// credentials. Browser endpoints, cookies, CORS and device grants are not bound.
package authnoidc

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

type Config struct {
	// All URLs are explicit HTTPS URLs without userinfo, query or fragment.
	// No discovery, untrusted issuer selection or redirect following is used.
	Issuer                string
	ClientID              string
	RedirectURI           string
	AuthorizationEndpoint string
	TokenEndpoint         string
	JWKSURL               string
	// Users maps this issuer's exact subject to a provisioned Plug User ID.
	// Unknown subjects are refused; email, roles and claims never provision users.
	Users map[string]string
	// HTTPClient is copied; redirects are disabled and timeout is bounded. A
	// private trust transport permits disposable local TLS IdPs in tests.
	HTTPClient  *http.Client
	Now         func() time.Time
	FlowTTL     time.Duration // default and maximum: five minutes
	SessionTTL  time.Duration // default: fifteen minutes; maximum: one hour
	MaxFlows    int           // default: 128; maximum: 4096
	MaxSessions int           // default: 1024; maximum: 65536
}

type Provider struct {
	mu       sync.RWMutex
	config   Config
	client   *http.Client
	now      func() time.Time
	flows    map[[32]byte]flow
	sessions map[string]sessionRecord
}

var _ authn.CoreSessionSource = (*Provider)(nil)

func New(config Config) (*Provider, error) {
	for _, raw := range []string{config.Issuer, config.RedirectURI, config.AuthorizationEndpoint, config.TokenEndpoint, config.JWKSURL} {
		if !httpsURL(raw) {
			return nil, fmt.Errorf("authnoidc: explicit HTTPS issuer, redirect and endpoints are required")
		}
	}
	if !boundedText(config.ClientID, 256) || len(config.Users) == 0 || len(config.Users) > 4096 {
		return nil, fmt.Errorf("authnoidc: a client and bounded provisioned subject map are required")
	}
	users := make(map[string]string, len(config.Users))
	for sub, id := range config.Users {
		if !boundedText(sub, 255) || !boundedText(id, 256) || authn.ValidatePrincipal(model.PrincipalRef{Kind: "user", ID: id}) != nil {
			return nil, fmt.Errorf("authnoidc: invalid provisioned subject or User")
		}
		users[sub] = id
	}
	config.Users = users
	if config.FlowTTL == 0 {
		config.FlowTTL = 5 * time.Minute
	}
	if config.SessionTTL == 0 {
		config.SessionTTL = 15 * time.Minute
	}
	if config.MaxFlows == 0 {
		config.MaxFlows = 128
	}
	if config.MaxSessions == 0 {
		config.MaxSessions = 1024
	}
	if config.FlowTTL <= 0 || config.FlowTTL > 5*time.Minute || config.SessionTTL <= 0 ||
		config.SessionTTL > time.Hour || config.MaxFlows <= 0 || config.MaxFlows > 4096 ||
		config.MaxSessions <= 0 || config.MaxSessions > 65536 {
		return nil, fmt.Errorf("authnoidc: flow, session or capacity limit is outside the supported bounds")
	}
	client := http.Client{Timeout: 10 * time.Second}
	if config.HTTPClient != nil {
		client = *config.HTTPClient
		if client.Timeout <= 0 || client.Timeout > 10*time.Second {
			client.Timeout = 10 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client.Jar = nil
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Provider{config: config, client: &client, now: now,
		flows: make(map[[32]byte]flow), sessions: make(map[string]sessionRecord)}, nil
}

func httpsURL(raw string) bool {
	if raw == "" || len(raw) > 2048 || strings.ContainsAny(raw, "?#\\\r\n\t ") {
		return false
	}
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.Hostname() != "" && u.User == nil &&
		u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.Opaque == ""
}

func boundedText(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, r := range value {
		if r <= 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func canceled(ctx context.Context) bool { return ctx == nil || ctx.Err() != nil }

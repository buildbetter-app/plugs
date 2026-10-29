// Package authnoidc verifies a bounded public-client OIDC code flow and issues
// revocable, opaque Plug core sessions. It never admits issuer JWTs as bearer
// credentials. HTTPHandler adds explicit login/callback routes and
// non-credentialed CORS for configured browser origins; cookies and device
// grants remain unsupported.
package authnoidc

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
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
	// AllowedOrigins is the operator-owned browser allowlist for other-origin
	// login and bearer requests. Origins are exact HTTPS origins; loopback HTTP
	// origins are accepted only for local development fixtures. Credentials and
	// cookies never use this list.
	AllowedOrigins []string
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
	origins  map[string]struct{}
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
	origins, err := normalizeOrigins(config.AllowedOrigins)
	if err != nil {
		return nil, err
	}
	config.AllowedOrigins = append([]string(nil), origins...)
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
	allowed := make(map[string]struct{}, len(origins))
	for _, origin := range origins {
		allowed[origin] = struct{}{}
	}
	return &Provider{config: config, client: &client, now: now,
		flows: make(map[[32]byte]flow), sessions: make(map[string]sessionRecord), origins: allowed}, nil
}

func normalizeOrigins(values []string) ([]string, error) {
	if len(values) > 64 {
		return nil, fmt.Errorf("authnoidc: origin allowlist is too large")
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, raw := range values {
		origin, ok := normalizeOrigin(raw)
		if !ok {
			return nil, fmt.Errorf("authnoidc: invalid allowed origin")
		}
		if _, exists := seen[origin]; exists {
			return nil, fmt.Errorf("authnoidc: duplicate allowed origin")
		}
		seen[origin] = struct{}{}
		result = append(result, origin)
	}
	return result, nil
}

func normalizeOrigin(raw string) (string, bool) {
	if raw == "" || len(raw) > 2048 || strings.ContainsAny(raw, "?#\\\r\n\t ") {
		return "", false
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Host == "" || u.Path != "" || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || u.Opaque != "" || strings.HasSuffix(u.Host, ":") ||
		(u.Scheme != "https" && u.Scheme != "http") {
		return "", false
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || strings.ContainsAny(host, "[]%") || strings.HasSuffix(host, ".") {
		return "", false
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		host = ip.String()
	}
	port := u.Port()
	if port != "" {
		for _, digit := range port {
			if digit < '0' || digit > '9' {
				return "", false
			}
		}
		number := 0
		for _, digit := range port {
			number = number*10 + int(digit-'0')
			if number > 65535 {
				return "", false
			}
		}
		if number == 0 {
			return "", false
		}
		if (u.Scheme == "https" && number == 443) || (u.Scheme == "http" && number == 80) {
			port = ""
		}
	}
	if u.Scheme == "http" && !isLoopbackOriginHost(host) {
		return "", false
	}
	if port != "" {
		if strings.Contains(host, ":") {
			return u.Scheme + "://[" + host + "]:" + port, true
		}
		return u.Scheme + "://" + host + ":" + port, true
	}
	return u.Scheme + "://" + host, true
}

func isLoopbackOriginHost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

// OriginAllowed reports whether an origin is present in the immutable operator
// allowlist. It is suitable for CORS response headers and login route checks;
// bearer admission additionally calls AllowOrigin below.
func (p *Provider) OriginAllowed(origin string) bool {
	canonical, ok := normalizeOrigin(origin)
	if !ok {
		return false
	}
	p.mu.RLock()
	_, allowed := p.origins[canonical]
	p.mu.RUnlock()
	return allowed
}

// AllowOrigin is the authn.CrossOriginAuthorizer composition seam. It admits
// only a currently live Plug core session from a configured origin. A session
// started by the HTTP login adapter is bound to that origin. Private flows
// without an origin are never admitted as other-origin browser bearers.
func (p *Provider) AllowOrigin(ctx context.Context, origin, token string) bool {
	if ctx == nil {
		return false
	}
	canonical, ok := normalizeOrigin(origin)
	if !ok {
		return false
	}
	p.mu.RLock()
	_, allowed := p.origins[canonical]
	p.mu.RUnlock()
	if !allowed {
		return false
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	record, err := p.sessionForToken(lookupCtx, token)
	return err == nil && record.origin == canonical
}

func httpsURL(raw string) bool {
	if raw == "" || len(raw) > 2048 || strings.ContainsAny(raw, "?#\\\r\n\t ") {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || strings.HasSuffix(u.Host, ":") {
		return false
	}
	port := u.Port()
	if port == "" {
		return true
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	number := 0
	for _, digit := range port {
		number = number*10 + int(digit-'0')
		if number > 65535 {
			return false
		}
	}
	return number > 0
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

package authnoidc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/url"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/authn"
)

type flow struct {
	binding   [sha256.Size]byte
	origin    string
	nonce     string
	verifier  string
	startedAt time.Time
	expiresAt time.Time
}

// Login is a private flow handle. Keep Binding with the initiating client,
// separate from the authorization URL, and never expose it to the IdP. This
// package does not install a browser cookie/redirect handler on the RPC server.
type Login struct {
	AuthorizationURL string
	State            string `json:"-"`
	Binding          string `json:"-"`
	ExpiresAt        time.Time
}

func (Login) String() string     { return "OIDC login (redacted)" }
func (l Login) GoString() string { return l.String() }

// Begin starts a fresh interactive public-client login. State, nonce, S256
// verifier and a separate client binding each have 256 bits of entropy.
func (p *Provider) Begin(ctx context.Context) (Login, error) {
	return p.BeginForOrigin(ctx, "")
}

// BeginForOrigin starts a login for an operator-allowlisted browser origin.
// The origin is bound into the resulting core session and never sent to the
// identity provider as an authority claim.
func (p *Provider) BeginForOrigin(ctx context.Context, origin string) (Login, error) {
	if canceled(ctx) {
		return Login{}, authn.ErrUnavailable
	}
	canonicalOrigin := ""
	if origin != "" {
		var ok bool
		canonicalOrigin, ok = normalizeOrigin(origin)
		if !ok || !p.OriginAllowed(canonicalOrigin) {
			return Login{}, authn.ErrForbidden
		}
	}
	values := make([]string, 4)
	for i := range values {
		value, err := randomSecret()
		if err != nil {
			return Login{}, authn.ErrUnavailable
		}
		values[i] = value
	}
	state, nonce, verifier, binding := values[0], values[1], values[2], values[3]
	now := p.now()
	entry := flow{binding: sha256.Sum256([]byte(binding)), origin: canonicalOrigin, nonce: nonce, verifier: verifier,
		startedAt: now, expiresAt: now.Add(p.config.FlowTTL)}
	key := sha256.Sum256([]byte(state))
	p.mu.Lock()
	for id, old := range p.flows {
		if !now.Before(old.expiresAt) {
			delete(p.flows, id)
		}
	}
	if len(p.flows) >= p.config.MaxFlows {
		p.mu.Unlock()
		return Login{}, authn.ErrUnavailable
	}
	if _, exists := p.flows[key]; exists {
		p.mu.Unlock()
		return Login{}, authn.ErrUnavailable
	}
	p.flows[key] = entry
	p.mu.Unlock()
	u, _ := url.Parse(p.config.AuthorizationEndpoint)
	challenge := sha256.Sum256([]byte(verifier))
	u.RawQuery = url.Values{
		"response_type": {"code"}, "client_id": {p.config.ClientID}, "redirect_uri": {p.config.RedirectURI},
		"scope": {"openid"}, "state": {state}, "nonce": {nonce},
		"code_challenge_method": {"S256"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(challenge[:])},
		"prompt": {"login"}, "max_age": {"0"},
	}.Encode()
	return Login{AuthorizationURL: u.String(), State: state, Binding: binding, ExpiresAt: entry.expiresAt}, nil
}

// Complete accepts a code only for the initiating client's private Binding.
// A matching flow is consumed before any egress, including failed exchanges.
// IdP access/refresh tokens and the verified ID token are never retained.
func (p *Provider) Complete(ctx context.Context, state, binding, code string) (IssuedSession, error) {
	return p.complete(ctx, "", state, binding, code)
}

// CompleteForOrigin consumes a callback delivered by an operator-allowlisted
// browser origin. It enforces the origin binding created by BeginForOrigin
// before exchanging the code with the identity provider.
func (p *Provider) CompleteForOrigin(ctx context.Context, origin, state, binding, code string) (IssuedSession, error) {
	canonicalOrigin := ""
	if origin != "" {
		var ok bool
		canonicalOrigin, ok = normalizeOrigin(origin)
		if !ok || !p.OriginAllowed(canonicalOrigin) {
			return IssuedSession{}, authn.ErrForbidden
		}
	}
	return p.complete(ctx, canonicalOrigin, state, binding, code)
}

func (p *Provider) complete(ctx context.Context, origin, state, binding, code string) (IssuedSession, error) {
	if canceled(ctx) {
		return IssuedSession{}, authn.ErrUnavailable
	}
	if !secretShape(state) || !secretShape(binding) || !boundedText(code, 4096) {
		return IssuedSession{}, authn.ErrUnauthenticated
	}
	key, digest := sha256.Sum256([]byte(state)), sha256.Sum256([]byte(binding))
	p.mu.Lock()
	f, exists := p.flows[key]
	if !exists || subtle.ConstantTimeCompare(digest[:], f.binding[:]) != 1 {
		p.mu.Unlock()
		return IssuedSession{}, authn.ErrUnauthenticated
	}
	delete(p.flows, key)
	p.mu.Unlock()
	if f.origin != "" && f.origin != origin {
		return IssuedSession{}, authn.ErrForbidden
	}
	if !p.now().Before(f.expiresAt) {
		return IssuedSession{}, authn.ErrTokenExpired
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	raw, err := p.exchange(ctx, code, f.verifier)
	if err != nil {
		return IssuedSession{}, err
	}
	user, expires, err := p.verifyIDToken(ctx, raw, f)
	if err != nil {
		return IssuedSession{}, err
	}
	if canceled(ctx) {
		return IssuedSession{}, authn.ErrUnavailable
	}
	if !p.now().Before(f.expiresAt) {
		return IssuedSession{}, authn.ErrTokenExpired
	}
	return p.issue(user, expires, f.origin)
}

func randomSecret() (string, error) {
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes[:]), nil
}

func secretShape(raw string) bool {
	data, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	return err == nil && len(data) == 32 && base64.RawURLEncoding.EncodeToString(data) == raw
}

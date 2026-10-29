// Package authnapikey implements an optional native API-key authenticator.
// Its key ring is process-local; only credential digests are retained. Issuance
// returns the bearer secret once. No API key supplies interactive human proof.
package authnapikey

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

type Config struct {
	// Use the host's current installed-chain predicate, also used by authn.
	AuthorizationInstalled func() bool
	// Now is an optional host clock. Nil uses time.Now.
	Now func() time.Time
}

type Provider struct {
	mu    sync.RWMutex
	keys  map[string]keyRecord
	authz func() bool
	now   func() time.Time
}

type keyRecord struct {
	digest    [sha256.Size]byte
	principal model.PrincipalRef
	expiresAt time.Time
	revoked   bool
}

// IssuedKey contains the only copy of the bearer token returned by Issue.
// Callers must deliver it privately, never place it in logs or RPC audit data.
type IssuedKey struct {
	ID        string
	Token     string
	ExpiresAt time.Time
}

// Formatting an issuance is redacted; callers must explicitly select Token.
func (k IssuedKey) String() string   { return "API key " + k.ID + " (redacted)" }
func (k IssuedKey) GoString() string { return k.String() }

var _ authn.Provider = (*Provider)(nil)

func New(config Config) *Provider {
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Provider{keys: make(map[string]keyRecord), authz: config.AuthorizationInstalled, now: now}
}

// Issue is a trusted host operation, not an unauthenticated RPC method. The
// principal is host-validated; it is never accepted from request actor fields.
// Keys have an explicit future expiry. A scoped key cannot be issued without
// an installed authorization plugin to enforce its sponsor/caveats (P58).
func (p *Provider) Issue(principal model.PrincipalRef, expiresAt time.Time) (IssuedKey, error) {
	if err := authn.ValidatePrincipal(principal); err != nil {
		return IssuedKey{}, err
	}
	if authn.IsScoped(principal) && !p.hasAuthorization() {
		return IssuedKey{}, authn.ErrAuthzNotInstalled
	}
	if !p.now().Before(expiresAt) {
		return IssuedKey{}, errors.New("API key expiry must be in the future")
	}
	principal, err := clonePrincipal(principal)
	if err != nil {
		return IssuedKey{}, authn.ErrUnauthenticated
	}
	for {
		var id [16]byte
		var secret [32]byte
		if _, err := rand.Read(id[:]); err != nil {
			return IssuedKey{}, authn.ErrUnavailable
		}
		if _, err := rand.Read(secret[:]); err != nil {
			return IssuedKey{}, authn.ErrUnavailable
		}
		keyID := hex.EncodeToString(id[:])
		token := "plg_k_" + keyID + "_" + base64.RawURLEncoding.EncodeToString(secret[:])
		record := keyRecord{digest: sha256.Sum256([]byte(token)), principal: principal, expiresAt: expiresAt}
		p.mu.Lock()
		if _, exists := p.keys[keyID]; exists {
			p.mu.Unlock()
			continue
		}
		p.keys[keyID] = record
		p.mu.Unlock()
		return IssuedKey{ID: keyID, Token: token, ExpiresAt: expiresAt}, nil
	}
}

// Revoke is effective on the next credential check and is permanent for this
// key. There is no positive authentication cache or anonymous retry path.
func (p *Provider) Revoke(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	record, exists := p.keys[id]
	if !exists {
		return false
	}
	record.revoked = true
	p.keys[id] = record
	return true
}

func (p *Provider) Authenticate(ctx context.Context, token string) (model.PrincipalRef, error) {
	if ctx.Err() != nil {
		return model.PrincipalRef{}, authn.ErrUnavailable
	}
	id, ok := parseKey(token)
	if !ok {
		return model.PrincipalRef{}, authn.ErrUnauthenticated
	}
	p.mu.RLock()
	record, exists := p.keys[id]
	p.mu.RUnlock()
	// Both operands are fixed-size digests, even for an unknown key ID.
	digest := sha256.Sum256([]byte(token))
	match := subtle.ConstantTimeCompare(digest[:], record.digest[:])
	if match != 1 || !exists || record.revoked {
		return model.PrincipalRef{}, authn.ErrUnauthenticated
	}
	if !p.now().Before(record.expiresAt) {
		return model.PrincipalRef{}, authn.ErrTokenExpired
	}
	if authn.IsScoped(record.principal) && !p.hasAuthorization() {
		return model.PrincipalRef{}, authn.ErrAuthzNotInstalled
	}
	principal, err := clonePrincipal(record.principal)
	if err != nil || ctx.Err() != nil {
		return model.PrincipalRef{}, authn.ErrUnavailable
	}
	return principal, nil
}

func (p *Provider) hasAuthorization() bool { return p.authz != nil && p.authz() }

func parseKey(token string) (string, bool) {
	if !strings.HasPrefix(token, "plg_k_") || len(token) != 6+32+1+43 {
		return "", false
	}
	id, secret, ok := strings.Cut(strings.TrimPrefix(token, "plg_k_"), "_")
	if !ok || len(id) != 32 || id != strings.ToLower(id) {
		return "", false
	}
	if _, err := hex.DecodeString(id); err != nil {
		return "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != secret {
		return "", false
	}
	return id, true
}

func clonePrincipal(principal model.PrincipalRef) (model.PrincipalRef, error) {
	data, err := json.Marshal(principal)
	if err != nil {
		return model.PrincipalRef{}, err
	}
	var copy model.PrincipalRef
	err = json.Unmarshal(data, &copy)
	return copy, err
}

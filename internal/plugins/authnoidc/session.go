package authnoidc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type sessionRecord struct {
	digest    [sha256.Size]byte
	user      string
	origin    string
	expiresAt time.Time
}

// IssuedSession contains the bearer secret once. Persist only an appropriate
// private client credential; the provider retains its digest, not the secret.
type IssuedSession struct {
	ID        string
	Token     string `json:"-"`
	ExpiresAt time.Time
}

func (s IssuedSession) String() string   { return "Plug session " + s.ID + " (redacted)" }
func (s IssuedSession) GoString() string { return s.String() }

// issue is intentionally private: only the completed, cryptographically
// verified flow can mint an interactive session. No principal/interactive
// flag, ID token or arbitrary credential exchange is a public issuance API.
func (p *Provider) issue(ctx context.Context, user string, idExpiry time.Time, origin string) (IssuedSession, error) {
	if canceled(ctx) {
		return IssuedSession{}, authn.ErrUnavailable
	}
	var idBytes [16]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return IssuedSession{}, authn.ErrUnavailable
	}
	secret, err := randomSecret()
	if err != nil {
		return IssuedSession{}, authn.ErrUnavailable
	}
	id := hex.EncodeToString(idBytes[:])
	token := authn.CoreSessionPrefix + id + "_" + secret
	now := p.now()
	expires := now.Add(p.config.SessionTTL)
	if idExpiry.Before(expires) {
		expires = idExpiry
	}
	if !now.Before(expires) {
		return IssuedSession{}, authn.ErrTokenExpired
	}
	digest := sha256.Sum256([]byte(token))
	if p.store != nil {
		// Browser-origin caveats remain provider-local, as in the HTTP adapter.
		// Reserve a bounded local entry before durable publication. The entry
		// never authenticates anything: every admission still reads the store.
		if origin != "" {
			p.mu.Lock()
			for key, record := range p.sessions {
				if !now.Before(record.expiresAt) {
					delete(p.sessions, key)
				}
			}
			if len(p.sessions) >= p.config.MaxSessions {
				p.mu.Unlock()
				return IssuedSession{}, authn.ErrUnavailable
			}
			p.sessions[id] = sessionRecord{digest: digest, user: user, origin: origin, expiresAt: expires}
			p.mu.Unlock()
		}
		err := p.store.PutCoreSession(ctx, driver.CoreSessionRecord{
			ID: id, TokenDigest: digest[:], Principal: model.PrincipalRef{Kind: "user", ID: user},
			ExpiresAt: expires,
		}, now, p.config.MaxSessions)
		if err == nil {
			return IssuedSession{ID: id, Token: token, ExpiresAt: expires}, nil
		}
		p.mu.Lock()
		delete(p.sessions, id)
		p.mu.Unlock()
		if ctx.Err() != nil {
			return IssuedSession{}, authn.ErrUnavailable
		}
		if errors.Is(err, driver.ErrConflict) {
			return IssuedSession{}, authn.ErrUnavailable
		}
		return IssuedSession{}, authn.ErrUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, record := range p.sessions {
		if !now.Before(record.expiresAt) {
			delete(p.sessions, key)
		}
	}
	if len(p.sessions) >= p.config.MaxSessions {
		return IssuedSession{}, authn.ErrUnavailable
	}
	if _, exists := p.sessions[id]; exists {
		return IssuedSession{}, authn.ErrUnavailable
	}
	p.sessions[id] = sessionRecord{digest: digest, user: user, origin: origin, expiresAt: expires}
	return IssuedSession{ID: id, Token: token, ExpiresAt: expires}, nil
}

// Revoke is a trusted host logout operation. After it returns, both new
// admissions and already retained Identity proofs refuse this session.
func (p *Provider) Revoke(id string) bool {
	if p.store != nil {
		revoked, err := p.store.RevokeCoreSession(context.Background(), id)
		p.mu.Lock()
		delete(p.sessions, id)
		p.mu.Unlock()
		return err == nil && revoked
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.sessions[id]; !exists {
		return false
	}
	delete(p.sessions, id)
	return true
}

type liveSession struct {
	owner  *Provider
	id     string
	digest [sha256.Size]byte
	user   string
}

func (s liveSession) Principal() model.PrincipalRef {
	return model.PrincipalRef{Kind: "user", ID: s.user}
}

func (s liveSession) RequireInteractiveUser() error {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := s.owner.lookup(ctx, s.id, s.digest)
	return err
}

// VerifyCoreSession is the authn trusted-source adapter, with no provider
// egress and no positive cache. Raw IdP JWTs are always refused here.
func (p *Provider) VerifyCoreSession(ctx context.Context, token string) (authn.CoreSession, error) {
	if canceled(ctx) {
		return nil, authn.ErrUnavailable
	}
	id, digest, err := parseSessionToken(token)
	if err != nil {
		return nil, err
	}
	record, err := p.lookup(ctx, id, digest)
	if err != nil {
		return nil, err
	}
	if canceled(ctx) {
		return nil, authn.ErrUnavailable
	}
	return liveSession{owner: p, id: id, digest: digest, user: record.user}, nil
}

func (p *Provider) sessionForToken(ctx context.Context, token string) (sessionRecord, error) {
	id, digest, err := parseSessionToken(token)
	if err != nil {
		return sessionRecord{}, err
	}
	return p.lookup(ctx, id, digest)
}

func parseSessionToken(token string) (string, [sha256.Size]byte, error) {
	id, secret, ok := strings.Cut(strings.TrimPrefix(token, authn.CoreSessionPrefix), "_")
	if !strings.HasPrefix(token, authn.CoreSessionPrefix) || len(id) != 32 || !secretShape(secret) || !ok {
		return "", [sha256.Size]byte{}, authn.ErrUnauthenticated
	}
	if bytes, err := hex.DecodeString(id); err != nil || hex.EncodeToString(bytes) != id {
		return "", [sha256.Size]byte{}, authn.ErrUnauthenticated
	}
	return id, sha256.Sum256([]byte(token)), nil
}

func (p *Provider) lookup(ctx context.Context, id string, digest [sha256.Size]byte) (sessionRecord, error) {
	if canceled(ctx) {
		return sessionRecord{}, authn.ErrUnavailable
	}
	if p.store != nil {
		record, err := p.store.LookupCoreSession(ctx, id)
		if errors.Is(err, driver.ErrNotFound) {
			return sessionRecord{}, authn.ErrUnauthenticated
		}
		if err != nil {
			if ctx.Err() != nil {
				return sessionRecord{}, authn.ErrUnavailable
			}
			return sessionRecord{}, authn.ErrUnavailable
		}
		if len(record.TokenDigest) != sha256.Size || subtle.ConstantTimeCompare(digest[:], record.TokenDigest) != 1 || record.Revoked {
			return sessionRecord{}, authn.ErrUnauthenticated
		}
		if record.Principal.Kind != "user" || record.Principal.Sponsor != nil || len(record.Principal.Caveats) != 0 || authn.ValidatePrincipal(record.Principal) != nil {
			return sessionRecord{}, authn.ErrUnauthenticated
		}
		result := sessionRecord{digest: digest, user: record.Principal.ID, expiresAt: record.ExpiresAt}
		// Origin is a transport caveat, not part of the shared core-session
		// storage contract. Reuse it only when this provider issued the session;
		// a restarted provider therefore fails closed for foreign origins.
		local, exists, err := p.localSession(ctx, id)
		if err != nil {
			return sessionRecord{}, err
		}
		if exists && subtle.ConstantTimeCompare(digest[:], local.digest[:]) == 1 {
			result.origin = local.origin
		}
		if !p.now().Before(record.ExpiresAt) {
			return result, authn.ErrTokenExpired
		}
		return result, nil
	}
	record, exists, err := p.localSession(ctx, id)
	if err != nil {
		return sessionRecord{}, err
	}
	if subtle.ConstantTimeCompare(digest[:], record.digest[:]) != 1 || !exists {
		return sessionRecord{}, authn.ErrUnauthenticated
	}
	if !p.now().Before(record.expiresAt) {
		// Preserve the authenticated record so cross-origin admission can check
		// its origin caveat before reporting expiry. The error still denies it.
		return record, authn.ErrTokenExpired
	}
	return record, nil
}

func (p *Provider) localSession(ctx context.Context, id string) (sessionRecord, bool, error) {
	if !p.mu.TryRLock() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for !p.mu.TryRLock() {
			select {
			case <-ctx.Done():
				return sessionRecord{}, false, authn.ErrUnavailable
			case <-ticker.C:
			}
		}
	}
	defer p.mu.RUnlock()
	if canceled(ctx) {
		return sessionRecord{}, false, authn.ErrUnavailable
	}
	record, exists := p.sessions[id]
	return record, exists, nil
}

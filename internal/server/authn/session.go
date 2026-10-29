package authn

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// CoreSessionPrefix reserves a credential namespace. Session-shaped credentials
// never fall through to an ordinary provider, even when verification fails.
const CoreSessionPrefix = "plg_s_"

// CoreSessionSource is trusted composition, not a public authentication claim.
// Implementations issue sessions only after validating an interactive login,
// retain revocation authority locally, and must honor the admission deadline.
type CoreSessionSource interface {
	VerifyCoreSession(context.Context, string) (CoreSession, error)
}

// CoreSession is a live reference to a verified login, never a serialized flag.
// RequireInteractiveUser must check current revocation and half-open expiry on
// every call, including calls on identities admitted before a revocation.
type CoreSession interface {
	Principal() model.PrincipalRef
	RequireInteractiveUser() error
}

func (a *Authenticator) admitSession(ctx context.Context, token string) (Identity, error) {
	if a.sessions == nil {
		return Identity{}, ErrUnauthenticated
	}
	s, err := a.sessions.VerifyCoreSession(ctx, token)
	if ctx.Err() != nil {
		return Identity{}, ErrUnavailable
	}
	if err != nil {
		return Identity{}, sessionFailure(err)
	}
	if s == nil {
		return Identity{}, ErrUnavailable
	}
	p := s.Principal()
	if p.Kind != "user" || p.Sponsor != nil || len(p.Caveats) != 0 || ValidatePrincipal(p) != nil {
		return Identity{}, ErrUnauthenticated
	}
	if err := s.RequireInteractiveUser(); err != nil {
		return Identity{}, sessionFailure(err)
	}
	p, err = copyPrincipal(p)
	if err != nil {
		return Identity{}, ErrUnauthenticated
	}
	return Identity{principal: p, session: s}, nil
}

func sessionFailure(err error) error {
	for _, code := range []Failure{ErrUnauthenticated, ErrTokenExpired, ErrHumanPresenceRequired} {
		if errors.Is(err, code) {
			return code
		}
	}
	return ErrUnavailable
}

// RequireInteractiveUser checks the live session, so retaining an Identity
// cannot retain human authority after logout or expiry. It is an admission
// check, not a transaction-bound challenge/commit guard.
func (i Identity) RequireInteractiveUser() error {
	if i.principal.Kind != "user" || i.session == nil {
		return ErrHumanPresenceRequired
	}
	if err := i.session.RequireInteractiveUser(); err != nil {
		return sessionFailure(err)
	}
	return nil
}

type identityKey struct{}

// WithIdentity carries an already admitted identity across a trusted transport
// composition. JSON parameters and principal-only contexts cannot create one.
func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, identity)
}

// RequireInteractiveUser checks both the live proof and the executing User.
// A deputy or a different principal cannot reuse a transported User proof.
func RequireInteractiveUser(ctx context.Context, by model.PrincipalRef) error {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	if !ok || by.Kind != "user" || by.Sponsor != nil || len(by.Caveats) != 0 ||
		identity.principal.Kind != by.Kind || identity.principal.ID != by.ID {
		return ErrHumanPresenceRequired
	}
	return identity.RequireInteractiveUser()
}

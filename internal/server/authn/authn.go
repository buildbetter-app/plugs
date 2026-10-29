// Package authn stamps request identities and enforces transport admission.
// Authentication is optional; an absent provider admits only local anonymous
// callers. A supplied credential is never treated as anonymous.
package authn

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Failure is a credential-free error code. Its unwrapped model error lets the
// RPC dispatcher use the normal error envelope without importing a provider.
type Failure string

const (
	ErrUnauthenticated       Failure = "unauthenticated"
	ErrTokenExpired          Failure = "token_expired"
	ErrUnavailable           Failure = "authn_unavailable"
	ErrForbidden             Failure = "forbidden"
	ErrAuthzNotInstalled     Failure = "authz_not_installed"
	ErrAuthzRequiresAuthn    Failure = "authz_requires_authn"
	ErrHumanPresenceRequired Failure = "human_presence_required"
)

func (e Failure) Error() string { return string(e) }

func (e Failure) Unwrap() error {
	return &model.Error{
		Message: string(e),
		Data:    model.ErrorData{Code: model.ErrorCode(e), Retryable: e == ErrUnavailable},
	}
}

// Provider verifies a bearer credential and returns its authoritative identity.
// It must honor cancellation. Provider claims never convey human presence.
type Provider interface {
	Authenticate(context.Context, string) (model.PrincipalRef, error)
}

// Admitter is the narrow boundary consumed by RPC/service binding. The token
// comes from the transport, never from an actor/principal in RPC parameters.
// Network transports must first use AdmitHTTP and Listen's posture checks.
type Admitter interface {
	Admit(context.Context, string) (Identity, error)
}

type Config struct {
	Provider Provider
	// AuthorizationInstalled reads the current installed chain, not a caller
	// supplied claim. Nil means no authorization plugin is installed.
	AuthorizationInstalled func() bool
}

type Authenticator struct {
	provider Provider
	authz    func() bool
}

func New(config Config) (*Authenticator, error) {
	a := &Authenticator{provider: config.Provider, authz: config.AuthorizationInstalled}
	if a.provider == nil && a.hasAuthorization() {
		return nil, ErrAuthzRequiresAuthn
	}
	return a, nil
}

func (a *Authenticator) hasAuthorization() bool { return a.authz != nil && a.authz() }

// Identity has no caller-settable proof flags. Only anonymous and bearer
// identities exist in this slice; even a User authenticated by API key fails
// RequireInteractiveUser. Interactive sessions/proofs are a separate feature.
type Identity struct {
	principal model.PrincipalRef
}

// Principal returns a detached copy so callers cannot mutate stored caveats or
// sponsor identity. The zero Identity has no authenticated principal.
func (i Identity) Principal() model.PrincipalRef {
	p, _ := copyPrincipal(i.principal)
	return p
}

func (i Identity) RequireInteractiveUser() error { return ErrHumanPresenceRequired }

// Admit is for trusted in-process/stdio callers and prevalidated transports.
// Errors always return the zero Identity, never a partially admitted principal.
func (a *Authenticator) Admit(ctx context.Context, token string) (Identity, error) {
	if ctx.Err() != nil {
		return Identity{}, ErrUnavailable
	}
	if a.provider == nil {
		if token != "" {
			return Identity{}, ErrUnauthenticated
		}
		if a.hasAuthorization() {
			return Identity{}, ErrAuthzRequiresAuthn
		}
		return Identity{principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}}, nil
	}
	if token == "" || len(token) > 4096 || strings.ContainsAny(token, " \t\r\n") {
		return Identity{}, ErrUnauthenticated
	}
	ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	p, err := a.provider.Authenticate(ctx, token)
	if ctx.Err() != nil {
		return Identity{}, ErrUnavailable
	}
	if err != nil {
		// Never propagate a provider error message that could contain secrets.
		for _, code := range []Failure{ErrUnauthenticated, ErrTokenExpired, ErrAuthzNotInstalled} {
			if errors.Is(err, code) {
				return Identity{}, code
			}
		}
		return Identity{}, ErrUnavailable
	}
	if err := ValidatePrincipal(p); err != nil {
		return Identity{}, ErrUnauthenticated
	}
	if IsScoped(p) && !a.hasAuthorization() {
		return Identity{}, ErrAuthzNotInstalled
	}
	p, err = copyPrincipal(p)
	if err != nil {
		return Identity{}, ErrUnauthenticated
	}
	return Identity{principal: p}, nil
}

// IsScoped also treats an Agent credential as scoped when its caveats are
// empty: without an authorizer neither its sponsor nor its bounds are enforced.
func IsScoped(p model.PrincipalRef) bool { return p.Kind == "agent" || len(p.Caveats) != 0 }

// ValidatePrincipal rejects internal host/system identities at the provider
// boundary. A provider may authenticate a User, Service or sponsored Agent.
func ValidatePrincipal(p model.PrincipalRef) error {
	if !validSubject(p) {
		return ErrUnauthenticated
	}
	if p.Kind == "agent" {
		if p.Sponsor == nil || p.Sponsor.Kind == "agent" || !validSubject(*p.Sponsor) || p.Sponsor.Sponsor != nil {
			return ErrUnauthenticated
		}
	} else if p.Sponsor != nil {
		return ErrUnauthenticated
	}
	if _, err := json.Marshal(p); err != nil {
		return ErrUnauthenticated
	}
	return nil
}

func validSubject(p model.PrincipalRef) bool {
	switch p.Kind {
	case "user", "service", "agent":
		return strings.HasPrefix(p.ID, p.Kind+":") && len(p.ID) > len(p.Kind)+1 &&
			!strings.ContainsAny(p.ID, " \t\r\n")
	default:
		return false
	}
}

func copyPrincipal(p model.PrincipalRef) (model.PrincipalRef, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return model.PrincipalRef{}, err
	}
	var result model.PrincipalRef
	err = json.Unmarshal(data, &result)
	return result, err
}

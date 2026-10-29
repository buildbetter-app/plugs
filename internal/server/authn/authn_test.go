package authn

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type providerFunc func(context.Context, string) (model.PrincipalRef, error)

func (f providerFunc) Authenticate(ctx context.Context, token string) (model.PrincipalRef, error) {
	return f(ctx, token)
}

func newAuthenticator(t *testing.T, config Config) *Authenticator {
	t.Helper()
	a, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestAnonymousInProcess(t *testing.T) {
	a := newAuthenticator(t, Config{})
	var boundary Admitter = a
	identity, err := boundary.Admit(context.Background(), "")
	if err != nil || identity.Principal().Kind != "anonymous" || identity.Principal().ID != "anonymous" {
		t.Fatal("default admission did not stamp anonymous")
	}
	if !errors.Is(identity.RequireInteractiveUser(), ErrHumanPresenceRequired) {
		t.Fatal("anonymous admission supplied interactive proof")
	}
	identity, err = a.Admit(context.Background(), "unsupported-credential")
	assertDenied(t, identity, err, ErrUnauthenticated)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	identity, err = a.Admit(ctx, "")
	assertDenied(t, identity, err, ErrUnavailable)
}

func TestAuthorizationRequiresProvider(t *testing.T) {
	installed := true
	_, err := New(Config{AuthorizationInstalled: func() bool { return installed }})
	if !errors.Is(err, ErrAuthzRequiresAuthn) {
		t.Fatal("authorization was accepted without authentication")
	}
	installed = false
	a := newAuthenticator(t, Config{AuthorizationInstalled: func() bool { return installed }})
	installed = true
	i, err := a.Admit(context.Background(), "")
	assertDenied(t, i, err, ErrAuthzRequiresAuthn)
}

func TestProviderFailuresNeverBecomeAnonymous(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want Failure
	}{
		{"bad credential", ErrUnauthenticated, ErrUnauthenticated},
		{"expired", ErrTokenExpired, ErrTokenExpired},
		{"wrapped expired", fmt.Errorf("private detail: %w", ErrTokenExpired), ErrTokenExpired},
		{"catalog expired", &model.Error{Message: "sensitive-provider-detail", Data: model.ErrorData{Code: "token_expired"}}, ErrTokenExpired},
		{"nil catalog error", (*model.Error)(nil), ErrUnavailable},
		{"scoped refusal", ErrAuthzNotInstalled, ErrAuthzNotInstalled},
		{"provider failure", errors.New("sensitive-provider-detail"), ErrUnavailable},
		{"canceled provider", context.Canceled, ErrUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newAuthenticator(t, Config{Provider: providerFunc(func(context.Context, string) (model.PrincipalRef, error) {
				return model.PrincipalRef{Kind: "user", ID: "user:test"}, test.err
			})})
			i, err := a.Admit(context.Background(), "placeholder")
			assertDenied(t, i, err, test.want)
			if err.Error() != string(test.want) {
				t.Fatal("provider details escaped the admission boundary")
			}
		})
	}
}

func TestConfiguredNilProviderIsNotAnonymous(t *testing.T) {
	var provider providerFunc
	a, err := New(Config{Provider: provider})
	if a != nil || !errors.Is(err, ErrUnavailable) {
		t.Fatal("configured nil provider was accepted as an anonymous configuration")
	}
}

func TestProviderRequiresCredentialAndHonorsDeadline(t *testing.T) {
	calls := 0
	a := newAuthenticator(t, Config{Provider: providerFunc(func(ctx context.Context, _ string) (model.PrincipalRef, error) {
		calls++
		<-ctx.Done()
		return model.PrincipalRef{}, ctx.Err()
	})})
	for _, token := range []string{"", " ", "contains spaces", "contains\nnewline"} {
		i, err := a.Admit(context.Background(), token)
		assertDenied(t, i, err, ErrUnauthenticated)
	}
	if calls != 0 {
		t.Fatal("malformed credentials reached provider")
	}
	i, err := a.Admit(context.Background(), "placeholder")
	assertDenied(t, i, err, ErrUnavailable)
	if calls != 1 {
		t.Fatal("provider did not execute")
	}
}

func TestScopedAdmissionAndDetachedIdentity(t *testing.T) {
	principal := model.PrincipalRef{
		Kind: "agent", ID: "agent:worker",
		Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:sponsor"},
		Caveats: map[string]any{"projects": []any{"project:one"}},
	}
	installed := false
	a := newAuthenticator(t, Config{
		Provider:               providerFunc(func(context.Context, string) (model.PrincipalRef, error) { return principal, nil }),
		AuthorizationInstalled: func() bool { return installed },
	})
	i, err := a.Admit(context.Background(), "placeholder")
	assertDenied(t, i, err, ErrAuthzNotInstalled)
	installed = true
	i, err = a.Admit(context.Background(), "placeholder")
	if err != nil {
		t.Fatal(err)
	}
	principal.Sponsor.ID = "user:changed"
	principal.Caveats["projects"].([]any)[0] = "project:changed"
	copy := i.Principal()
	if copy.Sponsor.ID != "user:sponsor" || copy.Caveats["projects"].([]any)[0] != "project:one" {
		t.Fatal("provider mutated admitted identity")
	}
	copy.Sponsor.ID = "user:changed-again"
	copy.Caveats["projects"].([]any)[0] = "project:changed-again"
	if i.Principal().Sponsor.ID != "user:sponsor" || i.Principal().Caveats["projects"].([]any)[0] != "project:one" {
		t.Fatal("consumer mutated admitted identity")
	}
	installed = false
	i, err = a.Admit(context.Background(), "placeholder")
	assertDenied(t, i, err, ErrAuthzNotInstalled)
}

func TestProviderCannotStampInternalOrInvalidPrincipals(t *testing.T) {
	for _, p := range []model.PrincipalRef{
		{},
		{Kind: "anonymous", ID: "anonymous"},
		{Kind: "system", ID: "system:core"},
		{Kind: "plugin", ID: "plugin:example/one"},
		{Kind: "user", ID: "system:core"},
		{Kind: "user", ID: "user:"},
		{Kind: "agent", ID: "agent:worker"},
		{Kind: "agent", ID: "agent:worker", Sponsor: &model.PrincipalRef{Kind: "system", ID: "system:core"}},
		{Kind: "service", ID: "service:test", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:test"}},
		{Kind: "user", ID: "user:test", Caveats: map[string]any{"invalid": math.Inf(1)}},
	} {
		a := newAuthenticator(t, Config{Provider: providerFunc(func(context.Context, string) (model.PrincipalRef, error) { return p, nil })})
		i, err := a.Admit(context.Background(), "placeholder")
		assertDenied(t, i, err, ErrUnauthenticated)
	}
}

func TestUserBearerNeverProvidesInteractiveProof(t *testing.T) {
	a := newAuthenticator(t, Config{Provider: providerFunc(func(context.Context, string) (model.PrincipalRef, error) {
		return model.PrincipalRef{Kind: "user", ID: "user:admin"}, nil
	})})
	i, err := a.Admit(context.Background(), "placeholder")
	if err != nil || i.Principal().Kind != "user" {
		t.Fatal("bearer user was not authenticated")
	}
	if !errors.Is(i.RequireInteractiveUser(), ErrHumanPresenceRequired) {
		t.Fatal("API-key user acquired interactive human proof")
	}
	if !errors.Is((Identity{}).RequireInteractiveUser(), ErrHumanPresenceRequired) {
		t.Fatal("zero identity acquired human proof")
	}
}

func TestFailuresMapToCatalogErrors(t *testing.T) {
	for _, failure := range []Failure{ErrUnauthenticated, ErrTokenExpired, ErrUnavailable, ErrForbidden,
		ErrAuthzNotInstalled, ErrAuthzRequiresAuthn, ErrHumanPresenceRequired} {
		var target *model.Error
		if !errors.As(fmt.Errorf("wrapped: %w", failure), &target) || string(target.Data.Code) != string(failure) {
			t.Fatal("authn error did not map to canonical model error")
		}
		if target.Data.Retryable != (failure == ErrUnavailable) {
			t.Fatal("incorrect retryability")
		}
	}
}

func assertDenied(t *testing.T, identity Identity, err error, want error) {
	t.Helper()
	if !errors.Is(err, want) || identity.Principal().ID != "" {
		t.Fatalf("admission did not fail closed: error=%v", err)
	}
}

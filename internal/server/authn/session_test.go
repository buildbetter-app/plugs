package authn

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type sessionSourceFunc func(context.Context, string) (CoreSession, error)

func (f sessionSourceFunc) VerifyCoreSession(ctx context.Context, token string) (CoreSession, error) {
	return f(ctx, token)
}

type testSession struct {
	user model.PrincipalRef
	err  error
}

func (s *testSession) Principal() model.PrincipalRef { return s.user }
func (s *testSession) RequireInteractiveUser() error { return s.err }

func TestTrustedSessionProofIsLiveAndBoundToExecutingUser(t *testing.T) {
	s := &testSession{user: model.PrincipalRef{Kind: "user", ID: "user:one"}}
	a := newAuthenticator(t, Config{CoreSessions: sessionSourceFunc(func(context.Context, string) (CoreSession, error) { return s, nil })})
	i, err := a.Admit(context.Background(), "plg_s_test")
	if err != nil || i.RequireInteractiveUser() != nil {
		t.Fatal("configured session did not supply live proof")
	}
	ctx := WithIdentity(context.Background(), i)
	if RequireInteractiveUser(ctx, i.Principal()) != nil {
		t.Fatal("transported session proof was lost")
	}
	for _, by := range []model.PrincipalRef{
		{Kind: "user", ID: "user:other"},
		{Kind: "service", ID: "service:one"},
		{Kind: "user", ID: "user:one", Caveats: map[string]any{"interactive": true}},
	} {
		if !errors.Is(RequireInteractiveUser(ctx, by), ErrHumanPresenceRequired) {
			t.Fatal("another executing identity reused User proof")
		}
	}
	if !errors.Is(RequireInteractiveUser(context.Background(), s.user), ErrHumanPresenceRequired) {
		t.Fatal("principal-only context fabricated a session")
	}
	s.err = ErrUnauthenticated
	if !errors.Is(i.RequireInteractiveUser(), ErrUnauthenticated) ||
		!errors.Is(RequireInteractiveUser(ctx, i.Principal()), ErrUnauthenticated) {
		t.Fatal("retained identity survived revocation")
	}
	s.err = ErrTokenExpired
	if !errors.Is(i.RequireInteractiveUser(), ErrTokenExpired) {
		t.Fatal("retained identity survived expiry")
	}
}

func TestReservedSessionsNeverFallThroughToOrdinaryProvider(t *testing.T) {
	called := false
	a := newAuthenticator(t, Config{Provider: providerFunc(func(context.Context, string) (model.PrincipalRef, error) {
		called = true
		return model.PrincipalRef{Kind: "user", ID: "user:one"}, nil
	})})
	i, err := a.Admit(context.Background(), "plg_s_invalid")
	assertDenied(t, i, err, ErrUnauthenticated)
	if called {
		t.Fatal("session credential reached ordinary provider")
	}
	i, err = a.Admit(context.Background(), "ordinary")
	if err != nil || !called || !errors.Is(i.RequireInteractiveUser(), ErrHumanPresenceRequired) {
		t.Fatal("ordinary User provider gained session proof or stopped working")
	}
}

func TestSessionSourceFailsClosedAndSanitizesErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		s    CoreSession
		err  error
		want error
	}{
		{"nil", nil, nil, ErrUnavailable},
		{"source failure", nil, errors.New("private-provider-detail"), ErrUnavailable},
		{"expired", nil, ErrTokenExpired, ErrTokenExpired},
		{"agent", &testSession{user: model.PrincipalRef{Kind: "agent", ID: "agent:one"}}, nil, ErrUnauthenticated},
		{"claims flag", &testSession{user: model.PrincipalRef{Kind: "user", ID: "user:one", Caveats: map[string]any{"interactive": true}}}, nil, ErrUnauthenticated},
		{"revoked", &testSession{user: model.PrincipalRef{Kind: "user", ID: "user:one"}, err: ErrUnauthenticated}, nil, ErrUnauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newAuthenticator(t, Config{CoreSessions: sessionSourceFunc(func(context.Context, string) (CoreSession, error) { return tc.s, tc.err })})
			i, err := a.Admit(context.Background(), "plg_s_test")
			assertDenied(t, i, err, tc.want)
			if err.Error() != tc.want.Error() {
				t.Fatal("private session failure escaped")
			}
		})
	}
	var source sessionSourceFunc
	if _, err := New(Config{CoreSessions: source}); !errors.Is(err, ErrUnavailable) {
		t.Fatal("typed-nil trusted authority was accepted")
	}
}

package oidc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/plugins/authnoidc"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

func configured(t *testing.T, config authnoidc.Config) (*authnoidc.Provider, *authn.Authenticator) {
	t.Helper()
	p, err := authnoidc.New(config)
	if err != nil {
		t.Fatal(err)
	}
	a, err := authn.New(authn.Config{CoreSessions: p})
	if err != nil {
		t.Fatal(err)
	}
	return p, a
}

func login(t *testing.T, idp *mockIDP, p *authnoidc.Provider) authnoidc.IssuedSession {
	t.Helper()
	flow, err := p.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state, code := idp.authorize(flow)
	session, err := p.Complete(context.Background(), state, flow.Binding, code)
	if err != nil {
		t.Fatalf("verified login failed: %v", err)
	}
	return session
}

func TestSignedOIDCFlowIssuesRevocableInteractivePlugSession(t *testing.T) {
	idp := newIDP(t)
	p, a := configured(t, idp.config())
	session := login(t, idp, p)
	identity, err := a.Admit(context.Background(), session.Token)
	if err != nil || identity.Principal().ID != "user:nikhil" || identity.RequireInteractiveUser() != nil {
		t.Fatal("verified login did not supply a Plug User session")
	}
	ctx := authn.WithIdentity(context.Background(), identity)
	if authn.RequireInteractiveUser(ctx, identity.Principal()) != nil {
		t.Fatal("live proof was not composable")
	}
	if !session.ExpiresAt.Equal(idp.clock().Add(15*time.Minute)) || idp.tokens != 1 || idp.keys != 1 {
		t.Fatal("flow did not perform actual bounded token/JWKS requests")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", session, session), session.Token) {
		t.Fatal("formatted issuance exposed bearer secret")
	}
	encoded, err := json.Marshal(session)
	if err != nil || strings.Contains(string(encoded), session.Token) {
		t.Fatal("JSON issuance exposed bearer secret")
	}
	if !p.Revoke(session.ID) {
		t.Fatal("logout failed")
	}
	if _, err := a.Admit(context.Background(), session.Token); !errors.Is(err, authn.ErrUnauthenticated) {
		t.Fatal("revoked session was admitted")
	}
	if !errors.Is(identity.RequireInteractiveUser(), authn.ErrUnauthenticated) ||
		!errors.Is(authn.RequireInteractiveUser(ctx, identity.Principal()), authn.ErrUnauthenticated) {
		t.Fatal("retained proof survived revocation")
	}
}

func TestRetainedSessionProofExpiresAtExactBound(t *testing.T) {
	idp := newIDP(t)
	config := idp.config()
	config.SessionTTL = time.Minute
	p, a := configured(t, config)
	session := login(t, idp, p)
	identity, err := a.Admit(context.Background(), session.Token)
	if err != nil {
		t.Fatal(err)
	}
	idp.now.Store(session.ExpiresAt.Unix())
	if !errors.Is(identity.RequireInteractiveUser(), authn.ErrTokenExpired) {
		t.Fatal("retained session proof survived half-open expiry")
	}
	if _, err := a.Admit(context.Background(), session.Token); !errors.Is(err, authn.ErrTokenExpired) {
		t.Fatal("expired session was admitted")
	}
}

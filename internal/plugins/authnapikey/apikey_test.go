package authnapikey

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

func TestIssueAuthenticateAndRevoke(t *testing.T) {
	p := New(Config{})
	issued, err := p.Issue(model.PrincipalRef{Kind: "service", ID: "service:ci"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	principal, err := p.Authenticate(context.Background(), issued.Token)
	if err != nil || principal.ID != "service:ci" {
		t.Fatal("issued credential failed authentication")
	}
	if !p.Revoke(issued.ID) {
		t.Fatal("issued key was not revoked")
	}
	principal, err = p.Authenticate(context.Background(), issued.Token)
	if !errors.Is(err, authn.ErrUnauthenticated) || principal.ID != "" {
		t.Fatal("revoked credential was accepted")
	}
}

func TestMalformedAndMismatchedCredentials(t *testing.T) {
	p := New(Config{})
	first, err := p.Issue(model.PrincipalRef{Kind: "user", ID: "user:first"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Issue(model.PrincipalRef{Kind: "service", ID: "service:second"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	_, firstSecret, _ := strings.Cut(strings.TrimPrefix(first.Token, "plg_k_"), "_")
	_, secondSecret, _ := strings.Cut(strings.TrimPrefix(second.Token, "plg_k_"), "_")
	for _, token := range []string{
		"", "unsupported", "Bearer " + first.Token,
		strings.Replace(first.Token, "plg_k_", "plg_x_", 1),
		first.Token[:len(first.Token)-1], first.Token + " ",
		"plg_k_" + first.ID + "_" + secondSecret,
		"plg_k_" + strings.Repeat("z", 32) + "_" + firstSecret,
		"plg_k_" + first.ID + "_" + strings.Repeat("!", 43),
	} {
		principal, err := p.Authenticate(context.Background(), token)
		if !errors.Is(err, authn.ErrUnauthenticated) || principal.ID != "" {
			t.Fatal("malformed or mismatched credential did not fail closed")
		}
	}
	// A token for another valid key keeps its own authoritative identity.
	principal, err := p.Authenticate(context.Background(), second.Token)
	if err != nil || principal.ID != "service:second" {
		t.Fatal("second credential lost its principal binding")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	principal, err = p.Authenticate(ctx, first.Token)
	if !errors.Is(err, authn.ErrUnavailable) || principal.ID != "" {
		t.Fatal("canceled credential check was admitted")
	}
	for _, rendered := range []string{fmt.Sprint(first), fmt.Sprintf("%+v", first), fmt.Sprintf("%#v", first)} {
		if strings.Contains(rendered, first.Token) || strings.Contains(rendered, firstSecret) {
			t.Fatal("issued key formatting exposed its secret")
		}
	}
}

func TestExpiryIsHalfOpenAndRequiresCorrectCredential(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	p := New(Config{Now: func() time.Time { return now }})
	subject := model.PrincipalRef{Kind: "user", ID: "user:test"}
	for _, expiry := range []time.Time{{}, now.Add(-time.Second), now} {
		if _, err := p.Issue(subject, expiry); err == nil {
			t.Fatal("expired or unbounded credential was issued")
		}
	}
	issued, err := p.Issue(subject, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	now = issued.ExpiresAt.Add(-time.Nanosecond)
	if _, err := p.Authenticate(context.Background(), issued.Token); err != nil {
		t.Fatal("credential expired before its expiry boundary")
	}
	now = issued.ExpiresAt
	principal, err := p.Authenticate(context.Background(), issued.Token)
	if !errors.Is(err, authn.ErrTokenExpired) || principal.ID != "" {
		t.Fatal("credential was accepted at its expiry boundary")
	}
	// A wrong secret must not reveal the expiry state of a known key ID.
	wrong := "plg_k_" + issued.ID + "_" + strings.Repeat("A", 43)
	if _, err := p.Authenticate(context.Background(), wrong); !errors.Is(err, authn.ErrUnauthenticated) {
		t.Fatal("wrong secret revealed key expiry")
	}
	p.Revoke(issued.ID)
	if _, err := p.Authenticate(context.Background(), issued.Token); !errors.Is(err, authn.ErrUnauthenticated) {
		t.Fatal("revoked key remained distinguishable as an expired credential")
	}
}

func TestScopedIssuanceAndCurrentAuthorizationInstallation(t *testing.T) {
	installed := false
	authz := func() bool { return installed }
	p := New(Config{AuthorizationInstalled: authz})
	principal := model.PrincipalRef{
		Kind: "agent", ID: "agent:worker",
		Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:sponsor"},
		Caveats: map[string]any{"projects": []any{"project:one"}},
	}
	for _, subject := range []model.PrincipalRef{
		principal,
		{Kind: "agent", ID: "agent:empty", Sponsor: principal.Sponsor},
		{Kind: "user", ID: "user:scoped", Caveats: principal.Caveats},
	} {
		if _, err := p.Issue(subject, time.Now().Add(time.Hour)); !errors.Is(err, authn.ErrAuthzNotInstalled) {
			t.Fatal("scoped credential was issued without authorization")
		}
	}
	installed = true
	issued, err := p.Issue(principal, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	principal.Sponsor.ID = "user:changed"
	principal.Caveats["projects"].([]any)[0] = "project:changed"
	a, err := authn.New(authn.Config{Provider: p, AuthorizationInstalled: authz})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := a.Admit(context.Background(), issued.Token)
	if err != nil || identity.Principal().Sponsor.ID != "user:sponsor" || identity.Principal().Caveats["projects"].([]any)[0] != "project:one" {
		t.Fatal("scoped principal was not frozen at issuance")
	}
	copy := identity.Principal()
	copy.Sponsor.ID = "user:changed-again"
	copy.Caveats["projects"].([]any)[0] = "project:changed-again"
	current, err := p.Authenticate(context.Background(), issued.Token)
	if err != nil || current.Sponsor.ID != "user:sponsor" || current.Caveats["projects"].([]any)[0] != "project:one" {
		t.Fatal("request mutated provider's stored principal")
	}
	installed = false
	current, err = p.Authenticate(context.Background(), issued.Token)
	if !errors.Is(err, authn.ErrAuthzNotInstalled) || current.ID != "" {
		t.Fatal("scoped key survived removal of the authorization chain")
	}
	identity, err = a.Admit(context.Background(), issued.Token)
	if !errors.Is(err, authn.ErrAuthzNotInstalled) || identity.Principal().ID != "" {
		t.Fatal("admission cached a scoped identity after chain removal")
	}
}

func TestIssuanceRejectsInternalPrincipals(t *testing.T) {
	p := New(Config{})
	for _, principal := range []model.PrincipalRef{
		{Kind: "anonymous", ID: "anonymous"},
		{Kind: "system", ID: "system:core"},
		{Kind: "plugin", ID: "plugin:example/one"},
		{Kind: "user", ID: "service:someone"},
	} {
		if _, err := p.Issue(principal, time.Now().Add(time.Hour)); !errors.Is(err, authn.ErrUnauthenticated) {
			t.Fatal("host/internal identity was issued as an API key")
		}
	}
}

func TestConcurrentChecksAndAcknowledgedRevocation(t *testing.T) {
	p := New(Config{})
	issued, err := p.Issue(model.PrincipalRef{Kind: "service", ID: "service:ci"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	start, revoked := make(chan struct{}), make(chan struct{})
	for range 8 {
		wg.Go(func() {
			<-start
			for range 20 {
				// Checks overlapping revocation may linearize on either side.
				_, _ = p.Authenticate(context.Background(), issued.Token)
			}
			<-revoked
			if principal, err := p.Authenticate(context.Background(), issued.Token); !errors.Is(err, authn.ErrUnauthenticated) || principal.ID != "" {
				t.Error("credential was accepted after acknowledged revocation")
			}
		})
	}
	close(start)
	if !p.Revoke(issued.ID) {
		t.Fatal("revocation did not acknowledge an issued key")
	}
	close(revoked)
	wg.Wait()
	if p.Revoke("unknown") {
		t.Fatal("unknown key was reported as revoked")
	}
}

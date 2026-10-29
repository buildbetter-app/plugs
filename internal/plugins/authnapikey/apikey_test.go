package authnapikey

import (
	"context"
	"errors"
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

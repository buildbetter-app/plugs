package composition

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

func TestRestoreImportAuthenticatedBindingRequiresLiveInteractiveUser(t *testing.T) {
	a := &restoreImportAdapter{authMode: "authenticated"}
	principal := model.PrincipalRef{Kind: "user", ID: "user:restore"}
	if _, _, _, _, err := a.binding(context.Background(), principal, ""); !errors.Is(err, authn.ErrHumanPresenceRequired) {
		t.Fatalf("provider-shaped user without live session was admitted: %v", err)
	}
	for _, other := range []model.PrincipalRef{
		{Kind: "service", ID: "service:restore"},
		{Kind: "agent", ID: "agent:restore", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:restore"}},
		{Kind: "anonymous", ID: "anonymous"},
	} {
		if _, _, _, _, err := a.binding(context.Background(), other, ""); !errors.Is(err, authn.ErrUnauthenticated) && !errors.Is(err, authn.ErrHumanPresenceRequired) {
			t.Fatalf("non-user principal was not refused: %+v err=%v", other, err)
		}
	}
}

func TestRestoreImportAnonymousBindingKeepsHonestScope(t *testing.T) {
	a := &restoreImportAdapter{authMode: "anonymous"}
	actor := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	mode, got, scope, authority, err := a.binding(context.Background(), actor, "local-agent")
	if err != nil || mode != "anonymous-local" || !reflect.DeepEqual(got, actor) || scope != "local-agent" || authority != "trusted-human-presence" {
		t.Fatalf("anonymous binding=%q %+v %q %q err=%v", mode, got, scope, authority, err)
	}
	if _, _, _, _, err := a.binding(context.Background(), model.PrincipalRef{Kind: "user", ID: "user:operator"}, "local-agent"); !errors.Is(err, authn.ErrHumanPresenceRequired) {
		t.Fatalf("user was promoted through anonymous mode: %v", err)
	}
	if _, _, _, _, err := a.binding(context.Background(), actor, " bad "); err == nil {
		t.Fatal("noncanonical anonymous scope was accepted")
	}
}

func TestRestoreImportAdmissionDigestIsBoundAndOpaque(t *testing.T) {
	actor := model.PrincipalRef{Kind: "user", ID: "user:restore"}
	one, err := importAdmissionDigest("authenticated", actor, "", "restore-admin", "pl", strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 32))
	if err != nil || len(one) != 64 {
		t.Fatalf("digest=%q err=%v", one, err)
	}
	two, err := importAdmissionDigest("authenticated", actor, "", "restore-admin", "pl", strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("d", 32))
	if err != nil || one == two || one == strings.Repeat("a", 64) {
		t.Fatalf("binding digest was not complete/opaque: one=%q two=%q", one, two)
	}
}

func TestRestoreImportAdapterFailsClosedWithoutTrustedHostPieces(t *testing.T) {
	a := &restoreImportAdapter{}
	_, err := a.importArchive(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, strings.NewReader("not-an-archive"), RestoreImportRequest{})
	if !errors.Is(err, converterroundtrip.ErrRestoreUnavailable) {
		t.Fatalf("adapter did not fail closed: %v", err)
	}
}

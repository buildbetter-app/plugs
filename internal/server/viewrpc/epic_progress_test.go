package viewrpc

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestExplicitAnonymousEpicProgressUsesRealSnapshot(t *testing.T) {
	f := newFixture(t)
	source := service.StaticOntologySource(model.RefByID("ontology", model.Natural64(0)))
	cfg, err := NewAnonymousEpicProgressConfig(f.store, source)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	refs := []model.Ref{model.RefByID("child", model.Natural64(0))}
	value, err := cfg.AuthorizeScope(context.Background(), by, r, model.RefByID("parent", model.Natural64(0)), refs)
	if err != nil || len(value.Visible) != 1 || !value.Hidden.IsZero() || value.PolicyBinding == "" {
		t.Fatalf("explicit anonymous scope: %v %v", value, err)
	}
	if err := cfg.ValidateAuthority(context.Background(), by, value.PolicyBinding); err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateAuthority(context.Background(), by, "different-incarnation"); err == nil {
		t.Fatal("incarnation binding was not enforced")
	}
	for _, p := range []model.PrincipalRef{{Kind: "user", ID: "user:operator"}, {Kind: "agent", ID: "agent:worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:operator"}}} {
		if _, err := cfg.AuthorizeScope(context.Background(), p, r, refs[0], refs); err == nil {
			t.Fatal("authenticated principal silently downgraded")
		}
		if err := cfg.ValidateAuthority(context.Background(), p, value.PolicyBinding); err == nil {
			t.Fatal("authenticated binding silently downgraded")
		}
	}
	if _, err := viewops.New(f.store, f.lease, viewops.Config{EpicProgress: cfg, AuthorizeRead: func(context.Context, model.PrincipalRef, model.ResourceRef, string) error { return nil }}); err == nil {
		t.Fatal("anonymous mode combined with a read authorizer")
	}
	if _, err := NewAnonymousEpicProgressConfig(nil, source); err == nil {
		t.Fatal("anonymous mode admitted without a store")
	}
}

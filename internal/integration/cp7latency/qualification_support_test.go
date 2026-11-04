package cp7latency_test

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This is the installed host's complete fixed-chain adapter. The original
// opaque-callback fixture and its historical measurements stay unchanged.
func qualificationViews(t *testing.T, f *fixture, beforeFence func(), configure ...func(*viewops.Config)) *viewops.Service {
	t.Helper()
	selection := f.app.Service.ViewSelection()
	bind := selection.Bind
	selection.Bind = func(ctx context.Context, p model.PrincipalRef, inline string) (*viewops.SelectionAuthority, error) {
		authority, err := bind(ctx, p, inline)
		if err == nil && authority != nil && beforeFence != nil {
			fence := authority.Fence
			authority.Fence = func(ctx context.Context) error { beforeFence(); return fence(ctx) }
		}
		return authority, err
	}
	cfg := viewops.Config{
		OntologyRevision: model.Natural64(1), Selection: selection,
		ParentProjection: &viewops.ParentProjectionConfig{
			Project: func(ctx context.Context, p model.PrincipalRef, r driver.Reader, refs []model.Ref) (viewops.ParentProjectionAuthority, error) {
				a, err := f.app.Checker.AuthorizeParentProjections(ctx, p, r, refs)
				out := viewops.ParentProjectionAuthority{PolicyBinding: a.PolicyBinding, Parents: make(map[string]viewops.ParentDisclosure, len(a.Parents))}
				for id, value := range a.Parents {
					out.Parents[id] = viewops.ParentDisclosure{Exists: value.Exists, Ref: value.Ref}
				}
				return out, err
			},
			ValidateAuthority: f.app.Checker.ValidateReadAuthorityBinding,
		},
	}
	for _, edit := range configure {
		edit(&cfg)
	}
	views, err := viewops.New(f.store.Store, f.lease, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return views
}

func summaryRequest() viewops.RunRequest {
	req := groupedRequest()
	req.Definition.Fields, req.Definition.Sort = []string{}, nil
	return req
}

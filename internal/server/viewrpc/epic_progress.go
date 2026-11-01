package viewrpc

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NewEpicProgressConfig is the trusted composition adapter for EP2's fenced
// current-authority helper. It supplies no new policy or hierarchy write path.
// Pass the result to composition.Config.EpicProgress or viewops.Config before
// binding view.run. An absent capability keeps epic progress unavailable.
func NewEpicProgressConfig(checker *authzrpc.Checker, source service.OntologySourceLocator) (*viewops.EpicProgressConfig, error) {
	if checker == nil || source == nil {
		return nil, fmt.Errorf("epic progress requires the fixed checker and durable ontology source")
	}
	return &viewops.EpicProgressConfig{
		OntologySource: source,
		ValidateAuthority: func(ctx context.Context, by model.PrincipalRef, expected string) error {
			return checker.ValidateReadAuthorityBinding(ctx, by, expected)
		},
		AuthorizeScope: func(ctx context.Context, by model.PrincipalRef, content driver.Reader, parent model.Ref, children []model.Ref) (viewops.EpicProgressAuthority, error) {
			value, err := checker.AuthorizeEpicProgress(ctx, by, content, parent, children)
			return viewops.EpicProgressAuthority{Visible: value.Visible, Hidden: value.Hidden, PolicyBinding: value.PolicyBinding}, err
		},
	}, nil
}

// NewAnonymousEpicProgressConfig explicitly opts an anonymous no-plugin host
// into the same real hierarchy/ontology producer. It is never a fallback for a
// missing fixed checker. Composition rejects this mode with authn/authz, and
// the callbacks independently refuse every authenticated principal.
func NewAnonymousEpicProgressConfig(store viewops.ReadStore, source service.OntologySourceLocator) (*viewops.EpicProgressConfig, error) {
	if store == nil || source == nil {
		return nil, fmt.Errorf("anonymous epic progress requires a read store and durable ontology source")
	}
	refuse := func() error {
		return &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "epic progress requires the configured authenticated authority"}}
	}
	binding := func(incarnation string) string { return "anonymous/no-plugin/1:" + incarnation }
	return &viewops.EpicProgressConfig{
		AnonymousOnly:  true,
		OntologySource: source,
		AuthorizeScope: func(ctx context.Context, by model.PrincipalRef, content driver.Reader, _ model.Ref, children []model.Ref) (viewops.EpicProgressAuthority, error) {
			if by.Kind != "anonymous" || by.ID != "anonymous" {
				return viewops.EpicProgressAuthority{}, refuse()
			}
			if err := ctx.Err(); err != nil {
				return viewops.EpicProgressAuthority{}, err
			}
			if content == nil || content.Snapshot().Incarnation == "" {
				return viewops.EpicProgressAuthority{}, fmt.Errorf("anonymous epic content is unbound")
			}
			return viewops.EpicProgressAuthority{Visible: append([]model.Ref(nil), children...), Hidden: model.Natural64(0), PolicyBinding: binding(content.Snapshot().Incarnation)}, nil
		},
		ValidateAuthority: func(ctx context.Context, by model.PrincipalRef, expected string) error {
			if by.Kind != "anonymous" || by.ID != "anonymous" {
				return refuse()
			}
			meta, err := store.Meta(ctx)
			if err != nil {
				return err
			}
			if meta.Incarnation == "" || expected != binding(meta.Incarnation) {
				return &model.Error{Data: model.ErrorData{Code: "incarnation_changed", Hint: "anonymous epic snapshot belongs to another store incarnation"}}
			}
			return nil
		},
	}, nil
}

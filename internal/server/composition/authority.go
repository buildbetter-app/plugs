package composition

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// bindRuntimeAuthority binds one genuine installed authority to the ontology
// used by both Writer and Service. An installed chain without the distinct
// transition capability remains observed-but-unavailable, rather than silently
// acquiring the no-authorization AZ9 fallback.
func bindRuntimeAuthority(ctx context.Context, config Config, auth authn.Config, store driver.Store, lease driver.ActiveLease, snapshot ontology.Snapshot) (*service.Ontology, *authzrpc.Checker, error) {
	var resolve func(string) ontology.Effective
	if config.Ontology != nil && !(config.NativeImportOntology && snapshot.Digest() == "") {
		resolve = snapshot.Effective
	}
	var checker *authzrpc.Checker
	var observer service.TransitionPermissionObserver
	configured := false
	if config.Authority != nil {
		if config.Ontology != nil && (config.Ontology.Authorizer != nil || config.Ontology.EventContext != nil) {
			return nil, nil, fmt.Errorf("composition: authority conflicts with separately configured ontology write authority")
		}
		configured = true
		observer = func(ctx context.Context, q service.TransitionPermissionRequest, reader port.SnapshotReader) (service.TransitionPermissionObservation, error) {
			return checker.ObserveTransitionPermission(ctx, q, reader)
		}
	} else if config.Ontology != nil && config.Ontology.Authorizer != nil {
		configured = true
		if source, ok := config.Ontology.Authorizer.(service.TransitionPermissionSource); ok {
			observer = source.ObserveTransitionPermission
		}
	} else if auth.AuthorizationInstalled != nil {
		configured = true
		observer = func(context.Context, service.TransitionPermissionRequest, port.SnapshotReader) (service.TransitionPermissionObservation, error) {
			return service.TransitionPermissionObservation{AuthzInstalled: auth.AuthorizationInstalled()}, nil
		}
	}
	var options []service.OntologyOption
	if configured {
		options = append(options, service.WithTransitionPermissionObserver(observer))
	}
	shared := service.NewOntology(resolve, options...)
	if config.Authority != nil {
		var err error
		checker, err = authzrpc.NewChecker(ctx, store, lease, shared, *config.Authority)
		if err != nil {
			return nil, nil, fmt.Errorf("composition: install authority: %w", err)
		}
	}
	return shared, checker, nil
}

package watch

import (
	"context"
	"sync"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

// visibilityAuthorization is a per-watch adapter over the actual service
// authorization. It never grants access. It only remembers resources that
// this watch has delivered so a current denial can become the contract's
// visibility.lost marker.
type visibilityAuthorization struct {
	base    coreevents.Authorization
	mu      sync.Mutex
	visible map[string]bool
}

func (a *visibilityAuthorization) Read(ctx context.Context, principal model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	visibility, err := a.base.Read(ctx, principal, event)
	if err != nil {
		return coreevents.Visibility{}, err
	}
	a.mu.Lock()
	previous := a.visible[event.Resource]
	if visibility.PreviouslyVisible {
		previous = true
	}
	if visibility.Readable {
		a.visible[event.Resource] = true
	} else if previous {
		// Keep the resource marked until this delivery turns it into one
		// visibility.lost marker. The revalidation path removes the marker in
		// the same call below; a second queued frame then stays hidden.
		delete(a.visible, event.Resource)
	}
	a.mu.Unlock()
	visibility.PreviouslyVisible = previous
	return visibility, nil
}

func (a *visibilityAuthorization) ReadResource(ctx context.Context, principal model.PrincipalRef, resource model.AuditResource) (coreevents.ResourceVisibility, error) {
	if resourceAuth, ok := a.base.(coreevents.ResourceAuthorization); ok {
		return resourceAuth.ReadResource(ctx, principal, resource)
	}
	return coreevents.ResourceVisibility{}, coreevents.ErrResourceAuthorization
}

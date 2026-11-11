package authzrpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// ReadyCount traverses the same current-authority sequence as Ready. Every
// page retains its existing cap and fresh authority fence; no raw delegate
// total, hidden row, partial count, or cursor escapes on failure.
func (s *GuardedService) ReadyCount(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.ReadyCount, error) {
	return service.CountReadyPages(ctx, by, req, s.Ready)
}

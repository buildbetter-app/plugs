package sqlite

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
)

func (r *reader) SelectReadyOrder(ctx context.Context, p driver.ReadyOrderPlan) (driver.ReadyOrderResult, error) {
	return readyindex.Select(ctx, r, p)
}

var _ driver.ReadyOrderReader = (*reader)(nil)
var _ driver.ReadyOrderReader = (*transaction)(nil)

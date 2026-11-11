package sqlite

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

func (r *reader) CycleTopology(ctx context.Context, cursor *driver.Cursor, limit int) (driver.Page[driver.CycleTopologyEdge], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.Page[driver.CycleTopologyEdge]{}, err
	}
	if r.owner != nil {
		return driver.Page[driver.CycleTopologyEdge]{}, driver.ErrUnsupported
	}
	return sourceindex.SQLite.CycleTopology(ctx, r.tx, r.meta, r.at, cursor, limit)
}

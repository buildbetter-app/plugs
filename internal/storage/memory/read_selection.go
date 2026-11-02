package memory

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Query holds its original lock and has already checked context and AsOf.
func (r *reader) queryReadSelection(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	empty := driver.Page[driver.Record]{}
	snapshot := driver.ReadSelectionView(r.at, r.incarnation)
	selection, err := driver.FreezeReadSelection(q, snapshot, model.Natural64(r.at).String() == r.state.meta.Head.String())
	if err != nil {
		return empty, err
	}
	page, err := driver.SelectReadMemory(ctx, selection)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return empty, err
	}
	return driver.Page[driver.Record]{ReadSelection: &page, AsOf: *snapshot.At}, nil
}

package sqlite

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Query holds its original lock and has already checked context and AsOf.
func (r *reader) queryReadSelection(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	empty := driver.Page[driver.Record]{}
	snapshot := driver.ReadSelectionView(r.at, r.meta.Incarnation)
	selection, err := driver.FreezeReadSelection(q, snapshot, r.owner == nil && r.at == mustUint(r.meta.Head))
	if err != nil {
		return empty, err
	}
	statement, args, err := driver.ReadSelectionSQL(selection, "sqlite")
	if err != nil {
		return empty, err
	}
	rows, err := r.tx.QueryContext(ctx, statement, args...)
	if err != nil {
		return empty, err
	}
	page, err := driver.ReadSelectionRows(ctx, rows, selection)
	if err != nil {
		return empty, err
	}
	return driver.Page[driver.Record]{ReadSelection: &page, AsOf: *snapshot.At}, nil
}

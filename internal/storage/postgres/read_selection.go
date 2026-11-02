package postgres

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Query holds its original lock and has already checked context and AsOf.
func (r *reader) queryReadSelection(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	empty := driver.Page[driver.Record]{}
	snapshot := driver.ReadSelectionView(r.at, r.meta.Incarnation)
	selection, err := driver.FreezeReadSelection(q, snapshot, r.writer == nil && model.Natural64(r.at).String() == r.meta.Head.String())
	if err != nil {
		return empty, err
	}
	statement, args, err := driver.ReadSelectionSQL(selection, "postgres")
	if err != nil {
		return empty, err
	}
	rows, err := r.sql.QueryContext(ctx, statement, args...)
	if err != nil {
		return empty, err
	}
	page, err := driver.ReadSelectionRows(ctx, rows, selection)
	if err != nil {
		return empty, err
	}
	return driver.Page[driver.Record]{ReadSelection: &page, AsOf: *snapshot.At}, nil
}

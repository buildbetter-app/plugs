package postgres

import (
	"context"
	"fmt"
	"math"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// queryCurrentIndex selects one bounded page through the persistent live
// kind/key index. Content still uses exactly one call-local ordered get batch;
// no body is re-encoded, and history/staged overlays keep their existing path.
// This index is maintained by PostgreSQL with the source records themselves,
// not by an authority cache or a projection that may lag after restore.
func (r *reader) queryCurrentIndex(ctx context.Context, q driver.Query, filter string, start uint64, limit int) (driver.Page[driver.Record], error) {
	if start > math.MaxInt64 {
		return driver.Page[driver.Record]{}, fmt.Errorf("postgres: query cursor index beyond results")
	}
	where := ` FROM plug_postgres.records WHERE NOT deleted`
	args := []any{}
	if kind, ok := q.Filter["kind"]; ok {
		where += ` AND kind=$1`
		args = append(args, kind)
	}
	pageArgs := append(append([]any{}, args...), limit+1, int64(start))
	query := fmt.Sprintf(`SELECT ref`+where+` ORDER BY key LIMIT $%d OFFSET $%d`, len(args)+1, len(args)+2)
	rows, err := r.sql.QueryContext(ctx, query, pageArgs...)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	ordered := make([]driver.Ref, 0, limit+1)
	for rows.Next() {
		var raw []byte
		var ref driver.Ref
		if err = rows.Scan(&raw); err == nil {
			err = decode(raw, &ref)
		}
		if err != nil {
			break
		}
		ordered = append(ordered, ref)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if len(ordered) == 0 && start != 0 {
		var count uint64
		if err = r.sql.QueryRowContext(ctx, `SELECT COUNT(*)`+where, args...).Scan(&count); err != nil {
			return driver.Page[driver.Record]{}, err
		}
		if start > count {
			return driver.Page[driver.Record]{}, fmt.Errorf("postgres: query cursor index beyond results")
		}
	}
	items, err := r.get(ctx, ordered)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if len(items) != len(ordered) {
		return driver.Page[driver.Record]{}, fmt.Errorf("postgres: current read index disagrees with snapshot")
	}
	page := driver.Page[driver.Record]{Items: items, AsOf: model.Natural64(r.at)}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = r.next("query", filter, start+uint64(limit))
	}
	return page, nil
}

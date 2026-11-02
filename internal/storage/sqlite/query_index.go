package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// queryCurrentIndex uses the durable kind/key index only for a pinned current
// read without staged writes. The index contains source identities, never
// authorization decisions. Historical reconstruction and writer overlays keep
// the original path. SQL maintains this index in the source transaction, so
// reopen, restore and incarnation changes cannot reuse an old read image.
func (r *reader) queryCurrentIndex(ctx context.Context, q driver.Query, digest string, start uint64, limit int) (driver.Page[driver.Record], error) {
	if start > math.MaxInt64 {
		return driver.Page[driver.Record]{}, fmt.Errorf("sqlite: cursor index exceeds page")
	}
	from := ` FROM records AS r JOIN objects AS o ON o.key=r.key`
	args := []any{}
	if kind, ok := q.Filter["kind"]; ok {
		from += ` WHERE o.kind=?`
		args = append(args, kind)
	}
	pageArgs := append(append([]any{}, args...), limit+1, int64(start))
	rows, err := r.tx.QueryContext(ctx, `SELECT r.data`+from+` ORDER BY o.key LIMIT ? OFFSET ?`, pageArgs...)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	items := make([]driver.Record, 0, limit+1)
	for rows.Next() {
		var raw []byte
		var record driver.Record
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &record)
		}
		if err != nil {
			break
		}
		items = append(items, record)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if len(items) == 0 && start != 0 {
		var count uint64
		if err = r.tx.QueryRowContext(ctx, `SELECT COUNT(*)`+from, args...).Scan(&count); err != nil {
			return driver.Page[driver.Record]{}, err
		}
		if start > count {
			return driver.Page[driver.Record]{}, fmt.Errorf("sqlite: cursor index exceeds page")
		}
	}
	page := driver.Page[driver.Record]{Items: items, AsOf: model.Natural64(r.at)}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = r.nextCursor("query", digest, start+uint64(limit))
	}
	return page, nil
}

package sqlite

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	sqlite "modernc.org/sqlite"
)

func init() {
	for _, part := range []string{"parent", "number"} {
		part := part
		sqlite.MustRegisterDeterministicScalarFunction("plug_child_"+part, 1, func(_ *sqlite.FunctionContext, args []sqldriver.Value) (sqldriver.Value, error) {
			key, ok := args[0].(string)
			if !ok {
				return "", nil
			}
			parent, number := driver.ChildIDOrder(key)
			if part == "parent" {
				return parent, nil
			}
			return number, nil
		})
	}
}

// The same immutable key projection indexes both live and historical identity
// catalogs. SQLite maintains these B-trees on ordinary/restored record writes;
// opening an existing store builds them from keys, never from bead documents.
const childIDSchema = `
CREATE INDEX IF NOT EXISTS records_numeric_child
 ON records(plug_child_parent(key),plug_child_number(key) DESC,key);
CREATE INDEX IF NOT EXISTS objects_numeric_child
 ON objects(plug_child_parent(key),plug_child_number(key) DESC,key);
`

const currentChildIDSQL = `SELECT r.key,plug_child_number(r.key),o.kind
 FROM records AS r INDEXED BY records_numeric_child JOIN objects AS o ON o.key=r.key
 WHERE plug_child_parent(r.key)=? ORDER BY plug_child_number(r.key) DESC,r.key LIMIT 1`

func (r *reader) MaxChildNumber(ctx context.Context, parent string, excluded []model.Ref) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return 0, err
	}
	exclude := map[string]bool{}
	for _, ref := range excluded {
		key, err := recordKey(ref)
		if err != nil {
			return 0, err
		}
		exclude[key] = true
	}
	var staged map[string]*driver.Record
	if r.owner != nil {
		staged = r.owner.records
	}
	query := currentChildIDSQL
	head, _ := r.meta.Head.Uint64()
	if r.at != head {
		query = `SELECT r.key,plug_child_number(r.key),r.kind FROM objects AS r INDEXED BY objects_numeric_child WHERE plug_child_parent(r.key)=? ORDER BY plug_child_number(r.key) DESC,r.key LIMIT 1`
	}
	maximum := uint64(0)
	args := []any{parent}
	for {
		var key, rank, kind string
		err := r.tx.QueryRowContext(ctx, query, args...).Scan(&key, &rank, &kind)
		if err == sql.ErrNoRows {
			break
		}
		if err != nil {
			return 0, err
		}
		driver.ReportReadWork(ctx, driver.ReadChildIDCandidates, 1)
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		_, replaced := staged[key]
		eligible := !replaced && !exclude[key] && kind == "bead"
		if eligible && r.at != head {
			eligible, err = r.childIDVisible(ctx, key)
			if err != nil {
				return 0, err
			}
		}
		if eligible {
			if rank == "~" {
				return 0, fmt.Errorf("%w: child counter overflow", driver.ErrUnsupported)
			}
			maximum, err = strconv.ParseUint(rank, 10, 64)
			if err != nil {
				return 0, err
			}
			break
		}
		if len(args) == 1 {
			query = strings.Replace(query, " ORDER BY", " AND (plug_child_number(r.key)<? OR (plug_child_number(r.key)=? AND r.key>?)) ORDER BY", 1)
		}
		args = []any{parent, rank, rank, key}
	}
	for key, record := range staged {
		driver.ReportReadWork(ctx, driver.ReadChildIDCandidates, 1)
		if record == nil || record.Kind != "bead" || exclude[key] {
			continue
		}
		p, rank := driver.ChildIDOrder(key)
		if p != parent {
			continue
		}
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		if rank == "~" {
			return 0, fmt.Errorf("%w: child counter overflow", driver.ErrUnsupported)
		}
		n, err := strconv.ParseUint(rank, 10, 64)
		if err != nil {
			return 0, err
		}
		maximum = max(maximum, n)
	}
	return maximum, nil
}

func (r *reader) childIDVisible(ctx context.Context, key string) (bool, error) {
	var live bool
	err := r.tx.QueryRowContext(ctx, `SELECT NOT COALESCE(json_extract(CAST(data AS TEXT),'$.Deleted'),0) FROM versions WHERE key=? AND rev<=? ORDER BY rev DESC LIMIT 1`, key, revKey(r.at)).Scan(&live)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return live, err
}

var _ driver.NumericChildReader = (*reader)(nil)
var _ driver.NumericChildReader = (*transaction)(nil)

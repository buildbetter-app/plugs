package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These pure byte-key projections match driver.ChildIDOrder. Hex parsing
// keeps opaque identity bytes out of text conversion; only ASCII digits are
// decoded as UTF8. Their expression
// indexes are maintained by every existing record publication, including
// imports and deletes. They add no table or create-time census/backfill.
const childIDSchema = `
CREATE OR REPLACE FUNCTION plug_postgres.child_id_parent(k bytea) RETURNS bytea
 LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
 SELECT COALESCE(decode(m[1],'hex'),''::bytea) FROM
 (SELECT regexp_match(encode(k,'hex') COLLATE "C",'^303a69643a((?:[0-9a-f]{2})+)2e((?:3[0-9])+)3a30$') AS m) x
$$;
CREATE OR REPLACE FUNCTION plug_postgres.child_id_number(k bytea) RETURNS text
 LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
 SELECT CASE WHEN n IS NULL OR n='' THEN ''
 WHEN length(n)>20 OR (length(n)=20 AND n COLLATE "C">'18446744073709551615') THEN '~'
 ELSE lpad(n,20,'0') END FROM
 (SELECT ltrim(convert_from(decode(m[2],'hex'),'UTF8'),'0') AS n FROM
 (SELECT regexp_match(encode(k,'hex') COLLATE "C",'^303a69643a((?:[0-9a-f]{2})+)2e((?:3[0-9])+)3a30$') AS m) x) y
$$;
CREATE INDEX IF NOT EXISTS records_numeric_child_live
 ON plug_postgres.records(plug_postgres.child_id_parent(key),plug_postgres.child_id_number(key) COLLATE "C" DESC,key)
 WHERE kind='bead' AND NOT deleted;
CREATE INDEX IF NOT EXISTS records_numeric_child_history
 ON plug_postgres.records(plug_postgres.child_id_parent(key),plug_postgres.child_id_number(key) COLLATE "C" DESC,key)
 WHERE kind='bead';
`

const currentChildIDSQL = `SELECT key,plug_postgres.child_id_number(key)
 FROM plug_postgres.records WHERE kind='bead' AND NOT deleted
 AND plug_postgres.child_id_parent(key)=$1
 AND plug_postgres.child_id_number(key)<>''
 ORDER BY plug_postgres.child_id_number(key) COLLATE "C" DESC,key LIMIT 1`

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
	if r.writer != nil {
		staged = r.writer.records
	}
	query := currentChildIDSQL
	head, _ := r.meta.Head.Uint64()
	if r.at != head {
		query = `SELECT key,plug_postgres.child_id_number(key) FROM plug_postgres.records WHERE kind='bead' AND plug_postgres.child_id_parent(key)=$1 AND plug_postgres.child_id_number(key)<>'' ORDER BY plug_postgres.child_id_number(key) COLLATE "C" DESC,key LIMIT 1`
	}
	maximum := uint64(0)
	args := []any{[]byte(parent)}
	for {
		var raw []byte
		var rank string
		err := r.sql.QueryRowContext(ctx, query, args...).Scan(&raw, &rank)
		if err == sql.ErrNoRows {
			break
		}
		if err != nil {
			return 0, err
		}
		key := string(raw)
		driver.ReportReadWork(ctx, driver.ReadChildIDCandidates, 1)
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		_, replaced := staged[key]
		eligible := !replaced && !exclude[key]
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
			query = strings.Replace(query, " ORDER BY", ` AND (plug_postgres.child_id_number(key) COLLATE "C"<$2 OR (plug_postgres.child_id_number(key) COLLATE "C"=$2 AND key>$3)) ORDER BY`, 1)
		}
		args = []any{[]byte(parent), rank, raw}
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
	err := r.sql.QueryRowContext(ctx, `SELECT COALESCE((convert_from(value,'UTF8')::jsonb->>'Present')::boolean,false) FROM plug_postgres.states WHERE key=$1 AND revision<=$2::numeric ORDER BY revision DESC LIMIT 1`, []byte(key), decimal(r.at)).Scan(&live)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return live, err
}

var _ driver.NumericChildReader = (*reader)(nil)
var _ driver.NumericChildReader = (*transaction)(nil)

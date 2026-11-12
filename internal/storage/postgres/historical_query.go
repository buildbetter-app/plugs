package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Compact projections live on existing version rows, so even sparse equality
// filters select matching keys before any record reconstruction. Base64 keeps
// arbitrary accepted string bytes (including JSON \u0000) out of jsonb text.
func historicalQueryFields(raw []byte) (string, error) {
	var version driver.Version
	if err := decode(raw, &version); err != nil {
		return "", err
	}
	encoded := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	fields := map[string]any{"deleted": version.Deleted, "id": encoded(version.Ref.ID), "key": encoded(version.Ref.Key)}
	if !version.Deleted {
		body := version.Body
		if !version.Base {
			var err error
			body, err = applyPatch(version.Patch)
			if err != nil {
				return "", err
			}
		}
		var object map[string]json.RawMessage
		if err := decode(body, &object); err != nil {
			fields["body_error"] = true
		} else {
			for _, name := range []string{"type", "project", "status"} {
				if raw, exists := object[name]; exists {
					var value string
					// matches treats a present JSON null as the empty string.
					if err := decode(raw, &value); err == nil {
						fields[name] = encoded(value)
					}
				}
			}
		}
	}
	rawFields, err := json.Marshal(fields)
	return string(rawFields), err
}

func populateHistoricalQueryFields(ctx context.Context, q querier, key, revision string, raw []byte) error {
	fields, err := historicalQueryFields(raw)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `UPDATE plug_postgres.versions SET query_fields=$3::jsonb WHERE key=$1 AND revision=$2`, []byte(key), revision, fields)
	return err
}

func ensureHistoricalQueryFields(ctx context.Context, q querier) error {
	if _, err := q.ExecContext(ctx, `ALTER TABLE plug_postgres.versions ADD COLUMN IF NOT EXISTS query_fields jsonb`); err != nil {
		return err
	}
	if _, err := q.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS versions_query_pending ON plug_postgres.versions(key,revision) WHERE query_fields IS NULL`); err != nil {
		return err
	}
	// Replace the initial full-value indexes before backfill. Previously
	// admitted strings are unbounded, and duplicating their full projections
	// beside a record key can exceed PostgreSQL's btree tuple limit.
	for _, field := range []string{"id", "key", "type", "project", "status"} {
		if _, err := q.ExecContext(ctx, `DROP INDEX IF EXISTS plug_postgres.versions_query_`+field); err != nil {
			return err
		}
	}
	for {
		var key, raw []byte
		var revision string
		err := q.QueryRowContext(ctx, `SELECT key,revision::text,value FROM plug_postgres.versions WHERE query_fields IS NULL ORDER BY key,revision LIMIT 1`).Scan(&key, &revision, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if err := populateHistoricalQueryFields(ctx, q, string(key), revision, raw); err != nil {
			return err
		}
	}
	for _, field := range []string{"id", "key", "type", "project", "status"} {
		if _, err := q.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS versions_query_prefix_`+field+` ON plug_postgres.versions((left(query_fields->>'`+field+`',64)),key,revision)`); err != nil {
			return err
		}
	}
	_, err := q.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS versions_query_body_error ON plug_postgres.versions(key,revision) WHERE query_fields->>'body_error'='true'`)
	return err
}

func historicalQueryWhere(filter map[string]any, after string, at uint64) ([]string, []any) {
	where := []string{"v.key > $1", "v.revision=(SELECT v2.revision FROM plug_postgres.versions AS v2 WHERE v2.key=v.key AND v2.revision<=$2 ORDER BY v2.revision DESC LIMIT 1)", "v.query_fields->>'deleted'='false'"}
	args := []any{[]byte(after), decimal(at)}
	identity := []string{}
	body := []string{}
	for _, field := range []string{"kind", "id", "key", "type", "project", "status"} {
		if value, ok := filter[field]; ok {
			pos := len(args) + 1
			if field == "kind" {
				identity = append(identity, fmt.Sprintf("r.kind=$%d", pos))
				args = append(args, value)
			} else {
				// Prefixes bound index tuples even for long admitted strings.
				// The full equality remains inside the selector, before LIMIT,
				// so prefix collisions never consume the page or lookahead.
				predicate := fmt.Sprintf("(left(v.query_fields->>'%s',64)=left($%d::text,64) AND v.query_fields->>'%s'=$%d)", field, pos, field, pos)
				if field == "id" || field == "key" {
					identity = append(identity, predicate)
				} else {
					body = append(body, predicate)
				}
				args = append(args, base64.StdEncoding.EncodeToString([]byte(value.(string))))
			}
		}
	}
	if len(identity) > 0 {
		where = append(where, strings.Join(identity, " AND "))
	}
	if len(body) > 0 {
		where = append(where, "(("+strings.Join(body, " AND ")+") OR v.query_fields->>'body_error'='true')")
	}

	return where, args
}

// Historical continuation accepts a genuine ordinal token issued by a head
// body-filter query before a later commit. Resolve its anchor from compact
// metadata once, then emit ordinary keyset tokens for subsequent pages.
func (r *reader) historicalQueryCursor(ctx context.Context, c *driver.Cursor, digest string, filter map[string]any) (string, uint64, error) {
	if c == nil || c.After != "" {
		return r.queryCursor(c, digest, true)
	}
	index, err := r.cursor(c, "query", digest)
	if err != nil {
		return "", 0, err
	}
	if index == 0 {
		return "", 0, nil
	}
	if index > uint64(1<<63-1) {
		return "", 0, fmt.Errorf("postgres: cursor binding mismatch")
	}
	where, args := historicalQueryWhere(filter, "", r.at)
	args = append(args, int64(index))
	query := `WITH ranked AS (SELECT v.key,ROW_NUMBER() OVER (ORDER BY v.key) AS position
 FROM plug_postgres.versions AS v JOIN plug_postgres.records AS r ON r.key=v.key
 WHERE ` + strings.Join(where, " AND ") + fmt.Sprintf(`) SELECT key FROM ranked WHERE position=$%d`, len(args))
	var key []byte
	err = r.sql.QueryRowContext(ctx, query, args...).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, fmt.Errorf("postgres: cursor binding mismatch")
	}
	if err != nil {
		return "", 0, err
	}
	return string(key), index, nil
}

func (r *reader) historicalQueryBodyError(ctx context.Context, filter map[string]any) error {
	bodyFilter := false
	identity := map[string]any{}
	for field, value := range filter {
		if field == "kind" || field == "id" || field == "key" {
			identity[field] = value
		} else {
			bodyFilter = true
		}
	}
	if !bodyFilter {
		return nil
	}
	where, args := historicalQueryWhere(identity, "", r.at)
	where = append(where, "v.query_fields->>'body_error'='true'")
	var key []byte
	err := r.sql.QueryRowContext(ctx, `SELECT v.key FROM plug_postgres.versions AS v JOIN plug_postgres.records AS r ON r.key=v.key WHERE `+strings.Join(where, " AND ")+` ORDER BY v.key LIMIT 1`, args...).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	value, found, err := r.recordAt(ctx, string(key))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("postgres: historical query projection mismatch")
	}
	_, err = matches(value.record, filter)
	return err
}

// A materialized selection contains only limit+lookahead keys. The outer join
// fetches current bodies only when already visible at the pinned revision;
// otherwise it replays a batched nearest-base history range. That distinction
// preserves the predecessor's raw current-post-image versus patch bytes.
func (r *reader) queryHistorical(ctx context.Context, filter map[string]any, digest, after string, index uint64, limit int) (driver.Page[driver.Record], error) {
	if err := r.historicalQueryBodyError(ctx, filter); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	where, args := historicalQueryWhere(filter, after, r.at)
	selectionFrom := `plug_postgres.versions AS v JOIN plug_postgres.records AS r ON r.key=v.key`
	selectionKey := "v.key"
	if len(filter) == 0 {
		// With no selective predicate, advance ordered record keys and look
		// up each latest visible version before fetching page payloads.
		selectionFrom = `plug_postgres.records AS r JOIN LATERAL (
 SELECT v.key,v.revision,v.query_fields FROM plug_postgres.versions AS v
 WHERE v.key=r.key AND v.revision<=$2 ORDER BY v.revision DESC LIMIT 1
) AS v ON true`
		selectionKey = "r.key"
		where = append(where, "r.key > $1")
	}
	args = append(args, limit+1)
	// Keep the ordered history range lateral to each selected key. A flat
	// outer join lets PostgreSQL join the complete versions relation before
	// applying the page-dependent range, even when the current body is visible.
	query := `WITH selected AS MATERIALIZED (
 SELECT ` + selectionKey + `,v.revision AS last_revision
 FROM ` + selectionFrom + `
 WHERE ` + strings.Join(where, " AND ") + fmt.Sprintf(` ORDER BY `+selectionKey+` LIMIT $%d
)
SELECT s.key,r.kind,r.revision::text,r.revision<=$2,
       CASE WHEN r.revision<=$2 THEN r.ref END,
       CASE WHEN r.revision<=$2 THEN r.body END,
       v.value,h.is_base,h.value
FROM selected AS s
JOIN LATERAL (
 SELECT r.kind,r.revision,r.ref,r.body FROM plug_postgres.records AS r
 WHERE r.key=s.key LIMIT 1
) AS r ON true
JOIN LATERAL (
 SELECT v.value FROM plug_postgres.versions AS v
 WHERE v.key=s.key AND v.revision=s.last_revision LIMIT 1
) AS v ON true
LEFT JOIN LATERAL (
 SELECT b.revision AS first_revision FROM plug_postgres.versions AS b
 WHERE b.key=s.key AND b.revision<=s.last_revision AND b.is_base
 ORDER BY b.revision DESC LIMIT 1
) AS b ON r.revision>$2
LEFT JOIN LATERAL (
 SELECT h.revision,h.is_base,h.value FROM plug_postgres.versions AS h
 WHERE r.revision>$2 AND h.key=s.key AND h.revision>=b.first_revision AND h.revision<=s.last_revision
 ORDER BY h.revision
) AS h ON true
ORDER BY s.key,h.revision`, len(args))
	rows, err := r.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	defer rows.Close()
	items := make([]driver.Record, 0, limit+1)
	keys := make([]string, 0, limit+1)
	key := ""
	var record driver.Record
	found := false
	patches := 0
	currentVisible := false
	finish := func() error {
		if key == "" {
			return nil
		}
		if !found {
			return fmt.Errorf("postgres: historical version has no base")
		}
		driver.ReportReadPayload(ctx, record.Body)
		ok, err := matches(record, filter)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("postgres: historical query projection mismatch")
		}
		items = append(items, record)
		keys = append(keys, key)
		return nil
	}
	for rows.Next() {
		var rawKey, ref, body, latest, raw []byte
		var kind, revision string
		var visible bool
		var base sql.NullBool
		if err := rows.Scan(&rawKey, &kind, &revision, &visible, &ref, &body, &latest, &base, &raw); err != nil {
			return driver.Page[driver.Record]{}, err
		}
		if string(rawKey) != key {
			if err := finish(); err != nil {
				return driver.Page[driver.Record]{}, err
			}
			key = string(rawKey)
			found = false
			patches = 0
			currentVisible = visible
			if visible {
				n, err := parseCounter(revision)
				if err != nil {
					return driver.Page[driver.Record]{}, err
				}
				record = driver.Record{Kind: kind, Revision: model.Natural64(n), Body: bytes.Clone(body)}
				if err := decode(ref, &record.Ref); err != nil {
					return driver.Page[driver.Record]{}, err
				}
				found = true
			} else {
				var last driver.Version
				if err := decode(latest, &last); err != nil {
					return driver.Page[driver.Record]{}, err
				}
				record = driver.Record{Ref: last.Ref, Kind: kind, Revision: last.Revision}
			}
			driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		}
		if !currentVisible && raw != nil {
			var version driver.Version
			if err := decode(raw, &version); err != nil {
				return driver.Page[driver.Record]{}, err
			}
			switch {
			case version.Deleted:
				record.Body = nil
			case version.Base:
				record.Body = bytes.Clone(version.Body)
				found = true
			default:
				patches++
				if patches >= driver.VersionBaseEvery {
					return driver.Page[driver.Record]{}, fmt.Errorf("postgres: stored version base cadence violated")
				}
				body, err := applyPatch(version.Patch)
				if err != nil {
					return driver.Page[driver.Record]{}, err
				}
				record.Body = bytes.Clone(body)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if err := finish(); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	page := driver.Page[driver.Record]{Items: items, AsOf: model.Natural64(r.at)}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = r.nextQueryCursor(digest, keys[limit-1], index+uint64(limit))
	}
	driver.ReportReadWork(ctx, driver.ReadQueryReturned, int64(len(page.Items)))
	return page, nil
}

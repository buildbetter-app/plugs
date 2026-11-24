package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Historical equality predicates must run before LIMIT, including sparse body
// predicates. Keep only their small projections on the existing version row;
// neither a query nor a cursor validation reconstructs discarded bodies. These
// fields are derived during publication/backfill, never during a read.
func historicalQueryFields(raw []byte, streamed bool) ([]byte, error) {
	var v driver.Version
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	fields := map[string]any{"deleted": v.Deleted, "id": v.Ref.ID, "key": v.Ref.Key}
	if !v.Deleted {
		body, err := historicalQueryBody(v)
		if err != nil {
			return nil, err
		}
		if streamed && driver.IsNativeHeadHistoryMarker(v.Ref, "beads-archive") {
			// The admitted marker may contain many authenticated frames.
			// Historical predicates only need three top-level scalars.
			object, err := driver.ReadNativeHistoryMarkerFields(body)
			if err != nil {
				return nil, err
			}
			for _, name := range []string{"type", "project", "status"} {
				value := bytes.TrimSpace(object[name])
				var selected string
				if len(value) != 0 && value[0] == '"' && json.Unmarshal(value, &selected) == nil {
					fields[name] = selected
				}
			}
			return json.Marshal(fields)
		}
		var object map[string]any
		if err := json.Unmarshal(body, &object); err != nil {
			// Body filters preserve the predecessor's decoding error, while
			// unfiltered/kind/id/key queries still admit opaque/nil bodies.
			fields["body_error"] = true
		} else {
			for _, name := range []string{"type", "project", "status"} {
				if value, ok := object[name].(string); ok {
					fields[name] = value
				}
			}
		}
	}
	return json.Marshal(fields)
}

// This is the same root-replacement replay as recordAt. In particular, patch
// values retain their raw JSON bytes rather than being decoded and re-encoded.
func historicalQueryBody(v driver.Version) ([]byte, error) {
	if v.Deleted {
		return nil, nil
	}
	if v.Base {
		return bytes.Clone(v.Body), nil
	}
	var patches []struct {
		Op, Path string
		Value    json.RawMessage
	}
	if err := json.Unmarshal(v.Patch, &patches); err != nil {
		return nil, err
	}
	if len(patches) != 1 || patches[0].Op != "replace" || patches[0].Path != "" {
		return nil, fmt.Errorf("sqlite: invalid forward patch")
	}
	return bytes.Clone(patches[0].Value), nil
}

func populateHistoricalQueryFields(ctx context.Context, c *sql.Conn, key, rev string, raw []byte) error {
	fields, err := historicalQueryFields(raw, driver.NativeReverseRead(ctx))
	if err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, `UPDATE versions SET query_fields=? WHERE key=? AND rev=?`, string(fields), key, rev)
	return err
}

func ensureHistoricalQueryFields(ctx context.Context, c *sql.Conn) error {
	var exists bool
	if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info('versions') WHERE name='query_fields')`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := c.ExecContext(ctx, `ALTER TABLE versions ADD COLUMN query_fields TEXT`); err != nil {
			return err
		}
	}
	// The partial pending index makes subsequent opens constant work when the
	// populated-store backfill has already completed. Bound migration memory
	// to one version, and finish it in the same initialization transaction.
	if _, err := c.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS versions_query_pending ON versions(key,rev) WHERE query_fields IS NULL`); err != nil {
		return err
	}
	for {
		var key, rev string
		var raw []byte
		err := c.QueryRowContext(ctx, `SELECT key,rev,data FROM versions WHERE query_fields IS NULL ORDER BY key,rev LIMIT 1`).Scan(&key, &rev, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if err := populateHistoricalQueryFields(ctx, c, key, rev, raw); err != nil {
			return err
		}
	}
	for _, field := range []string{"id", "key", "type", "project", "status"} {
		if _, err := c.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS versions_query_`+field+` ON versions(json_extract(query_fields,'$.`+field+`'),key,rev)`); err != nil {
			return err
		}
	}
	_, err := c.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS versions_query_body_error ON versions(key,rev) WHERE json_extract(query_fields,'$.body_error')=1`)
	return err
}

const historicalQueryAfterPrefix = "sqh1."

func (r *reader) historicalQueryAfter(key, digest string, index uint64) string {
	binding := key + "\x00" + digest + "\x00" + r.meta.Incarnation + "\x00" + revKey(r.at) + "\x00" + revKey(index)
	hash := sha256.Sum256([]byte(binding))
	return historicalQueryAfterPrefix + base64.RawURLEncoding.EncodeToString([]byte(key)) + "." + hex.EncodeToString(hash[:])
}

func (r *reader) historicalQueryCursor(ctx context.Context, c *driver.Cursor, digest string, filter map[string]any) (string, uint64, error) {
	if c == nil {
		return "", 0, nil
	}
	index, err := r.cursor(c, "query", digest)
	if err != nil {
		return "", 0, err
	}
	if c.After == "" && index == 0 {
		return "", 0, nil
	}
	var key []byte
	if strings.HasPrefix(c.After, historicalQueryAfterPrefix) {
		parts := strings.Split(strings.TrimPrefix(c.After, historicalQueryAfterPrefix), ".")
		if len(parts) != 2 || index == 0 {
			return "", 0, fmt.Errorf("sqlite: cursor binding mismatch")
		}
		key, err = base64.RawURLEncoding.DecodeString(parts[0])
		if err != nil || len(key) == 0 || r.historicalQueryAfter(string(key), digest, index) != c.After {
			return "", 0, fmt.Errorf("sqlite: cursor binding mismatch")
		}
	} else {
		// A cursor genuinely issued at head can become historical after a
		// later commit. Nil/kind tokens carry the raw key, while other head
		// filters carry only an ordinal. Resolve that legacy ordinal using
		// compact metadata, never OFFSET or discarded-body reconstruction.
		if index == 0 || index > uint64(1<<63-1) {
			return "", 0, fmt.Errorf("sqlite: cursor binding mismatch")
		}
		where, args := historicalQueryWhere(filter, "", r.at)
		query := `WITH ranked AS (SELECT v.key,ROW_NUMBER() OVER (ORDER BY v.key) AS position
 FROM versions AS v JOIN objects AS o ON o.key=v.key WHERE ` + strings.Join(where, " AND ") + `)
 SELECT key FROM ranked WHERE position=?`
		args = append(args, int64(index))
		if c.After != "" {
			query += ` AND key=?`
			args = append(args, c.After)
		}
		var anchor string
		if err := r.tx.QueryRowContext(ctx, query, args...).Scan(&anchor); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", 0, fmt.Errorf("sqlite: cursor binding mismatch")
			}
			return "", 0, err
		}
		key = []byte(anchor)
	}
	// Validate the anchor in this same pinned snapshot. Its one reconstructed
	// payload is included in request-local work, just like head validation.
	record, found, err := r.recordAt(ctx, string(key))
	if err != nil {
		return "", 0, err
	}
	if !found {
		return "", 0, fmt.Errorf("sqlite: cursor binding mismatch")
	}
	ok, err := matches(record, filter)
	if err != nil {
		return "", 0, err
	}
	if !ok {
		return "", 0, fmt.Errorf("sqlite: cursor binding mismatch")
	}
	driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
	return string(key), index, nil
}

// historicalQueryWhere filters compact metadata, before bounded payload replay.
func historicalQueryWhere(filter map[string]any, after string, at uint64) ([]string, []any) {
	where := []string{"v.key>?", "v.rev=(SELECT v2.rev FROM versions AS v2 WHERE v2.key=v.key AND v2.rev<=? ORDER BY v2.rev DESC LIMIT 1)", "json_extract(v.query_fields,'$.deleted')=0"}
	args := []any{after, revKey(at)}
	identity := []string{}
	body := []string{}
	for _, field := range []string{"kind", "id", "key", "type", "project", "status"} {
		if value, ok := filter[field]; ok {
			if field == "kind" {
				identity = append(identity, "o.kind=?")
			} else {
				predicate := "json_extract(v.query_fields,'$." + field + "')=?"
				if field == "id" || field == "key" {
					identity = append(identity, predicate)
				} else {
					body = append(body, predicate)
				}
			}
			args = append(args, value)
		}
	}
	if len(identity) > 0 {
		where = append(where, strings.Join(identity, " AND "))
	}
	if len(body) > 0 {
		where = append(where, "(("+strings.Join(body, " AND ")+") OR json_extract(v.query_fields,'$.body_error')=1)")
	}

	return where, args
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
	where = append(where, "json_extract(v.query_fields,'$.body_error')=1")
	var key string
	err := r.tx.QueryRowContext(ctx, `SELECT v.key FROM versions AS v JOIN objects AS o ON o.key=v.key WHERE `+strings.Join(where, " AND ")+` ORDER BY v.key LIMIT 1`, args...).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	record, found, err := r.recordAt(ctx, key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("sqlite: historical query projection mismatch")
	}
	_, err = matches(record, filter)
	return err
}

// queryHistorical first selects revision-visible matching keys with a bounded
// keyset LIMIT, then fetches the selected keys' nearest-base replay ranges in
// one SQL batch. No historical body outside that page/lookahead is rebuilt.
func (r *reader) queryHistorical(ctx context.Context, filter map[string]any, digest, after string, index uint64, limit int) (driver.Page[driver.Record], error) {
	// The predecessor validates body decoding across the entire snapshot
	// before slicing. Locate invalid-body metadata even past lookahead, and
	// reproduce its original decoding error with at most one reconstruction.
	if err := r.historicalQueryBodyError(ctx, filter); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	where, args := historicalQueryWhere(filter, after, r.at)
	args = append(args, limit+1)
	rows, err := r.tx.QueryContext(ctx, `WITH selected AS MATERIALIZED (
 SELECT v.key,v.rev AS last_rev,v.data AS latest,o.kind,
        (SELECT b.rev FROM versions AS b WHERE b.key=v.key AND b.rev<=v.rev AND b.base=1 ORDER BY b.rev DESC LIMIT 1) AS first_rev
 FROM versions AS v JOIN objects AS o ON o.key=v.key
 WHERE `+strings.Join(where, " AND ")+` ORDER BY v.key LIMIT ?
)
SELECT s.key,s.kind,s.latest,h.data FROM selected AS s
LEFT JOIN versions AS h ON h.key=s.key AND h.rev>=s.first_rev AND h.rev<=s.last_rev
ORDER BY s.key,h.rev`, args...)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	defer rows.Close()
	items := make([]driver.Record, 0, limit+1)
	keys := make([]string, 0, limit+1)
	var key string
	var record driver.Record
	var body []byte
	foundBase := false
	finish := func() error {
		if key == "" {
			return nil
		}
		if !foundBase {
			return sql.ErrNoRows
		}
		record.Body = body
		driver.ReportReadPayload(ctx, body)
		ok, err := matches(record, filter)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("sqlite: historical query projection mismatch")
		}
		items = append(items, record)
		keys = append(keys, key)
		return nil
	}
	for rows.Next() {
		var nextKey, kind string
		var latest, raw []byte
		if err := rows.Scan(&nextKey, &kind, &latest, &raw); err != nil {
			return driver.Page[driver.Record]{}, err
		}
		if nextKey != key {
			if err := finish(); err != nil {
				return driver.Page[driver.Record]{}, err
			}
			var last driver.Version
			if err := json.Unmarshal(latest, &last); err != nil {
				return driver.Page[driver.Record]{}, err
			}
			key, body, foundBase = nextKey, nil, false
			record = driver.Record{Ref: last.Ref, Kind: kind, Revision: last.Revision}
			driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		}
		if raw != nil {
			var version driver.Version
			if err := json.Unmarshal(raw, &version); err != nil {
				return driver.Page[driver.Record]{}, err
			}
			body, err = historicalQueryBody(version)
			if err != nil {
				return driver.Page[driver.Record]{}, err
			}
			foundBase = foundBase || version.Base
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
		nextIndex := index + uint64(limit)
		page.NextCursor = r.nextCursor("query", digest, nextIndex)
		page.NextCursor.After = r.historicalQueryAfter(keys[limit-1], digest, nextIndex)
	}
	driver.ReportReadWork(ctx, driver.ReadQueryReturned, int64(len(page.Items)))
	return page, nil
}

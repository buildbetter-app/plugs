package postgres

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
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func applyPatch(raw []byte) ([]byte, error) {
	var ops []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	if err := decode(raw, &ops); err != nil {
		return nil, err
	}
	if len(ops) != 1 || ops[0].Op != "replace" || ops[0].Path != "" || !json.Valid(ops[0].Value) {
		return nil, fmt.Errorf("postgres: invalid stored forward patch")
	}
	return ops[0].Value, nil
}

func (r *reader) cursor(c *driver.Cursor, method, filter string) (uint64, error) {
	if c == nil {
		return 0, nil
	}
	if c.Incarnation != r.meta.Incarnation {
		return 0, driver.ErrIncarnationChanged{From: c.Incarnation, To: r.meta.Incarnation}
	}
	if c.Method != method || c.FilterDigest != filter || c.PrincipalDigest != "" || c.OntologyRevision.Validate() != nil || !c.OntologyRevision.IsZero() {
		return 0, fmt.Errorf("postgres: cursor binding mismatch")
	}
	at, err := c.Revision.Uint64()
	if err != nil {
		return 0, err
	}
	if at != r.at {
		return 0, fmt.Errorf("postgres: cursor snapshot mismatch")
	}
	return c.Index.Uint64()
}

// queryAfter binds the opaque keyset marker to the query snapshot and filter.
// The key is encoded rather than exposed directly so clients cannot infer a
// storage identity from the cursor's public representation. The checksum is a
// corruption/tampering marker; the existing digest, incarnation and snapshot
// checks remain the authoritative cursor bindings.
const queryAfterPrefix = "pgq1."

func encodeQueryAfter(key, filter string, at, index uint64) string {
	data := []byte(key + "\x00" + filter + "\x00" + decimal(at) + "\x00" + decimal(index))
	hash := sha256.Sum256(data)
	return queryAfterPrefix + base64.RawURLEncoding.EncodeToString([]byte(key)) + "." + hex.EncodeToString(hash[:])
}

func decodeQueryAfter(value, filter string, at, index uint64) (string, error) {
	if !strings.HasPrefix(value, queryAfterPrefix) {
		return "", fmt.Errorf("postgres: cursor binding mismatch")
	}
	parts := strings.Split(value[len(queryAfterPrefix):], ".")
	if len(parts) != 2 || parts[0] == "" || len(parts[1]) != sha256.Size*2 {
		return "", fmt.Errorf("postgres: cursor binding mismatch")
	}
	key, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(key) == 0 {
		return "", fmt.Errorf("postgres: cursor binding mismatch")
	}
	hash := sha256.Sum256([]byte(string(key) + "\x00" + filter + "\x00" + decimal(at) + "\x00" + decimal(index)))
	got, err := hex.DecodeString(parts[1])
	if err != nil || len(got) != len(hash) || string(got) != string(hash[:]) {
		return "", fmt.Errorf("postgres: cursor binding mismatch")
	}
	return string(key), nil
}

// queryCursor preserves the predecessor ordinal cursor for staged and
// non-keyset current queries. Head and historical keyset pages use After; a nonzero Index without it
// is an old offset cursor and is refused rather than translated through an
// OFFSET query.
func (r *reader) queryCursor(c *driver.Cursor, filter string, currentHead bool) (string, uint64, error) {
	if c == nil {
		return "", 0, nil
	}
	if c.Incarnation != r.meta.Incarnation {
		return "", 0, driver.ErrIncarnationChanged{From: c.Incarnation, To: r.meta.Incarnation}
	}
	if c.Method != "query" || c.FilterDigest != filter || c.PrincipalDigest != "" || c.OntologyRevision.Validate() != nil || !c.OntologyRevision.IsZero() {
		return "", 0, fmt.Errorf("postgres: cursor binding mismatch")
	}
	revision, err := c.Revision.Uint64()
	if err != nil {
		return "", 0, err
	}
	if revision != r.at {
		return "", 0, fmt.Errorf("postgres: cursor snapshot mismatch")
	}
	if err := c.Index.Validate(); err != nil {
		return "", 0, err
	}
	if !currentHead {
		index, err := r.cursor(c, "query", filter)
		return "", index, err
	}
	if c.After == "" {
		if !c.Index.IsZero() {
			return "", 0, fmt.Errorf("postgres: cursor binding mismatch")
		}
		return "", 0, nil
	}
	// Index remains a contiguous progress marker for callers that guard against
	// a non-advancing page. It never positions the SQL query; After does that.
	index, err := c.Index.Uint64()
	if err != nil || index == 0 {
		return "", 0, fmt.Errorf("postgres: cursor binding mismatch")
	}
	after, err := decodeQueryAfter(c.After, filter, r.at, index)
	if err != nil {
		return "", 0, err
	}
	return after, index, nil
}

func (r *reader) nextQueryCursor(filter, key string, index uint64) *driver.Cursor {
	return &driver.Cursor{
		Incarnation:  r.meta.Incarnation,
		Revision:     model.Natural64(r.at),
		Index:        model.Natural64(index),
		After:        encodeQueryAfter(key, filter, r.at, index),
		Method:       "query",
		FilterDigest: filter,
	}
}
func (r *reader) next(method, filter string, index uint64) *driver.Cursor {
	return &driver.Cursor{Incarnation: r.meta.Incarnation, Revision: model.Natural64(r.at), Index: model.Natural64(index), Method: method, FilterDigest: filter}
}

func matches(record driver.Record, filter map[string]any) (bool, error) {
	var body map[string]json.RawMessage
	for field, want := range filter {
		var got string
		switch field {
		case "kind":
			got = record.Kind
		case "id":
			got = record.Ref.ID
		case "key":
			got = record.Ref.Key
		default:
			if body == nil {
				if err := decode(record.Body, &body); err != nil {
					return false, err
				}
			}
			raw, exists := body[field]
			if !exists {
				return false, nil
			}
			// Non-string body values do not equal string equality filters.
			if err := decode(raw, &got); err != nil {
				return false, nil
			}
		}
		if got != want.(string) {
			return false, nil
		}
	}
	return true, nil
}

func (r *reader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	driver.ReportReadWork(ctx, driver.ReadQueryCalls, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	empty := driver.Page[driver.Record]{}
	if err := r.check(ctx); err != nil {
		return empty, err
	}
	if q.AsOf != nil {
		n, err := q.AsOf.Uint64()
		if err != nil {
			return empty, err
		}
		if n != r.at {
			return empty, fmt.Errorf("postgres: query as-of differs from snapshot")
		}
	}
	for field, value := range q.Filter {
		switch field {
		case "kind", "id", "key", "type", "project", "status":
		default:
			return empty, fmt.Errorf("%w: postgres query filter %q", driver.ErrUnsupported, field)
		}
		if _, ok := value.(string); !ok {
			return empty, fmt.Errorf("postgres: equality filter %q requires string", field)
		}
	}
	filter, err := digest(q.Filter)
	if err != nil {
		return empty, err
	}
	limit, err := limitValue(q.Limit)
	if err != nil {
		return empty, err
	}
	kind, hasKind := q.Filter["kind"]
	head, err := r.meta.Head.Uint64()
	if err != nil {
		return empty, err
	}
	// Only an ordinary read pinned at its current head can use the keyset
	// projection. Staged transactions and other current-head filters retain
	// the predecessor's reconstruction and ordinal cursor path. Historical
	// queries have a separate bounded selector below.
	currentHead := r.writer == nil && r.at == head && (len(q.Filter) == 0 || len(q.Filter) == 1 && hasKind)
	if currentHead {
		after, index, err := r.queryCursor(q.Cursor, filter, true)
		if err != nil {
			return empty, err
		}
		return r.queryHead(ctx, kind, hasKind, filter, after, index, limit)
	}
	if r.writer == nil && r.at < head {
		after, index, err := r.historicalQueryCursor(ctx, q.Cursor, filter, q.Filter)
		if err != nil {
			return empty, err
		}
		return r.queryHistorical(ctx, q.Filter, filter, after, index, limit)
	}
	_, start, err := r.queryCursor(q.Cursor, filter, false)
	if err != nil {
		return empty, err
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT key,ref FROM plug_postgres.records ORDER BY key`)
	if err != nil {
		return empty, err
	}
	refs := make(map[string]driver.Ref)
	for rows.Next() {
		var key, raw []byte
		if err = rows.Scan(&key, &raw); err != nil {
			_ = rows.Close()
			return empty, err
		}
		var ref driver.Ref
		if err = decode(raw, &ref); err != nil {
			_ = rows.Close()
			return empty, err
		}
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		refs[string(key)] = ref
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return empty, err
	}
	if r.writer != nil {
		for key, record := range r.writer.records {
			if record == nil {
				delete(refs, key)
			} else {
				refs[key] = record.Ref
			}
		}
	}
	keys := make([]string, 0, len(refs))
	for key := range refs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	// Reuse the reader's call-local batch reconstruction in the same key
	// order. Per-key calls repeat SQL for every row on every query page;
	// get already preserves historical content, tombstones and staged writes.
	ordered := make([]driver.Ref, len(keys))
	for i, key := range keys {
		ordered[i] = refs[key]
	}
	values, err := r.get(ctx, ordered)
	if err != nil {
		return empty, err
	}
	items := make([]driver.Record, 0)
	for _, value := range values {
		match, err := matches(value, q.Filter)
		if err != nil {
			return empty, err
		}
		if match {
			items = append(items, value)
		}
	}
	if start > uint64(len(items)) {
		return empty, fmt.Errorf("postgres: query cursor index beyond results")
	}
	end := start + uint64(limit)
	if end > uint64(len(items)) {
		end = uint64(len(items))
	}
	page := driver.Page[driver.Record]{Items: items[start:end], AsOf: model.Natural64(r.at)}
	if end < uint64(len(items)) {
		page.NextCursor = r.next("query", filter, end)
	}
	driver.ReportReadWork(ctx, driver.ReadQueryReturned, int64(len(page.Items)))
	return page, nil
}

// queryHead reads a bounded current-head page. Live partial indexes make both
// the unfiltered and kind-filtered key order available without a temporary
// sort, and the cursor's keyset predicate keeps continuation work independent
// of the corpus size.
func (r *reader) queryHead(ctx context.Context, kind any, hasKind bool, filter, after string, index uint64, limit int) (driver.Page[driver.Record], error) {
	page := driver.Page[driver.Record]{AsOf: model.Natural64(r.at)}
	where := []string{"NOT deleted", "key > $1"}
	args := []any{[]byte(after)}
	if hasKind {
		where = append(where, fmt.Sprintf("kind = $%d", len(args)+1))
		args = append(args, kind)
	}
	limitPos := len(args) + 1
	args = append(args, limit+1)
	query := `SELECT key,ref,kind,revision::text,body FROM plug_postgres.records WHERE ` + strings.Join(where, " AND ") + fmt.Sprintf(" ORDER BY key LIMIT $%d", limitPos)
	rows, err := r.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	defer rows.Close()
	items := make([]driver.Record, 0, limit+1)
	keys := make([]string, 0, limit+1)
	for rows.Next() {
		var rawKey, rawRef, body []byte
		var revision string
		var record driver.Record
		if err := rows.Scan(&rawKey, &rawRef, &record.Kind, &revision, &body); err != nil {
			return driver.Page[driver.Record]{}, err
		}
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		driver.ReportReadPayload(ctx, body)
		if err := decode(rawRef, &record.Ref); err != nil {
			return driver.Page[driver.Record]{}, err
		}
		n, err := parseCounter(revision)
		if err != nil {
			return driver.Page[driver.Record]{}, err
		}
		record.Revision = model.Natural64(n)
		record.Body = bytes.Clone(body)
		items = append(items, record)
		keys = append(keys, string(rawKey))
		if len(items) > limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = r.nextQueryCursor(filter, keys[limit-1], index+uint64(limit))
	} else {
		page.Items = items
	}
	driver.ReportReadWork(ctx, driver.ReadQueryReturned, int64(len(page.Items)))
	return page, nil
}

func (r *reader) Versions(ctx context.Context, ref driver.Ref, p driver.PageReq) (driver.Page[driver.Version], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	empty := driver.Page[driver.Version]{}
	if err := r.check(ctx); err != nil {
		return empty, err
	}
	key, err := recordKey(ref)
	if err != nil {
		return empty, err
	}
	start, err := r.cursor(p.Cursor, "versions", key)
	if err != nil {
		return empty, err
	}
	limit, err := limitValue(p.Limit)
	if err != nil {
		return empty, err
	}
	var count int64
	if err = r.sql.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.versions WHERE key=$1 AND revision<=$2`, []byte(key), decimal(r.at)).Scan(&count); err != nil {
		return empty, err
	}
	if start > uint64(count) {
		return empty, fmt.Errorf("postgres: version cursor index beyond results")
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT value FROM plug_postgres.versions WHERE key=$1 AND revision<=$2 ORDER BY revision LIMIT $3 OFFSET $4`, []byte(key), decimal(r.at), limit, int64(start))
	if err != nil {
		return empty, err
	}
	defer rows.Close()
	page := driver.Page[driver.Version]{Items: []driver.Version{}, AsOf: model.Natural64(r.at)}
	for rows.Next() {
		var raw []byte
		var value driver.Version
		if err = rows.Scan(&raw); err == nil {
			err = decode(raw, &value)
		}
		if err != nil {
			return empty, err
		}
		page.Items = append(page.Items, value)
	}
	if err = rows.Err(); err != nil {
		return empty, err
	}
	end := start + uint64(len(page.Items))
	if end < uint64(count) {
		page.NextCursor = r.next("versions", key, end)
	}
	return page, nil
}

func (r *reader) Changesets(ctx context.Context, c *driver.Cursor, limit int) ([]model.Changeset, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	max, err := limitValue(limit)
	if err != nil {
		return nil, err
	}
	var after uint64
	if c != nil {
		if c.Incarnation != r.meta.Incarnation {
			return nil, driver.ErrIncarnationChanged{From: c.Incarnation, To: r.meta.Incarnation}
		}
		if c.Index.Validate() != nil || !c.Index.IsZero() || (c.Method != "" && c.Method != "changesets") || c.FilterDigest != "" || c.PrincipalDigest != "" || c.OntologyRevision.Validate() != nil || !c.OntologyRevision.IsZero() {
			return nil, fmt.Errorf("postgres: invalid changeset cursor binding")
		}
		after, err = c.Revision.Uint64()
		if err != nil {
			return nil, err
		}
	}
	floor, err := r.meta.HistoryFloor.Uint64()
	if err != nil {
		return nil, err
	}
	if after < floor {
		return nil, driver.ErrTruncated{Floor: r.meta.HistoryFloor}
	}
	if after > r.at {
		return nil, fmt.Errorf("postgres: changeset cursor beyond snapshot")
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT value FROM plug_postgres.changesets WHERE revision>$1 AND revision<=$2 ORDER BY revision LIMIT $3`, decimal(after), decimal(r.at), max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []model.Changeset{}
	for rows.Next() {
		var raw []byte
		var value model.Changeset
		if err = rows.Scan(&raw); err == nil {
			err = decode(raw, &value)
		}
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func lookupReceipt(ctx context.Context, q querier, scope []byte, at uint64, retainExpired bool) (*driver.IdempotencyRecord, error) {
	var raw []byte
	err := q.QueryRowContext(ctx, `SELECT value FROM plug_postgres.receipts
		WHERE scope=$1 AND revision<=$2 AND ($3 OR expires>clock_timestamp())`, scope, decimal(at), retainExpired).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	value, err := decodeReceipt(raw)
	return &value, err
}
func (r *reader) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	return r.readReceipt(ctx, principal, agent, method, key, false)
}

func (r *reader) ReadSourceRecoveryReceipt(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	return r.readReceipt(ctx, principal, agent, method, key, driver.RetainSourceImportReceipt(ctx, method))
}

func (r *reader) readReceipt(ctx context.Context, principal, agent, method, key string, retainExpired bool) (*driver.IdempotencyRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	scope, err := scopeKey(principal, agent, method, key)
	if err != nil {
		return nil, err
	}
	return lookupReceipt(ctx, r.sql, scope, r.at, retainExpired)
}
func (r *reader) ChangesetByKey(ctx context.Context, principal, agent, method, key string) (*model.Changeset, error) {
	value, err := r.IdempotencyByKey(ctx, principal, agent, method, key)
	if err != nil || value == nil || value.Changeset.Kind == "" {
		return nil, err
	}
	return &value.Changeset, nil
}

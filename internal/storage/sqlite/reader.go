package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type reader struct {
	allocatorImage *driver.AllocatorSnapshot
	mu             sync.Mutex
	store          *Store
	tx             *sql.Tx
	meta           driver.Meta
	at             uint64
	closed         bool
	owner          *transaction
}

func (s *Store) Read(ctx context.Context, v driver.View) (driver.Reader, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if err := branchZero(v.Branch); err != nil {
		return nil, err
	}
	r := &reader{store: s}
	if err := r.pin(ctx, v); err != nil {
		return nil, err
	}
	if err := s.track(r); err != nil {
		_ = r.tx.Rollback()
		return nil, err
	}
	return r, nil
}
func (r *reader) pin(ctx context.Context, v driver.View) error {
	tx, err := r.store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	m, err := readMeta(ctx, tx) // First SELECT pins the SQLite WAL snapshot.
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if v.Incarnation != m.Incarnation {
		_ = tx.Rollback()
		return driver.ErrIncarnationChanged{From: v.Incarnation, To: m.Incarnation}
	}
	head, _ := m.Head.Uint64()
	at := head
	requested := m.Head
	if v.At != nil {
		requested = *v.At
		at, err = requested.Uint64()
	}
	if err == nil {
		floor, _ := m.HistoryFloor.Uint64()
		if at < floor {
			err = driver.ErrTruncated{Floor: m.HistoryFloor}
		}
		if at > head {
			err = driver.ErrFutureRevision{Requested: requested, Head: m.Head}
		}
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if r.tx != nil {
		_ = r.tx.Rollback()
	}
	r.tx = tx
	r.meta = m
	r.at = at
	return nil
}
func (r *reader) Snapshot() driver.View {
	r.mu.Lock()
	defer r.mu.Unlock()
	at := model.Natural64(r.at)
	return driver.View{At: &at, Incarnation: r.meta.Incarnation, Branch: model.Natural64(0)}
}
func (r *reader) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.owner != nil {
		if err := r.store.fence(ctx, r.store.db, r.owner.lease); err != nil {
			return err
		}
	}
	if r.closed {
		return driver.ErrTxClosed
	}
	return r.store.check(ctx)
}
func (r *reader) Close() error {
	if r.owner != nil {
		return r.owner.Rollback(context.Background())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.tx.Rollback()
	r.tx = nil
	r.store.untrack(r)
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}
func copyRecord(v driver.Record) driver.Record { v.Body = bytes.Clone(v.Body); return v }
func copyKey(k driver.LockKey) driver.LockKey {
	if k.Ref != nil {
		v := *k.Ref
		k.Ref = &v
	}
	if k.Derived != nil {
		v := *k.Derived
		k.Derived = &v
	}
	return k
}
func (r *reader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	driver.ReportReadWork(ctx, driver.ReadGetCalls, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	result := make([]driver.Record, 0, len(refs))
	for _, ref := range refs {
		id, err := recordKey(ref)
		if err != nil {
			return nil, err
		}
		record, found, err := r.recordAt(ctx, id)
		if err != nil {
			return nil, err
		}
		if found {
			result = append(result, record)
		}
	}
	return result, nil
}
func (r *reader) recordAt(ctx context.Context, id string) (driver.Record, bool, error) {
	if r.owner != nil {
		if v, ok := r.owner.records[id]; ok {
			if v == nil {
				return driver.Record{}, false, nil
			}
			driver.ReportReadPayload(ctx, v.Body)
			return copyRecord(*v), true, nil
		}
	}
	var data []byte
	if r.at == mustUint(r.meta.Head) {
		err := r.tx.QueryRowContext(ctx, `SELECT data FROM records WHERE key=?`, id).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			return driver.Record{}, false, nil
		}
		var record driver.Record
		if err == nil {
			err = json.Unmarshal(data, &record)
		}
		if err == nil {
			driver.ReportReadPayload(ctx, record.Body)
		}
		return record, err == nil, err
	}
	// Locate the last version and preceding base, then replay at most 31
	// forward patches. Tombstones reset the body before a recreation patch.
	var lastRev string
	var lastData []byte
	err := r.tx.QueryRowContext(ctx, `SELECT rev,data FROM versions WHERE key=? AND rev<=? ORDER BY rev DESC LIMIT 1`, id, revKey(r.at)).Scan(&lastRev, &lastData)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.Record{}, false, nil
	}
	if err != nil {
		return driver.Record{}, false, err
	}
	var last driver.Version
	if err = json.Unmarshal(lastData, &last); err != nil {
		return driver.Record{}, false, err
	}
	if last.Deleted {
		return driver.Record{}, false, nil
	}
	var first string
	err = r.tx.QueryRowContext(ctx, `SELECT rev FROM versions WHERE key=? AND rev<=? AND base=1 ORDER BY rev DESC LIMIT 1`, id, lastRev).Scan(&first)
	if err != nil {
		return driver.Record{}, false, err
	}
	rows, err := r.tx.QueryContext(ctx, `SELECT data FROM versions WHERE key=? AND rev>=? AND rev<=? ORDER BY rev`, id, first, lastRev)
	if err != nil {
		return driver.Record{}, false, err
	}
	defer rows.Close()
	var body []byte
	for rows.Next() {
		var data []byte
		var v driver.Version
		if err = rows.Scan(&data); err != nil {
			return driver.Record{}, false, err
		}
		if err = json.Unmarshal(data, &v); err != nil {
			return driver.Record{}, false, err
		}
		if v.Deleted {
			body = nil
			continue
		}
		if v.Base {
			body = bytes.Clone(v.Body)
			continue
		}
		var patches []struct {
			Op, Path string
			Value    json.RawMessage
		}
		if err = json.Unmarshal(v.Patch, &patches); err != nil {
			return driver.Record{}, false, err
		}
		if len(patches) != 1 || patches[0].Op != "replace" || patches[0].Path != "" {
			return driver.Record{}, false, fmt.Errorf("sqlite: invalid forward patch")
		}
		body = bytes.Clone(patches[0].Value)
	}
	if err = rows.Err(); err != nil {
		return driver.Record{}, false, err
	}
	var kind string
	if err = r.tx.QueryRowContext(ctx, `SELECT kind FROM objects WHERE key=?`, id).Scan(&kind); err != nil {
		return driver.Record{}, false, err
	}
	driver.ReportReadPayload(ctx, body)
	return driver.Record{Ref: last.Ref, Kind: kind, Revision: last.Revision, Body: body}, true, nil
}
func mustUint(v model.Natural) uint64 { n, _ := v.Uint64(); return n }
func (r *reader) Blob(ctx context.Context, hash driver.Hash) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if r.owner != nil {
		if data, ok := r.owner.blobs[hash]; ok {
			return bytes.Clone(data), nil
		}
	}
	var data []byte
	err := r.tx.QueryRowContext(ctx, `SELECT data FROM blobs WHERE hash=? AND rev<=?`, string(hash), revKey(r.at)).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	return data, err
}
func (r *reader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if err := driver.ValidateLockKeys(keys); err != nil {
		return nil, err
	}
	result := make([]driver.LockState, 0, len(keys))
	for _, key := range keys {
		if key.Kind == driver.LockRecord {
			if _, err := recordKey(*key.Ref); err != nil {
				return nil, err
			}
		}
		state, err := r.observe(ctx, key)
		if err != nil {
			return nil, err
		}
		result = append(result, state)
	}
	return result, nil
}
func (r *reader) observe(ctx context.Context, key driver.LockKey) (driver.LockState, error) {
	state := driver.LockState{Key: copyKey(key), Revision: model.Natural64(0)}
	var data []byte
	err := r.tx.QueryRowContext(ctx, `SELECT data FROM key_history WHERE key=? AND rev<=? ORDER BY rev DESC LIMIT 1`, keyID(key), revKey(r.at)).Scan(&data)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return state, err
	}
	if err == nil {
		if err = json.Unmarshal(data, &state); err != nil {
			return state, err
		}
	}
	if key.Kind == driver.LockRecord {
		v, found, err := r.recordAt(ctx, keyID(key))
		if err != nil {
			return state, err
		}
		state.Present = found
		if found {
			state.Record = &v
		}
	}
	if r.owner != nil {
		if v, ok := r.owner.derived[keyID(key)]; ok {
			state.Present = v.Present
			state.Derived = nil
			if v.Derived != nil {
				value := *v.Derived
				value.Value = bytes.Clone(value.Value)
				state.Derived = &value
			}
		}
	}
	return state, nil
}
func pageLimit(n int) (int, error) {
	if n < 0 || n > 10000 {
		return 0, fmt.Errorf("sqlite: limit must be in [0,10000]")
	}
	if n == 0 {
		n = 100
	}
	return n, nil
}
func (r *reader) cursor(c *driver.Cursor, method, digest string) (uint64, error) {
	if c == nil {
		return 0, nil
	}
	if c.Incarnation != r.meta.Incarnation {
		return 0, driver.ErrIncarnationChanged{From: c.Incarnation, To: r.meta.Incarnation}
	}
	if c.Method != method || c.FilterDigest != digest || c.PrincipalDigest != "" || c.OntologyRevision.Validate() != nil || !c.OntologyRevision.IsZero() {
		return 0, fmt.Errorf("sqlite: cursor binding mismatch")
	}
	revision, err := c.Revision.Uint64()
	if err != nil {
		return 0, err
	}
	if revision != r.at {
		return 0, fmt.Errorf("sqlite: cursor snapshot mismatch")
	}
	return c.Index.Uint64()
}
func (r *reader) nextCursor(method, digest string, index uint64) *driver.Cursor {
	return &driver.Cursor{Method: method, FilterDigest: digest, Incarnation: r.meta.Incarnation, Revision: model.Natural64(r.at), Index: model.Natural64(index)}
}
func (r *reader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	driver.ReportReadWork(ctx, driver.ReadQueryCalls, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if q.AsOf != nil {
		at, err := q.AsOf.Uint64()
		if err != nil {
			return driver.Page[driver.Record]{}, err
		}
		if at != r.at {
			return driver.Page[driver.Record]{}, fmt.Errorf("sqlite: query as-of differs from reader")
		}
	}
	for field, want := range q.Filter {
		switch field {
		case "kind", "id", "key", "type", "project", "status":
		default:
			return driver.Page[driver.Record]{}, fmt.Errorf("%w: sqlite filter %s", driver.ErrUnsupported, field)
		}
		if _, ok := want.(string); !ok {
			return driver.Page[driver.Record]{}, fmt.Errorf("sqlite: equality filters require strings")
		}
	}
	encoded, err := json.Marshal(q.Filter)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	hash := sha256.Sum256(encoded)
	digest := hex.EncodeToString(hash[:])
	start, err := r.cursor(q.Cursor, "query", digest)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	limit, err := pageLimit(q.Limit)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	rows, err := r.tx.QueryContext(ctx, `SELECT key FROM objects ORDER BY key`)
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	ids := make(map[string]bool)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		ids[id] = true
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if r.owner != nil {
		for id := range r.owner.records {
			ids[id] = true
		}
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	items := make([]driver.Record, 0)
	for _, id := range ordered {
		record, found, err := r.recordAt(ctx, id)
		if err != nil {
			return driver.Page[driver.Record]{}, err
		}
		if !found {
			continue
		}
		match, err := matches(record, q.Filter)
		if err != nil {
			return driver.Page[driver.Record]{}, err
		}
		if match {
			items = append(items, record)
		}
	}
	if start > uint64(len(items)) {
		return driver.Page[driver.Record]{}, fmt.Errorf("sqlite: cursor index exceeds page")
	}
	end := min(start+uint64(limit), uint64(len(items)))
	page := driver.Page[driver.Record]{Items: items[start:end], AsOf: model.Natural64(r.at)}
	if end < uint64(len(items)) {
		page.NextCursor = r.nextCursor("query", digest, end)
	}
	driver.ReportReadWork(ctx, driver.ReadQueryReturned, int64(len(page.Items)))
	return page, nil
}
func matches(r driver.Record, filter map[string]any) (bool, error) {
	for field, want := range filter {
		var got any
		switch field {
		case "kind":
			got = r.Kind
		case "id":
			got = r.Ref.ID
		case "key":
			got = r.Ref.Key
		default:
			var body map[string]any
			if err := json.Unmarshal(r.Body, &body); err != nil {
				return false, err
			}
			got = body[field]
		}
		if !reflect.DeepEqual(got, want) {
			return false, nil
		}
	}
	return true, nil
}
func (r *reader) Versions(ctx context.Context, ref driver.Ref, req driver.PageReq) (driver.Page[driver.Version], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.Page[driver.Version]{}, err
	}
	id, err := recordKey(ref)
	if err != nil {
		return driver.Page[driver.Version]{}, err
	}
	start, err := r.cursor(req.Cursor, "versions", id)
	if err != nil {
		return driver.Page[driver.Version]{}, err
	}
	limit, err := pageLimit(req.Limit)
	if err != nil {
		return driver.Page[driver.Version]{}, err
	}
	var count uint64
	if err = r.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM versions WHERE key=? AND rev<=?`, id, revKey(r.at)).Scan(&count); err != nil {
		return driver.Page[driver.Version]{}, err
	}
	if start > count {
		return driver.Page[driver.Version]{}, fmt.Errorf("sqlite: version cursor out of range")
	}
	rows, err := r.tx.QueryContext(ctx, `SELECT data FROM versions WHERE key=? AND rev<=? ORDER BY rev LIMIT ? OFFSET ?`, id, revKey(r.at), limit, int64(start))
	if err != nil {
		return driver.Page[driver.Version]{}, err
	}
	defer rows.Close()
	page := driver.Page[driver.Version]{Items: make([]driver.Version, 0), AsOf: model.Natural64(r.at)}
	for rows.Next() {
		var data []byte
		var v driver.Version
		if err = rows.Scan(&data); err == nil {
			err = json.Unmarshal(data, &v)
		}
		if err != nil {
			return driver.Page[driver.Version]{}, err
		}
		page.Items = append(page.Items, v)
	}
	if err = rows.Err(); err != nil {
		return driver.Page[driver.Version]{}, err
	}
	end := start + uint64(len(page.Items))
	if end < count {
		page.NextCursor = r.nextCursor("versions", id, end)
	}
	return page, nil
}
func (r *reader) Changesets(ctx context.Context, after *driver.Cursor, limit int) ([]model.Changeset, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	n, err := pageLimit(limit)
	if err != nil {
		return nil, err
	}
	var at uint64
	if after != nil {
		if after.Incarnation != r.meta.Incarnation {
			return nil, driver.ErrIncarnationChanged{From: after.Incarnation, To: r.meta.Incarnation}
		}
		if after.Index.Validate() != nil || !after.Index.IsZero() || after.Method != "" && after.Method != "changesets" || after.FilterDigest != "" || after.PrincipalDigest != "" || after.OntologyRevision.Validate() != nil || !after.OntologyRevision.IsZero() {
			return nil, fmt.Errorf("sqlite: changeset cursor binding mismatch")
		}
		at, err = after.Revision.Uint64()
		if err != nil {
			return nil, err
		}
	}
	if at < mustUint(r.meta.HistoryFloor) {
		return nil, driver.ErrTruncated{Floor: r.meta.HistoryFloor}
	}
	if at > r.at {
		return nil, fmt.Errorf("sqlite: changeset cursor exceeds snapshot")
	}
	rows, err := r.tx.QueryContext(ctx, `SELECT data FROM changesets WHERE rev>? AND rev<=? ORDER BY rev LIMIT ?`, revKey(at), revKey(r.at), n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.Changeset, 0)
	for rows.Next() {
		var data []byte
		var entry model.Changeset
		if err = rows.Scan(&data); err == nil {
			err = json.Unmarshal(data, &entry)
		}
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, rows.Err()
}
func scopeKey(principal, agent, method, key string) string {
	if !strings.HasPrefix(principal, "anonymous:") {
		agent = ""
	}
	data, _ := json.Marshal([]string{principal, agent, method, key})
	return string(data)
}
func (r *reader) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	value, err := r.store.readReplay(ctx, r.tx, scopeKey(principal, agent, method, key))
	if err != nil || value == nil {
		return nil, err
	}
	if mustUint(value.Commit.Revision) > r.at {
		return nil, nil
	}
	return &value.IdempotencyRecord, nil
}
func (r *reader) ChangesetByKey(ctx context.Context, principal, agent, method, key string) (*model.Changeset, error) {
	v, err := r.IdempotencyByKey(ctx, principal, agent, method, key)
	if v == nil || err != nil || v.Changeset.Kind == "" {
		return nil, err
	}
	return &v.Changeset, nil
}

type storedReplay struct {
	driver.IdempotencyRecord
	finalized bool
	post      []byte
}

func (s *Store) readReplay(ctx context.Context, q queryer, scope string) (*storedReplay, error) {
	var data, entry, post []byte
	var request string
	var created int64
	var finalized bool
	var pending bool
	err := q.QueryRowContext(ctx, `SELECT created,request_hash,response,changeset,finalized,post,pending FROM replays WHERE scope=?`, scope).Scan(&created, &request, &data, &entry, &finalized, &post, &pending)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !s.now().Before(time.Unix(0, created).Add(driver.IdempotencyWindow)) {
		return nil, nil
	}
	value := &storedReplay{IdempotencyRecord: driver.IdempotencyRecord{RequestHash: request, ResponsePending: pending}, finalized: finalized, post: post}
	if err = json.Unmarshal(data, &value.Commit); err != nil {
		return nil, err
	}
	if len(entry) > 0 {
		if err = json.Unmarshal(entry, &value.Changeset); err != nil {
			return nil, err
		}
	}
	return value, nil
}

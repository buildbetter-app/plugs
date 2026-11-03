package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type reader struct {
	allocatorImage *driver.AllocatorSnapshot
	mu             sync.Mutex
	store          *Store
	sql            *sql.Tx
	meta           driver.Meta
	at             uint64
	closed         bool
	writer         *transaction
	// Writers reserve their connection so cancellation cleanup can wait for
	// pgx's asynchronous server termination before another lock probe starts.
	conn        *sql.Conn
	cleanupDone <-chan struct{}
}

func (s *Store) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if err := branchZero(view.Branch); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	fail := func(err error) (driver.Reader, error) { _ = tx.Rollback(); return nil, err }
	m, err := readMeta(ctx, tx, "")
	if err != nil {
		return fail(err)
	}
	if view.Incarnation != m.Incarnation {
		return fail(driver.ErrIncarnationChanged{From: view.Incarnation, To: m.Incarnation})
	}
	at, err := m.Head.Uint64()
	if err != nil {
		return fail(err)
	}
	requested := m.Head
	if view.At != nil {
		requested = *view.At
		at, err = requested.Uint64()
		if err != nil {
			return fail(fmt.Errorf("%w: postgres as-of revision exceeds uint64", driver.ErrUnsupported))
		}
	}
	floor, err := m.HistoryFloor.Uint64()
	if err != nil {
		return fail(err)
	}
	head, err := m.Head.Uint64()
	if err != nil {
		return fail(err)
	}
	if at < floor {
		return fail(driver.ErrTruncated{Floor: m.HistoryFloor})
	}
	if at > head {
		return fail(driver.ErrFutureRevision{Requested: requested, Head: m.Head})
	}
	r := &reader{store: s, sql: tx, meta: m, at: at}
	if err = s.track(r); err != nil {
		return fail(err)
	}
	return r, nil
}

func (r *reader) Snapshot() driver.View {
	r.mu.Lock()
	defer r.mu.Unlock()
	at := model.Natural64(r.at)
	return driver.View{Branch: model.Natural64(0), At: &at, Incarnation: r.meta.Incarnation}
}
func (r *reader) finish() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.allocatorImage = nil
	err := r.sql.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		err = nil
	}
	if r.conn != nil {
		if err != nil {
			// A context-cancelled query may return while PostgreSQL still holds
			// its prefix locks. Failed rollback means pgx is terminating that
			// connection asynchronously; wait for its bounded cleanup first.
			timer := time.NewTimer(15 * time.Second)
			select {
			case <-r.cleanupDone:
			case <-timer.C:
			}
			timer.Stop()
		}
		_ = r.conn.Close()
	}
	r.store.untrack(r)
	if r.writer != nil {
		r.writer.clearSourceOverlay()
		r.writer.records = nil
		r.writer.identities = nil
		r.writer.derived = nil
		r.writer.blobs = nil
		r.writer.allocators = nil
		r.writer.parentCounters = nil
		r.writer.bumps = nil
		r.writer.allocated = nil
		r.writer.acquired = nil
		close(r.writer.done)
	}
	return err
}
func (r *reader) Close() error { r.mu.Lock(); defer r.mu.Unlock(); return r.finish() }

func (r *reader) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.writer != nil && r.writer.lease.ended() {
		return driver.ErrFenced{ExpectedEpoch: r.writer.lease.epoch}
	}
	if r.closed {
		return driver.ErrTxClosed
	}
	if r.writer != nil {
		if !r.writer.leasePrepared {
			if _, err := r.store.checkLease(ctx, r.sql, r.writer.lease, false); err != nil {
				return err
			}
		}
		if !r.writer.hasHead {
			m, err := readMeta(ctx, r.sql, "")
			if err != nil {
				return err
			}
			r.meta = m
			r.at, _ = m.Head.Uint64()
		}
	}
	return nil
}

type storedRecord struct {
	record  driver.Record
	deleted bool
	count   uint64
}

// recordAt uses the current row when possible. Historical reads start at the
// nearest full base and replay at most 31 RFC6902 root-replacement patches.
func (r *reader) recordAt(ctx context.Context, key string) (storedRecord, bool, error) {
	var result storedRecord
	// Revision zero precedes every persisted record, just as in recordsAt.
	// Callers check the live context/lease and their staged overlay separately.
	if r.at == 0 {
		return result, false, nil
	}
	var raw, body []byte
	var rev, count string
	var kind string
	var deleted bool
	err := r.sql.QueryRowContext(ctx, `SELECT ref,kind,revision::text,body,deleted,version_count::text FROM plug_postgres.records WHERE key=$1 AND revision<=$2`, []byte(key), decimal(r.at)).Scan(&raw, &kind, &rev, &body, &deleted, &count)
	if err == nil {
		var ref driver.Ref
		if err = decode(raw, &ref); err != nil {
			return result, false, err
		}
		n, e := parseCounter(rev)
		if e != nil {
			return result, false, e
		}
		c, e := parseCounter(count)
		if e != nil {
			return result, false, e
		}
		if !deleted {
			driver.ReportReadPayload(ctx, body)
		}
		return storedRecord{record: driver.Record{Ref: ref, Kind: kind, Revision: model.Natural64(n), Body: body}, deleted: deleted, count: c}, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return result, false, err
	}
	err = r.sql.QueryRowContext(ctx, `SELECT value FROM plug_postgres.versions WHERE key=$1 AND revision<=$2 ORDER BY revision DESC LIMIT 1`, []byte(key), decimal(r.at)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return result, false, nil
	}
	if err != nil {
		return result, false, err
	}
	var last driver.Version
	if err = decode(raw, &last); err != nil {
		return result, false, err
	}
	// Kinds are immutable, including across deletion and recreation.
	if err = r.sql.QueryRowContext(ctx, `SELECT kind FROM plug_postgres.records WHERE key=$1`, []byte(key)).Scan(&kind); err != nil {
		return result, false, err
	}
	result = storedRecord{record: driver.Record{Ref: last.Ref, Kind: kind, Revision: last.Revision}, deleted: last.Deleted}
	if last.Deleted {
		return result, true, nil
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT value FROM plug_postgres.versions WHERE key=$1 AND revision<=$2 AND revision>=(SELECT max(revision) FROM plug_postgres.versions WHERE key=$1 AND revision<=$2 AND is_base) ORDER BY revision`, []byte(key), last.Revision.String())
	if err != nil {
		return result, false, err
	}
	defer rows.Close()
	patches := 0
	found := false
	for rows.Next() {
		if err = rows.Scan(&raw); err != nil {
			return result, false, err
		}
		var v driver.Version
		if err = decode(raw, &v); err != nil {
			return result, false, err
		}
		found = true
		switch {
		case v.Deleted:
			result.record.Body = nil
		case v.Base:
			result.record.Body = v.Body
		default:
			patches++
			if patches >= driver.VersionBaseEvery {
				return result, false, fmt.Errorf("postgres: stored version base cadence violated")
			}
			value, e := applyPatch(v.Patch)
			if e != nil {
				return result, false, e
			}
			result.record.Body = value
		}
	}
	if err = rows.Err(); err != nil {
		return result, false, err
	}
	if !found {
		return result, false, fmt.Errorf("postgres: historical version has no base")
	}
	driver.ReportReadPayload(ctx, result.record.Body)
	return result, true, nil
}

// recordsAt batches current rows without changing the bounded historical
// reconstruction path. Any current-row miss still uses recordAt, except at
// revision zero where no committed record can exist. Nothing survives this call.
func (r *reader) recordsAt(ctx context.Context, ids []string) (map[string]storedRecord, error) {
	result := make(map[string]storedRecord, len(ids))
	if len(ids) == 0 || r.at == 0 {
		return result, nil
	}
	keys := make([][]byte, len(ids))
	for i, id := range ids {
		keys[i] = []byte(id)
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT key,ref,kind,revision::text,body,deleted,version_count::text
	 FROM plug_postgres.records WHERE key=ANY($1::bytea[]) AND revision<=$2`, keys, decimal(r.at))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, raw []byte
		var value storedRecord
		var revision, count string
		if err = rows.Scan(&id, &raw, &value.record.Kind, &revision, &value.record.Body, &value.deleted, &count); err != nil {
			break
		}
		if err = decode(raw, &value.record.Ref); err != nil {
			break
		}
		var n uint64
		if n, err = parseCounter(revision); err != nil {
			break
		}
		value.record.Revision = model.Natural64(n)
		if value.count, err = parseCounter(count); err != nil {
			break
		}
		if !value.deleted {
			driver.ReportReadPayload(ctx, value.record.Body)
		}
		result[string(id)] = value
	}
	if err == nil {
		err = rows.Err()
	}
	closeErr := rows.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	for _, id := range ids {
		if _, exists := result[id]; exists {
			continue
		}
		value, exists, err := r.recordAt(ctx, id)
		if err != nil {
			return nil, err
		}
		if exists {
			result[id] = value
		}
	}
	return result, nil
}

func (r *reader) get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	ids := make([]string, len(refs))
	read := []string{}
	values := map[string]storedRecord{}
	seen := map[string]bool{}
	for i, ref := range refs {
		key, err := recordKey(ref)
		if err != nil {
			return nil, err
		}
		ids[i] = key
		if seen[key] {
			continue
		}
		seen[key] = true
		if r.writer != nil {
			if v, ok := r.writer.records[key]; ok {
				if v != nil {
					values[key] = storedRecord{record: *v}
				}
				continue
			}
		}
		read = append(read, key)
	}
	stored, err := r.recordsAt(ctx, read)
	if err != nil {
		return nil, err
	}
	for id, value := range stored {
		values[id] = value
	}
	result := make([]driver.Record, 0, len(refs))
	for _, id := range ids {
		if value, exists := values[id]; exists && !value.deleted {
			row := value.record
			row.Body = bytes.Clone(row.Body)
			driver.ReportReadPayload(ctx, row.Body)
			result = append(result, row)
		}
	}
	return result, nil
}
func (r *reader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	driver.ReportReadWork(ctx, driver.ReadGetCalls, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	return r.get(ctx, refs)
}

func (r *reader) Blob(ctx context.Context, hash driver.Hash) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if r.writer != nil {
		if data, ok := r.writer.blobs[hash]; ok {
			return append([]byte{}, data...), nil
		}
	}
	var data []byte
	err := r.sql.QueryRowContext(ctx, `SELECT value FROM plug_postgres.blobs WHERE hash=$1 AND revision<=$2`, string(hash), decimal(r.at)).Scan(&data)
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
	return r.observeKeys(ctx, keys)
}

// observeKeys runs under the reader mutex and the caller's fresh check/view.
// Lock also uses it after acquiring the complete set and refreshing readMeta.
func (r *reader) observeKeys(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	states := make([]driver.LockState, len(keys))
	recordIDs := []string{}
	stateIDs := [][]byte{}
	indexes := map[string]int{}
	for i, key := range keys {
		id, err := keyID(key)
		if err != nil {
			return nil, err
		}
		owned := copyLockKey(key)
		states[i] = driver.LockState{Key: owned, Revision: model.Natural64(0)}
		indexes[id] = i
		if key.Kind == driver.LockRecord {
			recordIDs = append(recordIDs, id)
		} else {
			stateIDs = append(stateIDs, []byte(id))
		}
	}
	records, err := r.recordsAt(ctx, recordIDs)
	if err != nil {
		return nil, err
	}
	for id, value := range records {
		i := indexes[id]
		states[i].Revision = value.record.Revision
		states[i].Present = !value.deleted
		if !value.deleted {
			row := value.record
			states[i].Record = &row
		}
	}
	if len(stateIDs) != 0 {
		rows, err := r.sql.QueryContext(ctx, `SELECT DISTINCT ON (key) key,value FROM plug_postgres.states
		 WHERE key=ANY($1::bytea[]) AND revision<=$2 ORDER BY key,revision DESC`, stateIDs, decimal(r.at))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var id, raw []byte
			if err := rows.Scan(&id, &raw); err != nil {
				return nil, err
			}
			if err := decode(raw, &states[indexes[string(id)]]); err != nil {
				return nil, err
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	// Staging changes the visible image, not the committed optimistic token.
	// Load those tokens in batches above even for deleted/replaced rows, then
	// copy the overlay so caller mutation cannot alter this transaction.
	if r.writer != nil {
		for i, key := range keys {
			id, _ := keyID(key)
			if key.Kind == driver.LockRecord {
				if record, staged := r.writer.records[id]; staged {
					states[i].Record = nil
					states[i].Present = record != nil
					if record != nil {
						row, err := copied(*record)
						if err != nil {
							return nil, err
						}
						row.Revision = states[i].Revision
						states[i].Record = &row
					}
				}
			} else if staged, ok := r.writer.derived[id]; ok {
				states[i].Present = staged.Present
				states[i].Derived = nil
				if staged.Derived != nil {
					row := staged.Derived.Clone()
					states[i].Derived = &row
				}
			}
		}
	}
	// Restore binary-owned annotations without changing ordinary opaque states.
	byTable := map[driver.DerivedTable][]int{}
	for i, s := range states {
		if s.Present && s.Derived != nil && s.Key.Derived != nil {
			byTable[s.Key.Derived.Table] = append(byTable[s.Key.Derived.Table], i)
		}
	}
	for table, positions := range byTable {
		values := make([]driver.DerivedRow, 0, len(positions))
		for _, i := range positions {
			values = append(values, *states[i].Derived)
		}
		if err := r.selectionAnnotations(ctx, values, table); err != nil {
			return nil, err
		}
		for j, i := range positions {
			states[i].Derived = &values[j]
		}
	}
	return states, nil
}

var _ driver.Reader = (*reader)(nil)

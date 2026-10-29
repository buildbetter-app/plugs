package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type reader struct {
	mu     sync.Mutex
	store  *Store
	sql    *sql.Tx
	meta   driver.Meta
	at     uint64
	closed bool
	writer *transaction
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
	if view.At != nil {
		at, err = view.At.Uint64()
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
		return fail(fmt.Errorf("postgres: requested revision beyond head"))
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
	err := r.sql.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		err = nil
	}
	r.store.untrack(r)
	if r.writer != nil {
		r.writer.records = nil
		r.writer.identities = nil
		r.writer.derived = nil
		r.writer.blobs = nil
		r.writer.bumps = nil
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
		if _, err := r.store.checkLease(ctx, r.sql, r.writer.lease, false); err != nil {
			return err
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
	return result, true, nil
}

func (r *reader) get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	result := make([]driver.Record, 0, len(refs))
	for _, ref := range refs {
		key, err := recordKey(ref)
		if err != nil {
			return nil, err
		}
		if r.writer != nil {
			if v, ok := r.writer.records[key]; ok {
				if v != nil {
					copy, err := copied(*v)
					if err != nil {
						return nil, err
					}
					result = append(result, copy)
				}
				continue
			}
		}
		value, ok, err := r.recordAt(ctx, key)
		if err != nil {
			return nil, err
		}
		if ok && !value.deleted {
			result = append(result, value.record)
		}
	}
	return result, nil
}
func (r *reader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
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

func (r *reader) observe(ctx context.Context, key driver.LockKey) (driver.LockState, error) {
	id, err := keyID(key)
	if err != nil {
		return driver.LockState{}, err
	}
	key, err = copied(key)
	if err != nil {
		return driver.LockState{}, err
	}
	state := driver.LockState{Key: key, Revision: model.Natural64(0)}
	if key.Kind == driver.LockRecord {
		if r.writer != nil {
			if record, ok := r.writer.records[id]; ok {
				if record != nil {
					v, e := copied(*record)
					if e != nil {
						return state, e
					}
					state.Record = &v
					state.Present = true
					state.Revision = v.Revision
				}
				return state, nil
			}
		}
		value, ok, err := r.recordAt(ctx, id)
		if err != nil {
			return state, err
		}
		if ok {
			state.Revision = value.record.Revision
			state.Present = !value.deleted
			if state.Present {
				state.Record = &value.record
			}
		}
		return state, nil
	}
	if r.writer != nil {
		if state, ok := r.writer.derived[id]; ok {
			return copied(state)
		}
	}
	var raw []byte
	err = r.sql.QueryRowContext(ctx, `SELECT value FROM plug_postgres.states WHERE key=$1 AND revision<=$2 ORDER BY revision DESC LIMIT 1`, []byte(id), decimal(r.at)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	err = decode(raw, &state)
	return state, err
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
	states := make([]driver.LockState, 0, len(keys))
	for _, key := range keys {
		state, err := r.observe(ctx, key)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
}

var _ driver.Reader = (*reader)(nil)

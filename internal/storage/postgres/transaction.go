package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
	"github.com/jackc/pgx/v5/stdlib"
)

type transaction struct {
	*reader
	lease *activeLease
	// leasePrepared records the one lease/clock observation made after this
	// writer's SQL transaction begins. Staging reuses that observation while
	// local context, lease-loss and transaction-closed checks remain live;
	// publication performs a fresh row-locking fence.
	leasePrepared    bool
	locked, hasHead  bool
	restore          *driver.Meta
	nativeRestore    *driver.NativeRestoreProfile
	nativeAllocators bool
	importRestore    *driver.RestoreImportProfile
	offlineHistory   *driver.PreparedOfflineHistory
	acquired         driver.LockSet
	records          map[string]*driver.Record
	identities       map[string]driver.Record
	derived          map[string]driver.LockState
	blobs            map[driver.Hash][]byte
	allocators       map[string]driver.OfflineAllocator
	parentCounters   map[string]driver.ParentCounter
	bumps            map[string]driver.LockKey
	allocated        map[string]bool
	done             chan struct{}

	checkpointGuards          map[string]uint64
	checkpointWrites          map[string]driver.Checkpoint
	sourceOverlay             sourceindex.Overlay
	sourceCoverage            sourceindex.CoveragePair
	sourceCoverageHead        model.Revision
	sourceCoverageIncarnation string
	sourceCoverageValid       bool
}

func (s *Store) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var cleanupDone <-chan struct{}
	if err = conn.Raw(func(raw any) error {
		cleanupDone = raw.(*stdlib.Conn).Conn().PgConn().CleanupDone()
		return nil
	}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	// Prepare the writer against the same SQL transaction that will stage and
	// publish it. The observation is deliberately not a row lock: Release or a
	// takeover may proceed while staging, and the final ownership fence remains
	// authoritative at publication.
	l, err := s.checkLease(ctx, tx, lease, false)
	if err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		return nil, err
	}
	m, err := readMeta(ctx, tx, "")
	if err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		return nil, err
	}
	r := &reader{store: s, sql: tx, meta: m, conn: conn, cleanupDone: cleanupDone}
	r.at, _ = m.Head.Uint64()
	t := &transaction{reader: r, lease: l, leasePrepared: true, records: make(map[string]*driver.Record), identities: make(map[string]driver.Record), derived: make(map[string]driver.LockState), blobs: make(map[driver.Hash][]byte), allocators: make(map[string]driver.OfflineAllocator), bumps: make(map[string]driver.LockKey), allocated: make(map[string]bool), done: make(chan struct{})}
	r.writer = t
	if err = s.track(r); err != nil {
		_ = r.finish()
		return nil, err
	}
	// Loss also releases idle transactions, so an old holder cannot retain the
	// head/record locks forever while a new server waits to publish.
	go func() {
		select {
		case <-l.Lost():
			_ = t.Close()
		case <-t.done:
		}
	}()
	return t, nil
}

func sqlState(err error) string {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		return state.SQLState()
	}
	return ""
}
func (t *transaction) Rollback(context.Context) error { return t.Close() }

func (t *transaction) Lock(ctx context.Context, requests []driver.LockRequest) (_ []driver.LockState, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer func() {
		if err == nil {
			return
		}
		// pgx can close a timed-out connection before the context's timer has
		// recorded Err(). Preserve the public deadline result at every SQL
		// boundary, including lease checks and savepoint release, and release
		// any acquired prefix before returning a cancelled acquisition.
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		} else if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			err = context.DeadlineExceeded
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			_ = t.finish()
		}
	}()
	if err := t.check(ctx); err != nil {
		return nil, err
	}
	if t.locked || t.hasHead {
		return nil, fmt.Errorf("postgres: acquire the complete lock set once")
	}
	if err := driver.ValidateLockSet(requests); err != nil {
		return nil, err
	}
	owned := make([]driver.LockRequest, len(requests))
	for i, request := range requests {
		owned[i] = driver.LockRequest{Key: copyLockKey(request.Key), Mode: request.Mode}
	}
	requests = owned
	ids := make([]string, len(requests))
	restoreFence, err := driver.NativeAllocatorRestoreLock()
	if err != nil {
		return nil, err
	}
	restoreFenceID, err := keyID(restoreFence)
	if err != nil {
		return nil, err
	}
	hasRestoreFence, allUpdate := false, true
	for i, request := range requests {
		ids[i], err = keyID(request.Key)
		if err != nil {
			return nil, err
		}
		if ids[i] == restoreFenceID {
			hasRestoreFence = true
		}
		if request.Mode != driver.LockUpdate {
			allUpdate = false
		}
	}
	wideRestore := hasRestoreFence && allUpdate
	// A native restore declares its exclusive allocator fence in the complete
	// update set. It can contain tens of thousands of record and sentinel keys:
	// taking an advisory lock for every key would exhaust PostgreSQL's shared
	// lock table. The exclusive restore barrier instead covers that entire set.
	// Ordinary transactions hold the compatible shared barrier until they end,
	// before acquiring any individual key, so the restore still excludes every
	// concurrent writer and reader of those keys. The two-int advisory namespace
	// is disjoint from the per-key bigint namespace.
	const restoreBarrier = "(1347179847,1381192786)"
	// A transaction-scoped advisory lock represents a key without adding a
	// durable coordination row. In particular, a failed cold write leaves the
	// locks table unchanged, and two cold LockShare holders can coexist.
	// Derive the lock number from the lossless encoded key, not a JSON or text
	// round trip; the namespace keeps it separate from other advisory uses.
	lockIDs := make([]int64, len(ids))
	if !wideRestore {
		for i, id := range ids {
			digest := sha256.Sum256([]byte("plug/postgres/record-lock/v1\x00" + id))
			lockIDs[i] = int64(binary.BigEndian.Uint64(digest[:8]))
		}
	}
	for {
		if err = t.check(ctx); err != nil {
			_ = t.finish()
			return nil, err
		}
		if _, err = t.sql.ExecContext(ctx, "SAVEPOINT complete_lock_set"); err != nil {
			_ = t.finish()
			return nil, err
		}
		selected := make(driver.LockSet, 0, len(requests))
		busy := false
		barrierQuery := "SELECT pg_try_advisory_xact_lock_shared" + restoreBarrier
		if wideRestore {
			barrierQuery = "SELECT pg_try_advisory_xact_lock" + restoreBarrier
		}
		var barrierGranted bool
		err = t.sql.QueryRowContext(ctx, barrierQuery).Scan(&barrierGranted)
		if err == nil && !barrierGranted {
			// During a restore every ordinary key is busy. A pure SkipLocked
			// request therefore observes the empty subset without waiting.
			for _, request := range requests {
				if request.Mode != driver.LockSkipLocked {
					busy = true
					break
				}
			}
		}
		if err == nil && barrierGranted && wideRestore {
			selected = append(selected, requests...)
		}
		for i := 0; err == nil && barrierGranted && !wideRestore && i < len(requests); {
			mode := requests[i].Mode
			end := i + 1
			for end < len(requests) && requests[end].Mode == mode {
				end++
			}
			// PostgreSQL evaluates the volatile target expression after the
			// ordinality sort. A failed try-lock never waits on another writer;
			// rolling back this savepoint releases the complete acquired prefix.
			query := `SELECT pg_try_advisory_xact_lock(id)
			 FROM unnest($1::bigint[]) WITH ORDINALITY AS pending(id, ord) ORDER BY ord`
			if mode == driver.LockShare {
				query = `SELECT pg_try_advisory_xact_lock_shared(id)
				 FROM unnest($1::bigint[]) WITH ORDINALITY AS pending(id, ord) ORDER BY ord`
			}
			var rows *sql.Rows
			rows, err = t.sql.QueryContext(ctx, query, lockIDs[i:end])
			if err != nil {
				break
			}
			seen := 0
			for rows.Next() {
				var granted bool
				if err = rows.Scan(&granted); err != nil {
					break
				}
				if granted {
					selected = append(selected, requests[i+seen])
				} else if mode != driver.LockSkipLocked {
					busy = true
				}
				seen++
			}
			if err == nil {
				err = rows.Err()
			}
			closeErr := rows.Close()
			if err == nil {
				err = closeErr
			}
			if err == nil && seen != end-i {
				err = sql.ErrNoRows
			}
			if err != nil || busy {
				break
			}
			i = end
		}
		if err != nil || busy {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, rollbackErr := t.sql.ExecContext(cleanup, "ROLLBACK TO SAVEPOINT complete_lock_set")
			if rollbackErr == nil {
				_, rollbackErr = t.sql.ExecContext(cleanup, "RELEASE SAVEPOINT complete_lock_set")
			}
			cancel()
			if rollbackErr != nil {
				_ = t.finish()
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, rollbackErr
			}
			if ctx.Err() != nil {
				_ = t.finish()
				return nil, ctx.Err()
			}
			if err != nil {
				_ = t.finish()
				return nil, err
			}
			timer := time.NewTimer(2 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				_ = t.finish()
				return nil, ctx.Err()
			case <-t.lease.Lost():
				timer.Stop()
				_ = t.finish()
				return nil, driver.ErrFenced{ExpectedEpoch: t.lease.epoch}
			case <-timer.C:
				continue
			}
		}
		if _, err = t.sql.ExecContext(ctx, "RELEASE SAVEPOINT complete_lock_set"); err != nil {
			_ = t.finish()
			return nil, err
		}
		m, err := readMeta(ctx, t.sql, "")
		if err != nil {
			_ = t.finish()
			return nil, err
		}
		t.meta = m
		t.at, _ = m.Head.Uint64()
		selectedKeys := make([]driver.LockKey, len(selected))
		for i, request := range selected {
			selectedKeys[i] = request.Key
		}
		states, err := t.observeKeys(ctx, selectedKeys)
		if err != nil {
			_ = t.finish()
			return nil, err
		}
		t.acquired = selected
		t.locked = true
		return states, nil
	}
}

func (t *transaction) Head(ctx context.Context) (model.Revision, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return "", err
	}
	if !t.locked {
		return "", fmt.Errorf("postgres: Head requires complete lock set")
	}
	if t.hasHead {
		return model.Natural64(t.at), nil
	}
	m, err := readMeta(ctx, t.sql, " FOR UPDATE")
	if err != nil {
		return "", err
	}
	t.meta = m
	t.at, _ = m.Head.Uint64()
	t.hasHead = true
	return m.Head, nil
}
func (t *transaction) writable(key driver.LockKey) error {
	if !t.locked {
		return fmt.Errorf("postgres: write requires complete lock set")
	}
	want, err := keyID(key)
	if err != nil {
		return err
	}
	// ValidateLockSet and skip-locked selection retain EncodeLockKey order.
	i := sort.Search(len(t.acquired), func(i int) bool { id, _ := keyID(t.acquired[i].Key); return id >= want })
	if i < len(t.acquired) {
		id, _ := keyID(t.acquired[i].Key)
		if id == want && t.acquired[i].Mode != driver.LockShare {
			return nil
		}
	}
	return fmt.Errorf("postgres: missing update lock for %s", want)
}
func (t *transaction) Put(ctx context.Context, record driver.Record) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.putSourceRecord(ctx, record, nil)
}

func (t *transaction) putSourceRecord(ctx context.Context, record driver.Record, declaration *sourceindex.Declaration) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	key, err := recordKey(record.Ref)
	if err != nil {
		return err
	}
	if err = t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref}); err != nil {
		return err
	}
	if !canonical(record.Kind) || !json.Valid(record.Body) {
		return fmt.Errorf("postgres: record requires canonical kind and valid JSON body")
	}
	previous, exists, err := t.recordAt(ctx, key)
	if err != nil {
		return err
	}
	if exists && previous.record.Kind != record.Kind {
		return fmt.Errorf("%w: postgres cannot change record kind", driver.ErrUnsupported)
	}
	if staged, ok := t.records[key]; ok && staged != nil && staged.Kind != record.Kind {
		return fmt.Errorf("%w: postgres cannot change staged kind", driver.ErrUnsupported)
	}
	if record.Kind == "bead" {
		if err = t.guardSourcePut(ctx, declaration); err != nil {
			return err
		}
	}
	record.Revision = model.Natural64(0)
	if exists {
		record.Revision = previous.record.Revision
	}
	if record.Ref.IsID() {
		record.Ref.Branch = model.Natural64(0)
	}
	record.Body = bytes.Clone(record.Body)
	t.sourceOverlay.Replace(key, declaration)
	t.records[key] = &record
	t.identities[key] = driver.Record{Ref: record.Ref, Kind: record.Kind}
	return nil
}
func (t *transaction) Delete(ctx context.Context, ref driver.Ref) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	key, err := recordKey(ref)
	if err != nil {
		return err
	}
	if err = t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}); err != nil {
		return err
	}
	if staged, ok := t.records[key]; ok {
		if staged == nil {
			return driver.ErrNotFound
		}
	} else {
		value, ok, err := t.recordAt(ctx, key)
		if err != nil {
			return err
		}
		if !ok || value.deleted {
			return driver.ErrNotFound
		}
		t.identities[key] = driver.Record{Ref: value.record.Ref, Kind: value.record.Kind}
	}
	if t.identities[key].Kind == "bead" {
		coverage, err := t.currentSourceCoverage(ctx)
		if err != nil {
			return err
		}
		if coverage.Parent.Ready || coverage.Links.Ready {
			if err = t.writable(driver.GraphBlockingSentinelLock); err != nil {
				return err
			}
		}
	}
	t.sourceOverlay.Replace(key, nil)
	t.records[key] = nil
	return nil
}
func (t *transaction) PutBlob(ctx context.Context, data []byte) (driver.Hash, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return "", err
	}
	if !t.locked {
		return "", fmt.Errorf("postgres: blob requires complete lock set")
	}
	hash := sha256.Sum256(data)
	id := driver.Hash(hex.EncodeToString(hash[:]))
	t.blobs[id] = append([]byte{}, data...)
	return id, nil
}
func (t *transaction) PutDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if err := (driver.DerivedRowKey{Table: table, Key: "validation"}).Validate(); err != nil {
		return err
	}
	for _, rows := range [][]driver.DerivedRow{del, add} {
		for _, row := range rows {
			if row.Index != nil {
				return fmt.Errorf("%w: PostgreSQL selection indexes are not implemented", driver.ErrUnsupported)
			}
			if err := t.writable(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}); err != nil {
				return err
			}
		}
	}
	for _, row := range del {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		id, _ := keyID(key)
		t.derived[id] = driver.LockState{Key: key, Revision: model.Natural64(0)}
	}
	for _, row := range add {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		id, _ := keyID(key)
		copy := driver.DerivedRow{Key: row.Key, Value: bytes.Clone(row.Value)}
		t.derived[id] = driver.LockState{Key: key, Revision: model.Natural64(0), Derived: &copy, Present: true}
	}
	return nil
}
func (t *transaction) bump(ctx context.Context, keys []driver.LockKey) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	for _, key := range keys {
		if err := t.writable(key); err != nil {
			return err
		}
	}
	for _, key := range keys {
		id, _ := keyID(key)
		t.bumps[id] = key
	}
	return nil
}
func (t *transaction) BumpSentinels(ctx context.Context, values []driver.Sentinel) error {
	keys := make([]driver.LockKey, len(values))
	for i, value := range values {
		keys[i] = driver.LockKey{Kind: driver.LockSentinel, Sentinel: value}
	}
	return t.bump(ctx, keys)
}
func (t *transaction) BumpReadFacts(ctx context.Context, values []driver.ReadFact) error {
	keys := make([]driver.LockKey, len(values))
	for i, value := range values {
		keys[i] = driver.LockKey{Kind: driver.LockReadFact, Fact: value}
	}
	return t.bump(ctx, keys)
}

var _ driver.WriteTx = (*transaction)(nil)

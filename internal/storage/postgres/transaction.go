package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

type transaction struct {
	*reader
	lease *activeLease
	// leasePrepared records the one lease/clock observation made after this
	// writer's SQL transaction begins. Staging reuses that observation while
	// local context, lease-loss and transaction-closed checks remain live;
	// publication performs a fresh row-locking fence.
	leasePrepared     bool
	locked, hasHead   bool
	restore           *driver.Meta
	nativeRestore     *driver.NativeRestoreProfile
	nativeAllocators  bool
	importRestore     *driver.RestoreImportProfile
	sourcePlaneImport *driver.SourcePlaneImportPublication
	offlineHistory    *driver.PreparedOfflineHistory
	stream            *historyStreamStage
	acquired          driver.LockSet
	records           map[string]*driver.Record
	identities        map[string]driver.Record
	derived           map[string]driver.LockState
	blobs             map[driver.Hash][]byte
	allocators        map[string]driver.OfflineAllocator
	parentCounters    map[string]driver.ParentCounter
	bumps             map[string]driver.LockKey
	allocated         map[string]bool
	done              chan struct{}
	commitEntered     *bool

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
		sqlConn := raw.(*commitEntryConn).Conn
		cleanupDone = sqlConn.Conn().PgConn().CleanupDone()
		return nil
	}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	commitEntered := false
	txCtx := context.WithValue(ctx, commitEntryKey{}, &commitEntered)
	tx, err := conn.BeginTx(txCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
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
	t.commitEntered = &commitEntered
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
	for i, request := range requests {
		ids[i], err = keyID(request.Key)
		if err != nil {
			return nil, err
		}
	}
	// Placeholder rows carry no graph state. Check for already-created rows
	// without taking a row lock, then prepare only missing rows on the driver's
	// reserved lock connection. Asking the general pool for a second connection
	// while every writer already owns one self-deadlocks under a wide probe, and
	// repeatedly inserting existing rows would contend with the lock set itself.
	missing := make([]string, 0, len(ids))
	present := make(map[string]bool, len(ids))
	keyBytes := make([][]byte, len(ids))
	for i, id := range ids {
		keyBytes[i] = []byte(id)
	}
	if len(ids) != 0 {
		rows, e := t.sql.QueryContext(ctx, `SELECT key FROM plug_postgres.locks WHERE key=ANY($1::bytea[])`, keyBytes)
		if e != nil {
			return nil, e
		}
		for rows.Next() {
			var id []byte
			if e = rows.Scan(&id); e != nil {
				break
			}
			present[string(id)] = true
		}
		if e == nil {
			e = rows.Err()
		}
		closeErr := rows.Close()
		if e != nil {
			return nil, e
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	for _, id := range ids {
		if !present[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) != 0 {
		if err = t.store.ensureLockKeys(ctx, missing); err != nil {
			return nil, err
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
		for i := 0; i < len(requests); {
			request := requests[i]
			end := i + 1
			for end < len(requests) && requests[end].Mode == request.Mode {
				end++
			}
			clause := " FOR UPDATE NOWAIT"
			if request.Mode == driver.LockShare {
				clause = " FOR SHARE NOWAIT"
			} else if request.Mode == driver.LockSkipLocked {
				clause = " FOR UPDATE SKIP LOCKED"
			}
			// A batch contains only one contiguous mode run. PostgreSQL locks
			// ORDER BY's bytea keys in the exact validated global key order;
			// mode runs are never regrouped across an intervening request.
			var rows *sql.Rows
			rows, err = t.sql.QueryContext(ctx, `SELECT key FROM plug_postgres.locks
			 WHERE key=ANY($1::bytea[]) ORDER BY key`+clause, keyBytes[i:end])
			locked := map[string]bool{}
			if err == nil {
				for rows.Next() {
					var id []byte
					if err = rows.Scan(&id); err != nil {
						break
					}
					locked[string(id)] = true
				}
				if err == nil {
					err = rows.Err()
				}
				closeErr := rows.Close()
				if err == nil {
					err = closeErr
				}
				if err == nil && len(locked) != end-i && request.Mode != driver.LockSkipLocked {
					err = sql.ErrNoRows
				}
			}
			if err != nil {
				busy = sqlState(err) == "55P03"
				break
			}
			for j := i; j < end; j++ {
				if locked[ids[j]] {
					selected = append(selected, requests[j])
				}
			}
			i = end
		}
		if err != nil {
			// NOWAIT avoids holding a prefix while another member is unavailable.
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
				return nil, err
			}
			if ctx.Err() != nil {
				_ = t.finish()
				return nil, ctx.Err()
			}
			if !busy {
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

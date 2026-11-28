// Package sqlite implements the internal storage contract using a local,
// file-backed SQLite database in WAL mode. No engine handle crosses the driver.
package sqlite

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	nativesqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Driver's optional Clock controls persisted time and expiry comparisons. A nil
// Clock uses time.Now exactly as before. An injected clock must be concurrency
// safe and nondecreasing. Real lease timers and context deadlines always keep
// using wall/monotonic time; Clock is not a deadline or TTL override.
type Driver struct {
	Clock func() time.Time
	// AbortStalledCommit is an optional process-owned fail-stop handler. It
	// must terminate the process without closing the store or waiting on the
	// publication. Embedded callers without a supervisor leave it nil.
	AbortStalledCommit func(error)
}

func (Driver) Info() driver.Info {
	return driver.Info{Name: "sqlite", Version: "1", Capabilities: []string{"transactions", "history", "leases", "checkpoints", driver.CapabilityPersistentCheckpoints, "allocator_continuation", "core_sessions", "noop", "response_finalization", driver.CapabilityRestoreCurrentImage}}
}

type Store struct {
	abortStalledCommit func(error)
	clock              func() time.Time
	db                 *sql.DB
	lockDir            string
	path               string
	mu                 sync.Mutex
	closed             bool
	children           map[io.Closer]struct{}
	leases             map[*activeLease]struct{}
}

// Persistent reports that SQLite checkpoint bytes survive Store.Close and a
// subsequent open of the same file.
func (*Store) Persistent() bool { return true }

// SupportsRestore advertises the only restore mode this store can publish
// atomically through its existing immediate transaction boundary.
func (*Store) SupportsRestore(mode driver.RestoreMode) bool {
	return mode == driver.RestoreModeCurrentImage
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func (d Driver) Open(ctx context.Context, cfg driver.Config) (driver.Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(cfg.Options) != 0 || cfg.DSN == "" || cfg.DSN == ":memory:" || strings.ContainsRune(cfg.DSN, '\x00') {
		return nil, fmt.Errorf("%w: sqlite requires a file DSN and no options", driver.ErrUnsupported)
	}
	path := cfg.DSN
	if strings.HasPrefix(path, "file:") {
		u, err := url.Parse(path)
		if err != nil || u.Host != "" || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("%w: sqlite accepts local file URIs without parameters", driver.ErrUnsupported)
		}
		path = u.Path
		if path == "" {
			path = u.Opaque
		}
	}
	if path == "" || path == ":memory:" {
		return nil, driver.ErrUnsupported
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = validateDatabaseFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	s := &Store{abortStalledCommit: d.AbortStalledCommit, clock: d.Clock, path: path, lockDir: path + ".plug-locks", children: make(map[io.Closer]struct{}), leases: make(map[*activeLease]struct{})}
	if err = os.MkdirAll(s.lockDir, 0700); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: path}
	dsn := u.String() + "?_pragma=busy_timeout(0)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"
	s.db, err = sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Open/init and every publication share an OS file lock across handles and
	// processes. WAL read transactions never need this lock.
	err = s.immediate(ctx, nil, func(c *sql.Conn) error {
		var hasMeta bool
		if e := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta')`).Scan(&hasMeta); e != nil {
			return e
		}
		if hasMeta {
			var version int
			if e := c.QueryRowContext(ctx, `SELECT schema_version FROM meta WHERE singleton=1`).Scan(&version); e != nil {
				return e
			}
			if version == 1 {
				// Additive upgrade from the published SQLite checkpoint. Existing
				// receipts are final; never infer unfinished posts for old writes.
				if _, e := c.ExecContext(ctx, `ALTER TABLE replays ADD COLUMN pending INTEGER NOT NULL DEFAULT 0 CHECK(pending IN (0,1))`); e != nil {
					return e
				}
				if _, e := c.ExecContext(ctx, `UPDATE replays SET post=? WHERE finalized=1 AND (post IS NULL OR CAST(post AS TEXT)='null')`, []byte("[]")); e != nil {
					return e
				}
				if _, e := c.ExecContext(ctx, `UPDATE meta SET schema_version=2 WHERE singleton=1`); e != nil {
					return e
				}
				version = 2
			}
			if version == 2 {
				var hasObjects bool
				if e := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='objects')`).Scan(&hasObjects); e != nil {
					return e
				}
				if hasObjects {
					var hasOrderColumn, hasKindOrderColumn bool
					if e := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info('objects') WHERE name='query_order')`).Scan(&hasOrderColumn); e != nil {
						return e
					}
					if !hasOrderColumn {
						if _, e := c.ExecContext(ctx, `ALTER TABLE objects ADD COLUMN query_order INTEGER NOT NULL DEFAULT 0`); e != nil {
							return e
						}
					}
					if e := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info('objects') WHERE name='query_kind_order')`).Scan(&hasKindOrderColumn); e != nil {
						return e
					}
					if !hasKindOrderColumn {
						if _, e := c.ExecContext(ctx, `ALTER TABLE objects ADD COLUMN query_kind_order INTEGER NOT NULL DEFAULT 0`); e != nil {
							return e
						}
					}
				}
				// Cursor continuation is now carried by the token's indexed sort
				// value. Drop the old stateful table from the candidate schema.
				if _, e := c.ExecContext(ctx, `DROP TABLE IF EXISTS query_cursor_keys`); e != nil {
					return e
				}
				if _, e := c.ExecContext(ctx, `DROP INDEX IF EXISTS objects_kind_key`); e != nil {
					return e
				}
				if _, e := c.ExecContext(ctx, `DROP INDEX IF EXISTS objects_query_order`); e != nil {
					return e
				}
				if _, e := c.ExecContext(ctx, `DROP INDEX IF EXISTS objects_kind_query_order`); e != nil {
					return e
				}
				if _, e := c.ExecContext(ctx, `DROP INDEX IF EXISTS objects_kind_query_order_key`); e != nil {
					return e
				}
				if _, e := c.ExecContext(ctx, `UPDATE meta SET schema_version=3 WHERE singleton=1`); e != nil {
					return e
				}
				version = 3
			}
			// A schema-3 database can have been created by the first stateless
			// migration before the per-kind rank column was added. Repair that
			// shape in place without inventing another public schema version.
			if version == 3 {
				var hasObjects bool
				if e := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='objects')`).Scan(&hasObjects); e != nil {
					return e
				}
				if hasObjects {
					var hasOrderColumn, hasKindOrderColumn bool
					if e := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info('objects') WHERE name='query_order')`).Scan(&hasOrderColumn); e != nil {
						return e
					}
					if !hasOrderColumn {
						if _, e := c.ExecContext(ctx, `ALTER TABLE objects ADD COLUMN query_order INTEGER NOT NULL DEFAULT 0`); e != nil {
							return e
						}
					}
					if e := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info('objects') WHERE name='query_kind_order')`).Scan(&hasKindOrderColumn); e != nil {
						return e
					}
					if !hasKindOrderColumn {
						if _, e := c.ExecContext(ctx, `ALTER TABLE objects ADD COLUMN query_kind_order INTEGER NOT NULL DEFAULT 0`); e != nil {
							return e
						}
					}
				}
			}
			if version != schemaVersion && version != streamSchemaVersion {
				return fmt.Errorf("%w: sqlite schema version %d", driver.ErrUnsupported, version)
			}
		} else {
			var tables int
			if e := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`).Scan(&tables); e != nil {
				return e
			}
			if tables != 0 {
				return fmt.Errorf("%w: sqlite database is not an empty or Plug store", driver.ErrUnsupported)
			}
		}
		// Remove the obsolete candidate read-work table and triggers. Head
		// queries now report the rows they actually materialize, so no
		// persistent aggregate state is needed.
		for _, statement := range []string{
			`DROP TRIGGER IF EXISTS query_read_work_objects_insert`,
			`DROP TRIGGER IF EXISTS query_read_work_records_insert`,
			`DROP TRIGGER IF EXISTS query_read_work_records_update`,
			`DROP TRIGGER IF EXISTS query_read_work_records_delete`,
			`DROP TABLE IF EXISTS query_read_work`,
		} {
			if _, e := c.ExecContext(ctx, statement); e != nil {
				return e
			}
		}
		_, e := c.ExecContext(ctx, schema)
		if e != nil {
			return e
		}
		if e = ensureHistoricalQueryFields(ctx, c); e != nil {
			return e
		}
		if _, e = c.ExecContext(ctx, childIDSchema); e != nil {
			return e
		}
		_, e = c.ExecContext(ctx, `INSERT OR IGNORE INTO meta VALUES(1,?,?,?,?,?)`, randomID(), randomID(), schemaVersion, revKey(0), revKey(0))
		if e != nil {
			return e
		}
		_, e = c.ExecContext(ctx, `INSERT OR IGNORE INTO active_lease VALUES(1,?,'','',0)`, revKey(0))
		if e != nil {
			return e
		}
		if e = rebuildQueryOrder(ctx, c); e != nil {
			return e
		}
		_, e = readMeta(ctx, c)
		return e
	})
	if err == nil {
		// journal_mode cannot change inside a transaction. Initialization is
		// serialized with publishers, and repeated WAL assignment is harmless.
		var mode string
		gate, e := s.fileLock(ctx, "publication", false, nil)
		if e == nil {
			e = s.db.QueryRowContext(ctx, `PRAGMA journal_mode=WAL`).Scan(&mode)
			unlockFile(gate)
		}
		if e != nil {
			err = e
		} else if mode != "wal" {
			err = fmt.Errorf("sqlite requires WAL mode, got %s", mode)
		}
	}
	if err == nil {
		_, err = s.Meta(ctx)
	}
	if err != nil {
		_ = s.db.Close()
		return nil, err
	}
	return s, nil
}

func revKey(n uint64) string { return fmt.Sprintf("%020d", n) }
func parseRev(text string) (model.Revision, error) {
	n, err := strconv.ParseUint(text, 10, 64)
	return model.Natural64(n), err
}

// rebuildQueryOrder assigns dense lexical ranks to live records. The rank is
// an indexed sort value carried by stateless query cursors; tombstones retain
// their object identity but receive rank zero and never enter a current page.
// Writes perform this once per publication, while reads only use the indexes.
func rebuildQueryOrder(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}) error {
	_, err := q.ExecContext(ctx, `
WITH ranked AS (
 SELECT o.key AS key,
        ROW_NUMBER() OVER (ORDER BY o.key) AS query_order,
        ROW_NUMBER() OVER (PARTITION BY o.kind ORDER BY o.key) AS query_kind_order
 FROM objects AS o JOIN records AS r ON r.key=o.key
)
UPDATE objects
SET query_order=COALESCE((SELECT ranked.query_order FROM ranked WHERE ranked.key=objects.key), 0),
    query_kind_order=COALESCE((SELECT ranked.query_kind_order FROM ranked WHERE ranked.key=objects.key), 0)`)
	return err
}
func branchZero(b model.Branch) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if !b.IsZero() {
		return fmt.Errorf("%w: sqlite supports branch zero", driver.ErrUnsupported)
	}
	return nil
}
func recordKey(ref driver.Ref) (string, error) {
	if err := branchZero(ref.Branch); err != nil {
		return "", err
	}
	return driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
}
func keyID(k driver.LockKey) string { id, _ := driver.EncodeLockKey(k); return id }
func (s *Store) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("sqlite: store closed")
	}
	return nil
}
func (s *Store) track(c io.Closer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("sqlite: store closed")
	}
	s.children[c] = struct{}{}
	return nil
}
func (s *Store) untrack(c io.Closer) { s.mu.Lock(); delete(s.children, c); s.mu.Unlock() }

func (s *Store) fileLock(ctx context.Context, name string, share bool, lost <-chan struct{}) (*os.File, error) {
	return s.fileLockUntil(ctx, name, share, lost, nil)
}

// fileLockUntil also lets a waiter finish without taking the lock. Renewal uses
// this on every attempt, including when a publication starts after Renew starts
// waiting. A nil file with no error means the alternative completed the work.
func (s *Store) fileLockUntil(ctx context.Context, name string, share bool, lost <-chan struct{}, alternative func() (bool, error)) (*os.File, error) {
	hash := sha256.Sum256([]byte(name))
	path := filepath.Join(s.lockDir, hex.EncodeToString(hash[:]))
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case <-lost:
			return nil, driver.ErrFenced{}
		default:
		}
		if alternative != nil {
			if done, err := alternative(); done || err != nil {
				return nil, err
			}
		}
		f, ok, err := tryFileLock(path, share)
		if err != nil {
			return nil, err
		}
		if ok {
			return f, nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-lost:
			timer.Stop()
			return nil, driver.ErrFenced{}
		case <-timer.C:
		}
	}
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readMeta(ctx context.Context, q queryer) (driver.Meta, error) {
	var m driver.Meta
	var head, floor string
	var version int64
	err := q.QueryRowContext(ctx, `SELECT store_id,incarnation,schema_version,head,history_floor FROM meta WHERE singleton=1`).Scan(&m.StoreID, &m.Incarnation, &version, &head, &floor)
	if err != nil {
		return m, err
	}
	if version != schemaVersion && version != streamSchemaVersion {
		return m, fmt.Errorf("%w: sqlite schema version %d", driver.ErrUnsupported, version)
	}
	m.SchemaVersion = driver.SchemaVersion(version)
	m.State = "ready"
	m.Head, err = parseRev(head)
	if err == nil {
		m.HistoryFloor, err = parseRev(floor)
	}
	return m, err
}
func (s *Store) Meta(ctx context.Context) (driver.Meta, error) {
	if err := s.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	return readMeta(ctx, s.db)
}
func (s *Store) Migrate(ctx context.Context, to driver.SchemaVersion) (driver.MigrationReport, error) {
	m, err := s.Meta(ctx)
	if err != nil {
		return driver.MigrationReport{}, err
	}
	if to != m.SchemaVersion {
		return driver.MigrationReport{}, fmt.Errorf("%w: sqlite migration %d", driver.ErrUnsupported, to)
	}
	return driver.MigrationReport{From: to, To: to, Applied: []string{}, Warnings: []string{}}, nil
}
func (s *Store) Health(ctx context.Context) (driver.StoreHealth, error) {
	if err := s.check(ctx); err != nil {
		return driver.StoreHealth{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return driver.StoreHealth{}, err
	}
	defer tx.Rollback()
	m, err := readMeta(ctx, tx)
	if err != nil {
		return driver.StoreHealth{}, err
	}
	row, err := s.readLease(ctx, tx)
	if err != nil {
		return driver.StoreHealth{}, err
	}
	role := "unknown"
	if row.token != "" && s.now().UnixNano() < row.expires {
		role = "active"
	}
	return driver.StoreHealth{Driver: "sqlite", Role: role, LeaseEpoch: row.epoch, Head: m.Head, Incarnation: m.Incarnation, AuthorizationChain: []string{}}, nil
}

// A COMMIT error has an uncertain durable outcome, even if it is cancellation:
// modernc's deferred context check can replace a successful native result.
// Graph, checkpoint and finalizer callers map this private error to public
// ErrIndeterminate without unwrapping a no-publication rejection. Graph/receipt
// outcomes retain their request key; checkpoint callers retain their original
// key and expected generation. A confirmed COMMIT has no fallible follow-up.
type commitError struct{ error }

const publicationRollbackTimeout = 5 * time.Second

func (e commitError) Unwrap() error { return e.error }

func (e commitError) interrupted() bool {
	return errors.Is(e.error, context.Canceled) || errors.Is(e.error, context.DeadlineExceeded)
}

func (s *Store) immediate(ctx context.Context, l *activeLease, fn func(*sql.Conn) error) error {
	return s.immediatePublication(ctx, l, false, fn)
}

func (s *Store) immediatePublication(ctx context.Context, l *activeLease, restore bool, fn func(*sql.Conn) error) error {
	var lost <-chan struct{}
	if l != nil {
		lost = l.Lost()
	}
	gate, err := s.fileLock(ctx, "publication", false, lost)
	if err != nil {
		return err
	}
	return s.immediateLocked(ctx, gate, l, restore, fn)
}

// immediateLocked owns the cross-process publication gate through cleanup.
// Renew persists its own journal before acceptance, so COMMIT is the only native
// publication and its confirmed success cannot turn into a handoff error.
func (s *Store) immediateLocked(ctx context.Context, gate *os.File, l *activeLease, restore bool, fn func(*sql.Conn) error) (err error) {
	var reportAbort func()
	defer func() {
		unlockFile(gate)
		if reportAbort != nil {
			reportAbort()
		}
	}()
	c, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	var began, registered, committed bool
	var durable leaseHeartbeat
	start := time.Now()
	// Cancellation may be reported after native BEGIN acquired the writer.
	// Every unconfirmed path rolls back before returning the connection. After
	// a confirmed COMMIT there must be no further native transaction work.
	defer func() {
		if committed {
			return
		}
		var uncertain commitError
		attempted := errors.As(err, &uncertain)
		cleanup, stopCleanup := context.WithTimeout(context.WithoutCancel(ctx), publicationRollbackTimeout)
		_, rollbackErr := c.ExecContext(cleanup, `ROLLBACK`)
		stopCleanup()
		// SQLite may roll back the whole transaction when an earlier write
		// is interrupted. Autocommit is proof of rollback only when this
		// connection never attempted COMMIT.
		rolledBack := rollbackErr == nil || (!attempted && noNativeTransaction(rollbackErr))
		if registered {
			renewed := l.abortPublication(durable, rolledBack, attempted)
			duration, cause := time.Since(start), err
			reportAbort = func() {
				logPublicationAbort(ctx, l.epoch, start, duration, cause, rollbackErr, attempted, renewed, rolledBack)
			}
		}
		if !rolledBack && (began || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			// A connection that might still own an open native transaction
			// must never return to the pool after the publication gate opens.
			_ = c.Raw(func(any) error { return sqldriver.ErrBadConn })
		}
		// Preserve the existing restore-specific classification. Other
		// publication callers retain their established COMMIT error contract.
		if restore && rollbackErr == nil && attempted && uncertain.interrupted() {
			err = uncertain.error
		}
	}()
	if _, err = c.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	began = true
	if l != nil {
		durable, err = s.beginLeasePublication(ctx, c, l)
		if err != nil {
			return err
		}
		registered = true
	}
	if err = fn(c); err != nil {
		return err
	}
	if _, err = s.commitImmediate(ctx, c, l, durable); err != nil {
		return err
	}
	committed = true
	if l != nil {
		l.finishPublication()
		registered = false
	}
	return nil
}

// The reserved connection just executed the literal ROLLBACK. SQLite uses the
// same primary code for other failures, so require its exact autocommit
// diagnostic as well. An unrecognized error still fences the lease.
func noNativeTransaction(err error) bool {
	var native *nativesqlite.Error
	return errors.As(err, &native) && native.Code() == sqlite3.SQLITE_ERROR &&
		native.Error() == "SQL logic error: cannot rollback - no transaction is active (1)"
}

func logPublicationAbort(ctx context.Context, epoch uint64, start time.Time, duration time.Duration, cause, rollbackErr error, commitAttempted, renewed, rolledBack bool) {
	method := driver.PublicationMethod(ctx)
	if method == "" {
		method = "store.internal"
	}
	attrs := []any{
		"method", method, "epoch", epoch, "duration", duration, "commit_attempted", commitAttempted,
		"rollback_confirmed", rolledBack, "heartbeat_advanced", renewed,
	}
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		attrs = append(attrs, "budget_at_begin", deadline.Sub(start))
	}
	if timing, ok := driver.GetPublicationTiming(ctx); ok {
		attrs = append(attrs, "queue_wait", timing.QueueWait)
		if hasDeadline {
			deadlineSource := "operation"
			if deadline.Equal(timing.RequestDeadline) {
				deadlineSource = "request"
			}
			attrs = append(attrs, "deadline_source", deadlineSource)
		}
	}
	// Driver error text can contain SQL, record values or connection details.
	var native interface{ Code() int }
	switch {
	case cause == nil:
		attrs = append(attrs, "cause", "publication_interrupted")
	case errors.Is(cause, context.Canceled):
		attrs = append(attrs, "cause", "context_canceled")
	case errors.Is(cause, context.DeadlineExceeded):
		attrs = append(attrs, "cause", "deadline_exceeded")
	case errors.As(cause, &native):
		label := "sqlite_error"
		switch native.Code() & 0xff {
		case sqlite3.SQLITE_BUSY:
			label = "sqlite_busy"
		case sqlite3.SQLITE_LOCKED:
			label = "sqlite_locked"
		}
		attrs = append(attrs, "cause", label, "sqlite_code", native.Code())
	case errors.Is(cause, driver.ErrCheckpointConflict):
		attrs = append(attrs, "cause", "checkpoint_conflict")
	case errors.Is(cause, driver.ErrConflict):
		attrs = append(attrs, "cause", "write_conflict")
	default:
		if code, _, ok := driver.ErrorMapping(cause); ok {
			attrs = append(attrs, "cause", code)
		} else {
			attrs = append(attrs, "cause", "publication_error", "error_type", fmt.Sprintf("%T", cause))
		}
	}
	if !rolledBack {
		attrs = append(attrs, "rollback_error_type", fmt.Sprintf("%T", rollbackErr))
		if errors.As(rollbackErr, &native) {
			attrs = append(attrs, "rollback_sqlite_code", native.Code())
		}
		slog.WarnContext(ctx, "sqlite publication aborted", attrs...)
		return
	}
	slog.InfoContext(ctx, "sqlite publication aborted", attrs...)
}

func (s *Store) commitImmediate(ctx context.Context, c *sql.Conn, l *activeLease, durable leaseHeartbeat) (leaseHeartbeat, error) {
	if err := ctx.Err(); err != nil {
		return durable, err
	}
	heartbeat := durable
	if l != nil {
		l.mu.Lock()
		heartbeat = l.heartbeatLocked()
		l.mu.Unlock()
		if heartbeat.expires != durable.expires {
			if _, err := c.ExecContext(ctx, `UPDATE active_lease SET expires=? WHERE singleton=1 AND epoch=? AND token=?`, heartbeat.expires, revKey(l.epoch), l.token); err != nil {
				return durable, err
			}
		}
		// Check identity AND current effective expiry after the SQL work. BEGIN
		// alone is not sufficient: a holder that stops renewing must be fenced.
		if err := s.fence(ctx, c, l); err != nil {
			return durable, err
		}
	}
	// The final fence and request check authorize exactly one native COMMIT.
	// The publication gate excludes takeover until it returns. Once admitted,
	// COMMIT must return its native outcome even if the caller stops waiting.
	if err := ctx.Err(); err != nil {
		return durable, err
	}
	if s.abortStalledCommit != nil {
		stop, err := s.watchCommit(ctx, l)
		if err != nil {
			return durable, err
		}
		defer stop()
	}
	if _, err := c.ExecContext(context.WithoutCancel(ctx), `COMMIT`); err != nil {
		return durable, commitError{error: err}
	}
	return heartbeat, nil
}

const (
	publicationCommitCap         = 8 * time.Second
	publicationCommitLeaseMargin = 100 * time.Millisecond
)

type commitWatch struct {
	done chan struct{}
	// Accepted renewal publishes its new deadline here without requiring the
	// watchdog to take a mutex that journal fsync may be holding.
	leaseDeadline atomic.Pointer[time.Time]
}

func (s *Store) watchCommit(ctx context.Context, l *activeLease) (func(), error) {
	watch := &commitWatch{done: make(chan struct{})}
	var lost <-chan struct{}
	var epoch uint64
	if l != nil {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.lossReason != "" || !time.Now().Before(l.deadline) {
			return nil, driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: l.epoch}
		}
		deadline := l.deadline
		watch.leaseDeadline.Store(&deadline)
		lost, epoch = l.Lost(), l.epoch
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l != nil {
		l.commitWatch = watch
	}
	finished := make(chan struct{})
	started := time.Now()
	capDeadline := started.Add(publicationCommitCap)
	method := driver.PublicationMethod(ctx)
	if method == "" {
		method = "store.internal"
	}
	go func() {
		defer close(watch.done)
		for {
			deadline, reason := capDeadline, "commit cap exceeded"
			leaseDeadline := watch.leaseDeadline.Load()
			if leaseDeadline != nil {
				// Leave a small margin where the lease at admission allows it.
				// A commit admitted very near expiry keeps the remaining TTL.
				leaseLimit := leaseDeadline.Add(-publicationCommitLeaseMargin)
				if !started.Before(leaseLimit) {
					leaseLimit = *leaseDeadline
				}
				if leaseLimit.Before(deadline) {
					deadline, reason = leaseLimit, "lease deadline approaching"
				}
			}
			timer := time.NewTimer(time.Until(deadline))
			select {
			case <-finished:
				timer.Stop()
				return
			case <-lost:
				timer.Stop()
				reason = "active lease lost"
			case <-timer.C:
				if deadline.Before(capDeadline) && leaseDeadline != watch.leaseDeadline.Load() {
					continue
				}
			}
			select {
			case <-finished:
			default:
				s.abortStalledCommit(fmt.Errorf("sqlite COMMIT stalled: %s; outcome uncertain (method=%s epoch=%d elapsed=%s)", reason, method, epoch, time.Since(started)))
			}
			return
		}
	}()
	return func() {
		close(finished)
		<-watch.done
	}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	children := make([]io.Closer, 0, len(s.children))
	for c := range s.children {
		children = append(children, c)
	}
	leases := make([]*activeLease, 0, len(s.leases))
	for l := range s.leases {
		leases = append(leases, l)
	}
	s.mu.Unlock()
	var errs []error
	for _, c := range children {
		errs = append(errs, c.Close())
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, l := range leases {
		errs = append(errs, l.Release(releaseCtx))
	}
	errs = append(errs, s.db.Close())
	return errors.Join(errs...)
}

var _ driver.Driver = Driver{}
var _ driver.Store = (*Store)(nil)
var _ driver.RestoreCapabilities = (*Store)(nil)

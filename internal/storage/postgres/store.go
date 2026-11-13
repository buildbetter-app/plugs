// Package postgres implements the storage contract using PostgreSQL transactions
// and MVCC. One database contains one store; engine handles never leave here.
package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Store owns a connection pool and its readers, transactions and active leases.
// Independent Open calls share only the durable state in the DSN's database.
type Store struct {
	db       *sql.DB
	lockConn *sql.Conn
	// renewConn is reserved from the pool so lease heartbeats cannot wait for
	// request transactions to return a general-purpose connection.
	renewConn *sql.Conn
	renewMu   sync.Mutex
	lockMu    sync.Mutex
	mu        sync.Mutex
	closed    bool
	resources map[interface{ Close() error }]struct{}
	leases    map[*activeLease]struct{}
}

// Persistent reports that PostgreSQL checkpoint bytes survive Store.Close and
// a subsequent open of the same database.
func (*Store) Persistent() bool { return true }

// SupportsRestore advertises the only restore mode this store can publish
// through its existing lease-fenced transaction path. Full-history injection
// requires a separately qualified offline-history port.
func (*Store) SupportsRestore(mode driver.RestoreMode) bool {
	return mode == driver.RestoreModeCurrentImage
}

// Open opens a real database and initializes schema version 1 atomically.
// Options are deliberately refused until they have specified semantics.
func Open(ctx context.Context, cfg driver.Config) (*Store, error) {
	if cfg.DSN == "" || len(cfg.Options) != 0 {
		return nil, fmt.Errorf("%w: postgres requires DSN and no options", driver.ErrUnsupported)
	}
	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: invalid connection configuration")
	}
	db.SetMaxOpenConns(96)
	db.SetMaxIdleConns(16)
	lockConn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	renewConn, err := db.Conn(ctx)
	if err != nil {
		_ = lockConn.Close()
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db, lockConn: lockConn, renewConn: renewConn, resources: make(map[interface{ Close() error }]struct{}), leases: make(map[*activeLease]struct{})}
	if err := s.initialize(ctx); err != nil {
		_ = renewConn.Close()
		_ = lockConn.Close()
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func identity() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (s *Store) initialize(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// A database-local migration lock serializes concurrent first Open calls.
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(58193481276901)"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, schemaDDL); err != nil {
		return err
	}
	if err = ensureHistoricalQueryFields(ctx, tx); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, childIDSchema); err != nil {
		return err
	}
	id, err := identity()
	if err != nil {
		return err
	}
	incarnation, err := identity()
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO plug_postgres.meta VALUES (true,$1,$2,1,0,0) ON CONFLICT DO NOTHING`, id, incarnation); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO plug_postgres.lease VALUES (true,0,'','','epoch') ON CONFLICT DO NOTHING`); err != nil {
		return err
	}
	return tx.Commit()
}

type querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func readMeta(ctx context.Context, q querier, suffix string) (driver.Meta, error) {
	var m driver.Meta
	var head, floor string
	var schema uint64
	err := q.QueryRowContext(ctx, `SELECT store_id,incarnation,schema_version,head::text,history_floor::text FROM plug_postgres.meta WHERE singleton`+suffix).Scan(&m.StoreID, &m.Incarnation, &schema, &head, &floor)
	if err != nil {
		return m, err
	}
	h, err := parseCounter(head)
	if err != nil {
		return m, err
	}
	f, err := parseCounter(floor)
	if err != nil {
		return m, err
	}
	m.SchemaVersion, m.Head, m.HistoryFloor, m.State = driver.SchemaVersion(schema), model.Natural64(h), model.Natural64(f), "ready"
	return m, nil
}

func parseCounter(s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("postgres: counter outside uint64 domain: %w", err)
	}
	return n, nil
}
func decimal(n uint64) string { return strconv.FormatUint(n, 10) }

func (s *Store) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return driver.ErrTxClosed
	}
	return nil
}

// renewConnLocked returns the reserved heartbeat connection while renewMu is
// held. A cancelled PostgreSQL query can poison an *sql.Conn; callers must
// discard that handle before the next heartbeat rather than retaining it.
func (s *Store) renewConnLocked(ctx context.Context) (*sql.Conn, error) {
	if s.renewConn != nil {
		return s.renewConn, nil
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, driver.ErrTxClosed
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	closed = s.closed
	if !closed {
		s.renewConn = conn
	}
	s.mu.Unlock()
	if closed {
		_ = conn.Close()
		return nil, driver.ErrTxClosed
	}
	return conn, nil
}

// discardRenewConnLocked removes a failed reserved heartbeat connection while
// renewMu is held. The next heartbeat reacquires a fresh reserved handle.
func (s *Store) discardRenewConnLocked(conn *sql.Conn) {
	if s.renewConn != conn {
		return
	}
	s.renewConn = nil
	_ = conn.Close()
}

// ensureLockKeys uses the connection reserved for lock-row preparation. Lock
// acquisition itself holds one pooled writer connection; using the general
// pool here would deadlock when all writers reach this boundary together.
func (s *Store) ensureLockKeys(ctx context.Context, ids []string) error {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.lockConn == nil {
		return driver.ErrTxClosed
	}
	if len(ids) != 0 {
		keys := make([][]byte, len(ids))
		for i, id := range ids {
			keys[i] = []byte(id)
		}
		if _, err := s.lockConn.ExecContext(ctx, `INSERT INTO plug_postgres.locks (key)
		 SELECT key FROM unnest($1::bytea[]) AS pending(key) ORDER BY key ON CONFLICT DO NOTHING`, keys); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
	return nil
}
func (s *Store) track(r interface{ Close() error }) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return driver.ErrTxClosed
	}
	s.resources[r] = struct{}{}
	return nil
}
func (s *Store) untrack(r interface{ Close() error }) {
	s.mu.Lock()
	delete(s.resources, r)
	s.mu.Unlock()
}

func (s *Store) Meta(ctx context.Context) (driver.Meta, error) {
	if err := s.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	return readMeta(ctx, s.db, "")
}
func (s *Store) Migrate(ctx context.Context, to driver.SchemaVersion) (driver.MigrationReport, error) {
	m, err := s.Meta(ctx)
	if err != nil {
		return driver.MigrationReport{}, err
	}
	if to != m.SchemaVersion {
		return driver.MigrationReport{}, fmt.Errorf("%w: postgres schema version %d", driver.ErrUnsupported, to)
	}
	return driver.MigrationReport{From: to, To: to, Applied: []string{}, Warnings: []string{}}, nil
}
func (s *Store) Health(ctx context.Context) (driver.StoreHealth, error) {
	m, err := s.Meta(ctx)
	if err != nil {
		return driver.StoreHealth{}, err
	}
	var epoch, owner string
	var active bool
	if err = s.db.QueryRowContext(ctx, `SELECT epoch::text,owner,expires>clock_timestamp() FROM plug_postgres.lease WHERE singleton`).Scan(&epoch, &owner, &active); err != nil {
		return driver.StoreHealth{}, err
	}
	e, err := parseCounter(epoch)
	if err != nil {
		return driver.StoreHealth{}, err
	}
	role := "standby"
	s.mu.Lock()
	for l := range s.leases {
		if l.owner == owner && active && !l.ended() {
			role = "active"
		}
	}
	s.mu.Unlock()
	return driver.StoreHealth{Driver: "postgres", Role: role, LeaseEpoch: e, Head: m.Head, Incarnation: m.Incarnation, AuthorizationChain: []string{}}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	resources := make([]interface{ Close() error }, 0, len(s.resources))
	for r := range s.resources {
		resources = append(resources, r)
	}
	leases := make([]*activeLease, 0, len(s.leases))
	for l := range s.leases {
		leases = append(leases, l)
	}
	s.mu.Unlock()
	var errs []error
	// Roll back transactions before releasing the epoch, then close the pool.
	for _, r := range resources {
		errs = append(errs, r.Close())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, l := range leases {
		errs = append(errs, l.Release(ctx))
	}
	s.lockMu.Lock()
	if s.lockConn != nil {
		errs = append(errs, s.lockConn.Close())
		s.lockConn = nil
	}
	s.lockMu.Unlock()
	s.renewMu.Lock()
	if s.renewConn != nil {
		errs = append(errs, s.renewConn.Close())
		s.renewConn = nil
	}
	s.renewMu.Unlock()
	errs = append(errs, s.db.Close())
	return errors.Join(errs...)
}

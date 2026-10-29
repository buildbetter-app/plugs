// Package sqlite implements the internal storage contract using a local,
// file-backed SQLite database in WAL mode. No engine handle crosses the driver.
package sqlite

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	_ "modernc.org/sqlite"
)

type Driver struct{}

func (Driver) Info() driver.Info {
	return driver.Info{Name: "sqlite", Version: "1", Capabilities: []string{"transactions", "history", "leases", "checkpoints", "noop", "response_finalization"}}
}

type Store struct {
	db       *sql.DB
	lockDir  string
	mu       sync.Mutex
	closed   bool
	children map[io.Closer]struct{}
	leases   map[*activeLease]struct{}
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func (Driver) Open(ctx context.Context, cfg driver.Config) (driver.Store, error) {
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
	if err = f.Close(); err != nil {
		return nil, err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	s := &Store{lockDir: path + ".plug-locks", children: make(map[io.Closer]struct{}), leases: make(map[*activeLease]struct{})}
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
		_, e := c.ExecContext(ctx, schema)
		if e != nil {
			return e
		}
		_, e = c.ExecContext(ctx, `INSERT OR IGNORE INTO meta VALUES(1,?,?,1,?,?)`, randomID(), randomID(), revKey(0), revKey(0))
		if e != nil {
			return e
		}
		_, e = c.ExecContext(ctx, `INSERT OR IGNORE INTO active_lease VALUES(1,?,'','',0)`, revKey(0))
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
	if version != 1 {
		return m, fmt.Errorf("%w: sqlite schema version %d", driver.ErrUnsupported, version)
	}
	m.SchemaVersion = 1
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
	row, err := readLease(ctx, tx)
	if err != nil {
		return driver.StoreHealth{}, err
	}
	role := "unknown"
	if row.token != "" && time.Now().UnixNano() < row.expires {
		role = "active"
	}
	return driver.StoreHealth{Driver: "sqlite", Role: role, LeaseEpoch: row.epoch, Head: m.Head, Incarnation: m.Incarnation, AuthorizationChain: []string{}}, nil
}

// A COMMIT error has an uncertain durable outcome. Graph callers map this to
// ErrIndeterminate with the original request key rather than retrying blindly.
type commitError struct{ error }

func (s *Store) immediate(ctx context.Context, l *activeLease, fn func(*sql.Conn) error) error {
	var lost <-chan struct{}
	if l != nil {
		lost = l.Lost()
	}
	gate, err := s.fileLock(ctx, "publication", false, lost)
	if err != nil {
		return err
	}
	defer unlockFile(gate)
	c, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err = c.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	defer func() { _, _ = c.ExecContext(context.Background(), `ROLLBACK`) }()
	if l != nil {
		if err = s.fence(ctx, c, l); err != nil {
			return err
		}
	}
	if err = fn(c); err != nil {
		return err
	}
	if l != nil {
		if err = s.fence(ctx, c, l); err != nil {
			return err
		}
	}
	if _, err = c.ExecContext(ctx, `COMMIT`); err != nil {
		return commitError{err}
	}
	return nil
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
	for _, l := range leases {
		errs = append(errs, l.Release(context.Background()))
	}
	errs = append(errs, s.db.Close())
	return errors.Join(errs...)
}

var _ driver.Driver = Driver{}
var _ driver.Store = (*Store)(nil)

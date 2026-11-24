package sqlite

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSQLiteRestoreInterruptedCommitRequiresRollbackProof(t *testing.T) {
	for _, tc := range []struct {
		name      string
		published bool
	}{
		{name: "rolled-back"},
		{name: "publication-uncertain", published: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "restore-interrupted-commit.db")
			s := openStore(t, ctx, path)
			observer := openStore(t, ctx, path)
			lease := getLease(t, ctx, s, driver.LeaseTTL).(*activeLease)
			call, connector := cancelMetadataCommit(t, ctx, s, path, tc.published)
			const key = "restore-interrupted-commit"
			err := s.immediatePublication(call, lease, true, func(c *sql.Conn) error {
				_, err := c.ExecContext(call, `INSERT INTO checkpoints VALUES(?,?,?)`, key, revKey(1), []byte("restore"))
				return err
			})
			durable, readErr := observer.LoadCheckpoint(ctx, key)
			if readErr != nil || connector.rollbacks.Load() != 1 {
				t.Fatalf("readback=%+v err=%v rollbacks=%d", durable, readErr, connector.rollbacks.Load())
			}
			if tc.published {
				var unknown commitError
				if !errors.As(err, &unknown) || durable.Generation != 1 || connector.steps.Load() != 1 {
					t.Fatalf("unacknowledged publication: err=%v readback=%+v commits=%d", err, durable, connector.steps.Load())
				}
				requireUncertainLeaseLost(t, ctx, lease)
			} else {
				if err != context.Canceled || durable.Generation != 0 || connector.steps.Load() != 0 {
					t.Fatalf("confirmed rollback: err=%v readback=%+v commits=%d", err, durable, connector.steps.Load())
				}
				if err := lease.Renew(ctx); err != nil {
					t.Fatalf("rolled-back restore lost the active lease: %v", err)
				}
			}
		})
	}
}

// modernc's statement exec replaces its result with ctx.Err when the
// cancellation watcher wins, including after sqlite3_step has finished. This
// connector makes that boundary deterministic AFTER the real native BEGIN;
// it neither invents SQLITE_BUSY nor substitutes a database or lock.
type canceledBeginConnector struct {
	driver sqldriver.Driver
	dsn    string
	cancel context.CancelFunc
	armed  atomic.Bool
	steps  atomic.Int32
}

func (c *canceledBeginConnector) Driver() sqldriver.Driver { return c.driver }
func (c *canceledBeginConnector) Connect(context.Context) (sqldriver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &canceledBeginConn{Conn: conn, connector: c}, nil
}

type canceledBeginConn struct {
	sqldriver.Conn
	connector *canceledBeginConnector
}

func (c *canceledBeginConn) ExecContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	result, err := c.Conn.(sqldriver.ExecerContext).ExecContext(ctx, query, args)
	if query == "BEGIN IMMEDIATE" && err == nil && c.connector.armed.CompareAndSwap(true, false) {
		c.connector.steps.Add(1)
		c.connector.cancel()
		return nil, ctx.Err()
	}
	return result, err
}

func TestCanceledSuccessfulBeginReleasesNativeWriter(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "successful-canceled-begin.db")
	a := openStore(t, ctx, path)
	b := openStore(t, ctx, path)
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	connector := &canceledBeginConnector{driver: a.db.Driver(), dsn: path, cancel: cancel}
	if err := a.db.Close(); err != nil {
		t.Fatal(err)
	}
	a.db = sql.OpenDB(connector)
	a.db.SetMaxOpenConns(1)
	t.Cleanup(func() { _, _ = a.db.ExecContext(context.Background(), "ROLLBACK") })
	connector.armed.Store(true)
	bodyCalled := false
	err := a.immediate(call, nil, func(*sql.Conn) error {
		bodyCalled = true
		return nil
	})
	if !errors.Is(err, context.Canceled) || bodyCalled || connector.steps.Load() != 1 {
		t.Fatalf("actual-BEGIN cancellation seam: err=%v body=%v nativeBegins=%d", err, bodyCalled, connector.steps.Load())
	}
	if err := b.immediate(ctx, nil, func(c *sql.Conn) error {
		_, err := readLease(ctx, c)
		return err
	}); err != nil {
		t.Fatalf("native writer retained after failed BEGIN returned its connection: %T %v", err, err)
	}
	t.Log("real native BEGIN completed once, cancellation error returned, separate handle acquired native writer without retry")
}

// Exercise the actual driver's cancellation boundary, not a simulated busy
// error. A failed BEGIN must not leave the pooled connection owning SQLite's
// writer lock after the publication file lock has been released.
func TestCanceledPublicationBeginReleasesWriter(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "canceled-begin.db")
	a := openStore(t, ctx, path)
	b := openStore(t, ctx, path)
	a.db.SetMaxOpenConns(1)
	t.Cleanup(func() { _, _ = a.db.ExecContext(context.Background(), "ROLLBACK") })
	canceled, beforeBody := 0, 0
	for i := 0; i < 256; i++ {
		call, cancel := context.WithCancel(ctx)
		// Vary the real cancellation boundary around a short native BEGIN.
		// This timer ends work; it never waits for contention or retries it.
		timer := time.AfterFunc(time.Duration(1+i%32)*time.Microsecond, cancel)
		called := false
		err := a.immediate(call, nil, func(c *sql.Conn) error {
			called = true
			_, err := readLease(call, c)
			return err
		})
		timer.Stop()
		cancel()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("publication %d: %T %v", i, err, err)
			}
			canceled++
			if !called {
				beforeBody++
			}
		}
		// The first publisher has returned and released its advisory lock.
		// A different real handle must acquire the native writer immediately.
		if err := b.immediate(ctx, nil, func(c *sql.Conn) error {
			_, err := readLease(ctx, c)
			return err
		}); err != nil {
			t.Fatalf("writer retained after publication %d: body=%v original=%v probe=%T %v", i, called, call.Err(), err, err)
		}
	}
	t.Logf("real canceled publications=%d before-body=%d; separate handle acquired writer after every return", canceled, beforeBody)
}

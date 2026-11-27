package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	nativesqlite "modernc.org/sqlite"
)

func TestSQLiteRequestCancellationAtPublicationBoundary(t *testing.T) {
	for _, mode := range []string{"during_rollback", "before_commit", "after_commit_admission"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "publication-boundary.db")
			store := openStore(t, ctx, path)
			observer := openStore(t, ctx, path)
			lease := getLease(t, ctx, store, driver.LeaseTTL).(*activeLease)
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			connector := hookPublicationCommit(t, store, path)
			failure := errors.New("request aborted publication")
			var output bytes.Buffer
			logger := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
			t.Cleanup(func() { slog.SetDefault(logger) })
			if mode == "during_rollback" {
				connector.beforeRollback = func(cleanup context.Context) {
					cancel()
					deadline, hasDeadline := cleanup.Deadline()
					if cleanup.Err() != nil || !hasDeadline || time.Until(deadline) > publicationRollbackTimeout {
						t.Errorf("rollback did not get an independent cleanup budget: %v", cleanup.Err())
					}
				}
			}
			if mode == "after_commit_admission" {
				connector.before = cancel
			}
			const key = "publication-boundary"
			err := store.immediate(call, lease, func(c *sql.Conn) error {
				if _, err := c.ExecContext(call, `INSERT INTO checkpoints VALUES(?,?,?)`, key, revKey(1), []byte("staged")); err != nil {
					return err
				}
				if err := lease.Renew(ctx); err != nil {
					return err
				}
				switch mode {
				case "during_rollback":
					return failure
				case "before_commit":
					cancel()
				}
				return nil
			})
			wantGeneration := uint64(0)
			if mode == "after_commit_admission" {
				wantGeneration = 1
				if err != nil || connector.commitCalls.Load() != 1 || connector.steps.Load() != 1 || connector.rollbacks.Load() != 0 {
					t.Fatalf("admitted COMMIT = %v calls=%d completed=%d rollbacks=%d", err, connector.commitCalls.Load(), connector.steps.Load(), connector.rollbacks.Load())
				}
			} else {
				want := error(context.Canceled)
				if mode == "during_rollback" {
					want = failure
				}
				var uncertain commitError
				if !errors.Is(err, want) || errors.As(err, &uncertain) || connector.commitCalls.Load() != 0 || connector.rollbacks.Load() != 1 {
					t.Fatalf("aborted write = %v native COMMIT calls=%d rollbacks=%d", err, connector.commitCalls.Load(), connector.rollbacks.Load())
				}
				if log := output.String(); !strings.Contains(log, "commit_attempted=false") || !strings.Contains(log, "rollback_confirmed=true") {
					t.Fatalf("missing rollback proof in log: %s", log)
				}
			}
			if call.Err() != context.Canceled {
				t.Fatal("test missed request cancellation")
			}
			durable, err := observer.LoadCheckpoint(ctx, key)
			if err != nil || durable.Generation != wantGeneration {
				t.Fatalf("checkpoint generation = %d, want %d, error = %v", durable.Generation, wantGeneration, err)
			}
			assertDurableHeartbeat(t, ctx, observer, lease)
			t.Logf("commit calls=%d confirmed rollback=%t durable generation=%d lease epoch=%d", connector.commitCalls.Load(), connector.rollbacks.Load() == 1, durable.Generation, lease.Epoch())
		})
	}
}

func TestSQLitePublicationDeadlineAutoRollbackKeepsLease(t *testing.T) {
	ctx := testContext(t)
	var call context.Context
	var entered bool
	function := "plug_test_wait_" + randomID()
	if err := nativesqlite.RegisterScalarFunction(function, 0, func(*nativesqlite.FunctionContext, []sqldriver.Value) (sqldriver.Value, error) {
		entered = true
		<-call.Done()
		return int64(1), nil
	}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "publication-auto-rollback.db")
	store := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	lease := getLease(t, ctx, store, driver.LeaseTTL).(*activeLease)
	var nativeRollbackErr error
	connector := hookPublicationCommit(t, store, path)
	connector.afterRollback = func(err error) { nativeRollbackErr = err }
	var output bytes.Buffer
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(logger) })
	request, stopRequest := context.WithTimeout(ctx, 4*time.Second)
	defer stopRequest()
	request = driver.WithPublicationTiming(request, 150*time.Millisecond)
	var stopCall context.CancelFunc
	call, stopCall = context.WithTimeout(request, time.Second)
	defer stopCall()
	err := store.immediate(call, lease, func(c *sql.Conn) error {
		if _, err := c.ExecContext(call, `INSERT INTO checkpoints VALUES(?,?,?)`, "pending", revKey(1), []byte("staged")); err != nil {
			return err
		}
		if err := lease.Renew(ctx); err != nil {
			return err
		}
		query := fmt.Sprintf(`WITH RECURSIVE generated(n) AS (
			SELECT %s() UNION ALL SELECT n+1 FROM generated WHERE n<1000000
		) INSERT INTO checkpoints SELECT 'generated-' || n, ?, x'01' FROM generated`, function)
		_, err := c.ExecContext(call, query, revKey(1))
		return err
	})
	if !entered || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("did not expire inside native write: entered=%t error=%v", entered, err)
	}
	if !noNativeTransaction(nativeRollbackErr) || connector.commitCalls.Load() != 0 {
		t.Fatalf("missed SQLite's automatic rollback: cleanup=%v COMMIT calls=%d", nativeRollbackErr, connector.commitCalls.Load())
	}
	if cp, err := observer.LoadCheckpoint(ctx, "pending"); err != nil || cp.Generation != 0 {
		t.Fatalf("interrupted native write retained staged data: %v %v", cp, err)
	}
	assertDurableHeartbeat(t, ctx, observer, lease)
	fields := output.String()
	for _, part := range []string{"commit_attempted=false", "rollback_confirmed=true", "cause=deadline_exceeded", "budget_at_begin=", "queue_wait=150ms", "deadline_source=operation"} {
		if !strings.Contains(fields, part) {
			t.Fatalf("publication log omitted %q: %s", part, fields)
		}
	}
	t.Log(strings.TrimSpace(fields))
}

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
			call, connector := interruptMetadataCommit(t, ctx, s, path, tc.published)
			const key = "restore-interrupted-commit"
			err := s.immediatePublication(call, lease, true, func(c *sql.Conn) error {
				if _, err := c.ExecContext(call, `INSERT INTO checkpoints VALUES(?,?,?)`, key, revKey(1), []byte("restore")); err != nil {
					return err
				}
				return lease.Renew(ctx)
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
				row, err := observer.readLease(ctx, observer.db)
				if err != nil || row.expires != lease.expires {
					t.Fatalf("locally fenced lease lost its durable expiry: %v", err)
				}
				wait, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
				if next, err := observer.Lease(wait, "contender", driver.LeaseTTL); next != nil || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("takeover ignored the durable expiry after local loss: %v %v", next, err)
				}
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

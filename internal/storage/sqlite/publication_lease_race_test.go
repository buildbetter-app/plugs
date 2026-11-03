package sqlite

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

type publicationHeartbeatResult struct {
	successes int
	err       error
}

// Stopping the test's ticker does not cancel an admitted Renew halfway through
// its durable publication. Cancellation is exercised separately below.
func publicationHeartbeat(ctx context.Context, lease driver.ActiveLease, start, stop <-chan struct{}) <-chan publicationHeartbeatResult {
	done := make(chan publicationHeartbeatResult, 1)
	go func() {
		var result publicationHeartbeatResult
		defer func() { done <- result }()
		select {
		case <-start:
		case <-stop:
			return
		case <-ctx.Done():
			result.err = ctx.Err()
			return
		}
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			if err := lease.Renew(ctx); err != nil {
				result.err = err
				return
			}
			result.successes++
			select {
			case <-stop:
				return
			case <-ctx.Done():
				result.err = ctx.Err()
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

// These hooks surround REAL native COMMITs; they do not replace an engine
// result. They expose heartbeats during the SQL/fsync interval and after the
// durable graph commit but before the driver's publication handoff.
type publicationCommitConnector struct {
	driver sqldriver.Driver
	dsn    string
	before func()
	after  func()
	armed  atomic.Bool
	steps  atomic.Int32
}

func (c *publicationCommitConnector) Driver() sqldriver.Driver { return c.driver }
func (c *publicationCommitConnector) Connect(context.Context) (sqldriver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &publicationCommitConn{Conn: conn, connector: c}, nil
}

type publicationCommitConn struct {
	sqldriver.Conn
	connector *publicationCommitConnector
}

func (c *publicationCommitConn) ExecContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	hook := query == "COMMIT" && c.connector.armed.CompareAndSwap(true, false)
	if hook && c.connector.before != nil {
		c.connector.before()
	}
	result, err := c.Conn.(sqldriver.ExecerContext).ExecContext(ctx, query, args)
	if query == "COMMIT" && err == nil {
		c.connector.steps.Add(1)
		if hook && c.connector.after != nil {
			c.connector.after()
		}
	}
	return result, err
}

func hookPublicationCommit(t *testing.T, s *Store, path string) *publicationCommitConnector {
	t.Helper()
	connector := &publicationCommitConnector{driver: s.db.Driver(), dsn: path + "?_pragma=busy_timeout(0)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s.db = sql.OpenDB(connector)
	s.db.SetMaxOpenConns(1)
	connector.armed.Store(true)
	return connector
}

func TestSQLitePublicationLeaseHeartbeatsAcrossCommit(t *testing.T) {
	for _, mode := range []string{"graph", "renewal"} {
		t.Run(mode, func(t *testing.T) { publicationHeartbeatsAcrossCommit(t, mode) })
	}
}

func publicationHeartbeatsAcrossCommit(t *testing.T, mode string) {
	t.Helper()
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "commit-heartbeats.db")
	s := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, time.Second).(*activeLease)
	connector := hookPublicationCommit(t, s, path)
	start, stop := make(chan struct{}), make(chan struct{})
	done := publicationHeartbeat(ctx, lease, start, stop)
	var held time.Duration
	connector.before = func() {
		started := time.Now()
		close(start)
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		held = time.Since(started)
	}
	var lateRenew error
	connector.after = func() { lateRenew = lease.Renew(ctx) }
	var err error
	if mode == "renewal" {
		// A lease-only publication must not serialize other timely Renew calls
		// behind its own SQL/fsync interval either.
		err = lease.Renew(ctx)
	} else {
		err = s.immediate(ctx, lease, func(c *sql.Conn) error {
			_, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "commit-heartbeats", revKey(1), []byte("committed"))
			return err
		})
	}
	close(stop)
	renewal := <-done
	if err != nil || lateRenew != nil || renewal.err != nil || renewal.successes == 0 || held < 2*time.Second {
		t.Fatalf("publication=%v lateRenew=%v heartbeat=%+v held=%s", err, lateRenew, renewal, held)
	}
	if connector.steps.Load() < 2 {
		t.Fatal("heartbeat racing native COMMIT was not durably published before handoff")
	}
	row, err := readLease(ctx, observer.db)
	if err != nil {
		t.Fatal(err)
	}
	lease.mu.Lock()
	expires, deadline, publishing, lost := lease.expires, lease.deadline, lease.publishing, lease.lostFlag
	lease.mu.Unlock()
	if row.expires != expires || row.expires != deadline.UnixNano() || publishing || lost {
		t.Fatalf("handoff left private heartbeat: durable=%d effective=%d deadline=%s publishing=%v lost=%v", row.expires, expires, deadline, publishing, lost)
	}
	cp, err := observer.LoadCheckpoint(ctx, "commit-heartbeats")
	wantGeneration, wantPayload := uint64(1), "committed"
	if mode == "renewal" {
		wantGeneration, wantPayload = 0, ""
	}
	if err != nil || cp.Generation != wantGeneration || string(cp.Payload) != wantPayload {
		t.Fatalf("native publication=%+v err=%v want generation=%d", cp, err, wantGeneration)
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if next, err := observer.Lease(short, "contender", driver.LeaseTTL); next != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handoff exposed expired durable lease: next=%v err=%v", next, err)
	}
	t.Logf("held real COMMIT boundary %s with %d timely heartbeats; late heartbeat flushed through %d native COMMITs; independent handle still excluded", held, renewal.successes, connector.steps.Load())
}

func TestSQLitePublicationLeaseAbortFailsClosed(t *testing.T) {
	for _, mode := range []string{"callback-error", "cancellation", "after-durable-commit"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "abort-heartbeat.db")
			s := openStore(t, ctx, path)
			observer := openStore(t, ctx, path)
			lease := getLease(t, ctx, s, time.Second).(*activeLease)
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			failure := errors.New("discard graph publication")
			want := error(failure)
			var heartbeatErr error
			if mode == "after-durable-commit" {
				connector := hookPublicationCommit(t, s, path)
				connector.after = func() {
					heartbeatErr = lease.Renew(ctx)
					cancel()
				}
				want = context.Canceled
			}
			err := s.immediate(call, lease, func(c *sql.Conn) error {
				if _, err := c.ExecContext(call, `INSERT INTO checkpoints VALUES(?,?,?)`, "abort", revKey(1), []byte("graph")); err != nil {
					return err
				}
				if mode == "after-durable-commit" {
					return nil
				}
				heartbeatErr = lease.Renew(ctx)
				if heartbeatErr != nil {
					return heartbeatErr
				}
				if mode == "cancellation" {
					cancel()
					want = context.Canceled
					return call.Err()
				}
				return failure
			})
			if heartbeatErr != nil || !errors.Is(err, want) {
				t.Fatalf("accepted heartbeat=%v publication=%v want=%v", heartbeatErr, err, want)
			}
			select {
			case <-lease.Lost():
			default:
				t.Fatal("aborted private heartbeat escaped the publication gate while live")
			}
			requireFenced(t, lease.Renew(ctx))
			cp, readErr := observer.LoadCheckpoint(ctx, "abort")
			wantGeneration := uint64(0)
			if mode == "after-durable-commit" {
				wantGeneration = 1
				var uncertain commitError
				if !errors.As(err, &uncertain) {
					t.Fatalf("committed graph reported as determinate failure: %T %v", err, err)
				}
			}
			if readErr != nil || cp.Generation != wantGeneration {
				t.Fatalf("durable graph=%+v err=%v want generation=%d", cp, readErr, wantGeneration)
			}
			lease.mu.Lock()
			publishing := lease.publishing
			lease.mu.Unlock()
			if publishing {
				t.Fatal("failed publication left a registered heartbeat bridge")
			}
			t.Logf("%s: accepted heartbeat failed closed; graph generation=%d; stale Renew fenced", mode, cp.Generation)
		})
	}
}

func TestSQLitePublicationLeaseRepeatedWallStampAbortFailsClosed(t *testing.T) {
	ctx := testContext(t)
	clock := testclock.New(t, time.Date(2037, 1, 1, 0, 0, 0, 0, time.UTC))
	s := clockStore(t, clock)
	lease := getLease(t, ctx, s, time.Second).(*activeLease)
	original, err := readLease(ctx, s.db)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("abort heartbeat with identical wall stamp")
	err = s.immediate(ctx, lease, func(*sql.Conn) error {
		if err := lease.Renew(ctx); err != nil {
			return err
		}
		lease.mu.Lock()
		expires := lease.expires
		lease.mu.Unlock()
		if expires != original.expires {
			return fmt.Errorf("did not exercise identical durable/private wall stamp")
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	select {
	case <-lease.Lost():
	default:
		t.Fatal("identical timestamp hid an aborted acknowledged heartbeat")
	}
	requireFenced(t, lease.Renew(ctx))
}

func TestSQLitePublicationLeaseChecksMonotonicDeadlineWithoutExpiryCallback(t *testing.T) {
	ctx := testContext(t)
	// Freeze only the application wall clock. A real elapsed TTL must still
	// fence even if both the application clock and expiry callback are delayed.
	clock := testclock.New(t, time.Date(2037, 1, 1, 0, 0, 0, 0, time.UTC))
	s := clockStore(t, clock)
	lease := getLease(t, ctx, s, time.Second).(*activeLease)
	lease.mu.Lock()
	if !lease.timer.Stop() {
		lease.mu.Unlock()
		t.Fatal("expiry callback already ran before the test could delay it")
	}
	lease.mu.Unlock()
	err := s.immediate(ctx, lease, func(c *sql.Conn) error {
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-lease.Lost():
			return fmt.Errorf("expiry callback ran despite being delayed")
		default:
		}
		if err := lease.Renew(ctx); err == nil {
			return fmt.Errorf("real expired lease renewed with a delayed timer")
		} else {
			var fenced driver.ErrFenced
			if !errors.As(err, &fenced) {
				return err
			}
		}
		return nil
	})
	requireFenced(t, err)
	t.Log("real monotonic expiry fenced renewal and publication despite frozen application time and delayed expiry callback")
}

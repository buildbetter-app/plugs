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
	every  atomic.Bool
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
	hook := query == "COMMIT" && (c.connector.armed.CompareAndSwap(true, false) || c.connector.every.Load())
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

// A first-COMMIT-only delay misses a moving-target handoff: every flush may
// receive yet another heartbeat before its COMMIT returns. Keep the production
// TTL and renewal cadence, and delay EVERY real COMMIT beyond that cadence.
func TestSQLitePublicationLeaseBoundedHandoffWithEveryCommitSlow(t *testing.T) {
	for _, mode := range []string{"graph", "renewal"} {
		t.Run(mode, func(t *testing.T) { publicationBoundedSlowHandoff(t, mode) })
	}
}

func publicationBoundedSlowHandoff(t *testing.T, mode string) {
	t.Helper()
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "every-commit-slow.db")
	s := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL).(*activeLease)
	connector := hookPublicationCommit(t, s, path)
	connector.every.Store(true)
	// Cleanup must not apply the latency harness to the final Release.
	t.Cleanup(func() { connector.every.Store(false) })
	const commitDelay = 4 * time.Second
	start, stop := make(chan struct{}), make(chan struct{})
	done := make(chan publicationHeartbeatResult, 1)
	go func() {
		var result publicationHeartbeatResult
		defer func() { done <- result }()
		select {
		case <-start:
		case <-ctx.Done():
			result.err = ctx.Err()
			return
		}
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			// An already admitted Renew must finish, but a pending tick must not
			// start another SQL publication once the graph caller has returned.
			select {
			case <-stop:
				return
			case <-ctx.Done():
				result.err = ctx.Err()
				return
			default:
			}
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
	var cutoff, advancedDuringHandoff, shortCommit atomic.Bool
	var delayed atomic.Int32
	connector.before = func() {
		step := connector.steps.Load()
		if step == 0 {
			close(start)
		}
		lease.mu.Lock()
		before := lease.heartbeatLocked()
		if step == 1 {
			cutoff.Store(lease.publishing)
		}
		lease.mu.Unlock()
		started := time.Now()
		timer := time.NewTimer(commitDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		delayed.Add(1)
		if time.Since(started) < commitDelay {
			shortCommit.Store(true)
		}
		if step == 1 {
			lease.mu.Lock()
			advancedDuringHandoff.Store(lease.heartbeatLocked() != before)
			lease.mu.Unlock()
		}
	}
	started := time.Now()
	var publicationErr error
	if mode == "renewal" {
		publicationErr = lease.Renew(ctx)
	} else {
		publicationErr = s.immediate(ctx, lease, func(c *sql.Conn) error {
			_, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "bounded-handoff", revKey(1), []byte("committed"))
			return err
		})
	}
	elapsed := time.Since(started)
	close(stop)
	renewal := <-done
	if publicationErr != nil || renewal.err != nil || renewal.successes < 3 {
		t.Fatalf("publication=%v heartbeat=%+v elapsed=%s nativeCommits=%d", publicationErr, renewal, elapsed, connector.steps.Load())
	}
	if !cutoff.Load() || advancedDuringHandoff.Load() {
		t.Fatalf("finalization did not queue later renewals: cutoff=%v heartbeatAdvanced=%v", cutoff.Load(), advancedDuringHandoff.Load())
	}
	// Two commits publish the graph and its cutoff heartbeat; the third is
	// the Renew queued during handoff, which must finish after the gate opens.
	if connector.steps.Load() != 3 || delayed.Load() != 3 || shortCommit.Load() || elapsed < 2*commitDelay {
		t.Fatalf("unbounded or unexercised handoff: nativeCommits=%d delayed=%d short=%v elapsed=%s", connector.steps.Load(), delayed.Load(), shortCommit.Load(), elapsed)
	}
	row, err := readLease(ctx, observer.db)
	if err != nil {
		t.Fatal(err)
	}
	lease.mu.Lock()
	expires, deadline, publishing, lost := lease.expires, lease.deadline, lease.publishing, lease.lostFlag
	lease.mu.Unlock()
	if row.expires != expires || row.expires != deadline.UnixNano() || publishing || lost {
		t.Fatalf("handoff left private or lost lease: durable=%d effective=%d deadline=%s publishing=%v lost=%v", row.expires, expires, deadline, publishing, lost)
	}
	cp, err := observer.LoadCheckpoint(ctx, "bounded-handoff")
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
	t.Logf("%s: publication returned in %s using two 4s COMMITs with ttl=%s renewEvery=%s; queued Renew completed a third 4s COMMIT; lease remains live and durable", mode, elapsed, driver.LeaseTTL, driver.LeaseRenewEvery)
}

func TestSQLitePublicationLeaseQueuedHandoffRenewal(t *testing.T) {
	for _, mode := range []string{"cancellation", "expiry"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "queued-handoff.db")
			s := openStore(t, ctx, path)
			observer := openStore(t, ctx, path)
			lease := getLease(t, ctx, s, time.Second).(*activeLease)
			connector := hookPublicationCommit(t, s, path)
			connector.every.Store(true)
			t.Cleanup(func() { connector.every.Store(false) })
			var accepted, queuedErr error
			var queuedEarly, advanced bool
			var held time.Duration
			connector.after = func() {
				if connector.steps.Load() == 1 {
					// Accept a heartbeat after the graph COMMIT's snapshot so
					// finalization must enter its real lease-only transaction.
					accepted = lease.Renew(ctx)
				}
			}
			connector.before = func() {
				if connector.steps.Load() != 1 {
					return
				}
				lease.mu.Lock()
				before := lease.heartbeatLocked()
				lease.mu.Unlock()
				renewCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				queued := make(chan error, 1)
				go func() { queued <- lease.Renew(renewCtx) }()
				started := time.Now()
				delay := 100 * time.Millisecond
				if mode == "expiry" {
					delay = 2 * time.Second
				}
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case queuedErr = <-queued:
					// A successful return before the gate opens would falsely
					// acknowledge an extension excluded by the cutoff.
					queuedEarly = queuedErr == nil
					select {
					case <-timer.C:
					case <-ctx.Done():
					}
				case <-timer.C:
					if mode == "cancellation" {
						cancel()
					}
					queuedErr = <-queued
				case <-ctx.Done():
					cancel()
					queuedErr = <-queued
				}
				held = time.Since(started)
				lease.mu.Lock()
				advanced = lease.heartbeatLocked() != before
				lease.mu.Unlock()
			}
			err := s.immediate(ctx, lease, func(c *sql.Conn) error {
				_, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "queued-handoff", revKey(1), []byte("committed"))
				return err
			})
			if err != nil || accepted != nil || queuedEarly || advanced || connector.steps.Load() != 2 {
				t.Fatalf("publication=%v accepted=%v queued=%v early=%v advanced=%v nativeCommits=%d", err, accepted, queuedErr, queuedEarly, advanced, connector.steps.Load())
			}
			if mode == "cancellation" {
				if !errors.Is(queuedErr, context.Canceled) {
					t.Fatalf("queued Renew ignored cancellation: %v", queuedErr)
				}
				select {
				case <-lease.Lost():
					t.Fatal("canceling an unaccepted queued renewal lost the durable live lease")
				default:
				}
			} else {
				if held < 2*time.Second {
					t.Fatalf("did not cross the real TTL during handoff: held=%s", held)
				}
				requireFenced(t, queuedErr)
				requireFenced(t, lease.Renew(ctx))
				select {
				case <-lease.Lost():
				default:
					t.Fatal("queued renewal resurrected an expired handoff lease")
				}
				next := getLease(t, ctx, observer, driver.LeaseTTL)
				if next.Epoch() <= lease.Epoch() {
					t.Fatal("handoff expiry takeover did not advance the epoch")
				}
				requireFenced(t, lease.Renew(ctx))
			}
			cp, readErr := observer.LoadCheckpoint(ctx, "queued-handoff")
			if readErr != nil || cp.Generation != 1 || string(cp.Payload) != "committed" {
				t.Fatalf("already committed graph changed: checkpoint=%+v err=%v", cp, readErr)
			}
			lease.mu.Lock()
			registered := lease.publishing || lease.handoff
			lease.mu.Unlock()
			if registered {
				t.Fatal("completed handoff left heartbeat admission registered")
			}
			t.Logf("%s: unaccepted queued Renew returned %v without advancing the heartbeat; held=%s; graph remains committed", mode, queuedErr, held)
		})
	}
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

func TestSQLitePublicationLeaseClockCanSignalLoss(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "clock-loss.db")
	var armed atomic.Bool
	var s *Store
	var lease *activeLease
	value, err := (Driver{Clock: func() time.Time {
		if armed.CompareAndSwap(true, false) {
			// A supported clock seam can observe store state and signal Lost.
			// Neither internal mutex may be held across this callback.
			if err := s.check(ctx); err != nil {
				t.Error(err)
			}
			lease.signalLost()
		}
		return time.Now()
	}}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	s = value.(*Store)
	t.Cleanup(func() { _ = s.Close() })
	observer := openStore(t, ctx, path)
	lease = getLease(t, ctx, s, time.Second).(*activeLease)
	err = s.immediate(ctx, lease, func(c *sql.Conn) error {
		if _, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "clock-loss", revKey(1), []byte("must-rollback")); err != nil {
			return err
		}
		armed.Store(true)
		err := lease.Renew(ctx)
		requireFenced(t, err)
		return err
	})
	requireFenced(t, err)
	cp, err := observer.LoadCheckpoint(ctx, "clock-loss")
	if err != nil || cp.Generation != 0 {
		t.Fatalf("clock-signaled loss published graph: checkpoint=%+v err=%v", cp, err)
	}
}

func TestSQLitePublicationLeaseConcurrentSamplesCannotRegressDeadline(t *testing.T) {
	ctx := testContext(t)
	var armed atomic.Bool
	var samples atomic.Int32
	olderSampled, resumeOlder := make(chan struct{}), make(chan struct{})
	value, err := (Driver{Clock: func() time.Time {
		if armed.Load() && samples.Add(1) == 1 {
			close(olderSampled)
			select {
			case <-resumeOlder:
			case <-ctx.Done():
			}
		}
		// Return actual current time, including for the delayed first caller.
		return time.Now()
	}}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "concurrent-samples.db")})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, time.Second).(*activeLease)
	err = s.immediate(ctx, lease, func(*sql.Conn) error {
		armed.Store(true)
		older := make(chan error, 1)
		go func() { older <- lease.Renew(ctx) }()
		select {
		case <-olderSampled:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := lease.Renew(ctx); err != nil {
			close(resumeOlder)
			return err
		}
		lease.mu.Lock()
		newerHeartbeat, newerDeadline := lease.heartbeatLocked(), lease.deadline
		lease.mu.Unlock()
		close(resumeOlder)
		if err := <-older; err != nil {
			return err
		}
		lease.mu.Lock()
		defer lease.mu.Unlock()
		if lease.heartbeatLocked() != newerHeartbeat || !lease.deadline.Equal(newerDeadline) {
			return fmt.Errorf("older Renew overwrote the later accepted heartbeat")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	row, err := readLease(ctx, s.db)
	lease.mu.Lock()
	expires, publishing := lease.expires, lease.publishing
	lease.mu.Unlock()
	if err != nil || row.expires != expires || publishing {
		t.Fatalf("out-of-order renewal handoff: durable=%d effective=%d publishing=%v err=%v", row.expires, expires, publishing, err)
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

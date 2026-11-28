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

// These hooks surround REAL native transactions. They expose heartbeats during
// the SQL/fsync interval and after the durable commit but before unregistration.
// The error hooks model a native COMMIT failure or an acknowledgement lost
// after real native COMMIT, independently of the calling request context.
type publicationCommitConnector struct {
	driver            sqldriver.Driver
	dsn               string
	before            func()
	after             func()
	beforeRollback    func(context.Context)
	afterRollback     func(error)
	commitErrorBefore error
	commitErrorAfter  error
	rollbackFailure   error
	armed             atomic.Bool
	every             atomic.Bool
	steps             atomic.Int32
	commitCalls       atomic.Int32
	rollbacks         atomic.Int32
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

func (c *publicationCommitConn) BeginTx(ctx context.Context, options sqldriver.TxOptions) (sqldriver.Tx, error) {
	return c.Conn.(sqldriver.ConnBeginTx).BeginTx(ctx, options)
}

func (c *publicationCommitConn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	return c.Conn.(sqldriver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *publicationCommitConn) ExecContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	if query == "ROLLBACK" {
		c.connector.rollbacks.Add(1)
		if c.connector.beforeRollback != nil {
			c.connector.beforeRollback(ctx)
		}
		if c.connector.rollbackFailure != nil {
			return nil, c.connector.rollbackFailure
		}
	}
	if query == "COMMIT" {
		c.connector.commitCalls.Add(1)
	}
	hook := query == "COMMIT" && (c.connector.armed.CompareAndSwap(true, false) || c.connector.every.Load())
	if hook {
		if c.connector.before != nil {
			c.connector.before()
		}
		if c.connector.commitErrorBefore != nil {
			return nil, c.connector.commitErrorBefore
		}
	}
	result, err := c.Conn.(sqldriver.ExecerContext).ExecContext(ctx, query, args)
	if query == "ROLLBACK" && c.connector.afterRollback != nil {
		c.connector.afterRollback(err)
	}
	if query == "COMMIT" && err == nil {
		c.connector.steps.Add(1)
		if hook && c.connector.after != nil {
			c.connector.after()
		}
		if hook && c.connector.commitErrorAfter != nil {
			return nil, c.connector.commitErrorAfter
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

// The final fence is the SQLite publication authorization boundary. A native
// COMMIT cannot be preempted after that check, but the publication flock keeps
// takeover excluded until COMMIT returns. This regression pauses exactly in
// that interval, then verifies the confirmed publication, lost old holder and
// fenced subsequent use.
func TestSQLitePublicationLeaseFinalFencePauseCommitsWhileGateHeld(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "final-fence-pause.db")
	s := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, time.Second).(*activeLease)

	// Disable the expiry callback so finishPublication's real-time check is the
	// deterministic point that marks this holder Lost after native COMMIT.
	lease.mu.Lock()
	deadline := lease.deadline
	if lease.timer == nil || !lease.timer.Stop() {
		lease.mu.Unlock()
		t.Fatal("lease expiry callback was not available to stop")
	}
	lease.mu.Unlock()

	connector := hookPublicationCommit(t, s, path)
	paused := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	connector.before = func() {
		close(paused)
		select {
		case <-release:
		case <-ctx.Done():
		}
	}

	const key = "final-fence-pause"
	publicationDone := make(chan error, 1)
	go func() {
		publicationDone <- s.immediate(ctx, lease, func(c *sql.Conn) error {
			_, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, key, revKey(1), []byte("committed"))
			return err
		})
	}()
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// The contender starts while the hook is still between the final fence and
	// native COMMIT. It must time out on the publication flock, never acquire
	// the expired row, and never be allowed to observe a free lease in this
	// pause. Keep the hook blocked until its context has returned.
	contenderCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	contenderDone := make(chan struct {
		lease driver.ActiveLease
		err   error
	}, 1)
	go func() {
		contender, err := observer.Lease(contenderCtx, "contender", driver.LeaseTTL)
		contenderDone <- struct {
			lease driver.ActiveLease
			err   error
		}{lease: contender, err: err}
	}()

	// Ensure the nominal deadline passes while the publication gate remains
	// held; stopping the timer above makes this check independent of callback
	// scheduling.
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		timer := time.NewTimer(remaining)
		select {
		case result := <-contenderDone:
			timer.Stop()
			t.Fatalf("contender returned before the holder deadline: lease=%v err=%v", result.lease, result.err)
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		}
	}
	select {
	case result := <-contenderDone:
		t.Fatalf("contender escaped the publication pause: lease=%v err=%v", result.lease, result.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	default:
	}
	select {
	case result := <-contenderDone:
		if result.lease != nil || !errors.Is(result.err, context.DeadlineExceeded) {
			t.Fatalf("contender was not blocked by the publication flock: lease=%v err=%v", result.lease, result.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-lease.Lost():
		t.Fatal("holder was marked Lost before native COMMIT returned")
	default:
	}

	close(release)
	released = true
	select {
	case err := <-publicationDone:
		if err != nil {
			t.Fatalf("publication after final-fence pause: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if connector.steps.Load() != 1 || connector.rollbacks.Load() != 0 {
		t.Fatalf("pause performed unexpected native transaction work: commits=%d rollbacks=%d", connector.steps.Load(), connector.rollbacks.Load())
	}
	checkpoint, err := observer.LoadCheckpoint(ctx, key)
	if err != nil || checkpoint.Generation != 1 || string(checkpoint.Payload) != "committed" {
		t.Fatalf("confirmed publication was not durable: checkpoint=%+v err=%v", checkpoint, err)
	}
	select {
	case <-lease.Lost():
	default:
		t.Fatal("expired holder was not marked Lost after native COMMIT")
	}
	requireFenced(t, lease.Renew(ctx))
	if _, err := s.SaveCheckpoint(ctx, lease, key, checkpoint.Generation, []byte("stale")); err == nil {
		t.Fatal("expired holder published again after the confirmed commit")
	} else {
		requireFenced(t, err)
	}

	next := getLease(t, ctx, observer, driver.LeaseTTL)
	if next.Epoch() <= lease.Epoch() {
		t.Fatalf("successor epoch=%d did not advance past expired holder epoch=%d", next.Epoch(), lease.Epoch())
	}
	successor, err := observer.SaveCheckpoint(ctx, next, key, checkpoint.Generation, []byte("successor"))
	if err != nil || successor.Generation != checkpoint.Generation+1 || string(successor.Payload) != "successor" {
		t.Fatalf("successor publication=%+v err=%v", successor, err)
	}
	t.Logf("final fence authorized one native COMMIT after a %s nominal TTL deadline; contender timed out during the held flock, old epoch %d became Lost, and successor epoch %d published", time.Since(deadline), lease.Epoch(), next.Epoch())
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
	var renewal publicationHeartbeatResult
	connector.after = func() {
		lateRenew = lease.Renew(ctx)
		close(stop)
		renewal = <-done
	}
	var err error
	if mode == "renewal" {
		err = lease.Renew(ctx)
	} else {
		err = s.immediate(ctx, lease, func(c *sql.Conn) error {
			_, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "commit-heartbeats", revKey(1), []byte("committed"))
			return err
		})
	}
	if err != nil || lateRenew != nil || renewal.err != nil || renewal.successes == 0 || held < 2*time.Second {
		t.Fatalf("publication=%v lateRenew=%v heartbeat=%+v held=%s", err, lateRenew, renewal, held)
	}
	if connector.steps.Load() != 1 || connector.rollbacks.Load() != 0 {
		t.Fatalf("publication performed post-COMMIT transaction work: nativeCommits=%d rollbacks=%d", connector.steps.Load(), connector.rollbacks.Load())
	}
	assertDurableHeartbeat(t, ctx, observer, lease)
	// The graph's SQL snapshot is necessarily older than the heartbeat accepted
	// AFTER its real COMMIT. The independent journal, not a flush, protects it.
	raw, err := readLease(ctx, observer.db)
	if err != nil || raw.expires >= lease.expires {
		t.Fatalf("did not exercise a heartbeat newer than SQLite's committed row: raw=%+v err=%v", raw, err)
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
		t.Fatalf("publication exposed expired durable lease: next=%v err=%v", next, err)
	}
	t.Logf("held real COMMIT boundary %s with %d timely heartbeats; post-COMMIT heartbeat independently durable after ONE native COMMIT", held, renewal.successes)
}

func assertDurableHeartbeat(t *testing.T, ctx context.Context, observer *Store, lease *activeLease) {
	t.Helper()
	row, err := observer.readLease(ctx, observer.db)
	if err != nil {
		t.Fatal(err)
	}
	lease.mu.Lock()
	expires, deadline, publishing, lost := lease.expires, lease.deadline, lease.publishing, lease.lossReason != ""
	lease.mu.Unlock()
	if row.epoch != lease.epoch || row.token != lease.token || row.expires != expires || row.expires != deadline.UnixNano() || publishing || lost {
		t.Fatalf("publication left private or lost heartbeat: durable=%+v effective=%d deadline=%s publishing=%v lost=%v", row, expires, deadline, publishing, lost)
	}
}

// Keep the unchanged production TTL/cadence and delay EVERY native COMMIT.
// A moving-target handoff cannot converge; the journal needs exactly one COMMIT.
func TestSQLitePublicationLeaseBoundedHandoffWithEveryCommitSlow(t *testing.T) {
	for _, mode := range []string{"graph", "renewal"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "every-commit-slow.db")
			s := openStore(t, ctx, path)
			observer := openStore(t, ctx, path)
			lease := getLease(t, ctx, s, driver.LeaseTTL).(*activeLease)
			connector := hookPublicationCommit(t, s, path)
			connector.every.Store(true)
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
			var held time.Duration
			connector.before = func() {
				close(start)
				started := time.Now()
				timer := time.NewTimer(commitDelay)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
				}
				held = time.Since(started)
			}
			var renewal publicationHeartbeatResult
			connector.after = func() { close(stop); renewal = <-done }
			started := time.Now()
			var err error
			if mode == "renewal" {
				err = lease.Renew(ctx)
			} else {
				err = s.immediate(ctx, lease, func(c *sql.Conn) error {
					_, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "bounded-handoff", revKey(1), []byte("committed"))
					return err
				})
			}
			elapsed := time.Since(started)
			if err != nil || renewal.err != nil || renewal.successes < 2 || held < commitDelay || connector.steps.Load() != 1 {
				t.Fatalf("publication=%v heartbeat=%+v held=%s elapsed=%s nativeCommits=%d", err, renewal, held, elapsed, connector.steps.Load())
			}
			assertDurableHeartbeat(t, ctx, observer, lease)
			cp, err := observer.LoadCheckpoint(ctx, "bounded-handoff")
			wantGeneration := uint64(1)
			if mode == "renewal" {
				wantGeneration = 0
			}
			if err != nil || cp.Generation != wantGeneration || wantGeneration == 1 && string(cp.Payload) != "committed" {
				t.Fatalf("native publication=%+v err=%v want generation=%d", cp, err, wantGeneration)
			}
			t.Logf("%s: returned after ONE slow COMMIT in %s with ttl=%s renewEvery=%s; all %d heartbeats independently durable", mode, elapsed, driver.LeaseTTL, driver.LeaseRenewEvery, renewal.successes)
		})
	}
}

// This replaces the former cutoff test, which incorrectly expected a timely
// queued owner to be fenced. Renewals must remain serviceable even AFTER the
// primary native COMMIT throughout a final persistence interval longer than TTL.
func TestSQLitePublicationLeasePostCommitRenewalsStayLive(t *testing.T) {
	for _, mode := range []string{"live", "canceled-renewal", "genuine-pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "post-commit.db")
			s := openStore(t, ctx, path)
			observer := openStore(t, ctx, path)
			lease := getLease(t, ctx, s, time.Second).(*activeLease)
			connector := hookPublicationCommit(t, s, path)
			var accepted, canceledRenew error
			var held time.Duration
			var renewal publicationHeartbeatResult
			connector.after = func() {
				accepted = lease.Renew(ctx)
				if mode == "canceled-renewal" {
					lease.mu.Lock()
					before := lease.heartbeatLocked()
					lease.mu.Unlock()
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					canceledRenew = lease.Renew(canceled)
					lease.mu.Lock()
					unchanged := lease.heartbeatLocked() == before
					lease.mu.Unlock()
					if !unchanged || !errors.Is(canceledRenew, context.Canceled) {
						t.Errorf("canceled renewal changed heartbeat: err=%v unchanged=%v", canceledRenew, unchanged)
					}
				}
				start, stop := make(chan struct{}), make(chan struct{})
				var done <-chan publicationHeartbeatResult
				if mode != "genuine-pause" {
					done = publicationHeartbeat(ctx, lease, start, stop)
					close(start)
				}
				started := time.Now()
				timer := time.NewTimer(2 * time.Second)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
				}
				held = time.Since(started)
				if done != nil {
					close(stop)
					renewal = <-done
				}
			}
			err := s.immediate(ctx, lease, func(c *sql.Conn) error {
				_, err := c.ExecContext(ctx, `INSERT INTO checkpoints VALUES(?,?,?)`, "post-commit", revKey(1), []byte("committed"))
				return err
			})
			if err != nil || accepted != nil || held < 2*time.Second || connector.steps.Load() != 1 {
				t.Fatalf("publication=%v accepted=%v held=%s nativeCommits=%d", err, accepted, held, connector.steps.Load())
			}
			cp, readErr := observer.LoadCheckpoint(ctx, "post-commit")
			if readErr != nil || cp.Generation != 1 || string(cp.Payload) != "committed" {
				t.Fatalf("already committed graph changed: checkpoint=%+v err=%v", cp, readErr)
			}
			if mode != "genuine-pause" {
				if renewal.err != nil || renewal.successes < 10 {
					t.Fatalf("timely post-COMMIT renewals blocked: %+v", renewal)
				}
				assertDurableHeartbeat(t, ctx, observer, lease)
			} else {
				requireFenced(t, lease.Renew(ctx))
				select {
				case <-lease.Lost():
				default:
					t.Fatal("genuinely paused holder revived")
				}
				next := getLease(t, ctx, observer, driver.LeaseTTL)
				if next.Epoch() <= lease.Epoch() {
					t.Fatal("takeover did not advance epoch")
				}
				requireFenced(t, lease.Renew(ctx))
			}
			t.Logf("%s: final persistence held %s after real COMMIT; %d successful timely renewals; publication success preserved", mode, held, renewal.successes)
		})
	}
}

func TestSQLitePublicationLeaseCheckpointOutcomeAfterHandoffExpiry(t *testing.T) {
	for _, method := range []string{"SaveCheckpoint", "SaveCheckpointIfRecord"} {
		for _, mode := range []string{"expiry", "cancellation"} {
			t.Run(method+"/"+mode, func(t *testing.T) {
				ctx := testContext(t)
				path := filepath.Join(t.TempDir(), "checkpoint-outcome.db")
				s := openStore(t, ctx, path)
				observer := openStore(t, ctx, path)
				setup := getLease(t, ctx, s, driver.LeaseTTL)
				ref := testRef("pl-checkpoint-handoff")
				entry := seedRecord(t, ctx, s, setup, ref.ID, `{"value":"guard"}`)
				meta := getMeta(t, ctx, s)
				const key = "checkpoint-handoff"
				initial, err := s.SaveCheckpoint(ctx, setup, key, 0, []byte("before"))
				if err != nil || initial.Generation != 1 {
					t.Fatalf("initial checkpoint=%+v err=%v", initial, err)
				}
				if err := setup.Release(ctx); err != nil {
					t.Fatal(err)
				}
				const ttl = time.Second
				lease := getLease(t, ctx, s, ttl).(*activeLease)
				call, cancel := context.WithCancel(ctx)
				defer cancel()
				connector := hookPublicationCommit(t, s, path)
				var accepted, duringErr error
				var during driver.Checkpoint
				var held time.Duration
				connector.after = func() {
					accepted = lease.Renew(ctx)
					during, duringErr = observer.LoadCheckpoint(ctx, key)
					if mode == "cancellation" {
						cancel()
						return
					}
					started := time.Now()
					timer := time.NewTimer(2 * ttl)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-ctx.Done():
					}
					held = time.Since(started)
				}
				payload := []byte("confirmed publication")
				save := func(ctx context.Context, store *Store, value driver.ActiveLease, expected uint64, body []byte) (driver.Checkpoint, error) {
					if method == "SaveCheckpointIfRecord" {
						return store.SaveCheckpointIfRecord(ctx, value, key, expected, body, ref, entry.Revision, meta.Incarnation)
					}
					return store.SaveCheckpoint(ctx, value, key, expected, body)
				}
				saved, saveErr := save(call, s, lease, initial.Generation, payload)
				durable, readErr := observer.LoadCheckpoint(ctx, key)
				if accepted != nil || connector.steps.Load() != 1 || mode == "expiry" && held < 2*ttl {
					t.Fatalf("post-COMMIT interval unexercised: accepted=%v held=%s nativeCommits=%d", accepted, held, connector.steps.Load())
				}
				if duringErr != nil || during.Generation != initial.Generation+1 || string(during.Payload) != string(payload) || readErr != nil || durable.Generation != during.Generation || string(durable.Payload) != string(payload) {
					t.Fatalf("independent durable checkpoint: during=%+v err=%v after=%+v err=%v", during, duringErr, durable, readErr)
				}
				if saveErr != nil || saved.Generation != durable.Generation || string(saved.Payload) != string(durable.Payload) {
					t.Fatalf("confirmed checkpoint reported a rejection: saved=%+v err=%v durable=%+v", saved, saveErr, durable)
				}
				if mode == "expiry" {
					requireFenced(t, lease.Renew(ctx))
					next := getLease(t, ctx, observer, driver.LeaseTTL)
					if next.Epoch() <= lease.Epoch() {
						t.Fatal("expiry takeover did not advance lease epoch")
					}
					_, staleErr := save(ctx, s, lease, durable.Generation, []byte("stale"))
					requireFenced(t, staleErr)
					unchanged, err := observer.LoadCheckpoint(ctx, key)
					if err != nil || unchanged.Generation != durable.Generation || string(unchanged.Payload) != string(durable.Payload) {
						t.Fatalf("stale holder changed checkpoint: %+v err=%v", unchanged, err)
					}
					fresh, err := save(ctx, observer, next, durable.Generation, []byte("new holder"))
					if err != nil || fresh.Generation != durable.Generation+1 {
						t.Fatalf("successor checkpoint=%+v err=%v", fresh, err)
					}
				} else {
					assertDurableHeartbeat(t, ctx, observer, lease)
				}
				t.Logf("%s/%s: generation %d independently durable before post-COMMIT interruption, returned confirmed success with no follow-up", method, mode, durable.Generation)
			})
		}
	}
}

func TestSQLitePublicationLeaseConfirmedAbortKeepsLease(t *testing.T) {
	for _, mode := range []string{"callback-error", "cancellation", "native-commit-rejected", "after-durable-commit"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testContext(t)
			path := filepath.Join(t.TempDir(), "abort-heartbeat.db")
			s := openStore(t, ctx, path)
			observer := openStore(t, ctx, path)
			if _, err := s.db.ExecContext(ctx, `CREATE TABLE proof_parent(id INTEGER PRIMARY KEY);
CREATE TABLE proof_child(id INTEGER REFERENCES proof_parent(id) DEFERRABLE INITIALLY DEFERRED)`); err != nil {
				t.Fatal(err)
			}
			lease := getLease(t, ctx, s, driver.LeaseTTL).(*activeLease)
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			failure := errors.New("discard graph publication")
			want := error(failure)
			var heartbeatErr error
			if mode == "after-durable-commit" {
				connector := hookPublicationCommit(t, s, path)
				connector.after = func() { heartbeatErr = lease.Renew(ctx); cancel() }
				want = nil
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
				if mode == "native-commit-rejected" {
					want = nil
					_, err := c.ExecContext(call, `INSERT INTO proof_child VALUES(1)`)
					return err
				}
				return failure
			})
			if mode == "native-commit-rejected" {
				var native commitError
				if !errors.As(err, &native) {
					t.Fatalf("native deferred constraint did not fail at COMMIT: %v", err)
				}
			} else if heartbeatErr != nil || !errors.Is(err, want) {
				t.Fatalf("heartbeat=%v publication=%v want=%v", heartbeatErr, err, want)
			}
			cp, readErr := observer.LoadCheckpoint(ctx, "abort")
			wantGeneration := uint64(0)
			if mode == "after-durable-commit" {
				wantGeneration = 1
				assertDurableHeartbeat(t, ctx, observer, lease)
			} else {
				assertDurableHeartbeat(t, ctx, observer, lease)
				short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
				if next, err := observer.Lease(short, "contender", driver.LeaseTTL); next != nil || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("second handle took over a live journal heartbeat: %v %v", next, err)
				}
				if _, err := s.SaveCheckpoint(ctx, lease, "next-publication", 0, []byte("same epoch")); err != nil {
					t.Fatalf("next publication with the original lease: %v", err)
				}
			}
			if readErr != nil || cp.Generation != wantGeneration {
				t.Fatalf("durable graph=%+v err=%v want generation=%d", cp, readErr, wantGeneration)
			}
			lease.mu.Lock()
			publishing := lease.publishing
			lease.mu.Unlock()
			if publishing {
				t.Fatal("completed publication left heartbeat admission registered")
			}
			t.Logf("%s: graph generation=%d; original lease epoch=%d", mode, cp.Generation, lease.Epoch())
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
			lease.signalLost("test fence")
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
	row, err := s.readLease(ctx, s.db)
	lease.mu.Lock()
	expires, publishing := lease.expires, lease.publishing
	lease.mu.Unlock()
	if err != nil || row.expires != expires || publishing {
		t.Fatalf("out-of-order renewal handoff: durable=%d effective=%d publishing=%v err=%v", row.expires, expires, publishing, err)
	}
}

func TestSQLitePublicationLeaseRepeatedWallStampRollbackKeepsLease(t *testing.T) {
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
	if err := lease.Renew(ctx); err != nil {
		t.Fatalf("confirmed rollback with identical wall stamp lost lease: %v", err)
	}
}

func TestSQLitePublicationLeaseUnconfirmedRollbackFencesAndDiscardsConnection(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "unconfirmed-rollback.db")
	s := openStore(t, ctx, path)
	observer := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL).(*activeLease)
	connector := hookPublicationCommit(t, s, path)
	connector.rollbackFailure = errors.New("injected private rollback detail")

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	call, cancel := context.WithCancel(driver.WithPublicationMethod(ctx, "bead.update"))
	defer cancel()
	err := s.immediate(call, lease, func(c *sql.Conn) error {
		if _, err := c.ExecContext(call, `INSERT INTO checkpoints VALUES(?,?,?)`, "private-key", revKey(1), []byte("private-payload")); err != nil {
			return err
		}
		if err := lease.Renew(ctx); err != nil {
			return err
		}
		cancel()
		return call.Err()
	})
	if !errors.Is(err, context.Canceled) || lease.LossReason() != "native publication rollback was unconfirmed" || connector.rollbacks.Load() != 1 {
		t.Fatalf("abort=%v reason=%q rollback attempts=%d", err, lease.LossReason(), connector.rollbacks.Load())
	}
	requireUncertainLeaseLost(t, ctx, lease)
	if cp, err := observer.LoadCheckpoint(ctx, "private-key"); err != nil || cp.Generation != 0 {
		t.Fatalf("unconfirmed rollback published staged data: %+v %v", cp, err)
	}
	if err := observer.immediate(ctx, nil, func(*sql.Conn) error { return nil }); err != nil {
		t.Fatalf("discarded connection retained the native writer: %v", err)
	}
	log := output.String()
	for _, field := range []string{"sqlite publication aborted", "method=bead.update", "cause=context_canceled", "rollback_confirmed=false", "heartbeat_advanced=true"} {
		if !strings.Contains(log, field) {
			t.Fatalf("missing %q in diagnostic: %s", field, log)
		}
	}
	if strings.Contains(log, "private") {
		t.Fatalf("publication diagnostic exposed request or error content: %s", log)
	}
	t.Log(strings.TrimSpace(log))
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

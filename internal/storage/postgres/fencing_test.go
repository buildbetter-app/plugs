package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type waitingLeaseContextKey struct{}
type waitingLeaseQueryKey struct{}

// Observe a real NOWAIT conflict before stopping the next attempt inside pgx,
// after database/sql has checked the context but before pgx sends the query.
// Cancellation here is safe to retry at the transport layer, which stdlib
// reports as driver.ErrBadConn rather than the caller's context error.
type waitingLeaseCancellationTracer struct {
	conflicted bool
	waiting    chan struct{}
	queryError chan error
}

func (q *waitingLeaseCancellationTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(waitingLeaseContextKey{}) == nil || !strings.HasPrefix(data.SQL, "SELECT epoch::text,owner='' OR expires<=clock_timestamp()") {
		return ctx
	}
	if q.conflicted {
		close(q.waiting)
		<-ctx.Done()
	}
	return context.WithValue(ctx, waitingLeaseQueryKey{}, true)
}

func (q *waitingLeaseCancellationTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(waitingLeaseQueryKey{}) == nil {
		return
	}
	if sqlState(data.Err) == "55P03" {
		q.conflicted = true
	} else {
		q.queryError <- data.Err
	}
}

func TestWaitingLeaseCancellationPreservesContextAndConnection(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name, want := "cancel", context.Canceled
		if deadline {
			name, want = "deadline", context.DeadlineExceeded
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			dsn := freshDSN(t)
			holderStore := openTest(t, dsn)
			active, err := holderStore.Lease(ctx, "cancellation-holder", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer active.Release(context.Background())
			publication, err := holderStore.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer publication.Rollback()
			if _, err := holderStore.checkLease(ctx, publication, active, true); err != nil {
				t.Fatal(err)
			}

			tracer := &waitingLeaseCancellationTracer{waiting: make(chan struct{}), queryError: make(chan error, 1)}
			config, err := pgx.ParseConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			config.Tracer = tracer
			registered := stdlib.RegisterConnConfig(config)
			defer stdlib.UnregisterConnConfig(registered)
			standby := openTest(t, registered)
			// Two reserved handles leave exactly one general-purpose connection.
			// A leaked contender transaction would prevent all later pool work.
			standby.db.SetMaxOpenConns(3)
			standby.db.SetMaxIdleConns(1)
			contenderCtx := context.WithValue(ctx, waitingLeaseContextKey{}, true)
			var stop context.CancelFunc
			if deadline {
				contenderCtx, stop = context.WithTimeout(contenderCtx, 2*time.Second)
			} else {
				contenderCtx, stop = context.WithCancel(contenderCtx)
			}
			defer stop()
			contended := make(chan error, 1)
			go func() {
				next, err := standby.Lease(contenderCtx, "cancelled-contender", driver.LeaseTTL)
				if err == nil {
					_ = next.Release(context.Background())
				}
				contended <- err
			}()
			select {
			case <-tracer.waiting:
			case err := <-contended:
				t.Fatalf("contender returned before the synchronized cancellation boundary: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			renewCtx, done := context.WithTimeout(ctx, 500*time.Millisecond)
			err = active.Renew(renewCtx)
			done()
			if err != nil {
				t.Fatalf("renewal blocked by waiting contender: %v", err)
			}
			if !deadline {
				stop()
			}
			select {
			case err := <-contended:
				if !errors.Is(err, want) {
					t.Fatalf("waiting contender cancellation: got %v, want %v", err, want)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case err := <-tracer.queryError:
				if !errors.Is(err, want) {
					t.Fatalf("pgx query did not observe synchronized cancellation: %v", err)
				}
				t.Logf("real NOWAIT conflict followed by pgx cancellation: %v", err)
			default:
				t.Fatal("contender did not execute the cancelled pgx query")
			}
			if _, err := standby.Meta(ctx); err != nil {
				t.Fatalf("pool unusable after waiting contender cancellation: %v", err)
			}
			if err := publication.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := active.Release(ctx); err != nil {
				t.Fatal(err)
			}
			next, err := standby.Lease(ctx, "after-cancellation", driver.LeaseTTL)
			if err != nil {
				t.Fatalf("lease acquisition after cancellation: %v", err)
			}
			defer next.Release(context.Background())
			if next.Epoch() <= active.Epoch() {
				t.Fatal("subsequent lease did not advance its epoch")
			}
			if err := next.Renew(ctx); err != nil {
				t.Fatalf("subsequent holder renewal: %v", err)
			}
		})
	}
}

func TestRenewRemainsIndependentOfBlockedPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	dsn := freshDSN(t)
	s, standby := openTest(t, dsn), openTest(t, dsn)
	if _, err := s.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.renewable_publication() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(330078); RETURN NEW; END$$; CREATE TRIGGER renewable_publication BEFORE INSERT ON plug_postgres.checkpoints FOR EACH ROW EXECUTE FUNCTION plug_postgres.renewable_publication()`); err != nil {
		t.Fatal(err)
	}
	gate, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	if _, err = gate.ExecContext(ctx, `SELECT pg_advisory_lock(330078)`); err != nil {
		t.Fatal(err)
	}
	gateHeld := true
	active, err := s.Lease(ctx, "renewable-publisher", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	l := active.(*activeLease)
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		if gateHeld {
			_, _ = gate.ExecContext(cleanup, `SELECT pg_advisory_unlock(330078)`)
		}
		_ = l.Release(cleanup)
	}()
	l.mu.Lock()
	originalExpiry := l.timerDeadline
	l.mu.Unlock()
	published := make(chan error, 1)
	go func() {
		_, err := s.SaveCheckpoint(ctx, l, "renewable", 0, []byte("renewed holder"))
		published <- err
	}()
	for {
		var waiting bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objid=330078 AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	// A normal standby contender must not queue a stronger row lock ahead of
	// the current holder's heartbeat while its publication is still gated.
	contenderCtx, stopContender := context.WithCancel(ctx)
	defer stopContender()
	contended := make(chan error, 1)
	go func() {
		next, err := standby.Lease(contenderCtx, "waiting-contender", driver.LeaseTTL)
		if err == nil {
			_ = next.Release(context.Background())
		}
		contended <- err
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for i := 0; i < 4; i++ {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
		renewCtx, done := context.WithTimeout(ctx, 500*time.Millisecond)
		started := time.Now()
		err := l.Renew(renewCtx)
		done()
		if err != nil {
			t.Fatalf("heartbeat %d queued behind the holder or its contender: %v", i, err)
		}
		t.Logf("blocked publisher heartbeat=%d elapsed=%s", i, time.Since(started))
		select {
		case err := <-published:
			t.Fatalf("publication passed its SQL gate early: %v", err)
		case err := <-contended:
			t.Fatalf("standby displaced a renewed holder: %v", err)
		default:
		}
	}
	if time.Now().Before(originalExpiry) || l.ended() {
		t.Fatal("publication was not held beyond its original TTL with live renewal")
	}
	stopContender()
	select {
	case err := <-contended:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting contender cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := gate.ExecContext(ctx, `SELECT pg_advisory_unlock(330078)`); err != nil {
		t.Fatal(err)
	}
	gateHeld = false
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("renewed publication failed its final fence: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if cp, err := s.LoadCheckpoint(ctx, "renewable"); err != nil || cp.Generation != 1 || string(cp.Payload) != "renewed holder" {
		t.Fatalf("renewed publication missing: %+v %v", cp, err)
	}
}

// Pause only after the real lease-row query has completed. This reproduces
// renewal between the old row read and its separate clock query without
// replacing SQL rows or the holder's renewal path.
type leaseCheckBoundary struct {
	querier
	afterRow func()
}

func (q leaseCheckBoundary) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	row := q.querier.QueryRowContext(ctx, query, args...)
	if strings.HasPrefix(query, "SELECT epoch::text,owner,") {
		q.afterRow()
	}
	return row
}

func TestLeaseCheckDoesNotFenceRenewedExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	active, err := s.Lease(ctx, "expiry-boundary", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	l := active.(*activeLease)
	defer l.Release(ctx)
	l.mu.Lock()
	oldExpiry := l.timerDeadline
	l.mu.Unlock()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Second):
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	boundary := leaseCheckBoundary{querier: tx, afterRow: func() {
		renewCtx, done := context.WithTimeout(ctx, 500*time.Millisecond)
		defer done()
		if err := l.Renew(renewCtx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Until(oldExpiry) + 25*time.Millisecond):
		}
	}}
	if _, err := s.checkLease(ctx, boundary, l, true); err != nil {
		t.Fatalf("successful concurrent renewal was checked against stale expiry: %v", err)
	}
	if l.ended() {
		t.Fatal("lease check closed a successfully renewed holder")
	}
}

func TestIndependentTakeoverDuringEveryPublication(t *testing.T) {
	for _, operation := range []string{"graph", "noop", "checkpoint", "finalizer"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			dsn := freshDSN(t)
			oldStore, newStore := openTest(t, dsn), openTest(t, dsn)
			old := testLease(t, oldStore)
			var publish func() error
			table, event := "changesets", "INSERT"
			switch operation {
			case "graph":
				ref := testRef("pl-racing")
				tx, h := beginTest(t, oldStore, old, testLock(ref))
				h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}
				if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{}`)}); err != nil {
					t.Fatal(err)
				}
				publish = func() error { _, err := tx.Commit(ctx, h); return err }
			case "noop":
				table = "receipts"
				tx, h := beginTest(t, oldStore, old)
				h.Method, h.IdempotencyKey, h.RequestHash = "test.noop", "racing", "hash"
				publish = func() error { _, err := tx.(driver.NoopCommitter).CommitNoop(ctx, h); return err }
			case "checkpoint":
				table = "checkpoints"
				publish = func() error {
					_, err := oldStore.SaveCheckpoint(ctx, old, "racing", 0, []byte("unpublished"))
					return err
				}
			case "finalizer":
				table, event = "receipts", "UPDATE"
				tx, h := beginTest(t, oldStore, old)
				h.Method, h.IdempotencyKey, h.RequestHash = "test.finalize", "racing", "hash"
				h.ResponsePending = true
				entry, err := tx.Commit(ctx, h)
				if err != nil {
					t.Fatal(err)
				}
				publish = func() error {
					return oldStore.FinalizeResponse(ctx, old, "system:postgres-test", "", h.Method, h.IdempotencyKey, entry.Revision, []model.Warning{{Code: "post_subscriber_failed", Message: "must roll back"}})
				}
			}
			// This test-owned SQL gate pauses after the driver's lease-row lock and
			// before its final commit, exercising the actual publication interval.
			_, err := oldStore.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.block_publication() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(246802468); RETURN NEW; END$$; CREATE TRIGGER block_publication BEFORE `+event+` ON plug_postgres.`+table+` FOR EACH ROW EXECUTE FUNCTION plug_postgres.block_publication()`)
			if err != nil {
				t.Fatal(err)
			}
			gate, err := newStore.db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer gate.Close()
			if _, err = gate.ExecContext(ctx, `SELECT pg_advisory_lock(246802468)`); err != nil {
				t.Fatal(err)
			}
			defer gate.ExecContext(context.Background(), `SELECT pg_advisory_unlock(246802468)`)
			completed := make(chan error, 1)
			go func() { completed <- publish() }()
			for {
				var waiting bool
				err = newStore.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objid=246802468 AND NOT granted)`).Scan(&waiting)
				if err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case err := <-completed:
					t.Fatalf("publication ended before SQL gate: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			released := make(chan error, 1)
			go func() { released <- old.Release(ctx) }()
			select {
			case <-old.Lost():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			type acquired struct {
				lease driver.ActiveLease
				err   error
			}
			takeover := make(chan acquired, 1)
			go func() {
				lease, err := newStore.Lease(ctx, "new-independent-pool", driver.LeaseTTL)
				takeover <- acquired{lease, err}
			}()
			select {
			case value := <-takeover:
				t.Fatalf("takeover bypassed publisher's epoch lock: %+v", value)
			case <-time.After(20 * time.Millisecond):
			}
			if _, err = gate.ExecContext(ctx, `SELECT pg_advisory_unlock(246802468)`); err != nil {
				t.Fatal(err)
			}
			var fenced driver.ErrFenced
			if err = <-completed; !errors.As(err, &fenced) {
				t.Fatalf("old publication=%v", err)
			}
			if err = <-released; err != nil {
				t.Fatal(err)
			}
			next := <-takeover
			if next.err != nil {
				t.Fatal(next.err)
			}
			defer next.lease.Release(context.Background())
			if next.lease.Epoch() <= old.Epoch() {
				t.Fatal("takeover reused epoch")
			}
			if _, err = oldStore.Write(ctx, old); !errors.As(err, &fenced) {
				t.Fatalf("old holder still writes: %v", err)
			}
			meta, err := newStore.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expected := "0"
			if operation == "finalizer" {
				expected = "1"
			}
			if meta.Head.String() != expected {
				t.Fatalf("stale publication changed head: %+v", meta)
			}
			if operation == "checkpoint" {
				cp, err := newStore.LoadCheckpoint(ctx, "racing")
				if err != nil || cp.Generation != 0 {
					t.Fatalf("stale checkpoint published: %+v %v", cp, err)
				}
			}
			if operation == "noop" {
				r := testRead(t, newStore, nil)
				value, err := r.IdempotencyByKey(ctx, "system:postgres-test", "", "test.noop", "racing")
				_ = r.Close()
				if err != nil || value != nil {
					t.Fatalf("stale no-op published: %+v %v", value, err)
				}
			}
			if operation == "finalizer" {
				r := testRead(t, newStore, nil)
				value, err := r.IdempotencyByKey(ctx, "system:postgres-test", "", "test.finalize", "racing")
				_ = r.Close()
				if err != nil || value == nil || !value.ResponsePending || len(value.Commit.Warnings) != 0 {
					t.Fatalf("stale warnings published: %+v %v", value, err)
				}
			}
			// A fresh holder immediately gets the previously held record/head locks.
			if _, err = newStore.db.ExecContext(ctx, `DROP TRIGGER block_publication ON plug_postgres.`+table+`; DROP FUNCTION plug_postgres.block_publication()`); err != nil {
				t.Fatal(err)
			}
			seedTest(t, newStore, next.lease, "pl-after-takeover", `{}`)
			t.Log("separate SQL sessions fence the old publisher and serialize takeover across the publication interval")
		})
	}
}

func TestRenewWaitingOnExpiringPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	dsn := freshDSN(t)
	s, standby := openTest(t, dsn), openTest(t, dsn)
	if _, err := s.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.expiring_publication() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(330077); RETURN NEW; END$$; CREATE TRIGGER expiring_publication BEFORE INSERT ON plug_postgres.checkpoints FOR EACH ROW EXECUTE FUNCTION plug_postgres.expiring_publication()`); err != nil {
		t.Fatal(err)
	}
	gate, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	if _, err = gate.ExecContext(ctx, `SELECT pg_advisory_lock(330077)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_, _ = gate.ExecContext(cleanup, `SELECT pg_advisory_unlock(330077)`)
	}()
	active, err := s.Lease(ctx, "expiring-publisher", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	l := active.(*activeLease)
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_ = l.Release(cleanup)
	}()
	l.mu.Lock()
	expiry := l.timerDeadline
	l.mu.Unlock()
	pubCtx, pubCancel := context.WithCancel(ctx)
	defer pubCancel()
	renewCtx, renewCancel := context.WithCancel(ctx)
	defer renewCancel()
	published, renewed := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := s.SaveCheckpoint(pubCtx, l, "expiring", 0, []byte("must roll back"))
		published <- err
	}()
	waitFor := func(query, label string) {
		t.Helper()
		for {
			var waiting bool
			if err := s.db.QueryRowContext(ctx, query).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("waiting for %s: %v", label, ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
	}
	waitFor(`SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objid=330077 AND NOT granted)`, "publication SQL gate")
	// External contention can still prevent a heartbeat. It must expire and
	// cancel promptly; the publisher's compatible ownership fence does not
	// itself manufacture that contention.
	blocker, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(ctx, `SELECT epoch FROM plug_postgres.lease WHERE singleton FOR NO KEY UPDATE`); err != nil {
		t.Fatal(err)
	}
	go func() { renewed <- l.Renew(renewCtx) }()
	waitFor(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'UPDATE plug_postgres.lease SET expires=%')`, "renewal waiting for publication's lease row")
	t.Log("external NO KEY UPDATE holds expiry; renewal waits before expiry while publication remains gated")

	// A second heartbeat must honor cancellation even while the first waits.
	queuedCtx, queuedCancel := context.WithCancel(ctx)
	queuedCancel()
	queued := make(chan error, 1)
	go func() { queued <- l.Renew(queuedCtx) }()
	select {
	case err := <-queued:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("cancelled queued renewal: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("queued renewal ignored caller cancellation")
	}

	// Loss and the blocked heartbeat must finish while publication is still
	// gated. Neither assertion is rescued by manually cancelling renewal.
	select {
	case <-l.Lost():
	case <-time.After(time.Until(expiry) + 350*time.Millisecond):
		t.Error("renewal's SQL wait prevented expiry notification")
	}
	var fenced driver.ErrFenced
	select {
	case err := <-renewed:
		if !errors.As(err, &fenced) {
			t.Errorf("renewal after expiry: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("expired renewal remained blocked on the publication row")
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = gate.ExecContext(ctx, `SELECT pg_advisory_unlock(330077)`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-published:
		if !errors.As(err, &fenced) {
			t.Fatalf("expired publication: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expired publication could not finish its final lease check")
	}
	if _, err = s.db.ExecContext(ctx, `DROP TRIGGER expiring_publication ON plug_postgres.checkpoints; DROP FUNCTION plug_postgres.expiring_publication()`); err != nil {
		t.Fatal(err)
	}
	next, err := standby.Lease(ctx, "after-expiry", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release(ctx)
	if next.Epoch() <= l.Epoch() {
		t.Fatal("takeover reused the expired epoch")
	}
	if cp, err := standby.LoadCheckpoint(ctx, "expiring"); err != nil || cp.Generation != 0 {
		t.Fatalf("expired publication persisted: %+v %v", cp, err)
	}
	if cp, err := standby.SaveCheckpoint(ctx, next, "expiring", 0, []byte("new holder")); err != nil || cp.Generation != 1 {
		t.Fatalf("new holder could not publish: %+v %v", cp, err)
	}
	if err := l.Renew(ctx); !errors.As(err, &fenced) {
		t.Fatalf("expired lease was resurrected: %v", err)
	}
	t.Log("expiry cancels waiting renewal, fences and rolls back publication, and permits independent takeover without caller cancellation")
}

func TestRenewCancellationAndReplacedExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	active, err := s.Lease(ctx, "renew-cancellation", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	l := active.(*activeLease)
	defer l.Release(ctx)
	l.mu.Lock()
	oldExpiry := l.timerDeadline
	l.mu.Unlock()
	blocker, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err = blocker.ExecContext(ctx, `SELECT epoch FROM plug_postgres.lease WHERE singleton FOR SHARE`); err != nil {
		t.Fatal(err)
	}
	renewCtx, renewCancel := context.WithCancel(ctx)
	defer renewCancel()
	result := make(chan error, 1)
	go func() { result <- l.Renew(renewCtx) }()
	for {
		var waiting bool
		if err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'UPDATE plug_postgres.lease SET expires=%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	renewCancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled SQL renewal: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("renewal ignored caller cancellation during SQL wait")
	}
	select {
	case <-l.Lost():
		t.Fatal("cancellation before expiry terminated the lease")
	default:
	}
	if err = blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	// Renew well before expiry, leaving room to observe the old timer deadline
	// without crossing the replacement deadline on a loaded host.
	select {
	case <-time.After(time.Until(oldExpiry.Add(-time.Second))):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = l.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	newExpiry := l.timerDeadline
	l.mu.Unlock()
	select {
	case <-time.After(time.Until(oldExpiry.Add(100 * time.Millisecond))):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-l.Lost():
		t.Fatal("old expiry callback terminated a successfully renewed lease")
	default:
	}
	select {
	case <-l.Lost():
	case <-time.After(time.Until(newExpiry) + 350*time.Millisecond):
		t.Fatal("replacement expiry did not terminate the lease")
	}
	var fenced driver.ErrFenced
	if err = l.Renew(ctx); !errors.As(err, &fenced) {
		t.Fatalf("renewal resurrected the expired lease: %v", err)
	}
	t.Log("caller cancellation preserves the live lease; successful renewal replaces the old timer and cannot resurrect after the new expiry")
}

func TestRenewUsesReservedConnectionWhenRequestPoolIsExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)

	// Open reserves the lock and heartbeat connections before request traffic
	// starts. Reducing the general pool to those two handles leaves no pooled
	// request connection available; a heartbeat must still complete through its
	// reserved handle.
	s.db.SetMaxOpenConns(2)
	s.db.SetMaxIdleConns(0)
	if err := lease.Renew(ctx); err != nil {
		t.Fatalf("renewal starved by exhausted request pool: %v", err)
	}

	// Restore the pool limit so the test cleanup can release the lease through
	// the ordinary request path.
	s.db.SetMaxOpenConns(96)
	s.db.SetMaxIdleConns(16)
	t.Log("lease heartbeat completed on the reserved connection while the request pool had no free handle")
}

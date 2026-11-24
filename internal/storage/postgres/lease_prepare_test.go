package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// installLeaseClockCounter keeps the real SQL clock value from testclock while
// adding a non-transactional sequence counter. Sequence increments survive a
// rollback, so the test can distinguish the prepare observation from staging
// and final publication checks on the same disposable PostgreSQL database.
// Read-only clock evaluations do not increment: PostgreSQL prohibits nextval
// in read-only transactions. Lease/write evaluations remain fully counted.
func installLeaseClockCounter(t *testing.T, s *Store) func() int64 {
	t.Helper()
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, `
CREATE SEQUENCE plug_test_clock.lease_clock_calls MINVALUE 0 START 0;
CREATE OR REPLACE FUNCTION plug_test_clock.clock_timestamp() RETURNS timestamptz
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
  IF current_setting('transaction_read_only') = 'off' THEN
    PERFORM nextval('plug_test_clock.lease_clock_calls');
  END IF;
  RETURN (SELECT timestamptz 'epoch'
    + (last_value / 1000000) * interval '1 second'
    + (last_value % 1000000) * interval '1 microsecond'
    FROM plug_test_clock.micros);
END$$`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `SELECT setval('plug_test_clock.lease_clock_calls',0,false)`); err != nil {
		t.Fatal(err)
	}
	return func() int64 {
		var last int64
		var called bool
		if err := s.db.QueryRowContext(ctx, `SELECT last_value,is_called FROM plug_test_clock.lease_clock_calls`).Scan(&last, &called); err != nil {
			t.Fatal(err)
		}
		if !called {
			return 0
		}
		return last + 1
	}
}

func assertNoPreparedPublication(t *testing.T, s *Store, h driver.ChangesetHeader, ref driver.Ref) {
	t.Helper()
	ctx := context.Background()
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Head.IsZero() {
		t.Fatalf("stale prepared writer advanced head: %s", meta.Head)
	}
	for _, table := range []string{"records", "versions", "states", "changesets", "receipts"} {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.`+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("stale prepared writer published %d rows in %s", count, table)
		}
	}
	r := testRead(t, s, nil)
	rows, err := r.Get(ctx, []driver.Ref{ref})
	if err != nil || len(rows) != 0 {
		t.Fatalf("stale prepared writer record=%v err=%v", rows, err)
	}
	if receipt, err := r.IdempotencyByKey(ctx, "system:postgres-test", "", h.Method, h.IdempotencyKey); err != nil || receipt != nil {
		t.Fatalf("stale prepared writer receipt=%+v err=%v", receipt, err)
	}
}

func TestPreparedWriterStages1000OperationsWithoutPerPutLeaseClockQueries(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 3, 29, 1, 59, 59, 123456000, time.UTC))
	dsn := clock.BindPostgres(t, freshDSN(t))
	s := openTest(t, dsn)
	lease, err := s.Lease(ctx, "prepared-1000", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	calls := installLeaseClockCounter(t, s)

	refs := make([]driver.Ref, 1000)
	keys := make([]driver.LockKey, 1000)
	for i := range refs {
		refs[i] = testRef(fmt.Sprintf("prepared-%04d", i))
		keys[i] = testLock(refs[i])
	}
	tx, h := beginTest(t, s, lease, keys...)
	if got := calls(); got != 1 {
		t.Fatalf("writer preparation clock calls=%d, want exactly one", got)
	}
	for i, ref := range refs {
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
			t.Fatalf("stage %d: %v", i, err)
		}
	}
	if got := calls(); got != 1 {
		t.Fatalf("1000-op staging added lease clock calls=%d, want zero per-Put checks", got)
	}

	h.Changes = make([]model.ChangeRecord, len(refs))
	for i, ref := range refs {
		h.Changes[i] = model.ChangeRecord{Ref: ref, Op: model.OpCreate}
	}
	entry, err := tx.Commit(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Revision.String() != "1" || len(entry.Changes) != len(refs) {
		t.Fatalf("1000-op entry=%+v", entry)
	}
	if got := calls(); got != 4 {
		t.Fatalf("publication clock calls=%d, want prepare plus two fences and generated commit time", got)
	}
	r := testRead(t, s, nil)
	rows, err := r.Get(ctx, refs)
	if err != nil || len(rows) != len(refs) {
		t.Fatalf("durable 1000-op records=%d err=%v", len(rows), err)
	}
	t.Logf("real PostgreSQL 1000-op lease clock calls: prepare=1 staging=0 publication=%d; records=%d", calls()-1, len(rows))
}

func TestPreparedWriterRejectsReleaseExpiryAndTakeoverBeforeCommit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		release  bool
		expire   bool
		takeover bool
	}{
		{name: "release", release: true},
		{name: "expiry", expire: true},
		{name: "takeover", expire: true, takeover: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			clock := testclock.New(t, time.Date(2037, 3, 29, 1, 59, 59, 123456000, time.UTC))
			dsn := clock.BindPostgres(t, freshDSN(t))
			oldStore, newStore := openTest(t, dsn), openTest(t, dsn)
			old, err := oldStore.Lease(ctx, "prepared-fence-old", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			ref := testRef("prepared-fence")
			tx, h := beginTest(t, oldStore, old, testLock(ref))
			if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"prepared":true}`)}); err != nil {
				t.Fatal(err)
			}
			h.Method, h.IdempotencyKey, h.RequestHash = "test.prepared-fence", "prepared-fence", "prepared-fence-hash"
			h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}

			if tc.release {
				if err := old.Release(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if tc.expire {
				// The SQL clock advances without waiting for the local wall-clock
				// timer, so the final database fence is the deciding check.
				clock.Advance(t, driver.LeaseTTL+time.Microsecond)
			}
			if tc.takeover {
				next, err := newStore.Lease(ctx, "prepared-fence-new", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				defer next.Release(context.Background())
				if next.Epoch() <= old.Epoch() {
					t.Fatal("takeover reused prepared writer epoch")
				}
			}

			var fenced driver.ErrFenced
			if _, err := tx.Commit(ctx, h); !errors.As(err, &fenced) {
				t.Fatalf("prepared writer loss=%s commit=%v", tc.name, err)
			}
			assertNoPreparedPublication(t, newStore, h, ref)
			t.Logf("prepared writer %s refused atomically with ErrFenced", tc.name)
		})
	}
}

func TestPreparedWriterRetainsCancellationAfterStaging(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	first, second := testRef("prepared-cancel-first"), testRef("prepared-cancel-second")
	tx, h := beginTest(t, s, lease, testLock(first), testLock(second))
	if err := tx.Put(ctx, driver.Record{Ref: first, Kind: "test", Body: []byte(`{"first":true}`)}); err != nil {
		t.Fatal(err)
	}
	h.Method, h.IdempotencyKey, h.RequestHash = "test.prepared-cancel", "prepared-cancel", "prepared-cancel-hash"
	h.Changes = []model.ChangeRecord{{Ref: first, Op: model.OpCreate}}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := tx.Put(cancelCtx, driver.Record{Ref: second, Kind: "test", Body: []byte(`{"second":true}`)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled staged operation err=%v", err)
	}
	if _, err := tx.Commit(cancelCtx, h); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled commit err=%v", err)
	}
	assertNoPreparedPublication(t, s, h, first)
	t.Log("caller cancellation still wins after a prepared transaction has staged work")
}

func TestRestoreCommitTracksPostgresDriverEntry(t *testing.T) {
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	for _, tc := range []struct {
		name string
		stop bool
	}{
		{name: "commit"},
		{name: "cancel-before-driver", stop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			write, err := s.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			tx := write.(*transaction)
			defer tx.Close()
			tx.restore = &driver.Meta{}
			if tc.stop {
				cancel()
			}
			err = tx.commitSQL(ctx, "restore-commit-entry")
			if err != ctx.Err() {
				t.Fatalf("commit error=%v, want %v", err, ctx.Err())
			}
			if tx.commitEntered == nil || *tx.commitEntered != !tc.stop {
				t.Fatalf("driver entry must be %t for %s", !tc.stop, tc.name)
			}
		})
	}
}

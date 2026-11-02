package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

func TestS1PostgresClockControlsGeneratedCommitAndReceiptTime(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 3, 29, 1, 59, 59, 123456000, time.FixedZone("seed", -4*60*60)))
	dsn := clock.BindPostgres(t, freshDSN(t))
	if got := testclock.PostgresNow(t, dsn); !got.Equal(clock.Now()) {
		t.Fatalf("SQL clock=%s app=%s", got, clock.Now())
	}
	s := openTest(t, dsn)
	lease := testLease(t, s)
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "clock"}, Method: "s1.clock", IdempotencyKey: "generated", RequestHash: "generated-hash"}
	entry, err := conformance.Seed(ctx, s, lease, h, []conformance.FixtureChange{{Ref: testRef("pl-clock"), Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !entry.Time.Equal(clock.Now()) {
		t.Fatalf("SQL-generated commit time=%s; application clock=%s", entry.Time, clock.Now())
	}
	var expires time.Time
	if err = s.db.QueryRowContext(ctx, `SELECT expires FROM plug_postgres.receipts`).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if !expires.Equal(clock.Now().Add(driver.IdempotencyWindow)) {
		t.Fatalf("SQL receipt expiry=%s want=%s", expires, clock.Now().Add(driver.IdempotencyWindow))
	}
}

func TestS1PostgresSQLReceiptPendingFinalAndExpiryBoundaries(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 11, 1, 4, 29, 59, 999999000, time.UTC))
	s := openTest(t, clock.BindPostgres(t, freshDSN(t)))
	lease := testLease(t, s)
	initial := pendingTest(t, s, lease, "clock-pending", true)
	pendingTest(t, s, lease, "clock-final", false)
	pinned := testRead(t, s, nil)
	defer pinned.Close()
	read := func(key string) *driver.IdempotencyRecord {
		t.Helper()
		value, err := pinned.IdempotencyByKey(ctx, "system:postgres-test", "", "test.pending", key)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	var expires time.Time
	if err := s.db.QueryRowContext(ctx, `SELECT min(expires) FROM plug_postgres.receipts`).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if !expires.Equal(clock.Now().Add(driver.IdempotencyWindow)) {
		t.Fatalf("SQL expiry=%s", expires)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	clock.Advance(t, driver.IdempotencyWindow-time.Microsecond)
	lease = testLease(t, s)
	if value := read("clock-pending"); value == nil || !value.ResponsePending {
		t.Fatalf("pending before expiry=%+v", value)
	}
	if value := read("clock-final"); value == nil || value.ResponsePending {
		t.Fatalf("final before expiry=%+v", value)
	}
	if err := s.FinalizeResponse(ctx, lease, "system:postgres-test", "", "test.pending", "clock-pending", initial.Commit.Revision, nil); err != nil {
		t.Fatal(err)
	}
	if value := receiptTest(t, s, "clock-pending"); value.ResponsePending || !reflect.DeepEqual(value.Commit, initial.Commit) {
		t.Fatalf("fresh finalized receipt=%+v", value)
	}
	if value := read("clock-pending"); value == nil || !value.ResponsePending {
		t.Fatalf("finalization rewrote pinned snapshot=%+v", value)
	}
	for _, step := range []string{"exactly", "after"} {
		clock.Advance(t, time.Microsecond)
		for _, key := range []string{"clock-pending", "clock-final"} {
			if value := read(key); value != nil {
				t.Fatalf("%s SQL expiry key=%s retained=%+v", step, key, value)
			}
			if err := s.FinalizeResponse(ctx, lease, "system:postgres-test", "", "test.pending", key, initial.Commit.Revision, nil); !errors.Is(err, driver.ErrNotFound) {
				t.Fatalf("%s finalize expired=%v", step, err)
			}
		}
		t.Logf("SQL receipt boundary=%s now=%s pending/final lookup=nil finalizer=ErrNotFound", step, clock.Now().Format(time.RFC3339Nano))
	}
	var after time.Time
	if err := s.db.QueryRowContext(ctx, `SELECT min(expires) FROM plug_postgres.receipts`).Scan(&after); err != nil || !after.Equal(expires) {
		t.Fatalf("finalizer changed receipt age=%s -> %s err=%v", expires, after, err)
	}
	t.Log("SQL before/exactly/after 24h expiry uses the real driver queries, including a pinned MVCC reader; no receipt timestamp edits or sleeps")
}

func TestS1PostgresSQLLeaseExpiryAndRenewalBoundaries(t *testing.T) {
	for _, point := range []struct {
		name  string
		delta time.Duration
		live  bool
	}{
		{"before", driver.LeaseTTL - time.Microsecond, true},
		{"exactly", driver.LeaseTTL, false},
		{"after", driver.LeaseTTL + time.Microsecond, false},
	} {
		t.Run(point.name, func(t *testing.T) {
			ctx := context.Background()
			clock := testclock.New(t, time.Date(2037, 3, 29, 0, 0, 0, 123456000, time.UTC))
			s := openTest(t, clock.BindPostgres(t, freshDSN(t)))
			lease := testLease(t, s)
			clock.Advance(t, point.delta)
			_, err := s.SaveCheckpoint(ctx, lease, "clock-fence", 0, []byte("live"))
			var fenced driver.ErrFenced
			if point.live && err != nil || !point.live && !errors.As(err, &fenced) {
				t.Fatalf("SQL fence live=%v err=%v", point.live, err)
			}
			err = lease.Renew(ctx)
			if point.live {
				if err != nil {
					t.Fatalf("SQL renewal before expiry=%v", err)
				}
				var expires time.Time
				if err = s.db.QueryRowContext(ctx, `SELECT expires FROM plug_postgres.lease`).Scan(&expires); err != nil || !expires.Equal(clock.Now().Add(driver.LeaseTTL)) {
					t.Fatalf("renew SQL expiry=%s err=%v", expires, err)
				}
			} else {
				if !errors.As(err, &fenced) {
					t.Fatalf("expired SQL lease resurrected: %v", err)
				}
				select {
				case <-lease.Lost():
				default:
					t.Fatal("SQL expiry did not signal Lost")
				}
				next := testLease(t, s)
				if next.Epoch() <= lease.Epoch() {
					t.Fatal("SQL takeover reused epoch")
				}
			}
			t.Logf("SQL lease boundary=%s delta=%s ttl=%s live=%v", point.name, point.delta, driver.LeaseTTL, point.live)
		})
	}
}

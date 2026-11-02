package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

func TestS1SQLiteClockControlsGeneratedCommitAndReceiptTime(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 3, 29, 1, 59, 59, 123456789, time.FixedZone("seed", -4*60*60)))
	value, err := (Driver{Clock: clock.Now}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "clock.db")})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "clock"}, Method: "s1.clock", IdempotencyKey: "generated", RequestHash: "generated-hash"}
	entry, err := conformance.Seed(ctx, s, lease, h, []conformance.FixtureChange{{Ref: model.RefByID("pl-clock", model.Natural64(0)), Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !entry.Time.Equal(clock.Now()) {
		t.Fatalf("driver-generated commit time=%s; application clock=%s", entry.Time, clock.Now())
	}
	var created int64
	if err = s.db.QueryRowContext(ctx, `SELECT created FROM replays`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != clock.Now().UnixNano() {
		t.Fatalf("driver receipt creation=%d want=%d", created, clock.Now().UnixNano())
	}
}

func clockStore(t *testing.T, clock *testclock.Clock) *Store {
	t.Helper()
	value, err := (Driver{Clock: clock.Now}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "clock.db")})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestS1SQLiteGoReceiptPendingFinalAndExpiryBoundaries(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 11, 1, 4, 29, 59, 999999999, time.UTC))
	s := clockStore(t, clock)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	entry, h := commitReceipt(t, ctx, s, lease, "pl-clock-pending", true)
	_, final := commitReceipt(t, ctx, s, lease, "pl-clock-final", false)
	pinned := getReader(t, ctx, s, getMeta(t, ctx, s), nil)
	initial := lookupReceipt(t, ctx, pinned, h)
	var created int64
	if err := s.db.QueryRowContext(ctx, `SELECT min(created) FROM replays`).Scan(&created); err != nil || created != clock.Now().UnixNano() {
		t.Fatalf("created=%d err=%v", created, err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	clock.Advance(t, driver.IdempotencyWindow-time.Nanosecond)
	lease = getLease(t, ctx, s, driver.LeaseTTL)
	if got := lookupReceipt(t, ctx, pinned, h); !got.ResponsePending {
		t.Fatalf("pending before expiry=%+v", got)
	}
	if got := lookupReceipt(t, ctx, pinned, final); got.ResponsePending {
		t.Fatalf("final before expiry=%+v", got)
	}
	if err := s.FinalizeResponse(ctx, lease, "system:engine-test", "", h.Method, h.IdempotencyKey, entry.Revision, nil); err != nil {
		t.Fatal(err)
	}
	fresh := lookupReceipt(t, ctx, getReader(t, ctx, s, getMeta(t, ctx, s), nil), h)
	if fresh.ResponsePending || !reflect.DeepEqual(fresh.Commit, initial.Commit) {
		t.Fatalf("fresh final receipt=%+v", fresh)
	}
	if got := lookupReceipt(t, ctx, pinned, h); !got.ResponsePending {
		t.Fatal("finalization rewrote pinned pending image")
	}
	for _, step := range []string{"exactly", "after"} {
		clock.Advance(t, time.Nanosecond)
		for _, header := range []driver.ChangesetHeader{h, final} {
			got, err := pinned.IdempotencyByKey(ctx, "system:engine-test", "", header.Method, header.IdempotencyKey)
			if err != nil || got != nil {
				t.Fatalf("%s expiry lookup=%+v err=%v", step, got, err)
			}
			if err = s.FinalizeResponse(ctx, lease, "system:engine-test", "", header.Method, header.IdempotencyKey, entry.Revision, nil); !errors.Is(err, driver.ErrNotFound) {
				t.Fatalf("%s expired finalizer=%v", step, err)
			}
		}
		t.Logf("Go receipt boundary=%s now=%s pending/final lookup=nil finalizer=ErrNotFound", step, clock.Now().Format(time.RFC3339Nano))
	}
	var after int64
	if err := s.db.QueryRowContext(ctx, `SELECT min(created) FROM replays`).Scan(&after); err != nil || after != created {
		t.Fatalf("finalization changed receipt age=%d -> %d err=%v", created, after, err)
	}
	t.Log("SQLite persisted Go time and before/exactly/after 24h expiry preserve nanoseconds; native SQL clock is not applicable (no production SQL time call)")
}

func TestS1SQLiteGoLeaseExpiryAndRenewalBoundaries(t *testing.T) {
	for _, point := range []struct {
		name  string
		delta time.Duration
		live  bool
	}{
		{"before", driver.LeaseTTL - time.Nanosecond, true},
		{"exactly", driver.LeaseTTL, false},
		{"after", driver.LeaseTTL + time.Nanosecond, false},
	} {
		t.Run(point.name, func(t *testing.T) {
			ctx := context.Background()
			clock := testclock.New(t, time.Date(2037, 3, 29, 0, 0, 0, 123456789, time.UTC))
			s := clockStore(t, clock)
			realStart := time.Now()
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			deadline := lease.(*activeLease).deadline
			if deadline.Before(realStart.Add(driver.LeaseTTL)) || deadline.After(time.Now().Add(driver.LeaseTTL)) {
				t.Fatalf("real TTL budget changed: %s", deadline)
			}
			clock.Advance(t, point.delta)
			health, err := s.Health(ctx)
			if err != nil || (health.Role == "active") != point.live {
				t.Fatalf("health=%+v err=%v", health, err)
			}
			_, err = s.SaveCheckpoint(ctx, lease, "clock-fence", 0, []byte("live"))
			var fenced driver.ErrFenced
			if point.live && err != nil || !point.live && !errors.As(err, &fenced) {
				t.Fatalf("Go lease fence live=%v err=%v", point.live, err)
			}
			err = lease.Renew(ctx)
			if point.live {
				if err != nil {
					t.Fatal(err)
				}
				var expires int64
				if err = s.db.QueryRowContext(ctx, `SELECT expires FROM active_lease`).Scan(&expires); err != nil || expires != clock.Now().Add(driver.LeaseTTL).UnixNano() {
					t.Fatalf("renew expiry=%d err=%v", expires, err)
				}
			} else {
				if !errors.As(err, &fenced) {
					t.Fatalf("expired Go lease resurrected: %v", err)
				}
				select {
				case <-lease.Lost():
				default:
					t.Fatal("Go expiry did not signal Lost")
				}
				next := getLease(t, ctx, s, driver.LeaseTTL)
				if next.Epoch() <= lease.Epoch() {
					t.Fatal("takeover reused epoch")
				}
			}
			t.Logf("Go lease boundary=%s delta=%s ttl=%s live=%v real_timer_budget_preserved=true", point.name, point.delta, driver.LeaseTTL, point.live)
		})
	}
}

func TestS1SQLiteDefaultClockUsesWallTimeAndUnchangedLeaseBudget(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "default.db"))
	before := time.Now()
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	entry := seedRecord(t, ctx, s, lease, "pl-default-clock", `{}`)
	after := time.Now()
	if s.clock != nil || entry.Time.Before(before) || entry.Time.After(after) {
		t.Fatalf("default clock changed: %s", entry.Time)
	}
	local := lease.(*activeLease).deadline
	row, err := readLease(ctx, s.db)
	if err != nil || row.expires != local.UnixNano() || local.Before(before.Add(driver.LeaseTTL)) || local.After(after.Add(driver.LeaseTTL)) {
		t.Fatalf("default durable/local TTL diverged: row=%+v deadline=%s err=%v", row, local, err)
	}
}

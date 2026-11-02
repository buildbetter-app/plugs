package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

func TestS1MemoryGoReceiptPendingFinalAndExpiryBoundaries(t *testing.T) {
	ctx := context.Background()
	clock := testclock.New(t, time.Date(2037, 11, 1, 4, 29, 59, 999999999, time.UTC))
	s, err := NewWithOptions(Options{Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lease, err := s.Lease(ctx, "clock", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	var pending model.Changeset
	for _, key := range []string{"pending", "final"} {
		h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "clock"}, Method: "s1.clock", IdempotencyKey: key, RequestHash: key, ResponsePending: key == "pending"}
		entry, err := conformance.Seed(ctx, s, lease, h, nil)
		if err != nil || !entry.Time.Equal(clock.Now()) {
			t.Fatalf("generated memory time=%s err=%v", entry.Time, err)
		}
		if key == "pending" {
			pending = entry
		}
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	clock.Advance(t, driver.IdempotencyWindow-time.Nanosecond)
	lease, err = s.Lease(ctx, "clock-next", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"pending", "final"} {
		value, err := pinned.IdempotencyByKey(ctx, "system:clock", "", "s1.clock", key)
		if err != nil || value == nil || value.ResponsePending != (key == "pending") {
			t.Fatalf("before expiry key=%s value=%+v err=%v", key, value, err)
		}
	}
	if err := s.FinalizeResponse(ctx, lease, "system:clock", "", "s1.clock", "pending", pending.Revision, nil); err != nil {
		t.Fatal(err)
	}
	fresh, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	value, err := fresh.IdempotencyByKey(ctx, "system:clock", "", "s1.clock", "pending")
	if err != nil || value == nil || value.ResponsePending {
		t.Fatalf("fresh final receipt=%+v err=%v", value, err)
	}
	value, err = pinned.IdempotencyByKey(ctx, "system:clock", "", "s1.clock", "pending")
	if err != nil || value == nil || !value.ResponsePending {
		t.Fatal("finalization rewrote pinned snapshot")
	}
	for _, point := range []string{"exactly", "after"} {
		clock.Advance(t, time.Nanosecond)
		for _, key := range []string{"pending", "final"} {
			value, err := pinned.IdempotencyByKey(ctx, "system:clock", "", "s1.clock", key)
			if err != nil || value != nil {
				t.Fatalf("%s expiry key=%s value=%+v err=%v", point, key, value, err)
			}
			if err = s.FinalizeResponse(ctx, lease, "system:clock", "", "s1.clock", key, pending.Revision, nil); !errors.Is(err, driver.ErrNotFound) {
				t.Fatalf("%s expired finalizer=%v", point, err)
			}
		}
		t.Logf("memory receipt boundary=%s now=%s tick=1ns", point, clock.Now().Format(time.RFC3339Nano))
	}
}

func TestS1MemoryGoLeaseExpiryAndRenewalBoundaries(t *testing.T) {
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
			s, err := NewWithOptions(Options{Clock: clock.Now})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			lease, err := s.Lease(ctx, "clock", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			clock.Advance(t, point.delta)
			_, err = s.SaveCheckpoint(ctx, lease, "clock-fence", 0, []byte("live"))
			var fenced driver.ErrFenced
			if point.live && err != nil || !point.live && !errors.As(err, &fenced) {
				t.Fatalf("memory fence live=%v err=%v", point.live, err)
			}
			err = lease.Renew(ctx)
			if point.live && err != nil || !point.live && !errors.As(err, &fenced) {
				t.Fatalf("memory renewal live=%v err=%v", point.live, err)
			}
			t.Logf("memory lease boundary=%s delta=%s ttl=%s live=%v", point.name, point.delta, driver.LeaseTTL, point.live)
		})
	}
}

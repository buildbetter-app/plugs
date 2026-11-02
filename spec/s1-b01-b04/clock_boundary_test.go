package s1b01b04

import (
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestS1B04ApplicationClaimAndDriverReceiptBoundaries(t *testing.T) {
	h := newPostgresHarness(t)
	for _, backend := range backends(t, h) {
		t.Run(backend.name, func(t *testing.T) {
			f := newFixture(t, backend.name, backend.dsn, defaultOntology(), []string{"b04-clock"})
			createNodes(f, createOp("b04-clock", project, map[string]any{"title": "clock"}, ""))
			claimedAt := f.clock.Now()
			if code := errorCode(t, f.call("clock-claim", "bead.claim", owner, map[string]any{"id": ref("b04-clock")})); code != "" {
				t.Fatalf("claim=%s", code)
			}
			if got := f.document("b04-clock").Lease; got == nil || !got.ExpiresAt.Equal(claimedAt.Add(5*time.Minute)) {
				t.Fatalf("application-generated claim expiry=%+v", got)
			}
			principal, err := driver.PrincipalKey(f.document("b04-clock").Lease.Principal)
			if err != nil {
				t.Fatal(err)
			}
			tick := time.Nanosecond
			if backend.name == "postgres" {
				tick = time.Microsecond
			}
			f.advance(5*time.Minute - tick)
			patch := func(label string) []byte {
				return f.call(label, "bead.update", owner, map[string]any{"id": ref("b04-clock"), "fields": map[string]any{"title": label}})
			}
			if code := errorCode(t, patch("before-claim-expiry")); code != "" {
				t.Fatalf("before application expiry=%s", code)
			}
			for _, point := range []string{"exactly", "after"} {
				f.advance(tick)
				before := f.imageDigest()
				if code := errorCode(t, patch(point+"-claim-expiry")); code != "lease_expired" {
					t.Fatalf("%s application expiry=%s", point, code)
				}
				if got := f.imageDigest(); got != before {
					t.Fatal("expired write changed durable image")
				}
			}
			// The claim's receipt was created by the actual driver clock, not
			// header.Time. Cross its unchanged 24-hour window on the same store.
			f.advance(claimedAt.Add(driver.IdempotencyWindow - tick).Sub(f.clock.Now()))
			for _, point := range []string{"before", "exactly", "after"} {
				r, err := f.store.Read(f.ctx, driver.View{Incarnation: f.metaSnapshot().Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := r.IdempotencyByKey(f.ctx, principal, owner, "bead.claim", f.key("clock-claim", "bead.claim", owner))
				_ = r.Close()
				if err != nil || (receipt != nil) != (point == "before") {
					t.Fatalf("%s driver receipt expiry=%+v err=%v", point, receipt, err)
				}
				t.Logf("engine=%s receipt_boundary=%s now=%s tick=%s", backend.name, point, f.clock.Now().Format(time.RFC3339Nano), tick)
				if point != "after" {
					f.advance(tick)
				}
			}
			t.Logf("B04 application 5m expiry and actual driver 24h receipt expiry crossed without sleeps; SQLite SQL time=N/A, PG SQL time=clock_timestamp")
		})
	}
}

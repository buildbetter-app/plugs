package b01readyorder_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// All rank rows below come from public writes through the actual Writer and
// ontology classifier. Independent driver receipt expiry proves that the
// driver clock is reached; it is not a substitute oracle for ready ordering.
func TestB01IndependentDriverClockCannotAgePublicReadyRanks(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite_wal", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			base := time.Date(2026, 11, 3, 12, 0, 0, 123456000, time.UTC)
			application := testclock.New(t, base)
			storage := testclock.New(t, base.Add(7*time.Hour))
			f := newFixtureWithClocks(t, engine, application, storage)
			tick := time.Nanosecond
			if engine == "postgres" {
				tick = time.Microsecond
			}
			assertClocks := func(t *testing.T, point string, wantApplication, wantStorage time.Time) {
				t.Helper()
				if !application.Now().Equal(wantApplication) || !storage.Now().Equal(wantStorage) {
					t.Fatalf("%s clocks coupled: application=%s driver=%s", point, application.Now(), storage.Now())
				}
				sqlReadback := "N/A: driver Go clock, no SQL clock expression"
				if engine == "postgres" {
					actual := testclock.PostgresNow(t, f.sqlDSN)
					if !actual.Equal(wantStorage) {
						t.Fatalf("%s actual driver SQL clock=%s want=%s", point, actual, wantStorage)
					}
					sqlReadback = actual.Format(time.RFC3339Nano)
				}
				t.Logf("B01 independent_clocks engine=%s point=%s application=%s driver=%s SQL_readback=%s application_tick=1ns driver_tick=%s", engine, point, application.Now().Format(time.RFC3339Nano), storage.Now().Format(time.RFC3339Nano), sqlReadback, tick)
			}
			assertClocks(t, "initial", base, base.Add(7*time.Hour))

			aging := publicWrite(t, f, "bead.create", "independent-aging", map[string]any{"type": "issue", "fields": map[string]any{"title": "aging", "priority": 0}})
			agingRef := aging.Results[0].Ref
			connectedFact(t, f, agingRef, aging.Revision, base, base, 0, 0)
			originalReceipt := connectedReceipt(t, f, "bead.create", "", "independent-aging")
			if originalReceipt == nil || originalReceipt.ResponsePending {
				t.Fatal("public aging creation omitted its final driver receipt")
			}

			// Advance only the application by almost two days. A coupled driver
			// clock would have expired the original 24h receipt already.
			application.Advance(t, 48*time.Hour-time.Nanosecond)
			recent := publicWrite(t, f, "bead.create", "independent-recent", map[string]any{"type": "issue", "fields": map[string]any{"title": "recent", "priority": 4}})
			recentRef := recent.Results[0].Ref
			connectedFact(t, f, recentRef, recent.Revision, application.Now(), application.Now(), 0, 4)
			if got := connectedReceipt(t, f, "bead.create", "", "independent-aging"); !reflect.DeepEqual(got, originalReceipt) {
				t.Fatal("application-only 48h advance changed or expired the driver receipt")
			}
			t.Run("application-before-48h", func(t *testing.T) {
				assertClocks(t, "before-48h", base.Add(48*time.Hour-time.Nanosecond), base.Add(7*time.Hour))
				f.unchanged(t, func() { f.pages(t, nil, agingRef.ID, recentRef.ID) })
			})

			application.Advance(t, time.Nanosecond)
			// Refresh at the inclusive cutoff through an ordinary public title
			// update, without a fixture rank or a background refresh hook.
			updated := publicWrite(t, f, "bead.update", "independent-at-cutoff", map[string]any{"id": agingRef, "fields": map[string]any{"title": "updated at 48h"}})
			stored := connectedFact(t, f, agingRef, updated.Revision, base, application.Now(), 0, 0)
			t.Run("application-at-48h", func(t *testing.T) {
				assertClocks(t, "at-48h", base.Add(48*time.Hour), base.Add(7*time.Hour))
				f.unchanged(t, func() { f.pages(t, nil, agingRef.ID, recentRef.ID) })
			})
			application.Advance(t, time.Nanosecond)

			// No more writes are needed. Release the fixture's writer lease
			// before advancing driver time, so this remains an ordering test.
			if err := f.lease.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			f.lease = nil
			storage.Advance(t, driver.IdempotencyWindow-tick)
			for _, point := range []string{"before-driver-24h", "at-driver-24h", "after-driver-24h"} {
				t.Run(point, func(t *testing.T) {
					wantStorage := base.Add(7*time.Hour + driver.IdempotencyWindow)
					switch point {
					case "before-driver-24h":
						wantStorage = wantStorage.Add(-tick)
					case "after-driver-24h":
						wantStorage = wantStorage.Add(tick)
					}
					assertClocks(t, point, base.Add(48*time.Hour+time.Nanosecond), wantStorage)
					receipt := connectedReceipt(t, f, "bead.create", "", "independent-aging")
					if point == "before-driver-24h" {
						if !reflect.DeepEqual(receipt, originalReceipt) {
							t.Fatal("driver receipt expired before its actual 24h boundary")
						}
					} else if receipt != nil {
						t.Fatal("driver clock advance did not expire the real receipt")
					}
					// Live classification would put recent p4 first. The saved
					// p0 rank is still only 1ns old on the application clock.
					f.unchanged(t, func() { f.pages(t, nil, agingRef.ID, recentRef.ID) })
				})
				if point != "after-driver-24h" {
					storage.Advance(t, tick)
				}
			}

			for _, step := range []struct {
				name  string
				delta time.Duration
				first string
				last  string
			}{
				{"application-at-P74-60s", time.Minute - time.Nanosecond, agingRef.ID, recentRef.ID},
				{"application-after-P74-60s", time.Nanosecond, recentRef.ID, agingRef.ID},
			} {
				application.Advance(t, step.delta)
				t.Run(step.name, func(t *testing.T) {
					wantApplication := base.Add(48*time.Hour + time.Minute)
					if step.name == "application-after-P74-60s" {
						wantApplication = wantApplication.Add(time.Nanosecond)
					}
					assertClocks(t, step.name, wantApplication, base.Add(7*time.Hour+driver.IdempotencyWindow+tick))
					f.unchanged(t, func() { f.pages(t, nil, step.first, step.last) })
					if row := connectedFact(t, f, agingRef, updated.Revision, base, base.Add(48*time.Hour), 0, 0); !reflect.DeepEqual(row, stored) {
						t.Fatal("ready fallback rewrote the original public rank")
					}
				})
			}
		})
	}
}

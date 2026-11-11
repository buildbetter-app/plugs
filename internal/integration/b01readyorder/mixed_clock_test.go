package b01readyorder_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// These public inputs match the independent native fixture's three ordering
// roles at a second-aligned creation origin. Native's omitted-sort priority
// vector and explicit hybrid vector remain separate from Plug's stored rank
// allowance. Native runtime qualification requires that fixture's originals;
// neither the vectors below nor this consumer supplies that native proof.
func TestB01MixedPlaneOrderUsesIndependentClocksThroughP74AndSQLReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite_wal", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			boundary := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
			storage := boundary.Add(7 * time.Hour)
			p := newPublicAcceptanceWithClocks(t, engine, true, boundary.Add(-48*time.Hour), storage)
			aging := p.create(t, "mixed-clock-aging", "issue", "", 0, nil)
			p.clock.Advance(t, 47*time.Hour)
			ordinary := p.create(t, "mixed-clock-control", "issue", "", 4, nil)
			blocker := p.create(t, "mixed-clock-blocker", "issue", "in_progress", 0, nil)
			items := p.invoke(t, "mixed-clock", []map[string]any{
				{"label": "active", "title": "active recent priority two", "type": "issue", "fields": map[string]any{"priority": 2}},
				{"label": "blocked", "title": "active blocked priority zero", "type": "issue", "fields": map[string]any{"priority": 0}, "links": []map[string]any{{"kind": "blocks", "to": blocker}}},
			})
			active := items[0].ID
			p.clock.Advance(t, time.Hour-time.Nanosecond)
			p.point(t, "mixed-48h-before", nil, aging.ID, active, ordinary.ID)
			p.clock.Advance(t, time.Nanosecond)
			update := p.write(t, "bead.update", "mixed-clock-rank-at-cutoff", map[string]any{"id": aging, "fields": map[string]any{"title": "ranked at inclusive mixed cutoff"}})
			stored := connectedFact(t, p.fixture, aging, update.Revision, boundary.Add(-48*time.Hour), boundary, 0, 0)
			p.document(t, aging, "issue", "todo", "ranked at inclusive mixed cutoff", 0, nil)
			p.point(t, "mixed-48h-at", nil, aging.ID, active, ordinary.ID)
			p.clock.Advance(t, time.Microsecond)
			p.point(t, "mixed-after-cutoff-P74-stored-order", nil, aging.ID, active, ordinary.ID)
			// Native recomputes hybrid as [active, ordinary, aging] here. Its
			// public omitted-sort priority remains [aging, active, ordinary].
			// Plug retains the actual canonical Writer rank through 60s.

			before, checkpoint, receipts := p.state(t), p.checkpoint(t), p.receipts(t)
			if err := p.lease.Release(p.ctx); err != nil {
				t.Fatal(err)
			}
			p.lease = nil
			p.storage.Advance(t, time.Second)
			p.bind(t)
			if !p.clock.Now().Equal(boundary.Add(time.Microsecond)) || !p.storage.Now().Equal(storage.Add(time.Second)) {
				t.Fatal("SQL-only movement changed the independent application instant")
			}
			if engine == "postgres" && !testclock.PostgresNow(t, p.dsn).Equal(storage.Add(time.Second)) {
				t.Fatal("actual independent PostgreSQL clock did not move by one second")
			}
			if !reflect.DeepEqual(before, p.state(t)) || !reflect.DeepEqual(checkpoint, p.checkpoint(t)) || !reflect.DeepEqual(receipts, p.receipts(t)) {
				t.Fatal("driver-only movement or same-process service rebind changed source, ranks, history, checkpoint or receipts")
			}
			p.point(t, "mixed-SQL-only-after-cutoff-P74-unchanged", nil, aging.ID, active, ordinary.ID)
			p.clock.Advance(t, time.Minute-time.Microsecond)
			p.point(t, "mixed-P74-at-60s", nil, aging.ID, active, ordinary.ID)
			p.clock.Advance(t, time.Nanosecond)
			p.point(t, "mixed-P74-after-60s-live-hybrid", nil, active, ordinary.ID, aging.ID)
			p.point(t, "mixed-P74-durable-only-before-page", map[string]any{"includeEphemeral": false}, ordinary.ID, aging.ID)
			if !reflect.DeepEqual(stored, connectedFact(t, p.fixture, aging, update.Revision, boundary.Add(-48*time.Hour), boundary, 0, 0)) {
				t.Fatal("mixed reads or live fallback rewrote the canonical stored rank")
			}
			p.document(t, ordinary, "issue", "todo", "mixed-clock-control", 4, nil)
			p.document(t, blocker, "issue", "in_progress", "mixed-clock-blocker", 0, nil)
		})
	}
}

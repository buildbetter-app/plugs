package conformance

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// RunAudit is opt-in follow-on conformance for drivers that persist canonical
// event plans. Run remains the unchanged baseline for drivers adopting it later.
func RunAudit(t *testing.T, factory Factory) {
	t.Run("ordered_plan_seed_log_lookup_and_pinned_history", func(t *testing.T) { auditPublication(t, factory) })
	t.Run("logical_order_between_actual_changes", func(t *testing.T) { auditChangeOrder(t, factory) })
	t.Run("invalid_plans_refuse_atomically", func(t *testing.T) { auditValidation(t, factory) })
	t.Run("complete_plan_cap_and_overflow", func(t *testing.T) { auditEventBound(t, factory) })
	t.Run("noop_refusal_and_nil_legacy_indexes", func(t *testing.T) { auditNoopAndLegacy(t, factory) })
	t.Run("copy_isolation_replay_and_finalization_CAS", func(t *testing.T) { auditIsolationAndReceipts(t, factory) })
	t.Run("rollback_cancel_and_fence_publish_nothing", func(t *testing.T) { auditAborts(t, factory) })
	t.Run("takeover_races_atomic_event_publication", func(t *testing.T) { auditTakeoverRace(t, factory) })
}

func auditChangeOrder(t *testing.T, factory Factory) {
	f := newAuditFixture(t, factory, 2)
	// This record sorts before the blocker in the lock set, but its actual
	// change and ordinary event follow the blocker's entire notice group.
	f.changes = append(f.changes, FixtureChange{Ref: ref("pl-a"), Kind: "bead", Op: model.OpCreate, Body: []byte(`{}`)})
	p := f.header.Events[0].Provenance
	p.Method, p.Slot = "bead.create", ""
	f.header.Events = append(f.header.Events, model.EventDescriptor{Index: "3", ChangeIndex: "1", Type: model.EventBeadCreated, Resource: model.AuditResource{Kind: "bead", Ref: ref("pl-a")}, Provenance: p})
	entry, err := Seed(f.ctx, f.store, f.lease, f.header, f.changes)
	if err != nil {
		t.Fatal(err)
	}
	r := read(t, f.ctx, f.store, f.meta, nil)
	log, err := r.Changesets(f.ctx, &driver.Cursor{Incarnation: f.meta.Incarnation, Revision: "1"}, 1)
	if err != nil || len(log) != 1 || !reflect.DeepEqual(log[0], entry) || !reflect.DeepEqual(entry.Events, f.header.Events) {
		t.Fatalf("logical event order changed: %+v %v", log, err)
	}
	if len(entry.Changes) != 2 || entry.Changes[0].Ref.ID != "pl-blocker" || entry.Changes[1].Ref.ID != "pl-a" || len(entry.Events) != 4 {
		t.Fatalf("changes/descriptor order changed: %+v", entry)
	}
}

func auditPublication(t *testing.T, factory Factory) {
	f := newAuditFixture(t, factory, 2)
	ctx, s, lease, meta, h := f.ctx, f.store, f.lease, f.meta, f.header
	pinned := read(t, ctx, s, meta, nil)
	entry, err := Seed(ctx, s, lease, h, f.changes)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.ValidateEvents(); err != nil {
		t.Fatal(err)
	}
	if len(entry.Changes) != 1 || !reflect.DeepEqual(entry.Events, h.Events) {
		t.Fatalf("lost plan or fabricated dependent mutation: %+v", entry)
	}
	current := read(t, ctx, s, meta, nil)
	log, err := current.Changesets(ctx, &driver.Cursor{Incarnation: meta.Incarnation, Revision: *pinned.Snapshot().At}, 1)
	if err != nil || len(log) != 1 || !reflect.DeepEqual(log[0], entry) {
		t.Fatalf("persisted plan=%+v %v", log, err)
	}
	lookup, err := current.ChangesetByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey)
	if err != nil || lookup == nil || !reflect.DeepEqual(*lookup, entry) {
		t.Fatalf("keyed plan=%+v %v", lookup, err)
	}
	if old, err := pinned.ChangesetByKey(ctx, "system:conformance", "", h.Method, h.IdempotencyKey); err != nil || old != nil {
		t.Fatalf("plan leaked into old snapshot: %+v %v", old, err)
	}
	for _, event := range entry.Events[1:] {
		pin := event.Details.Dependent
		rows, err := current.Get(ctx, []driver.Ref{pin.Ref})
		if err != nil || len(rows) != 1 || rows[0].Revision.String() != pin.Revision.String() {
			t.Fatalf("dependent acquired a fake revision: %+v %v", rows, err)
		}
		versions, err := current.Versions(ctx, pin.Ref, driver.PageReq{})
		if err != nil || len(versions.Items) != 1 {
			t.Fatalf("dependent acquired a fake version: %+v %v", versions, err)
		}
	}
	before, err := pinned.Get(ctx, []driver.Ref{ref("pl-blocker")})
	if err != nil || len(before) != 1 || string(before[0].Body) != `{"status":"working"}` {
		t.Fatalf("old blocker image changed: %+v %v", before, err)
	}
	if meta, err := s.Meta(ctx); err != nil || meta.Head.String() != entry.Revision.String() {
		t.Fatalf("audit consumed extra revisions: %+v %v", meta, err)
	}
	// A new reader pinned to the audited revision sees the entire plan too.
	historical := read(t, ctx, s, meta, &entry.Revision)
	history, err := historical.Changesets(ctx, &driver.Cursor{Incarnation: meta.Incarnation, Revision: *pinned.Snapshot().At}, 1)
	if err != nil || len(history) != 1 || !reflect.DeepEqual(history[0].Events, h.Events) {
		t.Fatalf("historical plan=%+v %v", history, err)
	}
}

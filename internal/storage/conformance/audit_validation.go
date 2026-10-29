package conformance

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func auditValidation(t *testing.T, factory Factory) {
	cases := []struct {
		name   string
		mutate func(*driver.ChangesetHeader)
	}{
		{"present_empty", func(h *driver.ChangesetHeader) { h.Events = []model.EventDescriptor{} }},
		{"missing_ordinary", func(h *driver.ChangesetHeader) {
			h.Events = h.Events[1:]
			h.Events[0].Index = "0"
			h.Events[1].Index = "1"
		}},
		{"missing_notice_details", func(h *driver.ChangesetHeader) { h.Events[1].Details = nil }},
		{"ordinary_with_details", func(h *driver.ChangesetHeader) { h.Events[0].Details = h.Events[1].Details }},
		{"duplicate_index", func(h *driver.ChangesetHeader) { h.Events[1].Index = "0" }},
		{"index_gap", func(h *driver.ChangesetHeader) { h.Events[1].Index = "8" }},
		{"index_overflow", func(h *driver.ChangesetHeader) { h.Events[1].Index = model.Natural("1e1000000000") }},
		{"invalid_anchor", func(h *driver.ChangesetHeader) { h.Events[1].ChangeIndex = model.Natural("0.5") }},
		{"wrong_anchor", func(h *driver.ChangesetHeader) { h.Events[1].ChangeIndex = "1" }},
		{"wrong_changed_resource", func(h *driver.ChangesetHeader) { h.Events[0].Resource.Ref = ref("pl-other") }},
		{"wrong_operation", func(h *driver.ChangesetHeader) { h.Events[0].Type = model.EventBeadCreated }},
		{"unknown_type", func(h *driver.ChangesetHeader) { h.Events[0].Type = "future.event" }},
		{"chain_mismatch", func(h *driver.ChangesetHeader) { h.Chain = []string{"other"} }},
		{"missing_setby", func(h *driver.ChangesetHeader) { h.Events[0].Provenance.SetBy = nil }},
		{"notice_provenance", func(h *driver.ChangesetHeader) { h.Events[1].Provenance.Method = "bead.cancel" }},
		{"notice_source_revision", func(h *driver.ChangesetHeader) { h.Events[1].Details.BlockerBefore.Revision = "0" }},
		{"notice_final_revision", func(h *driver.ChangesetHeader) { h.Events[1].Details.BlockerAfter.Revision = "3" }},
		{"reversed_notices", func(h *driver.ChangesetHeader) {
			h.Events[1], h.Events[2] = h.Events[2], h.Events[1]
			h.Events[1].Index = "1"
			h.Events[2].Index = "2"
		}},
		{"duplicate_edge", func(h *driver.ChangesetHeader) {
			h.Events[2] = model.CloneEventDescriptors(h.Events[1:2])[0]
			h.Events[2].Index = "2"
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := newAuditFixture(t, factory, 2)
			before := read(t, f.ctx, f.store, f.meta, nil)
			h := f.header
			h.Events = model.CloneEventDescriptors(h.Events)
			test.mutate(&h)
			if _, err := Seed(f.ctx, f.store, f.lease, h, f.changes); err == nil {
				t.Fatal("invalid plan committed")
			}
			auditUnchanged(t, f, before)
			// Refusal must not consume the next revision or reserve the key.
			entry, err := Seed(f.ctx, f.store, f.lease, f.header, f.changes)
			if err != nil || entry.Revision.String() != "2" {
				t.Fatalf("valid retry after refusal=%+v %v", entry, err)
			}
		})
	}
}

func auditEventBound(t *testing.T, factory Factory) {
	// All source records exist, including the edge beyond the admitted cap.
	f := newAuditFixture(t, factory, model.MaxChangesetEvents)
	before := read(t, f.ctx, f.store, f.meta, nil)
	if _, err := Seed(f.ctx, f.store, f.lease, f.header, f.changes); err == nil {
		t.Fatal("overflow plan was truncated or committed")
	}
	auditUnchanged(t, f, before)
	f.header.Events = f.header.Events[:model.MaxChangesetEvents]
	entry, err := Seed(f.ctx, f.store, f.lease, f.header, f.changes)
	if err != nil || len(entry.Events) != model.MaxChangesetEvents || !reflect.DeepEqual(entry.Events, f.header.Events) {
		t.Fatalf("exact cap did not persist completely: count=%d error=%v", len(entry.Events), err)
	}
	r := read(t, f.ctx, f.store, f.meta, nil)
	log, err := r.Changesets(f.ctx, &driver.Cursor{Incarnation: f.meta.Incarnation, Revision: *before.Snapshot().At}, 1)
	if err != nil || len(log) != 1 || len(log[0].Events) != model.MaxChangesetEvents {
		t.Fatalf("event plan was partially returned: %+v %v", log, err)
	}
}

func auditNoopAndLegacy(t *testing.T, factory Factory) {
	for _, plan := range []string{"empty", "populated"} {
		t.Run("noop_"+plan, func(t *testing.T) {
			f := newAuditFixture(t, factory, 1)
			before := read(t, f.ctx, f.store, f.meta, nil)
			tx := begin(t, f.ctx, f.store, f.lease, nil)
			if _, err := tx.Head(f.ctx); err != nil {
				t.Fatal(err)
			}
			h := f.header
			if plan == "empty" {
				h.Events = []model.EventDescriptor{}
			}
			noop, ok := tx.(driver.NoopCommitter)
			if !ok {
				t.Fatal("NoopCommitter missing")
			}
			if _, err := noop.CommitNoop(f.ctx, h); err == nil {
				t.Fatal("no-op accepted a plan")
			}
			auditUnchanged(t, f, before)
			h.Events = nil
			h.Response.Results = []model.OpResult{{Ref: ref("pl-blocker"), Revision: "1"}}
			if response, err := noop.CommitNoop(f.ctx, h); err != nil || response.Revision.String() != "1" {
				t.Fatalf("legacy no-op refused: %+v %v", response, err)
			}
		})
	}
	f := newAuditFixture(t, factory, 1)
	f.header.Events = nil
	f.changes = append(f.changes, FixtureChange{Ref: ref("pl-a"), Kind: "bead", Op: model.OpCreate, Body: []byte(`{}`)})
	entry, err := Seed(f.ctx, f.store, f.lease, f.header, f.changes)
	if err != nil {
		t.Fatal(err)
	}
	// Lock order differs from caller change order; nil-only legacy indexes
	// must remain those exact ChangeRecord positions, never inferred events.
	want := []string{"pl-blocker", "pl-a"}
	r := read(t, f.ctx, f.store, f.meta, &entry.Revision)
	log, err := r.Changesets(f.ctx, nil, 100)
	if err != nil || len(log) != 2 || log[0].Events != nil || log[1].Events != nil {
		t.Fatalf("legacy plan discriminator changed: %+v %v", log, err)
	}
	for i, id := range want {
		if log[1].Changes[i].Ref.ID != id || entry.Changes[i].Ref.ID != id {
			t.Fatalf("legacy index %d moved", i)
		}
	}
}

func auditUnchanged(t *testing.T, f auditFixture, before driver.Reader) {
	t.Helper()
	after := read(t, f.ctx, f.store, f.meta, nil)
	if after.Snapshot().At.String() != before.Snapshot().At.String() {
		t.Fatal("failed audit advanced head")
	}
	for _, r := range []driver.Reader{before, after} {
		if cached, err := r.IdempotencyByKey(f.ctx, "system:conformance", "", f.header.Method, f.header.IdempotencyKey); err != nil || cached != nil {
			t.Fatalf("failed audit published receipt: %+v %v", cached, err)
		}
	}
	oldLog, err := before.Changesets(f.ctx, nil, 10000)
	if err != nil {
		t.Fatal(err)
	}
	newLog, err := after.Changesets(f.ctx, nil, 10000)
	if err != nil || !reflect.DeepEqual(newLog, oldLog) {
		t.Fatalf("failed audit changed log: %v", err)
	}
	oldRows, err := before.Query(f.ctx, driver.Query{Limit: 10000})
	if err != nil {
		t.Fatal(err)
	}
	newRows, err := after.Query(f.ctx, driver.Query{Limit: 10000})
	if err != nil || !reflect.DeepEqual(newRows.Items, oldRows.Items) {
		t.Fatalf("failed audit changed records: %v", err)
	}
	for _, row := range oldRows.Items {
		oldVersions, err := before.Versions(f.ctx, row.Ref, driver.PageReq{Limit: 10000})
		if err != nil {
			t.Fatal(err)
		}
		newVersions, err := after.Versions(f.ctx, row.Ref, driver.PageReq{Limit: 10000})
		if err != nil || !reflect.DeepEqual(newVersions.Items, oldVersions.Items) {
			t.Fatalf("failed audit changed versions for %s: %v", row.Ref.ID, err)
		}
	}
}

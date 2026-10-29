package extended

import (
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestB24DeferCategoryAndExplicitReopenAtControlledTime(t *testing.T) {
	f := newFixture(t, []string{"pl-deferred"}, compile(t, customDocument(), nil))
	f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "wait for upstream"}})
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-deferred")
	// This is the actual implemented defer slot plus reason-comment lowering.
	// There is no production time-target input or automatic wake binding yet.
	cs := model.ChangeSet{Ops: []model.Op{
		patch("pl-deferred", map[string]any{}),
		{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref("pl-deferred"), Body: "waiting for upstream"}},
	}}
	commit, err := f.writer.Apply(f.ctx, f.by, cs, f.meta("defer-with-reason", "bead.defer"))
	if err != nil {
		t.Fatalf("real defer slot: %v", err)
	}
	if commit.Revision.String() != "2" || len(commit.Results) != 2 {
		t.Fatalf("defer was not atomic: %+v", commit)
	}
	deferred := f.image(t)
	doc := deferred.document(t, "pl-deferred")
	if doc.Fields["status"] != "parked" || len(doc.Comments) != 1 || doc.Comments[0].Body != "waiting for upstream" ||
		!doc.Comments[0].Time.Equal(f.inputs.Now()) || doc.Lease != nil {
		t.Fatalf("defer lost its actual category/reason/controlled timestamp: %+v", doc)
	}
	pageIDs(t, f.success(t, "bead.ready", nil))
	f.advance(t, 5*time.Minute)
	// A read never fabricates a wake. This tests the supported category slot,
	// not full B24 expiry semantics, which remain an explicit obligation.
	pageIDs(t, f.success(t, "bead.ready", nil))
	f.unchanged(t, deferred)
	data := f.call(t, "patch-wake-refused", "bead.update", map[string]any{
		"id": wireRef("pl-deferred"), "fields": map[string]any{"status": "queued", "title": "must not land"}})
	if at[string](t, data, "/error/data/code") != "ontology_use_slot_verb" {
		t.Fatalf("direct wake bypassed the slot: %s", data)
	}
	f.unchanged(t, deferred)
	f.noReceipt(t, "patch-wake-refused", "bead.update")
	f.slot(t, "explicit-reopen", "bead.reopen", "pl-deferred", nil)
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-deferred")
	after := f.image(t)
	if after.row(t, "pl-deferred").Revision.String() != "3" || len(after.Versions["pl-deferred"]) != 3 || len(after.Log) != 3 {
		t.Fatalf("explicit reopen did not commit once: %+v", after)
	}
	want := doc
	want.Fields["status"] = "queued"
	sameJSON(t, after.document(t, "pl-deferred"), want)
	if !after.Log[2].Time.Equal(f.inputs.Now()) {
		t.Fatalf("reopen log ignored controlled time: %+v", after.Log[2])
	}
}

func TestB24ControlledClaimExpiryBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		advance time.Duration
		expired bool
	}{
		{"one-nanosecond-before", 5*time.Minute - time.Nanosecond, false},
		{"exact-deadline", 5 * time.Minute, true},
		{"one-nanosecond-after", 5*time.Minute + time.Nanosecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, []string{"pl-timed"}, nil)
			f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "clock boundary"}})
			start := f.inputs.Now()
			claimed := f.success(t, "bead.claim", map[string]any{"id": wireRef("pl-timed")})
			if at[string](t, claimed, "/result/leaseExpiresAt") != start.Add(5*time.Minute).Format(time.RFC3339Nano) ||
				at[int](t, claimed, "/result/leaseTtlMs") != 300000 {
				t.Fatalf("claim expiry did not use the controlled production clock/TTL: %s", claimed)
			}
			before := f.image(t)
			f.advance(t, tc.advance)
			// Started work remains outside ready. Automatic expired-claim
			// reclaim is not implemented and is not claimed by this test.
			pageIDs(t, f.success(t, "bead.ready", nil))
			f.unchanged(t, before)
			data := f.call(t, "timed-close", "bead.close", map[string]any{"id": wireRef("pl-timed"), "reason": "settle"})
			if tc.expired {
				if at[string](t, data, "/error/data/code") != "lease_expired" {
					t.Fatalf("deadline admitted expired close: %s", data)
				}
				f.unchanged(t, before)
				f.noReceipt(t, "timed-close", "bead.close")
				return
			}
			if at[int](t, data, "/result/revision") != 3 {
				t.Fatalf("live claim close failed: %s", data)
			}
			after := f.image(t)
			doc := after.document(t, "pl-timed")
			if doc.Lease != nil || doc.Fields["status"] != "done" || len(doc.Comments) != 1 || !doc.Comments[0].Time.Equal(f.inputs.Now()) {
				t.Fatalf("live close did not settle at the controlled instant: %+v", doc)
			}
			if len(after.Log) != 3 || len(after.Versions["pl-timed"]) != 3 {
				t.Fatalf("close history duplicated: %+v", after)
			}
		})
	}
}

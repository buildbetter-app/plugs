package extended

import (
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

func TestB24UntimedDeferAndExplicitReopenOverRPC(t *testing.T) {
	f := newFixture(t, []string{"pl-reasoned", "pl-optional"}, compile(t, customDocument(), nil))
	f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "reasoned defer"}})
	f.success(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "optional reason"}})
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-optional", "pl-reasoned")

	// Plug intentionally has no defer target instant. The strict RPC envelope
	// refuses the Beads-shaped field without publishing a write or receipt.
	before := f.image(t)
	data := f.call(t, "timed-defer-refused", "bead.defer", map[string]any{
		"id": wireRef("pl-reasoned"), "until": "2026-10-30T12:00:00Z"})
	if at[string](t, data, "/error/data/code") != "invalid_params" {
		t.Fatalf("timed defer input entered the untimed Plug slot: %s", data)
	}
	f.unchanged(t, before)
	f.noReceipt(t, "timed-defer-refused", "bead.defer")

	reasoned := f.success(t, "bead.defer", map[string]any{
		"id": wireRef("pl-reasoned"), "reason": "waiting for upstream",
		"expect": []any{map[string]any{"ref": wireRef("pl-reasoned"), "revision": 1}},
	})
	if at[int](t, reasoned, "/result/revision") != 3 ||
		at[int](t, reasoned, "/result/results/0/revision") != 3 ||
		at[int](t, reasoned, "/result/results/1/revision") != 3 {
		t.Fatalf("reasoned defer was not one atomic RPC commit: %s", reasoned)
	}
	optional := f.success(t, "bead.defer", map[string]any{
		"id":     wireRef("pl-optional"),
		"expect": []any{map[string]any{"ref": wireRef("pl-optional"), "revision": 2}},
	})
	if at[int](t, optional, "/result/revision") != 4 ||
		at[int](t, optional, "/result/results/0/revision") != 4 {
		t.Fatalf("reason-optional defer did not commit through RPC: %s", optional)
	}

	deferred := f.image(t)
	reasonedDoc := deferred.document(t, "pl-reasoned")
	if reasonedDoc.Fields["status"] != "parked" || len(reasonedDoc.Comments) != 1 ||
		reasonedDoc.Comments[0].Body != "waiting for upstream" ||
		!reasonedDoc.Comments[0].Time.Equal(f.inputs.Now()) || reasonedDoc.Lease != nil {
		t.Fatalf("reasoned defer lost category/comment semantics: %+v", reasonedDoc)
	}
	optionalDoc := deferred.document(t, "pl-optional")
	if optionalDoc.Fields["status"] != "parked" || len(optionalDoc.Comments) != 0 || optionalDoc.Lease != nil {
		t.Fatalf("reason-optional defer invented a comment or lease: %+v", optionalDoc)
	}
	pageIDs(t, f.success(t, "bead.ready", nil))

	f.advance(t, 30*time.Minute)
	pageIDs(t, f.success(t, "bead.ready", nil))
	f.unchanged(t, deferred)

	// An explicit stale expectation must refuse the reopen without history,
	// revision, comment, receipt, or derived-state publication.
	data = f.call(t, "stale-reopen-refused", "bead.reopen", map[string]any{
		"id": wireRef("pl-reasoned"), "reason": "must not append",
		"expect": []any{map[string]any{"ref": wireRef("pl-reasoned"), "revision": 1}},
	})
	if at[string](t, data, "/error/data/code") != "precondition_failed" {
		t.Fatalf("stale reopen expectation was admitted: %s", data)
	}
	f.unchanged(t, deferred)
	f.noReceipt(t, "stale-reopen-refused", "bead.reopen")

	optionalReopen := f.success(t, "bead.reopen", map[string]any{
		"id":     wireRef("pl-optional"),
		"expect": []any{map[string]any{"ref": wireRef("pl-optional"), "revision": 4}},
	})
	if at[int](t, optionalReopen, "/result/revision") != 5 ||
		at[int](t, optionalReopen, "/result/results/0/revision") != 5 {
		t.Fatalf("reason-optional reopen did not commit through RPC: %s", optionalReopen)
	}
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-optional")

	reasonedReopen := f.success(t, "bead.reopen", map[string]any{
		"id": wireRef("pl-reasoned"), "reason": "upstream ready",
		"expect": []any{map[string]any{"ref": wireRef("pl-reasoned"), "revision": 3}},
	})
	if at[int](t, reasonedReopen, "/result/revision") != 6 ||
		at[int](t, reasonedReopen, "/result/results/0/revision") != 6 ||
		at[int](t, reasonedReopen, "/result/results/1/revision") != 6 {
		t.Fatalf("reasoned reopen was not one atomic RPC commit: %s", reasonedReopen)
	}
	pageIDs(t, f.success(t, "bead.ready", nil), "pl-optional", "pl-reasoned")

	after := f.image(t)
	if after.row(t, "pl-reasoned").Revision.String() != "6" ||
		after.row(t, "pl-optional").Revision.String() != "5" ||
		len(after.Versions["pl-reasoned"]) != 3 || len(after.Versions["pl-optional"]) != 3 || len(after.Log) != 6 {
		t.Fatalf("defer/reopen history or revision sequence drifted: %+v", after)
	}
	wantReasoned := reasonedDoc
	wantReasoned.Fields["status"] = "queued"
	wantReasoned.Comments = append(wantReasoned.Comments, pipeline.Comment{Body: "upstream ready", Time: f.inputs.Now(), Principal: f.by})
	sameJSON(t, after.document(t, "pl-reasoned"), wantReasoned)
	wantOptional := optionalDoc
	wantOptional.Fields["status"] = "queued"
	sameJSON(t, after.document(t, "pl-optional"), wantOptional)
}

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

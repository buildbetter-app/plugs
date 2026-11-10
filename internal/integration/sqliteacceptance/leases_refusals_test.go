package sqliteacceptance_test

import (
	"bytes"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

func TestB20CloseReasonEventAcrossReopen(t *testing.T) {
	f := newFixture(t)
	ref, _, _ := f.create(t, "close reason event")
	f.commit(t, "bead.claim", writeParams(t, map[string]any{"id": ref}))
	params := writeParams(t, map[string]any{"id": ref, "reason": "complete"})
	closed, original := f.commit(t, "bead.close", params)
	if closed.Revision.String() != "3" || len(closed.Results) != 2 || original.Meta != nil {
		t.Fatalf("close did not commit patch and reason together: %+v", closed)
	}
	for _, result := range closed.Results {
		doc, err := pipeline.DecodeDocument(result.Record)
		if err != nil || doc.Fields["status"] != "done" || doc.Lease != nil || len(doc.Comments) != 1 || doc.Comments[0].Body != "complete" || !reflect.DeepEqual(doc.Comments[0].Principal, anonymous) || !doc.Comments[0].Time.Equal(f.now()) {
			t.Fatalf("close lost status, reason or claim settlement: %+v %v", doc, err)
		}
	}
	before := f.state(t)
	if len(before.Log) != 3 || len(before.Versions[ref.ID]) != 3 {
		t.Fatalf("close split the atomic transition: %+v", before)
	}
	entry := before.Log[2]
	if err := entry.ValidateEvents(); err != nil {
		t.Fatal(err)
	}
	if len(entry.Changes) != 1 || entry.Changes[0].Op != model.OpPatch || !slices.Contains(entry.Changes[0].ChangedPaths, "/fields/status") || !slices.Contains(entry.Changes[0].ChangedPaths, "/comments") || len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadClosed {
		t.Fatalf("byte-RPC close with a reason must persist bead.closed at revision 3/index 0: %+v", entry)
	}
	event := entry.Events[0]
	if event.Index.String() != "0" || event.ChangeIndex.String() != "0" || event.Resource.Ref.ID != ref.ID || event.Provenance.Method != "bead.close" || event.Provenance.Slot != "close" || event.Provenance.SetBy["comments"] != "request" || event.Details != nil {
		t.Fatalf("close event lost provenance or order: %+v", event)
	}
	f.reopen(t)
	requireSameState(t, before, f.state(t))
	_, replay := f.commit(t, "bead.close", params)
	if replay.Meta == nil || !replay.Meta.Replayed || !bytes.Equal(original.Result, replay.Result) {
		t.Fatal("close replay changed committed response bytes after SQLite reopen")
	}
	noop, _ := f.commit(t, "bead.close", writeParams(t, map[string]any{"id": ref}))
	if noop.Revision.String() != "3" {
		t.Fatalf("repeat close minted a revision: %+v", noop)
	}
	requireSameState(t, before, f.state(t))
	t.Log("B20 ordinary close-with-reason: one durable bead.closed at revision 3/index 0, atomic reason and claim settlement, exact replay and no-op across real SQLite reopen")
}

func TestB04B05LiveClaimOwnershipAcrossReopen(t *testing.T) {
	f := newFixture(t)
	ref, _, _ := f.create(t, "claim")
	params := writeParams(t, map[string]any{"id": ref})
	claim, originalReply := f.commit(t, "bead.claim", params)
	if claim.Revision.String() != "2" || claim.LeaseExpiresAt == nil || !claim.LeaseExpiresAt.Equal(f.now().Add(5*time.Minute)) || claim.LeaseTtlMs == nil || claim.LeaseTtlMs.String() != "300000" {
		t.Fatalf("claim did not return the durable five-minute lease: %+v", claim)
	}
	type claimView struct {
		Bead  bead `json:"bead"`
		Lease struct {
			Holder    string        `json:"holder"`
			ExpiresAt time.Time     `json:"expiresAt"`
			TTL       int64         `json:"ttlMs"`
			Epoch     model.Natural `json:"epoch"`
		} `json:"lease"`
	}
	var leaseResult claimView
	decodeResult(t, f.call(t, "bead.get", map[string]any{"id": ref}), &leaseResult)
	if leaseResult.Bead.Status != "in_progress" || leaseResult.Lease.Holder != anonymous.ID || !leaseResult.Lease.ExpiresAt.Equal(*claim.LeaseExpiresAt) || leaseResult.Lease.TTL != 300000 || leaseResult.Lease.Epoch.String() != model.Natural64(f.lease.Epoch()).String() {
		t.Fatalf("claim/get lease mismatch: %+v", leaseResult)
	}
	f.create(t, "advance head separately")
	f.clock.Add(int64(30 * time.Second))
	before := f.state(t)
	oldService, oldWriter, oldLease := f.domain, f.writer, f.lease
	f.reopen(t)
	requireSameState(t, before, f.state(t))
	select {
	case <-oldLease.Lost():
	default:
		t.Fatal("closed host retained a usable active lease")
	}
	_, err := oldService.Get(f.ctx, anonymous, service.GetRequest{ID: ref})
	domainCode(t, err, "busy")
	_, err = oldWriter.Apply(f.ctx, anonymous, model.ChangeSet{Ops: []model.Op{patchOp(ref, map[string]any{"title": "stale host"})}}, writeMeta(t))
	domainCode(t, err, "lease_not_held")
	_, replay := f.commit(t, "bead.claim", params)
	if replay.Meta == nil || !replay.Meta.Replayed || !bytes.Equal(replay.Result, originalReply.Result) {
		t.Fatal("claim replay changed expiry, identity or response after restart")
	}
	same, response := f.commit(t, "bead.claim", writeParams(t, map[string]any{"id": ref}))
	if same.Revision.String() != "3" || same.Results[0].Revision.String() != "2" || same.LeaseExpiresAt == nil || !same.LeaseExpiresAt.Equal(*claim.LeaseExpiresAt) || response.Meta != nil {
		t.Fatalf("same live owner refreshed the lease or minted a version: %+v", same)
	}
	for _, tc := range []struct {
		method, code string
		params       map[string]any
	}{
		{"bead.claim", "busy", writeParams(t, map[string]any{"id": ref})},
		{"bead.update", "lease_not_held", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "foreign label"}})},
	} {
		tc.params["_meta"].(map[string]any)["agent"] = "foreign"
		f.code(t, f.call(t, tc.method, tc.params), tc.code)
		if f.receipt(t, tc.method, "foreign", tc.params["_meta"].(map[string]any)["idempotencyKey"].(string)) != nil {
			t.Fatal("foreign owner refusal published a receipt")
		}
	}
	requireSameState(t, before, f.state(t))
	var after claimView
	decodeResult(t, f.call(t, "bead.get", map[string]any{"id": ref}), &after)
	// The claim epoch belongs to the preserved claim, not the reopened host.
	if !reflect.DeepEqual(after, leaseResult) {
		t.Fatalf("reopen/no-op changed the live claim: got=%+v want=%+v", after, leaseResult)
	}
	t.Log("B04/B05 live ownership only: anonymous agent scope, five-minute claim, exact retry and same-holder no-op survived reopen; stale host and foreign label refused without durable writes")
}

func TestUnsupportedCardRemainderRefusesWithoutWrites(t *testing.T) {
	f := newFixture(t)
	ref, _, _ := f.create(t, "unsupported controls")
	before := f.state(t)
	for _, method := range []string{
		"bead.heartbeat",
		"history.versions", "history.redact",
		"admin.restore.begin", "view.run",
	} {
		if _, exists := f.catalog.Descriptor(method); !exists {
			t.Fatalf("test names a method outside the actual wire catalog: %s", method)
		}
		f.code(t, f.call(t, method, map[string]any{}), "method_not_found")
	}
	// B12 now has real handlers. The ordinary host has neither a summary
	// provider nor a trusted presence verifier, so it must fail closed through
	// those handlers instead of presenting an obsolete method_not_found.
	for _, tc := range []struct {
		method, code string
		params       map[string]any
	}{
		{"admin.compact", "compaction_unavailable", writeParams(t, map[string]any{})},
		{"history.revert", "human_presence_required", writeParams(t, map[string]any{
			"ref": ref, "baseRevision": before.Meta.Head, "expect": []model.Precondition{},
			"humanPresence": map[string]any{"mode": "presence_check", "challenge": "unissued", "proof": "unissued"},
		})},
	} {
		f.code(t, f.call(t, tc.method, tc.params), tc.code)
		if f.receipt(t, tc.method, "owner", tc.params["_meta"].(map[string]any)["idempotencyKey"].(string)) != nil {
			t.Fatal("missing B12 host capability published a receipt")
		}
	}
	f.code(t, f.call(t, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "parented"}, "parent": model.RefByKey("unresolved-parent")})), "invalid_params")
	f.code(t, f.call(t, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "must not land", "status": "done"}})), "ontology_use_slot_verb")
	f.code(t, f.call(t, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "must not land", "unrecognized": true}})), "ontology_unknown_field")
	// The valid attachment and title patch must be discarded when the later
	// cross-project reparent refuses the entire ordered batch.
	_, err := f.writer.Apply(f.ctx, anonymous, model.ChangeSet{Ops: []model.Op{
		patchOp(ref, map[string]any{"title": "must not land"}),
		{Kind: model.OpAttach, Attach: &model.AttachOp{Target: ref, Attachment: model.Attachment{Kind: "link", URI: "https://example.invalid/evidence", MediaType: "text/plain", Summary: "unsupported"}}},
		{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: ref, Project: "other"}},
	}}, writeMeta(t))
	var refusal *model.Error
	if !errors.As(err, &refusal) || refusal.Data.Code != "invalid_params" || refusal.Message != "reparent cannot move a bead between projects" {
		t.Fatalf("want the late cross-project reparent refusal, got %T %v", err, err)
	}
	requireSameState(t, before, f.state(t))
	t.Log("unbound public heartbeat/remaining-history/restore/view methods, missing B12 host capabilities, malformed named parents and status/unknown-field writes explicitly refused; late cross-project reparent conserved state after staged title and valid attachment")
}

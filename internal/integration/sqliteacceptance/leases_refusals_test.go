package sqliteacceptance_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

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
		"tx.apply", "bead.heartbeat", "bead.reopen",
		"history.get", "history.versions", "history.revert", "history.redact",
		"admin.restore.begin", "view.run", "event.list",
	} {
		if _, exists := f.catalog.Descriptor(method); !exists {
			t.Fatalf("test names a method outside the actual wire catalog: %s", method)
		}
		f.code(t, f.call(t, method, map[string]any{}), "method_not_found")
	}
	f.code(t, f.call(t, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "parented"}, "parent": ref})), "invalid_params")
	f.code(t, f.call(t, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "must not land", "status": "done"}})), "ontology_use_slot_verb")
	f.code(t, f.call(t, "bead.update", writeParams(t, map[string]any{"id": ref, "fields": map[string]any{"title": "must not land", "unrecognized": true}})), "ontology_unknown_field")
	for _, op := range []model.Op{
		{Kind: model.OpAttach, Attach: &model.AttachOp{Target: ref, Attachment: model.Attachment{Kind: "link", URI: "https://example.invalid/evidence", MediaType: "text/plain", Summary: "unsupported"}}},
		{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: ref, Project: "other"}},
	} {
		_, err := f.writer.Apply(f.ctx, anonymous, model.ChangeSet{Ops: []model.Op{patchOp(ref, map[string]any{"title": "must not land"}), op}}, writeMeta(t))
		domainCode(t, err, "invalid_params")
	}
	requireSameState(t, before, f.state(t))
	t.Log("unbound public batch/heartbeat/reopen/history-mutation/restore/view/event methods and unsupported parent/attach/reparent/status/unknown-field writes explicitly refused")
}

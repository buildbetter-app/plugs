package e5acceptance_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

// This is one in-process memory component, not an E5 queue or database test.
func TestRealWASIOnOneAuthenticatedGuardedWriter(t *testing.T) {
	started := time.Now()
	f := newLocalFixture(t)
	originalHandler, originalChecker, originalHost := f.composition.Handler, f.composition.Checker, f.host
	originalProvider, originalLease := f.provider, f.lease
	originalKey := f.tokens["user:sponsor"]
	first := f.call(t, "user:sponsor", "first", 1)
	if first.Error != nil {
		t.Fatalf("allowed actual WASI RPC error code=%s", first.Error.Data.Code)
	}
	demandTitle(t, f.record(t), "FIRST|observed|read=1|base=1|call=1|refused=0")
	receipt := f.receipt(t, "user:sponsor", 1)
	demand(t, receipt != nil && !receipt.ResponsePending, "committed guest receipt missing")
	demand(t, sameJSON(t, receipt.Changeset.Principal, localAgent("user:sponsor")), "authenticated setter/principal changed")
	demand(t, len(receipt.Changeset.Mutators) == 1 && receipt.Changeset.Mutators[0] == bindingID, "actual guest binding absent from durable provenance")
	f.evidence["firstReceipt"] = receipt
	head := f.head(t)
	replay := f.call(t, "user:sponsor", "first", 1)
	demand(t, replay.Error == nil && replay.Meta.Replayed && bytes.Equal(replay.Result, first.Result) && f.head(t) == head, "idempotent replay changed response or head")
	denied := f.call(t, "user:outsider", "denied", 2)
	demand(t, denied.Error != nil && denied.Error.Data.Code == "forbidden" && f.head(t) == head && f.receipt(t, "user:outsider", 2) == nil, "denied agent published a write or receipt")
	second := f.call(t, "user:sponsor", "second", 3)
	demand(t, second.Error == nil, "second actual guest RPC failed")
	// The guest's retained call counter proves replay and prior denial did not
	// re-execute it. Its output also requires the actual separate-record read.
	demandTitle(t, f.record(t), "SECOND|observed|read=1|base=2|call=2|refused=0")
	beforeRefusal := f.record(t)
	head = f.head(t)
	refusal := f.call(t, "user:sponsor", "refuse-read", 4)
	demand(t, refusal.Error != nil && refusal.Error.Data.Code == "subscriber_unavailable", "undeclared guest read did not fail closed")
	demand(t, f.head(t) == head && sameJSON(t, beforeRefusal, f.record(t)) && f.receipt(t, "user:sponsor", 4) == nil, "guest read refusal published partial state")
	afterRefusal := f.call(t, "user:sponsor", "after-refusal", 5)
	demand(t, afterRefusal.Error == nil, "actual guest did not remain usable after read refusal")
	demandTitle(t, f.record(t), "AFTER-REFUSAL|observed|read=1|base=3|call=4|refused=1")
	finalReceipt := f.receipt(t, "user:sponsor", 5)
	demand(t, finalReceipt != nil, "final real guest commit receipt missing")
	revoked := f.base
	revoked.Grants = nil
	demand(t, f.facts.Replace(revoked) == nil, "current facts revocation failed")
	head = f.head(t)
	revokedReplay := f.call(t, "user:sponsor", "after-refusal", 5)
	demand(t, revokedReplay.Error != nil && revokedReplay.Error.Data.Code == "forbidden" && f.head(t) == head, "replay bypassed current adopted authorization")
	demand(t, f.composition.Handler == originalHandler && f.composition.Checker == originalChecker && f.host == originalHost && f.provider == originalProvider && f.lease == originalLease && f.tokens["user:sponsor"] == originalKey, "handler/checker/host/provider/lease/key lifetime changed")
	demand(t, sameJSON(t, finalReceipt, f.receipt(t, "user:sponsor", 5)), "revoked replay changed retained receipt")
	f.evidence["finalReceipt"] = finalReceipt
	f.evidence["guestCalls"] = 4
	f.evidence["guestReads"] = map[string]int{"allowed": 3, "refusedUndeclared": 1}
	demand(t, f.requests == 7, "unexpected component request count")
	f.evidence["authenticatedRPCRequests"] = f.requests
	f.evidence["successfulGuestCommits"] = 3
	f.evidence["replayedWithoutGuest"] = 1
	f.evidence["authorizationDenials"] = 2
	f.evidence["unchangedObjectLifetimes"] = true
	f.evidence["secondWriter"] = false
	f.evidence["databaseStarted"] = false
	f.evidence["queueAgents"] = 0
	f.evidence["publicWriteConflictRate"] = nil
	f.evidence["elapsedSeconds"] = time.Since(started).Seconds()
	encoded, err := json.MarshalIndent(f.evidence, "", "  ")
	demand(t, err == nil, "safe component receipt encode failed")
	writeEvidence(t, "component.json", encoded)
	t.Logf("actual source-built guest: commits=3 calls=4 scoped reads=3 refused reads=1; replay=1 authz denials=2; same handler/checker/host/lease/key; component elapsed=%.3fs; database/200-agent/lagging UNRUN", time.Since(started).Seconds())
}

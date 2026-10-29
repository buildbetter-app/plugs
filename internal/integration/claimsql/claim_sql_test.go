package claimsql_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSQLClaimRecoveryAtTheRPCBoundary(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)

		// A live holder can release its own claim, while a different asserted
		// anonymous agent cannot. Both calls use the real bead.release RPC.
		live := f.createAndClaim(t, 1, "alice")
		mustCode(t, dispatch(f.rpc, f.ctx, anonymous, "bead.release", params(map[string]any{"id": live}, writeKey(13), "bob")), "lease_not_held")
		liveRelease := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.release", params(map[string]any{"id": live, "reason": "alice paused"}, writeKey(14), "alice")))
		if liveRelease["revision"].(json.Number).String() != "3" {
			t.Fatalf("live release revision=%v", liveRelease["revision"])
		}
		f.assertBead(t, live, "todo", false)
		if !contains(f.readyIDs(t), live.ID) {
			t.Fatalf("live release did not restore ready membership: %v", f.readyIDs(t))
		}

		expired := f.createAndClaim(t, 2, "alice")
		f.advance(5 * time.Minute) // exact lease expiry is still inside grace.
		mustCode(t, dispatch(f.rpc, f.ctx, core, "bead.release", params(map[string]any{"id": expired}, writeKey(21), "")), "lease_expired")
		f.advance(10 * time.Minute) // exact expiry == now - 2*TTL is refused.
		mustCode(t, dispatch(f.rpc, f.ctx, core, "bead.release", params(map[string]any{"id": expired}, writeKey(22), "")), "lease_expired")
		mustCode(t, dispatch(f.rpc, f.ctx, anonymous, "bead.release", params(map[string]any{"id": expired}, writeKey(23), "alice")), "lease_expired")
		mustCode(t, dispatch(f.rpc, f.ctx, anonymous, "bead.release", params(map[string]any{"id": expired}, writeKey(24), "bob")), "lease_not_held")
		foreignBefore := f.state(t, "")
		if code := mustError(t, dispatch(f.rpc, f.ctx, anonymous, "bead.claim", params(map[string]any{"id": expired}, writeKey(25), "bob"))); code == "" {
			t.Fatal("foreign claim unexpectedly succeeded after expiry")
		}
		requireSameState(t, foreignBefore, f.state(t, ""))

		f.advance(time.Nanosecond) // strict expiry-before-cutoff now holds.
		recoveryKey := writeKey(26)
		recovery := mustResult(t, dispatch(f.rpc, f.ctx, core, "bead.release", params(map[string]any{"id": expired, "reason": "expired worker recovered"}, recoveryKey, "")))
		if recovery["revision"].(json.Number).String() != "6" {
			t.Fatalf("recovery revision=%v", recovery["revision"])
		}
		f.assertBead(t, expired, "todo", false)
		if !contains(f.readyIDs(t), expired.ID) {
			t.Fatalf("recovery did not restore ready membership: %v", f.readyIDs(t))
		}

		state := f.state(t, recoveryKey)
		if state.Receipt == nil {
			t.Fatal("recovery did not persist an idempotency receipt")
		}
		if len(state.Log) != 6 {
			t.Fatalf("unexpected SQL history length: %d", len(state.Log))
		}
		recovered := state.Log[len(state.Log)-1]
		if !reflect.DeepEqual(recovered.Principal, core) || len(recovered.Chain) != 0 || len(recovered.Events) != 1 || recovered.Events[0].Type != model.EventBeadLeaseReclaimed {
			t.Fatalf("recovery attribution=%+v", recovered)
		}
		event := recovered.Events[0]
		if event.Resource.Ref.ID != expired.ID || event.Provenance.Method != "bead.release" || event.Provenance.Cause.Root != recoveryKey || len(event.Provenance.Chain) != 0 {
			t.Fatalf("recovery event attribution=%+v", event)
		}
		if len(recovered.Changes) != 1 || recovered.Changes[0].Ref.ID != expired.ID || !contains(recovered.Changes[0].ChangedPaths, "/fields/status") || !contains(recovered.Changes[0].ChangedPaths, "/lease") || !contains(recovered.Changes[0].ChangedPaths, "/comments") {
			t.Fatalf("recovery change record=%+v", recovered.Changes)
		}
		versions := state.Versions[expired.ID]
		if len(versions) != 3 || versions[len(versions)-1].Revision.String() != "6" {
			t.Fatalf("recovery history versions=%+v", versions)
		}

		// The public history method still sees the claimed image at the prior
		// revision, while the current get/read sees the recovered image.
		priorRevision, err := state.Meta.Head.Uint64()
		if err != nil || priorRevision == 0 {
			t.Fatalf("invalid recovery history head: %s", state.Meta.Head)
		}
		history := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "history.query", params(map[string]any{
			"asOf":  map[string]any{"revision": model.Natural64(priorRevision - 1), "incarnation": f.inc},
			"limit": 100,
		}, "", "")))
		foundPrior := false
		for _, raw := range history["items"].([]any) {
			item := raw.(map[string]any)
			if item["id"] == expired.ID {
				foundPrior = item["status"] == "in_progress"
			}
		}
		if !foundPrior {
			t.Fatalf("history did not retain the pre-recovery claim: %+v", history)
		}

		// A new holder can claim the reopened bead. Replaying the original
		// recovery key after that change returns the exact durable response and
		// does not create another revision or event.
		bobClaim := mustResult(t, dispatch(f.rpc, f.ctx, anonymous, "bead.claim", params(map[string]any{"id": expired}, writeKey(27), "bob")))
		if bobClaim["revision"].(json.Number).String() != "7" {
			t.Fatalf("normal post-recovery claim revision=%v", bobClaim["revision"])
		}
		afterClaim := f.state(t, recoveryKey)
		firstBytes, _ := json.Marshal(recovery)
		replayed := dispatch(f.rpc, f.ctx, core, "bead.release", params(map[string]any{"id": expired, "reason": "expired worker recovered"}, recoveryKey, ""))
		replayResult := mustResult(t, replayed)
		replayBytes, _ := json.Marshal(replayResult)
		if !reflect.DeepEqual(recovery, replayResult) || string(firstBytes) != string(replayBytes) {
			t.Fatalf("recovery replay changed result: first=%+v replay=%+v", recovery, replayResult)
		}
		meta, ok := replayed.Envelope["_meta"].(map[string]any)
		if !ok || meta["replayed"] != true {
			t.Fatalf("missing RPC replay metadata: %s", replayed.Raw)
		}
		reuseBefore := f.state(t, recoveryKey)
		mustCode(t, dispatch(f.rpc, f.ctx, core, "bead.release", params(map[string]any{"id": expired, "reason": "different"}, recoveryKey, "")), "idempotency_key_reused")
		requireSameState(t, reuseBefore, f.state(t, recoveryKey))
		if afterClaim.Meta.Head != f.meta(t).Head {
			t.Fatalf("replay changed SQL head: before=%s after=%s", afterClaim.Meta.Head, f.meta(t).Head)
		}

		f.reopen(t)
		reopenedReplay := dispatch(f.rpc, f.ctx, core, "bead.release", params(map[string]any{"id": expired, "reason": "expired worker recovered"}, recoveryKey, ""))
		if got := mustResult(t, reopenedReplay); !reflect.DeepEqual(recovery, got) {
			t.Fatalf("reopen replay changed durable response: %+v", got)
		}
		if meta, ok := reopenedReplay.Envelope["_meta"].(map[string]any); !ok || meta["replayed"] != true {
			t.Fatalf("reopen replay missing metadata: %s", reopenedReplay.Raw)
		}
		if contains(f.readyIDs(t), expired.ID) {
			t.Fatal("claimed post-recovery bead remained ready")
		}
		t.Logf("%s exercised live/foreign/strict-expiry recovery, ready membership, event/history attribution, replay and close/reopen persistence", e.name)
	})
}

func TestSQLClaimRecoveryRollsBackOnRealSQLFailure(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		ref := f.createAndClaim(t, 3, "alice")
		f.advance(15*time.Minute + time.Nanosecond)
		key := writeKey(36)
		before := f.state(t, key)
		e.recoveryFault(t, true)
		failed := dispatch(f.rpc, f.ctx, core, "bead.release", params(map[string]any{"id": ref, "reason": "faulted recovery"}, key, ""))
		if mustError(t, failed) == "" {
			t.Fatal("SQL trigger did not fail recovery")
		}
		e.recoveryFault(t, false)
		requireSameState(t, before, f.state(t, key))
		if before.Receipt != nil {
			t.Fatal("faulted recovery unexpectedly persisted a receipt")
		}
		f.assertBead(t, ref, "in_progress", true)
		retry := mustResult(t, dispatch(f.rpc, f.ctx, core, "bead.release", params(map[string]any{"id": ref, "reason": "faulted recovery"}, key, "")))
		if retry["revision"].(json.Number).String() != "3" {
			t.Fatalf("retry revision=%v", retry["revision"])
		}
		f.assertBead(t, ref, "todo", false)
		if f.state(t, key).Receipt == nil {
			t.Fatal("successful retry did not persist an idempotency receipt")
		}
		t.Logf("%s rolled back the SQL recovery transaction at changeset publication and reused its key successfully", e.name)
	})
}

func TestSQLClaimRecoveryFencesAStaleHost(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		ref := f.createAndClaim(t, 4, "alice")
		f.advance(15*time.Minute + time.Nanosecond)

		oldStore, oldLease, oldRPC := f.store, f.lease, f.rpc
		if err := oldLease.Release(f.ctx); err != nil {
			t.Fatal(err)
		}
		nextStore, err := e.storage.Open(f.ctx, driver.Config{DSN: e.dsn})
		if err != nil {
			t.Fatal(err)
		}
		nextLease, err := nextStore.Lease(f.ctx, "claim-sql-takeover", driver.LeaseTTL)
		if err != nil {
			_ = nextStore.Close()
			t.Fatal(err)
		}
		incarnation := f.inc
		_, _, nextRPC := compose(t, nextStore, nextLease, f.now, incarnation)
		f.store, f.lease, f.rpc = nextStore, nextLease, nextRPC
		before := f.state(t, "")
		mustCode(t, dispatch(oldRPC, f.ctx, core, "bead.release", params(map[string]any{"id": ref}, writeKey(41), "")), "busy")
		requireSameState(t, before, f.state(t, ""))
		mustResult(t, dispatch(f.rpc, f.ctx, core, "bead.release", params(map[string]any{"id": ref, "reason": "takeover recovered"}, writeKey(42), "")))
		f.assertBead(t, ref, "todo", false)
		if err := oldStore.Close(); err != nil {
			t.Fatal(err)
		}
		f.lease = nextLease
		t.Logf("%s refused stale-host recovery after lease fencing; the active takeover host recovered the claim", e.name)
	})
}

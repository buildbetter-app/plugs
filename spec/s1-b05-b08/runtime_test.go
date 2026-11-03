package s1b05b08

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

func TestB05HeartbeatIsQuietReplayedAndReclaimableAcrossStores(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			id := f.create(t, "heartbeat")
			claim := f.call(t, f.ctx, "bead.claim", map[string]any{"id": id}, key("claim"), actor)
			if claim.Error != nil {
				t.Fatalf("claim refused: %s", claim.Error.Data.Code)
			}
			before := f.state(t, id)
			f.now = f.now.Add(10 * time.Second)
			first := f.call(t, f.ctx, "bead.heartbeat", map[string]any{"id": id}, key("heartbeat-a"), actor)
			if first.Error != nil {
				t.Fatalf("heartbeat refused: %s", first.Error.Data.Code)
			}
			var firstResult struct {
				LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
			}
			if err := json.Unmarshal(first.Result, &firstResult); err != nil {
				t.Fatal(err)
			}
			if !firstResult.LeaseExpiresAt.Equal(f.now.Add(5 * time.Minute)) {
				t.Fatalf("heartbeat expiry=%s", firstResult.LeaseExpiresAt)
			}
			after := f.state(t, id)
			if before.Head != after.Head || before.Versions != after.Versions || before.Changes != after.Changes || string(before.Record) != string(after.Record) {
				t.Fatalf("quiet heartbeat changed durable graph: before=%+v after=%+v", before, after)
			}
			if engine != "memory" {
				f.reopen(t)
			}
			// A retry with the same key is a response replay.  Keep the
			// controlled clock inside the one-second no-rearm window so the
			// next distinct request can prove the original expiry boundary.
			f.now = f.now.Add(500 * time.Millisecond)
			replay := f.call(t, f.ctx, "bead.heartbeat", map[string]any{"id": id}, key("heartbeat-a"), actor)
			if replay.Error != nil || replay.Meta["replayed"] != true {
				t.Fatalf("heartbeat replay=%s meta=%v", replay.Result, replay.Meta)
			}
			var replayResult struct {
				LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
			}
			if err := json.Unmarshal(replay.Result, &replayResult); err != nil || !replayResult.LeaseExpiresAt.Equal(firstResult.LeaseExpiresAt) {
				t.Fatalf("replay result=%s err=%v", replay.Result, err)
			}
			foreign := f.call(t, f.ctx, "bead.heartbeat", map[string]any{"id": id}, key("heartbeat-foreign"), "other-agent")
			if foreign.Error == nil || foreign.Error.Data.Code != "lease_not_held" {
				t.Fatalf("foreign heartbeat=%+v", foreign)
			}
			// Advance beyond the quiet overlay and prove expiry is typed.  No
			// intervening successful heartbeat is allowed to move the deadline.
			f.now = firstResult.LeaseExpiresAt.Add(time.Nanosecond)
			expired := f.call(t, f.ctx, "bead.heartbeat", map[string]any{"id": id}, key("heartbeat-expired"), actor)
			if expired.Error == nil || expired.Error.Data.Code != "lease_expired" {
				t.Fatalf("expired heartbeat=%+v", expired)
			}
			// Reclaim runs through the cataloged release method under trusted
			// system:core and is the only B05 path that mints graph history.
			f.now = f.now.Add(15*time.Minute + time.Nanosecond)
			reclaim := f.call(t, f.ctx, "bead.release", map[string]any{"id": id, "reason": "stale worker"}, key("reclaim"), actor)
			if reclaim.Error == nil {
				t.Fatalf("reclaim unexpectedly used anonymous principal")
			}
			coreCtx := rpc.WithPrincipal(f.ctx, model.PrincipalRef{Kind: "system", ID: "core"})
			reclaim = f.call(t, coreCtx, "bead.release", map[string]any{"id": id, "reason": "stale worker"}, key("reclaim"), actor)
			if reclaim.Error != nil {
				t.Fatalf("reclaim refused: %s", reclaim.Error.Data.Code)
			}
			var committed model.Commit
			if err := json.Unmarshal(reclaim.Result, &committed); err != nil || len(committed.Results) == 0 {
				t.Fatalf("reclaim result=%s err=%v", reclaim.Result, err)
			}
			row, err := f.service.Get(f.ctx, anonymous, service.GetRequest{ID: id})
			if err != nil {
				t.Fatal(err)
			}
			if row.Lease != nil || row.Bead.Status != "todo" {
				t.Fatalf("reclaimed row=%+v lease=%+v", row.Bead, row.Lease)
			}
			// A later claim under the same anonymous identity must use its new
			// durable deadline rather than a stale pre-reclaim overlay.
			f.now = f.now.Add(time.Second)
			reclaimClaim := f.call(t, f.ctx, "bead.claim", map[string]any{"id": id}, key("claim-after-reclaim"), actor)
			if reclaimClaim.Error != nil {
				t.Fatalf("claim after reclaim refused: %s", reclaimClaim.Error.Data.Code)
			}
			var claimedAgain model.Commit
			if err := json.Unmarshal(reclaimClaim.Result, &claimedAgain); err != nil || claimedAgain.LeaseExpiresAt == nil || !claimedAgain.LeaseExpiresAt.Equal(f.now.Add(5*time.Minute)) {
				t.Fatalf("claim after reclaim lease=%s err=%v", reclaimClaim.Result, err)
			}
		})
	}
}

func TestB06B07B08DependencyLifecycleEdges(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) { testDependencyLifecycleEdges(t, engine) })
	}
}

func testDependencyLifecycleEdges(t *testing.T, engine string) {
	f := newFixture(t, engine)
	parent := f.create(t, "parent")
	child := f.create(t, "child")
	// The explicit parent field drives close-child guards and the linked form
	// exercises the dependency vocabulary path independently.
	linked := f.call(t, f.ctx, "bead.link", map[string]any{"from": child, "to": parent, "kind": "parent-child"}, key("parent-link"), actor)
	if linked.Error != nil {
		t.Fatalf("parent-child link refused: %s", linked.Error.Data.Code)
	}
	// Re-adding the exact existing endpoints and type is an idempotent no-op.
	// Assert the stored edge itself, rather than accepting any response that
	// happens to be an error, so this proves B07 edge identity and history
	// suppression independently of the later close guards.
	beforeEdge := f.state(t, child)
	beforeDoc := f.document(t, child)
	same := f.call(t, f.ctx, "bead.link", map[string]any{"from": child, "to": parent, "kind": "parent-child"}, key("same-type"), actor)
	if same.Error != nil {
		t.Fatalf("same-type re-add refused: %s", same.Error.Data.Code)
	}
	if len(beforeDoc.Links) != 1 || beforeDoc.Links[0].To.ID != parent.ID || beforeDoc.Links[0].Kind != "parent-child" {
		t.Fatalf("seed edge missing before re-add: %+v", beforeDoc.Links)
	}
	afterEdge := f.state(t, child)
	afterDoc := f.document(t, child)
	if beforeEdge.Head != afterEdge.Head || beforeEdge.Versions != afterEdge.Versions || beforeEdge.Changes != afterEdge.Changes || string(beforeEdge.Record) != string(afterEdge.Record) {
		t.Fatalf("same-type re-add changed durable graph: before=%+v after=%+v", beforeEdge, afterEdge)
	}
	if len(afterDoc.Links) != 1 || afterDoc.Links[0].To.ID != parent.ID || afterDoc.Links[0].Kind != "parent-child" {
		t.Fatalf("same-type re-add changed edge image: %+v", afterDoc.Links)
	}
	blockedClose := f.call(t, f.ctx, "bead.close", map[string]any{"id": parent}, key("parent-close"), actor)
	if blockedClose.Error == nil || blockedClose.Error.Data.Code != "ontology_guard_failed" {
		t.Fatalf("open-child close=%+v", blockedClose)
	}
	custom := f.call(t, f.ctx, "bead.link", map[string]any{"from": parent, "to": child, "kind": "custom-edge"}, key("custom"), actor)
	if custom.Error != nil {
		t.Fatalf("custom edge refused: %s", custom.Error.Data.Code)
	}
	customSource := f.create(t, "custom-source")
	customTarget := f.create(t, "custom-target")
	customOnly := f.call(t, f.ctx, "bead.link", map[string]any{"from": customSource, "to": customTarget, "kind": "custom-edge"}, key("custom-only"), actor)
	if customOnly.Error != nil {
		t.Fatalf("standalone custom edge refused: %s", customOnly.Error.Data.Code)
	}
	customClose := f.call(t, f.ctx, "bead.close", map[string]any{"id": customSource}, key("custom-source-close"), actor)
	if customClose.Error != nil {
		t.Fatalf("custom edge incorrectly blocked close: %s", customClose.Error.Data.Code)
	}
	retype := f.call(t, f.ctx, "bead.link", map[string]any{"from": parent, "to": child, "kind": "blocks"}, key("retype"), actor)
	if retype.Error == nil || retype.Error.Data.Code != "ontology_guard_failed" {
		t.Fatalf("retype=%+v", retype)
	}
	cycleA := f.create(t, "cycle-a")
	cycleB := f.create(t, "cycle-b")
	firstCycle := f.call(t, f.ctx, "bead.link", map[string]any{"from": cycleA, "to": cycleB, "kind": "blocks"}, key("cycle-first"), actor)
	if firstCycle.Error != nil {
		t.Fatalf("cycle seed refused: %s", firstCycle.Error.Data.Code)
	}
	cycle := f.call(t, f.ctx, "bead.link", map[string]any{"from": cycleB, "to": cycleA, "kind": "blocks"}, key("cycle"), actor)
	if cycle.Error == nil || cycle.Error.Data.Code != "invalid_params" && cycle.Error.Data.Code != "ontology_guard_failed" {
		t.Fatalf("cycle=%+v", cycle)
	}
	if _, err := f.service.Close(f.ctx, anonymous, service.CloseRequest{ID: child}, model.WriteMeta{Project: project, IdempotencyKey: key("child-close"), Agent: actor, AgentLabel: actor, Method: "bead.close"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Close(f.ctx, anonymous, service.CloseRequest{ID: parent}, model.WriteMeta{Project: project, IdempotencyKey: key("parent-close-after"), Agent: actor, AgentLabel: actor, Method: "bead.close"}); err != nil {
		t.Fatal(err)
	}
}

package b04claimidentity_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Every prefix row matches the requested project/type and is genuinely ready.
// Only the installed caller's ref caveat makes it ineligible to win. Unlike a
// selector-excluded prefix, these rows consume the first actual queue page.
func TestB04PublicClaimNextWalksMatchingReadyFrontBeyondOnePage(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		const prefix = 1000
		created := time.Now().UTC()
		changes := make([]conformance.FixtureChange, 0, prefix+1)
		winner := ref("b04-front-target")
		for i := 0; i <= prefix; i++ {
			id, priority := fmt.Sprintf("b04-front-%04d", i), 0
			if i == prefix {
				id, priority = winner.ID, 1
			}
			body, err := json.Marshal(pipeline.Document{
				ID: id, Type: "issue", Project: poolA, CreatedAt: &created,
				Fields: map[string]any{"title": id, "description": "matching ready front", "status": "todo", "priority": priority},
			})
			if err != nil {
				t.Fatal(err)
			}
			changes = append(changes, conformance.FixtureChange{Ref: ref(id), Kind: "bead", Op: model.OpCreate, Body: body, ChangedPaths: []string{"fields"}, Graph: &conformance.FixtureGraphSource{}})
		}
		if _, err := conformance.Seed(f.ctx, f.store, f.lease,
			driver.ChangesetHeader{Time: created, Principal: model.PrincipalRef{Kind: "system", ID: "b04-ready-front-fixture"}}, changes); err != nil {
			t.Fatal(err)
		}
		by := f.issue("agent:front-tail", "user:pool-a", map[string]any{
			"actions":  []string{string(authz.Modify), "transition"},
			"projects": []model.Ref{ref(poolA)}, "refs": []model.Ref{winner},
		})
		selector := map[string]any{"filter": map[string]any{"types": []string{"issue"}}}
		// Observe the real unguarded queue source through its public read route.
		// The tail has lower priority, so the first page must be full without it.
		ready := f.request(f.bootstrap, credential{}, "bead.ready", poolA, f.key("front-observer"), "front-observer", map[string]any{"limit": 1000, "filter": selector["filter"]})
		var front struct{ Items []struct{ ID string } }
		if ready.code != "" || json.Unmarshal(ready.envelope["result"], &front) != nil || len(front.Items) != 1000 {
			t.Fatalf("matching ready front is not one full source page: %s", ready.raw)
		}
		for _, row := range front.Items {
			if row.ID == winner.ID {
				t.Fatal("tail unexpectedly entered the first matching ready page")
			}
		}
		key := f.key("matching-front-win")
		f.remember(by, "bead.claimNext", key, "front-tail")
		before := f.state()
		if len(before.Records) != prefix+4 {
			t.Fatalf("complete fixture census=%d want=%d", len(before.Records), prefix+4)
		}
		first := f.call(by, "bead.claimNext", poolA, key, "front-tail", selector)
		f.winner(before, by, "bead.claimNext", key, "front-tail", winner, first, true)
		before = f.state()
		f.call(by, "bead.claimNext", poolA, key, "changed-label", selector).replay(t, first)
		sameState(t, before, f.state())
		t.Logf("%s: %d matching ready rows precede the permitted hydrated winner; one version/event/receipt, denied rows conserved", e.name, prefix)
	})
}

// A separate real transaction retains the selected record lock throughout the
// public call. PostgreSQL must use its SKIP LOCKED path; memory enforces the same
// logical contract. SQLite serializes actual writers with BEGIN IMMEDIATE and
// is covered by the three-store matching-front and publication witnesses.
func TestB04PublicClaimNextSkipsHeldCandidateWithoutPublication(t *testing.T) {
	eachStoreNamed(t, []string{"memory", "postgres"}, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		by := f.issue("agent:held-front-worker", "user:pool-a", nil)
		selected, next := f.work["a1"], f.work["a2"]
		// Give the selected row a unique earlier priority, independent of IDs
		// or the request's deterministic equal-key rotation.
		f.request(f.bootstrap, credential{}, "bead.update", poolA, f.key("selected-priority"), "setup", map[string]any{"id": selected, "fields": map[string]any{"priority": 0}}).commit(t)
		ready := f.request(f.bootstrap, credential{}, "bead.ready", poolA, f.key("held-front-observer"), "setup", nil)
		var front struct{ Items []struct{ ID string } }
		if ready.code != "" || json.Unmarshal(ready.envelope["result"], &front) != nil || len(front.Items) != 2 || front.Items[0].ID != selected.ID {
			t.Fatalf("held candidate is not the first actual ready row: %s", ready.raw)
		}
		held, err := f.store.Write(f.ctx, f.lease)
		if err != nil {
			t.Fatal(err)
		}
		defer held.Close()
		states, err := held.Lock(f.ctx, driver.LockSet{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &selected}, Mode: driver.LockUpdate}})
		if err != nil || len(states) != 1 || !states[0].Present {
			t.Fatalf("actual selected record lock: states=%+v err=%v", states, err)
		}
		key := f.key("held-front-next")
		f.remember(by, "bead.claimNext", key, "held-front")
		before := f.state()
		first := f.call(by, "bead.claimNext", poolA, key, "held-front", nil)
		f.winner(before, by, "bead.claimNext", key, "held-front", next, first, true)
		// The holder still owns its real lock and sees its unchanged preimage.
		rows, err := held.Get(f.ctx, []model.Ref{selected})
		if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, before.record(t, selected).Body) {
			t.Fatalf("skipped held candidate changed: %v", err)
		}
		after := f.state()
		if err := held.Rollback(f.ctx); err != nil {
			t.Fatal(err)
		}
		sameState(t, after, f.state())
		// Releasing the real lock makes that untouched candidate claimable.
		secondKey := f.key("released-front-next")
		f.remember(by, "bead.claimNext", secondKey, "held-front")
		before = f.state()
		second := f.call(by, "bead.claimNext", poolA, secondKey, "held-front", nil)
		f.winner(before, by, "bead.claimNext", secondKey, "held-front", selected, second, true)
		t.Logf("%s: public claimNext passed a real held first candidate, conserved its row/history, and claimed it once after release", e.name)
	})
}

func TestB04PublicClaimNextLockedFrontNeverPublishesNull(t *testing.T) {
	eachStoreNamed(t, []string{"memory", "postgres"}, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		by := f.issue("agent:locked-front-worker", "user:pool-a", nil)
		selected := f.work["a1"]
		held, err := f.store.Write(f.ctx, f.lease)
		if err != nil {
			t.Fatal(err)
		}
		defer held.Close()
		states, err := held.Lock(f.ctx, driver.LockSet{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &selected}, Mode: driver.LockUpdate}})
		if err != nil || len(states) != 1 || !states[0].Present {
			t.Fatalf("actual selected record lock: states=%+v err=%v", states, err)
		}
		selector := map[string]any{"filter": map[string]any{"resources": []string{selected.ID}}}
		key := f.key("locked-front-refusal")
		f.refuse(by, "bead.claimNext", poolA, key, "locked-front", selector, "write_conflict")
		before := f.state()
		if err := held.Rollback(f.ctx); err != nil {
			t.Fatal(err)
		}
		sameState(t, before, f.state())
		first := f.call(by, "bead.claimNext", poolA, key, "locked-front", selector)
		f.winner(before, by, "bead.claimNext", key, "locked-front", selected, first, true)
		t.Logf("%s: locked nonempty front returns catalog write_conflict without null/receipt/history; released same key wins once", e.name)
	})
}

// The HTTP transport supplies a fresh trace ID after catalog mapping. The
// expected retryability comes from the canonical error catalog, never from a
// caller-provided domain flag. Exercise both public claim endpoints and the
// installed authentication/authorization chain on every real engine.
func TestB04PublicClaimErrorsCarryCatalogRetryabilityAndTraceID(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		owner := f.issue("agent:trace-owner", "user:pool-a", nil)
		foreign := f.issue("agent:trace-foreign", "user:pool-a", nil)

		// The core service returns a typed domain error before the HTTP
		// transport maps it through the catalog. Its TraceID must stay empty;
		// only the public envelope owns trace generation. Exercise both claim
		// entry points directly so this boundary cannot be inferred from the
		// transport response alone.
		_, rawClaimErr := f.app.Service.ClaimWithOutcome(f.ctx, owner.principal, service.ClaimRequest{}, model.WriteMeta{Method: "bead.claim", Project: poolA})
		assertRawCoreRefusal(t, rawClaimErr, "invalid_params")
		rawNext, rawNextErr := f.app.Service.ClaimNextWithOutcome(f.ctx, owner.principal, service.ClaimNextRequest{}, model.WriteMeta{Method: "bead.claimNext"})
		if rawNext != nil {
			t.Fatalf("raw claimNext refusal returned an outcome: %+v", rawNext)
		}
		assertRawCoreRefusal(t, rawNextErr, "project_required")

		key := f.key("trace-owner-win")
		before := f.state()
		first := f.call(owner, "bead.claim", poolA, key, "trace-owner", target(f.work["a1"]))
		f.winner(before, owner, "bead.claim", key, "trace-owner", f.work["a1"], first, true)

		f.refuse(foreign, "bead.claim", poolA, f.key("trace-busy"), "trace-foreign", target(f.work["a1"]), "busy")
		f.refuse(credential{}, "bead.claim", poolA, f.key("trace-unauthenticated"), "trace-anonymous", target(f.work["a2"]), "unauthenticated")
		f.refuse(owner, "bead.claimNext", poolA, f.key("trace-invalid"), "trace-owner", map[string]any{"limit": 1}, "invalid_params")
		f.refuse(foreign, "bead.claimNext", poolB, f.key("trace-forbidden"), "trace-foreign", nil, "forbidden")
		t.Logf("%s: claim and claimNext HTTP refusals carry catalog retryability and transport trace IDs", e.name)
	})
}

func assertRawCoreRefusal(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("raw core call unexpectedly succeeded; want %s", code)
	}
	var refusal *model.Error
	if !errors.As(err, &refusal) {
		t.Fatalf("raw core refusal is not a model.Error: %T %v", err, err)
	}
	if refusal.Data.Code != code {
		t.Fatalf("raw core refusal code=%q want=%q", refusal.Data.Code, code)
	}
	if refusal.Data.TraceID != "" {
		t.Fatalf("raw core refusal leaked transport trace ID %q", refusal.Data.TraceID)
	}
}

// The identity comes from a real issued bearer. Display spellings do not
// authenticate an alias, and are never normalized into a PrincipalRef ID.
func TestB04AuthenticatedOwnerSpellingsAndForeignIDs(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		owner := f.issue("agent:worker.primary", "user:pool-a", nil)
		secondKey := f.issue("agent:worker.primary", "user:pool-a", nil)
		id := f.work["a1"]
		key := f.key("owner-win")
		f.remember(owner, "bead.claim", key, "worker.primary")
		before := f.state()
		first := f.call(owner, "bead.claim", poolA, key, "worker.primary", target(id))
		claimed := f.winner(before, owner, "bead.claim", key, "worker.primary", id, first, true)

		for _, spelling := range []string{"worker__primary", "worker-primary", "worker/primary", "worker--primary", ""} {
			key := f.key("noop-" + spelling)
			scope := f.remember(secondKey, "bead.claim", key, spelling)
			before := f.state()
			got := f.call(secondKey, "bead.claim", poolA, key, spelling, target(id)).commit(t)
			sameClaimState(t, claimed, got)
			onlyNoopReceipt(t, before, f.state(), scope, got)
		}
		// The same canonical principal/key replays across display labels rather
		// than creating another receipt namespace or another claim mutation.
		f.remember(secondKey, "bead.claim", key, "worker__primary")
		before = f.state()
		f.call(secondKey, "bead.claim", poolA, key, "worker__primary", target(id)).replay(t, first)
		sameState(t, before, f.state())

		// These are different AUTHENTICATED IDs, including the superficially
		// similar spelling. No separator rewrite can grant another identity
		// the existing lease, even if its client label impersonates the owner.
		for _, foreignID := range []string{"agent:worker__primary", "agent:worker.secondary-3", "agent:worker--primary"} {
			foreign := f.issue(foreignID, "user:pool-a", nil)
			f.refuse(foreign, "bead.claim", poolA, f.key("foreign-"+foreignID), "worker.primary", target(id), "busy")
		}

		// Both the real authentication boundary and the current policy boundary
		// run before a receipt can convey a successful result.
		f.refuse(credential{}, "bead.claim", poolA, f.key("missing-bearer"), "worker.primary", target(id), "unauthenticated")
		otherSponsor := f.issue("agent:worker.primary", "user:pool-b", nil)
		f.refuse(otherSponsor, "bead.claim", poolA, key, "worker.primary", target(id), "forbidden")
		if !f.keys.Revoke(secondKey.key.ID) {
			t.Fatal("test bearer was not revoked")
		}
		f.refuse(secondKey, "bead.claim", poolA, key, "worker.primary", target(id), "unauthenticated")
		if e.name != "memory" {
			f.reopen()
			f.remember(owner, "bead.claim", key, "after-reopen")
			before = f.state()
			f.call(owner, "bead.claim", poolA, key, "after-reopen", target(id)).replay(t, first)
			sameState(t, before, f.state())
			noopKey := f.key("noop-after-reopen")
			scope := f.remember(owner, "bead.claim", noopKey, "worker__primary")
			before = f.state()
			got := f.call(owner, "bead.claim", poolA, noopKey, "worker__primary", target(id)).commit(t)
			sameClaimState(t, claimed, got)
			onlyNoopReceipt(t, before, f.state(), scope, got)
			foreign := f.issue("agent:worker__primary", "user:pool-a", nil)
			f.refuse(foreign, "bead.claim", poolA, f.key("foreign-after-reopen"), "worker.primary", target(id), "busy")
		}
		f.revokePoolA()
		f.refuse(owner, "bead.claim", poolA, key, "worker.primary", target(id), "forbidden")
		f.refuse(owner, "bead.claim", poolA, f.key("new-after-revocation"), "worker__primary", target(id), "forbidden")
		t.Logf("%s: authenticated spelling labels, quiet same-owner receipts, distinct foreign IDs, missing/revoked bearer and current-policy replay refusal", e.name)
	})
}

// This exercises installed team/project/caveat policy for pool work. It is
// intentionally not a replacement for Beads' exact claim.pools registry.
func TestB04InstalledPoolPolicyConfinement(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		workerA := f.issue("agent:pool-worker", "user:pool-a", nil)
		workerB := f.issue("agent:pool-worker-b", "user:pool-b", nil)
		bounded := f.issue("agent:pool-bounded", "user:pool-a", map[string]any{
			"actions":  []string{string(authz.Modify)},
			"projects": []model.Ref{ref(poolA)},
			"refs":     []model.Ref{f.work["a2"]},
		})
		for _, invalid := range []struct {
			name   string
			params map[string]any
		}{
			{"limit", map[string]any{"limit": 1}},
			{"offset", map[string]any{"offset": 1}},
			{"brief", map[string]any{"brief": true}},
			{"actor", map[string]any{"actor": "agent:pool-worker-b"}},
		} {
			f.refuse(workerA, "bead.claimNext", poolA, f.key("invalid-"+invalid.name), poolA, invalid.params, "invalid_params")
		}
		// The pool names differ only by separators. They are still exact,
		// distinct project bindings with distinct installed team grants.
		f.refuse(workerA, "bead.claim", poolB, f.key("foreign-pool-direct"), poolB, target(f.work["b1"]), "forbidden")
		f.refuse(workerB, "bead.claim", poolA, f.key("reverse-pool-direct"), poolA, target(f.work["a1"]), "forbidden")
		f.refuse(workerA, "bead.claimNext", poolB, f.key("foreign-pool-next"), poolB, nil, "forbidden")
		f.refuse(bounded, "bead.claim", poolA, f.key("outside-ref-caveat"), poolA, target(f.work["a1"]), "forbidden")

		key := f.key("bounded-next")
		f.remember(bounded, "bead.claimNext", key, poolB)
		before := f.state()
		selector := map[string]any{"filter": map[string]any{"resources": []string{f.work["a1"].ID, f.work["a2"].ID}}}
		first := f.call(bounded, "bead.claimNext", poolA, key, poolB, selector)
		f.winner(before, bounded, "bead.claimNext", key, poolB, f.work["a2"], first, true)
		if e.name != "memory" {
			f.reopen()
		}
		f.remember(bounded, "bead.claimNext", key, poolA)
		before = f.state()
		f.call(bounded, "bead.claimNext", poolA, key, poolA, selector).replay(t, first)
		sameState(t, before, f.state())
		f.refuse(bounded, "bead.claimNext", poolA, key, poolA, map[string]any{"filter": map[string]any{"resources": []string{f.work["a1"].ID}}}, "idempotency_key_reused")
		f.refuse(workerA, "bead.claim", poolB, f.key("foreign-after-reopen"), "pool__alpha", target(f.work["b1"]), "forbidden")
		// The other pool has real eligible work: a refusal was a policy result,
		// not an absent/closed row or an already-held lease.
		otherKey := f.key("other-pool-win")
		f.remember(workerB, "bead.claim", otherKey, poolA)
		before = f.state()
		other := f.call(workerB, "bead.claim", poolB, otherKey, poolA, target(f.work["b1"]))
		f.winner(before, workerB, "bead.claim", otherKey, poolA, f.work["b1"], other, true)
		f.revokePoolA()
		f.refuse(bounded, "bead.claimNext", poolA, key, poolA, selector, "forbidden")
		t.Logf("%s: installed exact team/project pool policy and project/ref caveats constrain direct/next winners, label changes and receipt replay", e.name)
	})
}

func TestB04AnonymousLabelsRemainUnverifiedExactScopes(t *testing.T) {
	eachStore(t, false, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		id := f.work["a1"]
		by := credential{}
		key := f.key("anonymous-win")
		f.remember(by, "bead.claim", key, "worker.primary")
		before := f.state()
		first := f.request(f.bootstrap, by, "bead.claim", poolA, key, "worker.primary", target(id))
		claimed := f.winner(before, by, "bead.claim", key, "worker.primary", id, first, false)
		for _, label := range []string{"worker__primary", "worker--primary", "worker.secondary-3"} {
			foreignKey := f.key("anonymous-foreign-" + label)
			scope := f.remember(by, "bead.claim", foreignKey, label)
			before := f.state()
			r := f.request(f.bootstrap, by, "bead.claim", poolA, foreignKey, label, target(id))
			if r.code != "busy" || len(r.envelope["result"]) != 0 {
				t.Fatalf("anonymous label was normalized or treated as a verified alias: %s", r.raw)
			}
			after := f.state()
			sameState(t, before, after)
			if after.Receipts[scope] != nil {
				t.Fatal("foreign anonymous label persisted a successful receipt")
			}
		}
		if e.name != "memory" {
			f.reopen()
		}
		// Any local process may assert the same label. This succeeds under the
		// one anonymous principal and demonstrates P60's absence of isolation.
		noopKey := f.key("anonymous-same-label")
		scope := f.remember(by, "bead.claim", noopKey, "worker.primary")
		before = f.state()
		got := f.request(f.bootstrap, by, "bead.claim", poolA, noopKey, "worker.primary", target(id)).commit(t)
		sameClaimState(t, claimed, got)
		onlyNoopReceipt(t, before, f.state(), scope, got)
		before = f.state()
		f.request(f.bootstrap, by, "bead.claim", poolA, key, "worker.primary", target(id)).replay(t, first)
		sameState(t, before, f.state())
		doc, err := pipeline.DecodeDocument(f.state().record(t, id).Body)
		if err != nil || doc.Lease.Principal.Kind != "anonymous" || doc.Lease.Principal.ID != "anonymous" {
			t.Fatal("anonymous label manufactured an authenticated principal")
		}
		t.Logf("%s: P60 labels remain exact unverified lease/receipt scopes under one anonymous principal", e.name)
	})
}

func TestB04SQLClaimPublicationFailureConservesState(t *testing.T) {
	eachStore(t, true, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		owner := f.issue("agent:sql-claim-worker", "user:pool-a", nil)
		id := f.work["a1"]
		key := f.key("sql-failed-claim")
		scope := f.remember(owner, "bead.claim", key, "sql-worker")
		before := f.state()
		e.publicationFault(t, true)
		failed := f.call(owner, "bead.claim", poolA, key, "sql-worker", target(id))
		e.publicationFault(t, false)
		if failed.code != "internal_error" || failed.status != http.StatusOK || len(failed.envelope["result"]) != 0 {
			t.Fatalf("real SQL publication failure must refuse the write: %s", failed.raw)
		}
		after := f.state()
		sameState(t, before, after)
		if after.Receipts[scope] != nil || bytes.Contains(after.record(t, id).Body, []byte(`"lease"`)) {
			t.Fatal("failed SQL claim left a lease or successful receipt")
		}
		first := f.call(owner, "bead.claim", poolA, key, "sql-worker", target(id))
		f.winner(before, owner, "bead.claim", key, "sql-worker", id, first, true)
		f.reopen()
		before = f.state()
		f.call(owner, "bead.claim", poolA, key, "sql-worker", target(id)).replay(t, first)
		sameState(t, before, f.state())
		t.Logf("%s: SQL publication trigger rolls back every observed claim component, same-key retry wins once, and reopen preserves replay", e.name)
	})
}

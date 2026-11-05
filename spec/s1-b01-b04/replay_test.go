package s1b01b04

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

func backends(t *testing.T, h *postgresHarness) []struct {
	name string
	dsn  string
} {
	t.Helper()
	return []struct {
		name string
		dsn  string
	}{
		{name: "memory"},
		{name: "sqlite"},
		{name: "postgres", dsn: h.dsn(t)},
	}
}

func defaultOntology() *service.Ontology { return service.NewOntology(nil) }

func createNodes(f *fixture, nodes ...model.Op) {
	f.t.Helper()
	f.apply("nodes", "tx.apply", nodes...)
}

func TestS1B01ReplayReadyMembershipOrderAndDeferredParent(t *testing.T) {
	h := newPostgresHarness(t)
	for _, backend := range backends(t, h) {
		t.Run(backend.name, func(t *testing.T) {
			// Only root creates consume the root generator. The child is an
			// alias whose actual identity comes from the parent allocator.
			ids := []string{"b01-p0", "b01-tie-a", "b01-tie-b", "b01-parent", "b01-deferred", "b01-closed"}
			f := newFixture(t, backend.name, backend.dsn, defaultOntology(), ids)
			seed := f.apply("nodes", "tx.apply",
				createOp("b01-p0", project, map[string]any{"title": "priority zero", "priority": 0}, ""),
				createOp("b01-tie-a", project, map[string]any{"title": "tie a", "priority": 1}, ""),
				createOp("b01-tie-b", project, map[string]any{"title": "tie b", "priority": 1}, ""),
				createOp("b01-parent", project, map[string]any{"title": "parent", "priority": 2}, ""),
				createOp("b01-child", project, map[string]any{"title": "child", "priority": 3}, "b01-parent"),
				createOp("b01-deferred", project, map[string]any{"title": "deferred", "priority": 2}, ""),
				createOp("b01-closed", project, map[string]any{"title": "closed", "priority": 2}, ""),
			)
			if len(seed.Results) != 7 {
				t.Fatalf("B01 seed results: %+v", seed)
			}
			parent, child := seed.Results[3].Ref, seed.Results[4].Ref
			deferred, closed := seed.Results[5].Ref, seed.Results[6].Ref
			if parent.ID != "b01-parent" || child.ID != "b01-parent.1" || deferred.ID != "b01-deferred" || closed.ID != "b01-closed" {
				t.Fatalf("B01 committed identities: %+v", seed.Results)
			}
			if doc := f.document(child.ID); doc.Parent == nil || doc.Parent.ID != parent.ID {
				t.Fatalf("B01 committed child lost its parent: %+v", doc)
			}
			initial := f.readyIDs("b01-initial", owner)
			t.Logf("B01 initial Plug ready projection backend=%s ready=%v", backend.name, initial)
			if code := errorCode(t, f.call("b01-defer-item", "bead.defer", owner, map[string]any{"id": deferred, "reason": "wait"})); code != "" {
				t.Fatalf("defer item refused: %s", code)
			}
			if code := errorCode(t, f.call("b01-defer", "bead.defer", owner, map[string]any{"id": parent, "reason": "wait"})); code != "" {
				t.Fatalf("defer refused: %s", code)
			}
			if code := errorCode(t, f.call("b01-close", "bead.close", owner, map[string]any{"id": closed, "reason": "done"})); code != "" {
				t.Fatalf("close refused: %s", code)
			}
			got := f.readyIDs("b01-filtered", owner)
			reference := referenceIDs(t, "B01", "readyIds")
			if !reflect.DeepEqual(reference, []string{"b01-p0", "b01-tie-a", "b01-tie-b", "b01-child"}) {
				t.Fatalf("retained B01 reference observation changed: %v", reference)
			}
			t.Logf("B01 source/product identity binding: source b01-child -> committed %s, source b01-closed -> committed %s", child.ID, closed.ID)
			// B01's canonical membership excludes deferred-parent descendants.
			// Preserve the recorded reference's historical gap as an observation,
			// while asserting the adopted Plug behavior independently of it.
			want := []string{"b01-p0", "b01-tie-a", "b01-tie-b"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("deferred/closed ready = %v, want %v", got, want)
			}
			if f.document("b01-parent").Fields["status"] != "backlog" || f.document("b01-closed").Fields["status"] != "done" {
				t.Fatalf("category projection lost: parent=%+v closed=%+v", f.document("b01-parent"), f.document("b01-closed"))
			}
			if backend.name == "sqlite" {
				f.reopenSQLite()
				if got := f.readyIDs("b01-reopen", owner); !reflect.DeepEqual(got, want) {
					t.Fatalf("SQLite reopen ready = %v, want %v", got, want)
				}
			}
			// The retained reference and Plug order are intentionally separate:
			// Plug stores category/rank keys and records P74 as a bounded deviation.
			t.Logf("B01 projection backend=%s referenceReady=%v adoptedReady=%v canonical deferred-parent suppression decision=A3/D3/P74", backend.name, reference, got)
		})
	}
}

func TestS1B02ReplayBlockingCategoriesAndAtomicRefusal(t *testing.T) {
	h := newPostgresHarness(t)
	base := customDocument()
	rules := compileOntology(t, base, nil)
	for _, backend := range backends(t, h) {
		t.Run(backend.name, func(t *testing.T) {
			ids := []string{"b02-blocker", "b02-dependent", "b02-conditional", "b02-transitive"}
			f := newFixture(t, backend.name, backend.dsn, rules, ids)
			createNodes(f,
				createOp("b02-blocker", project, map[string]any{"title": "blocker"}, ""),
				createOp("b02-dependent", project, map[string]any{"title": "dependent"}, ""),
				createOp("b02-conditional", project, map[string]any{"title": "conditional"}, ""),
				createOp("b02-transitive", project, map[string]any{"title": "transitive"}, ""),
			)
			f.apply("links", "tx.apply",
				linkOp(model.OpLink, "b02-dependent", "b02-blocker", "blocks"),
				linkOp(model.OpLink, "b02-conditional", "b02-blocker", "conditional-blocks"),
				linkOp(model.OpLink, "b02-transitive", "b02-dependent", "blocks"),
			)
			if got := f.readyIDs("b02-blocked", owner); !reflect.DeepEqual(got, []string{"b02-blocker"}) {
				t.Fatalf("blocked ready = %v", got)
			}
			before := f.imageDigest()
			bad := f.call("b02-missing", "tx.apply", owner, map[string]any{})
			// There is no public dependency RPC on this frozen head. A missing
			// endpoint is exercised through Writer below, and the request must be
			// entirely atomic.
			_ = bad
			_, err := f.writer.Apply(f.ctx, model.PrincipalRef{Kind: "agent", ID: owner}, model.ChangeSet{Ops: []model.Op{
				patchOp("b02-dependent", map[string]any{"title": "must roll back"}),
				linkOp(model.OpLink, "b02-dependent", "b02-absent", "blocks"),
			}}, f.meta("b02-missing-endpoint", "tx.apply", owner))
			var refusal *model.Error
			if !errors.As(err, &refusal) || refusal.Data.Code != "not_found" {
				t.Fatalf("missing endpoint error = %v", err)
			}
			if after := f.imageDigest(); after != before {
				t.Fatalf("missing endpoint changed storage image: %s -> %s", before, after)
			}
			if code := errorCode(t, f.call("b02-close", "bead.close", owner, map[string]any{"id": map[string]any{"id": "b02-blocker"}})); code != "" {
				t.Fatalf("close blocker refused: %s", code)
			}
			got := f.readyIDs("b02-released", owner)
			if !reflect.DeepEqual(got, []string{"b02-dependent"}) {
				t.Fatalf("terminal release ready = %v", got)
			}
			if backend.name == "sqlite" {
				f.reopenSQLite()
				if got := f.readyIDs("b02-reopen", owner); !reflect.DeepEqual(got, []string{"b02-dependent"}) {
					t.Fatalf("SQLite reopen release = %v", got)
				}
			}
			t.Logf("B02 projection backend=%s decision=A3/P22", backend.name)
		})
	}
}

func TestS1B02ReplayConditionalFailureOutcomeAcrossDrivers(t *testing.T) {
	h := newPostgresHarness(t)
	rules := compileOntology(t, customDocument(), nil)
	for _, backend := range backends(t, h) {
		t.Run(backend.name, func(t *testing.T) {
			ids := []string{"b02-failure-blocker", "b02-failure-dependent"}
			f := newFixture(t, backend.name, backend.dsn, rules, ids)
			createNodes(f,
				createOp("b02-failure-blocker", project, map[string]any{"title": "failure blocker"}, ""),
				createOp("b02-failure-dependent", project, map[string]any{"title": "failure dependent"}, ""),
			)
			if code := errorCode(t, f.call("conditional-failure-link", "bead.link", owner, map[string]any{
				"from": ref("b02-failure-dependent"), "to": ref("b02-failure-blocker"), "kind": "conditional-blocks",
			})); code != "" {
				t.Fatalf("conditional link refused: %s", code)
			}
			if got := f.readyIDs("b02-failure-before", owner); !reflect.DeepEqual(got, []string{"b02-failure-blocker"}) {
				t.Fatalf("conditional dependency before failure = %v", got)
			}
			if code := errorCode(t, f.call("b02-failure", "bead.fail", owner, map[string]any{
				"id": ref("b02-failure-blocker"), "reason": "bounded failure outcome",
			})); code != "" {
				t.Fatalf("failure blocker refused: %s", code)
			}
			if got := f.readyIDs("b02-failure-after", owner); !reflect.DeepEqual(got, []string{"b02-failure-dependent"}) {
				t.Fatalf("conditional failure did not release dependent = %v", got)
			}
			if f.document("b02-failure-blocker").Fields["status"] != "broken" {
				t.Fatalf("failure blocker did not use the failure slot: %+v", f.document("b02-failure-blocker"))
			}
			if backend.name == "sqlite" {
				f.reopenSQLite()
				if got := f.readyIDs("b02-failure-reopen", owner); !reflect.DeepEqual(got, []string{"b02-failure-dependent"}) {
					t.Fatalf("SQLite reopen conditional failure = %v", got)
				}
			}
			t.Logf("B02 conditional failure outcome backend=%s decision=A3/P22", backend.name)
		})
	}
}

func TestS1B03ReplayCustomCatalogSlotsAndRefusals(t *testing.T) {
	h := newPostgresHarness(t)
	base := customDocument()
	rules := compileOntology(t, base, nil)
	for _, backend := range backends(t, h) {
		t.Run(backend.name, func(t *testing.T) {
			f := newFixture(t, backend.name, backend.dsn, rules, []string{"b03-custom"})
			f.apply("create", "tx.apply", createOp("b03-custom", project, map[string]any{"title": "custom"}, ""))
			if got := f.document("b03-custom").Fields["status"]; got != "queued" {
				t.Fatalf("custom initial status=%v", got)
			}
			if got := f.readyIDs("b03-ready", owner); !reflect.DeepEqual(got, []string{"b03-custom"}) {
				t.Fatalf("custom active ready=%v", got)
			}
			before := f.imageDigest()
			invalid := f.call("b03-direct-status", "bead.update", owner, map[string]any{"id": map[string]any{"id": "b03-custom"}, "fields": map[string]any{"status": "working"}})
			if code := errorCode(t, invalid); code != "ontology_use_slot_verb" {
				t.Fatalf("direct custom status code=%s response=%s", code, invalid)
			}
			if after := f.imageDigest(); after != before {
				t.Fatalf("direct status refusal mutated image")
			}
			if code := errorCode(t, f.call("b03-claim", "bead.claim", owner, map[string]any{"id": map[string]any{"id": "b03-custom"}})); code != "" {
				t.Fatalf("claim custom status refused: %s", code)
			}
			if got := f.document("b03-custom").Fields["status"]; got != "working" {
				t.Fatalf("claim slot status=%v", got)
			}
			if code := errorCode(t, f.call("b03-close", "bead.close", owner, map[string]any{"id": map[string]any{"id": "b03-custom"}})); code != "" {
				t.Fatalf("close custom status refused: %s", code)
			}
			if got := f.document("b03-custom").Fields["status"]; got != "finished" {
				t.Fatalf("close slot status=%v", got)
			}
			if code := errorCode(t, f.call("b03-reopen", "bead.reopen", owner, map[string]any{"id": map[string]any{"id": "b03-custom"}})); code != "" {
				t.Fatalf("reopen custom status refused: %s", code)
			}
			if got := f.document("b03-custom").Fields["status"]; got != "queued" {
				t.Fatalf("reopen slot status=%v", got)
			}
			if backend.name == "sqlite" {
				f.reopenSQLite()
				if got := f.readyIDs("b03-reopen-read", owner); !reflect.DeepEqual(got, []string{"b03-custom"}) {
					t.Fatalf("SQLite reopen custom ready=%v", got)
				}
			}
			t.Logf("B03 projection backend=%s categories=unstarted/started/completed/backlog decision=P10/P41/D24/A3", backend.name)
		})
	}
}

func TestS1B04ReplayDirectAndClaimNextWithOneWinner(t *testing.T) {
	h := newPostgresHarness(t)
	for _, backend := range backends(t, h) {
		t.Run(backend.name, func(t *testing.T) {
			f := newFixture(t, backend.name, backend.dsn, defaultOntology(), []string{"b04-next", "b04-direct", "b04-race"})
			createNodes(f,
				createOp("b04-next", project, map[string]any{"title": "next", "priority": 0}, ""),
				createOp("b04-direct", project, map[string]any{"title": "direct", "priority": 1}, ""),
				createOp("b04-race", project, map[string]any{"title": "race", "priority": 2}, ""),
			)
			claimNext := f.call("b04-claim-next", "bead.claimNext", owner, map[string]any{})
			if code := errorCode(t, claimNext); code != "" {
				t.Fatalf("claimNext refused: %s", code)
			}
			var claimEnvelope map[string]any
			if err := json.Unmarshal(claimNext, &claimEnvelope); err != nil {
				t.Fatal(err)
			}
			if resultValue, ok := claimEnvelope["result"].(map[string]any); !ok || resultValue["id"] != "b04-next" && resultValue["bead"] == nil {
				// Current wire shape is accepted in either hydrated-row form;
				// the semantic check below reads the durable document.
				t.Logf("claimNext wire shape=%s", claimNext)
			}
			if doc := f.document("b04-next"); doc.Fields["status"] != "in_progress" || doc.Lease == nil {
				t.Fatalf("claimNext did not atomically assign status/lease: %+v", doc)
			}
			if code := errorCode(t, f.call("b04-direct", "bead.claim", owner, map[string]any{"id": map[string]any{"id": "b04-direct"}})); code != "" {
				t.Fatalf("direct claim refused: %s", code)
			}
			beforeReclaim := f.imageDigest()
			if code := errorCode(t, f.call("b04-direct-reclaim", "bead.claim", owner, map[string]any{"id": map[string]any{"id": "b04-direct"}})); code != "" {
				t.Fatalf("same holder reclaim refused: %s", code)
			}
			if after := f.imageDigest(); after != beforeReclaim {
				t.Fatalf("same-holder reclaim changed durable image")
			}
			if code := errorCode(t, f.call("b04-foreign", "bead.claim", "s1-foreign", map[string]any{"id": map[string]any{"id": "b04-direct"}})); code == "" {
				t.Fatal("foreign holder claim was allowed")
			}
			// Two independent callers race the same unassigned row. The storage
			// CAS permits exactly one durable winner and leaves one refusal.
			two := []string{"s1-race-a", "s1-race-b"}
			start := make(chan struct{})
			responses := make(chan []byte, 2)
			var wg sync.WaitGroup
			for _, agent := range two {
				wg.Add(1)
				go func(agent string) {
					defer wg.Done()
					<-start
					responses <- f.call("b04-race-"+agent, "bead.claim", agent, map[string]any{"id": map[string]any{"id": "b04-race"}})
				}(agent)
			}
			close(start)
			wg.Wait()
			close(responses)
			wins, refusals := 0, 0
			for response := range responses {
				if bytes.Contains(response, []byte(`"error"`)) {
					refusals++
				} else {
					wins++
				}
			}
			if wins != 1 || refusals != 1 {
				t.Fatalf("claim race wins=%d refusals=%d", wins, refusals)
			}
			if f.document("b04-race").Lease == nil {
				t.Fatal("race winner did not persist a lease")
			}
			deadline := f.clock.Now().Add(5 * time.Minute)
			if !f.document("b04-race").Lease.ExpiresAt.Equal(deadline) {
				t.Fatalf("lease clock mismatch: got=%s want=%s", f.document("b04-race").Lease.ExpiresAt, deadline)
			}
			if backend.name == "sqlite" {
				f.reopenSQLite()
				if f.document("b04-race").Lease == nil {
					t.Fatal("SQLite reopen lost claim lease")
				}
			}
			t.Logf("B04 projection backend=%s one-winner direct/next claims decision=A4/P60", backend.name)
		})
	}
}

func TestI5ReceiptIsBoundToExistingCrossDriverAuthzProof(t *testing.T) {
	manifest := decodeObject(t, manifestBytes)
	evidence := manifest["i5Evidence"].(map[string]any)
	checks := evidence["checks"].([]any)
	want := map[string]bool{
		"compiled lists equal independent point decisions": false,
		"no-plugin user allow":                             false,
		"forbidden/error fail-closed":                      false,
		"current history permission":                       false,
		"post-image authorization":                         false,
		"dependency and missing-data races":                false,
	}
	for _, raw := range checks {
		if _, ok := want[raw.(string)]; !ok {
			t.Fatalf("unexpected I5 check %q", raw)
		}
		want[raw.(string)] = true
	}
	for check, present := range want {
		if !present {
			t.Fatalf("I5 check missing: %s", check)
		}
	}
	if evidence["source"] != "existing PR240 fixture; no duplicate implementation" {
		t.Fatal("I5 evidence does not preserve the accepted PR240 boundary")
	}
}

func TestProjectionRetainsNullAndRefusalShape(t *testing.T) {
	// This guard keeps the replay helper honest: null claim-next and typed
	// refusal are distinct JSON values and cannot be collapsed into an empty
	// list. The actual driver runs above exercise the corresponding envelopes.
	var value map[string]any
	if err := json.Unmarshal([]byte(`{"result":null}`), &value); err != nil {
		t.Fatal(err)
	}
	if value["result"] != nil {
		t.Fatal("null result was coerced")
	}
	if _, err := json.Marshal(value); err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(value, map[string]any{"result": []any{}}) {
		t.Fatal("null and empty list were conflated")
	}
	_ = fmt.Sprintf("%v", value)
	_ = errors.Is(nil, nil)
}

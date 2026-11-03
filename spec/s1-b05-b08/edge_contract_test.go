package s1b05b08

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Contract authorities: behaviors/work.md B07 (edge identity, mixed editor,
// vocabulary, unresolved cross-prefix targets); census.md dependency rows
// (typed external/workflow targets and thread preservation); D25/P22/P45/P48.
// OpenRPC LinkOp/LinkMetadata additionally pins omission versus explicit {}.
// These are authored public-runtime tests, never native reference recordings.
// Durable cross-plane writes remain refused at the named/tx.apply boundary;
// valid unresolved external/cross-prefix targets are retained as edge values.

func edgeLinkOp(from, to model.Ref, kind string) model.Op {
	return model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: kind}}
}

func edgePatchOp(ref model.Ref, title string) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": title}}}
}

func edgeRequireSuccess(t *testing.T, result envelope) {
	t.Helper()
	if result.Error != nil {
		t.Fatalf("public write refused: %+v", result)
	}
}

func edgeRequireRefusal(t *testing.T, result envelope, code string) {
	t.Helper()
	if result.Error == nil || result.Error.Data.Code != code {
		t.Fatalf("public refusal=%+v want=%s", result, code)
	}
}

func edgeReadyContains(t *testing.T, e *edgeContractFixture, id string, want bool) {
	t.Helper()
	got := e.f.call(t, e.f.ctx, "bead.ready", map[string]any{"limit": 1000}, "", actor)
	if got.Error != nil {
		t.Fatalf("public ready refused: %+v", got)
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		NextCursor string `json:"nextCursor"`
	}
	if err := json.Unmarshal(got.Result, &page); err != nil || page.NextCursor != "" {
		t.Fatalf("ready was not a complete membership observation: %s error=%v", got.Result, err)
	}
	found := false
	for _, item := range page.Items {
		if item.ID == id {
			found = true
			break
		}
	}
	if found != want {
		t.Fatalf("ready membership for %s=%t want=%t items=%+v", id, found, want, page.Items)
	}
}

func TestB07GuardedDurableEdgeBatchSuccessAndReplayConserveAllState(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			e := newEdgeContractFixture(t, engine)
			from := e.create(t, "batch-source")
			other := e.create(t, "batch-other-source")
			to := e.create(t, "batch-read-only-target")
			neighbor := e.create(t, "batch-unrelated-neighbor")
			seed := e.probe("bead.link", "batch-seed")
			edgeRequireSuccess(t, e.call(t, seed, map[string]any{"from": other, "to": to, "kind": "discovered-from"}))
			p := e.probe("tx.apply", "batch-success")
			before := e.snapshot(t)
			fromRev, otherRev := before.Records[edgeRecordKey(from)].Revision, before.Records[edgeRecordKey(other)].Revision
			cs := model.ChangeSet{
				Expect: []model.Precondition{{Ref: from, Revision: &fromRev}, {Ref: other, Revision: &otherRev}},
				Ops: []model.Op{
					edgePatchOp(from, "committed with edges"), edgeLinkOp(from, to, "blocks"),
					edgeLinkOp(other, from, "relates-to"),
					{Kind: model.OpUnlink, Link: &model.LinkOp{From: other, To: to, LinkKind: "discovered-from"}},
				},
			}
			params := map[string]any{"changeSet": cs}
			first := e.call(t, p, params)
			edgeRequireSuccess(t, first)
			after := e.snapshot(t)
			commit := edgeRequireReceipt(t, after, p, first.Result, false)
			edgeRequireSingleCommit(t, before, after, commit, from, other)
			edgeRequireLinks(t, after, from, edgeLinkValue(to, "blocks"))
			edgeRequireLinks(t, after, other, edgeLinkValue(from, "relates-to"))
			edgeRequireOnlyEdgeAndFields(t, before, after, from, map[string]any{"title": "committed with edges"})
			edgeRequireOnlyEdgeAndFields(t, before, after, other, nil)
			edgeRequirePublicReady(t, e, other.ID, to.ID, neighbor.ID)
			// Inspect the real historical reader at the captured pre-batch head.
			r, err := e.f.store.Read(e.f.ctx, driver.View{Incarnation: before.Meta.Incarnation, At: &before.Meta.Head})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := r.Get(e.f.ctx, e.refs)
			closeErr := r.Close()
			if err != nil || closeErr != nil || len(rows) != len(before.Records) {
				t.Fatalf("historical source image unavailable: rows=%d error=%v close=%v", len(rows), err, closeErr)
			}
			seen := map[string]bool{}
			for _, row := range rows {
				id := edgeRecordKey(row.Ref)
				want, exists := before.Records[id]
				if !exists || seen[id] {
					t.Fatalf("historical reader returned unexpected or duplicate record %s", id)
				}
				seen[id] = true
				edgeRequireHistoricalRecordEqual(t, row, want)
			}
			e.f.reopen(t)
			edgeRequireStateEqual(t, after, e.snapshot(t))
			replayed := e.call(t, p, params)
			if replayed.Error != nil || replayed.Meta["replayed"] != true || string(replayed.Result) != string(first.Result) {
				t.Fatalf("guarded batch did not replay its exact response after reopen: %+v", replayed)
			}
			edgeRequireStateEqual(t, after, e.snapshot(t))
			edgeRequirePublicReady(t, e, other.ID, to.ID, neighbor.ID)
		})
	}
}

func TestB07GuardedDurableEdgeBatchRefusalConservesEveryPlane(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, scenario := range []string{"stale-source", "stale-read-target", "late-retype", "late-self", "late-cycle"} {
				t.Run(scenario, func(t *testing.T) {
					e := newEdgeContractFixture(t, engine)
					from, to, other := e.create(t, "refusal-source"), e.create(t, "refusal-target"), e.create(t, "refusal-other")
					seed := e.probe("bead.link", "refusal-seed")
					edgeRequireSuccess(t, e.call(t, seed, map[string]any{"from": from, "to": to, "kind": "blocks"}))
					staleState := e.snapshot(t)
					stale := staleState.Records[edgeRecordKey(from)].Revision
					updateRef := from
					if scenario == "stale-read-target" {
						updateRef = to
						stale = staleState.Records[edgeRecordKey(to)].Revision
					}
					update := e.probe("bead.update", "refusal-seed-update")
					edgeRequireSuccess(t, e.call(t, update, map[string]any{"id": updateRef, "fields": map[string]any{"title": "newer committed image"}}))
					p := e.probe("tx.apply", "batch-refused")
					before := e.snapshot(t)
					cs := model.ChangeSet{Ops: []model.Op{edgePatchOp(other, "must roll back"), edgeLinkOp(other, to, "tracks")}}
					code := "ontology_guard_failed"
					switch scenario {
					case "stale-source", "stale-read-target":
						cs.Expect = []model.Precondition{{Ref: updateRef, Revision: &stale}}
						code = "precondition_failed"
					case "late-retype":
						cs.Ops = append(cs.Ops, edgeLinkOp(from, to, "tracks"))
						code = "ontology_guard_failed"
					case "late-self":
						cs.Ops = append(cs.Ops, edgeLinkOp(to, to, "blocks"))
					case "late-cycle":
						cs.Ops = append(cs.Ops, edgeLinkOp(to, from, "blocks"))
					}
					edgeRequireRefusal(t, e.call(t, p, map[string]any{"changeSet": cs}), code)
					after := e.snapshot(t)
					edgeRequireStateEqual(t, before, after)
					if after.Receipts[p] != nil {
						t.Fatal("refused batch minted a receipt")
					}
					edgeRequireLinks(t, after, from, edgeLinkValue(to, "blocks"))
					edgeRequireLinks(t, after, other)
					edgeRequirePublicReady(t, e, to.ID, other.ID)
					e.f.reopen(t)
					edgeRequireStateEqual(t, before, e.snapshot(t))
					// A refusal cannot reserve the key or make a corrected request
					// replay an error. Reuse that same key for a real valid batch.
					good := model.ChangeSet{Ops: []model.Op{edgePatchOp(other, "corrected"), edgeLinkOp(other, to, "tracks")}}
					fixed := e.call(t, p, map[string]any{"changeSet": good})
					edgeRequireSuccess(t, fixed)
					final := e.snapshot(t)
					commit := edgeRequireReceipt(t, final, p, fixed.Result, false)
					edgeRequireSingleCommit(t, before, final, commit, other)
					edgeRequireLinks(t, final, other, edgeLinkValue(to, "tracks"))
					edgeRequireOnlyEdgeAndFields(t, before, final, other, map[string]any{"title": "corrected"})
				})
			}
		})
	}
}

func TestB07KnownDependencyKindsHaveOnlyAdoptedReadyAndCloseEffects(t *testing.T) {
	// Enumerate the fixed vocabulary, independently of the private production
	// map. D25 fixes its meanings; custom-kind bytes/inert loops retain their
	// separate opaque_kinds_test.go coverage.
	kinds := []string{
		"blocks", "parent-child", "conditional-blocks", "waits-for", "related", "discovered-from",
		"replies-to", "relates-to", "duplicates", "supersedes", "authored-by", "assigned-to",
		"approved-by", "attests", "tracks", "until", "caused-by", "validates", "delegated-from",
	}
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, kind := range kinds {
				t.Run(kind, func(t *testing.T) {
					e := newEdgeContractFixture(t, engine)
					from, to := e.create(t, "kind-source"), e.create(t, "kind-target")
					var child model.Ref
					if kind == "waits-for" {
						child = e.f.createWithParent(t, "kind-child", to)
						e.refs = append(e.refs, child)
						// createWithParent's receipt label is declared by the
						// existing runtime helper, without changing that helper.
						e.probes = append(e.probes, edgeReceiptProbe{"bead.create", key("create-kind-child")})
					}
					p := e.probe("bead.link", "kind-link")
					before := e.snapshot(t)
					params := map[string]any{"from": from, "to": to, "kind": kind}
					first := e.call(t, p, params)
					edgeRequireSuccess(t, first)
					after := e.snapshot(t)
					edgeRequireSingleCommit(t, before, after, edgeRequireReceipt(t, after, p, first.Result, false), from)
					edgeRequireLinks(t, after, from, edgeLinkValue(to, kind))
					edgeRequireOnlyEdgeAndFields(t, before, after, from, nil)
					want := []string{to.ID}
					scheduling := kind == "blocks" || kind == "parent-child" || kind == "conditional-blocks" || kind == "waits-for"
					if !scheduling {
						want = append(want, from.ID)
					}
					if child.ID != "" {
						want = append(want, child.ID)
					}
					edgeRequirePublicReady(t, e, want...)
					dup := e.probe("bead.link", "kind-duplicate")
					beforeNoop := e.snapshot(t)
					duplicate := e.call(t, dup, params)
					edgeRequireSuccess(t, duplicate)
					afterNoop := e.snapshot(t)
					edgeRequireReceipt(t, afterNoop, dup, duplicate.Result, true)
					edgeRequireOnlyReceipt(t, beforeNoop, afterNoop, dup)
					e.f.reopen(t)
					edgeRequireStateEqual(t, afterNoop, e.snapshot(t))
					closeProbe := e.probe("bead.close", "kind-close")
					beforeClose := e.snapshot(t)
					closed := e.call(t, closeProbe, map[string]any{"id": from})
					if kind == "blocks" || kind == "conditional-blocks" || kind == "waits-for" {
						edgeRequireRefusal(t, closed, "ontology_guard_failed")
						edgeRequireStateEqual(t, beforeClose, e.snapshot(t))
					} else {
						edgeRequireSuccess(t, closed)
						final := e.snapshot(t)
						edgeRequireSingleCommit(t, beforeClose, final, edgeRequireReceipt(t, final, closeProbe, closed.Result, false), from)
						edgeRequireLinks(t, final, from, edgeLinkValue(to, kind))
						edgeRequireOnlyEdgeAndFields(t, beforeClose, final, from, map[string]any{"status": "done"})
					}
				})
			}
		})
	}
}

func TestB07WaitsForMetadataValuesNoopOmissionAndReplacement(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			e := newEdgeContractFixture(t, engine)
			from, to := e.create(t, "metadata-source"), e.create(t, "metadata-spawner")
			childA := e.f.createWithParent(t, "metadata-child-a", to)
			childB := e.f.createWithParent(t, "metadata-child-b", to)
			e.refs = append(e.refs, childA, childB)
			for _, title := range []string{"metadata-child-a", "metadata-child-b"} {
				e.probes = append(e.probes, edgeReceiptProbe{"bead.create", key("create-" + title)})
			}
			p := e.probe("bead.link", "metadata-seed")
			params := map[string]any{"from": from, "to": to, "kind": "waits-for", "metadata": json.RawMessage(`{"gate":"any-children","also_blocks":false}`)}
			edgeRequireSuccess(t, e.call(t, p, params))
			closed := e.probe("bead.close", "metadata-close-one")
			edgeRequireSuccess(t, e.call(t, closed, map[string]any{"id": childA}))
			edgeRequirePublicReady(t, e, from.ID, to.ID, childB.ID)
			for i, metadata := range []any{
				json.RawMessage(`{ "also_blocks": false, "gate": "any-children" }`), nil,
			} {
				p := e.probe("bead.link", "metadata-equivalent-"+model.Natural64(uint64(i)).String())
				before := e.snapshot(t)
				params := map[string]any{"from": from, "to": to, "kind": "waits-for"}
				if metadata != nil {
					params["metadata"] = metadata
				}
				got := e.call(t, p, params)
				edgeRequireSuccess(t, got)
				after := e.snapshot(t)
				edgeRequireReceipt(t, after, p, got.Result, true)
				edgeRequireOnlyReceipt(t, before, after, p)
				want := edgeLinkValue(to, "waits-for")
				want["metadata"] = map[string]any{"gate": "any-children"}
				edgeRequireLinks(t, after, from, want)
			}
			// Explicit {} replaces the stored any-children predicate with the
			// catalog's all-children/false default. The open second child now
			// blocks, even though one child is already terminal.
			reset := e.probe("bead.link", "metadata-reset")
			before := e.snapshot(t)
			got := e.call(t, reset, map[string]any{"from": from, "to": to, "kind": "waits-for", "metadata": map[string]any{}})
			edgeRequireSuccess(t, got)
			after := e.snapshot(t)
			edgeRequireSingleCommit(t, before, after, edgeRequireReceipt(t, after, reset, got.Result, false), from)
			edgeRequireOnlyEdgeAndFields(t, before, after, from, nil)
			want := edgeLinkValue(to, "waits-for")
			want["metadata"] = map[string]any{"gate": "all-children"}
			edgeRequireLinks(t, after, from, want)
			edgeRequirePublicReady(t, e, to.ID, childB.ID)
			e.f.reopen(t)
			edgeRequireStateEqual(t, after, e.snapshot(t))
			edgeRequirePublicReady(t, e, to.ID, childB.ID)
			// A changed predicate uses the same atomic source guard as an
			// ordinary edge mutation; stale revision cannot update metadata.
			stale := before.Records[edgeRecordKey(from)].Revision
			staleProbe := e.probe("bead.link", "metadata-stale")
			beforeStale := e.snapshot(t)
			params["expect"] = []model.Precondition{{Ref: from, Revision: &stale}}
			edgeRequireRefusal(t, e.call(t, staleProbe, params), "precondition_failed")
			edgeRequireStateEqual(t, beforeStale, e.snapshot(t))
		})
	}
}

func TestB07UnresolvedExternalTargetsRetainIdentityAcrossBothProducers(t *testing.T) {
	for _, engine := range testEngines {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			for _, tc := range []struct {
				name, kind string
				ready      bool
			}{
				{name: "blocking-cross-prefix", kind: "blocks", ready: false},
				{name: "association-external", kind: "relates-to", ready: true},
			} {
				t.Run(engine+"/"+producer+"/"+tc.name, func(t *testing.T) {
					e := newEdgeContractFixture(t, engine)
					from := e.create(t, "unresolved-"+tc.name)
					to := model.RefByID("foreign-prefix-404", model.Natural64(0))
					p := e.probe(producer, "unresolved-"+producer+"-"+tc.name)
					params := map[string]any{}
					if producer == "bead.link" {
						params = map[string]any{"from": from, "to": to, "kind": tc.kind}
					} else {
						params = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{edgeLinkOp(from, to, tc.kind)}}}
					}
					before := e.snapshot(t)
					linked := e.call(t, p, params)
					edgeRequireSuccess(t, linked)
					after := e.snapshot(t)
					commit := edgeRequireReceipt(t, after, p, linked.Result, false)
					edgeRequireSingleCommit(t, before, after, commit, from)
					edgeRequireLinks(t, after, from, edgeLinkValue(to, tc.kind))
					if _, exists := after.Records[edgeRecordKey(to)]; exists {
						t.Fatal("unresolved target unexpectedly acquired a durable record")
					}
					edgeReadyContains(t, e, from.ID, tc.ready)
					e.f.reopen(t)
					edgeRequireStateEqual(t, after, e.snapshot(t))
					edgeReadyContains(t, e, from.ID, tc.ready)

					// An unresolved edge can be removed without inventing a target
					// record; unlink is guarded by the source image only.
					removeMethod := "bead.unlink"
					if producer == "tx.apply" {
						removeMethod = "tx.apply"
					}
					remove := e.probe(removeMethod, "unresolved-remove-"+producer+"-"+tc.name)
					removeParams := map[string]any{}
					if producer == "bead.link" {
						removeParams = map[string]any{"from": from, "to": to, "kind": tc.kind}
					} else {
						removeParams = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpUnlink, Link: &model.LinkOp{From: from, To: to, LinkKind: tc.kind}}}}}
					}
					beforeRemove := e.snapshot(t)
					removed := e.call(t, remove, removeParams)
					edgeRequireSuccess(t, removed)
					afterRemove := e.snapshot(t)
					removeCommit := edgeRequireReceipt(t, afterRemove, remove, removed.Result, false)
					edgeRequireSingleCommit(t, beforeRemove, afterRemove, removeCommit, from)
					edgeRequireLinks(t, afterRemove, from)
				})
			}
		}
	}
}

func TestB07WaitsForMetadataParityAcrossPublicAndTxApply(t *testing.T) {
	cases := []struct {
		name, gate string
		also       bool
		closeA     bool
		closeB     bool
		closeTo    bool
		ready      bool
	}{
		{name: "all-children", gate: "all-children", ready: true, closeA: true, closeB: true},
		{name: "any-children", gate: "any-children", ready: true, closeA: true},
		{name: "all-children-also-blocks", gate: "all-children", also: true, ready: true, closeA: true, closeB: true, closeTo: true},
		{name: "any-children-also-blocks", gate: "any-children", also: true, ready: true, closeA: true, closeB: true, closeTo: true},
	}
	for _, engine := range testEngines {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			for _, tc := range cases {
				t.Run(engine+"/"+producer+"/"+tc.name, func(t *testing.T) {
					e := newEdgeContractFixture(t, engine)
					from, to := e.create(t, "waits-parity-source"), e.create(t, "waits-parity-spawner")
					childA := e.f.createWithParent(t, "waits-parity-child-a", to)
					childB := e.f.createWithParent(t, "waits-parity-child-b", to)
					e.refs = append(e.refs, childA, childB)
					e.probes = append(e.probes,
						edgeReceiptProbe{"bead.create", key("create-waits-parity-child-a")},
						edgeReceiptProbe{"bead.create", key("create-waits-parity-child-b")})
					metadata := map[string]any{"gate": tc.gate, "also_blocks": tc.also}
					p := e.probe(producer, "waits-parity-"+tc.name)
					params := map[string]any{}
					if producer == "bead.link" {
						params = map[string]any{"from": from, "to": to, "kind": "waits-for", "metadata": metadata}
					} else {
						params = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: "waits-for", Metadata: &model.LinkMetadata{Gate: tc.gate, AlsoBlocks: tc.also}}}}}}
					}
					before := e.snapshot(t)
					linked := e.call(t, p, params)
					edgeRequireSuccess(t, linked)
					after := e.snapshot(t)
					commit := edgeRequireReceipt(t, after, p, linked.Result, false)
					edgeRequireSingleCommit(t, before, after, commit, from)
					want := edgeLinkValue(to, "waits-for")
					storedMetadata := map[string]any{"gate": tc.gate}
					if tc.also {
						storedMetadata["also_blocks"] = true
					}
					want["metadata"] = storedMetadata
					edgeRequireLinks(t, after, from, want)
					edgeReadyContains(t, e, from.ID, false)

					closeChild := func(label string, child model.Ref) {
						probe := e.probe("bead.close", label)
						closed := e.call(t, probe, map[string]any{"id": child})
						edgeRequireSuccess(t, closed)
					}
					if tc.closeA {
						closeChild("waits-parity-close-a", childA)
					}
					edgeReadyContains(t, e, from.ID, tc.gate == "any-children" && !tc.also && tc.closeA)
					if tc.closeB {
						closeChild("waits-parity-close-b", childB)
					}
					if tc.closeTo {
						closeChild("waits-parity-close-spawner", to)
					}
					edgeReadyContains(t, e, from.ID, tc.ready)
					final := e.snapshot(t)
					e.f.reopen(t)
					reopened := e.snapshot(t)
					edgeRequireStateEqual(t, final, reopened)
					edgeRequireLinks(t, reopened, from, want)
					edgeReadyContains(t, e, from.ID, tc.ready)
				})
			}
		}
	}
}

func TestB07InvalidMetadataRefusesPublicBatchWithoutAnyPublication(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			e := newEdgeContractFixture(t, engine)
			from, to := e.create(t, "invalid-metadata-source"), e.create(t, "invalid-metadata-target")
			for i, bad := range []json.RawMessage{
				json.RawMessage(`{"gate":""}`), json.RawMessage(`{"gate":"some-children"}`),
				json.RawMessage(`{"gate":"all-children","also_blocks":"true"}`),
				json.RawMessage(`{"gate":"all-children","extra":true}`),
			} {
				p := e.probe("tx.apply", "metadata-invalid-"+model.Natural64(uint64(i)).String())
				before := e.snapshot(t)
				// Raw RPC input is intentional: model.Op.MarshalJSON must not
				// reject malformed caller input before the public boundary sees it.
				cs := map[string]any{"ops": []any{
					edgePatchOp(from, "must roll back malformed metadata"),
					map[string]any{"kind": "link", "from": from, "to": to, "linkKind": "waits-for", "metadata": bad},
				}}
				edgeRequireRefusal(t, e.call(t, p, map[string]any{"changeSet": cs}), "invalid_params")
				edgeRequireStateEqual(t, before, e.snapshot(t))
				e.f.reopen(t)
				edgeRequireStateEqual(t, before, e.snapshot(t))
			}
		})
	}
}

func TestB07ConditionalEdgeUsesAdoptedFailureOutcomeAcrossReopen(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			e := newEdgeContractFixture(t, engine)
			from, to := e.create(t, "conditional-source"), e.create(t, "conditional-target")
			link := e.probe("bead.link", "conditional-link")
			edgeRequireSuccess(t, e.call(t, link, map[string]any{"from": from, "to": to, "kind": "conditional-blocks"}))
			edgeRequirePublicReady(t, e, to.ID)
			for i, step := range []struct {
				method, status string
				ready          []string
			}{
				{"bead.close", "done", nil},
				{"bead.reopen", "todo", []string{to.ID}},
				{"bead.fail", "failed", []string{from.ID}},
				{"bead.reopen", "todo", []string{to.ID}},
			} {
				p := e.probe(step.method, "conditional-transition-"+model.Natural64(uint64(i)).String())
				before := e.snapshot(t)
				got := e.call(t, p, map[string]any{"id": to})
				edgeRequireSuccess(t, got)
				after := e.snapshot(t)
				edgeRequireSingleCommit(t, before, after, edgeRequireReceipt(t, after, p, got.Result, false), to)
				edgeRequireOnlyEdgeAndFields(t, before, after, to, map[string]any{"status": step.status})
				edgeRequireLinks(t, after, from, edgeLinkValue(to, "conditional-blocks"))
				edgeRequirePublicReady(t, e, step.ready...)
				e.f.reopen(t)
				edgeRequireStateEqual(t, after, e.snapshot(t))
				edgeRequirePublicReady(t, e, step.ready...)
			}
		})
	}
}

func TestB07NamedEdgeMethodsKeepUnsupportedWorkflowReferencesAtomic(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			e := newEdgeContractFixture(t, engine)
			local := e.create(t, "unsupported-plane-source")
			// This tests the current named-method admission boundary. It does
			// not create an ephemeral item or claim mixed-plane runtime proof.
			workflow := model.RefByID("workflow:item/unsupported", model.Natural64(0))
			for _, method := range []string{"bead.link", "bead.unlink"} {
				for i, pair := range [][2]model.Ref{{local, workflow}, {workflow, local}} {
					p := e.probe(method, "unsupported-plane-"+method+"-"+model.Natural64(uint64(i)).String())
					before := e.snapshot(t)
					edgeRequireRefusal(t, e.call(t, p, map[string]any{"from": pair[0], "to": pair[1], "kind": "blocks"}), "invalid_params")
					edgeRequireStateEqual(t, before, e.snapshot(t))
					e.f.reopen(t)
					edgeRequireStateEqual(t, before, e.snapshot(t))
					edgeRequirePublicReady(t, e, local.ID)
				}
			}
			workflow = model.RefByID("workflow:item/0123456789abcdef0123456789abcdef", model.Natural64(0))
			for i, pair := range [][2]model.Ref{{local, workflow}, {workflow, local}} {
				p := e.probe("tx.apply", "unsupported-plane-tx-"+model.Natural64(uint64(i)).String())
				before := e.snapshot(t)
				cs := model.ChangeSet{Ops: []model.Op{edgeLinkOp(pair[0], pair[1], "blocks")}}
				edgeRequireRefusal(t, e.call(t, p, map[string]any{"changeSet": cs}), "invalid_params")
				edgeRequireStateEqual(t, before, e.snapshot(t))
				edgeRequirePublicReady(t, e, local.ID)
			}
		})
	}
}

// The ordinary workflow-ID matrix above proves the early durable boundary,
// while this case binds it to a real ephemeral producer. Persistent workflow
// items are created by the actual provider and then presented to both durable
// edge producers in both directions. Neither producer may manufacture a
// durable edge, receipt, graph revision, or workflow checkpoint mutation.
func TestB07DurableEdgesRefuseActualEphemeralWorkflowItemsAtomically(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newEphemeralCycleFixture(t, engine)
			if engine == "memory" {
				before := f.state(t)
				got := f.request(t, "bead.cycles", map[string]any{"includeEphemeral": true}, "b07-routing-memory")
				if got.Error == nil || got.Error.Data.Code != "plugin_durable_state_unavailable" {
					t.Fatalf("memory workflow capability=%+v", got.Error)
				}
				if after := f.state(t); !reflect.DeepEqual(before, after) {
					t.Fatal("memory workflow capability refusal changed durable or graph state")
				}
				return
			}

			created := f.request(t, "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": "durable routing source"},
			}, "b07-routing-create")
			if created.Error != nil {
				t.Fatalf("durable routing source refused: %+v", created.Error)
			}
			var commit model.Commit
			if err := json.Unmarshal(created.Result, &commit); err != nil || len(commit.Results) != 1 {
				t.Fatalf("durable routing source result=%s error=%v", created.Result, err)
			}
			durable := commit.Results[0].Ref
			workflowItems := f.invoke(t, "b07-routing-invoke", 1)
			workflow := workflowItems[0]
			before := f.state(t)
			attempts := []struct {
				method string
				params map[string]any
			}{
				{method: "bead.link", params: map[string]any{"from": durable, "to": workflow, "kind": "blocks"}},
				{method: "bead.link", params: map[string]any{"from": workflow, "to": durable, "kind": "blocks"}},
				{method: "tx.apply", params: map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{edgeLinkOp(durable, workflow, "blocks")}}}},
				{method: "tx.apply", params: map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{edgeLinkOp(workflow, durable, "blocks")}}}},
			}
			for i, attempt := range attempts {
				got := f.request(t, attempt.method, attempt.params, "b07-routing-refusal-"+model.Natural64(uint64(i)).String())
				if got.Error == nil || got.Error.Data.Code != "invalid_params" || len(got.Result) != 0 {
					t.Fatalf("%s mixed-plane refusal=%+v result=%s", attempt.method, got.Error, got.Result)
				}
				if after := f.state(t); !reflect.DeepEqual(before, after) {
					t.Fatalf("%s mixed-plane refusal changed durable/checkpoint/graph state", attempt.method)
				}
			}
			f.reopen(t)
			if after := f.state(t); !reflect.DeepEqual(before, after) {
				t.Fatal("mixed-plane refusals were not durable-state neutral after reopen")
			}
		})
	}
}

package s1b05b08

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
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

func edgeTypedLinkOp(op model.OpKind, from, to model.Ref, kind string, targetKind model.LinkTargetKind) model.Op {
	return model.Op{Kind: op, Link: &model.LinkOp{From: from, To: to, LinkKind: kind, TargetKind: targetKind}}
}

func edgeTypedLinkParams(producer string, op model.OpKind, from, to model.Ref, kind string, targetKind model.LinkTargetKind) map[string]any {
	if producer == "tx.apply" {
		return map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{edgeTypedLinkOp(op, from, to, kind, targetKind)}}}
	}
	params := map[string]any{"from": from, "to": to, "kind": kind}
	if targetKind != "" {
		params["targetKind"] = targetKind
	}
	return params
}

func edgeExternalLinkValue(to model.Ref, kind string) map[string]any {
	return map[string]any{"to": to, "kind": kind, "targetKind": "external"}
}

func edgePatchOp(ref model.Ref, title string) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"title": title}}}
}

func edgeRequireSuccess(t *testing.T, result envelope) {
	t.Helper()
	if result.Error != nil {
		t.Fatalf("public write refused: code=%s envelope=%+v", result.Error.Data.Code, result)
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
					if kind == "parent-child" {
						edgeRequireOnlyLocalParentChildEdit(t, before, after, from, &to)
					} else {
						edgeRequireOnlyEdgeAndFields(t, before, after, from, nil)
					}
					want := []string{to.ID}
					scheduling := kind == "blocks" || kind == "conditional-blocks" || kind == "waits-for"
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
						edgeRequireOnlyEdgeAndFields(t, beforeClose, final, from, map[string]any{"status": "done"}, edgeExpectedClose{At: &e.f.now, Reason: new(string)})
					}
				})
			}
		})
	}
}

// Pair this local parent-child oracle with an exact edge-value assertion. The
// canonical parent transition is supplied by the request, while every other
// source member remains strict. Other dependency kinds use the unchanged oracle.
func edgeRequireOnlyLocalParentChildEdit(t *testing.T, before, after edgeContractState, ref model.Ref, parent *model.Ref) {
	t.Helper()
	want := edgeJSONObject(t, before.Records[edgeRecordKey(ref)].Body)
	got := edgeJSONObject(t, after.Records[edgeRecordKey(ref)].Body)
	if links, exists := got["links"]; exists {
		want["links"] = links
	} else {
		delete(want, "links")
	}
	if parent == nil {
		delete(want, "parent")
	} else {
		raw, err := json.Marshal(parent)
		if err != nil {
			t.Fatal(err)
		}
		want["parent"] = edgeJSONObject(t, raw)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("local parent-child edit changed unrelated source members: got=%+v want=%+v", got, want)
	}
}

func TestB07LocalParentChildSchedulingRefusalAndTypedDetach(t *testing.T) {
	// Binding ruling a87e3cfa: open parents do not suppress ready children;
	// blocked ancestors do. Local legacy edges also persist canonical Parent.
	for _, engine := range testEngines {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			t.Run(engine+"/"+producer, func(t *testing.T) {
				e := newEdgeContractFixture(t, engine)
				child, parent, blocker := e.create(t, "local-hierarchy-child"), e.create(t, "local-hierarchy-parent"), e.create(t, "local-hierarchy-blocker")
				link := e.probe(producer, "local-hierarchy-link")
				before := e.snapshot(t)
				linked := e.call(t, link, edgeTypedLinkParams(producer, model.OpLink, child, parent, "parent-child", model.LinkTargetLocal))
				edgeRequireSuccess(t, linked)
				after := e.snapshot(t)
				edgeRequireSingleCommit(t, before, after, edgeRequireReceipt(t, after, link, linked.Result, false), child)
				edgeRequireLinks(t, after, child, edgeLinkValue(parent, "parent-child"))
				edgeRequireOnlyLocalParentChildEdit(t, before, after, child, &parent)
				edgeRequirePublicReady(t, e, child.ID, parent.ID, blocker.ID)

				block := e.probe(producer, "local-hierarchy-block-parent")
				beforeBlock := e.snapshot(t)
				blocked := e.call(t, block, edgeTypedLinkParams(producer, model.OpLink, parent, blocker, "blocks", model.LinkTargetLocal))
				edgeRequireSuccess(t, blocked)
				afterBlock := e.snapshot(t)
				edgeRequireSingleCommit(t, beforeBlock, afterBlock, edgeRequireReceipt(t, afterBlock, block, blocked.Result, false), parent)
				edgeRequireLinks(t, afterBlock, parent, edgeLinkValue(blocker, "blocks"))
				edgeRequireOnlyEdgeAndFields(t, beforeBlock, afterBlock, parent, nil)
				edgeRequirePublicReady(t, e, blocker.ID)

				conflict := e.probe(producer, "local-hierarchy-conflicting-parent")
				beforeConflict := e.snapshot(t)
				refused := e.call(t, conflict, edgeTypedLinkParams(producer, model.OpLink, child, blocker, "parent-child", model.LinkTargetLocal))
				edgeRequireRefusal(t, refused, "invalid_params")
				edgeRequireStateEqual(t, beforeConflict, e.snapshot(t))
				e.f.reopen(t)
				edgeRequireStateEqual(t, beforeConflict, e.snapshot(t))
				edgeRequirePublicReady(t, e, blocker.ID)

				closeBlocker := e.probe("bead.close", "local-hierarchy-close-blocker")
				beforeClose := e.snapshot(t)
				closed := e.call(t, closeBlocker, map[string]any{"id": blocker})
				edgeRequireSuccess(t, closed)
				afterClose := e.snapshot(t)
				edgeRequireSingleCommit(t, beforeClose, afterClose, edgeRequireReceipt(t, afterClose, closeBlocker, closed.Result, false), blocker)
				edgeRequireOnlyEdgeAndFields(t, beforeClose, afterClose, blocker, map[string]any{"status": "done"}, edgeExpectedClose{At: &e.f.now, Reason: new(string)})
				edgeRequirePublicReady(t, e, child.ID, parent.ID)

				// Same-spelled external identity never changes Parent. A local
				// unlink removes only the local arm and detaches its Parent.
				external := e.probe(producer, "local-hierarchy-external-sibling")
				beforeExternal := e.snapshot(t)
				opaque := e.call(t, external, edgeTypedLinkParams(producer, model.OpLink, child, parent, "parent-child", model.LinkTargetExternal))
				edgeRequireSuccess(t, opaque)
				afterExternal := e.snapshot(t)
				edgeRequireSingleCommit(t, beforeExternal, afterExternal, edgeRequireReceipt(t, afterExternal, external, opaque.Result, false), child)
				edgeRequireLinks(t, afterExternal, child, edgeLinkValue(parent, "parent-child"), edgeExternalLinkValue(parent, "parent-child"))
				edgeRequireOnlyEdgeAndFields(t, beforeExternal, afterExternal, child, nil)
				edgeRequirePublicReady(t, e, parent.ID)

				unlinkMethod := "bead.unlink"
				if producer == "tx.apply" {
					unlinkMethod = producer
				}
				unlink := e.probe(unlinkMethod, "local-hierarchy-unlink-local")
				beforeUnlink := e.snapshot(t)
				unlinked := e.call(t, unlink, edgeTypedLinkParams(producer, model.OpUnlink, child, parent, "parent-child", model.LinkTargetLocal))
				edgeRequireSuccess(t, unlinked)
				afterUnlink := e.snapshot(t)
				edgeRequireSingleCommit(t, beforeUnlink, afterUnlink, edgeRequireReceipt(t, afterUnlink, unlink, unlinked.Result, false), child)
				edgeRequireLinks(t, afterUnlink, child, edgeExternalLinkValue(parent, "parent-child"))
				edgeRequireOnlyLocalParentChildEdit(t, beforeUnlink, afterUnlink, child, nil)

				// Retaining an external parent-child edge cannot activate the
				// local legacy detach guard, even for an already detached child.
				detach := e.probe("tx.apply", "local-hierarchy-detach-with-external")
				beforeDetach := e.snapshot(t)
				detached := e.call(t, detach, map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: child}}}}})
				edgeRequireSuccess(t, detached)
				afterDetach := e.snapshot(t)
				edgeRequireReceipt(t, afterDetach, detach, detached.Result, true)
				edgeRequireOnlyReceipt(t, beforeDetach, afterDetach, detach)
				e.f.reopen(t)
				edgeRequireStateEqual(t, afterDetach, e.snapshot(t))
				edgeRequirePublicReady(t, e, parent.ID)
			})
		}
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

func TestB07MissingLocalEndpointsRefuseAtomicallyAcrossBothProducers(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, producer := range []string{"bead.link", "tx.apply"} {
				for _, scenario := range []string{"missing-source", "missing-target", "foreign-looking-local-target"} {
					t.Run(producer+"/"+scenario, func(t *testing.T) {
						e := newEdgeContractFixture(t, engine)
						from, to, other := e.create(t, "local-source"), e.create(t, "local-target"), e.create(t, "local-neighbor")
						requestFrom, requestTo := from, to
						switch scenario {
						case "missing-source":
							requestFrom = model.RefByID("missing-local-source", model.Natural64(0))
						case "missing-target":
							requestTo = model.RefByID(from.ID+"-missing", model.Natural64(0))
						case "foreign-looking-local-target":
							// Opaque ID spelling never opts a local reference into
							// external-target semantics (binding B07/B14 decision).
							requestTo = model.RefByID("foreign-prefix-404", model.Natural64(0))
						}
						p := e.probe(producer, "local-endpoint-"+scenario)
						params := map[string]any{"from": requestFrom, "to": requestTo, "kind": "blocks"}
						if producer == "tx.apply" {
							params = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
								edgePatchOp(other, "must roll back"), edgeLinkOp(requestFrom, requestTo, "blocks"),
							}}}
						}
						before := e.snapshot(t)
						edgeRequireRefusal(t, e.call(t, p, params), "not_found")
						after := e.snapshot(t)
						edgeRequireStateEqual(t, before, after)
						if after.Receipts[p] != nil {
							t.Fatal("missing local endpoint minted a receipt")
						}
						edgeRequirePublicReady(t, e, from.ID, to.ID, other.ID)
						e.f.reopen(t)
						edgeRequireStateEqual(t, before, e.snapshot(t))

						// A refused local reference neither reserves the key nor
						// prevents a corrected request from publishing once.
						good := map[string]any{"from": from, "to": to, "kind": "blocks"}
						changed := []model.Ref{from}
						var otherFields map[string]any
						if producer == "tx.apply" {
							good = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
								edgePatchOp(other, "corrected local request"), edgeLinkOp(from, to, "blocks"),
							}}}
							changed = append(changed, other)
							otherFields = map[string]any{"title": "corrected local request"}
						}
						fixed := e.call(t, p, good)
						edgeRequireSuccess(t, fixed)
						final := e.snapshot(t)
						edgeRequireSingleCommit(t, before, final, edgeRequireReceipt(t, final, p, fixed.Result, false), changed...)
						edgeRequireLinks(t, final, from, edgeLinkValue(to, "blocks"))
						edgeRequireOnlyEdgeAndFields(t, before, final, from, nil)
						edgeRequireOnlyEdgeAndFields(t, before, final, other, otherFields)
						edgeRequirePublicReady(t, e, to.ID, other.ID)
					})
				}
			}
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
				{name: "opaque-external", kind: "custom External/Edge@1", ready: true},
				{name: "workflow-spelling", kind: "blocks", ready: false},
				{name: "malformed-workflow-spelling", kind: "blocks", ready: false},
			} {
				t.Run(engine+"/"+producer+"/"+tc.name, func(t *testing.T) {
					e := newEdgeContractFixture(t, engine)
					from := e.create(t, "unresolved-"+tc.name)
					toID := "foreign-prefix-404"
					if tc.name == "opaque-external" {
						toID = "external:tracker/1"
					} else if tc.name == "workflow-spelling" {
						toID = "workflow:item/0123456789abcdef0123456789abcdef"
					} else if tc.name == "malformed-workflow-spelling" {
						toID = "workflow:item/ opaque 外部 "
					}
					to := model.RefByID(toID, model.Natural64(0))
					p := e.probe(producer, "unresolved-"+producer+"-"+tc.name)
					params := map[string]any{}
					if producer == "bead.link" {
						params = map[string]any{"from": from, "to": to, "kind": tc.kind, "targetKind": "external"}
					} else {
						params = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{edgeTypedLinkOp(model.OpLink, from, to, tc.kind, model.LinkTargetExternal)}}}
					}
					before := e.snapshot(t)
					linked := e.call(t, p, params)
					edgeRequireSuccess(t, linked)
					after := e.snapshot(t)
					commit := edgeRequireReceipt(t, after, p, linked.Result, false)
					edgeRequireSingleCommit(t, before, after, commit, from)
					edgeRequireLinks(t, after, from, edgeExternalLinkValue(to, tc.kind))
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
						removeParams = map[string]any{"from": from, "to": to, "kind": tc.kind, "targetKind": "external"}
					} else {
						removeParams = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpUnlink, Link: &model.LinkOp{From: from, To: to, LinkKind: tc.kind, TargetKind: model.LinkTargetExternal}}}}}
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

func TestB07TypedEndpointIdentityAndRefusalsAcrossBothProducers(t *testing.T) {
	for _, engine := range testEngines {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			t.Run(engine+"/"+producer, func(t *testing.T) {
				e := newEdgeContractFixture(t, engine)
				from, to, other := e.create(t, "typed-source"), e.create(t, "typed-target"), e.create(t, "typed-neighbor")
				local := e.probe(producer, "typed-local")
				external := e.probe(producer, "typed-external")
				duplicate := e.probe(producer, "typed-external-duplicate")
				retype := e.probe(producer, "typed-external-retype")
				localDefault := e.probe(producer, "typed-explicit-local")
				removeMethod := "bead.unlink"
				if producer == "tx.apply" {
					removeMethod = producer
				}
				wrongRemove := e.probe(removeMethod, "typed-external-wrong-unlink")
				remove := e.probe(removeMethod, "typed-external-unlink")
				localParams := edgeTypedLinkParams(producer, model.OpLink, from, to, "relates-to", "")
				externalParams := edgeTypedLinkParams(producer, model.OpLink, from, to, "blocks", model.LinkTargetExternal)
				edgeRequireSuccess(t, e.call(t, local, localParams))
				before := e.snapshot(t)
				first := e.call(t, external, externalParams)
				edgeRequireSuccess(t, first)
				after := e.snapshot(t)
				edgeRequireSingleCommit(t, before, after, edgeRequireReceipt(t, after, external, first.Result, false), from)
				edgeRequireLinks(t, after, from, edgeLinkValue(to, "relates-to"), edgeExternalLinkValue(to, "blocks"))
				edgeReadyContains(t, e, from.ID, false)

				dup := e.call(t, duplicate, externalParams)
				edgeRequireSuccess(t, dup)
				afterDup := e.snapshot(t)
				edgeRequireReceipt(t, afterDup, duplicate, dup.Result, true)
				edgeRequireOnlyReceipt(t, after, afterDup, duplicate)
				beforeRetype := e.snapshot(t)
				retypeParams := edgeTypedLinkParams(producer, model.OpLink, from, to, "tracks", model.LinkTargetExternal)
				if producer == "tx.apply" {
					retypeParams = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
						edgePatchOp(other, "must roll back"), edgeTypedLinkOp(model.OpLink, from, to, "tracks", model.LinkTargetExternal),
					}}}
				}
				edgeRequireRefusal(t, e.call(t, retype, retypeParams), "ontology_guard_failed")
				edgeRequireStateEqual(t, beforeRetype, e.snapshot(t))
				edgeRequireRefusal(t, e.call(t, wrongRemove, edgeTypedLinkParams(producer, model.OpUnlink, from, to, "tracks", model.LinkTargetExternal)), "not_found")
				edgeRequireStateEqual(t, beforeRetype, e.snapshot(t))
				// Reusing a receipt for another target identity domain must not
				// replay the external write as a local operation.
				edgeRequireRefusal(t, e.call(t, external, edgeTypedLinkParams(producer, model.OpLink, from, to, "blocks", model.LinkTargetLocal)), "idempotency_key_reused")
				edgeRequireStateEqual(t, beforeRetype, e.snapshot(t))

				beforeLocalDefault := e.snapshot(t)
				localNoop := e.call(t, localDefault, edgeTypedLinkParams(producer, model.OpLink, from, to, "relates-to", model.LinkTargetLocal))
				edgeRequireSuccess(t, localNoop)
				afterLocalDefault := e.snapshot(t)
				edgeRequireReceipt(t, afterLocalDefault, localDefault, localNoop.Result, true)
				edgeRequireOnlyReceipt(t, beforeLocalDefault, afterLocalDefault, localDefault)
				e.f.reopen(t)
				edgeRequireStateEqual(t, afterLocalDefault, e.snapshot(t))
				replay := e.call(t, external, externalParams)
				if replay.Error != nil || replay.Meta["replayed"] != true || string(replay.Result) != string(first.Result) {
					t.Fatalf("external identity receipt did not replay after reopen: %+v", replay)
				}
				edgeRequireStateEqual(t, afterLocalDefault, e.snapshot(t))
				removed := e.call(t, remove, edgeTypedLinkParams(producer, model.OpUnlink, from, to, "blocks", model.LinkTargetExternal))
				edgeRequireSuccess(t, removed)
				final := e.snapshot(t)
				edgeRequireSingleCommit(t, afterLocalDefault, final, edgeRequireReceipt(t, final, remove, removed.Result, false), from)
				edgeRequireLinks(t, final, from, edgeLinkValue(to, "relates-to"))
				edgeReadyContains(t, e, from.ID, true)
			})
		}
	}
}

func TestB07ExternalSchedulingNeverBindsTerminalLocalNames(t *testing.T) {
	for _, engine := range testEngines {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			for _, kind := range []string{"blocks", "parent-child", "conditional-blocks", "waits-for"} {
				t.Run(engine+"/"+producer+"/"+kind, func(t *testing.T) {
					e := newEdgeContractFixture(t, engine)
					from, to := e.create(t, "external-scheduling-source"), e.create(t, "external-terminal-local")
					closeTarget := e.probe("bead.close", "external-close-local")
					edgeRequireSuccess(t, e.call(t, closeTarget, map[string]any{"id": to}))
					p := e.probe(producer, "external-scheduling-link")
					before := e.snapshot(t)
					linked := e.call(t, p, edgeTypedLinkParams(producer, model.OpLink, from, to, kind, model.LinkTargetExternal))
					edgeRequireSuccess(t, linked)
					after := e.snapshot(t)
					edgeRequireSingleCommit(t, before, after, edgeRequireReceipt(t, after, p, linked.Result, false), from)
					edgeRequireLinks(t, after, from, edgeExternalLinkValue(to, kind))
					edgeReadyContains(t, e, from.ID, false)
					e.f.reopen(t)
					edgeRequireStateEqual(t, after, e.snapshot(t))
					edgeReadyContains(t, e, from.ID, false)
					closeSource := e.probe("bead.close", "external-close-source")
					beforeClose := e.snapshot(t)
					closed := e.call(t, closeSource, map[string]any{"id": from})
					if kind == "parent-child" {
						edgeRequireSuccess(t, closed)
					} else {
						edgeRequireRefusal(t, closed, "ontology_guard_failed")
						edgeRequireStateEqual(t, beforeClose, e.snapshot(t))
					}
				})
			}
		}
	}
}

func TestB07ExternalIdentitySurvivesLocalSelfSpellingAndDeletion(t *testing.T) {
	for _, engine := range testEngines {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			t.Run(engine+"/"+producer, func(t *testing.T) {
				e := newEdgeContractFixture(t, engine)
				from, local := e.create(t, "external-self-source"), e.create(t, "external-deleted-local")
				self := e.probe(producer, "external-self")
				sameName := e.probe(producer, "external-deleted-target")
				edgeRequireSuccess(t, e.call(t, self, edgeTypedLinkParams(producer, model.OpLink, from, from, "blocks", model.LinkTargetExternal)))
				edgeRequireSuccess(t, e.call(t, sameName, edgeTypedLinkParams(producer, model.OpLink, from, local, "parent-child", model.LinkTargetExternal)))
				remove := e.probe("tx.apply", "external-delete-local")
				before := e.snapshot(t)
				deleted := e.call(t, remove, map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: local}}}}})
				edgeRequireSuccess(t, deleted)
				after := e.snapshot(t)
				if _, exists := after.Records[edgeRecordKey(local)]; exists {
					t.Fatal("same-spelled local target was not deleted")
				}
				if !reflect.DeepEqual(before.Records[edgeRecordKey(from)], after.Records[edgeRecordKey(from)]) || !reflect.DeepEqual(before.Versions[edgeRecordKey(from)], after.Versions[edgeRecordKey(from)]) {
					t.Fatal("local deletion rewrote external source identity/history")
				}
				edgeRequireLinks(t, after, from, edgeExternalLinkValue(from, "blocks"), edgeExternalLinkValue(local, "parent-child"))
				edgeReadyContains(t, e, from.ID, false)
				cycles := e.f.call(t, e.f.ctx, "bead.cycles", map[string]any{}, "", actor)
				edgeRequireSuccess(t, cycles)
				var report struct {
					CycleCount model.Natural `json:"cycleCount"`
				}
				if err := json.Unmarshal(cycles.Result, &report); err != nil || !report.CycleCount.IsZero() {
					t.Fatalf("external self spelling entered local cycle report: %s error=%v", cycles.Result, err)
				}
				e.f.reopen(t)
				edgeRequireStateEqual(t, after, e.snapshot(t))
			})
		}
	}
}

func TestB07ExternalParentChildDoesNotJoinLocalWaitsForMembership(t *testing.T) {
	for _, engine := range testEngines {
		for _, producer := range []string{"bead.link", "tx.apply"} {
			t.Run(engine+"/"+producer, func(t *testing.T) {
				e := newEdgeContractFixture(t, engine)
				from, spawner, externalChild := e.create(t, "external-membership-source"), e.create(t, "external-membership-spawner"), e.create(t, "external-membership-child")
				p := e.probe(producer, "external-membership-parent")
				edgeRequireSuccess(t, e.call(t, p, edgeTypedLinkParams(producer, model.OpLink, externalChild, spawner, "parent-child", model.LinkTargetExternal)))
				wait := e.probe(producer, "external-membership-waits")
				params := map[string]any{"from": from, "to": spawner, "kind": "waits-for", "metadata": map[string]any{"gate": "all-children"}}
				if producer == "tx.apply" {
					params = map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: spawner, LinkKind: "waits-for", Metadata: &model.LinkMetadata{Gate: "all-children"}}}}}}
				}
				edgeRequireSuccess(t, e.call(t, wait, params))
				edgeReadyContains(t, e, from.ID, true)
				// The opaque external child edge cannot make these local beads
				// relatives or complete a local scheduling cycle.
				block := e.probe(producer, "external-membership-local-block")
				edgeRequireSuccess(t, e.call(t, block, edgeTypedLinkParams(producer, model.OpLink, spawner, externalChild, "blocks", "")))
				edgeReadyContains(t, e, from.ID, true)
				final := e.snapshot(t)
				e.f.reopen(t)
				edgeRequireStateEqual(t, final, e.snapshot(t))
				edgeReadyContains(t, e, from.ID, true)
			})
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
				reason         *string
				ready          []string
			}{
				{"bead.close", "done", new(string), nil},
				{"bead.reopen", "todo", nil, []string{to.ID}},
				{"bead.fail", "failed", nil, []string{from.ID}},
				{"bead.reopen", "todo", nil, []string{to.ID}},
			} {
				p := e.probe(step.method, "conditional-transition-"+model.Natural64(uint64(i)).String())
				before := e.snapshot(t)
				got := e.call(t, p, map[string]any{"id": to})
				edgeRequireSuccess(t, got)
				after := e.snapshot(t)
				edgeRequireSingleCommit(t, before, after, edgeRequireReceipt(t, after, p, got.Result, false), to)
				closedAt := &e.f.now
				if step.method == "bead.reopen" {
					closedAt = nil
				}
				edgeRequireOnlyEdgeAndFields(t, before, after, to, map[string]any{"status": step.status}, edgeExpectedClose{At: closedAt, Reason: step.reason})
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
			workflowFixture := newEphemeralCycleFixture(t, engine)
			if engine == "memory" {
				before := workflowFixture.state(t)
				got := workflowFixture.request(t, "bead.cycles", map[string]any{"includeEphemeral": true}, "b07-routing-memory")
				if got.Error == nil || got.Error.Data.Code != "plugin_durable_state_unavailable" {
					t.Fatalf("memory workflow capability=%+v", got.Error)
				}
				if after := workflowFixture.state(t); !reflect.DeepEqual(before, after) {
					t.Fatal("memory workflow capability refusal changed durable or graph state")
				}
				return
			}

			workflow := workflowFixture.invoke(t, "b07-routing-invoke", 1)[0]
			durableFixture := newEdgeContractFixture(t, engine)
			created := durableFixture.f.call(t, durableFixture.f.ctx, "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": "durable routing source"},
			}, key("b07-routing-create"), actor)
			if created.Error != nil {
				t.Fatalf("durable routing source refused: %+v", created.Error)
			}
			var commit model.Commit
			if err := json.Unmarshal(created.Result, &commit); err != nil || len(commit.Results) != 1 {
				t.Fatalf("durable routing source result=%s error=%v", created.Result, err)
			}
			durable := commit.Results[0].Ref
			attempts := []struct {
				method string
				params map[string]any
				probe  edgeReceiptProbe
			}{
				{method: "bead.link", params: map[string]any{"from": durable, "to": workflow, "kind": "blocks"}},
				{method: "bead.link", params: map[string]any{"from": workflow, "to": durable, "kind": "blocks"}},
				{method: "bead.unlink", params: map[string]any{"from": durable, "to": workflow, "kind": "blocks"}},
				{method: "bead.unlink", params: map[string]any{"from": workflow, "to": durable, "kind": "blocks"}},
				{method: "tx.apply", params: map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{edgeLinkOp(durable, workflow, "blocks")}}}},
				{method: "tx.apply", params: map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{edgeLinkOp(workflow, durable, "blocks")}}}},
				{method: "tx.apply", params: map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpUnlink, Link: &model.LinkOp{Kind: model.OpUnlink, From: durable, To: workflow, LinkKind: "blocks"}}}}}},
				{method: "tx.apply", params: map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpUnlink, Link: &model.LinkOp{Kind: model.OpUnlink, From: workflow, To: durable, LinkKind: "blocks"}}}}}},
			}
			// Register every refusal probe before taking the baseline. The observer
			// records the complete receipt key set, including absent receipts; adding
			// a probe between snapshots would make an atomic refusal look like a
			// database mutation on persistent stores.
			for i := range attempts {
				attempts[i].probe = durableFixture.probe(attempts[i].method, "b07-routing-refusal-"+model.Natural64(uint64(i)).String())
			}
			beforeDurable := durableFixture.snapshot(t)
			beforeWorkflow := workflowFixture.state(t)
			for _, attempt := range attempts {
				probe := attempt.probe
				got := durableFixture.call(t, probe, attempt.params)
				if got.Error == nil || got.Error.Data.Code != "invalid_params" || len(got.Result) != 0 {
					t.Fatalf("%s mixed-plane refusal=%+v result=%s", attempt.method, got.Error, got.Result)
				}
				if after := durableFixture.snapshot(t); !reflect.DeepEqual(beforeDurable, after) {
					t.Fatalf("%s mixed-plane refusal changed durable/graph state", attempt.method)
				}
				if after := workflowFixture.state(t); !reflect.DeepEqual(beforeWorkflow, after) {
					t.Fatalf("%s mixed-plane refusal changed workflow checkpoint state", attempt.method)
				}
			}
			durableFixture.f.reopen(t)
			workflowFixture.reopen(t)
			if after := durableFixture.snapshot(t); !reflect.DeepEqual(beforeDurable, after) {
				t.Fatal("mixed-plane refusals changed durable state after reopen")
			}
			if after := workflowFixture.state(t); !reflect.DeepEqual(beforeWorkflow, after) {
				t.Fatal("mixed-plane refusals changed workflow state after reopen")
			}
		})
	}
}

// Preserve PR381/PR371/B14's external-before-workflow controls on this distinct
// composition. Both planes use the same selected physical SQL store. Existing
// nil-provider controls above remain separate and retain their exact refusals.
func TestB07ExternalWorkflowCollisionConservesProviderAcrossProducers(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		for _, producer := range []string{"bead.link", "tx.apply", "direct"} {
			t.Run(engine+"/"+producer, func(t *testing.T) {
				e := newEdgeContractFixture(t, engine)
				from := e.create(t, "external-workflow-collision")
				install := func() {
					t.Helper()
					capability, ok := e.f.store.(driver.PersistentCheckpointStore)
					if !ok || !capability.Persistent() {
						t.Fatal("actual persistent SQL workflow capability required")
					}
					provider, err := workflowplugin.New(pluginhost.NewDurableState(capability, e.f.lease, workflowplugin.Alias, "default"))
					if err != nil {
						t.Fatal(err)
					}
					state, err := e.f.store.Meta(e.f.ctx)
					if err != nil {
						t.Fatal(err)
					}
					e.f.service = service.New(e.f.store, e.f.lease, e.f.writer,
						service.WithOntology(service.NewOntology(nil)), service.WithClock(func() time.Time { return e.f.now }),
						service.WithWorkflow(provider.WithIdentity(workflowplugin.Alias, "default", state.Incarnation)))
					registry, err := rpc.NewRegistry()
					if err != nil {
						t.Fatal(err)
					}
					if err := rpc.BindService(registry, e.f.service); err != nil {
						t.Fatal(err)
					}
					e.f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: state.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: []byte("s1-b05-b08-fixed-cursor-key-32-bytes")})
					if err != nil {
						t.Fatal(err)
					}
				}
				install()
				run, err := e.f.service.WorkflowInvoke(e.f.ctx, anonymous,
					workflowplugin.InvokeRequest{Template: "external-collision", Project: project, Mode: "ephemeral", Checklist: []workflowplugin.ChecklistItem{{Label: "item", Title: "actual collision", Type: "issue"}}},
					model.WriteMeta{Method: "x.workflow.invoke", Project: project, IdempotencyKey: key("external-collision-invoke")})
				if err != nil || run.Plane != "ephemeral" || len(run.Items) != 1 {
					t.Fatalf("actual SQL invocation=%+v error=%v", run, err)
				}
				item := model.RefByID(run.Items[0].ID, model.Natural64(0))
				method := producer
				if producer == "direct" {
					method = "bead.link"
				}
				link := e.probe(method, "collision-link")
				unlinkMethod := "bead.unlink"
				if producer == "tx.apply" {
					unlinkMethod = producer
				}
				unlink := e.probe(unlinkMethod, "collision-unlink")
				refuseLink := e.probe("bead.link", "collision-workflow-external-refusal")
				refuseUnlink := e.probe("bead.unlink", "collision-workflow-external-refusal")
				before := e.snapshot(t)
				if before.Checkpoints["plugin/workflow/default/durable"].Generation == 0 {
					t.Fatal("actual provider checkpoint absent")
				}
				for _, p := range []edgeReceiptProbe{refuseLink, refuseUnlink} {
					edgeRequireRefusal(t, e.call(t, p, edgeTypedLinkParams(p.Method, model.OpLink, item, from, "blocks", model.LinkTargetExternal)), "unsupported")
					edgeRequireStateEqual(t, before, e.snapshot(t))
				}
				write := func(p edgeReceiptProbe, op model.OpKind) envelope {
					t.Helper()
					if producer != "direct" {
						return e.call(t, p, edgeTypedLinkParams(p.Method, op, from, item, "blocks", model.LinkTargetExternal))
					}
					req := service.LinkRequest{From: from, To: item, Kind: "blocks", TargetKind: model.LinkTargetExternal}
					meta := model.WriteMeta{Method: p.Method, Project: project, Agent: actor, AgentLabel: actor, IdempotencyKey: p.Key}
					var commit model.Commit
					var err error
					if op == model.OpLink {
						commit, err = e.f.service.Link(context.Background(), anonymous, req, meta)
					} else {
						commit, err = e.f.service.Unlink(context.Background(), anonymous, req, meta)
					}
					if err != nil {
						t.Fatal(err)
					}
					raw, err := json.Marshal(commit)
					if err != nil {
						t.Fatal(err)
					}
					return envelope{Result: raw}
				}
				linked := write(link, model.OpLink)
				edgeRequireSuccess(t, linked)
				after := e.snapshot(t)
				edgeRequireSingleCommit(t, before, after, edgeRequireReceipt(t, after, link, linked.Result, false), from)
				edgeRequireLinks(t, after, from, edgeExternalLinkValue(item, "blocks"))
				if !reflect.DeepEqual(before.Checkpoints, after.Checkpoints) {
					t.Fatal("external collision entered the workflow plane")
				}
				edgeReadyContains(t, e, from.ID, false)
				live, err := e.f.service.Get(e.f.ctx, anonymous, service.GetRequest{ID: item, Project: project})
				if err != nil || live.Bead.ID != item.ID || live.Bead.Plane != "ephemeral" {
					t.Fatalf("real collision disappeared: %+v error=%v", live, err)
				}
				if _, err := e.f.service.CloseWithOutcome(e.f.ctx, anonymous, service.CloseRequest{ID: item}, model.WriteMeta{Method: "bead.close", Project: project, IdempotencyKey: key("collision-close")}); err != nil {
					t.Fatal(err)
				}
				closed := e.snapshot(t)
				if reflect.DeepEqual(after.Checkpoints, closed.Checkpoints) {
					t.Fatal("actual workflow close did not persist completion")
				}
				coreAfterClose := closed
				coreAfterClose.Checkpoints = after.Checkpoints
				edgeRequireStateEqual(t, after, coreAfterClose)
				edgeReadyContains(t, e, from.ID, false)
				e.f.reopen(t)
				install()
				edgeRequireStateEqual(t, closed, e.snapshot(t))
				edgeRequireLinks(t, closed, from, edgeExternalLinkValue(item, "blocks"))
				edgeReadyContains(t, e, from.ID, false)
				removed := write(unlink, model.OpUnlink)
				edgeRequireSuccess(t, removed)
				final := e.snapshot(t)
				edgeRequireSingleCommit(t, closed, final, edgeRequireReceipt(t, final, unlink, removed.Result, false), from)
				edgeRequireLinks(t, final, from)
				if !reflect.DeepEqual(closed.Checkpoints, final.Checkpoints) {
					t.Fatal("external unlink entered completed workflow state")
				}
				edgeReadyContains(t, e, from.ID, true)
				e.f.reopen(t)
				install()
				edgeRequireStateEqual(t, final, e.snapshot(t))
			})
		}
	}
}

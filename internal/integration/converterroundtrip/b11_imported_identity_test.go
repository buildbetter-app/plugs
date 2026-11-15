package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// The workflow provider exposes only branch-zero 128-bit item references.
// Keep this admitted fixture's source identity assertion while binding it to
// a deterministic valid public endpoint before the direct bead.link calls.
const b11AdmittedWorkflowItemID = "workflow:item/00000000000000000000000000000001"

func b11AdmittedProfile(t *testing.T, data []byte, store driver.Store, lease driver.ActiveLease) driver.RestoreImportProfile {
	t.Helper()
	profile := sourceReverseProfile(t, data, store, lease, true)
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		t.Fatal(err)
	}
	for i := range plan.Entities {
		entity := &plan.Entities[i]
		if entity.TargetKind != "workflow:item" {
			continue
		}
		entity.TargetID = b11AdmittedWorkflowItemID
		if entity.Route.ActiveTargetID != "" {
			entity.Route.ActiveTargetID = b11AdmittedWorkflowItemID
		}
	}
	// The endpoint rewrite changes the logical image; ask the binder to
	// recalculate its commitment instead of comparing the old image bytes.
	plan.Providers[0].LogicalImageSHA256, plan.Providers[0].LogicalImageSize = "", 0
	profile.SourcePlanes = nil
	profile.DerivedDigest = ""
	profile, err = BindSourceImport(context.Background(), bytes.NewReader(data), profile, plan)
	if err != nil {
		t.Fatal("rebind valid admitted workflow endpoint", err)
	}
	return profile
}

// This qualifies mutations of admitted source edges, separately from native
// capture/import qualification. The existing authored B07 fixture deliberately
// gives two source tables the same non-UUID row ID. A metadata edit or refused
// retype must retain that recorded identity and every unaffected sibling.
func TestB11AdmittedMixedEdgeMetadataIdentityAndReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx, root := context.Background(), proof.Output(t)
			data := positiveSourceArchive(t)
			proof.Write(t, root, "authored-source.tar", data)
			store, lease, reopen := openIdentityTarget(t, engine)
			profile := b11AdmittedProfile(t, data, store, lease)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil || report.SourcePlanes == nil || report.Receipt == nil || len(report.SourcePlanes.Providers) != 1 {
				t.Fatal("admitted source import missing", err)
			}
			plan, err := profile.SourcePlanes.Plan()
			if err != nil {
				t.Fatal(err)
			}
			byTarget, bySource := map[string]driver.SourcePlaneEntityV1{}, map[string]driver.SourcePlaneEntityV1{}
			for _, entity := range plan.Entities {
				byTarget[entity.TargetID], bySource[entity.Source.ID] = entity, entity
			}
			active := bySource["demo-9"]
			if active.TargetID != b11AdmittedWorkflowItemID || active.TargetKind != "workflow:item" || active.Source.Plane != dumpformat.EphemeralEntityTable {
				t.Fatal("source alias does not identify the admitted workflow endpoint")
			}
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			persistent := false
			marker, markerPresent := capability.(driver.PersistentCheckpointStore)
			if markerPresent {
				persistent = marker.Persistent()
			}
			wantLifetime := driver.CheckpointLifetimeRestart
			if engine == "memory" {
				wantLifetime = driver.CheckpointLifetimeProcess
			}
			if capability.WorkflowCheckpointLifetime() != wantLifetime || persistent != (engine != "memory") {
				t.Fatal("selected source store misrepresented lifetime or persistence")
			}
			proof.Save(t, root, "source-binding.json", map[string]any{"profile": profile, "originalReceipt": report.Receipt, "sourcePlanes": report.SourcePlanes, "archiveSHA256": proof.Hash(data), "lifetime": capability.WorkflowCheckpointLifetime(), "persistenceMarkerPresent": markerPresent, "persistent": persistent, "nativeCaptureCredit": false})
			if _, err := capability.SaveCheckpoint(ctx, lease, "b11/unrelated", 0, []byte(`{"exact":"unrelated source namespace"}`)); err != nil {
				t.Fatal(err)
			}
			domain, server := sourcePublicService(t, store, lease, profile, report, &sourcePublicPolicy{})
			if !domain.SourceRuntimeInstalled() {
				t.Fatal("public calls must use the installed admitted source runtime")
			}
			scopes := map[string]proof.Scope{"restore": {profile.Method, profile.IdempotencyKey}}
			sequence, observations := 0, 0
			call := func(method, label string, params map[string]any, want string) sourcePublicReply {
				t.Helper()
				key := proof.Hash([]byte(label))[:32]
				scopes[label] = proof.Scope{method, key}
				params["_meta"] = map[string]any{"project": "demo", "idempotencyKey": key}
				request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
				if err != nil {
					t.Fatal(err)
				}
				response := server.Dispatch(rpc.WithPrincipal(ctx, profile.Admission.Actor), request)
				sequence++
				proof.Write(t, root, fmt.Sprintf("rpc-%04d-request.json", sequence), request)
				proof.Write(t, root, fmt.Sprintf("rpc-%04d-response.json", sequence), response)
				var reply sourcePublicReply
				if err := json.Unmarshal(response, &reply); err != nil {
					t.Fatal(err)
				}
				if want != "" {
					if reply.Error == nil || reply.Error.Data.Code != want || len(reply.Result) != 0 {
						t.Fatalf("%s expected %s: %s", method, want, response)
					}
				} else if reply.Error != nil || len(reply.Result) == 0 {
					t.Fatalf("%s failed: %s", method, response)
				}
				return reply
			}
			capture := func() proof.State {
				t.Helper()
				state := proof.Capture(t, store, profile.Admission.Actor, profile.AgentLabel(), scopes)
				observations++
				proof.Save(t, root, fmt.Sprintf("state-%04d.json", observations), state)
				return state
			}
			// Independent literal expectations for all four authored source rows.
			// Metadata text is a JSON STRING token; future tokens stay opaque.
			tokens := func(metadataToken, threadToken string) map[string]string {
				return map[string]string{"beads.metadata": metadataToken, "beads.thread_id": threadToken, "beads.future_object": `{ "wide":9007199254740993, "decimal":1.00e+02 }`, "beads.future_empty": `""`, "beads.future_null": `null`, "beads.future_escaped": `"\\path\u03b1"`}
			}
			durableKey := positiveSourceTuple{"demo", dumpformat.DurableDependencyTable, "equal\x00edge", "demo-7", "demo-9", "related"}
			activeKey := positiveSourceTuple{"demo", dumpformat.EphemeralDependencyTable, "equal\x00edge", "demo-9", "demo-8", "related"}
			expected := map[positiveSourceTuple]map[string]string{
				{"demo", dumpformat.DurableDependencyTable, "edge-fixture", "demo-7.4", "demo-7", "blocks"}:       tokens(`{"extra":"kept"}`, `"thread-fixture"`),
				{"demo", dumpformat.DurableDependencyTable, "cross-project-block", "demo-7", "empty-1", "blocks"}: tokens(`"{}"`, `" raw cross-project thread "`),
				durableKey: tokens(`" {\"wide\":9007199254740993} "`, `null`),
				activeKey:  tokens(`" {\"wide\":9007199254740993} "`, `""`),
			}
			check := func(label string, state proof.State) []byte {
				t.Helper()
				positiveLiveSourceEdges(t, state, expected, byTarget)
				reversed, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
				if err != nil {
					t.Fatal("source reverse refused", err)
				}
				proof.Write(t, root, label+"-reverse.jsonl", reversed.Result.JSONL)
				actual := positiveReverseSourceEdges(t, reversed.Result.JSONL, bySource)
				if !reflect.DeepEqual(expected, actual) {
					t.Fatalf("source identity/type/token mismatch: want=%+v got=%+v", positiveEdgeList(expected), positiveEdgeList(actual))
				}
				proof.Save(t, root, label+"-edges.json", positiveEdgeList(actual))
				for _, edge := range positiveEdgeList(expected) {
					key, wantTokens := edge.Tuple, edge.Tokens
					read := call("bead.get", label+"-read-"+key.From, map[string]any{"id": model.RefByID(key.From, model.Natural64(0))}, "")
					var view struct {
						Bead struct {
							ID    string
							Links []workflowplugin.RelationView
						}
					}
					decode(t, read.Result, &view)
					from, to := bySource[key.From].TargetID, bySource[key.To].TargetID
					if view.Bead.ID != from {
						t.Fatal("public source alias changed the canonical endpoint ID")
					}
					count := 0
					for _, link := range view.Bead.Links {
						if link.From == from && link.To == model.RefByID(to, model.Natural64(0)) {
							count++
							if link.Kind != positiveSourceLiveKind(key.Kind) || link.Metadata == nil || link.Metadata.HasPredicate() || !reflect.DeepEqual(link.Metadata.NativeExtensions, wantTokens) {
								t.Fatal("public readback lost the exact source edge kind or lexical metadata")
							}
						}
					}
					if count != 1 {
						t.Fatal("public readback duplicated or dropped the source edge")
					}
				}
				proof.Same(t, state, capture())
				return reversed.Result.JSONL
			}
			ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
			metadataText := func(text string) *model.LinkMetadata {
				token, err := json.Marshal(text)
				if err != nil {
					t.Fatal(err)
				}
				return &model.LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": string(token)}}
			}
			baseline := check("initial", capture())
			type replayCase struct {
				label  string
				params map[string]any
				result json.RawMessage
			}
			var replays []replayCase
			for _, key := range []positiveSourceTuple{durableKey, activeKey} {
				label := key.From + "-to-" + key.To
				from, to := ref(key.From), ref(key.To)
				params := func(kind string, value *model.LinkMetadata) map[string]any {
					params := map[string]any{"from": from, "to": to, "kind": kind}
					if value != nil {
						params["metadata"] = value
					}
					return params
				}
				// The pinned native JCS comparison rounds these wide values equal.
				// It does not authorize rewriting the stored original text token.
				equalParams := params("relates-to", metadataText(`{"wide":9007199254740992.0}`))
				before := capture()
				equal := call("bead.link", label+"-equal-origin", equalParams, "")
				var commit model.Commit
				decode(t, equal.Result, &commit)
				after := capture()
				b11EqualSourceRelationEffect(t, before, after, commit, label+"-equal-origin")
				if !bytes.Equal(baseline, check(label+"-equal-origin", after)) {
					t.Fatal("semantic re-add rewrote the recorded source image")
				}
				replays = append(replays, replayCase{label + "-equal-origin", equalParams, equal.Result})

				changed := metadataText(` {"outer":{"n":1.0,"array":[true,null,{"text":"x"}]},"wide":9007199254740993} `)
				before = capture()
				updated := call("bead.link", label+"-nested-change", params("relates-to", changed), "")
				decode(t, updated.Result, &commit)
				after = capture()
				positiveJoinedEffect(t, before, after, commit, label+"-nested-change", "", "bead.link")
				expected[key]["beads.metadata"] = changed.NativeExtensions["beads.metadata"]
				changedReverse := check(label+"-nested-change", after)

				before = capture()
				call("bead.link", label+"-retype-refused", params("blocks", nil), "ontology_guard_failed")
				proof.Same(t, before, capture())
				call("tx.apply", label+"-late-retype-refused", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref(active.TargetID), Fields: map[string]any{"title": "discard active draft"}}},
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("demo-8"), Fields: map[string]any{"title": "discard durable draft"}}},
					{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: "blocks"}},
				}}}, "ontology_guard_failed")
				proof.Same(t, before, capture())

				nestedParams := params("relates-to", metadataText(`{"wide":9007199254740992,"outer":{"array":[true,null,{"text":"\u0078"}],"n":1e0}}`))
				equal = call("bead.link", label+"-equal-nested", nestedParams, "")
				decode(t, equal.Result, &commit)
				after = capture()
				b11EqualSourceRelationEffect(t, before, after, commit, label+"-equal-nested")
				if !bytes.Equal(changedReverse, check(label+"-equal-nested", after)) {
					t.Fatal("nested semantic equality changed raw origin/type/opaque tokens")
				}
				replays = append(replays, replayCase{label + "-equal-nested", nestedParams, equal.Result})
				baseline = changedReverse
			}
			before := capture()
			oldStore, oldEpoch := store, lease.Epoch()
			store, lease = reopen()
			if engine != "memory" && (store == oldStore || lease.Epoch() <= oldEpoch) {
				t.Fatal("SQL reopen did not replace the physical handle and lease epoch")
			}
			proof.Save(t, root, "reopen.json", map[string]any{"engine": engine, "oldStore": fmt.Sprintf("%p", oldStore), "newStore": fmt.Sprintf("%p", store), "oldEpoch": oldEpoch, "newEpoch": lease.Epoch(), "sameProcessMemory": engine == "memory"})
			bound, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.SourcePlanes, report.SourcePlanes) {
				t.Fatal("source binding/original import receipt changed after reopen", err)
			}
			_, server = sourcePublicService(t, store, lease, bound, recovered, &sourcePublicPolicy{})
			if !bytes.Equal(baseline, check("reopened", before)) {
				t.Fatal("reopen changed source row identity or lexical values")
			}
			for _, original := range replays {
				replay := call("bead.link", original.label, original.params, "")
				if !replay.Meta.Replayed || !bytes.Equal(original.result, replay.Result) {
					t.Fatal("source semantic re-add lost its original exact replay response")
				}
				proof.Same(t, before, capture())
			}
		})
	}
}

// An equivalent mixed relation publishes only the new public/provider receipts.
// Its namespace revision advances, but its item generation and lifecycle audit
// do not: no relation value changed. Every other byte remains conserved.
func b11EqualSourceRelationEffect(t *testing.T, before, after proof.State, commit model.Commit, label string) {
	t.Helper()
	receipt, found := after.Receipts[label]
	if !found || receipt.Pending || receipt.RequestHash == "" || !reflect.DeepEqual(receipt.Commit, commit) || !reflect.DeepEqual(receipt.Changeset, model.Changeset{}) || commit.Revision != before.Meta.Head || len(after.Receipts) != len(before.Receipts)+1 {
		t.Fatal("equivalent source relation lost its exact completed no-op receipt")
	}
	oldLogical, newLogical := before.Workflow(t), after.Workflow(t)
	var oldReceipts, newReceipts map[string]json.RawMessage
	if json.Unmarshal(oldLogical["receipts"], &oldReceipts) != nil || json.Unmarshal(newLogical["receipts"], &newReceipts) != nil || len(newReceipts) != len(oldReceipts)+1 {
		t.Fatal("equivalent source relation has the wrong provider receipt delta")
	}
	for key, raw := range oldReceipts {
		if !bytes.Equal(raw, newReceipts[key]) {
			t.Fatal("equivalent source relation changed an existing provider receipt")
		}
	}
	for key, raw := range newReceipts {
		if _, retained := oldReceipts[key]; retained {
			continue
		}
		var providerReceipt struct {
			Method, Key, Digest, ItemID, Project, Plugin, Instance string
			Result                                                 model.Commit
		}
		if json.Unmarshal(raw, &providerReceipt) != nil || providerReceipt.Method != "bead.link" || providerReceipt.Key == "" || providerReceipt.Digest == "" || providerReceipt.Project == "" || providerReceipt.Plugin != "workflow" || providerReceipt.Instance != "default" || len(providerReceipt.Result.Results) != 1 || providerReceipt.Result.Results[0].Ref != model.RefByID(providerReceipt.ItemID, model.Natural64(0)) {
			t.Fatal("equivalent source relation lost provider receipt identity")
		}
	}
	delete(oldLogical, "receipts")
	delete(newLogical, "receipts")
	if !reflect.DeepEqual(oldLogical, newLogical) {
		t.Fatal("equivalent source relation changed items, generation, audit or source/run identity")
	}
	proof.WorkflowNamespaceEffect(t, before, after)
	after.Receipts = maps.Clone(after.Receipts)
	delete(after.Receipts, label)
	after.Checkpoints = maps.Clone(after.Checkpoints)
	after.Checkpoints[proof.Namespace] = before.Checkpoints[proof.Namespace]
	proof.Same(t, before, after)
}

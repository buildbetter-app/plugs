package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/depid"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// This is a separate admitted public fixture. Every edge carries a lexical
// number/object plus explicit empty/null and escaped fields. Reused raw IDs in
// different source tables require the entire tuple in every comparison.
func positiveSourceArchive(t *testing.T) []byte {
	t.Helper()
	base := sourceReverseFixture(t, true)
	return rewriteData(t, func(members map[string][]byte) {
		reader := tar.NewReader(bytes.NewReader(base))
		for {
			header, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			members[header.Name], err = io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
		}
		var out bytes.Buffer
		for _, line := range bytes.Split(bytes.TrimSpace(members["records/edge.ndjson"]), []byte{'\n'}) {
			var record, source, fields, body map[string]json.RawMessage
			if json.Unmarshal(line, &record) != nil || json.Unmarshal(record["source"], &source) != nil || json.Unmarshal(source["fields"], &fields) != nil || json.Unmarshal(record["body"], &body) != nil {
				t.Fatal("invalid authored edge")
			}
			for key, token := range positiveSourceTokens() {
				fields[key] = json.RawMessage(token)
			}
			var table, id string
			if json.Unmarshal(source["table"], &table) != nil || json.Unmarshal(record["id"], &id) != nil {
				t.Fatal("invalid physical source tuple")
			}
			if id == "equal\x00edge" {
				token := `null`
				if table == dumpformat.EphemeralDependencyTable {
					token = `""`
				}
				fields["thread_id"], body["thread_id"] = json.RawMessage(token), json.RawMessage(token)
			}
			source["fields"] = positiveTokenObject(t, fields)
			record["source"] = positiveTokenObject(t, source)
			record["body"] = positiveTokenObject(t, body)
			out.Write(positiveTokenObject(t, record))
			out.WriteByte('\n')
		}
		members["records/edge.ndjson"] = out.Bytes()
	})
}

func positiveSourceTokens() map[string]string {
	return map[string]string{"future_object": `{ "wide":9007199254740993, "decimal":1.00e+02 }`, "future_empty": `""`, "future_null": `null`, "future_escaped": `"\\path\u03b1"`}
}

// json.Marshal compacts RawMessage objects. Copy validated tokens directly at
// the authored capture boundary to make lexical equality a meaningful oracle.
func positiveTokenObject(t *testing.T, fields map[string]json.RawMessage) []byte {
	t.Helper()
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out bytes.Buffer
	out.WriteByte('{')
	for i, key := range keys {
		if !json.Valid(fields[key]) {
			t.Fatal("invalid authored token", key)
		}
		if i > 0 {
			out.WriteByte(',')
		}
		name, _ := json.Marshal(key)
		out.Write(name)
		out.WriteByte(':')
		out.Write(fields[key])
	}
	out.WriteByte('}')
	return out.Bytes()
}

type positiveSourceTuple struct{ Project, Table, ID, From, To, Kind string }
type positiveSourceEdge struct {
	Tuple  positiveSourceTuple
	Tokens map[string]string
}

// Both established import projections lower raw related to live relates-to.
// The raw kind stays in the source/reverse tuple; this mapping applies only
// when comparing that tuple with the committed public link representation.
func positiveSourceLiveKind(raw string) string {
	if raw == "related" {
		return "relates-to"
	}
	return raw
}

func positiveEdgeList(edges map[positiveSourceTuple]map[string]string) []positiveSourceEdge {
	rows := make([]positiveSourceEdge, 0, len(edges))
	for key, tokens := range edges {
		rows = append(rows, positiveSourceEdge{key, tokens})
	}
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i].Tuple) < fmt.Sprint(rows[j].Tuple) })
	return rows
}

func TestB07AdmittedSourceMixedLinkSuccess(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx, root := context.Background(), proof.Output(t)
			data := positiveSourceArchive(t)
			proof.Write(t, root, "authored-source.tar", data)
			store, lease, reopen := openIdentityTarget(t, engine)
			profile := sourceReverseProfile(t, data, store, lease, true)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil || report.SourcePlanes == nil || report.Receipt == nil || len(report.SourcePlanes.Providers) != 1 {
				t.Fatal("admitted import missing", err)
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
			if active.TargetKind != "workflow:item" || active.TargetID != "workflow:item/restored" || active.Source.Plane != dumpformat.EphemeralEntityTable || report.SourcePlanes.Providers[0].Provider.Namespace != proof.Namespace {
				t.Fatal("actual admitted source/provider identity differs", active)
			}
			proof.Save(t, root, "admitted-bindings.json", map[string]any{"profile": profile, "commit": report.SourcePlanes, "originalReceipt": report.Receipt, "active": active, "archiveSHA256": proof.Hash(data), "engine": engine, "nativeHistoryCredit": false})
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := capability.SaveCheckpoint(ctx, lease, "b07positive/unrelated", 0, []byte(`{"exact":"unrelated admitted namespace"}`)); err != nil {
				t.Fatal(err)
			}
			domain, server := sourcePublicService(t, store, lease, profile, report, &sourcePublicPolicy{})
			if !domain.SourceRuntimeInstalled() {
				t.Fatal("fixture lacks admitted source runtime")
			}
			scopes := map[string]proof.Scope{"restore": {profile.Method, profile.IdempotencyKey}}
			sequence, observations := 0, 0
			call := func(t *testing.T, method, label string, params map[string]any, want string) sourcePublicReply {
				t.Helper()
				key := proof.Hash([]byte(label))[:32]
				scopes[label] = proof.Scope{method, key}
				if _, supplied := params["_meta"]; !supplied {
					params["_meta"] = map[string]any{"project": "demo", "idempotencyKey": key}
				}
				request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
				if err != nil {
					t.Fatal(err)
				}
				response := server.Dispatch(rpc.WithPrincipal(ctx, profile.Admission.Actor), request)
				sequence++
				proof.Write(t, root, fmt.Sprintf("rpc-%04d-request.json", sequence), request)
				proof.Write(t, root, fmt.Sprintf("rpc-%04d-response.json", sequence), response)
				var reply sourcePublicReply
				if json.Unmarshal(response, &reply) != nil {
					t.Fatalf("malformed public reply: %s", response)
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
			capture := func(t *testing.T) proof.State {
				t.Helper()
				state := proof.Capture(t, store, profile.Admission.Actor, profile.AgentLabel(), scopes)
				observations++
				proof.Save(t, root, fmt.Sprintf("state-%04d.json", observations), state)
				return state
			}
			expected := map[positiveSourceTuple]map[string]string{}
			for _, edge := range report.Archive.Records("edge") {
				tokens, err := dumpformat.NativeEdgeExtensions(edge)
				if err != nil {
					t.Fatal(err)
				}
				key := positiveSourceTuple{edge.Project, edge.Source.Table, edge.ID, stringValue(edge.Body["from"]), stringValue(edge.Body["to"]), stringValue(edge.Body["type"])}
				if _, duplicate := expected[key]; duplicate {
					t.Fatal("duplicate complete source edge identity")
				}
				expected[key] = tokens
			}
			if len(expected) != 4 {
				t.Fatal("independent source fixture edge census differs", len(expected))
			}
			kindBindings := []struct {
				Source   positiveSourceTuple
				LiveKind string
			}{}
			for _, edge := range positiveEdgeList(expected) {
				kindBindings = append(kindBindings, struct {
					Source   positiveSourceTuple
					LiveKind string
				}{edge.Tuple, positiveSourceLiveKind(edge.Tuple.Kind)})
			}
			proof.Save(t, root, "source-live-kind-bindings.json", kindBindings)
			check := func(t *testing.T, label string, state proof.State, titles map[string]string) []byte {
				t.Helper()
				positiveLiveSourceEdges(t, state, expected, byTarget)
				reversed, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
				if err != nil {
					t.Fatal("mixed source reverse refused", err)
				}
				proof.Write(t, root, label+"-reverse.jsonl", reversed.Result.JSONL)
				actual := positiveReverseSourceEdges(t, reversed.Result.JSONL, bySource)
				proof.Save(t, root, label+"-edges.json", positiveEdgeList(actual))
				if !reflect.DeepEqual(expected, actual) {
					t.Fatalf("source/reverse complete tuple or exact token mismatch\nwant=%+v\ngot=%+v", positiveEdgeList(expected), positiveEdgeList(actual))
				}
				rows := sourceReverseRows(t, reversed.Result.JSONL)
				for id, title := range titles {
					if rows[id]["title"] != title {
						t.Fatal("reverse exact patch readback differs", id)
					}
				}
				proof.Same(t, state, capture(t))
				return reversed.Result.JSONL
			}
			before := capture(t)
			check(t, "initial", before, nil)
			metadata := &model.LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": `" {\"public\":9007199254740993,\"n\":1.00e+02} "`, "beads.thread_id": `"\u03b1\\thread"`, "beads.future_empty": `""`, "beads.future_null": `null`, "beads.future_object": `{ "wide":9007199254740993, "n":1.00e+02 }`}}
			ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
			links := []model.Op{
				{Kind: model.OpLink, Link: &model.LinkOp{From: ref("demo-9"), To: ref("demo-7"), LinkKind: "relates-to", Metadata: metadata}},
				{Kind: model.OpLink, Link: &model.LinkOp{From: ref("demo-8"), To: ref(active.TargetID), LinkKind: "relates-to", Metadata: metadata}},
			}
			change := model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("demo-9"), Fields: map[string]any{"title": "admitted active committed"}}},
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("demo-8"), Fields: map[string]any{"title": "admitted durable committed"}}},
				links[0], links[1],
			}}
			first := call(t, "tx.apply", "mixed-link-success", map[string]any{"changeSet": change}, "")
			var committed model.Commit
			decode(t, first.Result, &committed)
			if first.Meta.Replayed || len(committed.Results) != 4 || committed.Results[0].Ref.ID != active.TargetID || committed.Results[1].Ref.ID != "demo-8" || committed.Results[2].Ref.ID != active.TargetID || committed.Results[3].Ref.ID != active.TargetID {
				t.Fatal("source mixed results lost request order/actual endpoints", committed)
			}
			after := capture(t)
			positiveJoinedEffect(t, before, after, committed, "mixed-link-success", "demo-8", "bead.update", "bead.link", "bead.link")
			for _, pair := range [][2]string{{"demo-9", "demo-7"}, {"demo-8", "demo-9"}} {
				table := dumpformat.DurableDependencyTable
				if pair[0] == "demo-9" {
					table = dumpformat.EphemeralDependencyTable
				}
				expected[positiveSourceTuple{"demo", table, depid.New(pair[0], pair[1]), pair[0], pair[1], "relates-to"}] = metadata.NativeExtensions
			}
			titles := map[string]string{"demo-9": "admitted active committed", "demo-8": "admitted durable committed"}
			reverseAfter := check(t, "mixed-success", after, titles)
			alias := call(t, "bead.get", "source-address-read", map[string]any{"id": ref("demo-9")}, "")
			explicit := call(t, "bead.get", "active-target-read", map[string]any{"id": ref(active.TargetID)}, "")
			if !bytes.Equal(alias.Result, explicit.Result) {
				t.Fatal("source address did not resolve to the actual active target")
			}
			var view struct {
				Bead struct {
					ID, Project, Kind, Plugin, Plane string
					Fields                           map[string]any
				} `json:"bead"`
			}
			decode(t, explicit.Result, &view)
			if view.Bead.ID != active.TargetID || view.Bead.Fields["title"] != titles["demo-9"] || view.Bead.Project != "demo" || view.Bead.Kind != workflowplugin.ItemKind || view.Bead.Plugin != workflowplugin.Alias || view.Bead.Plane != "ephemeral" {
				t.Fatal("public admitted readback changed provider identity/title", view)
			}
			proof.Same(t, after, capture(t))
			t.Run("ReplayAndPhysicalReopen", func(t *testing.T) {
				replayed := call(t, "tx.apply", "mixed-link-success", map[string]any{"changeSet": change}, "")
				if !replayed.Meta.Replayed || !bytes.Equal(first.Result, replayed.Result) {
					t.Fatal("mixed source replay changed exact result/flag")
				}
				proof.Same(t, after, capture(t))
				oldStore, epoch := store, lease.Epoch()
				store, lease = reopen()
				if engine != "memory" && (store == oldStore || lease.Epoch() <= epoch) {
					t.Fatal("SQL reopen lacks distinct handle/increasing epoch")
				}
				proof.Save(t, root, "reopen.json", map[string]any{"engine": engine, "oldStore": fmt.Sprintf("%p", oldStore), "newStore": fmt.Sprintf("%p", store), "oldEpoch": epoch, "newEpoch": lease.Epoch(), "sameProcessMemory": engine == "memory"})
				bound, recovered, found, err := RecoverBoundSourceImport(ctx, store)
				if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.SourcePlanes, report.SourcePlanes) {
					t.Fatal("reopen changed actual admitted binding/receipt", err)
				}
				_, server = sourcePublicService(t, store, lease, bound, recovered, &sourcePublicPolicy{})
				replayed = call(t, "tx.apply", "mixed-link-success", map[string]any{"changeSet": change}, "")
				if !replayed.Meta.Replayed || !bytes.Equal(first.Result, replayed.Result) {
					t.Fatal("reopened source mixed replay changed result")
				}
				if !bytes.Equal(reverseAfter, check(t, "replayed-reopened", after, titles)) {
					t.Fatal("reopen/replay changed reverse bytes")
				}
			})
			t.Run("ChangedRequestKeyReuseConservesBothPlanes", func(t *testing.T) {
				changed := model.ChangeSet{Ops: append([]model.Op(nil), change.Ops...)}
				changed.Ops[0] = model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("demo-9"), Fields: map[string]any{"title": "same key different request"}}}
				call(t, "tx.apply", "mixed-link-success", map[string]any{"changeSet": changed}, "idempotency_key_reused")
				proof.Same(t, after, capture(t))
			})
			t.Run("LateRetypeRefusalRollsBackBothPlanes", func(t *testing.T) {
				bad := model.ChangeSet{Ops: []model.Op{
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("demo-9"), Fields: map[string]any{"title": "discard active draft"}}},
					{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("demo-8"), Fields: map[string]any{"title": "discard durable draft"}}},
					{Kind: model.OpUnlink, Link: &model.LinkOp{From: ref("demo-9"), To: ref("demo-7"), LinkKind: "relates-to"}},
					{Kind: model.OpLink, Link: &model.LinkOp{From: ref("demo-9"), To: ref("demo-8"), LinkKind: "blocks"}},
				}}
				call(t, "tx.apply", "late-retype", map[string]any{"changeSet": bad}, "ontology_guard_failed")
				if !bytes.Equal(reverseAfter, check(t, "late-retype-refused", after, titles)) {
					t.Fatal("late refusal changed reverse bytes")
				}
			})
			t.Run("MalformedOpaqueTokenConservesBothPlanes", func(t *testing.T) {
				// Dispatch malformed opaque text to the server rather than asking
				// the valid-only ChangeSet marshaler to accept it locally.
				bad := map[string]any{"ops": []any{change.Ops[0], change.Ops[1], map[string]any{
					"kind": model.OpLink, "from": ref("demo-9"), "to": ref("demo-7"), "linkKind": "relates-to",
					"metadata": map[string]any{"nativeExtensions": map[string]string{"beads.future_invalid": "null true"}},
				}}}
				call(t, "tx.apply", "malformed-opaque", map[string]any{"changeSet": bad}, "invalid_params")
				proof.Same(t, after, capture(t))
			})
			t.Run("TypedUnsupportedAndWrongProjectConserveBothPlanes", func(t *testing.T) {
				bad := model.ChangeSet{Ops: []model.Op{change.Ops[0], change.Ops[1], {Kind: model.OpLink, Link: &model.LinkOp{From: ref(active.TargetID), To: ref("remote"), TargetKind: model.LinkTargetExternal, LinkKind: "relates-to"}}}}
				call(t, "tx.apply", "unsupported-external", map[string]any{"changeSet": bad}, "unsupported")
				proof.Same(t, after, capture(t))
				call(t, "tx.apply", "wrong-project", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref(active.TargetID), Fields: map[string]any{"title": "never outside admitted project"}}}}}, "_meta": map[string]any{"project": "empty", "idempotencyKey": proof.Hash([]byte("wrong-project"))[:32]}}, "not_found")
				proof.Same(t, after, capture(t))
			})
			t.Run("ExactUnlinkRelinkAndReplay", func(t *testing.T) {
				unlink := model.ChangeSet{Ops: []model.Op{{Kind: model.OpUnlink, Link: &model.LinkOp{From: ref(active.TargetID), To: ref("demo-7"), LinkKind: "relates-to"}}}}
				removed := call(t, "tx.apply", "unlink-success", map[string]any{"changeSet": unlink}, "")
				var commit model.Commit
				decode(t, removed.Result, &commit)
				unlinked := capture(t)
				positiveJoinedEffect(t, after, unlinked, commit, "unlink-success", "", "bead.unlink")
				key := positiveSourceTuple{"demo", dumpformat.EphemeralDependencyTable, depid.New("demo-9", "demo-7"), "demo-9", "demo-7", "relates-to"}
				delete(expected, key)
				check(t, "unlinked", unlinked, titles)
				added := call(t, "tx.apply", "relink-success", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{links[0]}}}, "")
				decode(t, added.Result, &commit)
				relinked := capture(t)
				positiveJoinedEffect(t, unlinked, relinked, commit, "relink-success", "", "bead.link")
				expected[key] = metadata.NativeExtensions
				if !bytes.Equal(reverseAfter, check(t, "relinked", relinked, titles)) {
					t.Fatal("unlink/relink changed exact reverse representation")
				}
				replayed := call(t, "tx.apply", "mixed-link-success", map[string]any{"changeSet": change}, "")
				if !replayed.Meta.Replayed || !bytes.Equal(first.Result, replayed.Result) {
					t.Fatal("replay after later writes lost original result")
				}
				proof.Same(t, relinked, capture(t))
			})
			t.Run("ExistingOriginMetadataChangeReplayAndReverse", func(t *testing.T) {
				before := capture(t)
				key := positiveSourceTuple{"demo", dumpformat.EphemeralDependencyTable, "equal\x00edge", "demo-9", "demo-8", "related"}
				original, present := expected[key]
				if !present || original["beads.thread_id"] != `""` {
					t.Fatal("existing raw origin/thread fixture missing")
				}
				changedTokens := map[string]string{
					"beads.metadata":      `" {\"changed\":9007199254740993,\"n\":7.00e+03} "`,
					"beads.future_object": `{ "wide":9007199254740993, "changed":7.00e+03 }`,
				}
				change := model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: ref("demo-9"), To: ref("demo-8"), LinkKind: "relates-to", Metadata: &model.LinkMetadata{NativeExtensions: changedTokens}}}}}
				updated := call(t, "tx.apply", "existing-origin-metadata", map[string]any{"changeSet": change}, "")
				var commit model.Commit
				decode(t, updated.Result, &commit)
				after := capture(t)
				positiveJoinedEffect(t, before, after, commit, "existing-origin-metadata", "", "bead.link")
				wantTokens := map[string]string{}
				for field, token := range original {
					wantTokens[field] = token
				}
				for field, token := range changedTokens {
					wantTokens[field] = token
				}
				expected[key] = wantTokens
				reverse := check(t, "existing-origin-metadata", after, titles)
				replayed := call(t, "tx.apply", "existing-origin-metadata", map[string]any{"changeSet": change}, "")
				if !replayed.Meta.Replayed || !bytes.Equal(updated.Result, replayed.Result) {
					t.Fatal("existing-origin metadata replay changed exact result/flag")
				}
				if !bytes.Equal(reverse, check(t, "existing-origin-metadata-replayed", after, titles)) {
					t.Fatal("existing-origin replay changed raw identity/type/thread/token bytes")
				}
			})
		})
	}
}

func positiveJoinedEffect(t *testing.T, before, after proof.State, commit model.Commit, label, changedCore string, methods ...string) {
	t.Helper()
	if len(commit.Results) == 0 {
		t.Fatal("joined publication has no operation results")
	}
	title := ""
	if changedCore != "" {
		title = "admitted active committed"
	}
	proof.WorkflowEffect(t, before, after, commit.Results[0].Ref.ID, title, methods...)
	head, err := before.Meta.Head.Uint64()
	if err != nil || after.Meta.Head != model.Natural64(head+1) || commit.Revision != after.Meta.Head || len(after.Changes) != len(before.Changes)+1 || !reflect.DeepEqual(before.Changes, after.Changes[:len(before.Changes)]) || after.Checkpoints[proof.Namespace].Generation != before.Checkpoints[proof.Namespace].Generation+1 {
		t.Fatal("expected exactly one joined core/history/namespace effect")
	}
	if len(before.Rows) != len(after.Rows) || len(before.Checkpoints) != len(after.Checkpoints) || len(after.Receipts) != len(before.Receipts)+1 || after.Receipts[label].Pending || !reflect.DeepEqual(after.Receipts[label].Commit, commit) {
		t.Fatal("joined publication lost rows/namespaces/exact nonpending receipt")
	}
	expectedMeta := before.Meta
	expectedMeta.Head = after.Meta.Head
	if after.Meta != expectedMeta || !reflect.DeepEqual(after.Receipts[label].Changeset, after.Changes[len(before.Changes)]) {
		t.Fatal("joined publication changed store identity or receipt/log equality")
	}
	proof.DerivedTitleEffect(t, before, after, changedCore)
	for key, receipt := range before.Receipts {
		if !reflect.DeepEqual(receipt, after.Receipts[key]) {
			t.Fatal("previous receipt changed", key)
		}
	}
	for key, checkpoint := range before.Checkpoints {
		if key != proof.Namespace && !reflect.DeepEqual(checkpoint, after.Checkpoints[key]) {
			t.Fatal("unrelated namespace changed", key)
		}
	}
	for _, row := range before.Rows {
		if row.Ref.ID == changedCore {
			if len(after.Versions[row.Ref.ID]) != len(before.Versions[row.Ref.ID])+1 {
				t.Fatal("changed durable version count differs")
			}
			next := after.Row(t, row.Ref.ID)
			if next.Kind != row.Kind || next.Ref != row.Ref || next.Revision != commit.Revision {
				t.Fatal("mixed patch changed the durable row identity/revision")
			}
			oldDoc, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				t.Fatal(err)
			}
			nextDoc, err := pipeline.DecodeDocument(after.Row(t, row.Ref.ID).Body)
			if err != nil {
				t.Fatal(err)
			}
			if nextDoc.Fields["title"] != "admitted durable committed" {
				t.Fatal("exact durable title is missing")
			}
			oldDoc.Fields["title"] = nextDoc.Fields["title"]
			if !reflect.DeepEqual(oldDoc, nextDoc) || !reflect.DeepEqual(before.Versions[row.Ref.ID], after.Versions[row.Ref.ID][:len(before.Versions[row.Ref.ID])]) {
				t.Fatal("mixed patch lost retained fields/versions")
			}
			continue
		}
		if !reflect.DeepEqual(row, after.Row(t, row.Ref.ID)) || !reflect.DeepEqual(before.Versions[row.Ref.ID], after.Versions[row.Ref.ID]) {
			t.Fatal("unrelated core row/version changed", row.Ref.ID)
		}
	}
	if changedCore == "" && !reflect.DeepEqual(before.Derived, after.Derived) {
		t.Fatal("link-only mixed publication changed ordinary derived state")
	}
	oldLogical, newLogical := before.Workflow(t), after.Workflow(t)
	for _, key := range []string{"source", "runs"} {
		if !bytes.Equal(oldLogical[key], newLogical[key]) {
			t.Fatal("mixed link changed admitted origin/run identity", key)
		}
	}
	var oldReceipts, newReceipts map[string]json.RawMessage
	if json.Unmarshal(oldLogical["receipts"], &oldReceipts) != nil || json.Unmarshal(newLogical["receipts"], &newReceipts) != nil {
		t.Fatal("invalid provider receipts")
	}
	want := 1
	if changedCore != "" {
		want = 3
	}
	if len(newReceipts) != len(oldReceipts)+want {
		t.Fatal("joined provider receipt delta differs")
	}
	for key, receipt := range oldReceipts {
		if !bytes.Equal(receipt, newReceipts[key]) {
			t.Fatal("mixed link changed retained provider receipt")
		}
	}
}

func positiveLiveSourceEdges(t *testing.T, state proof.State, expected map[positiveSourceTuple]map[string]string, entities map[string]driver.SourcePlaneEntityV1) {
	t.Helper()
	actual := map[positiveSourceTuple]map[string]string{}
	add := func(fromID, toID, kind string, metadata *model.LinkMetadata) {
		t.Helper()
		from, to := entities[fromID], entities[toID]
		if from.Source.ID == "" || to.Source.ID == "" {
			t.Fatal("live edge lacks actual admitted source endpoint")
		}
		var key positiveSourceTuple
		count := 0
		for candidate := range expected {
			if candidate.Project == from.Source.Project && candidate.From == from.Source.ID && candidate.To == to.Source.ID && positiveSourceLiveKind(candidate.Kind) == kind {
				key = candidate
				count++
			}
		}
		if count != 1 || metadata == nil {
			t.Fatal("live edge does not have one complete source tuple/token projection", fromID, toID, kind)
		}
		if _, duplicate := actual[key]; duplicate {
			t.Fatal("duplicate live mixed edge")
		}
		actual[key] = metadata.NativeExtensions
	}
	for _, row := range state.Rows {
		if row.Kind != "bead" {
			continue
		}
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			t.Fatal(err)
		}
		for _, link := range doc.Links {
			add(doc.ID, link.To.ID, link.Kind, link.Metadata)
		}
	}
	var items map[string]struct{ Links, Inbound []workflowplugin.RelationView }
	if json.Unmarshal(state.Workflow(t)["items"], &items) != nil {
		t.Fatal("invalid live workflow state")
	}
	for _, item := range items {
		for _, link := range append(item.Links, item.Inbound...) {
			add(link.From, link.To.ID, link.Kind, link.Metadata)
		}
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("source/live exact edge tuple/token mismatch\nwant=%+v\ngot=%+v", positiveEdgeList(expected), positiveEdgeList(actual))
	}
}

func positiveReverseSourceEdges(t *testing.T, data []byte, entities map[string]driver.SourcePlaneEntityV1) map[positiveSourceTuple]map[string]string {
	t.Helper()
	result := map[positiveSourceTuple]map[string]string{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var issue struct {
			ID           string                       `json:"id"`
			Dependencies []map[string]json.RawMessage `json:"dependencies"`
		}
		if err := decoder.Decode(&issue); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		for _, edge := range issue.Dependencies {
			text := func(name string) string {
				var value string
				if raw, ok := edge[name]; ok {
					if err := json.Unmarshal(raw, &value); err != nil {
						t.Fatal(name, err)
					}
				}
				return value
			}
			from := entities[issue.ID]
			table := dumpformat.DurableDependencyTable
			if from.Source.Plane == dumpformat.EphemeralEntityTable {
				table = dumpformat.EphemeralDependencyTable
			}
			key := positiveSourceTuple{from.Source.Project, table, text("id"), text("issue_id"), text("depends_on_id"), text("type")}
			if key.From != issue.ID || key.Project == "" || key.ID == "" {
				t.Fatal("reverse edge lost source/endpoints")
			}
			tokens := map[string]string{}
			for name, value := range edge {
				if dumpformat.NativeEdgeField(name) && name != "depends_on_id" && name != "metadata" {
					tokens["beads."+name] = string(value)
				}
			}
			var metadata map[string]json.RawMessage
			if err := json.Unmarshal([]byte(text("metadata")), &metadata); err != nil {
				t.Fatal("reverse edge lacks metadata identity envelope", err)
			}
			var id string
			if err := json.Unmarshal(metadata["_plug_edge_id"], &id); err != nil || id != key.ID {
				t.Fatal("reverse metadata lost exact raw source edge ID")
			}
			if token, present := metadata["_plug_metadata"]; present {
				tokens["beads.metadata"] = string(token)
			}
			if _, duplicate := result[key]; duplicate {
				t.Fatal("reverse duplicated complete source tuple")
			}
			result[key] = tokens
		}
	}
	return result
}

package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func b07SourceOpaqueTokens() map[string]string {
	return map[string]string{
		"future_escaped": `"\\path\u03b1"`,
		"future_number":  `1.00e+02`,
		"future_object":  `{ "wide":9007199254740993, "empty":"", "null":null }`,
	}
}

func b07SourceOpaqueArchive(t *testing.T) []byte {
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
			var record, source, fields map[string]json.RawMessage
			if json.Unmarshal(line, &record) != nil || json.Unmarshal(record["source"], &source) != nil || json.Unmarshal(source["fields"], &fields) != nil {
				t.Fatal("invalid authored edge input")
			}
			for key, token := range b07SourceOpaqueTokens() {
				fields[key] = json.RawMessage(token)
			}
			source["fields"] = b07SourceObject(t, fields)
			record["source"] = b07SourceObject(t, source)
			out.Write(b07SourceObject(t, record))
			out.WriteByte('\n')
		}
		members["records/edge.ndjson"] = out.Bytes()
	})
}

func b07SourceObject(t *testing.T, fields map[string]json.RawMessage) []byte {
	t.Helper()
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out bytes.Buffer
	out.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			out.WriteByte(',')
		}
		if !json.Valid(fields[key]) {
			t.Fatal("invalid authored token", key)
		}
		raw, _ := json.Marshal(key)
		out.Write(raw)
		out.WriteByte(':')
		out.Write(fields[key])
	}
	out.WriteByte('}')
	return out.Bytes()
}

func TestB07SourcePlaneOpaqueEdgeRoundTrip(t *testing.T) {
	data := b07SourceOpaqueArchive(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			root := filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), strings.ReplaceAll(t.Name(), "/", "_"))
			if !filepath.IsAbs(root) {
				t.Fatal("absolute output directory required")
			}
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			write := func(name string, raw []byte) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			write("authored-source.tar", data)
			store, lease, reopen := openIdentityTarget(t, engine)
			profile := sourceReverseProfile(t, data, store, lease, true)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			if report.SourcePlanes == nil {
				t.Fatal("source plane import absent")
			}
			snapshot := func() []byte {
				t.Helper()
				capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
				if err != nil {
					t.Fatal(err)
				}
				checkpoint, err := capability.LoadCheckpoint(ctx, report.SourcePlanes.Providers[0].Provider.Namespace)
				if err != nil {
					t.Fatal(err)
				}
				core := savedLabelImport(t, store, report.Archive, profile, strings.Repeat("6", 32))
				liveEdges := 0
				metadata := func(value *model.LinkMetadata) {
					t.Helper()
					if value == nil {
						t.Fatal("live source edge lost opaque metadata")
					}
					for key, token := range b07SourceOpaqueTokens() {
						if value.NativeExtensions["beads."+key] != token {
							t.Fatalf("live source edge changed %s token", key)
						}
					}
					liveEdges++
				}
				for _, row := range core.Records {
					if row.Kind != "bead" {
						continue
					}
					doc, err := pipeline.DecodeDocument(row.Body)
					if err != nil {
						t.Fatal(err)
					}
					for _, link := range doc.Links {
						metadata(link.Metadata)
					}
					if doc.ParentMetadata != nil {
						metadata(doc.ParentMetadata)
					}
				}
				type relation struct {
					Metadata *model.LinkMetadata `json:"metadata"`
				}
				var namespace struct {
					Values map[string]struct {
						Value struct {
							Items map[string]struct{ Links, Inbound []relation }
						}
					}
				}
				if err := json.Unmarshal(checkpoint.Payload, &namespace); err != nil {
					t.Fatal(err)
				}
				state, exists := namespace.Values["workflow/state"]
				if !exists || len(state.Value.Items) == 0 {
					t.Fatal("source workflow checkpoint missing items")
				}
				for _, item := range state.Value.Items {
					for _, link := range append(item.Links, item.Inbound...) {
						metadata(link.Metadata)
					}
				}
				if liveEdges != len(report.Archive.Records("edge")) {
					t.Fatal("live edge token census differs", liveEdges)
				}
				return encode(t, struct {
					Core      labelImportState
					Namespace driver.Checkpoint
				}{core, checkpoint})
			}
			before := snapshot()
			write("state-before.json", before)
			wantTitles := map[string]string{}
			check := func(label string) {
				t.Helper()
				reversed, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
				if err != nil {
					t.Fatal(err)
				}
				write(label+"-reverse.jsonl", reversed.Result.JSONL)
				current := sourceReverseRows(t, reversed.Result.JSONL)
				for id, title := range wantTitles {
					if current[id]["title"] != title {
						t.Fatalf("mixed public write missing from reverse: %s", id)
					}
				}
				count := 0
				decoder := json.NewDecoder(bytes.NewReader(reversed.Result.JSONL))
				for {
					var issue struct {
						Dependencies []map[string]json.RawMessage `json:"dependencies"`
					}
					if err := decoder.Decode(&issue); err == io.EOF {
						break
					} else if err != nil {
						t.Fatal(err)
					}
					for _, dependency := range issue.Dependencies {
						count++
						for key, token := range b07SourceOpaqueTokens() {
							if string(dependency[key]) != token {
								t.Fatalf("mixed source reverse changed %s token: %s", key, dependency[key])
							}
						}
					}
				}
				if count != len(report.Archive.Records("edge")) {
					t.Fatal("reverse edge census differs")
				}
				after := snapshot()
				write(label+"-state.json", after)
				if !bytes.Equal(before, after) {
					t.Fatal("reverse or reopen changed core/source/namespace state")
				}
			}
			check("initial")
			oldStore, oldEpoch := store, lease.Epoch()
			store, lease = reopen()
			if engine != "memory" && (store == oldStore || lease.Epoch() <= oldEpoch) {
				t.Fatal("physical SQL reopen missing")
			}
			t.Logf("B07_SOURCE_REOPEN engine=%s old=%p new=%p epoch=%v->%v", engine, oldStore, store, oldEpoch, lease.Epoch())
			check("reopened")
			_, server := sourcePublicService(t, store, lease, profile, report, &sourcePublicPolicy{})
			call := func(label, method string, params map[string]any) sourcePublicReply {
				t.Helper()
				request := encode(t, map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
				raw := server.Dispatch(rpc.WithPrincipal(ctx, profile.Admission.Actor), request)
				write(label+"-request.json", request)
				write(label+"-response.json", raw)
				var reply sourcePublicReply
				if err := json.Unmarshal(raw, &reply); err != nil {
					t.Fatal(err)
				}
				return reply
			}
			// Existing admitted source support exposes a real mixed Writer on
			// all three stores. This does not claim a normal memory provider.
			mixed := map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-9", model.Natural64(0)), Fields: map[string]any{"title": "B07 committed active"}}},
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-8", model.Natural64(0)), Fields: map[string]any{"title": "B07 committed durable"}}},
			}}, "_meta": map[string]any{"project": "demo", "idempotencyKey": strings.Repeat("6", 32)}}
			var prior, later struct {
				Core      labelImportState
				Namespace driver.Checkpoint
			}
			decode(t, before, &prior)
			first := call("mixed-success", "tx.apply", mixed)
			if first.Error != nil {
				t.Fatal("source mixed public write refused", first.Error)
			}
			var commit model.Commit
			decode(t, first.Result, &commit)
			before = snapshot()
			write("mixed-state.json", before)
			decode(t, before, &later)
			head, err := prior.Core.Meta.Head.Uint64()
			if err != nil || later.Core.Meta.Head != model.Natural64(head+1) || commit.Revision != later.Core.Meta.Head || later.Namespace.Generation != prior.Namespace.Generation+1 || len(later.Core.Log) != len(prior.Core.Log)+1 || len(commit.Results) != 2 || commit.Results[0].Ref.ID != "workflow:item/restored" || commit.Results[1].Ref.ID != "demo-8" || prior.Core.WriteReceipt != nil || later.Core.WriteReceipt == nil || later.Core.WriteReceipt.ResponsePending {
				t.Fatal("mixed write did not commit once in both planes with original operation order")
			}
			wantTitles = map[string]string{"demo-9": "B07 committed active", "demo-8": "B07 committed durable"}
			check("mixed-success")
			oldStore, oldEpoch = store, lease.Epoch()
			store, lease = reopen()
			if engine != "memory" && (oldStore == store || lease.Epoch() <= oldEpoch) {
				t.Fatal("mixed write physical reopen missing")
			}
			t.Logf("B07_MIXED_REOPEN engine=%s old=%p new=%p epoch=%v->%v", engine, oldStore, store, oldEpoch, lease.Epoch())
			_, server = sourcePublicService(t, store, lease, profile, report, &sourcePublicPolicy{})
			replay := call("mixed-replay", "tx.apply", mixed)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatal("reopened mixed public receipt was not replayed")
			}
			check("mixed-replayed")
			// The source archive already has demo-9 -> demo-8 as related.
			// A different kind is a genuine retype refusal after both drafts.
			late := call("mixed-late-relation-route", "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-9", model.Natural64(0)), Fields: map[string]any{"title": "discard active draft"}}},
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-8", model.Natural64(0)), Fields: map[string]any{"title": "discard durable draft"}}},
				{Kind: model.OpLink, Link: &model.LinkOp{From: model.RefByID("demo-9", model.Natural64(0)), To: model.RefByID("demo-8", model.Natural64(0)), LinkKind: "blocks"}},
			}}, "_meta": map[string]any{"project": "demo", "idempotencyKey": strings.Repeat("8", 32)}})
			if late.Error == nil || late.Error.Data.Code != "ontology_guard_failed" {
				t.Fatal("late mixed relation preparation was not refused", late.Error)
			}
			check("mixed-late-relation-refused")
			params := map[string]any{"changeSet": map[string]any{"ops": []any{
				map[string]any{"kind": "patch", "target": model.RefByID("demo-8", model.Natural64(0)), "fields": map[string]any{"title": "must not publish"}},
				map[string]any{"kind": "patch", "target": model.RefByID("demo-9", model.Natural64(0)), "fields": map[string]any{"title": "active must not publish"}},
				map[string]any{"kind": "link", "from": model.RefByID("demo-9", model.Natural64(0)), "to": model.RefByID("demo-8", model.Natural64(0)), "linkKind": "related", "metadata": map[string]any{"nativeExtensions": map[string]string{"beads.future_invalid": "null true"}}},
			}}, "_meta": map[string]any{"project": "demo", "idempotencyKey": strings.Repeat("7", 32)}}
			request := encode(t, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tx.apply", "params": params})
			raw := server.Dispatch(rpc.WithPrincipal(ctx, profile.Admission.Actor), request)
			write("malformed-request.json", request)
			write("malformed-response.json", raw)
			var reply sourcePublicReply
			if json.Unmarshal(raw, &reply) != nil || reply.Error == nil || reply.Error.Data.Code != "invalid_params" {
				t.Fatalf("malformed opaque mixed batch: %s", raw)
			}
			after := snapshot()
			write("malformed-state.json", after)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("malformed mixed batch published either plane")
			}
			write("bindings.json", encode(t, map[string]any{"archive_sha256": hashBytes(data), "request_sha256": hashBytes(request), "response_sha256": hashBytes(raw), "source": "authored complete source-plane fixture", "native_history_credit": false}))
		})
	}
}

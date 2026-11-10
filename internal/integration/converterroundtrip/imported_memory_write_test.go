package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type importedMemoryWriteScope struct {
	method string
	key    int
}

type importedMemoryWriteState struct {
	Meta        driver.Meta
	Records     []driver.Record
	Versions    map[string]driver.Page[driver.Version]
	Changes     []model.Changeset
	Derived     map[driver.DerivedTable][]driver.DerivedRow
	Receipts    map[string]*driver.IdempotencyRecord
	Checkpoints map[string]driver.Checkpoint
}

func importedMemoryWriteSnapshot(t *testing.T, store driver.Store, lease driver.ActiveLease, refs []model.Ref, scopes []importedMemoryWriteScope, namespaces []string) importedMemoryWriteState {
	t.Helper()
	ctx := context.Background()
	core := reverseState(t, store)
	s := importedMemoryWriteState{Meta: core.Meta, Records: core.Records, Versions: map[string]driver.Page[driver.Version]{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[string]*driver.IdempotencyRecord{}, Checkpoints: map[string]driver.Checkpoint{}}
	var r driver.Reader
	var joint driver.SourcePlaneSnapshot
	var err error
	view := driver.View{Incarnation: s.Meta.Incarnation}
	if len(namespaces) != 0 {
		capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
		if err != nil {
			t.Fatal(err)
		}
		joint, err = capability.ReadSourcePlaneSnapshot(ctx, view, namespaces)
		if err != nil {
			t.Fatal(err)
		}
		r = joint
	} else {
		r, err = store.Read(ctx, view)
		if err != nil {
			t.Fatal(err)
		}
	}
	defer r.Close()
	for _, ref := range refs {
		page, err := r.Versions(ctx, ref, driver.PageReq{Limit: 1000})
		if err != nil || page.NextCursor != nil {
			t.Fatalf("incomplete imported-memory versions: %v", err)
		}
		s.Versions[ref.ID] = page
	}
	var cursor *driver.Cursor
	if !s.Meta.HistoryFloor.IsZero() {
		cursor = &driver.Cursor{Incarnation: s.Meta.Incarnation, Revision: s.Meta.HistoryFloor, Index: model.Natural64(0)}
	}
	s.Changes, err = r.Changesets(ctx, cursor, 1000)
	if err != nil || len(s.Changes) >= 1000 {
		t.Fatalf("incomplete imported-memory changesets: %v", err)
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		s.Derived[table], err = r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range scopes {
		key := fmt.Sprintf("%032d", scope.key)
		s.Receipts[scope.method+key], err = r.IdempotencyByKey(ctx, principal, "", scope.method, key)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, namespace := range namespaces {
		checkpoint, err := joint.SourcePlaneCheckpoint(namespace)
		if err != nil {
			t.Fatal(err)
		}
		s.Checkpoints[namespace] = checkpoint
	}
	return s
}

// These are explicitly authored mixed-value archives, not native recordings.
// Public RPC writes must coexist with their strictly validated hidden mirrors.
func TestImportedMemoryWritesPreserveSourceEnvelopesAllDrivers(t *testing.T) {
	for _, mode := range []string{"legacy", "joint"} {
		t.Run(mode, func(t *testing.T) {
			data := authoredMixedMemoryFixture(t)
			if mode == "joint" {
				data = authoredMixedMemoryFixtureFrom(t, sourceReverseFixture(t, false))
			}
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					ctx := context.Background()
					store, lease, reopen := openIdentityTarget(t, engine)
					var profile driver.RestoreImportProfile
					if mode == "joint" {
						profile = sourceReverseProfile(t, data, store, lease, false)
					} else {
						profile, _ = admittedImportProfile(t, data)
					}
					report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
					if err != nil {
						t.Fatal(err)
					}
					// Keep the real lease alive while the complete conservation oracles
					// inspect SQL; stop and join renewal before any physical reopen.
					startRenewal := func() func() {
						renewCtx, cancel := context.WithCancel(ctx)
						done := make(chan error, 1)
						active := lease
						go func() {
							ticker := time.NewTicker(driver.LeaseRenewEvery)
							defer ticker.Stop()
							for {
								select {
								case <-renewCtx.Done():
									done <- nil
									return
								case <-ticker.C:
									if err := active.Renew(renewCtx); err != nil {
										if renewCtx.Err() != nil {
											err = nil
										}
										done <- err
										return
									}
								}
							}
						}()
						return func() {
							cancel()
							if err := <-done; err != nil {
								t.Errorf("memory write lease renewal: %v", err)
							}
						}
					}
					stopRenewal := startRenewal()
					t.Cleanup(func() { stopRenewal() })
					var namespaces []string
					if report.SourcePlanes != nil {
						for _, provider := range report.SourcePlanes.Providers {
							namespaces = append(namespaces, provider.Provider.Namespace)
						}
					}
					actor := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
					var server *rpc.Server
					bind := func() {
						ontology := service.NewOntology(nil)
						writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) { return "b13-after-import", nil }))
						domain := service.New(store, lease, writer, service.WithOntology(ontology))
						registry, err := rpc.NewRegistry()
						if err != nil {
							t.Fatal(err)
						}
						if err := rpc.BindService(registry, domain); err != nil {
							t.Fatal(err)
						}
						meta, err := store.Meta(ctx)
						if err != nil {
							t.Fatal(err)
						}
						server, err = rpc.New(registry, rpc.Config{DefaultProject: "demo", Incarnation: meta.Incarnation})
						if err != nil {
							t.Fatal(err)
						}
					}
					bind()
					refs := []model.Ref{model.RefByID("b13-after-import", model.Natural64(0))}
					for _, row := range reverseState(t, store).Records {
						refs = append(refs, row.Ref)
					}
					var scopes []importedMemoryWriteScope
					snapshot := func() importedMemoryWriteState {
						return importedMemoryWriteSnapshot(t, store, lease, refs, scopes, namespaces)
					}
					original := snapshot()
					conserve := func() {
						t.Helper()
						after := snapshot()
						rows := map[string]driver.Record{}
						for _, row := range after.Records {
							rows[row.Ref.ID] = row
						}
						for _, row := range original.Records {
							if !reflect.DeepEqual(rows[row.Ref.ID], row) || !reflect.DeepEqual(original.Versions[row.Ref.ID].Items, after.Versions[row.Ref.ID].Items) {
								t.Fatalf("public memory write changed imported row/history %s", row.Ref.ID)
							}
						}
						if len(after.Records) != len(original.Records) && len(after.Records) != len(original.Records)+1 || !reflect.DeepEqual(original.Derived, after.Derived) || !reflect.DeepEqual(original.Checkpoints, after.Checkpoints) || len(after.Changes) < len(original.Changes) || !reflect.DeepEqual(original.Changes, after.Changes[:len(original.Changes)]) {
							t.Fatal("public memory write changed unrelated membership/derived/source history")
						}
					}
					serial := 0
					refuse := func(t *testing.T, method string, params map[string]any, code string) {
						t.Helper()
						serial++
						scopes = append(scopes, importedMemoryWriteScope{method, serial})
						before := snapshot()
						sourcePublicCall(t, server, actor, method, serial, params, code)
						if !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("refused memory write changed state, versions, log, derived rows or receipts")
						}
					}
					idFor := func(key string) string {
						if mode == "joint" {
							return driver.SourcePlaneMirrorIDV1("memory", "", "config", key)
						}
						return "source/memory/" + key
					}
					for _, key := range []string{"secret", "object", "number", "null", "array", "boolean"} {
						t.Run("hidden-"+key, func(t *testing.T) {
							refuse(t, "memory.save", map[string]any{"key": key, "content": "must not become public"}, "invalid_params")
							refuse(t, "memory.delete", map[string]any{"id": idFor(key)}, "not_found")
							sourcePublicCall(t, server, actor, "memory.get", 0, map[string]any{"id": idFor(key)}, "not_found")
							sourcePublicCall(t, server, actor, "history.get", 0, map[string]any{"ref": model.RefByID(idFor(key), model.Natural64(0)), "asOf": map[string]any{"revision": original.Meta.Head}}, "not_found")
						})
					}
					for _, key := range []string{"fixture", "unicode"} {
						t.Run("visible-source-noop-"+key, func(t *testing.T) {
							var memory service.Memory
							decode(t, sourcePublicCall(t, server, actor, "memory.get", 0, map[string]any{"id": idFor(key)}, "").Result, &memory)
							before := snapshot()
							sourcePublicCall(t, server, actor, "memory.save", 81+len(key), map[string]any{"key": key, "content": memory.Content}, "")
							after := snapshot()
							if !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.Versions, after.Versions) || !reflect.DeepEqual(before.Changes, after.Changes) || before.Meta != after.Meta {
								t.Fatal("same imported value rewrote source/history")
							}
							conserve()
						})
					}
					accept := func(method string, params map[string]any, key int, op model.OpKind) (model.Commit, sourcePublicReply) {
						t.Helper()
						scopes = append(scopes, importedMemoryWriteScope{method, key})
						before := snapshot()
						reply := sourcePublicCall(t, server, actor, method, key, params, "")
						var commit model.Commit
						decode(t, reply.Result, &commit)
						after := snapshot()
						if len(commit.Results) != 1 || commit.Results[0].Ref.ID != "b13-after-import" || commit.Revision.String() == before.Meta.Head.String() || commit.Revision.String() != after.Meta.Head.String() || len(after.Changes) != len(before.Changes)+1 || !reflect.DeepEqual(before.Changes, after.Changes[:len(before.Changes)]) {
							t.Fatal("accepted memory write lost its single exact commit")
						}
						last := after.Changes[len(after.Changes)-1]
						if len(last.Changes) != 1 || last.Changes[0].Ref.ID != "b13-after-import" || last.Changes[0].Op != op {
							t.Fatalf("unexpected committed memory operation: %+v", last)
						}
						conserve()
						return commit, reply
					}
					firstParams := map[string]any{"key": "public after import ✓", "content": "  original\nπ 終  ", "metadata": map[string]any{"exact": json.Number("9007199254740993")}}
					first, firstReply := accept("memory.save", firstParams, 101, model.OpCreate)
					replaceParams := map[string]any{"key": "public after import ✓", "content": "  replaced\nΩ  ", "metadata": map[string]any{"nested": map[string]any{"literal": true}}}
					replaced, replaceReply := accept("memory.save", replaceParams, 102, model.OpPatch)
					checkRead := func(revision model.Revision, params map[string]any) {
						reply := sourcePublicCall(t, server, actor, "history.get", 0, map[string]any{"ref": model.RefByID("b13-after-import", model.Natural64(0)), "asOf": map[string]any{"revision": revision}}, "")
						var history struct {
							Body json.RawMessage `json:"body"`
						}
						decode(t, reply.Result, &history)
						var got pipeline.MemoryDocument
						decode(t, history.Body, &got)
						if got.ID != "b13-after-import" || got.Key != params["key"] || got.Content != params["content"] || !sameReverseJSON(got.Metadata, params["metadata"]) {
							t.Fatalf("canonical memory history lost exact public values: %+v", got)
						}
					}
					checkRead(first.Revision, firstParams)
					checkRead(replaced.Revision, replaceParams)
					replay := func(method string, params map[string]any, key int, original sourcePublicReply) {
						t.Helper()
						before := snapshot()
						got := sourcePublicCall(t, server, actor, method, key, params, "")
						if !got.Meta.Replayed || !bytes.Equal(got.Result, original.Result) || !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("memory replay changed its original result or durable state")
						}
					}
					checkViews := func(want int) {
						t.Helper()
						before := snapshot()
						list := sourcePublicCall(t, server, actor, "memory.list", 0, map[string]any{"limit": 100}, "")
						brief := sourcePublicCall(t, server, actor, "agent.brief", 0, map[string]any{}, "")
						var page struct {
							Items []service.Memory `json:"items"`
							Next  *string          `json:"nextCursor"`
						}
						var briefResult service.BriefResult
						decode(t, list.Result, &page)
						decode(t, brief.Result, &briefResult)
						if len(page.Items) != want || page.Next != nil || !reflect.DeepEqual(page.Items, briefResult.Memories) {
							t.Fatal("list/brief lost complete public-only membership")
						}
						for _, memory := range page.Items {
							if memory.Key != "fixture" && memory.Key != "unicode" && memory.Key != "public after import ✓" {
								t.Fatalf("hidden source row leaked into list/brief: %+v", memory)
							}
						}
						if !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("public views changed state")
						}
					}
					refresh := func() {
						before := snapshot()
						stopRenewal()
						stopRenewal = func() {}
						store, lease = reopen()
						stopRenewal = startRenewal()
						bind()
						if !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("SQL reopen or memory lease refresh changed state")
						}
					}
					checkViews(3)
					refresh()
					checkViews(3)
					checkRead(first.Revision, firstParams)
					checkRead(replaced.Revision, replaceParams)
					replay("memory.save", firstParams, 101, firstReply)
					replay("memory.save", replaceParams, 102, replaceReply)
					deleteParams := map[string]any{"id": "b13-after-import"}
					_, deletedReply := accept("memory.delete", deleteParams, 103, model.OpDelete)
					refresh()
					checkViews(2)
					sourcePublicCall(t, server, actor, "memory.get", 0, deleteParams, "not_found")
					replay("memory.delete", deleteParams, 103, deletedReply)
					versions := snapshot().Versions["b13-after-import"].Items
					if len(versions) != 3 || !versions[2].Deleted {
						t.Fatal("canonical save/replace/delete did not retain its tombstone history")
					}
					conserve()
					var source driver.Record
					for _, row := range original.Records {
						if row.Ref.ID == idFor("secret") {
							source = row
						}
					}
					for _, control := range []string{"source-value", "source-id", "classification", "extra-field"} {
						t.Run("malformed-"+control, func(t *testing.T) {
							var body map[string]any
							decode(t, source.Body, &body)
							mutated := source
							switch control {
							case "source-value":
								body["source"].(map[string]any)["value"] = "changed"
							case "source-id":
								body["source_id"] = "wrong"
							case "classification":
								body["classification"] = "private"
							case "extra-field":
								body["extra"] = true
							}
							mutated.Body = encode(t, body)
							putImportedMemoryControl(t, store, lease, mutated)
							refuse(t, "memory.save", map[string]any{"key": "unrelated malformed control", "content": "must refuse"}, "internal_error")
							refuse(t, "memory.delete", map[string]any{"id": source.Ref.ID}, "internal_error")
							sourcePublicCall(t, server, actor, "memory.get", 0, map[string]any{"id": source.Ref.ID}, "internal_error")
							putImportedMemoryControl(t, store, lease, source)
						})
					}
				})
			}
		})
	}
}

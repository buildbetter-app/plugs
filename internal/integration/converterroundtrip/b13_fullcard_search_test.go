package converterroundtrip

import (
	"bytes"
	"context"
	"database/sql"
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

func b13ImportedReplacementResult(t *testing.T, raw json.RawMessage, want bool) model.Commit {
	t.Helper()
	var commit model.Commit
	decode(t, raw, &commit)
	if len(commit.Results) != 1 || commit.Results[0].Replaced == nil || *commit.Results[0].Replaced != want {
		t.Fatalf("imported public memory.save must report replaced:%t: %s", want, raw)
	}
	return commit
}

// Separate from the inherited empty-source forget chain: replacing that empty
// value first would change its deliberately asserted physical create into a
// patch. This new control imports a fresh real archive through the same receiver.
func TestB13ImportedEmptyReplacementReplaysAndReopens(t *testing.T) {
	for _, mode := range []string{"legacy", "joint"} {
		t.Run(mode, func(t *testing.T) {
			data := b13ImportedMemoryFixture(t, mode)
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					ctx := context.Background()
					var dsn string
					store, lease, reopen := openIdentityTarget(t, engine, func(value string) { dsn = value })
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
					startRenewal := func() func() {
						live, cancel := context.WithCancel(ctx)
						done := make(chan error, 1)
						active := lease
						go func() {
							ticker := time.NewTicker(driver.LeaseRenewEvery)
							defer ticker.Stop()
							for {
								select {
								case <-live.Done():
									done <- nil
									return
								case <-ticker.C:
									if err := active.Renew(live); err != nil {
										if live.Err() != nil {
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
								t.Error(err)
							}
						}
					}
					stop := startRenewal()
					t.Cleanup(func() { stop() })
					var db *sql.DB
					openSQL := func() {
						if engine == "memory" {
							return
						}
						dialect := "sqlite"
						if engine == "postgres" {
							dialect = "pgx"
						}
						db, err = sql.Open(dialect, dsn)
						if err != nil {
							t.Fatal(err)
						}
					}
					openSQL()
					t.Cleanup(func() {
						if db != nil {
							db.Close()
						}
					})
					var server *rpc.Server
					bind := func() {
						ontology := service.NewOntology(nil)
						writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
						domain := service.New(store, lease, writer, service.WithOntology(ontology))
						registry, e := rpc.NewRegistry()
						if e != nil {
							t.Fatal(e)
						}
						if e = rpc.BindService(registry, domain); e != nil {
							t.Fatal(e)
						}
						server, e = rpc.New(registry, rpc.Config{DefaultProject: "demo", Incarnation: report.Restored.Incarnation})
						if e != nil {
							t.Fatal(e)
						}
					}
					bind()
					sourceID := "source/memory/fixture"
					if mode == "joint" {
						sourceID = driver.SourcePlaneMirrorIDV1("memory", "", "config", "fixture")
					}
					overlay := pipeline.MemoryOverlayRef(model.RefByID(sourceID, model.Natural64(0)))
					refs := []model.Ref{overlay}
					for _, row := range reverseState(t, store).Records {
						refs = append(refs, row.Ref)
					}
					var namespaces []string
					if report.SourcePlanes != nil {
						for _, provider := range report.SourcePlanes.Providers {
							namespaces = append(namespaces, provider.Provider.Namespace)
						}
					}
					scopes := []importedMemoryWriteScope{{"memory.save", 801}, {"memory.delete", 802}, {"memory.save", 803}}
					snapshot := func() importedMemoryWriteState {
						return importedMemoryWriteSnapshot(t, store, lease, refs, scopes, namespaces)
					}
					initial := snapshot()
					var frames []map[string]any
					call := func(method string, params map[string]any, key int, code string) sourcePublicReply {
						t.Helper()
						meta := map[string]any{"project": "demo"}
						if key != 0 {
							meta["idempotencyKey"] = fmt.Sprintf("%032d", key)
						}
						params["_meta"] = meta
						request := encode(t, map[string]any{"jsonrpc": "2.0", "id": len(frames) + 1, "method": method, "params": params})
						raw := server.Dispatch(rpc.WithPrincipal(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}), request)
						frames = append(frames, map[string]any{"request": json.RawMessage(request), "response": json.RawMessage(raw), "request_sha256": hashBytes(request), "response_sha256": hashBytes(raw)})
						var reply sourcePublicReply
						decode(t, raw, &reply)
						if code == "" && reply.Error != nil || code != "" && (reply.Error == nil || reply.Error.Data.Code != code || len(reply.Result) != 0) {
							t.Fatalf("%s: %s", method, raw)
						}
						return reply
					}
					t.Cleanup(func() { b13Retain(t, "fullcard-empty-"+mode+"-"+engine, "frames", frames) })
					check := func(phase, value string, present bool) {
						t.Helper()
						before := snapshot()
						want := map[string]string{b13SourceKey: b13SourceBody, b13TrimmedNeighborKey: b13TrimmedNeighborBody}
						if present {
							want["fixture"] = value
						}
						b13ImportedLiteralSearch(t, want, func(query string) json.RawMessage {
							return call("memory.list", map[string]any{"query": query, "limit": 100}, 0, "").Result
						})
						if present {
							var point service.Memory
							decode(t, call("memory.get", map[string]any{"id": sourceID}, 0, "").Result, &point)
							if point.Key != "fixture" || point.Content != value {
								t.Fatalf("literal imported-empty value: %+v", point)
							}
						} else {
							call("memory.get", map[string]any{"id": sourceID}, 0, "not_found")
						}
						var brief struct {
							Memories []service.Memory `json:"memories"`
						}
						decode(t, call("agent.brief", map[string]any{"budgetTokens": 2000}, 0, "").Result, &brief)
						got := map[string]string{}
						for _, item := range brief.Memories {
							got[item.Key] = item.Content
						}
						if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("imported empty list/brief/point or read conservation")
						}
						rows := map[string]driver.Record{}
						for _, row := range before.Records {
							rows[row.Ref.ID] = row
						}
						for _, row := range initial.Records {
							if !reflect.DeepEqual(row, rows[row.Ref.ID]) || !reflect.DeepEqual(initial.Versions[row.Ref.ID].Items, before.Versions[row.Ref.ID].Items) {
								t.Fatal("immutable imported row/version changed", row.Ref.ID)
							}
						}
						if !reflect.DeepEqual(initial.Derived, before.Derived) || !reflect.DeepEqual(initial.Checkpoints, before.Checkpoints) {
							t.Fatal("import binding/provider changed")
						}
						r, e := store.Read(ctx, driver.View{Incarnation: before.Meta.Incarnation})
						if e != nil {
							t.Fatal(e)
						}
						blob, e := r.Blob(ctx, driver.Hash(report.Archive.SHA256()))
						r.Close()
						if e != nil || !bytes.Equal(blob, data) {
							t.Fatal("source archive changed", e)
						}
						var physical map[string][]string
						if db != nil {
							physical = jointPhysicalSQL(t, db, engine)
						}
						b13Retain(t, "fullcard-empty-"+mode+"-"+engine, phase, map[string]any{"state": b13MemoryEvidence(before), "physical_SQL": physical, "literal_expected": want, "retained_archive_sha256": hashBytes(blob)})
					}
					refresh := func() {
						before := snapshot()
						stop()
						stop = func() {}
						if db != nil {
							db.Close()
							db = nil
						}
						store, lease = reopen()
						stop = startRenewal()
						openSQL()
						bind()
						if !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("imported-empty SQL reopen changed state")
						}
					}
					check("initial-empty", "", true)
					save := func(key int, want bool) sourcePublicReply {
						reply := call("memory.save", map[string]any{"key": "fixture", "content": "Previously empty, now replaced"}, key, "")
						commit := b13ImportedReplacementResult(t, reply.Result, want)
						state := snapshot()
						receipt := state.Receipts["memory.save"+fmt.Sprintf("%032d", key)]
						if receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, commit) || commit.Results[0].Ref != overlay {
							t.Fatal("imported-empty result not committed to its exact overlay receipt")
						}
						return reply
					}
					original := save(801, true)
					check("replaced-empty", "Previously empty, now replaced", true)
					refresh()
					check("replaced-empty-reopened", "Previously empty, now replaced", true)
					call("memory.delete", map[string]any{"id": overlay.ID}, 802, "")
					check("imported-forgotten", "", false)
					refresh()
					check("imported-forgotten-reopened", "", false)
					before := snapshot()
					replayed := call("memory.save", map[string]any{"key": "fixture", "content": "Previously empty, now replaced"}, 801, "")
					if !replayed.Meta.Replayed || !bytes.Equal(original.Result, replayed.Result) || !reflect.DeepEqual(before, snapshot()) {
						t.Fatal("true replacement receipt was recomputed after forget")
					}
					check("true-replay-while-forgotten", "", false)
					save(803, false)
					check("remembered-after-tombstone", "Previously empty, now replaced", true)
					refresh()
					check("remembered-after-tombstone-reopened", "Previously empty, now replaced", true)
				})
			}
		})
	}
}

// The caller supplies its independent effective-state oracle, not the list
// response. Every query's membership is authored explicitly, without using a
// product filter or deriving expected matches from actual returned rows.
func b13ImportedLiteralSearch(t *testing.T, want map[string]string, query func(string) json.RawMessage) {
	t.Helper()
	keys, original, replaced, remembered, core := map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}
	if value, exists := want[b13TrimmedNeighborKey]; exists {
		keys[b13TrimmedNeighborKey] = value
	}
	if value, exists := want[b13SourceKey]; exists {
		keys[b13SourceKey] = value
		switch value {
		case b13SourceBody:
			original[b13SourceKey] = b13SourceBody
		case "\nOverlay replaced Ω\t東京 ":
			replaced[b13SourceKey] = "\nOverlay replaced Ω\t東京 "
		case "Re-remembered Ω\n":
			remembered[b13SourceKey] = "Re-remembered Ω\n"
		default:
			t.Fatal("missing literal imported-memory search oracle")
		}
	}
	if value, exists := want["Core Ω"]; exists {
		core["Core Ω"] = value
	}
	for _, tc := range []struct {
		query string
		want  map[string]string
	}{
		{"", want},
		{"sOuRcE kEy/ω", keys},
		{"vErBaTiM sOuRcE", original},
		{"oVeRlAy rEpLaCeD", replaced},
		{"rE-rEmEmBeReD", remembered},
		{"cOrE ω", core},
		{"uNmAtChEd-SeTtInGs-VaLuE", map[string]string{}},
		{"nO sUcH mEmOrY", map[string]string{}},
	} {
		var page struct {
			Items []service.Memory `json:"items"`
			Next  string           `json:"nextCursor"`
		}
		decode(t, query(tc.query), &page)
		if page.Items == nil || page.Next != "" {
			t.Fatal("imported search must return a complete nonnull memory page")
		}
		got := map[string]string{}
		for _, item := range page.Items {
			if _, exists := got[item.Key]; exists {
				t.Fatal("imported search returned a duplicate effective key")
			}
			got[item.Key] = item.Content
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("literal imported search %q: got=%+v want=%+v", tc.query, got, tc.want)
		}
	}
}

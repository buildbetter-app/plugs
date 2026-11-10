package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const b13SourceKey = "  Source Key/Ω e\u0301  "
const b13SourceBody = " \tVerbatim source\n東京 e\u0301  "

type b13MemoryPolicy struct {
	deny  string
	stage port.AuthorizationStage
	seen  map[string]bool
}

func (p *b13MemoryPolicy) Authorize(_ context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, _ port.SnapshotReader) error {
	if stage != p.stage {
		return nil
	}
	for _, change := range proposal.Changes {
		p.seen[change.Resource.Ref.ID] = true
		if change.Resource.Kind == "memory" && change.Resource.Ref.ID == p.deny {
			return &model.Error{Data: model.ErrorData{Code: "forbidden", Details: map[string]any{"reason": "public memory control denies this exact slot"}}}
		}
	}
	return nil
}

func (p *b13MemoryPolicy) EventContext(_ context.Context, proposal port.ProposedChange, _ port.SnapshotReader) (port.EventContext, error) {
	return port.EventContext{Cause: model.EventCause{Root: proposal.RequestID, Depth: model.Natural64(0), Chain: []string{}}, Chain: []string{"b13-public-memory-policy"}}, nil
}

// This small authored source input retains the existing mixed classification
// fixture and its real joint plan. It is not a producer or native recording.
func b13ImportedMemoryFixture(t *testing.T, mode string) []byte {
	t.Helper()
	input := admittedFixtureBytes(t)
	if mode == "joint" {
		input = sourceReverseFixture(t, false)
	}
	base := authoredMixedMemoryFixtureFrom(t, input)
	return rewriteData(t, func(members map[string][]byte) {
		r := tar.NewReader(bytes.NewReader(base))
		for {
			h, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			members[h.Name], err = io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
		}
		var rows []map[string]any
		for _, line := range bytes.Split(bytes.TrimSpace(members["records/memory.ndjson"]), []byte{'\n'}) {
			var row map[string]any
			decode(t, line, &row)
			body := row["body"].(map[string]any)
			fields := row["source"].(map[string]any)["fields"].(map[string]any)
			if row["id"] == "unicode" {
				row["id"], body["key"], body["value"] = b13SourceKey, b13SourceKey, b13SourceBody
				fields["key"], fields["value"] = "kv.memory."+b13SourceKey, b13SourceBody
			} else if row["id"] == "fixture" {
				body["value"], fields["value"] = "", ""
			}
			rows = append(rows, row)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i]["id"].(string) < rows[j]["id"].(string) })
		members["records/memory.ndjson"] = nil
		for _, row := range rows {
			members["records/memory.ndjson"] = append(members["records/memory.ndjson"], encode(t, row)...)
		}
	})
}

func b13Retain(t *testing.T, engine, label string, value any) {
	t.Helper()
	if root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); root != "" {
		if err := os.WriteFile(filepath.Join(root, "b13-joint-"+engine+"-"+label+".json"), encode(t, value), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

// Quiet receipts contain a literal zero Changeset instead of a published log
// entry. Its empty enum cannot use the public Changeset JSON encoder. Preserve
// every field explicitly, including that empty kind, in the evidence DTO.
func b13MemoryEvidence(state importedMemoryWriteState) map[string]any {
	receipts := map[string]any{}
	for key, receipt := range state.Receipts {
		if receipt == nil {
			receipts[key] = nil
			continue
		}
		entry := receipt.Changeset
		receipts[key] = map[string]any{"RequestHash": receipt.RequestHash, "Commit": receipt.Commit, "ResponsePending": receipt.ResponsePending,
			"Changeset": map[string]any{"Revision": entry.Revision, "Branch": entry.Branch, "Incarnation": entry.Incarnation, "Kind": string(entry.Kind), "Time": entry.Time, "Principal": map[string]any{"kind": entry.Principal.Kind, "id": entry.Principal.ID, "sponsor": entry.Principal.Sponsor, "caveats": entry.Principal.Caveats}, "Chain": entry.Chain, "IdempotencyKey": entry.IdempotencyKey, "Mutators": entry.Mutators, "Changes": entry.Changes, "Events": entry.Events}}
	}
	return map[string]any{"Meta": state.Meta, "Records": state.Records, "Versions": state.Versions, "Changes": state.Changes, "Derived": state.Derived, "Receipts": receipts, "Checkpoints": state.Checkpoints}
}

func b13StableSQL(t *testing.T, image map[string][]string) map[string][]string {
	t.Helper()
	data := map[string][]string{}
	for table, rows := range image {
		if table != "lease" && table != "active_lease" {
			data[table] = rows
			continue
		}
		data[table] = nil
		for _, row := range rows {
			var cells []json.RawMessage
			decode(t, []byte(row), &cells)
			if len(cells) != 5 {
				t.Fatal("unexpected physical lease schema")
			}
			// The live fixture renews expiry asynchronously. Compare all four
			// ownership/fencing cells and retain full literal images separately.
			data[table] = append(data[table], string(encode(t, cells[:4])))
		}
	}
	return data
}

// The decided overlay is exercised through the ordinary Writer and byte RPC.
// Source evidence and provider namespaces remain immutable throughout.
func TestB13JointSourceCoreProjectionAndConservation(t *testing.T) {
	for _, mode := range []string{"legacy", "joint"} {
		t.Run(mode, func(t *testing.T) {
			data := b13ImportedMemoryFixture(t, mode)
			idFor := func(key string) string {
				if mode == "joint" {
					return driver.SourcePlaneMirrorIDV1("memory", "", "config", key)
				}
				return "source/memory/" + key
			}
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
					renew := func() func() {
						active := lease
						live, cancel := context.WithCancel(ctx)
						done := make(chan error, 1)
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
					stop := renew()
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
					policy := &b13MemoryPolicy{seen: map[string]bool{}}
					bind := func() {
						ontology := service.NewOntology(nil)
						writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithAuthorizer(policy), pipeline.WithEventContext(policy), pipeline.WithIDGenerator(func() (string, error) { return "b13-joint-core", nil }))
						domain := service.New(store, lease, writer, service.WithOntology(ontology))
						registry, e := rpc.NewRegistry()
						if e != nil {
							t.Fatal(e)
						}
						if e = rpc.BindService(registry, domain); e != nil {
							t.Fatal(e)
						}
						server, e = rpc.New(registry, rpc.Config{DefaultProject: "demo", Incarnation: report.Restored.Incarnation, CursorKey: []byte("b13-joint-memory-cursor-000000001")})
						if e != nil {
							t.Fatal(e)
						}
					}
					bind()
					var scopes []importedMemoryWriteScope
					refs := []model.Ref{model.RefByID("b13-joint-core", model.Natural64(0))}
					for _, key := range []string{"fixture", b13SourceKey} {
						refs = append(refs, pipeline.MemoryOverlayRef(model.RefByID(idFor(key), model.Natural64(0))))
					}
					for _, row := range reverseState(t, store).Records {
						refs = append(refs, row.Ref)
					}
					// PostgreSQL retains lock placeholders outside the rolled-back
					// write. Establish this exact fixture's census before conservation
					// baselines, as the existing joint import controls do.
					primePlan := restorePlan{sentinels: []driver.Sentinel{driver.SentinelGraphBlocking}}
					for _, ref := range refs {
						primePlan.records = append(primePlan.records, restoreRecord{ref: ref})
					}
					prime, err := store.Write(ctx, lease)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := prime.Lock(ctx, lockRequests(primePlan)); err != nil {
						t.Fatal(err)
					}
					if err := prime.Rollback(ctx); err != nil {
						t.Fatal(err)
					}
					if err := prime.Close(); err != nil {
						t.Fatal(err)
					}
					var namespaces []string
					if report.SourcePlanes != nil {
						for _, provider := range report.SourcePlanes.Providers {
							namespaces = append(namespaces, provider.Provider.Namespace)
						}
					}
					snapshot := func() importedMemoryWriteState {
						return importedMemoryWriteSnapshot(t, store, lease, refs, scopes, namespaces)
					}
					initial := snapshot()
					var frames []map[string]any
					call := func(method string, params map[string]any, key int, want string) sourcePublicReply {
						t.Helper()
						meta := map[string]any{"project": "demo"}
						if key != 0 {
							meta["idempotencyKey"] = fmt.Sprintf("%032d", key)
						}
						copied := map[string]any{}
						for k, v := range params {
							copied[k] = v
						}
						copied["_meta"] = meta
						request := encode(t, map[string]any{"jsonrpc": "2.0", "id": len(frames) + 1, "method": method, "params": copied})
						raw := server.Dispatch(rpc.WithPrincipal(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}), request)
						frames = append(frames, map[string]any{"request": json.RawMessage(request), "response": json.RawMessage(raw), "request_sha256": hashBytes(request), "response_sha256": hashBytes(raw)})
						var reply sourcePublicReply
						decode(t, raw, &reply)
						if want == "" && reply.Error != nil || want != "" && (reply.Error == nil || reply.Error.Data.Code != want || len(reply.Result) != 0) {
							t.Fatalf("%s want=%s: %s", method, want, raw)
						}
						return reply
					}
					t.Cleanup(func() { b13Retain(t, mode+"-"+engine, "frames", frames) })
					conserve := func(label string) {
						t.Helper()
						after := snapshot()
						byID := map[string]driver.Record{}
						for _, row := range after.Records {
							byID[row.Ref.ID] = row
						}
						for _, row := range initial.Records {
							if !reflect.DeepEqual(row, byID[row.Ref.ID]) || !reflect.DeepEqual(initial.Versions[row.Ref.ID].Items, after.Versions[row.Ref.ID].Items) {
								t.Fatalf("immutable imported row or version changed: %s", row.Ref.ID)
							}
						}
						if !reflect.DeepEqual(initial.Checkpoints, after.Checkpoints) || !reflect.DeepEqual(initial.Derived, after.Derived) {
							t.Fatal("ordinary memory work changed imported source namespace/derived state")
						}
						reader, e := store.Read(ctx, driver.View{Incarnation: after.Meta.Incarnation})
						if e != nil {
							t.Fatal(e)
						}
						blob, e := reader.Blob(ctx, driver.Hash(report.Archive.SHA256()))
						reader.Close()
						if e != nil || !bytes.Equal(blob, data) {
							t.Fatal("retained source archive bytes changed", e)
						}
						var physical map[string][]string
						if db != nil {
							physical = jointPhysicalSQL(t, db, engine)
						}
						b13Retain(t, mode+"-"+engine, label, map[string]any{"state": b13MemoryEvidence(after), "physical_SQL": physical, "retained_archive_sha256": hashBytes(blob)})
					}
					sourceValue, sourcePresent, emptyPresent := b13SourceBody, true, true
					views := func(core string) {
						t.Helper()
						before := snapshot()
						want := map[string]string{}
						if emptyPresent {
							want["fixture"] = ""
						}
						if sourcePresent {
							want[b13SourceKey] = sourceValue
						}
						if core != "" {
							want["Core Ω"] = core
						}
						var all []service.Memory
						cursor := ""
						for pages := 0; ; pages++ {
							if pages > len(want) {
								t.Fatal("memory list did not terminate")
							}
							params := map[string]any{"limit": 1}
							if cursor != "" {
								params["cursor"] = cursor
							}
							var page struct {
								Items []service.Memory `json:"items"`
								Next  string           `json:"nextCursor"`
							}
							decode(t, call("memory.list", params, 0, "").Result, &page)
							if page.Items == nil {
								t.Fatal("memory page is null/absent")
							}
							all = append(all, page.Items...)
							if page.Next == "" {
								break
							}
							if page.Next == cursor || len(page.Items) != 1 {
								t.Fatal("memory cursor failed to advance")
							}
							cursor = page.Next
						}
						if len(all) != len(want) {
							t.Fatalf("source/core membership: %+v", all)
						}
						seen := map[string]bool{}
						for _, item := range all {
							value, ok := want[item.Key]
							if !ok || seen[item.Key] || item.Content != value {
								t.Fatalf("settings/hidden row leak or value loss: %+v", item)
							}
							seen[item.Key] = true
							if item.Key != "Core Ω" {
								wantID := idFor(item.Key)
								if item.ID != wantID && item.ID != pipeline.MemoryOverlayRef(model.RefByID(wantID, model.Natural64(0))).ID || item.Metadata["plug.restore.archive_sha256"] != report.Archive.SHA256() || item.Metadata["plug.restore.source_id"] != item.Key {
									t.Fatalf("source provenance/identity changed: %+v", item)
								}
							}
							var got service.Memory
							decode(t, call("memory.get", map[string]any{"id": item.ID}, 0, "").Result, &got)
							if !reflect.DeepEqual(got, item) {
								t.Fatal("point/list source projection mismatch")
							}
						}
						var brief struct {
							Memories []service.Memory `json:"memories"`
						}
						decode(t, call("agent.brief", map[string]any{"budgetTokens": 2000}, 0, "").Result, &brief)
						if !reflect.DeepEqual(brief.Memories, all) {
							t.Fatal("normal agent.brief differs from joint memory listing")
						}
						for _, query := range []string{"source key/ω", "core ω", "unmatched-settings-value"} {
							var search struct {
								Items []service.Memory `json:"items"`
							}
							decode(t, call("memory.list", map[string]any{"query": query, "limit": 100}, 0, "").Result, &search)
							expected := []service.Memory{}
							for _, item := range all {
								if strings.Contains(strings.ToLower(item.Key), query) || strings.Contains(strings.ToLower(item.Content), query) {
									expected = append(expected, item)
								}
							}
							if !reflect.DeepEqual(search.Items, expected) {
								t.Fatal("effective source/core search leaked or lost a value")
							}
						}
						if !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("public projection changed durable state")
						}
					}
					t.Run("verbatim-empty-absent-and-joint-brief", func(t *testing.T) {
						views("")
						call("memory.get", map[string]any{"id": "b13-joint-absent"}, 0, "not_found")
						conserve("initial")
					})
					refuse := func(method string, params map[string]any, key int, code string) {
						t.Helper()
						scopes = append(scopes, importedMemoryWriteScope{method, key})
						before := snapshot()
						var physical map[string][]string
						if db != nil {
							physical = jointPhysicalSQL(t, db, engine)
						}
						call(method, params, key, code)
						after := snapshot()
						var afterSQL map[string][]string
						if db != nil {
							afterSQL = jointPhysicalSQL(t, db, engine)
						}
						b13Retain(t, mode+"-"+engine, fmt.Sprintf("refusal-%d-%s-%d", len(frames), method, key), map[string]any{"before": b13MemoryEvidence(before), "after": b13MemoryEvidence(after), "before_SQL": physical, "after_SQL": afterSQL, "expected_code": code})
						if !reflect.DeepEqual(before, after) || db != nil && !reflect.DeepEqual(b13StableSQL(t, physical), b13StableSQL(t, afterSQL)) {
							t.Fatal("refusal changed current/history/receipt/source/SQL state")
						}
					}
					t.Run("strict-hidden-categories-and-blank-refusals", func(t *testing.T) {
						for i, key := range []string{"secret", "number", "object", "null", "array", "boolean"} {
							id := idFor(key)
							refuse("memory.get", map[string]any{"id": id}, 0, "not_found")
							refuse("memory.save", map[string]any{"key": key, "content": "must not replace classification"}, 601+i, "invalid_params")
						}
						for i, key := range []string{"fixture", b13SourceKey} {
							refuse("memory.save", map[string]any{"key": key, "content": " \t\n"}, 621+i, "invalid_params")
						}
						scopes = append(scopes, importedMemoryWriteScope{"memory.save", 629})
						before := snapshot()
						writer := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)))
						_, e := writer.Apply(ctx, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "memory", Fields: map[string]any{"key": b13SourceKey, "content": " \t\n"}}}}}, model.WriteMeta{Method: "memory.save", Project: "demo", IdempotencyKey: fmt.Sprintf("%032d", 629)})
						var typed *model.Error
						if !errors.As(e, &typed) || typed.Data.Code != "invalid_params" || !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("direct Writer blank bypass changed source/history/receipt", e)
						}
						conserve("refusals")
					})
					refresh := func(label string) {
						before := snapshot()
						stop()
						stop = func() {}
						if db != nil {
							if err := db.Close(); err != nil {
								t.Fatal(err)
							}
							db = nil
						}
						store, lease = reopen()
						stop = renew()
						openSQL()
						bind()
						if !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("SQL reopen/memory rebind changed joint state")
						}
						t.Logf("B13 joint memory phase=%s engine=%s SQL_reopen=%t", label, engine, engine != "memory")
					}
					// Reverse is checked after the existing empty source is forgotten;
					// the unchanged converter still honestly refuses an empty import value.
					coreValue := ""
					reverse := func(label string) {
						before := snapshot()
						result, e := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
						if e != nil {
							t.Fatal("effective reverse", e)
						}
						got := map[string]any{}
						decoder := json.NewDecoder(bytes.NewReader(result.Result.JSONL))
						decoder.UseNumber()
						for {
							var row map[string]any
							e := decoder.Decode(&row)
							if e == io.EOF {
								break
							}
							if e != nil {
								t.Fatal(e)
							}
							if row["_type"] == "memory" {
								key, ok := row["key"].(string)
								if !ok {
									t.Fatal(row)
								}
								if _, duplicate := got[key]; duplicate {
									t.Fatal("duplicate reverse key")
								}
								got[key] = row["value"]
							}
						}
						for _, key := range []string{"fixture", b13SourceKey} {
							value, exists := got[key]
							present, want := sourcePresent, sourceValue
							if key == "fixture" {
								present, want = emptyPresent, ""
							}
							if exists != present || present && value != want {
								t.Fatalf("reverse effective value %q: %+v", key, got)
							}
						}
						want := map[string]any{}
						for _, row := range report.Archive.Records("memory") {
							want[row.ID] = row.Source.Fields["value"]
						}
						if !sourcePresent {
							delete(want, b13SourceKey)
						} else {
							want[b13SourceKey] = sourceValue
						}
						if !emptyPresent {
							delete(want, "fixture")
						}
						if coreValue != "" {
							want["Core Ω"] = coreValue
						}
						for _, row := range report.Archive.Records("config") {
							value := row.Source.Fields["value"]
							text, ok := value.(string)
							if !ok {
								data, e := json.Marshal(value)
								if e != nil {
									t.Fatal(e)
								}
								text = string(data)
							}
							want["plug.config/"+row.Source.Fields["key"].(string)] = text
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("complete effective reverse mismatch: got=%+v want=%+v", got, want)
						}

						if !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("reverse wrote state")
						}
						b13Retain(t, mode+"-"+engine, "reverse-"+label, map[string]any{"effective_rows": got, "result": result})
					}
					sourceRef := model.RefByID(idFor(b13SourceKey), model.Natural64(0))
					overlayRef := pipeline.MemoryOverlayRef(sourceRef)
					var replacedRevision model.Revision
					t.Run("source-and-overlay-authority-refuses-before-and-after-atomically", func(t *testing.T) {
						for _, stage := range []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeAfter} {
							for i, ref := range []model.Ref{sourceRef, overlayRef} {
								policy.deny, policy.stage, policy.seen = ref.ID, stage, map[string]bool{}
								refuse("memory.save", map[string]any{"key": b13SourceKey, "content": "denied overlay"}, 681+i, "forbidden")
								if !policy.seen[ref.ID] {
									t.Fatal("authorization did not cover selected source/overlay")
								}
							}
						}
						policy.deny, policy.stage = "", ""
					})
					t.Run("imported-replace-attribution-quiet-replay-reopen", func(t *testing.T) {
						params := map[string]any{"key": b13SourceKey, "content": b13SourceBody}
						scopes = append(scopes, importedMemoryWriteScope{"memory.save", 671})
						before := snapshot()
						call("memory.save", params, 671, "")
						after := snapshot()
						if !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.Versions, after.Versions) || !reflect.DeepEqual(before.Changes, after.Changes) || before.Meta != after.Meta {
							t.Fatal("same-source-value save was not quiet")
						}
						scopes = append(scopes, importedMemoryWriteScope{"memory.save", 672})
						params = map[string]any{"key": b13SourceKey, "content": "\nOverlay replaced Ω\t東京 ", "metadata": map[string]any{"wide": json.Number("9007199254740993")}}
						refuse("memory.save", map[string]any{"key": b13SourceKey, "content": "must refuse forged authority", "metadata": map[string]any{"plug.restore.source_id": "other-source"}}, 678, "invalid_params")

						before = snapshot()
						first := call("memory.save", params, 672, "")
						after = snapshot()
						var commit model.Commit
						decode(t, first.Result, &commit)
						if len(commit.Results) != 1 || commit.Results[0].Ref.ID != overlayRef.ID || len(after.Changes) != len(before.Changes)+1 || !reflect.DeepEqual(before.Changes, after.Changes[:len(before.Changes)]) {
							t.Fatal("replacement must publish one normal overlay change")
						}
						replacedRevision = commit.Revision
						var overlay pipeline.MemoryDocument
						for _, row := range after.Records {
							if row.Ref.ID == overlayRef.ID {
								decode(t, row.Body, &overlay)
							}
						}
						if overlay.Overlay == nil || overlay.Overlay.Deleted || overlay.Overlay.Principal.Kind != "anonymous" || overlay.Overlay.Principal.ID != "anonymous" || overlay.Overlay.Time.IsZero() || overlay.Overlay.Source.ID != sourceRef.ID || overlay.Overlay.Generation != 1 || overlay.Metadata["wide"] != json.Number("9007199254740993") {
							t.Fatalf("overlay attribution/binding missing: %+v", overlay)
						}
						refuse("memory.save", map[string]any{"key": b13SourceKey, "content": "stale replacement", "expect": []map[string]any{{"ref": sourceRef, "revision": initial.Meta.Head}}}, 679, "precondition_failed")

						sourceValue = params["content"].(string)
						views("")
						conserve("replaced")
						before = snapshot()
						for _, ref := range []model.Ref{sourceRef, overlayRef} {
							policy.deny, policy.stage = ref.ID, port.AuthorizeReplay
							refuse("memory.save", params, 672, "forbidden")
						}
						policy.deny, policy.stage = "", ""
						replay := call("memory.save", params, 672, "")
						if !replay.Meta.Replayed || !bytes.Equal(replay.Result, first.Result) || !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("overlay replay changed state")
						}
						scopes = append(scopes, importedMemoryWriteScope{"memory.save", 673})
						before = snapshot()
						call("memory.save", params, 673, "")
						after = snapshot()
						if !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.Versions, after.Versions) || !reflect.DeepEqual(before.Changes, after.Changes) || before.Meta != after.Meta {
							t.Fatal("overlay no-op changed value/time/history")
						}
						refresh("overlay-replaced")
						views("")
						conserve("replaced-reopened")
						before = snapshot()
						replayedQuiet := call("memory.save", params, 673, "")
						if !replayedQuiet.Meta.Replayed || !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("quiet overlay receipt changed after SQL reopen")
						}
						call("history.get", map[string]any{"ref": overlayRef, "asOf": map[string]any{"revision": replacedRevision}}, 0, "")
					})
					t.Run("empty-source-forget-enables-exact-effective-reverse", func(t *testing.T) {
						ref := model.RefByID(idFor("fixture"), model.Natural64(0))
						scopes = append(scopes, importedMemoryWriteScope{"memory.delete", 674})
						call("memory.delete", map[string]any{"id": ref.ID}, 674, "")
						changes := snapshot().Changes
						last := changes[len(changes)-1]
						if len(last.Changes) != 1 || last.Changes[0].Op != model.OpCreate || len(last.Events) != 1 || last.Events[0].Type != model.EventMemoryDeleted || last.Events[0].Provenance.Method != "memory.delete" {
							t.Fatal("new tombstone must physically create its row and emit memory.deleted")
						}
						emptyPresent = false
						views("")
						reverse("replaced")
						conserve("empty-forgotten")
					})
					t.Run("imported-forget-tombstone-hides-current-and-history-through-reopen", func(t *testing.T) {
						scopes = append(scopes, importedMemoryWriteScope{"memory.delete", 675})
						first := call("memory.delete", map[string]any{"id": overlayRef.ID}, 675, "")
						changes := snapshot().Changes
						last := changes[len(changes)-1]
						if len(last.Changes) != 1 || last.Changes[0].Op != model.OpPatch || len(last.Events) != 1 || last.Events[0].Type != model.EventMemoryDeleted || last.Events[0].Provenance.Method != "memory.delete" {
							t.Fatal("existing overlay forget must physically patch its row and emit memory.deleted")
						}
						sourcePresent = false
						for _, id := range []string{sourceRef.ID, overlayRef.ID} {
							call("memory.get", map[string]any{"id": id}, 0, "not_found")
							call("history.get", map[string]any{"ref": model.RefByID(id, model.Natural64(0)), "asOf": map[string]any{"revision": replacedRevision}}, 0, "not_found")
						}
						views("")
						reverse("forgotten")
						conserve("forgotten")
						refresh("overlay-forgotten")
						views("")
						reverse("forgotten-reopened")
						before := snapshot()
						replay := call("memory.delete", map[string]any{"id": overlayRef.ID}, 675, "")
						if !replay.Meta.Replayed || !bytes.Equal(replay.Result, first.Result) || !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("forget replay rewrote tombstone")
						}
						refuse("memory.delete", map[string]any{"id": sourceRef.ID}, 676, "not_found")
					})
					t.Run("imported-re-remember-new-lifetime-and-effective-reverse", func(t *testing.T) {
						scopes = append(scopes, importedMemoryWriteScope{"memory.save", 677})
						reply := call("memory.save", map[string]any{"key": b13SourceKey, "content": "Re-remembered Ω\n"}, 677, "")
						var commit model.Commit
						decode(t, reply.Result, &commit)
						sourcePresent, sourceValue = true, "Re-remembered Ω\n"
						views("")
						reverse("remembered")
						conserve("remembered")
						refresh("overlay-remembered")
						views("")
						reverse("remembered-reopened")
						before := snapshot()
						replay := call("memory.save", map[string]any{"key": b13SourceKey, "content": "Re-remembered Ω\n"}, 677, "")
						if !replay.Meta.Replayed || !bytes.Equal(reply.Result, replay.Result) || !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("re-remember replay changed the new lifetime")
						}
						call("history.get", map[string]any{"ref": overlayRef, "asOf": map[string]any{"revision": replacedRevision}}, 0, "not_found")
						call("history.get", map[string]any{"ref": overlayRef, "asOf": map[string]any{"revision": commit.Revision}}, 0, "")
					})
					var first sourcePublicReply
					t.Run("core-create-replace-noop-replay", func(t *testing.T) {
						for i, content := range []string{"  Core original\n東京  ", "\tCore replaced Ω\n"} {
							key := 651 + i
							scopes = append(scopes, importedMemoryWriteScope{"memory.save", key})
							before := snapshot()
							reply := call("memory.save", map[string]any{"key": "Core Ω", "content": content}, key, "")
							if i == 0 {
								first = reply
							}
							after := snapshot()
							if len(after.Changes) != len(before.Changes)+1 {
								t.Fatal("core change failed to publish once")
							}
							views(content)
							coreValue = content
							reverse(fmt.Sprintf("core-%d", i))
							conserve(fmt.Sprintf("core-%d", i))
						}
						scopes = append(scopes, importedMemoryWriteScope{"memory.save", 653})
						before := snapshot()
						call("memory.save", map[string]any{"key": "Core Ω", "content": "\tCore replaced Ω\n"}, 653, "")
						after := snapshot()
						if !reflect.DeepEqual(before.Records, after.Records) || !reflect.DeepEqual(before.Versions, after.Versions) || !reflect.DeepEqual(before.Changes, after.Changes) || before.Meta.Head != after.Meta.Head {
							t.Fatal("same-value core save wrote history")
						}
						before = snapshot()
						replay := call("memory.save", map[string]any{"key": "Core Ω", "content": "  Core original\n東京  "}, 651, "")
						if !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) || !reflect.DeepEqual(before, snapshot()) {
							t.Fatal("replay rewound core/source state")
						}
					})

					t.Run("sql-reopen-before-forget", func(t *testing.T) {
						refresh("before-forget")
						views("\tCore replaced Ω\n")
						reverse("core-reopened")
						conserve("reopened")
					})
					t.Run("core-forget-keeps-all-source-and-settings", func(t *testing.T) {
						scopes = append(scopes, importedMemoryWriteScope{"memory.delete", 661})
						call("memory.delete", map[string]any{"id": "b13-joint-core"}, 661, "")
						coreValue = ""
						refuse("memory.delete", map[string]any{"id": "b13-joint-core"}, 662, "not_found")
						refresh("after-forget")
						views("")
						reverse("core-forgotten")
						conserve("forgotten")
					})
					t.Run("invalid-overlay-bindings-refuse-atomically", func(t *testing.T) {
						var original driver.Record
						for _, row := range snapshot().Records {
							if row.Ref.ID == overlayRef.ID {
								original = row
							}
						}
						if original.Ref.ID == "" {
							t.Fatal("missing real overlay control")
						}
						for _, name := range []string{"source-ref", "source-revision", "source-digest", "key", "extra-field", "missing-attribution"} {
							t.Run(name, func(t *testing.T) {
								var body map[string]any
								decode(t, original.Body, &body)
								binding := body["sourceOverlay"].(map[string]any)
								switch name {
								case "source-ref":
									binding["source"] = model.RefByID("other-source", model.Natural64(0))
								case "source-revision":
									binding["sourceRevision"] = model.Natural64(999)
								case "source-digest":
									binding["sourceSha256"] = hashBytes([]byte("other"))
								case "key":
									body["key"] = "other-key"
								case "extra-field":
									binding["extra"] = true
								case "missing-attribution":
									delete(binding, "principal")
								}
								mutated := original
								mutated.Body = encode(t, body)
								putImportedMemoryControl(t, store, lease, mutated)
								refuse("memory.get", map[string]any{"id": sourceRef.ID}, 0, "internal_error")
								refuse("memory.list", map[string]any{}, 0, "internal_error")
								refuse("memory.save", map[string]any{"key": "unrelated strict control", "content": "must refuse"}, 691, "internal_error")
								putImportedMemoryControl(t, store, lease, original)
							})
						}
						conserve("malformed-overlay-controls")
					})

				})
			}
		})
	}
}

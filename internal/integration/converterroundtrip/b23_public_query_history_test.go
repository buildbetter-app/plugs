package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These literals bind actual Dolt commits over the public synthetic SQL schema.
// They are bounded synthetic source-history controls, not public-native parity,
// not an expectation computed by the importer or filled from the current head.
var b23SourceCommits = []string{
	"9cdifuj7ktk76mb9jbf3l83bkj0eir7b", "hscqilkral1c73v5bm7gt76jpoi59om0", "5hdbvff4nurkabp68v4f9rbbpp8e84g9", "jfekudg6ujbjrpk1krcr6u7queej98e5", "69cv8h7t4m9igo8jij6cvk6692pobfrd", "rn0lahlbu4hr6qg6qbtt8k3jl1tg62sk", "7lb380frsk0ibk50quniufi07b5p6m3o",
}

type b23SourceFrame struct{ Request, Response json.RawMessage }

func b23SourceServer(t *testing.T, store driver.Store, lease driver.ActiveLease, denied *bool) *rpc.Server {
	t.Helper()
	ontology := service.NewOntology(nil)
	core := service.New(store, lease, pipeline.New(store, lease, pipeline.WithOntology(ontology)), service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err = rpc.BindService(registry, core); err != nil {
		t.Fatal(err)
	}
	views, err := viewops.New(store, lease, viewops.Config{OntologyRevision: model.Natural64(1), AuthorizeRead: func(_ context.Context, _ model.PrincipalRef, ref model.ResourceRef, _ string) error {
		if *denied && ref.Ref.ID == "demo-42v" {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("b23-source-history-public-cursor-key")
	if err = viewrpc.BindWithConfig(registry, views, viewrpc.Config{CursorKey: key}); err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "demo", Incarnation: meta.Incarnation, CursorKey: key, OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	return server
}
func TestB23SyntheticSQLSourceMappedPublicQueryHistoryAndPhysicalReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, mode := range []driver.RestoreHistoryMode{driver.RestoreHistoryModeFull, driver.RestoreHistoryModeSnapshot} {
			t.Run(engine+"/"+string(mode), func(t *testing.T) {
				ctx := context.Background()
				// The shipped partial archive contains synthetic extra rows and is
				// retained in its original controls, never relabelled native parity.
				complete := true
				data, source := publicSourceFixture(t, complete)
				profile, _ := admittedImportProfile(t, data)
				profile.HistoryMode = mode
				when := time.Date(2026, 11, 11, 17, 0, 0, 0, time.UTC)
				var err error
				if mode != driver.RestoreHistoryModeSnapshot {
					profile, err = BindHistoryImport(ctx, bytes.NewReader(data), profile, when)
					if err != nil {
						t.Fatal(err)
					}
				}
				store, lease, reopen := openIdentityTarget(t, engine)
				report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
				if err != nil {
					t.Fatal(err)
				}
				frames := []b23SourceFrame{}
				t.Cleanup(func() {
					root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
					if root == "" {
						root = t.TempDir()
					}
					raw, e := json.MarshalIndent(map[string]any{"engine": engine, "mode": mode, "archive_sha256": source.SHA256(), "synthetic_sql_source_commits": b23SourceCommits, "qualification": "bounded_synthetic_SQL_source_history_not_native_parity", "restored": report.Restored, "frames": frames}, "", "  ")
					if e != nil {
						t.Error(e)
						return
					}
					if e = os.WriteFile(filepath.Join(root, "b23-source-query-"+engine+"-"+string(mode)+".json"), raw, 0600); e != nil {
						t.Error(e)
					}
				})
				before := reverseState(t, store)
				denied := false
				for _, phase := range []string{"imported", "reopened"} {
					if phase == "reopened" {
						store, lease = reopen()
					}
					server := b23SourceServer(t, store, lease, &denied)
					call := func(method, label string, p map[string]any) (json.RawMessage, *model.ErrorData) {
						t.Helper()
						p["_meta"] = map[string]any{"project": "demo", "agent": "b23-source-reader"}
						req, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": label, "method": method, "params": p})
						if e != nil {
							t.Fatal(e)
						}
						response := server.Dispatch(ctx, req)
						frames = append(frames, b23SourceFrame{req, append([]byte(nil), response...)})
						var envelope struct {
							Result json.RawMessage
							Error  *struct{ Data model.ErrorData }
						}
						if e = json.Unmarshal(response, &envelope); e != nil {
							t.Fatal(e)
						}
						if envelope.Error != nil {
							return envelope.Result, &envelope.Error.Data
						}
						return envelope.Result, nil
					}
					r, e := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
					if e != nil {
						t.Fatal(e)
					}
					assertPublicHistoryBinding(t, r, source, profile, report.Restored, when)
					mappingRows, e := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
					r.Close()
					if e != nil {
						t.Fatal(e)
					}
					type window struct {
						sequence uint64
						revision model.Revision
					}
					windows := []window{}
					if mode == driver.RestoreHistoryModeSnapshot {
						if len(mappingRows) != 0 {
							t.Fatal("snapshot invented history mapping")
						}
						windows = append(windows, window{7, report.Restored.Head})
					} else {
						var evidence driver.HistoryImportEvidence
						if len(mappingRows) != 1 || json.Unmarshal(mappingRows[0].Body, &evidence) != nil {
							t.Fatal("actual mapping missing")
						}
						for _, m := range evidence.Mapping {
							if m.Sequence < 1 || m.Sequence > 7 || !reflect.DeepEqual(m.CommitIDs, []string{b23SourceCommits[m.Sequence-1]}) {
								t.Fatalf("synthetic SQL commit identity/order changed: %+v", m)
							}
							windows = append(windows, window{m.Sequence, m.Revision})
						}
					}
					for _, w := range windows {
						at := viewops.AsOf{Revision: w.revision, Incarnation: report.Restored.Incarnation}
						wantTitle := ""
						if w.sequence == 4 {
							wantTitle = "Public source created through Beads"
						} else if w.sequence >= 5 {
							wantTitle = "Public source updated through Beads"
						}
						wantLabels := []string{}
						if w.sequence >= 6 {
							wantLabels = []string{"public-source-label"}
						}
						for _, method := range []string{"view.run", "history.query"} {
							params := map[string]any{"asOf": at}
							if method == "view.run" {
								params["definition"] = map[string]any{"filter": map[string]any{}, "fields": []string{"id", "type", "labels", "fields.title"}, "sort": []string{"id"}, "budgetTokens": 1}
							} else {
								params["limit"] = 1
							}
							raw, refusal := call(method, fmt.Sprintf("%s-%s-%d", phase, method, w.sequence), params)
							if refusal != nil {
								t.Fatalf("source window refused: %+v", refusal)
							}
							var result struct {
								Items []struct {
									ID     string
									Type   string
									Fields map[string]any
									Labels []string
								}
								AsOf       viewops.AsOf
								NextCursor string
							}
							if e = json.Unmarshal(raw, &result); e != nil {
								t.Fatal(e)
							}
							wantCount := 0
							if wantTitle != "" {
								wantCount = 1
							}
							if len(result.Items) != wantCount || result.NextCursor != "" || result.AsOf != at {
								t.Fatalf("source membership/window changed: %s want title=%q", raw, wantTitle)
							}
							if wantCount == 1 {
								row := result.Items[0]
								if row.ID != "demo-42v" || row.Type != "issue" || row.Fields["title"] != wantTitle || !reflect.DeepEqual(append([]string{}, row.Labels...), wantLabels) {
									t.Fatalf("synthetic SQL imported public value changed: %s", raw)
								}
							}
						}
						raw, refusal := call("bead.get", fmt.Sprintf("%s-get-%d", phase, w.sequence), map[string]any{"id": model.RefByID("demo-42v", model.Natural64(0)), "asOf": at})
						if wantTitle == "" {
							if refusal == nil || refusal.Code != "not_found" {
								t.Fatalf("unknown historical row substituted head: %s %+v", raw, refusal)
							}
						} else {
							var result struct {
								Bead struct {
									ID     string
									Fields map[string]any
									Labels []string
								} `json:"bead"`
							}
							if refusal != nil || json.Unmarshal(raw, &result) != nil || result.Bead.ID != "demo-42v" || result.Bead.Fields["title"] != wantTitle || !reflect.DeepEqual(append([]string{}, result.Bead.Labels...), wantLabels) {
								t.Fatalf("source get changed: %s %+v", raw, refusal)
							}
						}
						for _, deny := range []bool{false, true} {
							denied = deny
							want := 0
							if !deny && wantTitle != "" {
								want = 1
							}
							raw, refusal = call("view.run", fmt.Sprintf("%s-count-%d-deny-%t", phase, w.sequence, deny), map[string]any{"asOf": at, "countOnly": true, "definition": map[string]any{"filter": map[string]any{"query": "title=Public AND type=issue"}, "fields": []string{}, "budgetTokens": 1}})
							var body map[string]json.RawMessage
							if refusal != nil || json.Unmarshal(raw, &body) != nil {
								t.Fatalf("source count refused: %s %+v", raw, refusal)
							}
							var count int
							if json.Unmarshal(body["count"], &count) != nil || count != want || string(body["items"]) != "[]" {
								t.Fatalf("source count=%s want=%d", raw, want)
							}
							for _, key := range []string{"nextCursor", "elisions", "groups"} {
								if _, ok := body[key]; ok {
									t.Fatalf("denied count leaked %s: %s", key, raw)
								}
							}
						}
						denied = false
					}
					if mode != driver.RestoreHistoryModeFull {
						// Full/partial/snapshot remain separate qualifications. Below-floor
						// reads are exact contracted refusals, never nearby source windows.
						_, refusal := call("view.run", phase+"-below-floor", map[string]any{"asOf": viewops.AsOf{Revision: model.Natural64(0), Incarnation: report.Restored.Incarnation}, "definition": map[string]any{"filter": map[string]any{}, "fields": []string{"id"}}})
						if refusal == nil || refusal.Code != "cursor_expired" {
							t.Fatalf("below-floor source read: %+v", refusal)
						}
					}
					if !reflect.DeepEqual(before, reverseState(t, store)) {
						t.Fatal("public query/count/refusal changed restored records/history/mapping")
					}
				}
			})
		}
	}
}

package webui_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/server/webui"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// Capture the bytes written by the actual HTTP adapter without replacing
// its response. Synthetic-only evidence is emitted with the browser output.
type epicRPCResponse struct {
	http.ResponseWriter
	body bytes.Buffer
}

func (w *epicRPCResponse) Write(body []byte) (int, error) {
	n, err := w.ResponseWriter.Write(body)
	_, _ = w.body.Write(body[:n])
	return n, err
}

// This is the real anonymous, no-authorization-plugin browser composition.
// It certifies stored direct-child production and rendering, not fixed-policy
// hidden counts or authenticated-browser support. EP6 qualifies the separate
// authenticated HTTP authority path; the UI does not invent a login flow.
func TestBrowserEpicProgressStoredGraph(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			sourceRef := model.RefByID("ontology/epic-browser", model.Natural64(0))
			locate := service.StaticOntologySource(sourceRef)
			config := composition.Config{Driver: engine, Project: "web"}
			var store driver.Store
			if engine == "sqlite" {
				config.DSN = filepath.Join(t.TempDir(), "epic-browser.sqlite")
				var err error
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: config.DSN})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				store = memory.New()
			}
			epic, err := viewrpc.NewAnonymousEpicProgressConfig(store, locate)
			if err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			config.EpicProgress = epic
			// Composition owns this actual driver/lease lifetime; the trusted
			// constructor's generation check observes this exact same store.
			config.StoreFactory = func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil }
			runtime, err := composition.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(ctx)

			// Only the protected ontology input is a driver fixture. All bead
			// images, parent closure and subsequent mutations use the Writer.
			source, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conformance.Seed(ctx, runtime.Store, runtime.Lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: source}}); err != nil {
				t.Fatal(err)
			}
			writer := pipeline.New(runtime.Store, runtime.Lease,
				pipeline.WithOntology(service.NewOntology(nil)),
				pipeline.WithOntologyAudit(service.NewOntologyAudit(locate)))
			principal := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			var sequence int
			apply := func(ops ...model.Op) (model.Commit, error) {
				sequence++
				return writer.Apply(ctx, principal, model.ChangeSet{Ops: ops}, model.WriteMeta{Project: "web", Method: "tx.apply", IdempotencyKey: fmt.Sprintf("%064x", sequence)})
			}
			create := func(key, parent, project, title string) model.Op {
				op := model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Kind: model.OpCreate, Key: key, Type: "issue", Project: project, Fields: map[string]any{"title": title}}}
				if parent != "" {
					ref := model.RefByKey(parent)
					op.Create.Parent = &ref
				}
				return op
			}
			seed, err := apply(
				create("parent", "", "web", "Synthetic epic"),
				create("alternate", "", "web", "Other synthetic epic"),
				create("first", "parent", "web", "First visible direct child"),
				create("second", "parent", "web", "Second visible direct child"),
				create("foreign", "parent", "other-team", "Cross-project visible direct child"),
				create("grandchild", "first", "web", "excluded-grandchild-body"),
				create("unrelated", "", "web", "excluded-unrelated-body"),
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(seed.Results) != 7 {
				t.Fatal("Writer did not commit the complete synthetic graph")
			}
			parent, alternate, first, foreign := seed.Results[0].Ref, seed.Results[1].Ref, seed.Results[2].Ref, seed.Results[4].Ref
			patchStatus := func(ref model.Ref) model.Op {
				return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: ref, Fields: map[string]any{"status": "in_progress"}}}
			}
			if _, err := apply(patchStatus(foreign)); err != nil {
				t.Fatal(err)
			}
			manifest := map[string]any{
				"engine": engine, "project": "web", "parentID": parent.ID,
				"excludedText": []string{"excluded-grandchild-body", "excluded-unrelated-body"},
				"initial":      map[string]any{"total": 3, "statuses": [][]string{{"in_progress", "1"}, {"todo", "2"}}},
				"steps": []any{
					map[string]any{"name": "status", "total": 3, "statuses": [][]string{{"in_progress", "2"}, {"todo", "1"}}},
					map[string]any{"name": "reparent", "total": 2, "statuses": [][]string{{"in_progress", "1"}, {"todo", "1"}}},
				},
			}
			mux := http.NewServeMux()
			var responsesMu sync.Mutex
			var responses []json.RawMessage
			mux.Handle(httpjson.Path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				recorded := &epicRPCResponse{ResponseWriter: w}
				runtime.HTTPHandler.ServeHTTP(recorded, r)
				responsesMu.Lock()
				defer responsesMu.Unlock()
				responses = append(responses, append(json.RawMessage(nil), recorded.body.Bytes()...))
			}))
			for _, mount := range []struct {
				path string
				cap  int
			}{{"/ui/", 5}, {"/capped/ui/", 1}} {
				ui, err := webui.New(webui.Config{MountPath: mount.path, Project: "web", MaxRows: mount.cap})
				if err != nil {
					t.Fatal(err)
				}
				mux.Handle(mount.path, ui)
			}
			mux.HandleFunc("/runtime/manifest.json", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(manifest)
			})
			var mutations sync.Mutex
			next := "status"
			mux.HandleFunc("/runtime/advance", func(w http.ResponseWriter, r *http.Request) {
				mutations.Lock()
				defer mutations.Unlock()
				if r.Method != http.MethodPost || r.URL.Query().Get("step") != next {
					http.Error(w, "unexpected synthetic fixture step", http.StatusBadRequest)
					return
				}
				var op model.Op
				if next == "status" {
					op = patchStatus(first)
				} else {
					op = model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Kind: model.OpReparent, Target: first, Parent: &alternate}}
				}
				commit, err := apply(op)
				if err != nil {
					t.Errorf("synthetic fixture Writer mutation: %v", err)
					http.Error(w, "fixture Writer commit refused", http.StatusInternalServerError)
					return
				}
				t.Logf("%s real Writer %s commit revision=%s", engine, next, commit.Revision)
				if next == "status" {
					next = "reparent"
				} else {
					next = "complete"
				}
				w.WriteHeader(http.StatusNoContent)
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			runEpicProgressBrowser(t, server.URL, "runtime")
			mutations.Lock()
			defer mutations.Unlock()
			if next != "complete" {
				t.Fatal("browser did not observe both real Writer mutation steps")
			}
			responsesMu.Lock()
			defer responsesMu.Unlock()
			var summaries int
			for _, raw := range responses {
				var response struct {
					Result struct {
						Groups []struct {
							Key string `json:"key"`
						} `json:"groups"`
					} `json:"result"`
				}
				if err := json.Unmarshal(raw, &response); err != nil {
					t.Fatalf("actual HTTP response was not JSON: %v", err)
				}
				if len(response.Result.Groups) == 0 {
					continue
				}
				summaries++
				for _, child := range seed.Results[2:] {
					if bytes.Contains(raw, []byte(child.Ref.ID)) {
						t.Fatal("count-only HTTP response exposed a child identity")
					}
				}
				if strings.Contains(string(raw), "excluded-grandchild-body") || strings.Contains(string(raw), "excluded-unrelated-body") {
					t.Fatal("count-only HTTP response exposed a stored body")
				}
			}
			if summaries != 4 {
				t.Fatalf("expected four actual complete producer responses, got %d", summaries)
			}
			if output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); output != "" {
				evidence, err := json.MarshalIndent(responses, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(output, "epic-progress-runtime-"+engine+"-responses.json"), evidence, 0o600); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

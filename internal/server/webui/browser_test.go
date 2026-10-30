package webui_test

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/webui"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

//go:embed browser_check.mjs
var browserCheck []byte

//go:embed direct_blocker_check.mjs
var directBlockerCheck []byte

// Explicit consumer fixtures exercise the embedded page; they do not qualify
// the direct-blocker producer, current authorization, or the E3 workload.
func TestBrowserDirectBlockerContractFixtures(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal(err)
	}
	edge := func(ref map[string]any, state, reason string) map[string]any {
		return map[string]any{"blocker": ref, "kind": "blocks", "state": state, "reason": reason}
	}
	verdict := func(hidden int, edges ...map[string]any) map[string]any {
		if edges == nil {
			edges = []map[string]any{}
		}
		return map[string]any{"edges": edges, "hiddenCount": hidden}
	}
	hostile := `<img src=x onerror="globalThis.blockerExecuted=true"> & '`
	fixtures := []map[string]any{
		{"name": "absent", "absent": true, "hidden": 2},
		{"name": "empty", "direct": verdict(0)},
		{"name": "active", "direct": verdict(0, edge(map[string]any{"id": "pl-blocker"}, "unsatisfied", "blocker status does not satisfy blocks")), "select": "pl-blocker"},
		{"name": "satisfied", "direct": verdict(0, edge(map[string]any{"id": "pl-done", "branch": 0}, "satisfied", ""))},
		{"name": "unknown", "direct": verdict(0, edge(map[string]any{"id": "pl-missing"}, "unknown", "blocker is missing from the snapshot"))},
		{"name": "key", "direct": verdict(0, edge(map[string]any{"key": "unresolved:key"}, "unknown", "blocker reference is unresolved"))},
		{"name": "branch", "direct": verdict(0, edge(map[string]any{"id": "pl-branch", "branch": json.Number("9007199254740993")}, "unknown", "blocker reference is unresolved"))},
		{"name": "hidden", "direct": verdict(2), "hidden": 2},
		{"name": "mixed", "direct": verdict(1, edge(map[string]any{"id": "pl-visible"}, "unsatisfied", "blocker is unsatisfied")), "hidden": 1},
		{"name": "hostile", "direct": verdict(0, edge(map[string]any{"id": hostile}, "unsatisfied", hostile)), "select": hostile},
		{"name": "padded-id", "direct": verdict(0, edge(map[string]any{"id": " pl-blocker "}, "unknown", "blocker reference is unresolved")), "unselectable": true},
		{"name": "newline-id", "direct": verdict(0, edge(map[string]any{"id": "pl-\r\nblocker"}, "unknown", "blocker reference is unresolved")), "unselectable": true},
		{"name": "unicode-padded-id", "direct": verdict(0, edge(map[string]any{"id": "\u0085pl-blocker\u0085"}, "unknown", "blocker reference is unresolved")), "unselectable": true},
		{"name": "revoked", "direct": verdict(0, edge(map[string]any{"id": "pl-before-revocation"}, "unsatisfied", "blocker is unsatisfied")), "refresh": true},
		{"name": "bad-state", "direct": verdict(0, edge(map[string]any{"id": "pl-secret"}, "active", "private-reason")), "refused": true},
		{"name": "bad-ref", "direct": verdict(0, edge(map[string]any{"id": "pl-secret", "key": "also"}, "unknown", "private-reason")), "refused": true},
		{"name": "bad-count", "direct": verdict(101), "refused": true},
	}
	mux := http.NewServeMux()
	for _, fixture := range fixtures {
		name := fixture["name"].(string)
		ui, err := webui.New(webui.Config{MountPath: "/fixtures/" + name + "/ui/", RPCPath: "/fixtures/" + name + "/rpc", Project: "web", MaxRows: 5, BlockedViewID: "view-blocked"})
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle("/fixtures/"+name+"/ui/", ui)
		var reads atomic.Int64
		mux.HandleFunc("/fixtures/"+name+"/rpc", func(w http.ResponseWriter, r *http.Request) {
			var request struct {
				ID     string         `json:"id"`
				Method string         `json:"method"`
				Params map[string]any `json:"params"`
			}
			if r.Method != http.MethodPost || r.Header.Get("Plug-RPC") != "1" || json.NewDecoder(r.Body).Decode(&request) != nil {
				t.Error("invalid fixture request")
				http.Error(w, "invalid", 400)
				return
			}
			response := map[string]any{"jsonrpc": "2.0", "id": request.ID}
			switch request.Method {
			case "server.hello":
				response["result"] = map[string]any{"authMode": "anonymous"}
			case "rpc.methods":
				response["result"] = map[string]any{"methods": []string{"view.explain"}}
			case "view.explain":
				result := map[string]any{"viewId": "view-blocked", "itemId": request.Params["itemId"], "included": true, "reasons": []string{"included"}, "hiddenBlockers": fixture["hidden"]}
				if fixture["absent"] != true {
					result["directBlockers"] = fixture["direct"]
				}
				response["result"] = result
				if reads.Add(1) > 1 && fixture["refresh"] == true {
					delete(response, "result")
					response["error"] = map[string]any{"code": -32000, "message": "access revoked", "data": map[string]any{"code": "not_found"}}
				}
			default:
				t.Error("unexpected fixture method")
				http.Error(w, "unexpected", 400)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(response); err != nil {
				t.Error(err)
			}
		})
	}
	server := httptest.NewServer(mux)
	defer server.Close()
	dir := t.TempDir()
	script, cases := filepath.Join(dir, "direct-blocker-check.mjs"), filepath.Join(dir, "cases.json")
	data, err := json.Marshal(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{script: directBlockerCheck, cases: data} {
		if err := os.WriteFile(name, body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, script, server.URL, chrome, cases)
	output, err := command.CombinedOutput()
	fmt.Print(string(output))
	if err != nil {
		t.Fatalf("direct-blocker browser fixtures: %v; output above", err)
	}
}

type pausedStore struct {
	*memory.Store
	paused    atomic.Bool
	active    atomic.Int64
	cancelled atomic.Int64
}

func (s *pausedStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	if s.paused.Load() {
		s.active.Add(1)
		defer s.active.Add(-1)
		timer := time.NewTicker(10 * time.Millisecond)
		defer timer.Stop()
		for s.paused.Load() {
			select {
			case <-ctx.Done():
				s.cancelled.Add(1)
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return s.Store.Read(ctx, view)
}

// TestBrowserActualStoredRowsAndStateTransitions drives the actual embedded
// page, HTTP admission, RPC registry, service, pipeline and memory store.
// The pause only holds a real driver read; no product response is synthesized.
func TestBrowserActualStoredRowsAndStateTransitions(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("browser evidence requires Node 24 with its built-in WebSocket:", err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal("browser evidence requires google-chrome:", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &pausedStore{Store: memory.New()}
	defer store.Close()
	lease, err := store.Lease(ctx, "webui-browser", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(context.Background())
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if lease.Renew(ctx) != nil {
					return
				}
			}
		}
	}()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	// The first-created seed sorts onto the last page. Refresh must update a
	// displayed seed, independent of creation order or random ID allocation.
	ids := []string{"pl-webui-c", "pl-webui-a", "pl-webui-b"}
	var allocated atomic.Int64
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
		index := allocated.Add(1) - 1
		if index >= int64(len(ids)) {
			return "", fmt.Errorf("unexpected browser fixture create %d", index+1)
		}
		return ids[index], nil
	}))
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "web", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := httpjson.New(dispatcher, auth, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(httpjson.Path, admitted)
	for _, project := range []string{"web", "empty"} {
		mount := "/ui/"
		if project == "empty" {
			mount = "/empty/ui/"
		}
		config := webui.Config{MountPath: mount, RPCPath: httpjson.Path, Project: project, MaxRows: 2}
		if project == "web" {
			config.Frames = []webui.FrameBundle{
				{ID: "scope-probe", Grant: webui.BridgeGrant{Project: "web", Methods: []string{"bead.ready"}}, Assets: map[string][]byte{"index.html": []byte(`<!doctype html><title>Scope probe</title>`)}},
				{ID: "sibling-probe", Grant: webui.BridgeGrant{Project: "sibling"}, Assets: map[string][]byte{"index.html": []byte(`<!doctype html><title>Sibling probe</title>`)}},
			}
		}
		ui, err := webui.New(config)
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle(mount, ui)
	}
	// Fixture controls change only the real test store's lifetime/read gate.
	// They are never included in the product handler or a production listener.
	mux.HandleFunc("/fixture/pause", func(w http.ResponseWriter, r *http.Request) {
		store.paused.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/resume", func(w http.ResponseWriter, r *http.Request) {
		store.paused.Store(false)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/fence", func(w http.ResponseWriter, r *http.Request) {
		if err := lease.Release(ctx); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"active": store.active.Load(), "cancelled": store.cancelled.Load()})
	})
	server := httptest.NewServer(mux)
	defer func() {
		store.paused.Store(false)
		server.Close()
	}()
	var seedEvidence []map[string]any
	for index, title := range []string{"Review the stored change", "<img src=x onerror=alert(1)>", "An unread page stays unread"} {
		params := map[string]any{"type": "issue", "fields": map[string]any{"title": title}, "_meta": map[string]any{"project": "web", "idempotencyKey": strings.Repeat(fmt.Sprintf("%x", index+1), 64)}}
		response := rpcRequest(t, server.Client(), server.URL+httpjson.Path, "bead.create", params)
		if response["error"] != nil {
			t.Fatalf("fixture seed failed: %v", response)
		}
		seedEvidence = append(seedEvidence, map[string]any{"method": "bead.create", "params": params, "response": response})
	}
	evidence := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if evidence == "" {
		evidence = t.TempDir()
	}
	if err := os.MkdirAll(evidence, 0o755); err != nil {
		t.Fatal(err)
	}
	seed, err := json.MarshalIndent(seedEvidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evidence, "seed-rpc.json"), seed, 0o644); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "browser-check.mjs")
	if err := os.WriteFile(script, browserCheck, 0o600); err != nil {
		t.Fatal(err)
	}
	commandCtx, stop := context.WithTimeout(ctx, 90*time.Second)
	defer stop()
	command := exec.CommandContext(commandCtx, node, script, server.URL, evidence, chrome)
	output, err := command.CombinedOutput()
	fmt.Print(string(output))
	if err != nil {
		t.Fatalf("live browser check failed: %v; output above", err)
	}
	if store.active.Load() != 0 || store.cancelled.Load() < 1 {
		t.Fatalf("cancelled read retained: active=%d cancelled=%d", store.active.Load(), store.cancelled.Load())
	}
	// The harness makes three creates and one update through real HTTP/RPC.
	// All browser product calls must remain reads, as checked by the CDP log.
	current, err := store.Meta(ctx)
	if err != nil || current.Head.String() != "4" {
		t.Fatalf("read-only browser changed store head=%s err=%v", current.Head.String(), err)
	}
	fmt.Printf("PASS read_only_store_head=4 fixture_writes=4 cancelled_real_reads=%d evidence=%s\n", store.cancelled.Load(), evidence)
}

//go:embed comment_check.mjs
var commentCheck []byte

// Explicit consumer fixtures do not qualify restore admission or record policy.
func TestBrowserCommentProvenanceContractFixtures(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal(err)
	}
	actor := map[string]any{"kind": "user", "id": "restore-admin"}
	legacy := map[string]any{"body": "old body", "principal": actor, "time": "2026-10-30T10:00:00Z"}
	imported := func(author any) map[string]any {
		v := map[string]any{"body": `<img src=x onerror="globalThis.commentExecuted=true">`, "principal": actor, "time": "2026-10-30T10:00:00Z", "origin": "import", "sourceTime": "2020-01-01T00:00:00Z"}
		if author != nil {
			v["sourceAuthor"] = author
		}
		return v
	}
	fixtures := map[string]any{"legacy": []any{legacy}, "import": []any{imported("source-user")}, "hostile": []any{imported(`<script>globalThis.commentExecuted=true</script>`)}, "no-author": []any{imported(nil)}, "empty": []any{}, "absent": nil, "null": json.RawMessage(`null`), "refresh": []any{imported("secret-source")}}
	for name, extra := range map[string]map[string]any{"empty-origin": {"origin": ""}, "null-source": {"origin": "import", "sourceTime": nil}, "null-author": {"origin": "import", "sourceTime": "2020-01-01T00:00:00Z", "sourceAuthor": nil}, "empty-author": {"origin": "import", "sourceTime": "2020-01-01T00:00:00Z", "sourceAuthor": ""}, "legacy-source": {"sourceTime": nil}, "control": {"origin": "import", "sourceTime": "2020-01-01T00:00:00Z", "sourceAuthor": "bad\nauthor"}} {
		v := map[string]any{}
		for k, x := range legacy {
			v[k] = x
		}
		for k, x := range extra {
			v[k] = x
		}
		fixtures[name] = []any{v}
	}
	mux := http.NewServeMux()
	for name, comments := range fixtures {
		mount := "/fixtures/" + name + "/ui/"
		endpoint := "/fixtures/" + name + "/rpc"
		ui, err := webui.New(webui.Config{MountPath: mount, RPCPath: endpoint, Project: "web", MaxRows: 5})
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle(mount, ui)
		var reads atomic.Int64
		mux.HandleFunc(endpoint, func(w http.ResponseWriter, r *http.Request) {
			var request struct {
				ID     string         `json:"id"`
				Method string         `json:"method"`
				Params map[string]any `json:"params"`
			}
			if json.NewDecoder(r.Body).Decode(&request) != nil {
				t.Error("invalid fixture request")
				return
			}
			reply := map[string]any{"jsonrpc": "2.0", "id": request.ID}
			switch request.Method {
			case "server.hello":
				reply["result"] = map[string]any{"authMode": "anonymous"}
			case "rpc.methods":
				reply["result"] = map[string]any{"methods": []string{"bead.get"}}
			case "bead.get":
				v := map[string]any{"bead": map[string]any{"id": "pl-comments", "revision": 1}, "attachments": []any{}}
				if name != "absent" {
					v["comments"] = comments
				}
				reply["result"] = v
				if name == "refresh" && reads.Add(1) > 1 {
					delete(reply, "result")
					reply["error"] = map[string]any{"code": -32000, "message": "fixture access revoked", "data": map[string]any{"code": "forbidden"}}
				}
			default:
				t.Errorf("unexpected fixture method %s", request.Method)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(reply); err != nil {
				t.Error(err)
			}
		})
	}
	server := httptest.NewServer(mux)
	defer server.Close()
	dir := t.TempDir()
	script := filepath.Join(dir, "comment-check.mjs")
	if err := os.WriteFile(script, commentCheck, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, script, server.URL, chrome)
	out, err := cmd.CombinedOutput()
	fmt.Print(string(out))
	if err != nil {
		t.Fatalf("comment browser fixtures: %v", err)
	}
}

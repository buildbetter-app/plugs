package webui_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/server/webui"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
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

//go:embed watch_bootstrap_check.mjs
var watchBootstrapCheck []byte

func TestBrowserStoredOntologyWatchBootstrap(t *testing.T) {
	for _, mode := range []string{"ontology", "visibility", "view"} {
		t.Run(mode, func(t *testing.T) { runBrowserWatchBootstrap(t, mode) })
	}
}
func runBrowserWatchBootstrap(t *testing.T, mode string) {
	verifyUX := true
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "browser-view-watch", driver.LeaseTTL)
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
	seedOntology := func(op model.OpKind) error {
		source := service.OntologySource{Base: ontology.DefaultDocument()}
		raw, err := service.MarshalOntologySource(source)
		if err != nil {
			return err
		}
		_, err = conformance.Seed(ctx, store, lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "user", ID: "admin"}}, []conformance.FixtureChange{{Ref: model.RefByID("ontology/current", model.Natural64(0)), Kind: "ontology", Op: op, Body: raw}})
		return err
	}
	if err := seedOntology(model.OpCreate); err != nil {
		t.Fatal(err)
	}
	ids := []string{"pl-browser-c", "pl-browser-a", "pl-browser-b", "pl-browser-secret", "pl-browser-foreign", "pl-browser-view"}
	var allocated atomic.Int64
	ontology := service.NewOntology(nil)
	options := []pipeline.Option{pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
		i := allocated.Add(1) - 1
		if i >= int64(len(ids)) {
			return "", fmt.Errorf("unexpected create")
		}
		return ids[i], nil
	})}
	if verifyUX {
		// Claims require committed slot provenance for actual event replay.
		// Use the admitted audit implementation; its missing source locator
		// explicitly refuses conditional-edge decisions, which this fixture
		// does not create. Keep the original legacy nil-plan journey above.
		options = append(options, pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)))
	}
	writer := pipeline.New(store, lease, options...)
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	policy := &browserReadPolicy{service: domain}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	var key atomic.Int64
	setMeta := func(id, project, name string, value any) error {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		_, err = writer.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Kind: model.OpSetMeta, Target: model.RefByID(id, model.Natural64(0)), Key: name, Value: raw}}}}, model.WriteMeta{Project: project, Method: "bead.apply", IdempotencyKey: fmt.Sprintf("%064x", key.Add(1))})
		return err
	}
	budget := model.Natural64(2)
	definition := viewops.Definition{Filter: &viewops.Filter{Resources: ids[:4]}, Sort: []string{"id"}, BudgetTokens: &budget}
	saved := &storedBrowserViews{policy: policy}
	viewStore := &pausedStore{Store: store}
	views, err := viewops.New(viewStore, lease, viewops.Config{Views: saved, AuthorizeRead: policy.authorize, OntologyRevision: model.Natural64(1), PolicyVersion: "stored-browser-grants-v1"})
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := collab.New(ctx, collab.Config{Service: domain, Lease: lease, AuthorizationInstalled: func() bool { return true }, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	defer sessions.Close()
	codec, err := serverevents.NewCursorCodec([]byte(strings.Repeat("browser-watch-key-", 3)))
	if err != nil {
		t.Fatal(err)
	}
	events := &serverevents.Service{Reader: &coreevents.Reader{Log: serverevents.Log{Store: store}, Materializer: serverevents.HistoryMaterializer{Store: store}, Authorization: policy}, Cursors: codec}
	watches, err := watch.New(ctx, watch.Config{Events: events, Coalesce: 5 * time.Millisecond, MaxLag: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer watches.Close()
	adapter, err := watchhttp.New(watches, watchhttp.Config{Views: views, Ontology: model.Natural64(1), PolicyVersion: "stored-browser-grants-v1"})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, bind := range []func() error{func() error { return rpc.BindService(registry, domain) }, func() error { return collab.Bind(registry, sessions) }, func() error { return viewrpc.Bind(registry, views) }, func() error { return adapter.Register(registry) }} {
		if err := bind(); err != nil {
			t.Fatal(err)
		}
	}
	meta, _ := store.Meta(ctx)
	dispatcher, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "web", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	bridge := func(ctx context.Context, by model.PrincipalRef, id, agent string) (context.Context, error) {
		lifetime, err := sessions.SessionContext(ctx, collab.Caller{Principal: by, Agent: agent}, id)
		if err != nil {
			return nil, err
		}
		return watchhttp.WithSession(ctx, id, lifetime), nil
	}
	transport, err := httpjson.New(dispatcher, admission, httpjson.Config{SessionBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	var injected atomic.Bool
	mux.Handle(httpjson.Path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var call struct {
			Method string `json:"method"`
			Params struct {
				From string `json:"from"`
			} `json:"params"`
		}
		if json.Unmarshal(raw, &call) != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		if call.Method == "view.watch" && call.Params.From != "" && injected.CompareAndSwap(false, true) {
			_, err := domain.Update(ctx, by, service.UpdateRequest{ID: model.RefByID("pl-browser-a", model.Natural64(0)), Fields: map[string]any{"title": "Intervening before reopen"}}, model.WriteMeta{Project: "web", IdempotencyKey: fmt.Sprintf("%064x", 901)})
			if err != nil {
				t.Error(err)
				http.Error(w, "intervening real write failed", 500)
				return
			}
		}
		transport.ServeHTTP(w, r)
	}))
	var attached atomic.Int64
	var streamMu sync.Mutex
	var delivered []map[string]any
	mux.Handle(watchhttp.DefaultPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := admission.AdmitHTTP(r)
		if err != nil {
			http.Error(w, "admission refused", http.StatusForbidden)
			return
		}
		ctx, err := bridge(r.Context(), identity.Principal(), r.Header.Get("Plug-Session"), r.Header.Get("Plug-Agent"))
		if err != nil {
			http.Error(w, "session refused", http.StatusForbidden)
			return
		}
		attached.Add(1)
		defer attached.Add(-1)
		recorded := &recordedStream{ResponseWriter: w}
		adapter.ServeHTTP(recorded, r.WithContext(watchhttp.WithPrincipal(ctx, identity.Principal())))
		streamMu.Lock()
		delivered = append(delivered, map[string]any{"contentType": w.Header().Get("Content-Type"), "ndjson": recorded.bytes.String()})
		streamMu.Unlock()
	}))
	for _, mount := range []struct{ path, view string }{{"/ui/", "home"}, {"/empty/ui/", "empty"}, {"/unsupported/ui/", "unsupported"}, {"/oversize/ui/", "oversize"}, {"/missing/ui/", "missing"}, {"/inline/ui/", ""}, {"/slow/ui/", "slow"}} {
		config := webui.Config{MountPath: mount.path, Project: "web", MaxRows: 2, HomeViewID: mount.view, BlockedViewID: "home"}
		if mount.view != "" {
			config.WatchPath = watchhttp.DefaultPath
		}
		ui, err := webui.New(config)
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle(mount.path, ui)
	}
	mux.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		rows, err := sessions.List(r.Context(), collab.Caller{Principal: by}, "web")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		count := 0
		for _, row := range rows {
			count += watches.WatchCount(row.SessionID)
		}
		current, _ := store.Meta(ctx)
		_ = json.NewEncoder(w).Encode(map[string]any{"sessions": len(rows), "watches": count, "streams": attached.Load(), "policyChecks": policy.checks.Load(), "head": current.Head, "activeReads": viewStore.active.Load(), "cancelledReads": viewStore.cancelled.Load(), "openingReads": saved.active.Load()})
	})
	mux.HandleFunc("/fixture/pause-view", func(w http.ResponseWriter, r *http.Request) {
		viewStore.paused.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/resume-view", func(w http.ResponseWriter, r *http.Request) {
		viewStore.paused.Store(false)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/hold-open", func(w http.ResponseWriter, r *http.Request) {
		saved.held.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/release-open", func(w http.ResponseWriter, r *http.Request) {
		saved.held.Store(false)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/reset", func(w http.ResponseWriter, r *http.Request) {
		changed := definition
		changed.Filter = &viewops.Filter{Resources: ids[:1]}
		if err := setMeta("pl-browser-view", "config", "home", changed); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/restore", func(w http.ResponseWriter, r *http.Request) {
		if err := setMeta("pl-browser-view", "config", "home", definition); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/revoke", func(w http.ResponseWriter, r *http.Request) {
		if err := setMeta("pl-browser-a", "web", "browserReaders", []string{}); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/fixture/delivered", func(w http.ResponseWriter, r *http.Request) {
		streamMu.Lock()
		defer streamMu.Unlock()
		_ = json.NewEncoder(w).Encode(delivered)
	})
	mux.HandleFunc("/fixture/ontology", func(w http.ResponseWriter, r *http.Request) {
		if err := seedOntology(model.OpPatch); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	for i, id := range ids {
		project := "web"
		if id == "pl-browser-foreign" {
			project = "foreign"
		}
		if id == "pl-browser-view" {
			project = "config"
		}
		response := rpcRequest(t, server.Client(), server.URL+httpjson.Path, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": []string{"Stored C", "<img src=x onerror=alert(1)>", "Stored B", "SECRET DENIED", "FOREIGN DENIED", "Saved browser definitions"}[i]}, "_meta": map[string]any{"project": project, "idempotencyKey": fmt.Sprintf("%064x", 100+i)}})
		if response["error"] != nil {
			t.Fatalf("seed: %v", response)
		}
		if id != "pl-browser-secret" {
			if err := setMeta(id, project, "browserReaders", []string{"anonymous"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	empty := definition
	empty.Filter = &viewops.Filter{Resources: []string{"pl-browser-absent"}}
	unsupported := definition
	unsupported.Filter = &viewops.Filter{Query: "status=todo"}
	oversize := definition
	largeBudget := model.Natural64(3)
	oversize.BudgetTokens = &largeBudget
	for name, value := range map[string]viewops.Definition{"home": definition, "empty": empty, "unsupported": unsupported, "oversize": oversize, "slow": definition} {
		if err := setMeta("pl-browser-view", "config", name, value); err != nil {
			t.Fatal(err)
		}
	}

	script := filepath.Join(t.TempDir(), "watch-bootstrap-check.mjs")
	if err := os.WriteFile(script, watchBootstrapCheck, 0600); err != nil {
		t.Fatal(err)
	}
	commandCtx, stop := context.WithTimeout(ctx, 90*time.Second)
	defer stop()
	command := exec.CommandContext(commandCtx, node, script, server.URL, chrome, mode)
	output, err := command.CombinedOutput()
	fmt.Print(string(output))
	if err != nil {
		t.Fatalf("actual ontology/watch bootstrap browser: %v", err)
	}
	rows, err := sessions.List(ctx, collab.Caller{Principal: by}, "web")
	if err != nil || len(rows) != 0 || attached.Load() != 0 || policy.checks.Load() == 0 {
		t.Fatalf("cleanup sessions=%d streams=%d checks=%d error=%v", len(rows), attached.Load(), policy.checks.Load(), err)
	}
	streamMu.Lock()
	raw, err := json.Marshal(delivered)
	streamMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"watch.reset", "ontology_changed", "watch.delta"} {
		if !bytes.Contains(raw, []byte(kind)) {
			t.Fatalf("missing actual served %s evidence", kind)
		}
	}
	evidence := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if evidence != "" {
		if err := os.WriteFile(filepath.Join(evidence, "watch-bootstrap-"+mode+"-served.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Printf("PASS actual_ontology_watch_bootstrap cleanup_no_sessions_or_streams current_policy_checks=%d\n", policy.checks.Load())
}

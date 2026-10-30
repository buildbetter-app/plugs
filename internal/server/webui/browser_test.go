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

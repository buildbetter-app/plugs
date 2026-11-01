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
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/webui"
)

//go:embed sandbox_check.mjs
var sandboxCheck []byte

// Keep the actual CSP, sandbox attributes, fetch credentials mode and browser
// message identity intact. DevTools only drives test actions in the real frame;
// the bundle's own external script must load without injected initialization.
func TestBrowserPluginSandboxSafety(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := rpc.New(registry, rpc.Config{Incarnation: "sandbox-safety", DefaultProject: "web"})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	rpcHandler, err := httpjson.New(dispatcher, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	assets := map[string][]byte{
		"index.html": []byte(`<!doctype html><title>Sandbox probe</title><script src="./frame.js" defer></script><script type="module" src="./module.js"></script><p id="ready">Loading</p>`),
		"frame.js":   []byte(`globalThis.safetyLoaded = true; document.querySelector("#ready").textContent = "Bundle script loaded";`),
		"module.js":  []byte(`globalThis.safetyModuleLoaded = true;`),
	}
	digest, err := webui.FrameBundleDigest(assets)
	if err != nil {
		t.Fatal(err)
	}
	ui, err := webui.New(webui.Config{Project: "web", Frames: []webui.FrameBundle{
		{ID: "probe", Digest: digest, Grant: webui.BridgeGrant{Methods: []string{"rpc.methods"}}, Assets: assets},
		{ID: "sibling", Digest: digest, Grant: webui.BridgeGrant{}, Assets: assets},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var credentials, outsideAssets, egress, rpcCalls, siblingDocuments atomic.Int64
	trap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		egress.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer trap.Close()
	mux := http.NewServeMux()
	mux.Handle("/ui/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ui/frames/sibling/index.html" {
			siblingDocuments.Add(1)
		}
		ui.ServeHTTP(w, r)
	}))
	mux.Handle(httpjson.Path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcCalls.Add(1)
		if len(r.Header.Values("Cookie")) != 0 || len(r.Header.Values("Authorization")) != 0 {
			credentials.Add(1)
		}
		rpcHandler.ServeHTTP(w, r)
	}))
	mux.HandleFunc("/safety-stats", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"rpcCalls": rpcCalls.Load(), "siblingDocuments": siblingDocuments.Load()})
	})
	mux.HandleFunc("/outside.js", func(w http.ResponseWriter, r *http.Request) {
		outsideAssets.Add(1)
		w.Header().Set("Content-Type", "text/javascript")
		_, _ = w.Write([]byte(`globalThis.outsideBundleExecuted = true;`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	script := filepath.Join(t.TempDir(), "sandbox-check.mjs")
	if err := os.WriteFile(script, sandboxCheck, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, script, server.URL, trap.URL, chrome)
	output, err := command.CombinedOutput()
	fmt.Print(string(output))
	if err != nil {
		t.Fatalf("plugin sandbox browser check: %v; output above", err)
	}
	if credentials.Load() != 0 || outsideAssets.Load() != 0 || egress.Load() != 0 {
		t.Fatalf("sandbox crossed HTTP boundary: credentialed RPC=%d outside assets=%d egress=%d", credentials.Load(), outsideAssets.Load(), egress.Load())
	}
	t.Log("real bundle scripts loaded; frame/direct-open isolation, own-path CSP, source-bound bridge and credential-free RPC retained; no egress reached loopback trap")
}

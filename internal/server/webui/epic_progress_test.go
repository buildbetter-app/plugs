package webui_test

import (
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/webui"
)

//go:embed epic_progress_check.mjs
var epicProgressCheck []byte

// Presentation checks deliberately do not certify hierarchy, authorization,
// ontology binding or the view.run producer. Those require the real RPC path.
func TestBrowserEpicProgressPresentation(t *testing.T) {
	mux := http.NewServeMux()
	ui, err := webui.New(webui.Config{Project: "web", MaxRows: 5})
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle("/ui/", ui)
	mux.HandleFunc("/rpc/v1", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     string `json:"id"`
			Method string `json:"method"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			http.Error(w, "invalid presentation fixture request", http.StatusBadRequest)
			return
		}
		var result any
		switch request.Method {
		case "server.hello":
			result = map[string]any{"authMode": "anonymous"}
		case "rpc.methods":
			result = map[string]any{"methods": []string{}}
		default:
			t.Errorf("presentation check made an unexpected RPC: %s", request.Method)
			http.Error(w, "unexpected presentation RPC", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	runEpicProgressBrowser(t, server.URL, "presentation")
}

func runEpicProgressBrowser(t *testing.T, base, mode string) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("epic progress checks require Node 24:", err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal("epic progress checks require google-chrome:", err)
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "epic-progress-check.mjs")
	if err := os.WriteFile(script, epicProgressCheck, 0o600); err != nil {
		t.Fatal(err)
	}
	evidence := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if evidence == "" {
		evidence = dir
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, script, base, chrome, mode, evidence)
	output, err := command.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		t.Fatalf("epic progress browser check failed: %v", err)
	}
}

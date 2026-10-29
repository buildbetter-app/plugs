package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestServerFlagWinsEnvironmentAndMemoryWithoutOpeningLocalStore(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.RequestURI != "/rpc/v1" || r.Header.Get("Authorization") != "Bearer test-credential" {
			t.Error("remote route/credential selection failed")
		}
		var request struct{ ID string }
		_ = json.NewDecoder(r.Body).Decode(&request)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"items": []any{}, "asOf": map[string]any{"revision": 0, "incarnation": "remote-test"}}})
	}))
	defer server.Close()
	t.Setenv("PLUG_SERVER", "http://127.0.0.1:1")
	t.Setenv("PLUG_TOKEN", "test-credential")
	factory := func(context.Context, StorageConfig) (driver.Store, func() error, error) {
		t.Fatal("configured remote path opened local storage")
		return nil, nil, nil
	}
	var out, errOut bytes.Buffer
	code := execute(context.Background(), []string{"--server", server.URL, "--memory", "--json", "ready"}, strings.NewReader(""), &out, &errOut, factory)
	if code != 0 || requests != 1 || !strings.Contains(out.String(), `"items":[]`) || errOut.Len() != 0 {
		t.Fatalf("remote flag selection: exit=%d calls=%d out=%s err=%s", code, requests, out.String(), errOut.String())
	}
	out.Reset()
	errOut.Reset()
	code = execute(context.Background(), []string{"--server=", "--memory", "ready"}, strings.NewReader(""), &out, &errOut, factory)
	if code != 2 || requests != 1 || !strings.Contains(errOut.String(), "--server must not be empty") {
		t.Fatal("empty explicit server silently used environment or local state")
	}
}

func TestConfigFileSelectsServerAfterEnvironment(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("PLUG_SERVER", "")
	t.Setenv("PLUG_TOKEN", "config-credential")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.RequestURI != "/rpc/v1" || r.Header.Get("Authorization") != "Bearer config-credential" {
			t.Error("config server selection did not use the configured endpoint or token")
		}
		var request struct{ ID string }
		_ = json.NewDecoder(r.Body).Decode(&request)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"items": []any{}, "asOf": map[string]any{"revision": 0, "incarnation": "config-server"}}})
	}))
	defer server.Close()
	if err := os.Mkdir(".plug", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(".plug", "config.yaml"), []byte("server: "+server.URL+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := Execute(context.Background(), []string{"--json", "ready"}, strings.NewReader(""), &out, &errOut)
	if code != 0 || requests != 1 || !strings.Contains(out.String(), `"items":[]`) || errOut.Len() != 0 {
		t.Fatalf("config server selection: exit=%d requests=%d out=%s err=%s", code, requests, out.String(), errOut.String())
	}
}

func TestMalformedConfigFailsClosedBeforeMemorySelection(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("PLUG_SERVER", "")
	if err := os.Mkdir(".plug", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(".plug", "config.yaml"), []byte("storage:\n  driver: sqlite\n  unsupported: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := Execute(context.Background(), []string{"--memory", "ready"}, strings.NewReader(""), &out, &errOut)
	if code != 2 || out.Len() != 0 || !strings.Contains(errOut.String(), "unsupported storage key") || strings.Contains(errOut.String(), "memory storage is disposable") {
		t.Fatalf("malformed config was not refused before local state: exit=%d out=%s err=%s", code, out.String(), errOut.String())
	}
}

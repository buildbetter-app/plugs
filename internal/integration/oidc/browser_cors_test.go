package oidc_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

//go:embed browser_cors_check.mjs
var browserCORSCheck []byte

// Drive real browser preflights and CORS response enforcement against the
// existing OIDC/core-session/RPC composition. The only TLS trust exception is
// for this test's disposable self-signed IdP/server certificate.
func TestBrowserCORSUsesBoundBearerWithoutCredentials(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal(err)
	}
	page := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<!doctype html><title>Public-client CORS fixture</title>"))
	})
	allowed := httptest.NewServer(page)
	defer allowed.Close()
	other := httptest.NewServer(page)
	defer other.Close()
	idp := newIDP(t)
	f := realHTTP(t, idp, allowed.URL)
	session := loginOverHTTP(t, f, allowed.URL)
	input, err := json.Marshal(map[string]string{"allowed": allowed.URL, "other": other.URL, "rpc": f.url + "/rpc/v1", "token": session.AccessToken})
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "browser-cors-check.mjs")
	if err := os.WriteFile(script, browserCORSCheck, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, script, chrome)
	// The synthetic bearer is input only: never a file, command argument, log
	// or returned browser-evidence field.
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	fmt.Print(string(output))
	if err != nil {
		t.Fatalf("real browser CORS check: %v; output above", err)
	}
}

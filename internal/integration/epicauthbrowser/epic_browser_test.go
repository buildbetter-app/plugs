package epicauthbrowser_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

//go:embed epic_browser_check.mjs
var browserCheck []byte

func TestAuthenticatedChromeSignsInThroughNativeRPC(t *testing.T) {
	f := newFixture(t, "sqlite", t.TempDir()+"/signin.db", true)
	parent := f.create(t, epics, "browser epic", nil)
	f.create(t, visible, "readable child", &parent)
	f.create(t, hidden, "unreadable child", &parent)
	key := f.issue(t, model.PrincipalRef{Kind: "user", ID: "user:browser-reader", Caveats: map[string]any{"projects": []model.Ref{projectRef(epics), projectRef(visible)}}})
	runBrowser(t, map[string]any{"base": strings.TrimSuffix(f.api.url, "/rpc/v1"), "token": key.Token, "parent": parent.ID, "visible": 1, "hidden": 1})
}

func runBrowser(t *testing.T, input map[string]any) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal(err)
	}
	input["chrome"] = chrome
	script := filepath.Join(t.TempDir(), "epic-browser.mjs")
	if err := os.WriteFile(script, browserCheck, 0600); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, script)
	// Disposable credentials travel only over stdin, never argv or evidence.
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.CombinedOutput()
	t.Log(string(out))
	if err != nil {
		t.Fatalf("authenticated Chrome assertions failed: %v", err)
	}
}

// Keep every fixture request on the public transport rather than an in-process
// dispatch path. The full corpus/browser lifecycle is added after sign-in RED.
var _ = http.StatusOK

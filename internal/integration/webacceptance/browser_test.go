package webacceptance_test

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// browserCheck is a supplemental rendering check. The Go assertions remain
// the reproducible HTTP gate; this script only drives an installed browser
// against the same real fixture and records its requests/screenshots.
//
//go:embed browser_check.mjs
var browserCheck []byte

func TestSharedServerBrowserRendersRealRowsAndRefusals(t *testing.T) {
	f := newFixture(t, sqliteBackend)
	defer f.stop()
	ref, _ := f.create("seed <unsafe> row", "writer")

	node := hermeticNode(t)
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal("browser evidence requires google-chrome:", err)
	}
	evidence := os.Getenv("PLUG_WEB_ACCEPTANCE_EVIDENCE_DIR")
	if evidence == "" {
		evidence = os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	}
	if evidence == "" {
		evidence = t.TempDir()
	}
	if err := os.MkdirAll(evidence, 0o755); err != nil {
		t.Fatal(err)
	}
	seed := map[string]any{
		"id":             ref,
		"project":        project,
		"title":          "row added after browser refresh",
		"idempotencyKey": strings.Repeat("b", 64),
	}
	data, err := json.MarshalIndent(seed, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evidence, "browser-fixture.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "browser-check.mjs")
	if err := os.WriteFile(script, browserCheck, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := contextWithTimeout(t, 120*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, script, f.server.URL, evidence, chrome)
	output, err := command.CombinedOutput()
	fmt.Print(string(output))
	if err != nil {
		t.Fatalf("real browser acceptance failed: %v; output above", err)
	}
	for _, name := range []string{"browser-rpc.json", "browser-console.json", "home.png", "refreshed.png", "empty.png", "refused.png"} {
		if _, err := os.Stat(filepath.Join(evidence, name)); err != nil {
			t.Fatalf("browser evidence missing %s: %v", name, err)
		}
	}
	f.t.Helper()
}

func contextWithTimeout(t *testing.T, timeout time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), timeout)
}

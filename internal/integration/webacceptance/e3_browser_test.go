package webacceptance_test

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

//go:embed e3_browser_check.mjs
var e3BrowserCheck []byte

// This test verifies the recorder and real integration invariants. A green
// Bazel result is not an E3 qualification: task receipts separately retain
// BLOCKED, FAILED and TIMEOUT outcomes for missing or failing product paths.
func TestE3ScriptedBrowserTaskReceipts(t *testing.T) {
	f := newE3FixtureMode(t, true)
	started := time.Now()
	f.populate(1, 100)
	grant := f.seedBrowserMetadata()
	f.runAgents()
	f.populate(101, e3Beads)
	f.runPopulatedAgentUpdates()
	f.verifyStoredCorpus()
	f.receipt.PreparationElapsedMS = float64(time.Since(started).Microseconds()) / 1000
	f.receipt.Complete = true

	f.verifyPolicyBoundaries()
	writeE3JSON(t, filepath.Join(f.evidence, "browser-input.json"), map[string]any{
		"project": e3Project, "storedBeads": f.receipt.StoredBeads, "agents": f.receipt.Agents,
		"homeViewId": e3ID(99), "blockedViewId": e3ID(98), "grantCarrierId": e3ID(100), "grantKey": e3GrantKey,
		"grant": grant, "blockedId": e3ID(52), "blockerId": e3ID(51), "deniedId": e3ID(60),
		"updateKey": e3Key("browser-live-update"), "revokeKey": e3Key("browser-revoke"),
		"sourceHead": f.receipt.SourceHead, "sourceTree": f.receipt.SourceTree,
	})
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("E3 recorder requires actual Node:", err)
	}
	chrome, err := exec.LookPath("google-chrome")
	if err != nil {
		t.Fatal("E3 recorder requires actual Chrome:", err)
	}
	script := filepath.Join(t.TempDir(), "e3-browser.mjs")
	if err := os.WriteFile(script, e3BrowserCheck, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 6*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, node, script, f.server.URL, f.evidence, chrome)
	output, runErr := command.CombinedOutput()
	fmt.Print(string(output))
	if runErr != nil {
		t.Errorf("E3 browser recorder execution: %v (task evidence retained)", runErr)
	}
	var receipt struct {
		Qualification string `json:"qualification"`
		Tasks         []struct {
			Name      string   `json:"name"`
			Status    string   `json:"status"`
			ElapsedMS *float64 `json:"elapsedMs"`
		} `json:"tasks"`
	}
	raw, err := os.ReadFile(filepath.Join(f.evidence, "browser-outcomes.json"))
	if err != nil || json.Unmarshal(raw, &receipt) != nil {
		t.Fatal("E3 machine-readable browser outcomes absent or invalid:", err)
	}
	if len(receipt.Tasks) != 5 {
		t.Errorf("E3 recorder must retain all five tasks, got %d", len(receipt.Tasks))
	}
	for _, task := range receipt.Tasks {
		if task.Name == "" || task.Status == "" || task.ElapsedMS == nil {
			t.Error("E3 task has no name/outcome/timing")
		}
		t.Logf("E3 task %s: %s; elapsed=%v ms", task.Name, task.Status, task.ElapsedMS)
	}
	// Recount actual stored bodies after the browser's real RPC update and
	// stored revocation. Never infer corpus preservation from a seed counter.
	f.verifyStoredCorpus()
	f.receipt.Qualification = "real browser recorder executed; per-task outcomes in browser-outcomes.json; no all-five qualification"
	writeE3JSON(t, filepath.Join(f.evidence, "policy-counters.json"), map[string]any{
		"checks": f.browser.policy.checks.Load(), "allows": f.browser.policy.allows.Load(), "denials": f.browser.policy.denials.Load(),
		"scope": "explicit current persisted fixture grants; installed production policy remains unqualified",
	})
	t.Logf("E3 qualification=%s; actual browser/RPC/NDJSON evidence=%s", receipt.Qualification, f.evidence)
}

func (f *e3Fixture) verifyPolicyBoundaries() {
	f.t.Helper()
	policy := f.browser.policy
	reader := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	checks := []struct {
		Name      string
		Principal model.PrincipalRef
		ID        string
		Project   string
		Allowed   bool
	}{
		{"stored grant", reader, e3ID(1), e3Project, true},
		{"explicitly ungranted member", reader, e3ID(60), e3Project, false},
		{"different principal", model.PrincipalRef{Kind: "user", ID: "ungranted"}, e3ID(1), e3Project, false},
		{"foreign project", reader, e3ID(1), "other-project", false},
	}
	for _, check := range checks {
		allowed, err := policy.allowed(f.ctx, check.Principal, check.ID, check.Project)
		if err != nil || allowed != check.Allowed {
			f.t.Fatalf("E3 current stored policy %s: allowed=%v err=%v", check.Name, allowed, err)
		}
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := policy.allowed(ctx, reader, e3ID(1), e3Project); err == nil {
		f.t.Fatal("E3 canceled policy evaluation did not fail closed")
	}
	writeE3JSON(f.t, filepath.Join(f.evidence, "policy-boundaries.json"), checks)
}

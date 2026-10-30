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

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/watch"
)

//go:embed e3_browser_check.mjs
var e3BrowserCheck []byte

// Pure recorder checks execute the same exported predicate as the browser
// script. Artificial evidence here cannot qualify a transport or E3 workload.
func TestE3WhyBlockedRecorderEvidence(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("recorder evidence checks require Node:", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "e3-browser.mjs"), e3BrowserCheck, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, node, "--input-type=module", "-")
	command.Dir = dir
	command.Stdin = strings.NewReader(`
import assert from "node:assert/strict";
import { assessWhyBlockedEvidence as assess } from "./e3-browser.mjs";
const reason = "blocker status does not satisfy blocks";
function fixture() {
  const request = {jsonrpc:"2.0", id:"actual-browser-request", method:"view.explain", params:{viewId:"blocked-view",itemId:"dependent",_meta:{project:"fixture-project"}}};
  const result = {viewId:"blocked-view",itemId:"dependent",included:true,reasons:["included"],directBlockers:{edges:[{blocker:{id:"readable-blocker"},kind:"blocks",state:"unsatisfied",reason}],hiddenCount:0}};
  return {startedAt:10,project:"fixture-project",viewId:"blocked-view",itemId:"dependent",blockerId:"readable-blocker",explanation:"readable-blocker: "+reason,
    wire:[{at:11,request:{postData:JSON.stringify(request)},response:{status:200},rawResponse:JSON.stringify({jsonrpc:"2.0",id:request.id,result})}]};
}
function change(f, mutate) { const r=JSON.parse(f.wire[0].rawResponse); mutate(r); f.wire[0].rawResponse=JSON.stringify(r); }
let checks=0;
function rejects(label, mutate) { const f=fixture(); mutate(f); assert.equal(assess(f).observed,false,label); checks++; }
assert.equal(assess(fixture()).observed,true); checks++;
const conditional=fixture(); change(conditional,r=>{const e=r.result.directBlockers.edges[0];e.kind="conditional-blocks";e.reason="blocker status does not satisfy conditional-blocks";});
conditional.explanation="readable-blocker: blocker status does not satisfy conditional-blocks";
assert.equal(assess(conditional).observed,true); checks++;
const zeroBranch=fixture(); change(zeroBranch,r=>{r.result.directBlockers.edges[0].blocker.branch=0;}); assert.equal(assess(zeroBranch).observed,true); checks++;
rejects("fixture-ID and safe-looking DOM text cannot supply absent capability",f=>change(f,r=>delete r.result.directBlockers));
rejects("membership reasons cannot supply an edge",f=>change(f,r=>{delete r.result.directBlockers;r.result.reasons=[f.explanation];}));
rejects("empty capability",f=>change(f,r=>{r.result.directBlockers={edges:[],hiddenCount:0};}));
rejects("denied count-only is not a readable edge",f=>change(f,r=>{r.result.directBlockers={edges:[],hiddenCount:1};}));
for (const state of ["unknown","satisfied"]) rejects(state+" is not active",f=>change(f,r=>{r.result.directBlockers.edges[0].state=state;}));
for (const value of [undefined,""," ",null,7,"included","No direct blocker reason was supplied by the server for readable-blocker.","x".repeat(129),reason+"\u0000"]) {
  rejects("missing/malformed/placeholder/unbounded reason",f=>change(f,r=>{r.result.directBlockers.edges[0].reason=value;}));
}
for (const value of ["readable-blocker",{}, {id:"dependent"}, {id:"readable-blocker",branch:1}, {id:"readable-blocker",branch:"0"}, {id:"readable-blocker",branch:-1}, {id:"readable-blocker",key:"other"}, {key:"readable-blocker"}, {id:"readable-blocker",project:"private"}]) {
  rejects("noncanonical/wrong linked reference",f=>change(f,r=>{r.result.directBlockers.edges[0].blocker=value;}));
}
rejects("unknown kind",f=>change(f,r=>{r.result.directBlockers.edges[0].kind="relates-to";}));
rejects("private edge fields",f=>change(f,r=>{r.result.directBlockers.edges[0].status="private";}));
rejects("missing hidden count",f=>change(f,r=>delete r.result.directBlockers.hiddenCount));
rejects("out-of-bound count",f=>change(f,r=>{r.result.directBlockers.hiddenCount=101;}));
rejects("out-of-bound edges",f=>change(f,r=>{r.result.directBlockers.edges=Array(101).fill(r.result.directBlockers.edges[0]);}));
rejects("wire edge without DOM reason",f=>{f.explanation="readable-blocker";});
rejects("wire edge without DOM reference",f=>{f.explanation=reason;});
rejects("unavailable placeholder cannot qualify with appended safe reason",f=>{f.explanation="No direct blocker reason was supplied by the server for readable-blocker. "+reason;});
rejects("longer rendered ID is not the expected reference",f=>{f.explanation="readable-blocker-other: "+reason;});
rejects("child rendered ID is not the expected branch-zero blocker",f=>{f.explanation="readable-blocker.1: "+reason;});
rejects("stale request",f=>{f.wire[0].at=9;});
rejects("request ID absent",f=>{const r=JSON.parse(f.wire[0].request.postData);delete r.id;f.wire[0].request.postData=JSON.stringify(r);});
rejects("wrong project",f=>{f.project="other";});
rejects("wrong view",f=>{f.viewId="other";});
rejects("wrong dependent",f=>{f.itemId="other";});
rejects("mismatched response ID",f=>change(f,r=>{r.id="unrelated";}));
rejects("excluded dependent",f=>change(f,r=>{r.result.included=false;}));
rejects("error is not capability",f=>change(f,r=>{r.error={code:-32601,message:"unsupported"};}));
rejects("failed network response",f=>{f.wire[0].response.status=500;});
rejects("missing response body",f=>delete f.wire[0].rawResponse);
rejects("newer failed matching request cannot reuse old success",f=>{f.wire.push({...f.wire[0],at:12,rawResponse:"{}"});});
console.log(checks+" recorder evidence checks passed; artificial inputs only; E3 unqualified");
`)
	output, err := command.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		t.Fatal("why-blocked recorder evidence checks:", err)
	}
}

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
	// Preserve a direct actual-manager error when transport mapping redacts
	// its internal cause. This diagnostic opens and closes a real subscription;
	// it never supplies a successful frame or substitutes for browser evidence.
	diagnostic, diagnosticErr := f.browser.watches.Open(f.ctx, watch.Request{
		Method: watch.MethodViewWatch, ViewID: e3ID(99), SessionID: f.receipt.Agents[0].SessionID,
		Principal: f.receipt.Agents[0].Principal, Ontology: model.Natural64(1), PolicyVersion: "e3-stored-policy-v1",
	})
	if diagnostic != nil {
		_ = diagnostic.Close()
	}
	t.Logf("E3 actual watch manager opening diagnostic: %v", diagnosticErr)
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
		if task.ElapsedMS != nil {
			t.Logf("E3 task %s: %s; elapsed=%.3f ms", task.Name, task.Status, *task.ElapsedMS)
		}
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

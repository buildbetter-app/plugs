package epicauthbrowser_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/collab"
)

//go:embed epic_browser_check.mjs
var browserCheck []byte

// Artificial envelopes exercise the browser's actual disclosure assertion;
// these checks cannot qualify a native/browser lifecycle or its workload.
func TestHiddenParentDisclosureAssertionOnlyExemptsExactChildID(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "epic-browser.mjs"), browserCheck, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "--input-type=module", "-")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(`
import assert from "node:assert/strict";
import { assertHiddenParentExistenceOnly as check } from "./epic-browser.mjs";
const fixture = {parent:"hidden-parent-canary",parentTitle:"hidden parent title",child:{id:"hidden-parent-canary.1",branch:0}};
const reply = () => ({jsonrpc:"2.0",id:"child-read",result:{bead:{id:fixture.child.id,parent:{exists:true},project:"ep6-visible",status:"todo",fields:{title:"authorized child"}}}});
let checks = 0;
const accepted = reply(), raw = JSON.stringify(accepted);
check(raw, fixture); checks++;
assert.equal(JSON.stringify(accepted), raw, "assertion must not redact actual evidence");
function rejects(label, mutate) {
  const response = reply(); mutate(response);
  assert.throws(() => check(JSON.stringify(response), fixture), {name:"AssertionError"}, label); checks++;
}
const locations = {
  title: (r, value) => { r.result.bead.fields.title = value; },
  project: (r, value) => { r.result.bead.project = value; },
  status: (r, value) => { r.result.bead.status = value; },
  parent: (r, value) => { r.result.bead.parent.id = value; },
  otherID: (r, value) => { r.result.other = {id:value}; },
  nestedID: (r, value) => { r.result.bead.metadata = {id:value}; },
  counts: (r, value) => { r.result.counts = {[value]:1}; },
  url: (r, value) => { r.result.attachments = [{url:"https://fixture/" + value}]; },
  watchFrame: (r, value) => { r.watchFrames = ["data: " + JSON.stringify({id:value})]; },
  domText: (r, value) => { r.domText = "outside child's own ID: " + value; },
  envelope: (r, value) => { r.id = value; },
};
for (const canary of [fixture.parent, fixture.child.id, fixture.parentTitle, "ep6-epics"]) {
  for (const [location, mutate] of Object.entries(locations)) rejects(location + " must not waive " + canary, r => mutate(r, canary));
}
rejects("a different hierarchical child ID is not exempt", r => { r.result.bead.id = fixture.parent + ".2"; });
rejects("parent status stays hidden even without a canary", r => { r.result.bead.parent.status = "todo"; });
rejects("parent counts stay hidden even without a canary", r => { r.result.bead.parent.count = 50; });
const repeated = {...fixture, child:{id:fixture.child.id + "/" + fixture.parent}};
const repeatedReply = reply(); repeatedReply.result.bead.id = repeated.child.id;
assert.throws(() => check(JSON.stringify(repeatedReply), repeated), {name:"AssertionError"}, "only the creation-parent prefix is exempt, not another occurrence in the exact child ID"); checks++;
console.log(checks + " hidden-parent assertion checks passed; artificial envelopes only");
`)
	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		t.Fatal("hidden-parent disclosure assertion checks:", err)
	}
}

func readerKey(t *testing.T, f *fixture, id string, allowed ...string) authnapikey.IssuedKey {
	t.Helper()
	refs := make([]model.Ref, 0, len(allowed))
	for _, p := range allowed {
		refs = append(refs, projectRef(p))
	}
	return f.issue(t, model.PrincipalRef{Kind: "user", ID: id, Caveats: map[string]any{"projects": refs}})
}

func TestAuthenticatedChromeSignsInThroughNativeRPC(t *testing.T) {
	f := newFixture(t, "sqlite", t.TempDir()+"/signin.db", true)
	parent := f.create(t, epics, "browser epic", nil)
	f.create(t, visible, "readable child", &parent)
	f.create(t, hidden, "unreadable child", &parent)
	key := readerKey(t, f, "user:browser-reader", epics, visible)
	runBrowser(t, map[string]any{"base": base(f), "token": key.Token, "principal": "user:browser-reader", "parent": parent.ID, "visible": 1, "hidden": 1}, nil, func() []hostTrace { return hostTraces(f) })
}

// Dedicated F-SRC-1 coverage. These small, actual native hosts isolate browser
// authentication teardown; none replaces or qualifies the mandatory 20k case.
func TestAuthenticatedChromeBackgroundAuthenticationTeardown(t *testing.T) {
	for _, scenario := range []string{"watch-stream-401", "watch-read-401", "bridge-revoked", "bridge-expired", "non-auth-and-switch", "stop-watch-revoked", "stop-watch-expired", "opening-sign-out", "opening-switch", "stale-cleanup-401"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t, "sqlite", filepath.Join(t.TempDir(), "background.db"), true)
			parent := f.create(t, epics, "background authenticated parent", nil)
			child := f.create(t, visible, "background readable child", &parent)
			secret := f.create(t, hidden, "background hidden child", &parent)
			key := readerKey(t, f, "user:background-reader", epics, visible)
			other := readerKey(t, f, "user:background-other", epics, visible)
			by, err := f.keys.Authenticate(context.Background(), key.Token)
			if err != nil {
				t.Fatal(err)
			}
			h := newBackgroundAuthHost(t, f, parent)
			defer func() {
				h.gate.unblock()
				if h.gate.done != nil {
					select {
					case <-h.gate.done:
					case <-time.After(5 * time.Second):
						t.Error("held native request did not finish during evidence cleanup")
					}
				}
				if t.Failed() {
					data, _ := json.Marshal(hostTraces(f))
					t.Logf("EP6_NATIVE_FAILED_EVIDENCE %s", base64.StdEncoding.EncodeToString(data))
					h.mu.Lock()
					data, _ = json.Marshal(h.streams)
					h.mu.Unlock()
					t.Logf("EP6_NATIVE_FAILED_STREAM_EVIDENCE %s", base64.StdEncoding.EncodeToString(data))
				}
			}()
			activeKey := key
			control := func(action string) map[string]any {
				switch action {
				case "arm-stream":
					h.gate.arm("watch.attach")
				case "arm-get":
					h.gate.arm("bead.get")
				case "arm-view":
					h.gate.arm("view.run")
				case "arm-open-reply":
					h.gate.armAt("session.open", true)
				case "arm-close":
					h.gate.arm("session.close")
				case "wait-held":
					h.gate.wait(t)
				case "release":
					h.gate.unblock()
					select {
					case <-h.gate.done:
					case <-time.After(5 * time.Second):
						t.Fatal("held native request did not finish")
					}
				case "revoke":
					if !f.keys.Revoke(activeKey.ID) {
						t.Fatal("actual API key was not revoked")
					}
				case "expiring-key":
					activeKey, err = f.keys.Issue(by, time.Now().Add(5*time.Second))
					if err != nil {
						t.Fatal(err)
					}
					return map[string]any{"token": activeKey.Token}
				case "wait-expired":
					if wait := time.Until(activeKey.ExpiresAt); wait > 0 {
						time.Sleep(wait + time.Millisecond)
					}
				case "close-session":
					if err := h.sessions.CloseSession(context.Background(), collab.Caller{Principal: by}, h.gate.session); err != nil {
						t.Fatal(err)
					}
				case "write-parent":
					committed(t, f.write(t, f.owner, "bead.update", epics, map[string]any{"id": parent, "fields": map[string]any{"title": "background native update"}}))
				case "stats":
					presence, err := h.sessions.List(context.Background(), collab.Caller{Principal: by}, epics)
					if err != nil {
						t.Fatal(err)
					}
					h.gate.mu.Lock()
					heldSession := h.gate.session
					h.gate.mu.Unlock()
					heldSessionActive := false
					if heldSession != "" {
						_, err := h.sessions.SessionContext(context.Background(), collab.Caller{Principal: by}, heldSession)
						heldSessionActive = err == nil
						var domain *model.Error
						if err != nil && (!errors.As(err, &domain) || domain.Data.Code != "not_found") {
							t.Fatal(err)
						}
					}
					return map[string]any{"active": h.active.Load(), "cancelled": h.gate.cancelled.Load(), "presence": presence, "heldSession": heldSession, "heldSessionActive": heldSessionActive}
				case "streams":
					h.mu.Lock()
					rows := append([]streamRecord{}, h.streams...)
					h.mu.Unlock()
					data, err := json.Marshal(rows)
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("EP6_NATIVE_STREAM_EVIDENCE %s", base64.StdEncoding.EncodeToString(data))
					return map[string]any{"streams": rows}
				default:
					t.Fatalf("unexpected background control %q", action)
				}
				return map[string]any{"ok": true}
			}
			runBrowser(t, map[string]any{"base": base(f), "token": key.Token, "principal": by.ID, "otherToken": other.Token, "otherPrincipal": "user:background-other", "parent": parent.ID, "child": child, "secret": secret, "background": scenario}, control, func() []hostTrace { return hostTraces(f) })
		})
	}
}

func base(f *fixture) string { return strings.TrimSuffix(f.api.url, "/rpc/v1") }

type hostTrace struct {
	URL    string      `json:"url"`
	Native nativeTrace `json:"native"`
}

func hostTraces(fixtures ...*fixture) []hostTrace {
	var out []hostTrace
	for _, f := range fixtures {
		for _, row := range f.trace.snapshot() {
			out = append(out, hostTrace{URL: f.api.url, Native: row})
		}
	}
	return out
}

// This is a browser/agent fixture, not a promotion of the whole E3 card. All
// source writes cross API-key admission and the unchanged fixed-policy Writer.
func TestAuthenticatedChromeFifthTaskTwentyThousandNativeRestoreReopen(t *testing.T) {
	qualifyBrowserLifecycle(t, 20000)
}

// Supplemental UI/native orchestration proof. The unmodified 20k test above
// remains mandatory; this bounded case can never qualify its workload.
func TestAuthenticatedChromeBoundedLifecycleNativeRestoreReopen(t *testing.T) {
	qualifyBrowserLifecycle(t, 52)
}

func qualifyBrowserLifecycle(t *testing.T, population int) {
	t.Helper()
	dir := t.TempDir()
	f := newFixture(t, "sqlite", filepath.Join(dir, "source.db"), true)
	source := f
	parent := f.create(t, epics, "epic parent content canary", nil)
	direct := make([]model.Ref, 0, 50)
	issued := map[string]bool{}
	for i := 0; i < 50; i++ {
		by := agent("owner")
		by.ID = fmt.Sprintf("agent:ep6-%02d", i)
		key := f.issue(t, by)
		if issued[key.ID] {
			t.Fatal("distinct agents received the same credential identity")
		}
		issued[key.ID] = true
		project := visible
		if i >= 30 {
			project = hidden
		}
		op := model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: project, Fields: map[string]any{"title": fmt.Sprintf("agent child %02d", i)}, Parent: &parent}}
		direct = append(direct, committed(t, f.write(t, key.Token, "tx.apply", project, map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{op}}})).Results[0].Ref)
	}
	// Keep the original 51..20000 schedule, every complete 100-op request and
	// the final 49-op request. One unrelated record is instead a grandchild.
	var grandchild model.Ref
	for start := 51; start < population; start += 100 {
		end := start + 100
		if end > population {
			end = population
		}
		ops := make([]model.Op, 0, end-start)
		for i := start; i < end; i++ {
			create := &model.CreateOp{Type: "issue", Project: visible, Fields: map[string]any{"title": fmt.Sprintf("unrelated %05d", i)}}
			if i == 51 {
				create.Parent = &direct[1]
			}
			ops = append(ops, model.Op{Kind: model.OpCreate, Create: create})
		}
		commit := committed(t, f.write(t, f.owner, "tx.apply", visible, map[string]any{"changeSet": model.ChangeSet{Ops: ops}}))
		if start == 51 {
			grandchild = commit.Results[0].Ref
		}
	}
	images := persistedImages(t, f.store)
	if len(images) != population+1 {
		t.Fatalf("durable census=%d, want exactly %d beads plus protected ontology", len(images), population)
	}
	unique := map[string]bool{}
	for _, id := range direct {
		if unique[id.ID] || images[id.ID] == nil {
			t.Fatal("direct child is missing or duplicated")
		}
		unique[id.ID] = true
	}
	if images[parent.ID] == nil || images[grandchild.ID] == nil {
		t.Fatal("parent/grandchild missing from durable corpus")
	}
	f.observed.mu.Lock()
	if len(f.observed.agents) != 50 || f.observed.fullBatches != (population-51)/100 {
		t.Fatalf("actual admitted Writer agents=%d full100op=%d", len(f.observed.agents), f.observed.fullBatches)
	}
	for id, sponsor := range f.observed.agents {
		if sponsor != "user:owner" || !strings.HasPrefix(id, "agent:ep6-") {
			t.Fatal("wrong authenticated agent/sponsor")
		}
	}
	f.observed.mu.Unlock()
	if population == 20000 {
		t.Log("EP6_BROWSER_WORKLOAD PASS durable_beads=20000 distinct_authenticated_agents=50 distinct_credentials=50 direct_children=50 full100op_commits=199 final_ops=49; default authn/rule/authorizer/Writer/HTTP budgets unchanged")
	} else {
		t.Log("EP6_BROWSER_BOUNDED_ONLY durable_beads=52 distinct_authenticated_agents=50 direct_children=50; supplemental lifecycle orchestration, never20k qualification")
	}
	f.status(t, direct[0], "done", visible)
	viewer := readerKey(t, f, "user:browser-reader", epics, visible)
	narrow := readerKey(t, f, "user:browser-child-only", visible)
	restricted := readerKey(t, f, "user:browser-reader", epics)
	denied := f.issue(t, agent("owner"))
	var reopened *fixture
	var restoreIncarnation string
	input := map[string]any{"lifecycle": true, "population": population, "base": base(f), "token": viewer.Token, "principal": "user:browser-reader", "narrowToken": narrow.Token, "restrictedToken": restricted.Token, "deniedToken": denied.Token, "parent": parent.ID, "child": direct[0], "secret": direct[30], "hiddenProject": hidden, "parentTitle": "epic parent content canary", "visible": 30, "hidden": 20}
	control := func(action string) map[string]any {
		switch action {
		case "hidden-status":
			f.status(t, direct[30], "failed", hidden)
			return map[string]any{"ok": true}
		case "revoke":
			if !f.keys.Revoke(viewer.ID) {
				t.Fatal("real credential revocation failed")
			}
			return map[string]any{"ok": true}
		case "restore":
			beforeImages := persistedImages(t, f.store)
			beforeClosure := persistedClosure(t, f.store)
			if len(beforeImages) != population+1 || len(beforeClosure) != population+52 {
				t.Fatalf("pre-restore census images=%d closure=%d", len(beforeImages), len(beforeClosure))
			}
			sourceAdmin := admin(t, f.store, f.lease)
			data, archive := dumpHead(t, sourceAdmin)
			sourceAdmin.close(t)
			f.close(t)
			path := filepath.Join(dir, "restored.db")
			target, lease, closeTarget := openRenewingTarget(t, "sqlite", path)
			// Empty target receives no fixture seed, facts, ontology or graph repair.
			targetAdmin := admin(t, target, lease)
			restoreIncarnation = restoreHead(t, targetAdmin, data, archive)
			if !reflect.DeepEqual(persistedImages(t, target), beforeImages) || !reflect.DeepEqual(persistedClosure(t, target), beforeClosure) {
				t.Fatal("native restore changed source images or canonical closure")
			}
			targetAdmin.close(t)
			closeTarget()
			reopened = newFixtureMode(t, "sqlite", path, true, false)
			f = reopened
			if !reflect.DeepEqual(persistedImages(t, f.store), beforeImages) || !reflect.DeepEqual(persistedClosure(t, f.store), beforeClosure) {
				t.Fatal("nonseeding physical reopen changed restored state")
			}
			key := readerKey(t, f, "user:browser-reader", epics, visible)
			t.Logf("EP6_BROWSER_NATIVE_RESTORE PASS actual admin.dump/admin.restore.begin/chunk/finish; head history; bytes=%d images=%d canonical_closure=%d; physical SQLite close/nonseed reopen; incarnation=%s", len(data), population+1, population+52, restoreIncarnation)
			return map[string]any{"base": base(f), "token": key.Token, "incarnation": restoreIncarnation}
		case "detach":
			head := f.head(t)
			cycle := model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: parent, Parent: &grandchild}}}}
			requireCode(t, f.write(t, f.owner, "tx.apply", epics, map[string]any{"changeSet": cycle}), "ontology_guard_failed")
			if f.head(t) != head {
				t.Fatal("restored cycle refusal changed source head")
			}
			detach := model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: direct[0]}}}}
			committed(t, f.write(t, f.owner, "tx.apply", visible, map[string]any{"changeSet": detach}))
			if len(persistedClosure(t, f.store)) != population+51 {
				t.Fatal("restored detach did not maintain descendant closure")
			}
			return map[string]any{"ok": true}
		default:
			t.Fatalf("unexpected browser control %q", action)
			return nil
		}
	}
	runBrowser(t, input, control, func() []hostTrace {
		if reopened != nil {
			return hostTraces(source, reopened)
		}
		return hostTraces(source)
	})
	if reopened == nil || restoreIncarnation == "" {
		t.Fatal("browser did not execute native restore/nonseed reopen")
	}
}

func (f *fixture) status(t *testing.T, id model.Ref, status, project string) {
	t.Helper()
	method := map[string]string{"done": "bead.close", "failed": "bead.fail"}[status]
	before := f.head(t)
	requireCode(t, f.write(t, f.owner, method, project, map[string]any{"id": id}), "forbidden")
	if f.head(t) != before {
		t.Fatal("fixed-policy lifecycle denial changed source")
	}
	if f.lifecycle == nil {
		f.lifecycle = lifecycleFixture(t, f.store, f.lease)
	}
	f.sequence++
	committed(t, f.lifecycle.call(t, "", method, map[string]any{"id": id, "_meta": map[string]any{"project": project, "idempotencyKey": fmt.Sprintf("%032x", f.sequence), "return": "full"}}))
}

func runBrowser(t *testing.T, input map[string]any, control func(string) map[string]any, traces func() []hostTrace) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, script)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := false
	defer func() {
		if !finished {
			cancel()
			_ = cmd.Wait()
		}
	}()
	encoder := json.NewEncoder(in)
	// Credentials travel only over stdin, never argv, files, logs or artifacts.
	if err := encoder.Encode(input); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(out)
	scanner.Buffer(make([]byte, 65536), 16<<20)
	var evidence []byte
	for scanner.Scan() {
		var message struct {
			Event  string `json:"event"`
			Action string `json:"action"`
			Data   string `json:"data"`
			Text   string `json:"text"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			t.Fatal("browser emitted invalid evidence protocol")
		}
		switch message.Event {
		case "control":
			if control == nil {
				t.Fatal("small sign-in check requested lifecycle control")
			}
			if err := encoder.Encode(control(message.Action)); err != nil {
				t.Fatal(err)
			}
		case "evidence":
			evidence, err = base64.StdEncoding.DecodeString(message.Data)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("EP6_BROWSER_EVIDENCE %s", message.Data)
		case "screenshot":
			t.Logf("EP6_BROWSER_SCREENSHOT %s", message.Data)
		case "step":
			t.Logf("Chrome STEP/PASS %s", message.Text)
		default:
			t.Fatal("unexpected browser evidence message")
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	in.Close()
	err = cmd.Wait()
	finished = true
	if stderr.Len() > 0 {
		t.Log(stderr.String())
	}
	if err != nil {
		data, marshalErr := json.Marshal(traces())
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		t.Logf("EP6_NATIVE_FAILED_EVIDENCE %s", base64.StdEncoding.EncodeToString(data))
		t.Fatalf("authenticated Chrome assertions failed: %v", err)
	}
	if len(evidence) == 0 {
		t.Fatal("browser omitted actual wire/console/steps evidence")
	}
	correlateBrowser(t, evidence, traces())
}

func correlateBrowser(t *testing.T, evidence []byte, rows []hostTrace) {
	t.Helper()
	var browser struct {
		Wire []struct {
			URL     string `json:"url"`
			Request struct {
				ID     string          `json:"id"`
				Method string          `json:"method"`
				Params json.RawMessage `json:"params"`
			} `json:"request"`
			RawResponse string `json:"rawResponse"`
			Status      int    `json:"status"`
			Principal   string `json:"principal"`
			Cancelled   bool   `json:"cancelled"`
		} `json:"wire"`
		Console    []any `json:"console"`
		Exceptions []any `json:"exceptions"`
	}
	if err := json.Unmarshal(evidence, &browser); err != nil {
		t.Fatal(err)
	}
	if len(browser.Wire) == 0 || len(browser.Exceptions) > 0 {
		t.Fatal("browser evidence is empty or has uncaught exceptions")
	}
	canonical := func(raw json.RawMessage) string {
		var v any
		if json.Unmarshal(raw, &v) != nil {
			t.Fatal("invalid correlated JSON")
		}
		b, _ := json.Marshal(v)
		return string(b)
	}
	key := func(url, id, method string, params json.RawMessage, response string) string {
		return url + "|" + id + "|" + method + "|" + canonical(params) + "|" + response
	}
	native := map[string][]nativeTrace{}
	for _, row := range rows {
		r := row.Native
		k := key(row.URL, r.ID, r.Method, r.Params, r.ResponseSHA256)
		native[k] = append(native[k], r)
	}
	cancelled := 0
	for _, wire := range browser.Wire {
		sum := sha256.Sum256([]byte(wire.RawResponse))
		digest := hex.EncodeToString(sum[:])
		k := key(wire.URL, wire.Request.ID, wire.Request.Method, wire.Request.Params, digest)
		if wire.Cancelled {
			cancelled++
			// Chrome has no response body after abort. Bind the exact request to
			// the actual native cancellation record, without claiming byte parity.
			for candidate, records := range native {
				if len(records) > 0 && records[0].Cancelled && candidate == key(wire.URL, wire.Request.ID, wire.Request.Method, wire.Request.Params, records[0].ResponseSHA256) {
					k = candidate
					break
				}
			}
		}
		found := native[k]
		if len(found) == 0 {
			t.Fatalf("Chrome request has no exact native correlation: id=%s method=%s", wire.Request.ID, wire.Request.Method)
		}
		row := found[0]
		native[k] = found[1:]
		if !wire.Cancelled && (row.Status != wire.Status || row.Principal != wire.Principal) {
			t.Fatalf("native status/principal differs for Chrome id=%s: status=%d principal=%s", row.ID, row.Status, row.Principal)
		}
	}
	if len(browser.Wire) != len(rows) {
		t.Fatalf("unpaired native/browser trace count: browser=%d native=%d", len(browser.Wire), len(rows))
	}
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EP6_NATIVE_CORRELATED_EVIDENCE %s", base64.StdEncoding.EncodeToString(data))
	t.Logf("EP6_BROWSER_CORRELATION PASS actual Chrome requests=%d actual native traces=%d; exact URL/id/method/params; completed responses match SHA256/status/provider principal; cancelled=%d match native context cancellation without response-byte claims; browser console entries=%d uncaught=0", len(browser.Wire), len(rows), cancelled, len(browser.Console))
}

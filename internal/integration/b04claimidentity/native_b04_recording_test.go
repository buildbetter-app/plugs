package b04claimidentity_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type b04NativeCapture struct {
	RunID        string            `json:"runID"`
	Database     string            `json:"database"`
	SourceIDs    map[string]string `json:"sourceIDs"`
	ReadyBefore  []string          `json:"readyBefore"`
	Direct       string            `json:"direct"`
	SameOwner    string            `json:"sameOwner"`
	ForeignExit  int               `json:"foreignExit"`
	ClaimNextIDs []string          `json:"claimNextIDs"`
	EmptyJSON    json.RawMessage   `json:"emptyJSON"`
	Inventory    map[string]any    `json:"inventory"`
	DumpBytes    int64             `json:"dumpBytes"`
	Commands     int               `json:"commands"`
	Output       string            `json:"output"`
}

// This is the only B04 manual target. Ordinary focused/whole tests never
// invoke a native executable or consume this capture; they may only validate
// the embedded source contract.
func TestB04NativeClaimAndReplay(t *testing.T) {
	inputs := loadB04NativeInputs(t)
	retained := b04NativeEnv(t, "TEST_UNDECLARED_OUTPUTS_DIR")
	root, err := os.MkdirTemp(retained, "b04-native-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		inventory, invErr := b04NativeInventory(root)
		b04NativeJSON(t, filepath.Join(root, "output-inventory.json"), map[string]any{
			"entries": inventory, "complete": invErr == nil, "passed": !t.Failed(),
			"exclusions":    []string{"poll.log excluded by basename before stat/read/hash", "output-inventory.json self"},
			"qualification": "bounded B04 native/public replay only; no B05/B22/full-S1 qualification",
		})
		if invErr != nil {
			t.Errorf("native output inventory: %v", invErr)
		}
		t.Logf("B04_NATIVE_OUTPUT root=%s", root)
	})
	b04NativeWrite(t, filepath.Join(root, "native_b04_inputs.json"), b04NativeInputsBytes)
	binary := b04NativePrepared(t, root, inputs)

	captures := make([]b04NativeCapture, 0, len(inputs.Vector.Runs))
	for _, run := range inputs.Vector.Runs {
		run := run
		t.Run(run.ID, func(t *testing.T) {
			capture := recordB04NativeStore(t, binary, root, inputs, run.ID, run.Database)
			captures = append(captures, capture)
		})
	}
	if len(captures) != 2 {
		t.Fatalf("native twin capture count=%d want=2", len(captures))
	}
	// Compare only the semantic disposition. Raw commands, timestamps, server
	// ports and database bytes remain separate immutable receipts.
	semantic := func(c b04NativeCapture) any {
		return struct {
			IDs         map[string]string
			Ready       []string
			Direct      string
			SameOwner   string
			ForeignExit int
			Next        []string
			Empty       string
		}{c.SourceIDs, c.ReadyBefore, c.Direct, c.SameOwner, c.ForeignExit, c.ClaimNextIDs, string(c.EmptyJSON)}
	}
	if !reflect.DeepEqual(semantic(captures[0]), semantic(captures[1])) {
		t.Fatalf("native semantic twins differ: A=%+v B=%+v", semantic(captures[0]), semantic(captures[1]))
	}
	b04NativeJSON(t, filepath.Join(root, "native-twin-comparison.json"), map[string]any{
		"equal": true, "runA": captures[0], "runB": captures[1],
		"projection": "source IDs, ready order, direct/same-owner result identities, foreign refusal exit, claimNext winners and empty JSON; raw command/database bytes remain separate",
	})

	// One actual native source is replayed against each real Plug backend. The
	// binding is explicit and IDs are intentionally distinct: native Beads IDs
	// are evidence inputs, never Plug identity or fixture-derived expectations.
	for _, backend := range []string{"memory", "sqlite_wal", "postgres"} {
		backend := backend
		t.Run("replay/"+backend, func(t *testing.T) {
			eachStore(t, false, func(t *testing.T, e engine) {
				if e.name != backend {
					return
				}
				replayB04Native(t, captures[0], e, filepath.Join(root, "replay-"+backend), backend == "memory")
			})
		})
	}

}

func recordB04NativeStore(t *testing.T, binary, root string, inputs b04NativeInputs, runID, database string) b04NativeCapture {
	t.Helper()
	output := filepath.Join(root, runID)
	if err := os.MkdirAll(output, 0o700); err != nil {
		t.Fatal(err)
	}
	server := publicnative.StartDolt(t, output)
	workspace := t.TempDir()
	home := filepath.Join(workspace, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	env := server.ChildEnv([]string{
		"PATH=" + filepath.Dir(server.DoltBinary) + ":/usr/bin:/bin",
		"HOME=" + home, "USERPROFILE=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"),
		"USER=public-native-b04", "BEADS_ACTOR=s1-b04-native-owner", "BEADS_NO_DAEMON=1",
		"TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1",
	})
	c := &b04NativeCommand{t: t, binary: binary, workspace: workspace, output: output, env: env}
	var version struct {
		Commit string `json:"commit"`
		Build  string `json:"build"`
	}
	if err := b04NativeDecode(c.run("version", "version", "--json"), &version); err != nil {
		t.Fatalf("native version JSON: %v", err)
	}
	if version.Commit != inputs.Source || version.Build != "public-"+inputs.Source {
		t.Fatalf("native version commit=%s build=%s want source=%s", version.Commit, version.Build, inputs.Source)
	}
	c.run("init-server", "init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", "b04", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive")
	c.run("config-custom", "config", "set", "status.custom", "awaiting_review:active")
	ids := inputs.Vector.SourceIDs
	create := func(label, id, title, status string, priority int) {
		args := []string{"create", "--id", id, "--title", title, "--type", "task", "--force", "--priority", strconv.Itoa(priority), "--status", status, "--json"}
		c.run(label, args...)
	}
	create("create-direct", ids["direct"], "B04 direct", "open", 0)
	create("create-custom-active", ids["customActive"], "B04 custom active", "awaiting_review", 1)
	create("create-foreign-held", ids["foreignHeld"], "B04 foreign holder", "open", 2)
	create("create-next", ids["next"], "B04 next", "open", 3)
	create("create-empty", ids["empty"], "B04 empty front", "open", 4)
	readyBefore := c.run("ready-before", "ready", "--json", "--sort", "hybrid", "--limit", "0")
	readyIDs := b04NativeIDs(t, readyBefore)
	c.run("claim-direct", "update", ids["direct"], "--claim", "--json")
	c.run("claim-direct-same-owner", "update", ids["direct"], "--claim", "--json")
	c.run("claim-foreign-held", "update", ids["foreignHeld"], "--claim", "--json")
	foreignEnv := append([]string(nil), c.env...)
	foreignEnv = append(foreignEnv, "BEADS_ACTOR=s1-b04-foreign")
	foreign := &b04NativeCommand{t: t, binary: c.binary, workspace: c.workspace, output: filepath.Join(c.output, "foreign"), env: foreignEnv}
	if err := os.MkdirAll(foreign.output, 0o700); err != nil {
		t.Fatal(err)
	}
	foreignResult := foreign.expect("foreign-claim-refusal", 1, "update", ids["foreignHeld"], "--claim", "--json")
	if len(bytes.TrimSpace(foreignResult.Stderr)) == 0 && len(bytes.TrimSpace(foreignResult.Stdout)) == 0 {
		t.Fatal("foreign refusal had no preserved diagnostic stream")
	}
	claimNextIDs := []string{}
	for i := 0; i < 2; i++ {
		result := c.run(fmt.Sprintf("claim-next-%d", i+1), "ready", "--claim", "--json", "--limit", "1")
		if id := b04NativeFirstID(t, result); id != "" {
			claimNextIDs = append(claimNextIDs, id)
		} else {
			t.Fatalf("claimNext %d had no hydrated winner: %s", i+1, result)
		}
	}
	empty := c.run("claim-next-empty", "ready", "--claim", "--json", "--limit", "1")
	// The fourth call is intentionally retained as an explicit terminal. Some
	// producer versions return JSON null and others return an empty array; both
	// are accepted only as a successful nil front, never as a fabricated row.
	var emptyValue any
	if err := b04NativeDecode(empty, &emptyValue); err != nil {
		t.Fatalf("claimNext empty response is not closed JSON: %v", err)
	}
	if !b04NativeEmptyResult(emptyValue) {
		t.Fatalf("claimNext empty response is not nil/empty: %s", empty)
	}
	c.run("quiesce-commit", "dolt", "commit", "-m", "B04 native claim capture "+runID)
	databaseDir := server.RequireDatabaseDir(t, database)
	info, err := os.Lstat(databaseDir)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("native database identity is not a real directory: %v", err)
	}
	b04NativeJSON(t, filepath.Join(output, "physical-database.json"), map[string]any{
		"path": databaseDir, "mode": info.Mode().String(), "pid": server.PID, "port": server.Port,
		"dataRoot": server.DataRoot, "childEnv": env,
	})
	inventoryPath := filepath.Join(output, "inventory.json")
	c.run("public-inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventoryPath)
	if _, err := os.Stat(inventoryPath + ".sources"); err != nil {
		t.Fatalf("native inventory sidecar tree missing: %v", err)
	}
	sidecarBefore, err := b04NativeInventory(inventoryPath + ".sources")
	if err != nil {
		t.Fatal(err)
	}
	dumpPath := filepath.Join(output, "readback.tar")
	c.run("public-dump", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "-o", dumpPath)
	sidecarAfter, err := b04NativeInventory(inventoryPath + ".sources")
	if err != nil || !reflect.DeepEqual(sidecarBefore, sidecarAfter) {
		t.Fatalf("native dump changed source sidecars: err=%v", err)
	}
	_, dumpBytes, err := b04NativeFileSHA(dumpPath)
	if err != nil || dumpBytes == 0 {
		t.Fatalf("native dump is absent/empty: bytes=%d err=%v", dumpBytes, err)
	}
	projection := b04NativeCapture{
		RunID: runID, Database: database, SourceIDs: ids, ReadyBefore: readyIDs,
		Direct: ids["direct"], SameOwner: ids["direct"], ForeignExit: foreignResult.Exit,
		ClaimNextIDs: claimNextIDs, EmptyJSON: append(json.RawMessage(nil), bytes.TrimSpace(empty)...),
		Inventory: map[string]any{"path": inventoryPath, "sidecars": sidecarBefore, "database": databaseDir},
		DumpBytes: dumpBytes, Commands: c.count + foreign.count, Output: output,
	}
	b04NativeJSON(t, filepath.Join(output, "observed-projection.json"), projection)
	b04NativeJSON(t, filepath.Join(output, "source-to-actual-binding.json"), map[string]any{
		"nativeSourceIDs": ids, "plugBinding": map[string]string{
			"direct": "fixture:a1", "customActive": "fixture:a2", "foreignHeld": "fixture:b1", "next": "fixture:a2", "empty": "fixture:none",
		},
		"identityRule": "native source IDs and Plug refs are distinct evidence identities; replay binds actual returned Plug refs",
	})
	return projection
}

func b04NativeIDs(t *testing.T, data []byte) []string {
	t.Helper()
	var value any
	if err := b04NativeDecode(data, &value); err != nil {
		t.Fatalf("native ready response JSON: %v", err)
	}
	return b04NativeIDsValue(t, value)
}

func b04NativeIDsValue(t *testing.T, value any) []string {
	t.Helper()
	var rows []any
	switch v := value.(type) {
	case []any:
		rows = v
	case map[string]any:
		if raw, ok := v["issues"]; ok {
			rows, _ = raw.([]any)
		} else if raw, ok := v["items"]; ok {
			rows, _ = raw.([]any)
		}
	}
	ids := make([]string, 0, len(rows))
	for _, raw := range rows {
		if row, ok := raw.(map[string]any); ok {
			if id, ok := row["id"].(string); ok && id != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

func b04NativeFirstID(t *testing.T, data []byte) string {
	ids := b04NativeIDs(t, data)
	if len(ids) > 0 {
		return ids[0]
	}
	var value any
	if err := b04NativeDecode(data, &value); err != nil {
		t.Fatal(err)
	}
	if object, ok := value.(map[string]any); ok {
		for _, key := range []string{"id", "issueID", "issue_id"} {
			if id, ok := object[key].(string); ok {
				return id
			}
		}
	}
	return ""
}

func b04NativeEmptyResult(value any) bool {
	if value == nil {
		return true
	}
	switch v := value.(type) {
	case []any:
		return len(v) == 0
	case map[string]any:
		for _, key := range []string{"issues", "items", "result"} {
			if raw, ok := v[key]; ok {
				return b04NativeEmptyResult(raw)
			}
		}
	}
	return false
}

func replayB04Native(t *testing.T, capture b04NativeCapture, e engine, output string, contention bool) {
	t.Helper()
	if err := os.MkdirAll(output, 0o700); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, e)
	owner := f.issue("agent:b04-native-owner", "user:pool-a", nil)
	foreign := f.issue("agent:b04-native-foreign", "user:pool-a", nil)
	bindings := map[string]string{"direct": f.work["a1"].ID, "customActive": f.work["a2"].ID, "foreignHeld": f.work["b1"].ID, "next": f.work["a2"].ID, "empty": ""}
	b04NativeJSON(t, filepath.Join(output, "source-to-plug-refs.json"), map[string]any{"native": capture.SourceIDs, "plug": bindings})
	if capture.SourceIDs["direct"] == bindings["direct"] {
		t.Fatal("native source identity was incorrectly reused as Plug identity")
	}
	keyDirect := f.key("native-direct")
	before := f.state()
	first := f.call(owner, "bead.claim", poolA, keyDirect, "b04-native-owner", target(f.work["a1"]))
	claimed := f.winner(before, owner, "bead.claim", keyDirect, "b04-native-owner", f.work["a1"], first, true)
	repeatKey := f.key("native-direct-same-owner")
	noOpScope := f.remember(owner, "bead.claim", repeatKey, "different-display")
	beforeNoOp := f.state()
	repeat := f.call(owner, "bead.claim", poolA, repeatKey, "different-display", target(f.work["a1"]))
	repeatCommit := repeat.commit(t)
	sameClaimState(t, claimed, repeatCommit)
	onlyNoopReceipt(t, beforeNoOp, f.state(), noOpScope, repeatCommit)
	refusalBefore := f.state()
	f.refuse(foreign, "bead.claim", poolA, f.key("native-foreign"), "b04-native-owner", target(f.work["a1"]), "busy")
	sameState(t, refusalBefore, f.state())

	// Claim-next must walk the real ready set and then commit a durable nil
	// front. The two fixture pool-A rows are the actual Plug refs bound above.
	nextKey := f.key("native-claim-next")
	next := f.call(owner, "bead.claimNext", poolA, nextKey, "b04-native-owner", nil)
	nextCommit := next.commit(t)
	if len(nextCommit.Results) != 1 || nextCommit.Results[0].Ref.ID != f.work["a2"].ID {
		t.Fatalf("claimNext selected=%+v want actual fixture a2", nextCommit.Results)
	}
	emptyKey := f.key("native-claim-next-empty")
	emptyBefore := f.state()
	emptyReply := f.call(owner, "bead.claimNext", poolA, emptyKey, "b04-native-owner", nil)
	if string(emptyReply.envelope["result"]) != "null" {
		t.Fatalf("claimNext empty result=%s want JSON null", emptyReply.raw)
	}
	emptyAfter := f.state()
	if len(emptyAfter.Records) != len(emptyBefore.Records) || len(emptyAfter.Derived) != len(emptyBefore.Derived) || len(emptyAfter.Changesets) != len(emptyBefore.Changesets)+1 {
		t.Fatalf("empty claimNext changed durable graph beyond its terminal receipt")
	}
	if e.name != "memory" {
		f.reopen()
		replayed := f.call(owner, "bead.claimNext", poolA, emptyKey, "after-reopen", nil)
		var meta struct {
			Replayed bool `json:"replayed"`
		}
		if err := json.Unmarshal(replayed.envelope["_meta"], &meta); err != nil || !meta.Replayed || !bytes.Equal(replayed.envelope["result"], emptyReply.envelope["result"]) {
			t.Fatalf("empty claimNext replay changed result bytes: %s", replayed.raw)
		}
	}
	if contention {
		b04RunContention(t, f)
	}
	b04NativeJSON(t, filepath.Join(output, "replay-disposition.json"), map[string]any{
		"backend": e.name, "direct": f.work["a1"].ID, "claimNext": f.work["a2"].ID,
		"empty": "null", "contention": contention,
		"nativeSourceIDs": capture.SourceIDs,
		"qualification":   "actual bounded replay and conservation; no FullS1/native admission promotion",
	})
}

func b04RunContention(t *testing.T, f *fixture) {
	t.Helper()
	const agents = 200
	credentials := make([]credential, agents)
	for i := range credentials {
		credentials[i] = f.issue(fmt.Sprintf("agent:b04-contender-%03d", i), "user:pool-b", nil)
	}
	targetID := f.work["b1"]
	type outcome struct {
		code   string
		result bool
	}
	outcomes := make(chan outcome, agents)
	var wg sync.WaitGroup
	for i, by := range credentials {
		wg.Add(1)
		go func(i int, by credential) {
			defer wg.Done()
			params := map[string]any{"id": targetID, "_meta": map[string]any{"project": poolB, "agent": fmt.Sprintf("contender-%03d", i), "idempotencyKey": fmt.Sprintf("b04-contention-%03d", i)}}
			raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": i + 1, "method": "bead.claim", "params": params})
			if err != nil {
				outcomes <- outcome{code: "encode"}
				return
			}
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/rpc/v1", bytes.NewReader(raw)).WithContext(f.ctx)
			req.Header.Set("Plug-RPC", "1")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+by.key.Token)
			rec := httptest.NewRecorder()
			f.app.Handler.ServeHTTP(rec, req)
			var envelope map[string]json.RawMessage
			if json.Unmarshal(rec.Body.Bytes(), &envelope) != nil {
				outcomes <- outcome{code: "invalid-json"}
				return
			}
			if rawErr := envelope["error"]; len(rawErr) != 0 {
				var wireErr struct {
					Data struct {
						Code string `json:"code"`
					} `json:"data"`
				}
				if json.Unmarshal(rawErr, &wireErr) == nil {
					outcomes <- outcome{code: wireErr.Data.Code}
					return
				}
			}
			outcomes <- outcome{result: len(envelope["result"]) != 0 && string(envelope["result"]) != "null"}
		}(i, by)
	}
	wg.Wait()
	close(outcomes)
	winners, busy := 0, 0
	for got := range outcomes {
		if got.result {
			winners++
		}
		if got.code == "busy" {
			busy++
		}
	}
	if winners != 1 || busy != agents-1 {
		t.Fatalf("200-agent claim contention winners=%d busy=%d want 1/%d", winners, busy, agents-1)
	}
	t.Logf("B04 native replay contention: agents=%d winner=%d busy=%d", agents, winners, busy)
}

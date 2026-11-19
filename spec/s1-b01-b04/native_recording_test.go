package s1b01b04

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type nativeCapture struct {
	Vector      nativeVector
	Run         nativeRun
	Before      nativePhase
	After       nativePhase
	Transitions map[string]nativePhase
	Membership  map[string]nativeMembershipPhase
	Exclusions  map[string]nativeExclusionPhase
	Output      string
}

// This manual target is a physical recorder, never part of ordinary //...
// gates. Launch requires the separately delivered, reviewed prepared input.
// Ordinary compilation cannot establish a native recording or admission.
func TestB01NativeTimedPair(t *testing.T) {
	recordNativePair(t, "timed", 0)
}

func TestB01NativeIndefinitePair(t *testing.T) {
	recordNativePair(t, "indefinite", 1)
}

func recordNativePair(t *testing.T, mode string, shardIndex int) {
	var inputs nativeInputs
	nativeDecode(t, nativeInputsBytes, &inputs)
	if t.Name() != nativeVectorTestName(nativeVector{Mode: mode}) ||
		nativeEnv(t, "TEST_TOTAL_SHARDS") != "2" || nativeEnv(t, "TEST_SHARD_INDEX") != strconv.Itoa(shardIndex) {
		t.Fatal("native vector pair requires its distinct top-level case in the assigned two-shard runner")
	}
	if _, err := os.Stat(nativeEnv(t, "TEST_SHARD_STATUS_FILE")); err != nil {
		t.Fatalf("native runner did not acknowledge actual test sharding: %v", err)
	}
	retained := nativeEnv(t, "TEST_UNDECLARED_OUTPUTS_DIR")
	root, err := os.MkdirTemp(retained, "b01-native-pair-")
	if err != nil {
		t.Fatal(err)
	}
	// Registered first, so all owned helper cleanup receipts exist before hashes.
	t.Cleanup(func() {
		inventory, err := nativeInventory(root)
		if err != nil {
			t.Errorf("complete output inventory: %v", err)
		}
		nativeJSON(t, filepath.Join(root, "output-inventory.json"), map[string]any{
			"entries": inventory, "complete": err == nil, "passed": !t.Failed(),
			"exclusions":    []string{"poll.log excluded by name before metadata/read/hash", "output-inventory.json self", "SHA256SUMS self"},
			"qualification": "actual bounded native vectors and replay only; no expiry/full-B01/S1/B05 qualification",
		})
		var sums strings.Builder
		for _, entry := range inventory {
			if entry["type"] == "file" {
				fmt.Fprintf(&sums, "%s  %s\n", entry["sha256"], entry["path"])
			}
		}
		digest, _, hashErr := nativeDigestFile(filepath.Join(root, "output-inventory.json"))
		if hashErr != nil {
			t.Errorf("output-inventory digest: %v", hashErr)
		} else {
			fmt.Fprintf(&sums, "%s  output-inventory.json\n", digest)
		}
		nativeWrite(t, filepath.Join(root, "SHA256SUMS"), []byte(sums.String()))
		archive, err := nativePackEvidence(root, inventory)
		if err != nil {
			t.Errorf("retain complete native evidence archive: %v", err)
		}
		nativeJSON(t, root+"-archive.json", archive)
		t.Logf("B01_NATIVE_OUTPUT root=%s inventory=output-inventory.json archive=%s verified=%t", root, archive.Path, archive.Verified)
	})
	nativeWrite(t, filepath.Join(root, "native-inputs.json"), nativeInputsBytes)
	binary := nativePrepared(t, root, inputs)
	if len(inputs.Vectors) != 2 {
		t.Fatal("both distinct timed and indefinite input vectors are required")
	}
	replayManifest := nativeRequiredReplayManifest()
	allReplayPaths := nativeReplayManifestPaths(inputs)
	expectedRoots := 0
	for _, vector := range inputs.Vectors {
		if len(vector.Runs) != 2 {
			t.Fatalf("%s lacks two fresh stores for the finite replay matrix", vector.ID)
		}
		expectedRoots += len(vector.Runs)
	}
	pathsPerRoot := 1 + len(replayManifest.HierarchyPhases) + len(replayManifest.MembershipPhases)
	if expectedRoots != 4 || len(replayManifest.Backends) != 3 || len(allReplayPaths) != expectedRoots*len(replayManifest.Backends)*pathsPerRoot {
		t.Fatalf("native replay manifest matrix has roots=%d backends=%d paths=%d; want 4 roots, 3 backends, %d paths", expectedRoots, len(replayManifest.Backends), len(allReplayPaths), expectedRoots*len(replayManifest.Backends)*pathsPerRoot)
	}
	seenReplayPaths := map[string]bool{}
	for _, path := range allReplayPaths {
		if seenReplayPaths[path] {
			t.Fatalf("native replay manifest contains duplicate named path %s", path)
		}
		seenReplayPaths[path] = true
	}
	selectedInputs := inputs
	selectedInputs.Vectors = nil
	for _, vector := range inputs.Vectors {
		if vector.Mode == mode {
			selectedInputs.Vectors = append(selectedInputs.Vectors, vector)
		}
	}
	if len(selectedInputs.Vectors) != 1 || len(selectedInputs.Vectors[0].Runs) != 2 {
		t.Fatal("native shard must own exactly one complete vector and both distinct twins")
	}
	replayPaths := nativeReplayManifestPaths(selectedInputs)
	if len(replayPaths)*2 != len(allReplayPaths) {
		t.Fatal("native shard manifest does not partition the full two-vector matrix equally")
	}
	nativeJSON(t, filepath.Join(root, "native-shard.json"), map[string]any{
		"schema": "plug.s1.b01.native-vector-shard.v1", "test": t.Name(), "mode": mode,
		"index": shardIndex, "total": 2, "paths": replayPaths, "allPaths": allReplayPaths,
		"captures": 2, "replayRoots": 6, "commandsPerCapture": 433, "storeSlots": 1,
	})
	t.Logf("B01_NATIVE_SHARD test=%s mode=%s index=%d total=2 captures=2 replayRoots=6 paths=%d", t.Name(), mode, shardIndex, len(replayPaths))
	nativeJSON(t, filepath.Join(root, "native-replay-manifest.json"), map[string]any{
		"manifest": replayManifest,
		"paths":    replayPaths,
		"allPaths": allReplayPaths,
		"coverage": map[string]any{
			"matrix":              "2 vector/run roots x 3 backends per shard; 4 roots x 3 backends across both shards",
			"requiredNamedPhases": "every H03/H04 path in manifest",
			"rawXMLNamedSet":      "exact equality with paths; representation differences remain visible",
			"filteredOmission":    "fail closed",
		},
	})
	// Each of the two shard actions owns one store slot, preserving the total
	// two-store ceiling. Join both captures before their comparison and replay.
	replaySlots := make(chan struct{}, 1)
	var captures []nativeCapture
	for _, vector := range selectedInputs.Vectors {
		var twins []nativeCapture
		if len(vector.Runs) != 2 {
			t.Fatal("each vector requires two fresh stores")
		}
		results := make([]struct {
			capture nativeCapture
			passed  bool
		}, len(vector.Runs))
		var workers sync.WaitGroup
		for index, run := range vector.Runs {
			workers.Add(1)
			go func(index int, run nativeRun) {
				defer workers.Done()
				replaySlots <- struct{}{}
				defer func() { <-replaySlots }()
				results[index].passed = t.Run(vector.ID+"/"+run.ID, func(t *testing.T) {
					output := filepath.Join(root, vector.ID+"-"+run.ID)
					if err := os.Mkdir(output, 0o700); err != nil {
						t.Fatal(err)
					}
					results[index].capture = recordNativeVector(t, binary, output, inputs, vector, run)
				})
			}(index, run)
		}
		workers.Wait()
		// Retain declared run-a/run-b order, independent of worker completion.
		for _, result := range results {
			if result.passed {
				twins = append(twins, result.capture)
				captures = append(captures, result.capture)
			}
		}
		if len(twins) != 2 {
			t.Fatalf("%s lacks two successful actual captures; no replay or twin result", vector.ID)
		}
		// Only the explicitly projected semantic fields are compared. All raw
		// wall-clock timestamps, revisions and streams remain separate originals.
		equal := reflect.DeepEqual(twins[0].Before, twins[1].Before) && reflect.DeepEqual(twins[0].After, twins[1].After) && reflect.DeepEqual(twins[0].Transitions, twins[1].Transitions) && reflect.DeepEqual(twins[0].Membership, twins[1].Membership) && reflect.DeepEqual(twins[0].Exclusions, twins[1].Exclusions)
		nativeJSON(t, filepath.Join(root, vector.ID+"-twin-comparison.json"), map[string]any{
			"equal": equal, "runA": twins[0], "runB": twins[1],
			"projection": "role IDs, titles, statuses, priorities, parent edges, persisted until, ordered ready IDs and explain reasons/counts; raw bytes are not asserted identical",
		})
		if !equal {
			t.Fatalf("%s semantic twin projections differ; originals retained", vector.ID)
		}
	}
	// Both real captures owned by this shard finish before replay starts.
	// No reference is reconstructed from Plug expectations or historical files.
	h := newPostgresHarness(t)
	coverage := map[string]*nativeReplayCoverage{}
	// Replay roots have disjoint stores, clocks, authorities and output paths.
	// Bound their concurrency without extending the native test's deadline or
	// dropping any observations. Each root's operations and reopen stay serial.
	// Parallel children finish before parent cleanup. Keep this guard ahead of
	// PostgreSQL teardown and the final output inventory (cleanup is LIFO).
	t.Cleanup(func() {
		if len(coverage) != len(captures)*len(nativeReplayBackends) {
			t.Errorf("native replay coverage matrix has %d roots, want %d", len(coverage), len(captures)*len(nativeReplayBackends))
		}
		for _, capture := range captures {
			for _, backend := range nativeReplayBackends {
				key := capture.Vector.ID + "/" + capture.Run.ID + "/" + backend
				current, ok := coverage[key]
				if !ok {
					t.Errorf("native replay coverage omitted root %s", key)
					continue
				}
				current.require(t, capture.Vector.ID, capture.Run.ID)
			}
		}
	})
	for _, capture := range captures {
		for _, backend := range nativeReplayBackends {
			key := capture.Vector.ID + "/" + capture.Run.ID + "/" + backend
			current := newNativeReplayCoverage(backend)
			coverage[key] = current
			t.Run(capture.Vector.ID+"/"+capture.Run.ID+"/replay/"+backend, func(t *testing.T) {
				t.Parallel()
				replaySlots <- struct{}{}
				// Release the slot after this root's fixture cleanups, including
				// lease release and physical store close, have completed.
				t.Cleanup(func() { <-replaySlots })
				dsn := ""
				if backend == "postgres" {
					dsn = h.dsn(t)
				}
				replayNativeVector(t, capture, backend, dsn, inputs.AdoptedIndefiniteDecision, current)
				replayNativeExclusionVector(t, capture, backend, h)
			})
		}
	}
}

func recordNativeVector(t *testing.T, binary, output string, inputs nativeInputs, vector nativeVector, run nativeRun) nativeCapture {
	t.Helper()
	for _, role := range nativeRoles {
		if vector.SourceIDs[role] == "" {
			t.Fatalf("missing source identity for %s", role)
		}
	}
	// Retain literal absent/empty/refused/partial states even when an export
	// fails. inventory.json.sources is a sibling tree and is never discarded.
	t.Cleanup(func() {
		nativeExportStates(t, output, "before-final-inventory")
	})
	server := publicnative.StartDolt(t, output)
	workspace := t.TempDir()
	home := filepath.Join(workspace, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	// ChildEnv owns both roots: BEADS_DOLT_DATA_DIR is actual DataRoot;
	// DOLT_ROOT_PATH is the producer's private configuration root. Do not
	// replace either returned value with old proposal text or runner values.
	env := server.ChildEnv([]string{
		"PATH=" + filepath.Dir(server.DoltBinary) + ":/usr/bin:/bin",
		"HOME=" + home, "USERPROFILE=" + home,
		"XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "USER=public-native-b01",
		"TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1",
	})
	server.RegisterBeadsCleanup(t, binary, workspace, env)
	c := &nativeCommand{t: t, binary: binary, workspace: workspace, output: output, env: env}
	var version struct{ Commit, Build string }
	nativeDecode(t, c.run("version", "version", "--json"), &version)
	if version.Commit != inputs.Source || version.Build != "public-"+inputs.Source {
		t.Fatal("actual linked native version/build differs from the source pins")
	}
	c.run("init-server", "init", "--backend", "dolt", "--server", "--external",
		"--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port),
		"--database", run.Database, "--prefix", "b01", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive")
	for priority, role := range nativeRoles {
		c.run("create-"+role, "create", "--id", vector.SourceIDs[role], "--title", "B01 "+vector.Mode+" "+role,
			"--type", "task", "--force", "--priority", strconv.Itoa(priority), "--status", "open", "--json")
	}
	c.run("link-child", "update", vector.SourceIDs["child"], "--parent", vector.SourceIDs["parent"], "--json")
	c.run("link-grandchild", "update", vector.SourceIDs["grandchild"], "--parent", vector.SourceIDs["child"], "--json")
	before := recordNativePhase(t, c, vector, "before")
	deferArgs := []string{"defer", vector.SourceIDs["parent"]}
	if vector.Until != nil {
		deferArgs = append(deferArgs, "--until", *vector.Until)
	}
	deferArgs = append(deferArgs, "--reason", "distinct "+vector.Mode+" hierarchy vector", "--json")
	c.run("defer-parent", deferArgs...)
	after := recordNativePhase(t, c, vector, "after")
	// These actual writes close the f9 lifecycle/membership gap. The original
	// before/defer/after observations remain separate and retain both adopted
	// differences. No continuation is reconstructed from a Plug expectation.
	steps := []struct {
		phase string
		argv  []string
	}{
		{"resumed", []string{"update", vector.SourceIDs["parent"], "--status", "open", "--defer", "", "--json"}},
		{"blocked-parent", []string{"dep", "add", vector.SourceIDs["parent"], vector.SourceIDs["free"], "--type", "blocks", "--json"}},
		{"detached-from-blocked", []string{"update", vector.SourceIDs["child"], "--parent", "", "--json"}},
		{"reparented-to-open", []string{"update", vector.SourceIDs["child"], "--parent", vector.SourceIDs["free"], "--json"}},
		{"reattached-to-blocked", []string{"update", vector.SourceIDs["child"], "--parent", vector.SourceIDs["parent"], "--json"}},
		{"blocker-closed", []string{"close", vector.SourceIDs["free"], "--json"}},
		{"blocker-reopened", []string{"reopen", vector.SourceIDs["free"], "--json"}},
		{"restored", []string{"dep", "remove", vector.SourceIDs["parent"], vector.SourceIDs["free"], "--json"}},
		{"external-refused", []string{"dep", "add", vector.SourceIDs["free"], inputs.ExternalHierarchyTarget, "--type", "parent-child", "--json"}},
	}
	if len(inputs.HierarchyPhases) != len(steps) || inputs.ExternalHierarchyTarget == "" {
		t.Fatal("the finite continuation phase inputs are incomplete")
	}
	transitions := make(map[string]nativePhase, len(steps))
	for _, step := range steps {
		ok := t.Run(step.phase, func(t *testing.T) {
			c.t = t
			if step.phase == "external-refused" {
				c.refuse(step.phase+"-write", inputs.ExternalHierarchyRefusal, step.argv...)
			} else {
				c.run(step.phase+"-write", step.argv...)
			}
			transitions[step.phase] = recordNativePhase(t, c, vector, step.phase)
			if step.phase == "external-refused" {
				if !reflect.DeepEqual(transitions["restored"], transitions[step.phase]) {
					t.Fatal("native refused edge changed actual ready/explain/source projection")
				}
				for _, role := range nativeRoles {
					before, err := os.ReadFile(filepath.Join(output, "restored-show-"+role+".json"))
					if err != nil {
						t.Fatal(err)
					}
					after, err := os.ReadFile(filepath.Join(output, step.phase+"-show-"+role+".json"))
					if err != nil || string(before) != string(after) {
						t.Fatalf("native refused edge changed raw public show bytes for %s: %v", role, err)
					}
				}
			}
			t.Logf("B01_NATIVE_HIERARCHY_PHASE phase=%s vector=%s run=%s actualReady=%v", step.phase, vector.ID, run.ID, transitions[step.phase].Ready)
		})
		c.t = t
		if !ok {
			t.Fatalf("native hierarchy continuation %s failed; original command/phase bytes retained", step.phase)
		}
	}
	// Actual CommitAll includes config; inventory requires a clean native
	// working set. This one new command/store is declared in native_inputs.
	c.run("quiesce-commit", "dolt", "commit", "-m", "B01 "+vector.Mode+" public capture "+run.ID)
	actualDB := server.RequireDatabaseDir(t, run.Database)
	dbInfo, err := os.Lstat(actualDB)
	if err != nil {
		t.Fatal(err)
	}
	nativeJSON(t, filepath.Join(output, "physical-database.json"), map[string]any{
		"path": actualDB, "mode": dbInfo.Mode().String(), "identity": fmt.Sprint(dbInfo.Sys()),
		"serverPID": server.PID, "serverPort": server.Port, "dataRoot": server.DataRoot,
		"childEnv": env, "checkedUTC": time.Now().UTC(),
	})
	inventory := filepath.Join(output, "inventory.json")
	c.run("public-inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventory)
	nativeExportStates(t, output, "after-inventory")
	var census struct {
		Version         string
		Quiesced        bool
		WorkingSetClean bool `json:"working_set_clean"`
		External        []struct {
			Path, State string
		}
	}
	data, err := os.ReadFile(inventory)
	if err != nil {
		t.Fatal(err)
	}
	nativeDecode(t, data, &census)
	if census.Version != "beads-inventory/1" || !census.Quiesced || !census.WorkingSetClean {
		t.Fatal("actual public inventory lacks its version/quiesced/clean controls")
	}
	sidecars, err := os.Lstat(inventory + ".sources")
	if err != nil || !sidecars.IsDir() || sidecars.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("public inventory sidecar originals are missing or refused: %v", err)
	}
	// Validate inventory-declared sidecar paths against actual regular source
	// bytes. Inventory paths outside this sibling tree remain retained native
	// external-input declarations, rather than being silently relabelled.
	for _, external := range census.External {
		rel, err := filepath.Rel(inventory+".sources", external.Path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
			continue
		}
		info, err := os.Lstat(external.Path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("inventory-declared sidecar is not actual regular source bytes: %s: %v", external.Path, err)
		}
	}
	closure, err := nativeInventory(inventory + ".sources")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range closure {
		if entry["type"] != "file" && entry["type"] != "directory" {
			t.Fatalf("refused nonregular sidecar original: %+v", entry)
		}
	}
	nativeJSON(t, filepath.Join(output, "sidecar-closure-before-dump.json"), closure)
	c.run("public-dump", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventory, "-o", filepath.Join(output, "readback.tar"))
	nativeExportStates(t, output, "after-dump")
	closureAfter, err := nativeInventory(inventory + ".sources")
	if err != nil || !reflect.DeepEqual(closure, closureAfter) {
		t.Fatalf("public dump changed original sidecar closure: %v", err)
	}
	_, dumpBytes, err := nativeDigestFile(filepath.Join(output, "readback.tar"))
	if err != nil || dumpBytes == 0 || c.count != 26+8*len(steps) {
		t.Fatalf("public dump bytes=%d, commands=%d: %v", dumpBytes, c.count, err)
	}
	// Preserve the original successful hierarchy export before adding custom
	// status tables. The accepted converter explicitly refuses nonempty custom
	// status families; retain that new original refusal beside the old dump.
	membershipStart := c.count
	membership := recordNativeMembership(t, c, vector)
	if c.count-membershipStart != 1+13*len(nativeMembershipSteps()) {
		t.Fatal("native membership must execute every write/show/query/explain command")
	}
	c.run("membership-quiesce", "dolt", "commit", "-m", "B01 category capture "+run.ID)
	categoryInventory := filepath.Join(output, "membership-inventory.json")
	c.run("membership-inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", categoryInventory)
	c.refuse("membership-dump-refused", "unsupported nonempty family custom_statuses", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", categoryInventory, "-o", filepath.Join(output, "membership-refused-dump.tar"))
	nativeJSON(t, filepath.Join(output, "membership-export-states.json"), map[string]any{
		"inventory": nativeOutputState(categoryInventory), "sources": nativeOutputState(categoryInventory + ".sources"),
		"refusedDump": nativeOutputState(filepath.Join(output, "membership-refused-dump.tar")),
		"scope":       "Original hierarchy dump success is retained; custom-status native capture has a genuine explicitly unsupported export, no native import/restore proof",
	})
	if c.count != 26+8*len(steps)+1+13*len(nativeMembershipSteps())+3 {
		t.Fatal("native category/export command set is incomplete")
	}
	exclusionStart := c.count
	exclusions := recordNativeExclusions(t, c, vector)
	if c.count-exclusionStart != 1+9*len(nativeExclusionTypes()) {
		t.Fatal("native exclusion command set is incomplete")
	}
	c.run("exclusions-quiesce", "dolt", "commit", "-m", "B01 hidden type capture "+run.ID)
	c.run("exclusions-inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", filepath.Join(output, "exclusions-inventory.json"))
	checkNativeExclusionInventory(t, filepath.Join(output, "exclusions-inventory.json"), output)
	if c.count != 433 {
		t.Fatal("complete native hierarchy/category/type command set differs")
	}
	capture := nativeCapture{Vector: vector, Run: run, Before: before, After: after, Transitions: transitions, Membership: membership, Exclusions: exclusions, Output: output}
	nativeJSON(t, filepath.Join(output, "observed-projection.json"), capture)
	t.Logf("B01_NATIVE_CAPTURE vector=%s run=%s commands=%d actualPort=%d output=%s", vector.ID, run.ID, c.count, server.Port, output)
	return capture
}

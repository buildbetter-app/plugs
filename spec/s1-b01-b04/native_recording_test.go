package s1b01b04

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
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
	Output      string
}

// This manual target is a physical recorder, never part of ordinary //...
// gates. Launch requires the separately delivered, reviewed prepared input.
// Ordinary compilation cannot establish a native recording or admission.
func TestB01NativeTimedAndIndefinitePair(t *testing.T) {
	var inputs nativeInputs
	nativeDecode(t, nativeInputsBytes, &inputs)
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
		t.Logf("B01_NATIVE_OUTPUT root=%s inventory=output-inventory.json", root)
	})
	nativeWrite(t, filepath.Join(root, "native-inputs.json"), nativeInputsBytes)
	binary := nativePrepared(t, root, inputs)
	if len(inputs.Vectors) != 2 {
		t.Fatal("both distinct timed and indefinite input vectors are required")
	}
	var captures []nativeCapture
	for _, vector := range inputs.Vectors {
		var twins []nativeCapture
		if len(vector.Runs) != 2 {
			t.Fatal("each vector requires two fresh stores")
		}
		for _, run := range vector.Runs {
			var captured nativeCapture
			ok := t.Run(vector.ID+"/"+run.ID, func(t *testing.T) {
				output := filepath.Join(root, vector.ID+"-"+run.ID)
				if err := os.Mkdir(output, 0o700); err != nil {
					t.Fatal(err)
				}
				captured = recordNativeVector(t, binary, output, inputs, vector, run)
			})
			if ok {
				twins = append(twins, captured)
				captures = append(captures, captured)
			}
		}
		if len(twins) != 2 {
			t.Fatalf("%s lacks two successful actual captures; no replay or twin result", vector.ID)
		}
		// Only the explicitly projected semantic fields are compared. All raw
		// wall-clock timestamps, revisions and streams remain separate originals.
		equal := reflect.DeepEqual(twins[0].Before, twins[1].Before) && reflect.DeepEqual(twins[0].After, twins[1].After) && reflect.DeepEqual(twins[0].Transitions, twins[1].Transitions)
		nativeJSON(t, filepath.Join(root, vector.ID+"-twin-comparison.json"), map[string]any{
			"equal": equal, "runA": twins[0], "runB": twins[1],
			"projection": "role IDs, titles, statuses, priorities, parent edges, persisted until, ordered ready IDs and explain reasons/counts; raw bytes are not asserted identical",
		})
		if !equal {
			t.Fatalf("%s semantic twin projections differ; originals retained", vector.ID)
		}
	}
	// All four real captures are complete before replay starts. No reference
	// observation is reconstructed from Plug expectations or historical files.
	h := newPostgresHarness(t)
	for _, capture := range captures {
		for _, backend := range []string{"memory", "sqlite", "postgres"} {
			t.Run(capture.Vector.ID+"/"+capture.Run.ID+"/replay/"+backend, func(t *testing.T) {
				dsn := ""
				if backend == "postgres" {
					dsn = h.dsn(t)
				}
				replayNativeVector(t, capture, backend, dsn, inputs.AdoptedIndefiniteDecision)
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
	if err != nil || dumpBytes == 0 || c.count != 24+7*len(steps) {
		t.Fatalf("public dump bytes=%d, commands=%d: %v", dumpBytes, c.count, err)
	}
	if err := server.Stop(); err != nil {
		t.Fatalf("owned server shutdown: %v", err)
	}
	capture := nativeCapture{Vector: vector, Run: run, Before: before, After: after, Transitions: transitions, Output: output}
	nativeJSON(t, filepath.Join(output, "observed-projection.json"), capture)
	t.Logf("B01_NATIVE_CAPTURE vector=%s run=%s commands=%d actualPort=%d output=%s", vector.ID, run.ID, c.count, server.Port, output)
	return capture
}

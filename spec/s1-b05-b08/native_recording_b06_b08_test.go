package s1b05b08

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/publicnative"
)

//go:embed native_protocol_b06_b08.py
var b068Protocol []byte

type b068Capture struct {
	Directory string `json:"-"`
	Card      string `json:"card"`
	Commands  []struct {
		Argv   []string `json:"argv"`
		Exit   int      `json:"exitCode"`
		Stdout []byte   `json:"stdoutBase64"`
	} `json:"commands"`
	Observations json.RawMessage `json:"observations"`
}

func b068Output(t *testing.T, card string) string {
	t.Helper()
	root, err := os.MkdirTemp(nativeEnv(t, "TEST_UNDECLARED_OUTPUTS_DIR"), strings.ToLower(card)+"-native-")
	if err != nil {
		t.Fatal(err)
	}
	// Registered before owned servers: cleanup evidence is part of the closure.
	t.Cleanup(func() {
		inventory, err := nativeInventory(root)
		if err != nil {
			t.Error(err)
		}
		nativeJSON(t, filepath.Join(root, "output-inventory.json"), map[string]any{"entries": inventory, "complete": err == nil, "passed": !t.Failed(), "exclusions": []string{"poll.log by basename before stat/read/hash", "output-inventory.json self"}})
		t.Logf("B06_B08_NATIVE_OUTPUT card=%s root=%s", card, root)
	})
	nativeWrite(t, filepath.Join(root, "native-inputs.json"), nativeInputsBytes)
	nativeWrite(t, filepath.Join(root, "native-protocol.py"), b068Protocol)
	return root
}

func b068Record(t *testing.T, card, twin, root string) b068Capture {
	t.Helper()
	var inputs nativeInputs
	nativeDecode(t, nativeInputsBytes, &inputs)
	output := filepath.Join(root, twin)
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	binary := nativePrepared(t, output, inputs)
	server := publicnative.StartDolt(t, output)
	workspace := t.TempDir()
	home := filepath.Join(workspace, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	baseEnv := []string{"PATH=/usr/bin:/bin", "HOME=" + home, "USERPROFILE=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "USER=public-native-b06-b08", "TZ=UTC", "LC_ALL=C", "CI=true", "NO_COLOR=1", "GIT_CONFIG_NOSYSTEM=1", "BEADS_ACTOR=b06-b08-native"}
	preparation := map[string]string{}
	for _, name := range []string{"PLUG_PUBLIC_BEADS_BINARY", "PLUG_PUBLIC_BEADS_SHA256", "PLUG_PUBLIC_BEADS_BUILD_RUN", "PLUG_PUBLIC_BEADS_MANIFEST"} {
		preparation[name] = nativeEnv(t, name)
		baseEnv = append(baseEnv, name+"="+preparation[name])
	}
	env := server.ChildEnv(baseEnv)
	nativeJSON(t, filepath.Join(output, "four-exports-childenv.json"), map[string]any{"preparation": preparation, "baseEnv": baseEnv, "childEnv": env, "count": len(preparation), "routing": "Exactly four unchanged preparation values supplied before publicnative.ChildEnv; returned slice used unchanged for every actual child and receipt"})
	c := &nativeCommand{t: t, binary: binary, workspace: workspace, output: output, env: env}
	var version struct{ Commit, Build string }
	nativeDecode(t, c.run("linked-version", "version", "--json"), &version)
	if version.Commit != inputs.Source || version.Build != "public-"+inputs.Source {
		t.Fatal("native linked identity differs")
	}
	database := strings.ToLower(card) + "_" + strings.ReplaceAll(twin, "-", "_")
	c.run("init-owned-server", "init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", strings.ToLower(card), "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive")
	// The protocol is consumer-private and statically permits only B06/B08.
	// Its subprocesses inherit this exact ChildEnv and retain every stream.
	command := exec.Command("python3", filepath.Join(root, "native-protocol.py"), card, binary, output)
	command.Dir, command.Env = workspace, env
	stdout, stderr, runErr := runB068Protocol(command, output)
	if runErr != nil {
		t.Fatalf("actual native %s protocol failed: %v; stdout=%s stderr=%s", card, runErr, stdout, stderr)
	}
	var capture b068Capture
	data, err := os.ReadFile(filepath.Join(output, "observed-native.json"))
	if err != nil {
		t.Fatal(err)
	}
	nativeDecode(t, data, &capture)
	capture.Directory = output
	if capture.Card != card || len(capture.Commands) == 0 {
		t.Fatal("native protocol omitted its own observations")
	}
	c.run("quiesce-native-working-set", "dolt", "commit", "-m", card+" "+twin+" actual native capture")
	physical := server.RequireDatabaseDir(t, database)
	info, err := os.Lstat(physical)
	if err != nil {
		t.Fatal(err)
	}
	nativeJSON(t, filepath.Join(output, "physical-database.json"), map[string]any{"path": physical, "mode": info.Mode().String(), "identity": fmt.Sprint(info.Sys()), "pid": server.PID, "port": server.Port, "dataRoot": server.DataRoot, "childEnv": env, "checkedUTC": time.Now().UTC()})
	inventory := filepath.Join(output, "inventory.json")
	c.run("public-intact-inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventory)
	var census struct {
		Version  string
		Quiesced bool
		Clean    bool `json:"working_set_clean"`
	}
	data, err = os.ReadFile(inventory)
	if err != nil {
		t.Fatal(err)
	}
	nativeDecode(t, data, &census)
	if census.Version != "beads-inventory/1" || !census.Quiesced || !census.Clean {
		t.Fatal("native inventory is not genuine clean/quiesced source")
	}
	before, err := nativeInventory(inventory + ".sources")
	if err != nil {
		t.Fatal(err)
	}
	c.run("public-intact-dump", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventory, "-o", filepath.Join(output, "readback.tar"))
	after, err := nativeInventory(inventory + ".sources")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("dump changed original sidecars: %v", err)
	}
	if _, size, err := nativeDigestFile(filepath.Join(output, "readback.tar")); err != nil || size == 0 {
		t.Fatalf("native dump absent/empty: %v", err)
	}
	nativeJSON(t, filepath.Join(output, "intact-sidecar-closure.json"), after)
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	t.Logf("B06_B08_NATIVE_CAPTURE card=%s twin=%s commands=%d output=%s", card, twin, len(capture.Commands), output)
	return capture
}

func runB068Protocol(command *exec.Cmd, output string) ([]byte, []byte, error) {
	// Output() keeps separate successful stderr when a buffer is supplied.
	var stderr strings.Builder
	command.Stderr = &stderr
	start := time.Now().UTC()
	stdout, err := command.Output()
	end := time.Now().UTC()
	if e := os.WriteFile(filepath.Join(output, "protocol.stdout"), stdout, 0o600); e != nil {
		return nil, nil, e
	}
	if e := os.WriteFile(filepath.Join(output, "protocol.stderr"), []byte(stderr.String()), 0o600); e != nil {
		return nil, nil, e
	}
	code := -1
	if command.ProcessState != nil {
		code = command.ProcessState.ExitCode()
	}
	receipt, _ := json.MarshalIndent(map[string]any{"argv": command.Args, "cwd": command.Dir, "childEnv": command.Env, "exit": code, "start": start, "end": end, "stdoutSHA256": b068SHA(stdout), "stderrSHA256": b068SHA([]byte(stderr.String()))}, "", "  ")
	if e := os.WriteFile(filepath.Join(output, "protocol.receipt.json"), receipt, 0o600); e != nil {
		return nil, nil, e
	}
	return stdout, []byte(stderr.String()), err
}

func TestB06NativePublicCyclesTwinsAndThreeDriverReplay(t *testing.T) {
	root := b068Output(t, "B06")
	var captures []b068Capture
	for _, twin := range []string{"run-a", "run-b"} {
		t.Run(twin, func(t *testing.T) { captures = append(captures, b068Record(t, "B06", twin, root)) })
	}
	if len(captures) != 2 {
		t.Fatal("both real native twin captures are mandatory")
	}
	b068CompareTwins(t, root, "B06", captures)
	for i, capture := range captures {
		for _, engine := range testEngines {
			t.Run(fmt.Sprintf("replay-%d/%s", i, engine), func(t *testing.T) { b068ReplayCycles(t, engine, capture) })
			t.Run(fmt.Sprintf("import-%d/%s", i, engine), func(t *testing.T) { b068ReplayImportedCycles(t, engine, capture) })
		}
	}
}

func b068ReplayImportedCycles(t *testing.T, engine string, capture b068Capture) {
	t.Helper()
	p := b068ImportArchive(t, engine, capture, "readback.tar", "b06-source")
	var observed struct {
		CycleProbes []struct {
			Case  string     `json:"case"`
			Paths [][]string `json:"paths"`
		} `json:"cycleProbes"`
	}
	nativeDecode(t, capture.Observations, &observed)
	var wantIDs []string
	for _, command := range capture.Commands {
		if len(command.Argv) == 0 || command.Argv[0] != "create" {
			continue
		}
		for i, arg := range command.Argv {
			if arg == "--id" {
				wantIDs = append(wantIDs, command.Argv[i+1])
			}
		}
	}
	before := p.f.conservation(t)
	var gotIDs []string
	for _, row := range before.Rows {
		if row.Kind == "bead" {
			gotIDs = append(gotIDs, row.Ref.ID)
		}
	}
	slices.Sort(gotIDs)
	slices.Sort(wantIDs)
	if len(wantIDs) == 0 || !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("full native B06 role corpus differs: got=%v want=%v", gotIDs, wantIDs)
	}
	nativeJSON(t, filepath.Join(p.output, "initial-full-state.json"), before)
	for _, stage := range []string{"imported", "sql-reopened"} {
		if stage == "sql-reopened" {
			p.reopen(t)
			b08SameConservation(t, before, p.f.conservation(t))
		}
		seen := 0
		for _, probe := range observed.CycleProbes {
			if probe.Case != "default-excludes-tracks" && probe.Case != "tracks-expanded" {
				continue
			}
			seen++
			reply := p.call(t, "bead.cycles", "native-imported-"+stage+"-"+probe.Case, map[string]any{"includeTracks": probe.Case == "tracks-expanded", "limit": 100})
			var actual service.CyclesResult
			if reply.Error != nil || json.Unmarshal(reply.Result, &actual) != nil {
				t.Fatalf("actual imported native cycle report failed: %+v", reply)
			}
			paths := [][]string{}
			for _, cycle := range actual.Items {
				path := []string{}
				if cycle.Partial || !cycle.UnreadableCount.IsZero() {
					t.Fatal("unexpected unreadable native imported role")
				}
				for _, member := range cycle.Path {
					if member.Ref == nil {
						t.Fatal("imported native member missing")
					}
					path = append(path, member.Ref.ID)
				}
				paths = append(paths, path)
			}
			nativeJSON(t, filepath.Join(p.output, stage+"-"+probe.Case+".json"), map[string]any{"native": probe.Paths, "plug": actual, "paths": paths})
			if actual.Truncated || actual.CycleCount.String() != strconv.Itoa(len(probe.Paths)) || !reflect.DeepEqual(paths, probe.Paths) {
				t.Fatalf("actual full imported native cycle parity differs: native=%v Plug=%v", probe.Paths, paths)
			}
			b08SameConservation(t, before, p.f.conservation(t))
		}
		if seen != 2 {
			t.Fatal("native default and tracks-expanded observations are mandatory")
		}
	}
}

func b068ReplayCycles(t *testing.T, engine string, capture b068Capture) {
	t.Helper()
	f := newB08CloseFixture(t, engine, ontology.DefaultDocument())
	f.ids = nil
	for _, command := range capture.Commands {
		if len(command.Argv) > 0 && command.Argv[0] == "create" {
			for i, arg := range command.Argv {
				if arg == "--id" {
					f.ids = append(f.ids, command.Argv[i+1])
				}
			}
		}
	}
	refs := map[string]model.Ref{}
	for i, command := range capture.Commands {
		argv := command.Argv
		if len(argv) == 0 {
			t.Fatal("empty native argv")
		}
		switch {
		case argv[0] == "create":
			var id string
			for n, arg := range argv {
				if arg == "--id" {
					id = argv[n+1]
				}
			}
			refs[id] = f.createB08(t, id, "issue", nil, nil)
		case len(argv) > 3 && argv[0] == "dep" && argv[1] == "add":
			kind := "blocks"
			for n, arg := range argv {
				if arg == "--type" {
					kind = argv[n+1]
				}
			}
			params := map[string]any{"from": refs[argv[2]], "to": refs[argv[3]], "kind": kind}
			before := f.conservation(t)
			reply := f.callB08(t, "bead.link", fmt.Sprintf("native-edge-%d", i), params)
			if command.Exit == 0 {
				b08Commit(t, reply)
			} else {
				if reply.Error == nil || (reply.Error.Data.Code != "ontology_guard_failed" && reply.Error.Data.Code != "invalid_params") {
					t.Fatalf("native guard refused but Plug returned %+v", reply)
				}
				b08SameConservation(t, before, f.conservation(t))
			}
		case len(argv) > 1 && argv[0] == "dep" && argv[1] == "cycles" && slices.Contains(argv, "--json"):
			result, err := f.service.Cycles(f.ctx, anonymous, service.CyclesRequest{Project: project, IncludeTracks: slices.Contains(argv, "--include-tracks"), Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			// Native --limit applies only to text. Plug limit is an explicit
			// response envelope; full JSON paths are compared at limit100.
			raw := nativeCommandStdout(t, capture, i)
			var native []struct {
				Members []struct {
					ID string `json:"id"`
				} `json:"members"`
				Partial bool `json:"partial"`
			}
			nativeDecode(t, raw, &native)
			want := [][]string{}
			for _, cycle := range native {
				path := []string{}
				for _, member := range cycle.Members {
					path = append(path, member.ID)
				}
				want = append(want, path)
			}
			actual := [][]string{}
			for _, cycle := range result.Items {
				path := []string{}
				for _, member := range cycle.Path {
					if member.Ref == nil {
						t.Fatal("unexpected unreadable public fixture")
					}
					path = append(path, member.Ref.ID)
				}
				actual = append(actual, path)
			}
			if !reflect.DeepEqual(actual, want) {
				t.Fatalf("native/Plug cycle paths differ: %v vs %v", want, actual)
			}
		}
	}
	after := f.conservation(t)
	f.reopenB08(t)
	b08SameConservation(t, after, f.conservation(t))
}

// Decoding the retained base64 preserves each original stdout byte.
func nativeCommandStdout(t *testing.T, capture b068Capture, index int) []byte {
	t.Helper()
	if index < 0 || index >= len(capture.Commands) {
		t.Fatal("native command index outside capture")
	}
	return capture.Commands[index].Stdout
}

func TestB08NativePublicSettlementTwinsAndThreeDriverReplay(t *testing.T) {
	root := b068Output(t, "B08")
	var captures []b068Capture
	for _, twin := range []string{"run-a", "run-b"} {
		t.Run(twin, func(t *testing.T) { captures = append(captures, b068Record(t, "B08", twin, root)) })
	}
	if len(captures) != 2 {
		t.Fatal("both native close twins required")
	}
	b068CompareTwins(t, root, "B08", captures)
	for i, capture := range captures {
		for _, engine := range testEngines {
			t.Run(fmt.Sprintf("replay-%d/%s", i, engine), func(t *testing.T) { b068ReplaySettlement(t, engine, capture) })
		}
	}
}

func b068ReplaySettlement(t *testing.T, engine string, capture b068Capture) {
	t.Helper()
	var observed struct {
		BeforeReady, AfterReady, ReopenedReady   []string
		ForcedStatus, ChildStatus, ForeignHolder string
		ClosedParentRepeat, BlockerRepeat        string
		FinalRows                                []struct {
			ID string `json:"id"`
		}
	}
	nativeDecode(t, capture.Observations, &observed)
	if observed.ForcedStatus != "closed" || observed.ChildStatus != "open" || observed.ForeignHolder != "another-actor" || len(observed.FinalRows) != 7 || observed.ClosedParentRepeat != "open-child-refusal-full-state-conserved" || observed.BlockerRepeat != "ordinary-close-noop-full-state-conserved" {
		t.Fatal("native seven-role vector is incomplete")
	}
	p := b068ImportArchive(t, engine, capture, "initial-readback.tar", "b08-parent")
	f := p.f
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	parent, child := ref("b08-parent"), ref("b08-child")
	blocker, dependent, foreign, pinned := ref("b08-blocker"), ref("b08-dependent"), ref("b08-foreign"), ref("b08-pinned")
	// Census the complete imported corpus. No selected-row reconstruction or
	// discarded foreign/pinned members can satisfy this comparison.
	state := f.conservation(t)
	var gotIDs, wantIDs []string
	for _, row := range state.Rows {
		if row.Kind == "bead" {
			gotIDs = append(gotIDs, row.Ref.ID)
		}
	}
	for _, row := range observed.FinalRows {
		wantIDs = append(wantIDs, row.ID)
	}
	slices.Sort(gotIDs)
	slices.Sort(wantIDs)
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("complete imported role corpus differs: got=%v want=%v", gotIDs, wantIDs)
	}
	nativeJSON(t, filepath.Join(p.output, "initial-full-state.json"), state)
	b068CompareReady(t, p, observed.BeforeReady)
	for _, control := range []struct {
		name   string
		target model.Ref
		guard  ontology.GuardKind
	}{
		{"parent", parent, ontology.NoOpenChildren}, {"blocker", dependent, ontology.NoOpenBlockers},
	} {
		before := f.conservation(t)
		b08RequireRefusal(t, "bead.close", p.call(t, "bead.close", "native-"+control.name+"-refusal", b08ForceParams("bead.close", control.target, false, "")), "ontology_guard_failed", control.guard)
		b08SameConservation(t, before, f.conservation(t))
	}
	// Exercise both additional genuine roles. Missing typed holder/pinned
	// semantics produces an actual parity failure; source metadata is never
	// promoted to a grant, and no lease or authorization facts are invented.
	for _, control := range []struct {
		name   string
		target model.Ref
	}{{"foreign", foreign}, {"pinned", pinned}} {
		before := f.conservation(t)
		reply := p.call(t, "bead.close", "native-"+control.name+"-refusal", b08ForceParams("bead.close", control.target, false, ""))
		after := f.conservation(t)
		nativeJSON(t, filepath.Join(p.output, control.name+"-refusal-conservation.json"), map[string]any{"reply": reply, "before": before, "after": after})
		if reply.Error == nil {
			t.Errorf("actual imported %s close admitted where native refused; typed native authority/role parity is absent", control.name)
		}
		if !reflect.DeepEqual(before, after) {
			t.Errorf("actual imported %s refusal did not conserve complete state", control.name)
		}
	}
	params := b08ForceParams("bead.close", parent, true, "first forced reason")
	first := b08Commit(t, p.call(t, "bead.close", "native-production-close", params))
	if f.document(t, parent).Fields["status"] != "done" || f.document(t, child).Fields["status"] != "todo" {
		t.Fatal("actual imported force changed child conservation")
	}
	after := f.conservation(t)
	p.reopen(t)
	b08SameConservation(t, after, f.conservation(t))
	replay := p.call(t, "bead.close", "native-production-close", params)
	if replay.Meta["replayed"] != true || !reflect.DeepEqual(b08Commit(t, replay), first) {
		t.Fatal("actual imported production receipt changed")
	}
	b08SameConservation(t, after, f.conservation(t))
	b08RequireRefusal(t, "bead.close", p.call(t, "bead.close", "native-production-parent-repeat", b08ForceParams("bead.close", parent, false, "must preserve first close")), "ontology_guard_failed", ontology.NoOpenChildren)
	b08SameConservation(t, after, f.conservation(t))
	b08Commit(t, p.call(t, "bead.close", "native-production-blocker", b08ForceParams("bead.close", blocker, false, "settle blocker")))
	blockerClosed := f.conservation(t)
	b08Commit(t, p.call(t, "bead.close", "native-production-blocker-repeat", b08ForceParams("bead.close", blocker, false, "must preserve blocker first close")))
	blockerRepeated := f.conservation(t)
	repeatScope := f.scope("bead.close", "native-production-blocker-repeat", actor)
	receipt, exists := blockerRepeated.Receipts[repeatScope.identity()]
	if !exists || receipt.Changeset.Kind != "" || receipt.ResponsePending {
		t.Fatal("actual imported repeated blocker close did not retain a final no-op receipt")
	}
	delete(blockerRepeated.Receipts, repeatScope.identity())
	b08SameConservation(t, blockerClosed, blockerRepeated)
	b068CompareReady(t, p, observed.AfterReady)
	b08Commit(t, p.call(t, "bead.reopen", "native-production-reblock", map[string]any{"id": blocker, "reason": "reblock dependent"}))
	b068CompareReady(t, p, observed.ReopenedReady)
	reblocked := f.conservation(t)
	p.reopen(t)
	b08SameConservation(t, reblocked, f.conservation(t))
	b068CompareReady(t, p, observed.ReopenedReady)
	foreignBefore := f.document(t, foreign)
	b08Commit(t, p.call(t, "bead.close", "native-production-foreign-force", b08ForceParams("bead.close", foreign, true, "")))
	b068CompareHolder(t, p, foreign, observed.ForeignHolder, "closed")
	b08Commit(t, p.call(t, "bead.reopen", "native-production-foreign-reopen", map[string]any{"id": foreign}))
	b068CompareHolder(t, p, foreign, observed.ForeignHolder, "reopened")
	if !reflect.DeepEqual(foreignBefore.Metadata, f.document(t, foreign).Metadata) {
		t.Error("foreign close/reopen changed actual retained source metadata")
	}
	final := f.conservation(t)
	nativeJSON(t, filepath.Join(p.output, "final-full-state.json"), final)
	p.reopen(t)
	b08SameConservation(t, final, f.conservation(t))
}

func b068CompareHolder(t *testing.T, p *b068ImportedRuntime, ref model.Ref, expected, stage string) {
	t.Helper()
	reply := p.call(t, "bead.get", "native-holder-"+stage, map[string]any{"id": ref})
	var actual struct {
		Bead struct {
			ID       string `json:"id"`
			Assignee string `json:"assignee"`
		} `json:"bead"`
	}
	if reply.Error != nil || json.Unmarshal(reply.Result, &actual) != nil || actual.Bead.ID != ref.ID {
		t.Fatalf("actual imported holder read failed: %+v", reply)
	}
	nativeJSON(t, filepath.Join(p.output, "foreign-holder-"+stage+".json"), map[string]any{"nativeHolder": expected, "actualPublicResult": json.RawMessage(reply.Result), "actualStoredDocument": p.f.document(t, ref)})
	if actual.Bead.Assignee != expected {
		t.Errorf("actual %s holder parity absent: native=%q Plug=%q; opaque imported assignee is not authenticated holder authority", stage, expected, actual.Bead.Assignee)
	}
}

func b068CompareReady(t *testing.T, p *b068ImportedRuntime, native []string) {
	t.Helper()
	reply := p.call(t, "bead.ready", "native-production-ready", map[string]any{"limit": 1000})
	var actual struct {
		Items      []service.Bead `json:"items"`
		NextCursor *string        `json:"nextCursor"`
	}
	if reply.Error != nil || json.Unmarshal(reply.Result, &actual) != nil || actual.NextCursor != nil {
		t.Fatalf("complete production ready failed: %+v", reply)
	}
	want, got := append([]string{}, native...), []string{}
	for _, row := range actual.Items {
		got = append(got, row.ID)
	}
	slices.Sort(want)
	slices.Sort(got)
	nativeJSON(t, filepath.Join(p.output, fmt.Sprintf("ready-comparison-%03d.json", p.calls)), map[string]any{"nativeComplete": want, "plugComplete": got, "equal": reflect.DeepEqual(want, got)})
	if !reflect.DeepEqual(want, got) {
		t.Errorf("native/Plug complete role readiness differs: native=%v Plug=%v", want, got)
	}
}

func b068CompareTwins(t *testing.T, root, card string, captures []b068Capture) {
	t.Helper()
	var projections [2]any
	for i, capture := range captures {
		if card == "B06" {
			var observation struct {
				CycleProbes []struct {
					Case         string
					Paths        [][]string
					Total, Shown int
				}
				GuardRefusals      []struct{ Case string }
				CycleProbeCoverage struct {
					Status             string
					Supported, Blocked []string
				}
			}
			nativeDecode(t, capture.Observations, &observation)
			projections[i] = observation
		} else {
			var observation struct {
				BeforeReady, AfterReady, ReopenedReady   []string
				ForcedStatus, ChildStatus, ForeignHolder string
				ClosedParentRepeat, BlockerRepeat        string
				RefusalConservation                      string
			}
			nativeDecode(t, capture.Observations, &observation)
			projections[i] = observation
		}
	}
	equal := reflect.DeepEqual(projections[0], projections[1])
	nativeJSON(t, filepath.Join(root, "semantic-twin-comparison.json"), map[string]any{"card": card, "projections": projections, "equal": equal, "rawEvidence": "All original command streams, SQL/public rows, timestamps, captures, inventory and dump sidecars retained unchanged in each twin directory. This comparison binds the named behavioral observations."})
	if !equal {
		t.Fatal("independent native twins differ in the required behavioral observations")
	}
}

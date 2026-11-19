package s1b01b04

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// This contract is deliberately separate from native_inputs.json, which is
// the B01 recorder's consumer binding. The producer, control, helper and
// artifact pins stay shared; the consumer identity and parent are B02's own
// exact source lineage.
//
//go:embed native_b02_inputs.json
var nativeB02InputsBytes []byte

// This is a separately invoked physical recorder. Source review and ordinary
// compilation supply no observation of the producer or of these replay cases.
type b02NodeSpec struct {
	Role, Parent, Spawner, Gate string
	Ephemeral                   bool
}

type b02EdgeSpec struct{ From, To, Kind string }

type b02Step struct {
	Name, Action, Role, Target, Kind string
	NativeStatus, PlugMethod         string
	NativeRefusal, PlugRefusal       string
	NativeReady, PlugReady           []string
	Deviation                        string
	Force                            bool
}

type b02Plan struct {
	Name           string
	Nodes          []b02NodeSpec
	Edges          []b02EdgeSpec
	InitialReady   []string
	Steps          []b02Step
	CustomStatuses bool
	NativeOnly     string
}

func b02Plans() []b02Plan {
	return []b02Plan{
		{
			Name:         "diamond-chain",
			Nodes:        []b02NodeSpec{{Role: "root"}, {Role: "left"}, {Role: "right"}, {Role: "join"}, {Role: "tail"}},
			Edges:        []b02EdgeSpec{{"left", "root", "blocks"}, {"right", "root", "blocks"}, {"join", "left", "blocks"}, {"join", "right", "blocks"}, {"tail", "join", "blocks"}},
			InitialReady: []string{"root"},
			Steps: []b02Step{
				{Name: "blocked-close-refused", Action: "close", Role: "join", NativeRefusal: "cannot close blocked issue", PlugRefusal: "ontology_guard_failed", NativeReady: []string{"root"}, PlugReady: []string{"root"}},
				{Name: "self-link-refused", Action: "self", Role: "root", NativeRefusal: "cannot depend on itself", PlugRefusal: "invalid_params", NativeReady: []string{"root"}, PlugReady: []string{"root"}},
				{Name: "root-closed", Action: "close", Role: "root", PlugMethod: "bead.close", NativeReady: []string{"left", "right"}, PlugReady: []string{"left", "right"}},
				{Name: "left-closed", Action: "close", Role: "left", PlugMethod: "bead.close", NativeReady: []string{"right"}, PlugReady: []string{"right"}},
				{Name: "right-closed", Action: "close", Role: "right", PlugMethod: "bead.close", NativeReady: []string{"join"}, PlugReady: []string{"join"}},
				{Name: "join-closed", Action: "close", Role: "join", PlugMethod: "bead.close", NativeReady: []string{"tail"}, PlugReady: []string{"tail"}},
				{Name: "root-reopened", Action: "reopen", Role: "root", PlugMethod: "bead.reopen", NativeReady: []string{"root", "tail"}, PlugReady: []string{"root", "tail"}},
				{Name: "tail-unlinked", Action: "unlink", Role: "tail", Target: "join", Kind: "blocks", NativeReady: []string{"root", "tail"}, PlugReady: []string{"root", "tail"}},
			},
		},
		{
			Name:         "parent-reasons",
			Nodes:        []b02NodeSpec{{Role: "blocker"}, {Role: "parent"}, {Role: "child", Parent: "parent"}, {Role: "grandchild", Parent: "child"}, {Role: "direct", Parent: "parent"}},
			Edges:        []b02EdgeSpec{{"parent", "blocker", "blocks"}, {"direct", "blocker", "blocks"}},
			InitialReady: []string{"blocker"},
			Steps: []b02Step{
				{Name: "blocker-closed", Action: "close", Role: "blocker", PlugMethod: "bead.close", NativeReady: []string{"parent", "child", "grandchild", "direct"}, PlugReady: []string{"parent", "child", "grandchild", "direct"}},
				{Name: "blocker-reopened", Action: "reopen", Role: "blocker", PlugMethod: "bead.reopen", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}},
				{Name: "direct-unlinked-still-inherits", Action: "unlink", Role: "direct", Target: "blocker", Kind: "blocks", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}},
				{Name: "parent-unlinked", Action: "unlink", Role: "parent", Target: "blocker", Kind: "blocks", NativeReady: []string{"blocker", "parent", "child", "grandchild", "direct"}, PlugReady: []string{"blocker", "parent", "child", "grandchild", "direct"}},
			},
		},
		{
			Name:         "waits-gates",
			Nodes:        []b02NodeSpec{{Role: "spawner"}, {Role: "one", Parent: "spawner"}, {Role: "two", Parent: "spawner"}, {Role: "all", Spawner: "spawner", Gate: "all-children"}, {Role: "any", Spawner: "spawner", Gate: "any-children"}, {Role: "empty"}, {Role: "emptygate", Spawner: "empty", Gate: "any-children"}},
			InitialReady: []string{"spawner", "one", "two", "empty", "emptygate"},
			Steps: []b02Step{
				{Name: "one-closed", Action: "close", Role: "one", PlugMethod: "bead.close", NativeReady: []string{"spawner", "two", "any", "empty", "emptygate"}, PlugReady: []string{"spawner", "two", "any", "empty", "emptygate"}},
				{Name: "two-closed", Action: "close", Role: "two", PlugMethod: "bead.close", NativeReady: []string{"spawner", "all", "any", "empty", "emptygate"}, PlugReady: []string{"spawner", "all", "any", "empty", "emptygate"}},
				{Name: "also-blocks-stored-seam", Action: "also", Role: "all", Target: "spawner", NativeReady: []string{"spawner", "any", "empty", "emptygate"}, PlugReady: []string{"spawner", "any", "empty", "emptygate"}, Deviation: "Native metadata is deliberately injected through actual SQL; this proves evaluation of stored metadata, not a native product writer for it"},
				{Name: "spawner-closed", Action: "close", Role: "spawner", PlugMethod: "bead.close", NativeReady: []string{"all", "any", "empty", "emptygate"}, PlugReady: []string{"all", "any", "empty", "emptygate"}},
			},
		},
		{
			Name: "category-outcomes", CustomStatuses: true,
			Nodes:        []b02NodeSpec{{Role: "blocker"}, {Role: "ordinary"}, {Role: "conditional"}},
			Edges:        []b02EdgeSpec{{"ordinary", "blocker", "blocks"}, {"conditional", "blocker", "conditional-blocks"}},
			InitialReady: []string{"blocker"},
			Steps: []b02Step{
				{Name: "pinned-backlog", Action: "status", Role: "blocker", NativeStatus: "pinned", PlugMethod: "bead.defer", NativeReady: []string{"ordinary", "conditional"}, PlugReady: []string{}, Deviation: "A3/P22: native pinned satisfies both edges; Plug backlog satisfies neither"},
				{Name: "open-again", Action: "reopen", Role: "blocker", PlugMethod: "bead.reopen", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}},
				{Name: "custom-active", Action: "status", Role: "blocker", NativeStatus: "b02active", PlugMethod: "bead.reopen", PlugRefusal: "invalid_params", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, Deviation: "Native changes the active status name; Plug is already queued and its reopen slot refuses without mutation"},
				{Name: "custom-done", Action: "status", Role: "blocker", NativeStatus: "b02done", PlugMethod: "bead.close", NativeReady: []string{}, PlugReady: []string{"ordinary"}, Deviation: "A3: native blocker satisfaction uses literal closed/pinned; Plug completed-success satisfies ordinary blocks only"},
				{Name: "custom-frozen", Action: "status", Role: "blocker", NativeStatus: "b02frozen", PlugMethod: "bead.defer", NativeReady: []string{}, PlugReady: []string{}},
				{Name: "closed-success", Action: "close", Role: "blocker", PlugMethod: "bead.close", NativeReady: []string{"ordinary", "conditional"}, PlugReady: []string{"ordinary"}, Deviation: "A3: success emits dependency.conditionUnmet and leaves the conditional dependent blocked"},
				{Name: "reopen-for-failure", Action: "reopen", Role: "blocker", PlugMethod: "bead.reopen", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}},
				{Name: "closed-failure", Action: "close", Role: "blocker", PlugMethod: "bead.fail", NativeReady: []string{"ordinary", "conditional"}, PlugReady: []string{"ordinary", "conditional"}},
				{Name: "reopen-for-cancel", Action: "reopen", Role: "blocker", PlugMethod: "bead.reopen", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}},
				{Name: "closed-canceled", Action: "close", Role: "blocker", PlugMethod: "bead.cancel", NativeReady: []string{"ordinary", "conditional"}, PlugReady: []string{"ordinary"}, Deviation: "A3: native close reason records canceled intent; Plug canceled satisfies ordinary blocks and emits dependency.conditionUnmet"},
			},
		},
		{
			Name:         "terminal-subjects",
			Nodes:        []b02NodeSpec{{Role: "blocker"}, {Role: "terminal"}, {Role: "pinned"}},
			Edges:        []b02EdgeSpec{{"terminal", "blocker", "blocks"}, {"pinned", "blocker", "blocks"}},
			InitialReady: []string{"blocker"},
			Steps: []b02Step{
				{Name: "pinned-subject", Action: "status", Role: "pinned", NativeStatus: "pinned", PlugMethod: "bead.defer", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, Deviation: "Pinned maps to Plug backlog; only subject membership is compared"},
				{Name: "forced-terminal-subject", Action: "close", Role: "terminal", Force: true, PlugMethod: "bead.cancel", NativeReady: []string{"blocker"}, PlugReady: []string{"blocker"}, Deviation: "Native forced close and Plug unguarded cancel establish terminal subjects through distinct public operations; mutation parity is null"},
			},
		},
		{
			Name:         "mixed-chain",
			Nodes:        []b02NodeSpec{{Role: "root"}, {Role: "middle", Ephemeral: true}, {Role: "tail"}, {Role: "last", Ephemeral: true}},
			Edges:        []b02EdgeSpec{{"middle", "root", "blocks"}, {"tail", "middle", "blocks"}, {"last", "tail", "blocks"}},
			InitialReady: []string{"root"},
			NativeOnly:   "Real mixed-plane native rows are captured. This private core fixture has no accepted ephemeral workflow producer; no durable substitute or Plug/native mixed-plane parity is claimed",
			Steps: []b02Step{
				{Name: "root-closed", Action: "close", Role: "root", NativeReady: []string{"middle"}},
				{Name: "middle-closed", Action: "close", Role: "middle", NativeReady: []string{"tail"}},
				{Name: "tail-closed", Action: "close", Role: "tail", NativeReady: []string{"last"}},
			},
		},
		{
			Name:         "stale-projection",
			Nodes:        []b02NodeSpec{{Role: "blocker"}, {Role: "dependent"}},
			Edges:        []b02EdgeSpec{{"dependent", "blocker", "blocks"}},
			InitialReady: []string{"blocker"},
			NativeOnly:   "Deliberately stale native stored projection uses a labelled SQL corruption seam. Plug has no corresponding accepted product writer; cross-system projection-repair parity is null. Stable second reads do not prove an internal nonrecursive call trace",
			Steps: []b02Step{
				{Name: "stale-false-recomputed", Action: "stale", Role: "dependent", NativeReady: []string{"blocker"}},
			},
		},
	}
}

// These are independently authored subject/reason sets for every answering
// phase, not the complement of ready or a projection of returned blocked rows.
// The accepted producer reports active direct targets first, then immediate
// parent fallback; custom category names do not replace closed/pinned here.
func b02ExpectedBlocked(t *testing.T, plan, label string) map[string][]string {
	t.Helper()
	expectations := map[string]map[string]map[string][]string{
		"diamond-chain": {
			"initial":               {"left": {"root"}, "right": {"root"}, "join": {"left", "right"}, "tail": {"join"}},
			"blocked-close-refused": {"left": {"root"}, "right": {"root"}, "join": {"left", "right"}, "tail": {"join"}},
			"self-link-refused":     {"left": {"root"}, "right": {"root"}, "join": {"left", "right"}, "tail": {"join"}},
			"root-closed":           {"join": {"left", "right"}, "tail": {"join"}},
			"left-closed":           {"join": {"right"}, "tail": {"join"}},
			"right-closed":          {"tail": {"join"}},
			"join-closed":           {},
			"root-reopened":         {},
			"tail-unlinked":         {},
		},
		"parent-reasons": {
			"initial":                        {"parent": {"blocker"}, "child": {"parent"}, "grandchild": {"child"}, "direct": {"blocker"}},
			"blocker-closed":                 {},
			"blocker-reopened":               {"parent": {"blocker"}, "child": {"parent"}, "grandchild": {"child"}, "direct": {"blocker"}},
			"direct-unlinked-still-inherits": {"parent": {"blocker"}, "child": {"parent"}, "grandchild": {"child"}, "direct": {"parent"}},
			"parent-unlinked":                {},
		},
		"waits-gates": {
			"initial":                 {"all": {"spawner"}, "any": {"spawner"}},
			"one-closed":              {"all": {"spawner"}},
			"two-closed":              {},
			"also-blocks-stored-seam": {"all": {"spawner"}},
			"spawner-closed":          {},
		},
		"category-outcomes": {
			"initial":            {"ordinary": {"blocker"}, "conditional": {"blocker"}},
			"pinned-backlog":     {},
			"open-again":         {"ordinary": {"blocker"}, "conditional": {"blocker"}},
			"custom-active":      {"ordinary": {"blocker"}, "conditional": {"blocker"}},
			"custom-done":        {"ordinary": {"blocker"}, "conditional": {"blocker"}},
			"custom-frozen":      {"ordinary": {"blocker"}, "conditional": {"blocker"}},
			"closed-success":     {},
			"reopen-for-failure": {"ordinary": {"blocker"}, "conditional": {"blocker"}},
			"closed-failure":     {},
			"reopen-for-cancel":  {"ordinary": {"blocker"}, "conditional": {"blocker"}},
			"closed-canceled":    {},
		},
		"terminal-subjects": {
			"initial":                 {"terminal": {"blocker"}, "pinned": {"blocker"}},
			"pinned-subject":          {"terminal": {"blocker"}},
			"forced-terminal-subject": {},
		},
		"mixed-chain": {
			"initial":       {"middle": {"root"}, "tail": {"middle"}, "last": {"tail"}},
			"root-closed":   {"tail": {"middle"}, "last": {"tail"}},
			"middle-closed": {"last": {"tail"}},
			"tail-closed":   {},
		},
		"stale-projection": {
			"initial":                       {"dependent": {"blocker"}},
			"stale-false-recomputed":        {"dependent": {"blocker"}},
			"stale-false-recomputed-second": {"dependent": {"blocker"}},
		},
	}
	want, exists := expectations[plan][label]
	if !exists || want == nil {
		t.Fatalf("missing explicit native blocked expectation for %s/%s", plan, label)
	}
	return want
}

type b02NodeObservation struct{ ID, Title, Status, Parent string }

type b02Phase struct {
	Nodes            map[string]b02NodeObservation
	Ready            []string
	BlockedBy        map[string][]string
	ExplainReady     map[string]string
	ExplainBlockedBy map[string][]string
}

type b02Capture struct {
	Plan   b02Plan
	Run    string
	Output string
	Phases map[string]b02Phase
}

type b02DatabaseImage struct {
	Schemas map[string][]map[string]any
	Rows    map[string][]map[string]any
	Log     []map[string]any
	Status  []map[string]any
}

func b02Sorted(values []string) []string {
	out := append([]string{}, values...)
	sort.Strings(out)
	return out
}

// The accepted native inventory format reserves the source prefix before the
// first dash and admits only an alphanumeric hash suffix. Keep the authored
// plan name (and its human-readable titles/labels) intact, but encode its
// storage identity without punctuation so native export can census the actual
// rows instead of refusing them as an unsupported issue identity.
func b02ID(plan b02Plan, role string) string {
	return "b02-" + strings.ReplaceAll(plan.Name, "-", "") + role
}

func TestB02NativeBlockingAndCategoryPairs(t *testing.T) {
	var inputs nativeInputs
	nativeDecode(t, nativeB02InputsBytes, &inputs)
	if err := validateB02InputContract(inputs); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(nativeEnv(t, "TEST_UNDECLARED_OUTPUTS_DIR"), "b02-native-")
	if err != nil {
		t.Fatal(err)
	}
	// Registered before the owned servers, so physical cleanup is in the census.
	t.Cleanup(func() {
		entries, err := nativeInventory(root)
		if err != nil {
			t.Errorf("B02 output inventory: %v", err)
		}
		nativeJSON(t, filepath.Join(root, "output-inventory.json"), map[string]any{
			"entries": entries, "complete": err == nil, "passed": !t.Failed(),
			"exclusions":    []string{"poll.log excluded by basename before metadata or hashing", "output-inventory.json self", "SHA256SUMS self"},
			"qualification": "finite actual native observations and public core replay only; no full B02, native admission, hosted delivery or S1 promotion",
		})
		var sums strings.Builder
		for _, entry := range entries {
			if entry["type"] == "file" {
				fmt.Fprintf(&sums, "%s  %s\n", entry["sha256"], entry["path"])
			}
		}
		digest, _, err := nativeDigestFile(filepath.Join(root, "output-inventory.json"))
		if err != nil {
			t.Error(err)
		} else {
			fmt.Fprintf(&sums, "%s  output-inventory.json\n", digest)
		}
		nativeWrite(t, filepath.Join(root, "SHA256SUMS"), []byte(sums.String()))
	})
	binary := nativePrepared(t, root, inputs)
	plans := b02Plans()
	nativeJSON(t, filepath.Join(root, "authored-plan.json"), plans)
	var captures []b02Capture
	for _, plan := range plans {
		var pair []b02Capture
		for _, run := range []string{"a", "b"} {
			var capture b02Capture
			if !t.Run(plan.Name+"/native/"+run, func(t *testing.T) {
				output := filepath.Join(root, plan.Name+"-"+run)
				if err := os.Mkdir(output, 0o700); err != nil {
					t.Fatal(err)
				}
				capture = b02Record(t, binary, output, inputs, plan, run)
			}) {
				t.Fatalf("native %s/%s incomplete; later required phases and replay are NOT_RUN", plan.Name, run)
			}
			pair = append(pair, capture)
			captures = append(captures, capture)
		}
		equal := reflect.DeepEqual(pair[0].Phases, pair[1].Phases)
		nativeJSON(t, filepath.Join(root, plan.Name+"-semantic-twins.json"), map[string]any{
			"equal": equal, "runA": pair[0], "runB": pair[1],
			"projection": "actual identity/title/status/parent, ready membership, blocked reason IDs and ready explanation; original timestamps, database bytes and command streams stay separate and unmodified",
		})
		if !equal {
			t.Fatalf("%s semantic twin observations differ", plan.Name)
		}
	}
	harness := newPostgresHarness(t)
	for _, capture := range captures {
		if capture.Plan.NativeOnly != "" {
			nativeJSON(t, filepath.Join(capture.Output, "replay-boundary.json"), map[string]any{"parity": nil, "reason": capture.Plan.NativeOnly, "replayExecuted": false})
			continue
		}
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			if !t.Run(capture.Plan.Name+"/"+capture.Run+"/replay/"+engine, func(t *testing.T) {
				dsn := ""
				if engine == "postgres" {
					dsn = harness.dsn(t)
				}
				b02ReplayCapture(t, capture, engine, dsn)
			}) {
				t.Fatalf("public replay incomplete for %s/%s/%s", capture.Plan.Name, capture.Run, engine)
			}
		}
	}
}

func b02Record(t *testing.T, binary, output string, inputs nativeInputs, plan b02Plan, run string) b02Capture {
	t.Helper()
	server := publicnative.StartDolt(t, output)
	workspace := t.TempDir()
	home := filepath.Join(workspace, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	env := server.ChildEnv([]string{
		"PATH=" + filepath.Dir(server.DoltBinary) + ":/usr/bin:/bin", "HOME=" + home, "USERPROFILE=" + home,
		"XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "USER=public-native-b02", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1",
	})
	c := &nativeCommand{t: t, binary: binary, workspace: workspace, output: output, env: env}
	t.Cleanup(func() { b02ExportStates(t, output, "cleanup") })
	var version struct{ Commit, Build string }
	nativeDecode(t, c.run("version", "version", "--json"), &version)
	if version.Commit != inputs.Source || version.Build != "public-"+inputs.Source {
		t.Fatal("B02 executable linked source differs from the accepted preparation")
	}
	database := "b02_" + strings.ReplaceAll(plan.Name, "-", "_") + "_" + run
	c.run("init", "init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", "b02", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive")
	physical := server.RequireDatabaseDir(t, database)
	info, err := os.Lstat(physical)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("B02 physical database: %v", err)
	}
	nativeJSON(t, filepath.Join(output, "physical-database.json"), map[string]any{"path": physical, "identity": fmt.Sprint(info.Sys()), "serverPID": server.PID, "port": server.Port, "dataRoot": server.DataRoot, "childEnv": env})
	// A successful pre-custom baseline stays immutable beside the final capture.
	b02Export(t, c, "baseline", true, false)
	statuses := map[string]string{}
	for index, node := range plan.Nodes {
		args := []string{"create", "--id", b02ID(plan, node.Role), "--title", "B02 " + plan.Name + " " + node.Role, "--type", "task", "--force", "--priority", strconv.Itoa(index % 5), "--status", "open", "--json"}
		if node.Ephemeral {
			args = append(args, "--ephemeral")
		}
		if node.Spawner != "" {
			args = append(args, "--waits-for", b02ID(plan, node.Spawner), "--waits-for-gate", node.Gate)
		}
		c.run("create-"+node.Role, args...)
		statuses[node.Role] = "open"
	}
	for _, node := range plan.Nodes {
		if node.Parent != "" {
			c.run("parent-"+node.Role, "update", b02ID(plan, node.Role), "--parent", b02ID(plan, node.Parent), "--json")
		}
	}
	for index, edge := range plan.Edges {
		c.run(fmt.Sprintf("edge-%d", index), "dep", "add", b02ID(plan, edge.From), b02ID(plan, edge.To), "--type", edge.Kind, "--json")
	}
	if plan.CustomStatuses {
		c.run("custom-statuses", "config", "set", "status.custom", "b02active:active,b02done:done,b02frozen:frozen")
	}
	capture := b02Capture{Plan: plan, Run: run, Output: output, Phases: map[string]b02Phase{}}
	capture.Phases["initial"] = b02Observe(t, c, plan, "initial", statuses, plan.InitialReady)
	for _, step := range plan.Steps {
		parent := t
		if !t.Run(step.Name, func(t *testing.T) {
			c.t = t
			defer func() { c.t = parent }()
			before := b02Census(t, c, step.Name+"-before-action")
			args := []string{}
			id := b02ID(plan, step.Role)
			switch step.Action {
			case "status":
				args = []string{"update", id, "--status", step.NativeStatus, "--json"}
			case "close", "reopen":
				args = []string{step.Action, id, "--reason", "B02 " + step.Name, "--json"}
				if step.Force {
					args = append(args, "--force")
				}
			case "self":
				args = []string{"dep", "add", id, id, "--type", "blocks", "--json"}
			case "unlink":
				args = []string{"dep", "remove", id, b02ID(plan, step.Target), "--json"}
			case "also":
				b02StoredMutation(t, c, before, step, id)
			case "stale":
				b02StoredMutation(t, c, before, step, id)
			default:
				t.Fatalf("unknown authored native action %s", step.Action)
			}
			if len(args) != 0 {
				if step.NativeRefusal != "" {
					c.refuse(step.Name, step.NativeRefusal, args...)
				} else {
					c.run(step.Name, args...)
				}
			}
			after := b02Census(t, c, step.Name+"-after-action")
			if step.NativeRefusal != "" && !bytes.Equal(b02Bytes(t, before), b02Bytes(t, after)) {
				t.Fatal("native refusal changed complete schema/row/log/working-status image")
			}
			if step.NativeRefusal == "" {
				switch step.Action {
				case "status":
					statuses[step.Role] = step.NativeStatus
				case "close":
					statuses[step.Role] = "closed"
				case "reopen":
					statuses[step.Role] = "open"
				}
			}
			capture.Phases[step.Name] = b02Observe(t, c, plan, step.Name, statuses, step.NativeReady)
			if step.Action == "stale" {
				b02CheckRepair(t, c, plan, step, before, after)
			}
		}) {
			t.Fatalf("native step %s is incomplete; following steps are NOT_RUN", step.Name)
		}
		c.t = t
	}
	b02Export(t, c, "final", false, plan.CustomStatuses)
	nativeJSON(t, filepath.Join(output, "observed-projection.json"), capture)
	if err := server.Stop(); err != nil {
		t.Fatalf("owned B02 producer stop: %v", err)
	}
	t.Logf("B02_NATIVE_CAPTURE case=%s run=%s phases=%d commands=%d output=%s", plan.Name, run, len(capture.Phases), c.count, output)
	return capture
}

func b02SQLRows(t *testing.T, c *nativeCommand, label, query string) []map[string]any {
	t.Helper()
	rows := []map[string]any{}
	nativeDecode(t, c.run(label, "sql", query, "--json"), &rows)
	// Ordering is a separate comparison representation, never a raw rewrite.
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(b02Bytes(t, rows[i]), b02Bytes(t, rows[j])) < 0 })
	return rows
}

func b02QuoteIdentifier(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
func b02QuoteValue(value string) string     { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

func b02Census(t *testing.T, c *nativeCommand, label string) b02DatabaseImage {
	t.Helper()
	names := b02SQLRows(t, c, label+"-tables", "SHOW TABLES")
	image := b02DatabaseImage{Schemas: map[string][]map[string]any{}, Rows: map[string][]map[string]any{}}
	for index, row := range names {
		if len(row) != 1 {
			t.Fatal("SHOW TABLES must have one actual table-name column")
		}
		var name string
		for _, value := range row {
			name, _ = value.(string)
		}
		if name == "" || image.Rows[name] != nil {
			t.Fatal("empty or duplicate native table name")
		}
		prefix := fmt.Sprintf("%s-table-%d", label, index)
		image.Schemas[name] = b02SQLRows(t, c, prefix+"-schema", "SHOW CREATE TABLE "+b02QuoteIdentifier(name))
		image.Rows[name] = b02SQLRows(t, c, prefix+"-rows", "SELECT * FROM "+b02QuoteIdentifier(name))
	}
	if len(image.Rows) == 0 {
		t.Fatal("native application-table census is empty")
	}
	image.Log = b02SQLRows(t, c, label+"-log", "SELECT * FROM dolt_log")
	image.Status = b02SQLRows(t, c, label+"-working-status", "SELECT * FROM dolt_status")
	nativeJSON(t, filepath.Join(c.output, label+"-database.json"), image)
	return image
}

func b02OwnerRow(t *testing.T, image b02DatabaseImage, id string) (string, map[string]any) {
	t.Helper()
	var table string
	var found map[string]any
	for name, rows := range image.Rows {
		// The complete census includes derived projections such as
		// ready_issues. Only the native owner planes carry the authoritative
		// source row; a projection with the same id is not a second owner.
		if name != "issues" && name != "wisps" {
			continue
		}
		for _, row := range rows {
			if row["id"] == id && row["title"] != nil {
				if found != nil {
					t.Fatalf("multiple native owner rows for %s", id)
				}
				table, found = name, row
			}
		}
	}
	if found == nil {
		t.Fatalf("native owner row absent for %s", id)
	}
	return table, found
}

func b02Observe(t *testing.T, c *nativeCommand, plan b02Plan, label string, statuses map[string]string, want []string) b02Phase {
	t.Helper()
	b02Census(t, c, label+"-before-reads")
	phase := b02Phase{Nodes: map[string]b02NodeObservation{}, BlockedBy: map[string][]string{}, ExplainReady: map[string]string{}, ExplainBlockedBy: map[string][]string{}}
	roles := map[string]string{}
	for _, node := range plan.Nodes {
		roles[b02ID(plan, node.Role)] = node.Role
		var shown []struct {
			ID, Title, Status string
			Parent            *string
		}
		nativeDecode(t, c.run(label+"-show-"+node.Role, "show", b02ID(plan, node.Role), "--json"), &shown)
		parent := ""
		if len(shown) == 1 && shown[0].Parent != nil {
			parent = *shown[0].Parent
		}
		wantParent := ""
		if node.Parent != "" {
			wantParent = b02ID(plan, node.Parent)
		}
		if len(shown) != 1 || shown[0].ID != b02ID(plan, node.Role) || shown[0].Title != "B02 "+plan.Name+" "+node.Role || shown[0].Status != statuses[node.Role] || parent != wantParent {
			t.Fatalf("native identity/status/parent differs for %s: %+v", node.Role, shown)
		}
		phase.Nodes[node.Role] = b02NodeObservation{shown[0].ID, shown[0].Title, shown[0].Status, parent}
	}
	var ready []struct{ ID string }
	nativeDecode(t, c.run(label+"-ready", "ready", "--json", "--sort", "priority", "--limit", "0", "--include-ephemeral"), &ready)
	seen := map[string]bool{}
	for _, item := range ready {
		role := roles[item.ID]
		if role == "" || seen[role] {
			t.Fatal("native ready includes an unknown or duplicate identity")
		}
		seen[role] = true
		phase.Ready = append(phase.Ready, role)
	}
	phase.Ready = b02Sorted(phase.Ready)
	if !reflect.DeepEqual(phase.Ready, b02Sorted(want)) {
		t.Fatalf("native %s ready=%v want=%v; original streams retained", label, phase.Ready, want)
	}
	wantBlocked := b02ExpectedBlocked(t, plan.Name, label)
	bindBlocked := func(id string, reasons []string, count *int, projection map[string][]string) {
		role := roles[id]
		if role == "" || projection[role] != nil || seen[role] || statuses[role] == "closed" || statuses[role] == "pinned" {
			t.Fatal("native blocked includes a foreign, duplicate, terminal or ready subject")
		}
		if count == nil || *count != len(reasons) || len(reasons) == 0 {
			t.Fatal("native blocked subject lacks its complete nonempty reason count")
		}
		by := []string{}
		reasonSeen := map[string]bool{}
		for _, reason := range reasons {
			if roles[reason] == "" || reasonSeen[reason] {
				t.Fatal("native blocked reason names a foreign or duplicate row")
			}
			reasonSeen[reason] = true
			by = append(by, roles[reason])
		}
		projection[role] = b02Sorted(by)
	}
	var blocked []struct {
		ID             string
		BlockedBy      []string `json:"blocked_by"`
		BlockedByCount *int     `json:"blocked_by_count"`
	}
	nativeDecode(t, c.run(label+"-blocked", "blocked", "--json"), &blocked)
	for _, item := range blocked {
		bindBlocked(item.ID, item.BlockedBy, item.BlockedByCount, phase.BlockedBy)
	}
	if !reflect.DeepEqual(phase.BlockedBy, wantBlocked) {
		t.Fatalf("native %s blocked=%v want=%v; original streams retained", label, phase.BlockedBy, wantBlocked)
	}
	var explanation struct {
		Ready   []struct{ ID, Reason string }
		Blocked []struct {
			ID             string
			BlockedBy      []struct{ ID string } `json:"blocked_by"`
			BlockedByCount *int                  `json:"blocked_by_count"`
		}
		Summary struct {
			Ready   *int `json:"total_ready"`
			Blocked *int `json:"total_blocked"`
		}
	}
	nativeDecode(t, c.run(label+"-explain", "ready", "--json", "--explain", "--limit", "0"), &explanation)
	if explanation.Summary.Ready == nil || explanation.Summary.Blocked == nil || *explanation.Summary.Ready != len(explanation.Ready) || *explanation.Summary.Blocked != len(explanation.Blocked) {
		t.Fatal("native explanation cardinalities disagree with its actual arrays")
	}
	for _, item := range explanation.Ready {
		if roles[item.ID] == "" || item.Reason == "" || phase.ExplainReady[roles[item.ID]] != "" {
			t.Fatal("native explanation lacks a unique bound identity or reason")
		}
		phase.ExplainReady[roles[item.ID]] = item.Reason
	}
	for _, item := range explanation.Blocked {
		ids := []string{}
		for _, reason := range item.BlockedBy {
			ids = append(ids, reason.ID)
		}
		bindBlocked(item.ID, ids, item.BlockedByCount, phase.ExplainBlockedBy)
	}
	// --explain's accepted implementation ignores include-ephemeral; its raw
	// arrays remain evidence, never a replacement for the explicit mixed query.
	// Its blocked branch uses an empty filter and includes BOTH native planes,
	// so it has the same independently authored blocked projection, not ready's.
	if !reflect.DeepEqual(phase.ExplainBlockedBy, wantBlocked) {
		t.Fatalf("native %s explain blocked=%v want=%v; original streams retained", label, phase.ExplainBlockedBy, wantBlocked)
	}
	after := b02Census(t, c, label+"-after-reads")
	ownerTables := map[bool]string{}
	for _, node := range plan.Nodes {
		table, row := b02OwnerRow(t, after, b02ID(plan, node.Role))
		if row["status"] != statuses[node.Role] || row["updated_at"] == nil || row["is_blocked"] == nil {
			t.Fatal("actual native stored row lacks its status/timestamp/blocked projection")
		}
		if prior := ownerTables[node.Ephemeral]; prior != "" && prior != table {
			t.Fatal("same declared native plane has inconsistent actual owner tables")
		}
		ownerTables[node.Ephemeral] = table
	}
	if plan.Name == "mixed-chain" && (ownerTables[false] == "" || ownerTables[true] == "" || ownerTables[false] == ownerTables[true]) {
		t.Fatal("mixed native capture lacks two distinct physical owner tables")
	}
	nativeJSON(t, filepath.Join(c.output, label+"-observed.json"), phase)
	return phase
}

func b02StoredMutation(t *testing.T, c *nativeCommand, before b02DatabaseImage, step b02Step, id string) {
	t.Helper()
	query := ""
	if step.Action == "stale" {
		table, row := b02OwnerRow(t, before, id)
		if fmt.Sprint(row["is_blocked"]) != "1" && row["is_blocked"] != true {
			t.Fatal("stale projection control requires an actually blocked source row")
		}
		query = "UPDATE " + b02QuoteIdentifier(table) + " SET is_blocked=0, updated_at=updated_at WHERE id=" + b02QuoteValue(id)
	} else {
		for table, rows := range before.Rows {
			for _, row := range rows {
				if row["issue_id"] == id && row["type"] == "waits-for" && row["metadata"] != nil {
					if query != "" {
						t.Fatal("also_blocks requires one exact native dependency row")
					}
					query = "UPDATE " + b02QuoteIdentifier(table) + " SET metadata='{" + `"gate":"all-children","also_blocks":true` + "}' WHERE issue_id=" + b02QuoteValue(id) + " AND type='waits-for'"
				}
			}
		}
	}
	if query == "" {
		t.Fatal("native stored seam found no unique actual target")
	}
	var changed struct {
		Rows json.Number `json:"rows_affected"`
	}
	nativeDecode(t, c.run(step.Name+"-actual-sql-seam", "sql", query, "--json"), &changed)
	if changed.Rows.String() != "1" {
		t.Fatal("native stored seam did not change exactly one actual row")
	}
	derivedQuery := ""
	if step.Action == "also" {
		// The pinned c8eb producer recompute marks this open subject blocked
		// while its also_blocks spawner is open, conserving updated_at. Raw
		// metadata SQL bypasses that writer; ready reads the stored flag.
		// Inject the complete fixture state instead of assuming read repair.
		table, row := b02OwnerRow(t, before, id)
		if row["status"] != "open" || (fmt.Sprint(row["is_blocked"]) != "0" && row["is_blocked"] != false) {
			t.Fatal("also_blocks fixture requires an open, previously unblocked subject")
		}
		derivedQuery = "UPDATE " + b02QuoteIdentifier(table) + " SET is_blocked=1, updated_at=updated_at WHERE id=" + b02QuoteValue(id) + " AND is_blocked=0 AND status<>'closed' AND status<>'pinned'"
		nativeDecode(t, c.run(step.Name+"-actual-derived-sql-seam", "sql", derivedQuery, "--json"), &changed)
		if changed.Rows.String() != "1" {
			t.Fatal("also_blocks fixture did not mark exactly one actual subject")
		}
		rows := b02SQLRows(t, c, step.Name+"-derived-source-readback", "SELECT * FROM "+b02QuoteIdentifier(table)+" WHERE id="+b02QuoteValue(id))
		if len(rows) != 1 || (fmt.Sprint(rows[0]["is_blocked"]) != "1" && rows[0]["is_blocked"] != true) {
			t.Fatal("also_blocks fixture did not persist its blocked subject")
		}
		expected := make(map[string]any, len(row))
		for key, value := range row {
			expected[key] = value
		}
		expected["is_blocked"] = rows[0]["is_blocked"]
		if !reflect.DeepEqual(expected, rows[0]) {
			t.Fatal("also_blocks fixture changed unrelated owner cells or updated_at")
		}
	}
	nativeJSON(t, filepath.Join(c.output, step.Name+"-seam-boundary.json"), map[string]any{"query": query, "derivedQuery": derivedQuery, "productWriterParity": nil, "scope": "deliberate native stored-state injection; not a public product mutation proof"})
}

func b02CheckRepair(t *testing.T, c *nativeCommand, plan b02Plan, step b02Step, before, injected b02DatabaseImage) {
	t.Helper()
	_, original := b02OwnerRow(t, before, b02ID(plan, step.Role))
	_, stale := b02OwnerRow(t, injected, b02ID(plan, step.Role))
	if fmt.Sprint(stale["is_blocked"]) != "0" && stale["is_blocked"] != false {
		t.Fatal("actual stale false flag was not persisted before the answering read")
	}
	after := b02Census(t, c, step.Name+"-repaired")
	_, repaired := b02OwnerRow(t, after, b02ID(plan, step.Role))
	if (fmt.Sprint(repaired["is_blocked"]) != "1" && repaired["is_blocked"] != true) || !reflect.DeepEqual(original["updated_at"], repaired["updated_at"]) || !reflect.DeepEqual(stale["updated_at"], repaired["updated_at"]) {
		t.Fatal("answer did not recompute stale flag while conserving updated_at")
	}
	b02Observe(t, c, plan, step.Name+"-second", map[string]string{"blocker": "open", "dependent": "open"}, step.NativeReady)
	stable := b02Census(t, c, step.Name+"-second-complete")
	if !bytes.Equal(b02Bytes(t, after), b02Bytes(t, stable)) {
		t.Fatal("second native ready/blocked/explain reads changed the repaired complete image")
	}
}

func b02ExportStates(t *testing.T, output, stage string) {
	t.Helper()
	states := map[string]any{}
	for _, prefix := range []string{"baseline", "final"} {
		for _, suffix := range []string{"-inventory.json", "-inventory.json.sources", "-dump.tar"} {
			name := prefix + suffix
			states[name] = nativeOutputState(filepath.Join(output, name))
		}
	}
	nativeJSON(t, filepath.Join(output, "export-states-"+stage+".json"), states)
}

func b02Export(t *testing.T, c *nativeCommand, label string, dump, customRefusal bool) {
	t.Helper()
	c.run(label+"-quiesce", "dolt", "commit", "-m", "B02 "+label+" inventory")
	path := filepath.Join(c.output, label+"-inventory.json")
	c.run(label+"-inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", path)
	var inventory struct {
		Version  string
		Quiesced bool
		Clean    bool `json:"working_set_clean"`
		External []struct{ Family, Path, State string }
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	nativeDecode(t, data, &inventory)
	if inventory.Version != "beads-inventory/1" || !inventory.Quiesced || !inventory.Clean || len(inventory.External) == 0 {
		t.Fatal("native export inventory is not complete, quiesced and clean")
	}
	sources := path + ".sources"
	declared := map[string]map[string]any{}
	families := map[string]bool{}
	for _, external := range inventory.External {
		rel, err := filepath.Rel(sources, external.Path)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) || external.Family == "" || families[external.Family] || external.State != "json-config" {
			t.Fatal("native sidecar declaration escapes its scope or has an unexpected family/state")
		}
		rel = filepath.ToSlash(rel)
		if declared[rel] != nil {
			t.Fatal("duplicate native sidecar declaration")
		}
		info, err := os.Lstat(external.Path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("nonregular native sidecar original: %v", err)
		}
		digest, size, err := nativeDigestFile(external.Path)
		if err != nil || size == 0 {
			t.Fatalf("empty or unreadable native sidecar original: %v", err)
		}
		families[external.Family] = true
		declared[rel] = map[string]any{"sha256": digest, "bytes": size}
	}
	closure, err := nativeInventory(sources)
	if err != nil {
		t.Fatal(err)
	}
	files := 0
	for _, entry := range closure {
		if entry["type"] != "file" && entry["type"] != "directory" {
			t.Fatal("nonregular native sidecar closure entry")
		}
		if entry["type"] == "file" {
			files++
			original := declared[entry["path"].(string)]
			if original == nil || original["sha256"] != entry["sha256"] || original["bytes"] != entry["bytes"] {
				t.Fatal("native sidecar bytes differ from exact declared closure")
			}
		}
	}
	if files == 0 || files != len(declared) {
		t.Fatal("native sidecar closure is empty or incomplete")
	}
	nativeJSON(t, filepath.Join(c.output, label+"-sidecar-closure.json"), closure)
	args := []string{"--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", path, "-o", filepath.Join(c.output, label+"-dump.tar")}
	if customRefusal {
		c.refuse(label+"-dump-refused", "unsupported nonempty family custom_statuses", args...)
	} else if dump {
		c.run(label+"-dump", args...)
		_, size, err := nativeDigestFile(filepath.Join(c.output, label+"-dump.tar"))
		if err != nil || size == 0 {
			t.Fatalf("native baseline dump missing or empty: %v", err)
		}
	}
	after, err := nativeInventory(sources)
	if err != nil || !reflect.DeepEqual(closure, after) {
		t.Fatal("native export changed retained sidecar originals")
	}
	b02ExportStates(t, c.output, label)
}

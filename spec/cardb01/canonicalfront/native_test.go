package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type nativeFront struct {
	r           *nativeRecorder
	dir, binary string
	env         []string
	seq         int
}
type nativeFrontOutcome struct {
	Case         frontCase
	Anchor       time.Time
	Phases       map[string]frontVector
	ParentFilter map[string][]string
}

func (n *nativeFront) command(label string, args ...string) []byte {
	n.r.t.Helper()
	n.seq++
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Dir = n.dir
	cmd.Env = n.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	started := time.Now().UTC()
	err := cmd.Run()
	finished := time.Now().UTC()
	exit := 0
	if err != nil {
		exit = -1
		if failure, ok := err.(*exec.ExitError); ok {
			exit = failure.ExitCode()
		}
	}
	stem := fmt.Sprintf("%03d-%s", n.seq, label)
	frontSave(n.r.t, n.r.root, stem+".stdout", stdout.Bytes())
	frontSave(n.r.t, n.r.root, stem+".stderr", stderr.Bytes())
	n.r.writeJSON(filepath.Join(n.r.root, stem+".request.json"), map[string]any{"argv": cmd.Args, "directory": n.dir, "environment": n.env, "exit": exit, "error": fmt.Sprint(err), "stdout_sha256": hashBytes(stdout.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes()), "started": started, "finished": finished})
	if err != nil {
		n.r.t.Fatalf("%s real native exit%d: %v\n%s", label, exit, err, stderr.Bytes())
	}
	return stdout.Bytes()
}
func (n *nativeFront) ready(label string, want []string, extra ...string) []string {
	args := append([]string{"ready", "--json", "--limit", "0"}, extra...)
	raw := n.command(label, args...)
	var items []struct{ ID string }
	if err := json.Unmarshal(raw, &items); err != nil || items == nil {
		n.r.t.Fatalf("native ready array missing %s: %v", raw, err)
	}
	got := make([]string, len(items))
	for i, item := range items {
		if !strings.HasPrefix(item.ID, "b01-") {
			n.r.t.Fatal("unbound native ready ID", item.ID)
		}
		got[i] = strings.TrimPrefix(item.ID, "b01-")
	}
	frontEqual(n.r.t, label, got, want)
	return got
}
func (n *nativeFront) limited(label string, want []string, limit int, extra ...string) {
	args := append([]string{"ready", "--json", "--limit", strconv.Itoa(limit)}, extra...)
	raw := n.command(label, args...)
	var rows []struct{ ID string }
	if err := json.Unmarshal(raw, &rows); err != nil {
		n.r.t.Fatal(err)
	}
	got := make([]string, len(rows))
	for i, row := range rows {
		got[i] = strings.TrimPrefix(row.ID, "b01-")
	}
	count := limit
	if len(want) < count {
		count = len(want)
	}
	frontEqual(n.r.t, label, got, want[:count])
}
func (n *nativeFront) list(label string, want []string, limit int, extra ...string) []string {
	args := append([]string{"list", "--json", "--limit", strconv.Itoa(limit), "--sort", "id"}, extra...)
	raw := n.command(label, args...)
	var items []struct{ ID string }
	if json.Unmarshal(raw, &items) != nil || items == nil {
		n.r.t.Fatalf("native list array missing: %s", raw)
	}
	got := make([]string, len(items))
	for i, item := range items {
		if !strings.HasPrefix(item.ID, "b01-") {
			n.r.t.Fatal("unexpected native list ID", item.ID)
		}
		got[i] = strings.TrimPrefix(item.ID, "b01-")
	}
	count := len(want)
	if limit > 0 && limit < count {
		count = limit
	}
	frontEqual(n.r.t, label, got, want[:count])
	return got
}

func (n *nativeFront) image(label string, c frontCase, phase string, anchor time.Time) map[string][]map[string]any {
	result := map[string][]map[string]any{}
	for _, table := range []string{"issues", "wisps", "dependencies", "wisp_dependencies"} {
		raw := n.command(label+"-"+table, "--readonly", "sql", "SELECT * FROM "+table+" ORDER BY 1,2", "--json")
		var rows []map[string]any
		if err := json.Unmarshal(raw, &rows); err != nil {
			n.r.t.Fatalf("actual native table %s: %v", table, err)
		}
		result[table] = rows
	}
	actual := map[string]string{}
	for _, table := range []string{"issues", "wisps"} {
		for _, row := range result[table] {
			id, ok := row["id"].(string)
			if !ok || actual[id] != "" {
				n.r.t.Fatal("missing/duplicate actual physical ID", row)
			}
			actual[id] = table
		}
	}
	if len(actual) != len(c.Nodes) {
		n.r.t.Fatalf("native import row count %d want%d", len(actual), len(c.Nodes))
	}
	for _, node := range c.Nodes {
		want := "issues"
		if node.Wisp {
			want = "wisps"
		}
		if actual["b01-"+node.Role] != want {
			n.r.t.Fatalf("%s physical placement=%s want%s", node.Role, actual["b01-"+node.Role], want)
		}
		for _, row := range result[want] {
			if row["id"] != "b01-"+node.Role {
				continue
			}
			status := node.Status
			if status == "pinned" {
				status = "open"
			}
			if node.Role == "blocker" && (phase == "unblocked" || phase == "indefinite" || phase == "timed") {
				status = "closed"
			}
			if node.Role == "parent" && (phase == "indefinite" || phase == "timed") {
				status = "deferred"
			}
			if row["status"] != status || row["issue_type"] != node.Kind || row["priority"] != float64(node.Priority) {
				n.r.t.Fatalf("actual source values differ for %s: %+v", node.Role, row)
			}
			created, ok := row["created_at"].(string)
			parsed := false
			for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05.999999999 -0700 MST"} {
				at, err := time.Parse(layout, created)
				if err == nil && at.Equal(anchor.Add(-node.Age)) {
					parsed = true
				}
			}
			if !ok || !parsed {
				n.r.t.Fatalf("actual source creation time mismatch for %s: %+v", node.Role, row["created_at"])
			}
		}

	}
	// Bind every actual parent edge to the source's own dependency table.
	if len(frontParentEdges(c)) != 0 {
		for _, edge := range frontParentEdges(c) {
			table := "dependencies"
			if actual["b01-"+edge[0]] == "wisps" {
				table = "wisp_dependencies"
			}
			target, other := "depends_on_issue_id", "depends_on_wisp_id"
			if actual["b01-"+edge[1]] == "wisps" {
				target, other = other, target
			}
			found := false
			for _, row := range result[table] {
				// The pinned schema has separate durable and ephemeral target
				// columns, not the CLI response's depends_on_id projection.
				if row["issue_id"] == "b01-"+edge[0] && row[target] == "b01-"+edge[1] && row["type"] == "parent-child" {
					if row[other] != nil || row["depends_on_external"] != nil {
						n.r.t.Fatal("parent edge has conflicting physical target arms", row)
					}
					found = true
				}
			}
			if !found {
				n.r.t.Fatalf("actual parent edge %s->%s absent in %s", edge[0], edge[1], table)
			}
		}
		for _, row := range result[actual["b01-parent"]] {
			if row["id"] != "b01-parent" {
				continue
			}
			switch phase {
			case "indefinite":
				if row["status"] != "deferred" || row["defer_until"] != nil {
					n.r.t.Fatalf("indefinite source must have no until: %+v", row)
				}
			case "timed":
				if row["status"] != "deferred" || row["defer_until"] == nil {
					n.r.t.Fatalf("timed source must have actual future until: %+v", row)
				}
			}
		}
	}
	n.r.writeJSON(filepath.Join(n.r.root, label+"-physical-image.json"), map[string]any{"actual_id_planes": actual, "physical_tables": result, "case": c, "phase": phase})
	return result
}
func recordNativeFront(t *testing.T, r *nativeRecorder, server *publicnative.Server, binary, name string, c frontCase, anchor time.Time) nativeFrontOutcome {
	t.Helper()
	workspace := t.TempDir()
	home := filepath.Join(workspace, "home")
	dir := filepath.Join(workspace, "project")
	for _, p := range []string{home, dir, filepath.Join(workspace, "tmp")} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	env := []string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + home, "USER=b01-native", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B01 Public", "GIT_AUTHOR_EMAIL=b01@example.invalid", "GIT_COMMITTER_NAME=B01 Public", "GIT_COMMITTER_EMAIL=b01@example.invalid", "TMPDIR=" + filepath.Join(workspace, "tmp"), "BEADS_ACTOR=b01-recorder", "BEADS_NO_DAEMON=1"}
	n := &nativeFront{r: r, dir: dir, binary: "/usr/bin/git", env: server.ChildEnv(env)}
	n.command("git-init", "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	n.binary = binary
	version := n.command("version", "version", "--json")
	var identity struct{ Commit, Build string }
	if json.Unmarshal(version, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatalf("actual executed binary identity mismatch %s", version)
	}
	database := "b01_" + name + "_" + c.Name
	n.command("init", "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", "b01", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, database)
	n.command("custom-active-config", "config", "set", "status.custom", "b01active:active", "--json")
	n.command("custom-active-readback", "config", "get", "status.custom", "--json")
	// The pinned importer validates custom infrastructure names before ready
	// applies its default hidden-type policy. Retain the real config readback.
	n.command("infrastructure-type-config", "config", "set", "types.custom", "agent", "--json")
	n.command("infrastructure-type-readback", "config", "get", "types.custom", "--json")
	var lines bytes.Buffer
	for _, node := range c.Nodes {
		status := node.Status
		if status == "pinned" {
			status = "open"
		}
		row := map[string]any{"id": "b01-" + node.Role, "title": node.Role, "status": status, "priority": node.Priority, "issue_type": node.Kind, "created_at": anchor.Add(-node.Age).Format(time.RFC3339), "updated_at": anchor.Add(-node.Age).Format(time.RFC3339), "created_by": "b01-public"}
		if node.Status == "pinned" {
			row["pinned"] = true
		}
		if node.Status == "closed" {
			row["closed_at"] = anchor.Add(-node.Age).Format(time.RFC3339)
		}
		if node.Wisp {
			row["wisp_plane"] = true
			row["no_history"] = true
		}
		data, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		lines.Write(data)
		lines.WriteByte('\n')
	}
	fixture := filepath.Join(r.root, "native-fixture.jsonl")
	if err := os.WriteFile(fixture, lines.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	r.writeJSON(filepath.Join(r.root, "fixture-binding.json"), map[string]any{"fixture_sha256": hashBytes(lines.Bytes()), "anchor": anchor, "clock_cutoff_distance_minimum": "45 hours recent /24 hours old", "native_default_sort": "priority", "explicit_hybrid_sort": "hybrid", "fixture": c})
	n.command("import-fixture", "import", "-i", fixture, "--json")
	if c.Name == "category_and_blocked_front" {
		n.command("block-front", "dep", "add", "b01-blocked", "b01-blocker", "--type", "blocks", "--json")
	}
	for _, edge := range frontParentEdges(c) {
		n.command("parent-"+edge[0]+"-"+edge[1], "dep", "add", "b01-"+edge[0], "b01-"+edge[1], "--type", "parent-child", "--json")
	}
	if c.Name == "parent_selection" {
		n.command("block-descendant-outside-subtree", "dep", "add", "b01-blocked-child", "b01-blocker", "--type", "blocks", "--json")
	}
	outcome := nativeFrontOutcome{Case: c, Anchor: anchor, Phases: map[string]frontVector{}, ParentFilter: map[string][]string{}}
	for _, phase := range frontPhases(c) {
		switch phase {
		case "blocked":
			n.command("block-ancestor", "dep", "add", "b01-parent", "b01-blocker", "--type", "blocks", "--json")
		case "unblocked":
			n.command("close-ancestor-blocker", "close", "b01-blocker", "--reason", "restore parent and descendant readiness", "--json")
		case "indefinite":
			n.command("indefinite-parent", "defer", "b01-parent", "--reason", "distinct indefinite vector", "--json")
		case "timed":
			n.command("timed-parent", "defer", "b01-parent", "--until", anchor.Add(7*24*time.Hour).Format(time.RFC3339), "--reason", "distinct future timed vector", "--json")
		}
		before := n.image(phase+"-before-ready", c, phase, anchor)
		want := frontWant(c.Name, phase)
		gotDefault := n.ready(phase+"-literal-default", want.Default)
		n.ready(phase+"-repeat-default", want.Default)
		gotHybrid := n.ready(phase+"-explicit-hybrid", want.Hybrid, "--sort", "hybrid")
		n.ready(phase+"-repeat-hybrid", want.Hybrid, "--sort", "hybrid")
		n.limited(phase+"-limit1", want.Default, 1)
		n.limited(phase+"-limit2", want.Default, 2)
		if c.Name == "parent_selection" {
			outcome.ParentFilter["baseline"] = n.ready("parent-selection", []string{"child", "grandchild", "greatgrandchild", "priority-only"}, "--parent", "b01-parent")
			outcome.ParentFilter["priority1"] = n.ready("parent-priority1", []string{"child", "grandchild"}, "--parent", "b01-parent", "--priority", "1")
			outcome.ParentFilter["priority2"] = n.ready("parent-priority2", []string{"greatgrandchild", "priority-only"}, "--parent", "b01-parent", "--priority", "2")
			n.ready("parent-blocked-hidden-priority0", []string{}, "--parent", "b01-parent", "--priority", "0")
			n.ready("parent-other-tree", []string{"other-child"}, "--parent", "b01-other-root")
			n.ready("parent-unknown", []string{}, "--parent", "b01-missing")
			listWant := []string{"blocked-child", "child", "grandchild", "greatgrandchild", "priority-only"}
			outcome.ParentFilter["list"] = n.list("list-parent-all-task-descendants", listWant, 0, "--parent", "b01-parent", "--type", "task")
			outcome.ParentFilter["list-priority2"] = n.list("list-parent-priority2", []string{"greatgrandchild", "priority-only"}, 0, "--parent", "b01-parent", "--priority", "2")
			n.list("list-unknown-parent", []string{}, 0, "--parent", "b01-missing")
			for _, limit := range []int{1, 2} {
				n.limited(fmt.Sprintf("parent-prefix-limit%d", limit), outcome.ParentFilter["baseline"], limit, "--parent", "b01-parent")
				n.limited(fmt.Sprintf("parent-priority-prefix-limit%d", limit), outcome.ParentFilter["priority2"], limit, "--parent", "b01-parent", "--priority", "2")
				n.list(fmt.Sprintf("list-parent-prefix-limit%d", limit), listWant, limit, "--parent", "b01-parent", "--type", "task")
			}
		}
		if phase != "baseline" {
			parentWant := frontParentWant(phase).Default
			outcome.ParentFilter[phase] = n.ready(phase+"-transitive-parent-filter", parentWant, "--parent", "b01-parent")
			n.ready(phase+"-parent-filter-hybrid", frontParentWant(phase).Hybrid, "--parent", "b01-parent", "--sort", "hybrid")
			n.limited(phase+"-parent-limit1", parentWant, 1, "--parent", "b01-parent")
			n.limited(phase+"-parent-limit2", parentWant, 2, "--parent", "b01-parent")
			n.ready(phase+"-parent-filter-priority1", parentWant, "--parent", "b01-parent", "--priority", "1")
			n.ready(phase+"-parent-filter-type", parentWant, "--parent", "b01-parent", "--type", "task")
			n.ready(phase+"-unknown-parent", []string{}, "--parent", "b01-missing")
		}
		after := n.image(phase+"-after-ready", c, phase, anchor)
		if !reflect.DeepEqual(before, after) {
			t.Fatal("native ready changed actual issues/wisps/dependency rows")
		}
		outcome.Phases[phase] = frontVector{Default: gotDefault, Hybrid: gotHybrid}
	}
	r.writeJSON(filepath.Join(r.root, "native-outcome.json"), outcome)
	return outcome
}
func TestB01CanonicalNativeReadyAndPublicCounterparts(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute output directory required")
	}
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(checkout) {
		t.Fatal("runner source root required")
	}
	r := &nativeRecorder{t: t, root: root}
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if os.Getenv("PLUG_B01_REQUEST_SHA") == "" || actual != os.Getenv("PLUG_B01_REQUEST_SHA") {
		t.Fatal("exact candidate request mismatch")
	}
	protocol, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(protocol) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("existing canonical source build protocol changed")
	}
	var input struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(protocol, &input) != nil || input.ExporterLabel == "" {
		t.Fatal("canonical executable target missing")
	}
	r.target = input.ExporterLabel
	r.writeJSON(filepath.Join(root, "request-run-binding.json"), map[string]any{"candidate": actual, "target": os.Getenv("TEST_TARGET"), "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "native_source": pinnedSource, "official_producer_admission": "NOT_REQUESTED", "qualification": "PUBLIC_OPERATIONAL_REFERENCE_ONLY"})
	binary := r.buildCanonicalSource(pinnedSource)
	anchor := time.Now().UTC().Truncate(time.Second)
	twins := map[string]map[string]nativeFrontOutcome{}
	for _, name := range []string{"a", "b"} {
		t.Run("native_twin_"+name, func(t *testing.T) {
			serverDir := filepath.Join(root, "native-"+name)
			if err := os.MkdirAll(serverDir, 0700); err != nil {
				t.Fatal(err)
			}
			server := publicnative.StartDolt(t, serverDir)
			twins[name] = map[string]nativeFrontOutcome{}
			for _, c := range frontCases() {
				t.Run(c.Name, func(t *testing.T) {
					dir := filepath.Join(serverDir, c.Name)
					if err := os.Mkdir(dir, 0700); err != nil {
						t.Fatal(err)
					}
					twins[name][c.Name] = recordNativeFront(t, &nativeRecorder{t: t, root: dir, target: r.target}, server, binary, name, c, anchor)
				})
			}
			if err := server.Stop(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if len(twins["a"]) != len(frontCases()) || !reflect.DeepEqual(twins["a"], twins["b"]) {
		t.Fatal("actual independent native twins incomplete or ordered vectors diverged")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, c := range frontCases() {
			t.Run("paired_"+engine+"/"+c.Name, func(t *testing.T) {
				dir := filepath.Join(root, "paired-"+engine, c.Name)
				actualPublic := runPublicFront(t, engine, dir, c, anchor)
				native := twins["a"][c.Name]
				for _, phase := range frontPhases(c) {
					frontEqual(t, "actual public "+phase, actualPublic[phase], frontWant(c.Name, phase).Plug)
					if phase != "indefinite" && phase != "timed" {
						frontEqual(t, "actual native explicit hybrid counterpart "+phase, actualPublic[phase], native.Phases[phase].Hybrid)
					}
				}
				for key, actualNative := range native.ParentFilter {
					if key != "indefinite" && key != "timed" {
						frontEqual(t, "actual native/public parent "+key, actualPublic["parent:"+key], actualNative)
					}
				}
				frontSave(t, dir, "paired-comparison.json", frontJSON(t, map[string]any{"native": native, "public_actual": actualPublic, "native_default_preserved": true, "native_explicit_hybrid_compared": true, "adopted_indefinite_and_timed_transitive_vectors_separate": true, "native_wisp_mapped_to_ordinary_public_durable_semantics": true, "plug_ephemeral_plane_qualification": "NOT_CLAIMED", "parent_filter_public_capability": "TRANSITIVE_STORAGE_INDEX_BEFORE_ORDER_AND_PAGINATION", "clock_expiry_or_background_rank_SLA": "NOT_CLAIMED"}))
			})
		}
	}
	r.writeJSON(filepath.Join(root, "native-twin-vectors.json"), twins)
}

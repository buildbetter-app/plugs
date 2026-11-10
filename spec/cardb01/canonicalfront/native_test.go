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
	ParentFilter []string
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
func (n *nativeFront) limited(label string, want []string, limit int) {
	raw := n.command(label, "ready", "--json", "--limit", strconv.Itoa(limit))
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
func (n *nativeFront) image(label string, c frontCase, phase string) map[string][]map[string]any {
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
	}
	// Bind every actual parent edge to the source's own dependency table.
	if len(frontPhases(c)) > 1 {
		for _, edge := range []struct {
			Child, Parent string
			Wisp          bool
		}{{"child", "parent", c.ChildWisp}, {"grandchild", "child", !c.ChildWisp}} {
			table := "dependencies"
			if edge.Wisp {
				table = "wisp_dependencies"
			}
			found := false
			for _, row := range result[table] {
				if row["issue_id"] != "b01-"+edge.Child || row["type"] != "parent-child" {
					continue
				}
				for _, value := range row {
					if value == "b01-"+edge.Parent {
						found = true
					}
				}
			}
			if !found {
				n.r.t.Fatalf("actual parent edge %s->%s absent in %s", edge.Child, edge.Parent, table)
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
	if len(frontPhases(c)) > 1 {
		n.command("parent-child", "dep", "add", "b01-child", "b01-parent", "--type", "parent-child", "--json")
		n.command("child-grandchild", "dep", "add", "b01-grandchild", "b01-child", "--type", "parent-child", "--json")
	}
	outcome := nativeFrontOutcome{Case: c, Anchor: anchor, Phases: map[string]frontVector{}}
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
		before := n.image(phase+"-before-ready", c, phase)
		want := frontWant(c.Name, phase)
		gotDefault := n.ready(phase+"-literal-default", want.Default)
		n.ready(phase+"-repeat-default", want.Default)
		gotHybrid := n.ready(phase+"-explicit-hybrid", want.Hybrid, "--sort", "hybrid")
		n.ready(phase+"-repeat-hybrid", want.Hybrid, "--sort", "hybrid")
		n.limited(phase+"-limit1", want.Default, 1)
		n.limited(phase+"-limit2", want.Default, 2)
		if phase == "open" {
			outcome.ParentFilter = n.ready("transitive-parent-filter", []string{"child", "grandchild"}, "--parent", "b01-parent")
		}
		after := n.image(phase+"-after-ready", c, phase)
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
				frontSave(t, dir, "paired-comparison.json", frontJSON(t, map[string]any{"native": native, "public_actual": actualPublic, "native_default_preserved": true, "native_explicit_hybrid_compared": true, "adopted_indefinite_and_timed_transitive_vectors_separate": true, "native_wisp_mapped_to_ordinary_public_durable_semantics": true, "plug_ephemeral_plane_qualification": "NOT_CLAIMED", "parent_filter_public_capability": "NO_PARENT_FIELD_IN_SERVICE_FILTER_NOT_EMULATED", "clock_expiry_or_background_rank_SLA": "NOT_CLAIMED"}))
			})
		}
	}
	r.writeJSON(filepath.Join(root, "native-twin-vectors.json"), twins)
}

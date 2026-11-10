package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type b07Native struct {
	t                 *testing.T
	root, dir, binary string
	env               []string
	seq               int
	tables            []string
}
type b07NativeImage map[string][]map[string]json.RawMessage

func (n *b07Native) command(label string, fail bool, args ...string) []byte {
	n.t.Helper()
	n.seq++
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Dir = n.dir
	cmd.Env = append([]string(nil), n.env...)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		code = -2
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			code = exit.ExitCode()
		}
	}
	stem := filepath.Join(n.root, fmt.Sprintf("%04d-%s", n.seq, label))
	for suffix, data := range map[string][]byte{"stdout": out.Bytes(), "stderr": stderr.Bytes()} {
		if e := os.WriteFile(stem+"."+suffix, data, 0600); e != nil {
			n.t.Fatal(e)
		}
	}
	b07JSON(n.t, stem+".request.json", map[string]any{"argv": cmd.Args, "directory": cmd.Dir, "environment": cmd.Env, "exit": code, "error": fmt.Sprint(err), "stdout_sha256": b07Hash(out.Bytes()), "stderr_sha256": b07Hash(stderr.Bytes())})
	if ctx.Err() != nil || code < 0 || (!fail && code != 0) || (fail && code == 0) {
		n.t.Fatalf("native %s exit=%d error=%v stderr=%s", label, code, err, stderr.Bytes())
	}
	return out.Bytes()
}
func (n *b07Native) batch(label, script string, fail bool) {
	n.t.Helper()
	path := filepath.Join(n.root, fmt.Sprintf("script-%04d-%s.txt", n.seq, label))
	if e := os.WriteFile(path, []byte(script), 0600); e != nil {
		n.t.Fatal(e)
	}
	n.command(label, fail, "batch", "--file", path, "--json")
}
func (n *b07Native) create(id string) {
	n.command("create-"+id, false, "create", "--id", id, "--type", "task", "--title", id, "--json")
}
func (n *b07Native) graph(label string, nodes, edges []map[string]any, fail bool) {
	path := filepath.Join(n.root, label+".plan.json")
	b07JSON(n.t, path, map[string]any{"nodes": nodes, "edges": edges})
	n.command(label, fail, "create", "--graph", path, "--json")
}
func (n *b07Native) image(label string) b07NativeImage {
	n.t.Helper()
	image := b07NativeImage{}
	for _, table := range n.tables {
		raw := n.command(label+"-"+table, false, "--readonly", "sql", "SELECT * FROM `"+table+"`", "--json")
		var rows []map[string]json.RawMessage
		if e := json.Unmarshal(raw, &rows); e != nil {
			n.t.Fatal(e)
		}
		sort.Slice(rows, func(i, j int) bool {
			a, _ := json.Marshal(rows[i])
			b, _ := json.Marshal(rows[j])
			return string(a) < string(b)
		})
		image[table] = rows
	}
	b07JSON(n.t, filepath.Join(n.root, fmt.Sprintf("image-%04d-%s.json", n.seq, label)), image)
	return image
}
func (n *b07Native) equal(want b07NativeImage, label string) {
	n.t.Helper()
	got := n.image(label)
	if !reflect.DeepEqual(got, want) {
		n.t.Fatal("native no-op/refusal changed complete current user-table state")
	}
}
func (n *b07Native) ready(label string) []string {
	raw := n.command(label, false, "ready", "--json", "--limit", "0", "--include-ephemeral")
	var rows []struct {
		ID string `json:"id"`
	}
	if e := json.Unmarshal(raw, &rows); e != nil {
		n.t.Fatal(e)
	}
	out := []string{}
	for _, row := range rows {
		out = append(out, row.ID)
	}
	sort.Strings(out)
	return out
}
func (n *b07Native) requireReady(label string, want []string) {
	n.t.Helper()
	got := n.ready(label)
	if !reflect.DeepEqual(got, want) {
		n.t.Fatalf("native ready got=%v want=%v", got, want)
	}
}
func (n *b07Native) edge(image b07NativeImage, from, to string) map[string]json.RawMessage {
	n.t.Helper()
	var match map[string]json.RawMessage
	for _, rows := range image {
		for _, row := range rows {
			raw, ok := row["issue_id"]
			if !ok || b07Text(n.t, raw) != from {
				continue
			}
			target := false
			for key, v := range row {
				if strings.HasPrefix(key, "depends_on_") && string(v) != "null" && b07Text(n.t, v) == to {
					target = true
				}
			}

			if !target {
				continue
			}
			if _, ok := row["type"]; !ok {
				continue
			}
			if match != nil {
				n.t.Fatal("multiple native edges for endpoint pair")
			}
			match = row
		}
	}
	if match == nil {
		n.t.Fatal("native edge absent")
	}
	return match
}

func b07NewNative(t *testing.T, binary, name string) *b07Native {
	root := b07Output(t, "native-"+name)
	workspace := t.TempDir()
	dir := filepath.Join(workspace, "project")
	home := filepath.Join(workspace, "home")
	tmp := filepath.Join(workspace, "tmp")
	for _, p := range []string{dir, home, tmp} {
		if e := os.Mkdir(p, 0700); e != nil {
			t.Fatal(e)
		}
	}
	server := publicnative.StartDolt(t, root)
	n := &b07Native{t: t, root: root, dir: dir, binary: binary, env: server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + home, "USER=b07-native", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B07 Public", "GIT_AUTHOR_EMAIL=b07@example.invalid", "GIT_COMMITTER_NAME=B07 Public", "GIT_COMMITTER_EMAIL=b07@example.invalid", "TMPDIR=" + tmp})}
	n.binary = "/usr/bin/git"
	n.command("git-init", false, "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	n.binary = binary
	raw := n.command("version", false, "version", "--json")
	var identity struct{ Commit, Build string }
	if json.Unmarshal(raw, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatal("native executable identity differs from unchanged canonical build recipe")
	}
	n.command("init", false, "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b07_current", "--prefix", "b07", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, "b07_current")
	n.command("journal-enable", false, "config", "set", "events-journal", "true", "--json")
	n.command("journal-prune-disable", false, "config", "set", "events-journal-auto-prune", "false", "--json")
	raw = n.command("actual-table-catalog", false, "--readonly", "sql", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'b07_current' AND table_type = 'BASE TABLE' ORDER BY table_name", "--json")
	var tables []map[string]string
	if e := json.Unmarshal(raw, &tables); e != nil {
		t.Fatal(e)
	}
	for _, row := range tables {
		table := row["table_name"]
		if table == "" {
			table = row["TABLE_NAME"]
		}
		if strings.HasPrefix(table, "dolt_") {
			continue
		}
		if table == "" {
			t.Fatal("table catalog omitted name")
		}
		for _, c := range table {
			if c != '_' && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
				t.Fatal("unsafe table identifier")
			}
		}
		n.tables = append(n.tables, table)
	}
	if len(n.tables) == 0 {
		t.Fatal("no native current user tables captured")
	}
	b07JSON(t, filepath.Join(root, "actual-tables.json"), n.tables)
	return n
}

func (n *b07Native) vocabulary() []b07Observation {
	observed := []b07Observation{}
	for i, item := range b07Cases() {
		if !n.t.Run(item.Name, func(t *testing.T) {
			old := n.t
			n.t = t
			defer func() { n.t = old }()
			from, to := fmt.Sprintf("b07-%d", i*3+1), fmt.Sprintf("b07-%d", i*3+2)
			n.create(from)
			n.create(to)
			if item.Kind == "waits-for" {
				child := fmt.Sprintf("b07-%d", i*3+3)
				n.create(child)
				n.batch("spawner-child", "dep add "+child+" "+to+" parent-child\n", false)
			}
			baseline := n.ready("before-ready")
			if !b07Has(baseline, from) || !b07Has(baseline, to) {
				t.Fatal("native fresh endpoints not ready")
			}
			before := n.image("before-link")
			script := "dep add " + from + " " + to + " " + strconv.Quote(item.Kind) + "\n"
			n.batch("link", script, !item.Valid)
			o := b07Observation{Name: item.Name, Kind: item.Kind, From: from, To: to, Accepted: item.Valid}
			if !item.Valid {
				n.equal(before, "refusal-after")
				n.requireReady("refusal-ready", baseline)
				observed = append(observed, o)
				return
			}
			image := n.image("after-link")
			row := n.edge(image, from, to)
			if b07Text(t, row["type"]) != item.Kind {
				t.Fatal("native kind was trimmed or rewritten")
			}
			ready := n.ready("after-ready")
			o.SourceReady = b07Has(ready, from)
			o.TargetReady = b07Has(ready, to)
			blocked := item.Kind == "blocks" || item.Kind == "conditional-blocks" || item.Kind == "waits-for"
			want := baseline
			if blocked {
				want = b07Without(want, from)
			}
			if !reflect.DeepEqual(ready, want) {
				t.Fatalf("native current readiness differs for %s: %v vs %v", item.Kind, ready, want)
			}
			n.batch("same-kind-repeat", script, false)
			n.equal(image, "repeat-after")
			o.SameKindQuiet = true
			different := "blocks"
			if item.Kind == "blocks" {
				different = "related"
			}
			n.batch("retype-refusal", "dep add "+from+" "+to+" "+different+"\n", true)
			n.equal(image, "retype-after")
			o.RetypeRefused = true
			// Guarded public close; batch close intentionally has different source semantics.
			n.command("guarded-close", blocked, "close", from, "--json")
			if blocked {
				n.equal(image, "guard-after")
				n.requireReady("guard-ready", ready)
			} else {
				o.CloseAllowed = true
				n.requireReady("closed-ready", b07Without(ready, from))
			}
			observed = append(observed, o)
		}) {
			n.t.Fatal("native vocabulary unit incomplete")
		}
	}
	b07JSON(n.t, filepath.Join(n.root, "vocabulary-outcomes.json"), observed)
	return observed
}

func (n *b07Native) threadAndMixed() map[string]string {
	n.create("b07-900")
	n.create("b07-901")
	edges := []map[string]any{{"from_id": "b07-900", "to_id": "b07-901", "type": "replies-to", "thread_id": "thread \"α\" \\ tail"}}
	for i, thread := range []*string{nil, new(string)} {
		label := fmt.Sprintf("thread-initial-%d", i)
		if i == 1 {
			delete(edges[0], "thread_id")
		}
		n.graph(label, []map[string]any{{"key": "aux", "id": fmt.Sprintf("b07-%d", 910+i), "title": label}}, edges, false)
		_ = thread
	}
	initial := n.image("thread-omitted-retained")
	row := n.edge(initial, "b07-900", "b07-901")
	token, ok := row["thread_id"]
	if !ok || b07Text(n.t, token) != "thread \"α\" \\ tail" {
		n.t.Fatal("native omitted thread did not preserve value")
	}
	edges[0]["thread_id"] = ""
	n.graph("thread-empty", []map[string]any{{"key": "aux", "id": "b07-912", "title": "thread empty"}}, edges, false)
	if !bytes.Equal(n.edge(n.image("thread-empty-after"), "b07-900", "b07-901")["thread_id"], token) {
		n.t.Fatal("native empty thread changed stored thread")
	}
	edges[0]["thread_id"] = "next-thread"
	n.graph("thread-changed", []map[string]any{{"key": "aux", "id": "b07-913", "title": "thread changed"}}, edges, false)
	if b07Text(n.t, n.edge(n.image("thread-changed-after"), "b07-900", "b07-901")["thread_id"]) != "next-thread" {
		n.t.Fatal("native explicit thread replacement not observed")
	}
	nodes := []map[string]any{{"key": "durable", "id": "b07-920", "title": "mixed durable"}, {"key": "volatile", "id": "b07-921", "title": "mixed volatile", "storage_class": "ephemeral"}}
	n.graph("mixed-create", nodes, []map[string]any{{"from_key": "durable", "to_key": "volatile", "type": "related"}}, false)
	n.batch("mixed-reverse-edge", "dep add b07-921 b07-920 tracks\n", false)
	image := n.image("mixed-before-refusal")
	n.batch("mixed-atomic-refusal", "update b07-920 title=must-rollback\nupdate b07-921 title=must-rollback\ndep add b07-920 b07-921 blocks\n", true)
	n.equal(image, "mixed-after-refusal")
	// This map is the actual readonly SQL response token, never a request echo.
	return map[string]string{"beads.thread_id": string(token)}
}

func TestB07CanonicalNativeEdgeCounterparts(t *testing.T) {
	root := b07Output(t, "build")
	r := &nativeRecorder{t: t, root: root}
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	for _, key := range []string{"PLUG_B07_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing actual run binding %s", key)
		}
	}
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if actual != os.Getenv("PLUG_B07_REQUEST_SHA") {
		t.Fatal("candidate request mismatch")
	}
	r.buildCommand(checkout, "candidate-tree-parents", "/usr/bin/git", []string{"show", "-s", "--format=%H %T %P", "HEAD"})
	raw, e := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if e != nil || hashBytes(raw) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("canonical protocol differs")
	}
	var protocol struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(raw, &protocol) != nil || protocol.ExporterLabel == "" {
		t.Fatal("canonical target absent")
	}
	r.target = protocol.ExporterLabel
	b07JSON(t, filepath.Join(root, "run-binding.json"), map[string]any{"candidate": actual, "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "native_source": pinnedSource, "qualification": "PUBLIC_OPERATIONAL_RECORDING_ONLY"})
	binary := r.buildCanonicalSource(pinnedSource)
	var twins [][]b07Observation
	var nativeTokens map[string]string
	for _, name := range []string{"a", "b"} {
		if !t.Run("native_"+name, func(t *testing.T) {
			n := b07NewNative(t, binary, name)
			twins = append(twins, n.vocabulary())
			tokens := n.threadAndMixed()
			n.externalAndMetadata()
			if nativeTokens == nil {
				nativeTokens = tokens
			} else if !reflect.DeepEqual(nativeTokens, tokens) {
				t.Fatal("independent native token observations differ")
			}
		}) {
			t.Fatal("native counterpart incomplete")
		}
	}
	// IDs are explicit, while timestamps and physical identity remain in raw originals.
	if !reflect.DeepEqual(twins[0], twins[1]) {
		t.Fatal("native stable vocabulary projection differs")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("paired_"+engine, func(t *testing.T) {
			b07Compare(t, twins[0], b07Public(t, engine))
			b07OpaquePublic(t, engine, nativeTokens)
			b07OpaqueImport(t, engine, nativeTokens)
			b07MixedPublic(t, engine)
		})
	}
}

func (n *b07Native) externalAndMetadata() {
	n.create("b07-950")
	before := n.image("narrow-custom-before")
	n.command("standalone-custom-refusal", true, "dep", "add", "b07-950", "b07-901", "--type", "custom kind", "--json")
	n.equal(before, "narrow-custom-after")
	for i, target := range []string{"external:tracker:ticket-1", "remote-1"} {
		id := fmt.Sprintf("b07-%d", 951+i)
		n.create(id)
		ready := n.ready("external-before-ready")
		n.command("external-add", false, "dep", "add", id, target, "--type", "related", "--json")
		n.command("external-list", false, "dep", "list", id, "--json")
		n.edge(n.image("external-after"), id, target)
		n.requireReady("external-inert-ready", ready)
		n.command("external-remove", false, "dep", "remove", id, target, "--json")
		n.requireReady("external-removed-ready", ready)
	}
	// Public JSONL import is the source's generic metadata-bearing write route.
	// SQL is used only to observe the stored bytes; no table is seeded directly.
	importMetadata := func(label, metadata string) {
		n.t.Helper()
		path := filepath.Join(n.root, label+".jsonl")
		var out bytes.Buffer
		for _, id := range []string{"b07-960", "b07-961"} {
			row := map[string]any{"id": id, "title": id, "issue_type": "task", "status": "open", "priority": 2, "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}
			if id == "b07-960" {
				row["dependencies"] = []any{map[string]any{"issue_id": id, "depends_on_id": "b07-961", "type": "related", "metadata": metadata, "thread_id": "import-thread"}}
			}
			raw, e := json.Marshal(row)
			if e != nil {
				n.t.Fatal(e)
			}
			out.Write(raw)
			out.WriteByte('\n')
		}
		if e := os.WriteFile(path, out.Bytes(), 0600); e != nil {
			n.t.Fatal(e)
		}
		n.command(label, false, "import", "--input", path, "--json")
	}
	importMetadata("metadata-first", `{ "a": 1.0, "b": [true,null] }`)
	first := n.image("metadata-first-after")
	edge := n.edge(first, "b07-960", "b07-961")
	importMetadata("metadata-semantic-equal", `{"b":[true,null],"a":1e0}`)
	second := n.image("metadata-equal-after")
	if !reflect.DeepEqual(edge, n.edge(second, "b07-960", "b07-961")) {
		n.t.Fatal("semantic-equivalent native metadata changed stored edge")
	}
	importMetadata("metadata-different", `{"b":[true,null],"a":2}`)
	third := n.image("metadata-different-after")
	changed := n.edge(third, "b07-960", "b07-961")
	if bytes.Equal(edge["metadata"], changed["metadata"]) {
		n.t.Fatal("different native metadata was not applied")
	}
	b07JSON(n.t, filepath.Join(n.root, "metadata-comparison.json"), map[string]any{"first": edge, "equal": n.edge(second, "b07-960", "b07-961"), "different": changed, "route": "public JSONL import; full raw SQL images retained separately"})
}

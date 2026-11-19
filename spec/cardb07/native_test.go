package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
func (n *b07Native) graph(label string, nodes, edges []map[string]any, fail bool) []byte {
	path := filepath.Join(n.root, label+".plan.json")
	b07JSON(n.t, path, map[string]any{"nodes": nodes, "edges": edges})
	return n.command(label, fail, "create", "--graph", path, "--json")
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
	match := n.findEdge(image, from, to)
	if match == nil {
		n.t.Fatal("native edge absent")
	}
	return match
}
func (n *b07Native) findEdge(image b07NativeImage, from, to string) map[string]json.RawMessage {
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
	beforeMixed := n.image("mixed-create-refusal-before")
	// Canonical Beads refuses cross-bucket dependencies in CreateIssues. Keep
	// that actual refusal and use its supported existing-endpoint batch route.
	refusal := n.graph("mixed-create-refusal", nodes, []map[string]any{{"from_key": "durable", "to_key": "volatile", "type": "related"}}, true)
	var reason struct{ Error string }
	if json.Unmarshal(refusal, &reason) != nil || !strings.Contains(reason.Error, "cannot include cross-bucket dependency b07-920 -> b07-921") {
		n.t.Fatal("mixed create refusal differs from the observed canonical route", string(refusal))
	}
	n.equal(beforeMixed, "mixed-create-refusal-after")
	n.graph("mixed-create", nodes, []map[string]any{}, false)
	n.graph("mixed-second-active", []map[string]any{{"key": "second", "id": "b07-922", "title": "mixed second", "storage_class": "ephemeral"}}, []map[string]any{}, false)
	mixedReady := n.ready("mixed-identity-before-ready")
	n.batch("mixed-first-edge", "dep add b07-920 b07-921 related\n", false)
	n.batch("mixed-reverse-edge", "dep add b07-921 b07-920 tracks\n", false)
	n.batch("mixed-active-edge", "dep add b07-921 b07-922 custom-kind\n", false)
	n.requireReady("mixed-identity-after-ready", mixedReady)
	identity := n.image("mixed-identity-before")
	for _, edge := range []struct{ from, to, kind string }{{"b07-920", "b07-921", "related"}, {"b07-921", "b07-920", "tracks"}, {"b07-921", "b07-922", "custom-kind"}} {
		if b07Text(n.t, n.edge(identity, edge.from, edge.to)["type"]) != edge.kind {
			n.t.Fatal("native mixed edge kind or endpoints differ")
		}
		n.batch("mixed-same-kind-"+edge.from+"-"+edge.to, "dep add "+edge.from+" "+edge.to+" "+edge.kind+"\n", false)
		n.equal(identity, "mixed-repeat-after-"+edge.from+"-"+edge.to)
		n.batch("mixed-retype-"+edge.from+"-"+edge.to, "dep add "+edge.from+" "+edge.to+" blocks\n", true)
		n.equal(identity, "mixed-retype-after-"+edge.from+"-"+edge.to)
		n.requireReady("mixed-identity-ready-"+edge.from+"-"+edge.to, mixedReady)
	}
	ready := n.ready("mixed-success-before-ready")
	n.batch("mixed-success", "update b07-920 title=mixed-committed-durable\nupdate b07-921 title=mixed-committed-active\n", false)
	image := n.image("mixed-before-refusal")
	for id, want := range map[string]string{"b07-920": "mixed-committed-durable", "b07-921": "mixed-committed-active"} {
		found := 0
		for _, table := range []string{"issues", "wisps"} {
			for _, row := range image[table] {
				if b07Text(n.t, row["id"]) == id {
					if b07Text(n.t, row["title"]) != want {
						n.t.Fatal("successful native mixed batch lost title", id)
					}
					found++
				}
			}
		}
		if found != 1 {
			n.t.Fatal("successful native mixed batch lacks unique physical row", id)
		}
	}
	n.requireReady("mixed-success-after-ready", ready)
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
	var nativeTokens, nativeMetadataTokens map[string]string
	for _, name := range []string{"a", "b"} {
		if !t.Run("native_"+name, func(t *testing.T) {
			n := b07NewNative(t, binary, name)
			twins = append(twins, n.vocabulary())
			tokens := n.threadAndMixed()
			metadataTokens := n.externalAndMetadata()
			if nativeTokens == nil {
				nativeTokens, nativeMetadataTokens = tokens, metadataTokens
			} else if !reflect.DeepEqual(nativeTokens, tokens) || !reflect.DeepEqual(nativeMetadataTokens, metadataTokens) {
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
			b07ImportedWaits(t, engine, nativeTokens)
			b07MixedPublic(t, engine)
			t.Run("native_metadata_tokens", func(t *testing.T) {
				b07OpaquePublic(t, engine, nativeMetadataTokens)
				b07OpaqueImport(t, engine, nativeMetadataTokens)
			})
		})
	}
}

func (n *b07Native) externalAndMetadata() map[string]string {
	n.create("b07-950")
	before := n.image("narrow-custom-before")
	n.command("standalone-custom-refusal", true, "dep", "add", "b07-950", "b07-901", "--type", "custom kind", "--json")
	n.equal(before, "narrow-custom-after")
	n.command("missing-local-refusal", true, "dep", "add", "b07-950", "b07-absent", "--type", "related", "--json")
	n.equal(before, "missing-local-after")
	for i, target := range []string{"external:tracker:ticket-1", "remote-1"} {
		id := fmt.Sprintf("b07-%d", 951+i)
		n.create(id)
		ready := n.ready("external-before-ready")
		n.command("external-add", false, "dep", "add", id, target, "--type", "related", "--json")
		n.command("external-list", false, "dep", "list", id, "--json")
		n.edge(n.image("external-after"), id, target)
		n.requireReady("external-inert-ready", ready)
		n.command("external-remove", false, "dep", "remove", id, target, "--json")
		if n.findEdge(n.image("external-removed-after"), id, target) != nil {
			n.t.Fatal("native external removal retained the edge")
		}
		n.requireReady("external-removed-ready", ready)
	}
	n.create("b07-970")
	n.create("b07-971")
	loopReady := n.ready("opaque-loop-before-ready")
	n.batch("opaque-loop-forward", "dep add b07-970 b07-971 \"custom kind\"\n", false)
	n.batch("opaque-loop-reverse", "dep add b07-971 b07-970 \"custom kind\"\n", false)
	loopImage := n.image("opaque-loop-after")
	for _, pair := range [][2]string{{"b07-970", "b07-971"}, {"b07-971", "b07-970"}} {
		if b07Text(n.t, n.edge(loopImage, pair[0], pair[1])["type"]) != "custom kind" {
			n.t.Fatal("native custom loop changed edge identity")
		}
	}
	n.requireReady("opaque-loop-ready", loopReady)
	n.command("opaque-loop-close-a", false, "close", "b07-970", "--json")
	n.requireReady("opaque-loop-one-closed", b07Without(loopReady, "b07-970"))
	n.command("opaque-loop-close-b", false, "close", "b07-971", "--json")
	n.requireReady("opaque-loop-closed", b07Without(b07Without(loopReady, "b07-970"), "b07-971"))
	n.create("b07-972")
	n.create("b07-973")
	defaultReady := n.ready("omitted-kind-before-ready")
	n.batch("omitted-kind-default", "dep add b07-972 b07-973\n", false)
	defaultImage := n.image("omitted-kind-after")
	if b07Text(n.t, n.edge(defaultImage, "b07-972", "b07-973")["type"]) != "blocks" {
		n.t.Fatal("native omitted batch kind differs from its literal default")
	}
	n.requireReady("omitted-kind-blocks-ready", b07Without(defaultReady, "b07-972"))
	n.command("omitted-kind-guard", true, "close", "b07-972", "--json")
	n.equal(defaultImage, "omitted-kind-guard-after")
	// JSONL creates generic metadata, but reimport preserves an existing edge.
	// Keep that route observation separate from the public HTTP re-add comparator.
	// SQL only observes stored bytes; no table is seeded directly.
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
	if !reflect.DeepEqual(edge, changed) {
		n.t.Fatal("JSONL reimport changed the existing native edge")
	}
	b07JSON(n.t, filepath.Join(n.root, "metadata-import-observation.json"), map[string]any{"first": edge, "equal_input": n.edge(second, "b07-960", "b07-961"), "different_input": changed, "route": "public JSONL import preserves an existing edge; zero metadata-equality credit"})

	applyMetadata := n.metadataAPI()
	ready := n.ready("metadata-http-ready-before")
	beforeHTTP := n.image("metadata-http-before")
	applyMetadata("metadata-http-equivalent", `{"b":[true,null],"a":1e0}`, false)
	n.equal(beforeHTTP, "metadata-http-equivalent-after")
	// The native BatchApplier reports whether an audit event was inserted.
	// A metadata-only replacement writes the edge and journal but returns false.
	applyMetadata("metadata-http-different", `{"b":[true,null],"a":2}`, false)
	afterHTTP := n.image("metadata-http-different-after")
	n.metadataDelta(beforeHTTP, afterHTTP)
	updated := n.edge(afterHTTP, "b07-960", "b07-961")
	var value struct {
		A int   `json:"a"`
		B []any `json:"b"`
	}
	if json.Unmarshal([]byte(b07Text(n.t, updated["metadata"])), &value) != nil || value.A != 2 || !reflect.DeepEqual(value.B, []any{true, nil}) {
		n.t.Fatal("public HTTP metadata change was not stored")
	}
	if len(edge) != len(updated) {
		n.t.Fatal("metadata update changed edge field census")
	}
	for key, prior := range edge {
		if key != "metadata" && !bytes.Equal(prior, updated[key]) {
			n.t.Fatal("metadata update changed native edge identity/thread", key)
		}
	}
	applyMetadata("metadata-http-replay", `{ "a": 2.0, "b": [true,null] }`, false)
	n.equal(afterHTTP, "metadata-http-replay-after")
	n.requireReady("metadata-http-ready-after", ready)
	b07JSON(n.t, filepath.Join(n.root, "metadata-comparison.json"), map[string]any{"first": edge, "different": updated, "route": "POST /v0/beads/issues:batchApply", "semantic_equal_and_replay": "changed=false and complete current-table conservation", "different_response_changed": false, "different_storage": "metadata replacement plus exactly one journal row/sequence increment", "native_history_credit": false})
	return map[string]string{"beads.metadata": string(updated["metadata"]), "beads.thread_id": string(updated["thread_id"])}
}

// metadataAPI records the canonical binary's existing public edge-write route.
// The loopback server uses the same disposable database and unchanged binary.
func (n *b07Native) metadataAPI() func(string, string, bool) {
	n.t.Helper()
	stdoutPath, stderrPath := filepath.Join(n.root, "metadata-server.stdout"), filepath.Join(n.root, "metadata-server.stderr")
	stdout, err := os.Create(stdoutPath)
	if err != nil {
		n.t.Fatal(err)
	}
	stderr, err := os.Create(stderrPath)
	if err != nil {
		_ = stdout.Close()
		n.t.Fatal(err)
	}
	cmd := exec.Command(n.binary, "serve", "--addr", "127.0.0.1:0")
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = n.dir, append([]string(nil), n.env...), stdout, stderr
	b07JSON(n.t, filepath.Join(n.root, "metadata-server.request.json"), map[string]any{"argv": cmd.Args, "directory": cmd.Dir, "environment": cmd.Env})
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		n.t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	n.t.Cleanup(func() {
		select {
		case <-done:
		default:
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				n.t.Error("native HTTP stop", err)
			}
		}
		forced := false
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			forced = true
			if err := cmd.Process.Kill(); err != nil {
				n.t.Error("native HTTP forced stop", err)
			}
			<-done
		}
		if err := stdout.Close(); err != nil {
			n.t.Error(err)
		}
		if err := stderr.Close(); err != nil {
			n.t.Error(err)
		}
		out, outErr := os.ReadFile(stdoutPath)
		errorsRaw, errorsErr := os.ReadFile(stderrPath)
		b07JSON(n.t, filepath.Join(n.root, "metadata-server.exit.json"), map[string]any{"exit": cmd.ProcessState.ExitCode(), "error": fmt.Sprint(waitErr), "forced": forced, "stdout_sha256": b07Hash(out), "stderr_sha256": b07Hash(errorsRaw)})
		if forced || waitErr != nil || outErr != nil || errorsErr != nil {
			n.t.Errorf("native HTTP cleanup: forced=%v wait=%v stdout=%v stderr=%v", forced, waitErr, outErr, errorsErr)
		}
	})
	deadline := time.Now().Add(time.Minute)
	base := ""
	for base == "" && time.Now().Before(deadline) {
		select {
		case <-done:
			n.t.Fatal("native HTTP server exited before listening", waitErr)
		default:
		}
		raw, err := os.ReadFile(stdoutPath)
		if err != nil {
			n.t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			const prefix = "bd serve: listening on "
			if strings.HasPrefix(line, prefix) {
				base = strings.TrimPrefix(line, prefix)
			}
		}
		if base == "" {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !strings.HasPrefix(base, "http://127.0.0.1:") {
		n.t.Fatal("native HTTP loopback address missing", base)
	}
	transport := &http.Transport{Proxy: nil}
	n.t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Timeout: time.Minute, Transport: transport}
	health, err := client.Get(base + "/healthz")
	if err != nil {
		n.t.Fatal("native HTTP readiness", err)
	}
	healthRaw, readErr := io.ReadAll(health.Body)
	closeErr := health.Body.Close()
	if err := os.WriteFile(filepath.Join(n.root, "metadata-server.health.json"), healthRaw, 0600); err != nil {
		n.t.Fatal(err)
	}
	b07JSON(n.t, filepath.Join(n.root, "metadata-server.readiness.json"), map[string]any{"method": "GET", "url": base + "/healthz", "status": health.StatusCode, "headers": health.Header, "response_sha256": b07Hash(healthRaw)})
	if readErr != nil || closeErr != nil || health.StatusCode != http.StatusOK {
		n.t.Fatal("native HTTP readiness failed", health.StatusCode, readErr, closeErr)
	}
	return func(label, metadata string, changed bool) {
		n.t.Helper()
		// Build the wire body without re-encoding the supplied metadata token.
		body := []byte(`{"actor":"b07-native","items":[{"kind":"dep_add","dep_add":{"source":{"id":"b07-960"},"target":{"id":"b07-961"},"type":"related","metadata":` + metadata + `}}]}`)
		url := base + "/v0/beads/issues:batchApply"
		stem := filepath.Join(n.root, label)
		if err := os.WriteFile(stem+".request.json", body, 0600); err != nil {
			n.t.Fatal(err)
		}
		response, err := client.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			n.t.Fatal(err)
		}
		raw, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if err := os.WriteFile(stem+".response.json", raw, 0600); err != nil {
			n.t.Fatal(err)
		}
		b07JSON(n.t, stem+".binding.json", map[string]any{"method": "POST", "url": url, "status": response.StatusCode, "headers": response.Header, "request_sha256": b07Hash(body), "response_sha256": b07Hash(raw)})
		if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK {
			n.t.Fatalf("native HTTP metadata status=%d read=%v close=%v response=%s", response.StatusCode, readErr, closeErr, raw)
		}
		var result struct {
			Items []struct {
				Kind    string
				Changed bool
			}
		}
		if json.Unmarshal(raw, &result) != nil || len(result.Items) != 1 || result.Items[0].Kind != "dep_add" || result.Items[0].Changed != changed {
			n.t.Fatalf("native HTTP metadata changed outcome differs: %s", raw)
		}
	}
}

// The metadata-only change has one exact current-state delta. Historical
// qualification is separate; every other current table/row stays conserved.
func (n *b07Native) metadataDelta(before, after b07NativeImage) {
	n.t.Helper()
	if len(before) != len(after) {
		n.t.Fatal("metadata changed current table census")
	}
	rowKey := func(row map[string]json.RawMessage) string {
		raw, err := json.Marshal(row)
		if err != nil {
			n.t.Fatal(err)
		}
		return string(raw)
	}
	for table, old := range before {
		now, ok := after[table]
		if !ok {
			n.t.Fatal("metadata removed table", table)
		}
		switch table {
		case "dependencies":
			prior, next := n.edge(before, "b07-960", "b07-961"), n.edge(after, "b07-960", "b07-961")
			counts := map[string]int{}
			for _, row := range old {
				counts[rowKey(row)]++
			}
			for _, row := range now {
				copy := map[string]json.RawMessage{}
				for key, value := range row {
					copy[key] = value
				}
				if bytes.Equal(row["id"], next["id"]) {
					copy["metadata"] = prior["metadata"]
				}
				counts[rowKey(copy)]--
			}
			for _, count := range counts {
				if count != 0 {
					n.t.Fatal("metadata changed another edge field/row")
				}
			}
		case "bd_events_journal":
			counts := map[string]int{}
			for _, row := range old {
				counts[rowKey(row)]++
			}
			var added []map[string]json.RawMessage
			for _, row := range now {
				key := rowKey(row)
				if counts[key] > 0 {
					counts[key]--
				} else {
					added = append(added, row)
				}
			}
			for _, count := range counts {
				if count != 0 {
					n.t.Fatal("metadata rewrote journal history")
				}
			}
			if len(added) != 1 || b07Text(n.t, added[0]["op"]) != "dep_add" || b07Text(n.t, added[0]["issue_id"]) != "b07-960" || b07Text(n.t, added[0]["actor"]) != "b07-native" {
				n.t.Fatal("metadata journal delta differs", added)
			}
			var dep struct{ Kind, Target, Metadata string }
			if json.Unmarshal([]byte(b07Text(n.t, added[0]["dep_json"])), &dep) != nil || dep.Kind != "related" || dep.Target != "b07-961" || dep.Metadata != `{"b":[true,null],"a":2}` {
				n.t.Fatal("metadata journal payload differs", dep)
			}
		case "bd_events_seq":
			if len(old) != 1 || len(now) != 1 || len(old[0]) != len(now[0]) {
				n.t.Fatal("metadata sequence census differs")
			}
			var prior, next uint64
			if json.Unmarshal(old[0]["next_seq"], &prior) != nil || json.Unmarshal(now[0]["next_seq"], &next) != nil || next != prior+1 {
				n.t.Fatal("metadata journal sequence did not advance once")
			}
			for key, value := range old[0] {
				if key != "next_seq" && !bytes.Equal(value, now[0][key]) {
					n.t.Fatal("metadata changed sequence identity")
				}
			}
		default:
			if !reflect.DeepEqual(old, now) {
				n.t.Fatal("metadata changed unrelated current table", table)
			}
		}
	}
}

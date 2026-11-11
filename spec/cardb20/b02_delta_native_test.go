package cardb20

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	public "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// Only new native cases execute here. The older complete graph recording
// remains an immutable predecessor, including its stored-metadata scope.
func TestB02NativeNewPublicWriterWIPAndReasonProvenance(t *testing.T) {
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	root, checkout, request := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), os.Getenv("PLUG_SOURCE_MODULE_ROOT"), os.Getenv("PLUG_B02_REQUEST_SHA")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) || len(request) != 40 {
		t.Fatal("original RunsOn checkout, output and exact request required")
	}
	r := &nativeRecorder{t: t, root: root}
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "delta-candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	raw := r.buildCommand(checkout, "delta-candidate-commit", "/usr/bin/git", []string{"cat-file", "commit", "HEAD"})
	identity := sha1.Sum(append([]byte(fmt.Sprintf("commit %d\x00", len(raw))), raw...))
	if actual != request || hex.EncodeToString(identity[:]) != request {
		t.Fatal("retained original commit does not bind candidate")
	}
	input, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(input) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("immutable admitted build protocol changed")
	}
	var protocol struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(input, &protocol) != nil || protocol.ExporterLabel == "" {
		t.Fatal("admitted canonical target missing")
	}
	r.target = protocol.ExporterLabel
	for _, key := range []string{"GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatal("original workflow binding missing", key)
		}
	}
	r.writeJSON(filepath.Join(root, "delta-run-binding.json"), map[string]any{"candidate": request, "raw_commit_sha256": hashBytes(raw), "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "target": os.Getenv("TEST_TARGET"), "runtime_source": pinnedSource, "writer_source_search_pin": "ef3f1566ccc2dddafc55d86cff44ce4200237eb8", "qualification": "public disposable native delta only; no hosted, admission, history or FullS1 credit"})
	b02DeltaWriterSource(t, r, checkout)
	cli := r.buildCanonicalSource(pinnedSource)
	t.Run("new-wip", func(t *testing.T) {
		w := newB02DeltaCLI(t, cli, filepath.Join(root, "new-wip"))
		w.run("status-catalog", "config", "set", "status.custom", "b02active:active,b02wip:wip,b02done:done,b02frozen:frozen")
		catalog := w.run("status-catalog-readback", "config", "get", "status.custom", "--json")
		if !bytes.Contains(catalog, []byte("b02wip:wip")) {
			t.Fatal("actual native WIP catalog absent")
		}
		for _, role := range []string{"blocker", "ordinary", "conditional"} {
			w.create(role)
		}
		w.run("ordinary-edge", "dep", "add", w.ids["ordinary"], w.ids["blocker"], "--type", "blocks", "--json")
		w.run("conditional-edge", "dep", "add", w.ids["conditional"], w.ids["blocker"], "--type", "conditional-blocks", "--json")
		w.run("wip-update", "update", w.ids["blocker"], "--status", "b02wip", "--json")
		w.observe("wip-live", map[string][]string{"ordinary": {"blocker"}, "conditional": {"blocker"}}, map[string]string{"blocker": "b02wip", "ordinary": "open", "conditional": "open"})
		w.run("wip-close", "close", w.ids["blocker"], "--reason", "B02 new WIP transition", "--json")
		w.observe("wip-closed", map[string][]string{}, map[string]string{"blocker": "closed", "ordinary": "open", "conditional": "open"})
	})
	t.Run("public-also-writer", func(t *testing.T) {
		w := newB02DeltaCLI(t, cli, filepath.Join(root, "public-also-writer"))
		for _, command := range []struct {
			label    string
			argv     []string
			required string
		}{
			{"cook-help", []string{"cook", "--help"}, "--persist"},
			{"pour-help", []string{"mol", "pour", "--help"}, "proto-id"},
			{"update-help", []string{"update", "--help"}, "--metadata"},
			{"dependency-help", []string{"dep", "add", "--help"}, "--type"},
		} {
			if output := w.run(command.label, command.argv...); !bytes.Contains(output, []byte(command.required)) {
				t.Fatal("actual public writer search missing command", command.label)
			}
		}
		recipe := []byte("formula = \"b02-public-also\"\nversion = 1\ntype = \"workflow\"\n\n[[steps]]\nid = \"spawner\"\ntitle = \"B02 cooked spawner\"\n\n[[steps]]\nid = \"waiter\"\ntitle = \"B02 cooked waiter\"\nneeds = [\"spawner\"]\nwaits_for = \"all-children\"\n")
		path := filepath.Join(w.dir, "b02-public-also.formula.toml")
		if err := os.WriteFile(path, recipe, 0600); err != nil {
			t.Fatal(err)
		}
		w.rec.writeJSON(filepath.Join(w.rec.root, "writer-input.json"), map[string]any{"source": string(recipe), "sha256": hashBytes(recipe), "public_writer": "cook --persist followed by mol pour", "sql_writer": false})
		var cooked struct {
			ProtoID string `json:"proto_id"`
			Created int    `json:"created"`
		}
		if json.Unmarshal(w.run("cook-persist", "cook", path, "--persist", "--json"), &cooked) != nil || cooked.ProtoID == "" || cooked.Created != 3 {
			t.Fatal("actual public persisted cook result")
		}
		rows := w.rows("cooked-row-readback", "SELECT id,title,status,is_template FROM issues ORDER BY id")
		if len(rows) != 3 {
			t.Fatal("cook physical census", rows)
		}
		for _, row := range rows {
			role := ""
			switch row["title"] {
			case "b02-public-also":
				role = "proto-root"
			case "B02 cooked spawner":
				role = "proto-spawner"
			case "B02 cooked waiter":
				role = "proto-waiter"
			}
			id, ok := row["id"].(string)
			if !ok || role == "" || w.ids[role] != "" || row["status"] != "open" || fmt.Sprint(row["is_template"]) != "1" {
				t.Fatal("exact cooked row identity", row)
			}
			w.ids[role] = id
		}
		if w.ids["proto-root"] != cooked.ProtoID {
			t.Fatal("cook result not bound to committed root")
		}
		w.metadata("cooked-metadata", "proto-waiter", "proto-spawner")
		var poured struct {
			NewEpicID         string            `json:"new_epic_id"`
			IDMapping         map[string]string `json:"id_mapping"`
			Created, Attached int
			Phase             string
		}
		if json.Unmarshal(w.run("pour", "mol", "pour", cooked.ProtoID, "--json"), &poured) != nil || poured.Created != 3 || poured.Attached != 0 || poured.Phase != "liquid" || len(poured.IDMapping) != 3 {
			t.Fatal("actual public pour result")
		}
		for _, role := range []string{"root", "spawner", "waiter"} {
			id := poured.IDMapping[w.ids["proto-"+role]]
			if id == "" || id == w.ids["proto-"+role] {
				t.Fatal("pour omitted physical endpoint mapping")
			}
			w.ids[role] = id
		}
		if poured.NewEpicID != w.ids["root"] {
			t.Fatal("pour root identity mismatch")
		}
		w.metadata("poured-metadata", "waiter", "spawner")
		statuses := map[string]string{}
		for role := range w.ids {
			statuses[role] = "open"
		}
		w.observe("public-childless-open", map[string][]string{"proto-waiter": {"proto-spawner"}, "waiter": {"spawner"}}, statuses)
		w.run("close-poured-spawner", "close", w.ids["spawner"], "--reason", "B02 public collapsed edge", "--json")
		statuses["spawner"] = "closed"
		w.observe("public-spawner-closed", map[string][]string{"proto-waiter": {"proto-spawner"}}, statuses)
		w.run("reopen-poured-spawner", "reopen", w.ids["spawner"], "--reason", "B02 public writer live reopen", "--json")
		statuses["spawner"] = "open"
		w.observe("public-spawner-reopened", map[string][]string{"proto-waiter": {"proto-spawner"}, "waiter": {"spawner"}}, statuses)
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			t.Run("public-counterpart-"+engine, func(t *testing.T) { b02DeltaPublicAlso(t, engine) })
		}
	})
	t.Run("new-reason-provenance", func(t *testing.T) {
		w := newB02DeltaCLI(t, cli, filepath.Join(root, "new-reason-provenance"))
		for _, role := range []string{"root", "spawner", "open-child", "done-child", "parent", "open-parent", "open-fallback"} {
			if role == "open-fallback" {
				w.ids[role] = "b02-" + role
				w.run("create-"+role, "create", "--id", w.ids[role], "--title", "B02 "+role, "--type", "task", "--force", "--status", "open", "--waits-for", w.ids["spawner"], "--waits-for-gate", "all-children", "--json")
			} else {
				w.create(role)
			}
		}
		for _, role := range []string{"open-child", "done-child"} {
			w.run("parent-"+role, "update", w.ids[role], "--parent", w.ids["spawner"], "--json")
		}
		w.run("close-done-child", "close", w.ids["done-child"], "--reason", "B02 any gate", "--json")
		for _, waiter := range []struct{ role, gate string }{{"multi", "any-children"}, {"undisplayed", "all-children"}, {"fallback", "all-children"}} {
			w.ids[waiter.role] = "b02-" + waiter.role
			w.run("create-"+waiter.role, "create", "--id", w.ids[waiter.role], "--title", "B02 "+waiter.role, "--type", "task", "--force", "--status", "open", "--waits-for", w.ids["spawner"], "--waits-for-gate", waiter.gate, "--json")
		}
		w.run("multi-ordinary", "dep", "add", w.ids["multi"], w.ids["root"], "--type", "blocks", "--json")
		w.run("fallback-parent", "update", w.ids["fallback"], "--parent", w.ids["parent"], "--json")
		w.run("open-fallback-parent", "update", w.ids["open-fallback"], "--parent", w.ids["open-parent"], "--json")
		statuses := map[string]string{}
		for role := range w.ids {
			statuses[role] = "open"
		}
		statuses["done-child"] = "closed"
		initial := map[string][]string{"multi": {"root", "spawner"}, "undisplayed": {"spawner"}, "fallback": {"spawner"}, "open-fallback": {"spawner"}}
		w.observe("satisfied-any-shown-beside-ordinary", initial, statuses)
		w.run("closed-spawner-open-child", "close", w.ids["spawner"], "--force", "--reason", "B02 exact native display provenance", "--json")
		statuses["spawner"] = "closed"
		w.observe("closed-spawner-omission-and-parent-fallback", map[string][]string{"multi": {"root"}, "fallback": {"parent"}, "open-fallback": {"open-parent"}}, statuses)
		w.run("reopen-spawner", "reopen", w.ids["spawner"], "--reason", "B02 exact native live reopen", "--json")
		statuses["spawner"] = "open"
		w.observe("reason-spawner-reopened", initial, statuses)
	})
}

func b02DeltaWriterSource(t *testing.T, r *nativeRecorder, checkout string) {
	t.Helper()
	want := map[string]string{"cmd/bd/cook.go": "853b90a67f06163fcfb94fee9576fd189baaacc3", "cmd/bd/pour.go": "7260365eebfaa38f554ea11300db0e2e1f41c42c", "internal/storage/issueops/blocked.go": "f25beed559d9fa37a0a1350d0b5ca765bed8389a", "internal/storage/issueops/blocked_state.go": "66997bf2e2fc72f1f612b2263242ce2e1b2a5848"}
	f, err := os.Open(filepath.Join(checkout, "spec/s1-b09-native/testdata/"+pinnedSource.Name+".tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	archive := tar.NewReader(gz)
	found := map[string]string{}
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimPrefix(header.Name, "./")
		expected, ok := want[name]
		if !ok {
			continue
		}
		data, err := io.ReadAll(archive)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha1.Sum(append([]byte(fmt.Sprintf("blob %d\x00", len(data))), data...))
		actual := hex.EncodeToString(digest[:])
		if actual != expected {
			t.Fatal("source-search identity differs", name)
		}
		found[name] = actual
		if name == "cmd/bd/cook.go" && (!bytes.Contains(data, []byte("types.NewWaitsForBlockingDependency")) || !bytes.Contains(data, []byte("tx.AddDependency"))) {
			t.Fatal("public dependency writer path missing")
		}
	}
	if !reflect.DeepEqual(found, want) {
		t.Fatal("source-search files incomplete")
	}
	r.writeJSON(filepath.Join(r.root, "writer-source-search.json"), map[string]any{"ef3f_search_blobs_independently_reviewed": found, "actual_runtime_source": pinnedSource.Commit, "public_writer_exists": true, "classification": "cook --persist and mol pour write dependency also_blocks; update metadata is issue metadata; dep add has no dependency metadata flag"})
}

type b02DeltaCLI struct {
	t        *testing.T
	rec      *nativeRecorder
	cli, dir string
	env      []string
	ids      map[string]string
	server   *publicnative.Server
}

func newB02DeltaCLI(t *testing.T, cli, root string) *b02DeltaCLI {
	t.Helper()
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	server := publicnative.StartDolt(t, root)
	if err := os.MkdirAll("/tmp/plug-swarm/work", 0700); err != nil {
		t.Fatal(err)
	}
	workspace, err := os.MkdirTemp("/tmp/plug-swarm/work", "b02-new-native-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(workspace); err != nil {
			t.Error(err)
		}
	})
	home, dir, temporary := filepath.Join(workspace, "home"), filepath.Join(workspace, "project"), filepath.Join(workspace, "tmp")
	for _, path := range []string{home, dir, temporary} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	env := server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:" + os.Getenv("PATH"), "HOME=" + home, "USER=b02-new-native", "TMPDIR=" + temporary, "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B02 Public", "GIT_AUTHOR_EMAIL=b02@example.invalid", "GIT_COMMITTER_NAME=B02 Public", "GIT_COMMITTER_EMAIL=b02@example.invalid", "BEADS_ACTOR=b02-recorder", "BEADS_NO_DAEMON=1"})
	// ChildEnv prepends the declared database binary directory. Every child
	// still starts with the worker-safe boundary required by this lane.
	for i, value := range env {
		if strings.HasPrefix(value, "PATH=") {
			env[i] = "PATH=/tmp/plug-swarm/bin/worker-safe:" + strings.TrimPrefix(value, "PATH=")
		}
	}
	w := &b02DeltaCLI{t: t, rec: &nativeRecorder{t: t, root: root}, cli: cli, dir: dir, env: env, ids: map[string]string{}, server: server}
	w.command("git-init", "/usr/bin/git", []string{"-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template="})
	var identity struct{ Commit, Build string }
	if raw := w.run("version", "version", "--json"); json.Unmarshal(raw, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatal("actual binary identity mismatch")
	}
	w.run("init", "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b02_new", "--prefix", "b02", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	w.rec.writeJSON(filepath.Join(root, "physical-owner.json"), map[string]any{"database": server.RequireDatabaseDir(t, "b02_new"), "server_pid": server.PID, "data_root": server.DataRoot, "port": server.Port})
	return w
}

func (w *b02DeltaCLI) command(label, binary string, args []string) []byte {
	w.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = w.dir, w.env
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		code = -2
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			code = exit.ExitCode()
		}
	}
	for suffix, data := range map[string][]byte{"stdout": out.Bytes(), "stderr": stderr.Bytes()} {
		if err := os.WriteFile(filepath.Join(w.rec.root, label+"."+suffix), data, 0600); err != nil {
			w.t.Fatal(err)
		}
	}
	w.rec.writeJSON(filepath.Join(w.rec.root, label+".request.json"), map[string]any{"argv": cmd.Args, "directory": cmd.Dir, "environment": w.env, "exit": code, "error": fmt.Sprint(err), "stdout_sha256": hashBytes(out.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes())})
	if err != nil || ctx.Err() != nil {
		w.t.Fatalf("%s exit=%d: %v\n%s", label, code, err, stderr.Bytes())
	}
	return out.Bytes()
}
func (w *b02DeltaCLI) run(label string, args ...string) []byte { return w.command(label, w.cli, args) }
func (w *b02DeltaCLI) create(role string) {
	w.ids[role] = "b02-" + role
	w.run("create-"+role, "create", "--id", w.ids[role], "--title", "B02 "+role, "--type", "task", "--status", "open", "--priority", "1", "--force", "--json")
}
func (w *b02DeltaCLI) rows(label, query string) []map[string]any {
	w.t.Helper()
	raw := w.run(label, "--readonly", "sql", query, "--json")
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var rows []map[string]any
	if err := decoder.Decode(&rows); err != nil {
		w.t.Fatal(err)
	}
	return rows
}
func (w *b02DeltaCLI) metadata(label, source, target string) {
	w.t.Helper()
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
	rows := w.rows(label, "SELECT type,metadata FROM dependencies WHERE issue_id="+quote(w.ids[source])+" AND depends_on_issue_id="+quote(w.ids[target]))
	if len(rows) != 1 || rows[0]["type"] != "waits-for" {
		w.t.Fatal("public writer did not commit exactly one waits edge", rows)
	}
	text, ok := rows[0]["metadata"].(string)
	if !ok {
		w.t.Fatal("physical dependency metadata is not retained JSON", rows)
	}
	var actual map[string]any
	if json.Unmarshal([]byte(text), &actual) != nil || !reflect.DeepEqual(actual, map[string]any{"gate": "all-children", "also_blocks": true}) {
		w.t.Fatal("public writer exact metadata differs", text)
	}
}
func (w *b02DeltaCLI) observe(label string, want map[string][]string, statuses map[string]string) {
	w.t.Helper()
	before := w.rows(label+"-before-issues", "SELECT * FROM issues ORDER BY id")
	deps := w.rows(label+"-before-dependencies", "SELECT * FROM dependencies ORDER BY issue_id,depends_on_issue_id")
	if len(before) != len(w.ids) || len(statuses) != len(w.ids) {
		w.t.Fatal("native physical row census mismatch")
	}
	roles := map[string]string{}
	for role, id := range w.ids {
		if roles[id] != "" {
			w.t.Fatal("duplicate actual native endpoint")
		}
		roles[id] = role
	}
	for _, row := range before {
		id, ok := row["id"].(string)
		role := roles[id]
		if !ok || role == "" || row["status"] != statuses[role] || row["updated_at"] == nil || row["is_blocked"] == nil {
			w.t.Fatal("exact committed native status/projection", row)
		}
	}
	for repeat := 0; repeat < 2; repeat++ {
		var items []struct {
			ID    string
			By    []string `json:"blocked_by"`
			Count *int     `json:"blocked_by_count"`
		}
		raw := w.run(fmt.Sprintf("%s-blocked-%d", label, repeat), "blocked", "--json")
		if json.Unmarshal(raw, &items) != nil {
			w.t.Fatal("actual native blocked JSON")
		}
		got := map[string][]string{}
		for _, item := range items {
			role := roles[item.ID]
			if role == "" || got[role] != nil || item.Count == nil || *item.Count != len(item.By) {
				w.t.Fatal("actual native blocked identities/counts", string(raw))
			}
			by := []string{}
			for _, id := range item.By {
				if roles[id] == "" {
					w.t.Fatal("foreign native reason", id)
				}
				by = append(by, roles[id])
			}
			sort.Strings(by)
			got[role] = by
		}
		expected := map[string][]string{}
		for role, reasons := range want {
			expected[role] = append([]string{}, reasons...)
			sort.Strings(expected[role])
		}
		if !reflect.DeepEqual(got, expected) {
			w.t.Fatalf("%s exact native reasons=%v want=%v", label, got, expected)
		}
	}
	if after := w.rows(label+"-reopened-issues", "SELECT * FROM issues ORDER BY id"); !reflect.DeepEqual(before, after) {
		w.t.Fatal("native read changed exact committed rows/timestamps")
	}
	if after := w.rows(label+"-reopened-dependencies", "SELECT * FROM dependencies ORDER BY issue_id,depends_on_issue_id"); !reflect.DeepEqual(deps, after) {
		w.t.Fatal("native read changed exact committed edges")
	}
	w.server.RequireDatabaseDir(w.t, "b02_new")
	w.rec.writeJSON(filepath.Join(w.rec.root, label+"-expectations.json"), map[string]any{"blocked": want, "statuses": statuses, "actual_endpoint_roles": w.ids, "native_runtime_pin": pinnedSource.Commit, "sql_reopen": "each actual CLI process reopens the same physical database; exact before/after row images retained"})
}

func b02DeltaPublicAlso(t *testing.T, engine string) {
	h := public.NewHost(t, engine)
	create := func(role string) model.Ref {
		reply := h.Call("bead.create", "also-create-"+role, map[string]any{"type": "issue", "fields": map[string]any{"title": "B02 cooked " + role}}, "")
		var commit model.Commit
		if json.Unmarshal(reply.Result, &commit) != nil || len(commit.Results) != 1 {
			t.Fatal("actual counterpart create")
		}
		row := h.Capture().Row(t, commit.Results[0].Ref.ID)
		if row.Revision != commit.Revision || !bytes.Equal(row.Body, commit.Results[0].Record) {
			t.Fatal("counterpart exact committed create")
		}
		return commit.Results[0].Ref
	}
	spawner, waiter := create("spawner"), create("waiter")
	h.Call("bead.link", "also-public-link", map[string]any{"from": waiter, "to": spawner, "kind": "waits-for", "metadata": map[string]any{"gate": "all-children", "also_blocks": true}}, "")
	for _, phase := range []struct {
		label, method string
		blocked       bool
	}{{"childless-open", "", true}, {"spawner-closed", "bead.close", false}, {"spawner-reopened", "bead.reopen", true}} {
		if phase.method != "" {
			h.Call(phase.method, "also-"+phase.label, map[string]any{"id": spawner}, "")
		}
		before := h.Capture()
		check := func() {
			reply := h.Call("bead.blocked", "also-read-"+phase.label, map[string]any{"limit": 1000}, "")
			var page struct {
				Items []struct {
					Bead                           struct{ ID string }
					BlockedBy                      []model.Ref
					BlockedByCount, HiddenBlockers int
				}
				NextCursor string
			}
			if json.Unmarshal(reply.Result, &page) != nil || page.NextCursor != "" || len(page.Items) != map[bool]int{true: 1, false: 0}[phase.blocked] {
				t.Fatal("public also counterpart membership", string(reply.Result))
			}
			if phase.blocked {
				item := page.Items[0]
				if item.Bead.ID != waiter.ID || item.BlockedByCount != 1 || item.HiddenBlockers != 0 || !reflect.DeepEqual(item.BlockedBy, []model.Ref{spawner}) {
					t.Fatal("public also counterpart exact reasons")
				}
			}
		}
		check()
		proof.Same(t, before, h.Capture())
		if engine != "memory" {
			h.Reopen()
			proof.Same(t, before, h.Capture())
			check()
			proof.Same(t, before, h.Capture())
		}
	}
}

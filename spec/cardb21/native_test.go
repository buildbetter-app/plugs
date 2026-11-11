package cardb20

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// This new HTTP unit reuses the unchanged admitted B09/B20 canonical builder.
// It neither imports a store nor calls an official producer/recording action.
func TestB21NativePublicBatchHTTP(t *testing.T) {
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) {
		t.Fatal("RunsOn output and exact checkout required")
	}
	root = filepath.Join(root, "b21-native-http")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	protocol, e := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if e != nil || hashBytes(protocol) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("canonical build protocol changed")
	}
	var inputs struct {
		ExporterLabel string `json:"exporter_label"`
	}
	b21Decode(t, protocol, &inputs)
	if inputs.ExporterLabel == "" {
		t.Fatal("canonical target absent")
	}
	recorder := &nativeRecorder{t: t, root: root, target: inputs.ExporterLabel}
	binary := recorder.buildCanonicalSource(pinnedSource)
	n := newB21Native(t, root, binary)
	t.Run("declaration-order-backrefs-create-update-close", func(t *testing.T) {
		n.use(t)
		defer n.useParent(t)
		before := n.history()
		reply := n.apply("ordered", nativeB21Create("a", "native A"), nativeB21Create("b", "native B"), nativeB21Update(map[string]any{"key": "a"}, map[string]any{"title": "native A patched", "description": "preserved verbatim Ω"}), nativeB21Close(map[string]any{"key": "a"}))
		items := nativeB21Items(t, reply)
		keys := reply["keys"].(map[string]any)
		if len(items) != 4 || keys["a"] != "b21-a" || keys["b"] != "b21-b" {
			t.Fatalf("native ordered bindings %+v", reply)
		}
		for i, want := range []string{"b21-a", "b21-b", "b21-a", "b21-a"} {
			if items[i]["issue_id"] != want {
				t.Fatalf("native result index %d %+v", i, items[i])
			}
		}
		rows := n.sql("SELECT id,title,description,status,close_reason FROM issues WHERE id IN ('b21-a','b21-b') ORDER BY id")
		if len(rows) != 2 || rows[0]["title"] != "native A patched" || rows[0]["description"] != "preserved verbatim Ω" || rows[0]["status"] != "closed" || rows[0]["close_reason"] != "B21 completed" || rows[1]["title"] != "native B" || rows[1]["status"] != "open" || n.history() != before+1 {
			t.Fatalf("independent native committed values %+v", rows)
		}
	})
	t.Run("typed-declared-later-and-final-refusal", func(t *testing.T) {
		n.use(t)
		defer n.useParent(t)
		for _, later := range []bool{true, false} {
			key := "missing-key"
			items := []any{nativeB21Update(nativeB21Ref("b"), map[string]any{"title": "must rollback"}), nativeB21Close(map[string]any{"key": key})}
			if later {
				key = "later"
				items[1] = nativeB21Close(map[string]any{"key": key})
				items = append(items, nativeB21Create(key, "must rollback"))
			}
			before := n.census()
			problem := n.http("later-"+strconv.FormatBool(later), "POST", "/v0/beads/issues:batchApply", map[string]any{"actor": "b21-native", "items": items}, 400)
			if problem["code"] != "invalid_argument" || problem["declared_later"] != later || fmt.Sprint(problem["item_index"]) != "1" || problem["item_kind"] != "close" {
				t.Fatalf("native typed ref diagnosis %+v", problem)
			}
			b21Equal(t, before, n.census(), "native refusal physical conservation")
		}
		before := n.census()
		problem := n.http("final-refusal", "POST", "/v0/beads/issues:batchApply", map[string]any{"actor": "b21-native", "items": []any{nativeB21Create("rollback", "must rollback"), nativeB21Update(nativeB21Ref("b"), map[string]any{"title": "must rollback"}), nativeB21Close(nativeB21Ref("absent"))}}, 404)
		if problem["code"] != "not_found" || fmt.Sprint(problem["item_index"]) != "2" {
			t.Fatalf("native final typed refusal %+v", problem)
		}
		b21Equal(t, before, n.census(), "native final-item rollback all physical families")
	})
	t.Run("1000-bound-and-noop-no-history", func(t *testing.T) {
		n.use(t)
		defer n.useParent(t)
		ops := make([]any, 1000)
		for i := range ops {
			ops[i] = nativeB21Update(nativeB21Ref("b"), map[string]any{"title": "native B"})
		}
		before := n.census()
		result := n.apply("1000", ops...)
		items := nativeB21Items(t, result)
		if len(items) != 1000 {
			t.Fatal("native 1000 boundary did not execute every item")
		}
		for i, item := range items {
			if item["changed"] != false || item["issue_id"] != "b21-b" {
				t.Fatalf("native boundary index%d %+v", i, item)
			}
		}
		b21Equal(t, before, n.census(), "native no-op batch no history")
		ops = append(ops, ops[0])
		problem := n.http("1001", "POST", "/v0/beads/issues:batchApply", map[string]any{"actor": "b21-native", "items": ops}, 400)
		if problem["code"] != "invalid_argument" {
			t.Fatalf("native 1001 code %+v", problem)
		}
		b21Equal(t, before, n.census(), "native over-bound conservation")
	})
	t.Run("metadata-row-delta-despite-changed-false", func(t *testing.T) {
		n.use(t)
		defer n.useParent(t)
		n.apply("metadata-seed", nativeB21Create("waiter", "native waiter"), nativeB21Create("spawner", "native spawner"), nativeB21Dep("all-children"))
		before := n.history()
		result := n.apply("metadata-rewrite", nativeB21Dep("any-children"))
		items := nativeB21Items(t, result)
		if len(items) != 1 || items[0]["changed"] != false || n.history() != before+1 {
			t.Fatalf("native row-delta history versus Changed=false %+v", result)
		}
		rows := n.sql("SELECT metadata FROM dependencies WHERE issue_id='b21-waiter' AND depends_on_issue_id='b21-spawner'")
		if len(rows) != 1 {
			t.Fatalf("native waits edge %+v", rows)
		}
		var metadata map[string]any
		b21Decode(t, []byte(fmt.Sprint(rows[0]["metadata"])), &metadata)
		if metadata["gate"] != "any-children" {
			t.Fatalf("exact native gate %+v", metadata)
		}
		log := n.sql("SELECT message FROM dolt_log WHERE commit_hash=HASHOF('HEAD')")
		if len(log) != 1 || log[0]["message"] != "bd: apply batch" {
			t.Fatalf("exact native history name %+v", log)
		}
	})
	t.Run("ephemeral-only-batch-public-row-and-no-durable-history", func(t *testing.T) {
		n.use(t)
		defer n.useParent(t)
		before := n.history()
		create := nativeB21Create("active", "native ephemeral").(map[string]any)
		create["create"].(map[string]any)["ephemeral"] = true
		n.apply("ephemeral-only", create, nativeB21Update(map[string]any{"key": "active"}, map[string]any{"title": "native ephemeral patched"}))
		rows := n.sql("SELECT id,title FROM wisps WHERE id='b21-active'")
		if len(rows) != 1 || rows[0]["title"] != "native ephemeral patched" || n.history() != before {
			t.Fatalf("actual native ephemeral readback/history %+v", rows)
		}
		n.http("ephemeral-public-read", "GET", "/v0/beads/issues/b21-active", nil, 200)
	})
	t.Run("indexed-close-survivors-and-all-idempotent-no-history", func(t *testing.T) {
		n.use(t)
		defer n.useParent(t)
		n.apply("close-seed", nativeB21Create("closea", "close A"), nativeB21Create("closeb", "close B"))
		before := n.history()
		body := map[string]any{"actor": "b21-native", "items": []any{map[string]any{"id": "b21-closeb", "reason": "first B"}, map[string]any{"id": "b21-absent", "reason": "refused"}, map[string]any{"id": "b21-closea", "reason": "first A"}, map[string]any{"id": "b21-closeb", "reason": "ignored duplicate"}}}
		result := n.http("indexed-close", "POST", "/v0/beads/issues:batchClose", body, 200)
		raw, _ := result["outcomes"].([]any)
		if len(raw) != 4 {
			t.Fatalf("native indexed outcomes %+v", result)
		}
		for i, id := range []string{"b21-closeb", "b21-absent", "b21-closea", "b21-closeb"} {
			item := raw[i].(map[string]any)
			if item["issue_id"] != id {
				t.Fatalf("native close index %d %+v", i, item)
			}
			if i == 1 {
				if item["code"] != "not_found" || item["issue"] != nil || item["already_closed"] != nil {
					t.Fatalf("refusal branch %+v", item)
				}
			} else {
				if item["code"] != nil || item["issue"] == nil || item["already_closed"] != (i == 3) || fmt.Sprint(item["open_children"]) != "0" {
					t.Fatalf("success branch index%d %+v", i, item)
				}
			}
		}
		rows := n.sql("SELECT id,status,close_reason FROM issues WHERE id IN ('b21-closea','b21-closeb') ORDER BY id")
		if len(rows) != 2 || rows[0]["status"] != "closed" || rows[1]["status"] != "closed" || rows[0]["close_reason"] != "first A" || rows[1]["close_reason"] != "first B" || n.history() != before+1 {
			t.Fatalf("native survivors readback %+v", rows)
		}
		before = n.history()
		n.http("all-idempotent-close", "POST", "/v0/beads/issues:batchClose", map[string]any{"actor": "b21-native", "items": []any{map[string]any{"id": "b21-closea"}, map[string]any{"id": "b21-closeb"}}}, 200)
		if n.history() != before {
			t.Fatal("native all-idempotent history")
		}
		n.http("all-refused-close", "POST", "/v0/beads/issues:batchClose", map[string]any{"actor": "b21-native", "items": []any{map[string]any{"id": "b21-absent"}}}, 200)
		if n.history() != before {
			t.Fatal("native all-refused history")
		}
	})
	t.Run("SQL-and-public-reopen-readback", func(t *testing.T) {
		n.use(t)
		defer n.useParent(t)
		before := n.census()
		n.stop()
		n.start()
		b21Equal(t, before, n.census(), "native SQL physical reopen")
		n.http("reopened-public-a", "GET", "/v0/beads/issues/b21-a", nil, 200)
		n.http("reopened-public-ephemeral", "GET", "/v0/beads/issues/b21-active", nil, 200)
	})
}

func nativeB21Ref(id string) map[string]any { return map[string]any{"id": "b21-" + id} }
func nativeB21Create(key, title string) any {
	return map[string]any{"kind": "create", "create": map[string]any{"key": key, "id": "b21-" + key, "title": title, "issue_type": "task"}}
}
func nativeB21Update(ref any, patch map[string]any) any {
	return map[string]any{"kind": "update", "update": map[string]any{"target": ref, "patch": patch}}
}
func nativeB21Close(ref any) any {
	return map[string]any{"kind": "close", "close": map[string]any{"target": ref, "reason": "B21 completed"}}
}
func nativeB21Dep(gate string) any {
	return map[string]any{"kind": "dep_add", "dep_add": map[string]any{"source": nativeB21Ref("waiter"), "target": nativeB21Ref("spawner"), "type": "waits-for", "metadata": map[string]any{"gate": gate}}}
}
func nativeB21Items(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	raw, ok := body["items"].([]any)
	if !ok {
		t.Fatalf("native items absent %+v", body)
	}
	out := make([]map[string]any, len(raw))
	for i, item := range raw {
		out[i] = item.(map[string]any)
	}
	return out
}

type b21NativeBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (b *b21NativeBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}
func (b *b21NativeBuffer) data() []byte {
	b.Lock()
	defer b.Unlock()
	return append([]byte(nil), b.Buffer.Bytes()...)
}

type b21Native struct {
	t, parent               *testing.T
	root, binary, dir, base string
	env                     []string
	number                  int
	cmd                     *exec.Cmd
	done                    chan error
	out, err                *b21NativeBuffer
}

func (n *b21Native) use(t *testing.T)       { n.t = t }
func (n *b21Native) useParent(_ *testing.T) { n.t = n.parent }
func newB21Native(t *testing.T, root, binary string) *b21Native {
	server := publicnative.StartDolt(t, root)
	workRoot := "/tmp/plug-swarm/work"
	if e := os.MkdirAll(workRoot, 0700); e != nil {
		t.Fatal(e)
	}
	work, e := os.MkdirTemp(workRoot, "b21-public-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := os.RemoveAll(work); e != nil {
			t.Error(e)
		}
	})
	for _, name := range []string{"home", "project", "tmp"} {
		if e = os.Mkdir(filepath.Join(work, name), 0700); e != nil {
			t.Fatal(e)
		}
	}
	env := []string{"PATH=/tmp/plug-swarm/bin/worker-safe:" + filepath.Dir(server.DoltBinary) + ":" + os.Getenv("PATH"), "HOME=" + filepath.Join(work, "home"), "TMPDIR=" + filepath.Join(work, "tmp"), "USER=b21-public", "TZ=UTC", "LC_ALL=C", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B21 Public", "GIT_AUTHOR_EMAIL=b21@example.invalid", "GIT_COMMITTER_NAME=B21 Public", "GIT_COMMITTER_EMAIL=b21@example.invalid", "BEADS_ACTOR=b21-native", "BEADS_NO_DAEMON=1"}
	n := &b21Native{t: t, parent: t, root: root, binary: binary, dir: filepath.Join(work, "project"), env: server.ChildEnv(env)}
	n.command("git-init", "/usr/bin/git", "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	version := n.command("version", binary, "version", "--json")
	var identity struct{ Commit, Build string }
	b21Decode(t, version, &identity)
	if identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatalf("actual canonical version %+v", identity)
	}
	n.command("init", binary, "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b21_public", "--prefix", "b21", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	physical := server.RequireDatabaseDir(t, "b21_public")
	n.save("binding", map[string]any{"native_source": pinnedSource, "physical_database": physical, "dolt_pid": server.PID, "data_root": server.DataRoot, "binary_version": json.RawMessage(version), "qualification": "PUBLIC_OPERATIONAL_RECORDING_ONLY", "official": "UNSPENT"})
	t.Cleanup(n.stop)
	n.start()
	return n
}
func (n *b21Native) save(label string, value any) {
	n.t.Helper()
	n.number++
	raw, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		n.t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(n.root, fmt.Sprintf("%04d-%s.json", n.number, label)), append(raw, '\n'), 0600); e != nil {
		n.t.Fatal(e)
	}
}
func (n *b21Native) command(label, binary string, args ...string) []byte {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = n.dir, n.env
	var out, errout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errout
	e := cmd.Run()
	exit := 0
	if e != nil {
		exit = -1
		var x *exec.ExitError
		if errors.As(e, &x) {
			exit = x.ExitCode()
		}
	}
	n.save(label, map[string]any{"argv": cmd.Args, "directory": cmd.Dir, "stdout": out.String(), "stderr": errout.String(), "exit": exit, "error": fmt.Sprint(e)})
	if e != nil {
		n.t.Fatalf("native %s: %v %s", label, e, errout.Bytes())
	}
	return out.Bytes()
}
func (n *b21Native) start() {
	n.t.Helper()
	n.done = make(chan error, 1)
	n.out, n.err = &b21NativeBuffer{}, &b21NativeBuffer{}
	n.cmd = exec.Command(n.binary, "serve", "--addr", "127.0.0.1:0")
	n.cmd.Dir, n.cmd.Env = n.dir, n.env
	pipe, e := n.cmd.StdoutPipe()
	if e != nil {
		n.t.Fatal(e)
	}
	n.cmd.Stderr = n.err
	if e = n.cmd.Start(); e != nil {
		n.t.Fatal(e)
	}
	first := make(chan string, 1)
	go func() {
		scan := bufio.NewScanner(io.TeeReader(pipe, n.out))
		if scan.Scan() {
			first <- scan.Text()
		}
		for scan.Scan() {
		}
		close(first)
	}()
	go func() { n.done <- n.cmd.Wait() }()
	select {
	case line := <-first:
		addr, ok := strings.CutPrefix(line, "bd serve: listening on http://")
		host, port, e := net.SplitHostPort(strings.TrimSpace(addr))
		number, _ := strconv.Atoi(port)
		if !ok || e != nil || host != "127.0.0.1" || number < 1 || number > 65535 {
			n.t.Fatalf("native actual listener %q", line)
		}
		n.base = "http://" + strings.TrimSpace(addr)
	case <-time.After(90 * time.Second):
		n.t.Fatal("native serve startup deadline")
	}
	n.save("serve-start", map[string]any{"argv": n.cmd.Args, "directory": n.dir, "pid": n.cmd.Process.Pid, "url": n.base})
}
func (n *b21Native) stop() {
	if n.cmd == nil {
		return
	}
	e := n.cmd.Process.Signal(syscall.SIGTERM)
	var wait error
	select {
	case wait = <-n.done:
	case <-time.After(15 * time.Second):
		_ = n.cmd.Process.Kill()
		wait = <-n.done
	}
	n.save("serve-stop", map[string]any{"signal_error": fmt.Sprint(e), "wait_error": fmt.Sprint(wait), "stdout": string(n.out.data()), "stderr": string(n.err.data())})
	n.cmd = nil
	if e != nil || wait != nil {
		n.t.Errorf("native serve shutdown: %v %v", e, wait)
	}
}
func (n *b21Native) http(label, method, path string, body any, status int) map[string]any {
	n.t.Helper()
	raw, e := json.Marshal(body)
	if e != nil {
		n.t.Fatal(e)
	}
	req, e := http.NewRequest(method, n.base+path, bytes.NewReader(raw))
	if e != nil {
		n.t.Fatal(e)
	}
	req.Header.Set("Content-Type", "application/json")
	res, e := (&http.Client{Timeout: 5 * time.Minute}).Do(req)
	if e != nil {
		n.t.Fatal(e)
	}
	data, e := io.ReadAll(io.LimitReader(res.Body, 32<<20))
	closeErr := res.Body.Close()
	n.save(label, map[string]any{"method": method, "url": req.URL.String(), "request": json.RawMessage(raw), "response": json.RawMessage(data), "status": res.StatusCode})
	if e != nil || closeErr != nil || res.StatusCode != status {
		n.t.Fatalf("native HTTP %s status=%d want=%d body=%s errors=%v %v", label, res.StatusCode, status, data, e, closeErr)
	}
	var out map[string]any
	b21Decode(n.t, data, &out)
	return out
}
func (n *b21Native) apply(label string, items ...any) map[string]any {
	return n.http(label, "POST", "/v0/beads/issues:batchApply", map[string]any{"actor": "b21-native", "items": items}, 200)
}
func (n *b21Native) sql(query string) []map[string]any {
	n.t.Helper()
	raw := n.command("readonly-sql", n.binary, "--readonly", "sql", query, "--json")
	var rows []map[string]any
	b21Decode(n.t, raw, &rows)
	return rows
}
func (n *b21Native) history() int {
	n.t.Helper()
	rows := n.sql("SELECT COUNT(*) AS n FROM dolt_log")
	if len(rows) != 1 {
		n.t.Fatal("native history count shape")
	}
	count, e := strconv.Atoi(fmt.Sprint(rows[0]["n"]))
	if e != nil {
		n.t.Fatal(e)
	}
	return count
}
func (n *b21Native) census() map[string]any {
	n.t.Helper()
	out := map[string]any{}
	for _, table := range n.sql("SHOW TABLES") {
		if len(table) != 1 {
			n.t.Fatal("native physical census table shape")
		}
		for _, raw := range table {
			name, ok := raw.(string)
			if !ok || strings.Trim(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != "" {
				n.t.Fatal("native unsafe table name")
			}
			rows := n.sql("SELECT * FROM `" + name + "`")
			sort.Slice(rows, func(i, j int) bool { return mustNativeB21JSON(rows[i]) < mustNativeB21JSON(rows[j]) })
			out[name] = rows
		}
	}
	out["dolt_head"] = n.sql("SELECT commit_hash,message FROM dolt_log ORDER BY commit_hash")
	return out
}
func mustNativeB21JSON(value any) string { raw, _ := json.Marshal(value); return string(raw) }

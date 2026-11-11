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

type b14Native struct {
	t                 *testing.T
	root, dir, binary string
	env               []string
	seq               int
	base              string
	client            *http.Client
	stop              func()
}

type b14NativeIssue struct {
	ID        string   `json:"id"`
	Type      string   `json:"issue_type"`
	Status    string   `json:"status"`
	AwaitType string   `json:"await_type"`
	AwaitID   string   `json:"await_id"`
	Timeout   int64    `json:"timeout"`
	Waiters   []string `json:"waiters"`
	Template  bool     `json:"is_template"`
}

func b14JSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func (n *b14Native) command(label string, args ...string) []byte {
	n.t.Helper()
	n.seq++
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Dir, cmd.Env = n.dir, append([]string(nil), n.env...)
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
	stem := filepath.Join(n.root, fmt.Sprintf("%04d-%s", n.seq, label))
	for suffix, raw := range map[string][]byte{"stdout": out.Bytes(), "stderr": stderr.Bytes()} {
		if e := os.WriteFile(stem+"."+suffix, raw, 0600); e != nil {
			n.t.Fatal(e)
		}
	}
	b14JSON(n.t, stem+".binding.json", map[string]any{"argv": cmd.Args, "directory": cmd.Dir, "environment": cmd.Env, "exit": code, "error": fmt.Sprint(err), "stdout_sha256": hashBytes(out.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes())})
	if err != nil || ctx.Err() != nil || code != 0 {
		n.t.Fatalf("native %s exit=%d error=%v stderr=%s", label, code, err, stderr.Bytes())
	}
	return out.Bytes()
}

func (n *b14Native) serve() {
	n.t.Helper()
	n.seq++
	stem := filepath.Join(n.root, fmt.Sprintf("%04d-server", n.seq))
	stdout, err := os.Create(stem + ".stdout")
	if err != nil {
		n.t.Fatal(err)
	}
	stderr, err := os.Create(stem + ".stderr")
	if err != nil {
		_ = stdout.Close()
		n.t.Fatal(err)
	}
	cmd := exec.Command(n.binary, "serve", "--addr", "127.0.0.1:0")
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = n.dir, append([]string(nil), n.env...), stdout, stderr
	b14JSON(n.t, stem+".request.json", map[string]any{"argv": cmd.Args, "directory": cmd.Dir, "environment": cmd.Env})
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		n.t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	stopped := false
	n.stop = func() {
		if stopped {
			return
		}
		stopped = true
		select {
		case <-done:
		default:
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				n.t.Error(err)
			}
		}
		forced := false
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			forced = true
			_ = cmd.Process.Kill()
			<-done
		}
		outErr, errorsErr := stdout.Close(), stderr.Close()
		b14JSON(n.t, stem+".exit.json", map[string]any{"exit": cmd.ProcessState.ExitCode(), "error": fmt.Sprint(waitErr), "forced": forced})
		if forced || waitErr != nil || outErr != nil || errorsErr != nil {
			n.t.Errorf("native server shutdown forced=%v wait=%v stdout=%v stderr=%v", forced, waitErr, outErr, errorsErr)
		}
	}
	n.t.Cleanup(n.stop)
	n.base = ""
	deadline := time.Now().Add(time.Minute)
	for n.base == "" && time.Now().Before(deadline) {
		select {
		case <-done:
			n.t.Fatal("native server exited before listening", waitErr)
		default:
		}
		raw, err := os.ReadFile(stem + ".stdout")
		if err != nil {
			n.t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "bd serve: listening on ") {
				n.base = strings.TrimPrefix(line, "bd serve: listening on ")
			}
		}
		if n.base == "" {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !strings.HasPrefix(n.base, "http://127.0.0.1:") {
		n.t.Fatal("native loopback address not delivered")
	}
	n.http("health", http.MethodGet, "/healthz", nil, http.StatusOK)
}

func (n *b14Native) http(label, method, path string, body []byte, status int) []byte {
	n.t.Helper()
	n.seq++
	stem := filepath.Join(n.root, fmt.Sprintf("%04d-%s", n.seq, label))
	request, err := http.NewRequest(method, n.base+path, bytes.NewReader(body))
	if err != nil {
		n.t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
		if err := os.WriteFile(stem+".request.json", body, 0600); err != nil {
			n.t.Fatal(err)
		}
	}
	response, err := n.client.Do(request)
	if err != nil {
		n.t.Fatal(err)
	}
	raw, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err := os.WriteFile(stem+".response.json", raw, 0600); err != nil {
		n.t.Fatal(err)
	}
	b14JSON(n.t, stem+".binding.json", map[string]any{"method": method, "url": request.URL.String(), "status": response.StatusCode, "headers": response.Header, "request_sha256": hashBytes(body), "response_sha256": hashBytes(raw)})
	if readErr != nil || closeErr != nil || response.StatusCode != status {
		n.t.Fatalf("native %s status=%d want=%d read=%v close=%v body=%s", label, response.StatusCode, status, readErr, closeErr, raw)
	}
	return raw
}

func (n *b14Native) create(id, typ string, fields map[string]any) b14NativeIssue {
	n.t.Helper()
	body := map[string]any{"actor": "b14-native", "id": id, "title": id, "issue_type": typ}
	for key, value := range fields {
		body[key] = value
	}
	raw, err := json.Marshal(body)
	if err != nil {
		n.t.Fatal(err)
	}
	var issue b14NativeIssue
	if json.Unmarshal(n.http("create-"+id, http.MethodPost, "/v0/beads/issues", raw, http.StatusOK), &issue) != nil || issue.ID != id || issue.Type != typ {
		n.t.Fatal("native create did not return the stored identity/type")
	}
	return issue
}

func (n *b14Native) get(id string) b14NativeIssue {
	n.t.Helper()
	var issue b14NativeIssue
	if json.Unmarshal(n.http("get-"+id, http.MethodGet, "/v0/beads/issues/"+id, nil, http.StatusOK), &issue) != nil || issue.ID != id {
		n.t.Fatal("native get did not return requested issue")
	}
	return issue
}

func (n *b14Native) sql(label, query string) []map[string]json.RawMessage {
	n.t.Helper()
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(n.command(label, "--readonly", "sql", query, "--json"), &rows); err != nil {
		n.t.Fatal(err)
	}
	return rows
}

func (n *b14Native) link(from, to, metadata string) {
	n.t.Helper()
	// Preserve the supplied JSON token on the actual public wire.
	body := []byte(`{"actor":"b14-native","items":[{"kind":"dep_add","dep_add":{"source":{"id":"` + from + `"},"target":{"id":"` + to + `"},"type":"waits-for","metadata":` + metadata + `}}]}`)
	n.http("waits-"+from, http.MethodPost, "/v0/beads/issues:batchApply", body, http.StatusOK)
}

func b14NewNative(t *testing.T, binary, root string) *b14Native {
	t.Helper()
	workspace := t.TempDir()
	dir, home, tmp := filepath.Join(workspace, "project"), filepath.Join(workspace, "home"), filepath.Join(workspace, "tmp")
	for _, path := range []string{dir, home, tmp} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	server := publicnative.StartDolt(t, root)
	transport := &http.Transport{Proxy: nil}
	t.Cleanup(transport.CloseIdleConnections)
	n := &b14Native{t: t, root: root, dir: dir, binary: "/usr/bin/git", client: &http.Client{Timeout: time.Minute, Transport: transport}, env: server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + home, "USER=b14-native", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B14 Public", "GIT_AUTHOR_EMAIL=b14@example.invalid", "GIT_COMMITTER_NAME=B14 Public", "GIT_COMMITTER_EMAIL=b14@example.invalid", "TMPDIR=" + tmp})}
	n.command("git-init", "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	n.binary = binary
	var version struct{ Commit, Build string }
	if json.Unmarshal(n.command("version", "version", "--json"), &version) != nil || version.Commit != pinnedSource.Commit || version.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatal("native version differs from the unchanged canonical build recipe")
	}
	n.command("init", "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b14_current", "--prefix", "b14", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, "b14_current")
	n.serve()
	return n
}

func TestB14CanonicalNativeDefaultAndWaitsCounterparts(t *testing.T) {
	root := filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), "b14-native")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute RunsOn output directory required")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	recorder := nativeRecorder{t: t, root: root, target: "//cmd/bd:bd"}
	n := b14NewNative(t, recorder.buildCanonicalSource(pinnedSource), root)
	native := n.defaultAndWaits()
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			plug := b14Public(t, engine)
			if !reflect.DeepEqual(plug, native) {
				t.Fatalf("paired default/list/waiter/ready projections differ: Plug=%+v native=%+v", plug, native)
			}
			b14JSON(t, filepath.Join(root, "paired-"+engine+".json"), map[string]any{"native": native, "plug": plug, "oracle": "cases_test.go independent literals", "native_history": "current SQL readback and server reopen; Plug history separately asserted"})
		})
	}
}

func (n *b14Native) defaultAndWaits() b14Projection {
	n.t.Helper()
	n.create("b14-target", "task", nil)
	var human b14NativeIssue
	if json.Unmarshal(n.command("gate-create-omitted-await-type", "gate", "create", "--blocks", "b14-target", "--json"), &human) != nil || human.ID == "" || human.Type != "gate" || human.AwaitType != "human" {
		n.t.Fatal("native gate create omission did not select literal gate/human")
	}
	n.command("human-priority", "update", human.ID, "--priority", "4", "--json")
	var bare b14NativeIssue
	if json.Unmarshal(n.command("bare-create", "create", "--id", "b14-bare", "--type", "gate", "--title", "bare", "--priority", "4", "--json"), &bare) != nil || bare.Type != "gate" || bare.AwaitType != "" {
		n.t.Fatal("native bare gate acquired an await default")
	}
	timer := n.create("b14-timer", "gate", map[string]any{"priority": 3, "await_type": "timer", "timeout": 0})
	closed := n.create("b14-closed", "gate", map[string]any{"await_type": "human"})
	n.http("close-gate", http.MethodPost, "/v0/beads/issues/"+closed.ID+":close", []byte(`{"actor":"b14-native"}`), http.StatusOK)
	n.create("b14-ordinary-await", "task", map[string]any{"await_type": "human"})
	formula := filepath.Join(n.root, "b14-template.formula.toml")
	if err := os.WriteFile(formula, []byte("formula = \"b14-template\"\nversion = 1\ntype = \"workflow\"\n[[steps]]\nid = \"step\"\ntitle = \"template\"\n[steps.gate]\ntype = \"human\"\n"), 0600); err != nil {
		n.t.Fatal(err)
	}
	n.command("cook-template", "cook", formula, "--persist", "--json")
	var gates []b14NativeIssue
	if json.Unmarshal(n.command("default-list", "gate", "list", "--limit", "0", "--json"), &gates) != nil {
		n.t.Fatal("invalid native gate list")
	}
	names := map[string]string{bare.ID: "bare", timer.ID: "timer", human.ID: "human", closed.ID: "closed"}
	templates := 0
	for _, gate := range gates {
		if gate.Template {
			names[gate.ID] = "template"
			templates++
		}
	}
	if templates != 1 {
		n.t.Fatalf("actual formula did not persist exactly one template gate: %+v", gates)
	}
	project := func(rows []b14NativeIssue) []string {
		values := []string{}
		for _, row := range rows {
			name, ok := names[row.ID]
			if !ok || row.Type != "gate" {
				n.t.Fatalf("native gate list admitted unexpected row: %+v", row)
			}
			values = append(values, name)
		}
		sort.Strings(values)
		return values
	}
	p := b14Projection{DefaultType: bare.Type, BareAwait: bare.AwaitType, DefaultList: project(gates), Readiness: map[string][]bool{}}
	if json.Unmarshal(n.command("all-list", "gate", "list", "--all", "--limit", "0", "--json"), &gates) != nil {
		n.t.Fatal("invalid native all gate list")
	}
	p.AllList = project(gates)
	if !reflect.DeepEqual(p.DefaultList, []string{"bare", "human", "template", "timer"}) || !reflect.DeepEqual(p.AllList, []string{"bare", "closed", "human", "template", "timer"}) {
		n.t.Fatalf("native default/all membership differs: %+v", p)
	}
	check := n.command("check-excludes-template-before-limit", "gate", "check", "--dry-run", "--limit", "1", "--type", "timer", "--json")
	start := bytes.LastIndex(check, []byte("{\n"))
	var summary struct{ Checked int }
	if start < 0 || json.Unmarshal(check[start:], &summary) != nil || summary.Checked != 1 || !bytes.Contains(check, []byte(timer.ID)) {
		n.t.Fatalf("native template consumed check limit: %s", check)
	}
	p.CheckLimit = []string{"timer"}
	await := n.command("await-filter-after-limit", "gate", "check", "--dry-run", "--limit", "1", "--type", "human", "--json")
	if string(await) != "No open gates of type 'human' found.\n" {
		n.t.Fatalf("native await filter was not after limit: %s", await)
	}
	p.AwaitLimit = []string{}
	for i, values := range [][]string{{"", "b", "a", "b"}, {"next", "next", ""}} {
		body, err := json.Marshal(map[string]any{"actor": "b14-native", "patch": map[string]any{"await_id": "opaque", "waiters": values}})
		if err != nil {
			n.t.Fatal(err)
		}
		n.http(fmt.Sprintf("whole-list-%d", i), http.MethodPatch, "/v0/beads/issues/"+human.ID, body, http.StatusOK)
		if got := n.get(human.ID); got.AwaitID != "opaque" || got.AwaitType != "human" || !reflect.DeepEqual(got.Waiters, values) {
			n.t.Fatalf("native waiter list/member readback differs: %+v", got)
		}
	}
	p.Waiters = []string{"next", "next", ""}
	physical := n.sql("physical-human", "SELECT issue_type,await_type,await_id,timeout_ns,waiters FROM issues WHERE id='"+human.ID+"'")
	if len(physical) != 1 {
		n.t.Fatal("native gate has no exact physical row")
	}
	var stored string
	var storedWaiters []string
	if json.Unmarshal(physical[0]["waiters"], &stored) != nil || json.Unmarshal([]byte(stored), &storedWaiters) != nil || !reflect.DeepEqual(storedWaiters, p.Waiters) {
		n.t.Fatal("native physical waiter JSON differs", physical)
	}
	n.create("b14-spawner", "task", nil)
	for _, predicate := range b14Predicates {
		id := "b14-" + predicate.Name
		n.create(id, "task", nil)
		metadata := map[string]any{}
		if predicate.Gate != "" {
			metadata["gate"] = predicate.Gate
			metadata["also_blocks"] = predicate.Also
		}
		if predicate.Also {
			metadata["also_blocks"] = true
		}
		raw, _ := json.Marshal(metadata)
		n.link(id, "b14-spawner", string(raw))
	}
	checkStage := func(stage int) {
		n.t.Helper()
		var ready []struct{ ID string }
		if json.Unmarshal(n.command("ready-"+b14Stages[stage].Name, "ready", "--json", "--limit", "0", "--include-ephemeral"), &ready) != nil {
			n.t.Fatal("invalid native ready output")
		}
		present := map[string]bool{}
		for _, item := range ready {
			present[item.ID] = true
		}
		got := []bool{}
		for i, predicate := range b14Predicates {
			id := "b14-" + predicate.Name
			got = append(got, present[id])
			if b14Stages[stage].Ready[i] {
				n.http("close-ready-"+id, http.MethodPost, "/v0/beads/issues/"+id+":close", []byte(`{"actor":"b14-native"}`), http.StatusOK)
				n.http("reopen-"+id, http.MethodPost, "/v0/beads/issues/"+id+":reopen", []byte(`{"actor":"b14-native"}`), http.StatusOK)
			} else {
				before := n.sql("before-close-refusal", "SELECT * FROM issues WHERE id='"+id+"'")
				var problem struct{ Code string }
				raw := n.http("close-blocked-"+id, http.MethodPost, "/v0/beads/issues/"+id+":close", []byte(`{"actor":"b14-native"}`), http.StatusConflict)
				if json.Unmarshal(raw, &problem) != nil || problem.Code != "not_closable" || !reflect.DeepEqual(before, n.sql("after-close-refusal", "SELECT * FROM issues WHERE id='"+id+"'")) {
					n.t.Fatal("native close refusal lost typed policy or changed the stored row")
				}
			}
		}
		if !reflect.DeepEqual(got, b14Stages[stage].Ready) {
			n.t.Fatalf("native %s ready=%v want=%v", b14Stages[stage].Name, got, b14Stages[stage].Ready)
		}
		p.Readiness[b14Stages[stage].Name] = got
	}
	checkStage(0)
	n.create("b14-first-child", "task", map[string]any{"parent_id": "b14-spawner"})
	n.create("b14-second-child", "task", map[string]any{"parent_id": "b14-spawner"})
	checkStage(1)
	n.http("close-first", http.MethodPost, "/v0/beads/issues/b14-first-child:close", []byte(`{"actor":"b14-native"}`), http.StatusOK)
	checkStage(2)
	// This new typed waits-for case binds equality to effects and exact native
	// source tokens; the completed generic B07 metadata unit is not invoked.
	before := n.sql("metadata-before", "SELECT * FROM dependencies WHERE issue_id='b14-all'")
	n.link("b14-all", "b14-spawner", `{ "also_blocks":false, "gate":"all-children" }`)
	if !reflect.DeepEqual(before, n.sql("metadata-equal", "SELECT * FROM dependencies WHERE issue_id='b14-all'")) {
		n.t.Fatal("native semantic-equal waits-for metadata changed the original token/identity")
	}
	n.link("b14-all", "b14-spawner", `{"gate":"any-children"}`)
	n.http("close-changed-any", http.MethodPost, "/v0/beads/issues/b14-all:close", []byte(`{"actor":"b14-native"}`), http.StatusOK)
	n.http("reopen-changed-any", http.MethodPost, "/v0/beads/issues/b14-all:reopen", []byte(`{"actor":"b14-native"}`), http.StatusOK)
	n.link("b14-all", "b14-spawner", `{"gate":"all-children"}`)
	checkStage(2)
	beforeHuman := n.get(human.ID)
	n.stop()
	n.serve()
	if !reflect.DeepEqual(beforeHuman, n.get(human.ID)) || !reflect.DeepEqual(physical, n.sql("reopened-physical-human", "SELECT issue_type,await_type,await_id,timeout_ns,waiters FROM issues WHERE id='"+human.ID+"'")) {
		n.t.Fatal("native public server reopen changed persisted gate members")
	}
	checkStage(2)
	n.http("close-second", http.MethodPost, "/v0/beads/issues/b14-second-child:close", []byte(`{"actor":"b14-native"}`), http.StatusOK)
	checkStage(3)
	n.http("close-spawner", http.MethodPost, "/v0/beads/issues/b14-spawner:close", []byte(`{"actor":"b14-native"}`), http.StatusOK)
	checkStage(4)
	b14JSON(n.t, filepath.Join(n.root, "native-independent-projection.json"), p)
	return p
}

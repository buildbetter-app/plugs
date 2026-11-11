package cardb20

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type b23Native struct {
	t                        *testing.T
	root, work, binary, base string
	env                      []string
	cmd                      *exec.Cmd
	done                     chan error
	stdout, stderr           bytes.Buffer
	mu                       sync.Mutex
}

func (n *b23Native) command(label string, args ...string) []byte {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Dir = n.work
	cmd.Env = n.env
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	started := time.Now().UTC()
	err := cmd.Run()
	n.save(label+"-command.json", map[string]any{"argv": cmd.Args, "cwd": cmd.Dir, "environment": cmd.Env, "started": started, "ended": time.Now().UTC(), "stdout": out.String(), "stderr": errOut.String(), "error": fmt.Sprint(err)})
	if err != nil {
		n.t.Fatalf("native %s: %v stdout=%s stderr=%s", label, err, out.Bytes(), errOut.Bytes())
	}
	return out.Bytes()
}
func (n *b23Native) save(label string, value any) {
	n.t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		n.t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(n.root, label), raw, 0600); err != nil {
		n.t.Fatal(err)
	}
}
func (n *b23Native) request(label, method, path string, body any, status int) map[string]json.RawMessage {
	n.t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		n.t.Fatal(err)
	}
	request, err := http.NewRequest(method, n.base+path, bytes.NewReader(raw))
	if err != nil {
		n.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 3 * time.Minute}).Do(request)
	if err != nil {
		n.save(label+"-http.json", map[string]any{"method": method, "url": request.URL.String(), "request": json.RawMessage(raw), "error": err.Error()})
		n.t.Fatal(err)
	}
	reply, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	n.save(label+"-http.json", map[string]any{"method": method, "url": request.URL.String(), "request": json.RawMessage(raw), "status": response.StatusCode, "response": json.RawMessage(reply), "read_error": fmt.Sprint(readErr), "close_error": fmt.Sprint(closeErr)})
	if readErr != nil || closeErr != nil || response.StatusCode != status {
		n.t.Fatalf("native %s status=%d want=%d body=%s %v %v", label, response.StatusCode, status, reply, readErr, closeErr)
	}
	var result map[string]json.RawMessage
	if err = json.Unmarshal(reply, &result); err != nil {
		n.t.Fatal(err)
	}
	return result
}
func (n *b23Native) start() {
	n.t.Helper()
	n.cmd = exec.Command(n.binary, "serve", "--addr", "127.0.0.1:0")
	n.cmd.Dir = n.work
	n.cmd.Env = n.env
	pipe, err := n.cmd.StdoutPipe()
	if err != nil {
		n.t.Fatal(err)
	}
	n.cmd.Stderr = &n.stderr
	if err = n.cmd.Start(); err != nil {
		n.t.Fatal(err)
	}
	n.save("serve-start.json", map[string]any{"argv": n.cmd.Args, "cwd": n.work, "environment": n.env, "pid": n.cmd.Process.Pid})
	first := make(chan string, 1)
	n.done = make(chan error, 1)
	go func() {
		scan := bufio.NewScanner(io.TeeReader(pipe, &n.stdout))
		if scan.Scan() {
			first <- scan.Text()
		}
		for scan.Scan() {
		}
		close(first)
	}()
	go func() { n.done <- n.cmd.Wait() }()
	n.t.Cleanup(func() {
		_ = n.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-n.done:
			if err != nil {
				n.t.Error("native serve exit", err)
			}
		case <-time.After(15 * time.Second):
			_ = n.cmd.Process.Kill()
			n.t.Error("native serve shutdown timed out")
			<-n.done
		}
		n.save("serve-output.json", map[string]any{"stdout": n.stdout.String(), "stderr": n.stderr.String()})
	})
	select {
	case line := <-first:
		address, ok := strings.CutPrefix(line, "bd serve: listening on http://127.0.0.1:")
		if !ok {
			n.t.Fatalf("invalid native listener: %q", line)
		}
		if _, err = strconv.Atoi(strings.TrimSpace(address)); err != nil {
			n.t.Fatal(err)
		}
		n.base = "http://127.0.0.1:" + strings.TrimSpace(address)
	case <-time.After(time.Minute):
		n.t.Fatal("native serve startup timed out")
	}
}
func b23NativeItems(t *testing.T, result map[string]json.RawMessage) []string {
	t.Helper()
	var items []struct{ ID string }
	if err := json.Unmarshal(result["items"], &items); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, r := range items {
		ids = append(ids, r.ID)
	}
	return ids
}
func (n *b23Native) count(label, query string, want int) {
	n.t.Helper()
	body := n.request(label, "GET", "/v0/beads/issues:count?"+query, nil, 200)
	var count int
	if err := json.Unmarshal(body["total"], &count); err != nil || count != want {
		n.t.Fatalf("native count=%s want=%d %v", body["total"], want, err)
	}
}
func (n *b23Native) refuse(label, path, param, reason string) {
	n.t.Helper()
	body := n.request(label, "GET", path, nil, 400)
	for key, want := range map[string]string{"code": "invalid_argument", "param": param, "reason": reason} {
		var got string
		if json.Unmarshal(body[key], &got) != nil || got != want {
			n.t.Fatalf("native %s refusal %s=%s want=%q", label, key, body[key], want)
		}
	}
	for _, key := range []string{"total", "count", "items", "next_cursor"} {
		if _, ok := body[key]; ok {
			n.t.Fatalf("native refusal emitted %s", key)
		}
	}
}

// The native pin is exactly the behavior catalog's beads-base source. The
// unchanged B20 source builder verifies every Git blob and records the build.
// This is a fresh public fixture recording, never official/native admission.
func TestB23FreshNativeCombinedPredicateCountsAndOrderedKeysets(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("declared absolute output root required")
	}
	r := &nativeRecorder{t: t, root: root, target: "//cmd/bd:bd"}
	binary := r.buildCanonicalSource(pinnedSource)
	work := t.TempDir()
	for _, part := range []string{"home", "tmp", "project"} {
		if err := os.Mkdir(filepath.Join(work, part), 0700); err != nil {
			t.Fatal(err)
		}
	}
	server := publicnative.StartDolt(t, root)
	n := &b23Native{t: t, root: root, work: filepath.Join(work, "project"), binary: binary}
	n.env = server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + filepath.Join(work, "home"), "TMPDIR=" + filepath.Join(work, "tmp"), "USER=b23-native", "TZ=UTC", "LC_ALL=C", "CI=true", "NO_COLOR=1", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "BEADS_ACTOR=b23-recorder", "BEADS_NO_DAEMON=1"})
	git := exec.Command("/usr/bin/git", "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	git.Dir = n.work
	git.Env = n.env
	if out, err := git.CombinedOutput(); err != nil {
		t.Fatal(err, string(out))
	}
	n.command("version", "version", "--json")
	n.command("init", "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b23_query", "--prefix", "b23", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, "b23_query")
	n.start()
	rows := []struct {
		title, status string
		labels        []string
	}{
		{"Needle twin", "open", []string{"Alpha", "shared"}}, {"Needle twin", "open", []string{"Alpha", "gamma", "shared"}}, {"needle lower", "open", []string{"alpha", "shared"}}, {"Needle closed", "closed", []string{"Alpha", "shared"}}, {"Needle other", "open", []string{"gamma"}}, {"ordinary", "open", []string{"Alpha", "shared"}},
	}
	items := []any{}
	for i, row := range rows {
		items = append(items, map[string]any{"kind": "create", "create": map[string]any{"id": queryIDs[i], "title": row.title, "status": row.status, "issue_type": "task", "labels": row.labels, "priority": 2}})
	}
	seed := n.request("seed", "POST", "/v0/beads/issues:batchApply", map[string]any{"actor": "b23-recorder", "items": items}, 200)
	var committed []struct{ Changed bool }
	if json.Unmarshal(seed["items"], &committed) != nil || len(committed) != 6 {
		t.Fatal("native seed lost results")
	}
	for _, item := range committed {
		if !item.Changed {
			t.Fatal("native seed did not create")
		}
	}
	vectors := []struct {
		name, query string
		want        []string
	}{
		{"all-intersection", "title=Needle&label=Alpha&label=shared&status=open&type=task", []string{queryIDs[1], queryIDs[0]}},
		{"any-intersection", "title=Needle&label_any=Alpha&label_any=gamma&status=open&type=task", []string{queryIDs[1], queryIDs[0], queryIDs[4]}},
		{"case-exact-label", "title=needle&label=alpha&status=open&type=task", []string{queryIDs[2]}},
		{"closed", "title=Needle&label=Alpha&status=closed&type=task", []string{queryIDs[3]}},
		{"empty-label", "label=absent", []string{}},
	}
	for _, v := range vectors {
		body := n.request(v.name+"-list", "GET", "/v0/beads/issues?all=true&limit=1000&"+v.query, nil, 200)
		got := b23NativeItems(t, body)
		// Native list keysets have their own created/priority order. Use exact
		// public SQL ordered IDs as the independent native order oracle below;
		// for predicate membership, compare a literal set without changing pages.
		membership := map[string]bool{}
		for _, id := range got {
			if membership[id] {
				t.Fatal("native duplicate id")
			}
			membership[id] = true
		}
		if len(got) != len(v.want) {
			t.Fatalf("native %s membership=%v want=%v", v.name, got, v.want)
		}
		for _, id := range v.want {
			if !membership[id] {
				t.Fatalf("native %s missing %s", v.name, id)
			}
		}
		n.count(v.name+"-count", v.query, len(v.want))
	}
	// No count parity is inferred for native features with a different closed
	// vocabulary: exclusion is rejected, status OR is one unknown scalar.
	n.refuse("exclude-count", "/v0/beads/issues:count?exclude_label=gamma", "exclude_label", "unknown_parameter")
	n.count("status-or-count", "status=open%2Cclosed", 0)
	excluded := n.request("exclude-list", "GET", "/v0/beads/issues?all=true&limit=1000&title=Needle&label_any=Alpha&label_any=gamma&exclude_label=gamma&status=open&type=task", nil, 200)
	if !reflect.DeepEqual(b23NativeItems(t, excluded), []string{queryIDs[0]}) {
		t.Fatal("native combined exclusion mismatch", excluded)
	}
	// Freeze the original native ordered SQL expectation separately from the
	// HTTP page stream. Never sort the concatenated HTTP IDs after the fact.
	sql := n.command("ordered-native-sql", "--readonly", "sql", "SELECT id FROM issues ORDER BY priority ASC, created_at DESC, id ASC", "--json")
	var oracle []struct{ ID string }
	if err := json.Unmarshal(sql, &oracle); err != nil {
		t.Fatal(err)
	}
	want := []string{}
	for _, row := range oracle {
		want = append(want, row.ID)
	}
	if len(want) != 6 {
		t.Fatal("native order oracle lost rows")
	}
	got := []string{}
	seen := map[string]bool{}
	cursor := ""
	for page := 0; page <= 6; page++ {
		body := n.request(fmt.Sprintf("keyset-%d", page), "GET", "/v0/beads/issues?all=true&sort=priority&limit=1&cursor="+url.QueryEscape(cursor), nil, 200)
		ids := b23NativeItems(t, body)
		for _, id := range ids {
			if seen[id] {
				t.Fatal("native keyset repeated", id)
			}
			seen[id] = true
			got = append(got, id)
		}
		var next string
		_ = json.Unmarshal(body["next_cursor"], &next)
		var more bool
		if err := json.Unmarshal(body["has_more"], &more); err != nil {
			t.Fatal(err)
		}
		if !more {
			if next != "" || !reflect.DeepEqual(got, want) {
				t.Fatalf("native ordered keysets=%v want=%v next=%q", got, want, next)
			}
			break
		}
		if next == "" || next == cursor || len(ids) != 1 {
			t.Fatal("native keyset did not advance")
		}
		cursor = next
		if page == 6 {
			t.Fatal("native keyset did not terminate")
		}
	}
	natural := n.command("natural-id-list", "list", "--all", "--sort", "id", "--json")
	var display []struct{ ID string }
	if err := json.Unmarshal(natural, &display); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, row := range display {
		ids = append(ids, row.ID)
	}
	if !reflect.DeepEqual(ids, []string{queryIDs[5], queryIDs[0], queryIDs[2], queryIDs[1], queryIDs[3], queryIDs[4]}) {
		t.Fatalf("native natural 2/10 order=%v", ids)
	}
	n.refuse("http-id-sort", "/v0/beads/issues?sort=id", "sort", "invalid_value")
	// Each actual Plug engine executes the equivalent supported compound
	// predicates. Ordinary native and Plug vocabulary are independently named;
	// count wire shape and title/sort differences are never silently normalized.
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("plug-"+engine, func(t *testing.T) {
			f := open(t, engine)
			seedQuery(t, f)
			for _, i := range []int{0, 2, 3, 4, 8} {
				c := queryCases()[i]
				d := queryDefinition(c, 1)
				d["sort"] = []string{"id"}
				queryPages(t, f, "native-paired-"+c.name, d, nil, c.want)
				queryCount(t, f, "native-paired-count-"+c.name, d, nil, len(c.want))
			}
			f.reopen()
			c := queryCases()[0]
			queryCount(t, f, "native-paired-reopened", queryDefinition(c, 1), nil, 2)
		})
	}
	n.save("native-plug-disposition.json", map[string]any{"native_source": pinnedSource, "native_ordered_sql": json.RawMessage(sql), "native_ordered_http_ids": got, "native_natural_cli_ids": ids, "plug_sort_contract": "surfaces.md: Existing title matching and sorting stay unchanged", "native_count_exclusion": "typed unknown_parameter", "native_status_or_count": 0, "native_source_history_claim": false, "official_admission": "UNSPENT"})
}

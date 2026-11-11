package cardb20

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// All writes use the pinned native CLI or its public HTTP batch route. SQL is
// read-only and retains raw cells. This fixture adds only the comparison domains
// absent from the completed B07 ordinary semantic metadata originals.
type b11Native struct {
	t                 *testing.T
	root, dir, binary string
	env               []string
	seq               int
	tables            []string
}
type b11Image map[string][]map[string]json.RawMessage

const nativeDependencyNamespace = "bdd74eb9a20a554a985b54b8f3e64d8b"

// nativeDependencyID independently projects the pinned native UUIDv5 rule.
// The dependency type is deliberately absent from the name; only the source,
// 0x1f separator and target endpoint identify the physical row.
func nativeDependencyID(issueID, target string) string {
	namespace, err := hex.DecodeString(nativeDependencyNamespace)
	if err != nil {
		panic(err)
	}
	h := sha1.New()
	_, _ = h.Write(namespace)
	_, _ = h.Write([]byte(issueID))
	_, _ = h.Write([]byte{0x1f})
	_, _ = h.Write([]byte(target))
	digest := h.Sum(nil)
	digest[6] = (digest[6] & 0x0f) | 0x50
	digest[8] = (digest[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%s", digest[0:4], digest[4:6], digest[6:8], digest[8:10], hex.EncodeToString(digest[10:]))
}

func b11JSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
func b11Output(t *testing.T, name string) string {
	t.Helper()
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute undeclared output directory required")
	}
	path := filepath.Join(root, strings.ReplaceAll(t.Name(), "/", "_"), name)
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
func b11Text(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func (n *b11Native) command(label string, fail bool, args ...string) []byte {
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
	b11JSON(n.t, stem+".request.json", map[string]any{"argv": cmd.Args, "directory": cmd.Dir, "environment": cmd.Env, "exit": code, "error": fmt.Sprint(err), "stdout_sha256": hashBytes(out.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes())})
	if ctx.Err() != nil || code < 0 || (!fail && code != 0) || (fail && code == 0) {
		n.t.Fatalf("native %s exit=%d error=%v stderr=%s", label, code, err, stderr.Bytes())
	}
	return out.Bytes()
}
func (n *b11Native) image(label string) b11Image {
	n.t.Helper()
	image := b11Image{}
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
	b11JSON(n.t, filepath.Join(n.root, fmt.Sprintf("image-%04d-%s.json", n.seq, label)), image)
	return image
}
func (n *b11Native) equal(want b11Image, label string) {
	n.t.Helper()
	got := n.image(label)
	if !reflect.DeepEqual(got, want) {
		n.t.Fatal("native no-op/refusal changed complete current user-table state")
	}
}
func (n *b11Native) edge(image b11Image, from, to string) map[string]json.RawMessage {
	n.t.Helper()
	var found map[string]json.RawMessage
	for _, row := range image["dependencies"] {
		if b11Text(n.t, row["issue_id"]) == from && b11Text(n.t, row["depends_on_issue_id"]) == to {
			if found != nil {
				n.t.Fatal("duplicate native endpoint pair")
			}
			found = row
		}
	}
	if found == nil {
		n.t.Fatal("native dependency absent", from, to)
	}
	return found
}
func b11NewNative(t *testing.T, binary, name string) *b11Native {
	root := b11Output(t, "native-"+name)
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
	n := &b11Native{t: t, root: root, dir: dir, binary: binary, env: server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + home, "USER=b11-native", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B11 Public", "GIT_AUTHOR_EMAIL=b11@example.invalid", "GIT_COMMITTER_NAME=B11 Public", "GIT_COMMITTER_EMAIL=b11@example.invalid", "TMPDIR=" + tmp})}
	n.binary = "/usr/bin/git"
	n.command("git-init", false, "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=")
	n.binary = binary
	raw := n.command("version", false, "version", "--json")
	var identity struct{ Commit, Build string }
	if json.Unmarshal(raw, &identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit {
		t.Fatal("native executable identity differs from unchanged canonical build recipe")
	}
	n.command("init", false, "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b11_current", "--prefix", "b11", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, "b11_current")
	n.command("journal-enable", false, "config", "set", "events-journal", "true", "--json")
	n.command("journal-prune-disable", false, "config", "set", "events-journal-auto-prune", "false", "--json")
	raw = n.command("actual-table-catalog", false, "--readonly", "sql", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'b11_current' AND table_type = 'BASE TABLE' ORDER BY table_name", "--json")
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
	b11JSON(t, filepath.Join(root, "actual-tables.json"), n.tables)
	return n
}

func (n *b11Native) metadataAPI(name string) (func(string, string, string, string), func()) {
	n.t.Helper()
	prefix := filepath.Join(n.root, name)
	stdoutPath, stderrPath := prefix+"-metadata-server.stdout", prefix+"-metadata-server.stderr"
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
	b11JSON(n.t, prefix+"-metadata-server.request.json", map[string]any{"argv": cmd.Args, "directory": cmd.Dir, "environment": cmd.Env})
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		n.t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
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
			b11JSON(n.t, prefix+"-metadata-server.exit.json", map[string]any{"exit": cmd.ProcessState.ExitCode(), "error": fmt.Sprint(waitErr), "forced": forced, "stdout_sha256": hashBytes(out), "stderr_sha256": hashBytes(errorsRaw)})
			if forced || waitErr != nil || outErr != nil || errorsErr != nil {
				n.t.Errorf("native HTTP cleanup: forced=%v wait=%v stdout=%v stderr=%v", forced, waitErr, outErr, errorsErr)
			}
		})
	}
	n.t.Cleanup(stop)
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
	if err := os.WriteFile(prefix+"-metadata-server.health.json", healthRaw, 0600); err != nil {
		n.t.Fatal(err)
	}
	b11JSON(n.t, prefix+"-metadata-server.readiness.json", map[string]any{"method": "GET", "url": base + "/healthz", "status": health.StatusCode, "headers": health.Header, "response_sha256": hashBytes(healthRaw)})
	if readErr != nil || closeErr != nil || health.StatusCode != http.StatusOK {
		n.t.Fatal("native HTTP readiness failed", health.StatusCode, readErr, closeErr)
	}
	apply := func(label, from, to, metadata string) {
		n.t.Helper()
		// Build the wire body without re-encoding the supplied metadata token.
		body := []byte(`{"actor":"b11-native","items":[{"kind":"dep_add","dep_add":{"source":{"id":"` + from + `"},"target":{"id":"` + to + `"},"type":"related","metadata":` + metadata + `}}]}`)
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
		b11JSON(n.t, stem+".binding.json", map[string]any{"method": "POST", "url": url, "status": response.StatusCode, "headers": response.Header, "request_sha256": hashBytes(body), "response_sha256": hashBytes(raw)})
		if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK {
			n.t.Fatalf("native HTTP metadata status=%d read=%v close=%v response=%s", response.StatusCode, readErr, closeErr, raw)
		}
		var result struct {
			Items []struct {
				Kind    string
				Changed bool
			}
		}
		if json.Unmarshal(raw, &result) != nil || len(result.Items) != 1 || result.Items[0].Kind != "dep_add" || result.Items[0].Changed {
			n.t.Fatalf("native HTTP metadata changed outcome differs: %s", raw)
		}
	}

	return apply, stop
}

// The existing server binding supplies the actual loopback endpoint. Native's
// related-edge admission accepts syntactically valid overflow numbers, but its
// SQL JSON conversion fails. The public fallback is 500/internal, without the
// typed item fields used by validation refusals. Preserve its literal response.
func (n *b11Native) metadataOverflow(label, from, to, metadata string) {
	n.t.Helper()
	readiness, err := os.ReadFile(filepath.Join(n.root, "first-open-metadata-server.readiness.json"))
	if err != nil {
		n.t.Fatal(err)
	}
	var binding struct{ URL string }
	if json.Unmarshal(readiness, &binding) != nil || !strings.HasPrefix(binding.URL, "http://127.0.0.1:") || !strings.HasSuffix(binding.URL, "/healthz") {
		n.t.Fatal("actual native HTTP endpoint binding missing")
	}
	url := strings.TrimSuffix(binding.URL, "/healthz") + "/v0/beads/issues:batchApply"
	body := []byte(`{"actor":"b11-native","items":[{"kind":"dep_add","dep_add":{"source":{"id":"` + from + `"},"target":{"id":"` + to + `"},"type":"related","metadata":` + metadata + `}}]}`)
	stem := filepath.Join(n.root, label)
	if err := os.WriteFile(stem+".request.json", body, 0600); err != nil {
		n.t.Fatal(err)
	}
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: time.Minute, Transport: transport}
	response, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		n.t.Fatal(err)
	}
	raw, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err := os.WriteFile(stem+".response.json", raw, 0600); err != nil {
		n.t.Fatal(err)
	}
	b11JSON(n.t, stem+".binding.json", map[string]any{"method": "POST", "url": url, "status": response.StatusCode, "headers": response.Header, "request_sha256": hashBytes(body), "response_sha256": hashBytes(raw)})
	var problem struct {
		Status              int
		Code, Title, Detail string
	}
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusInternalServerError || json.Unmarshal(raw, &problem) != nil || problem.Status != http.StatusInternalServerError || problem.Code != "internal" || problem.Title != "Internal Server Error" || problem.Detail != "internal server error" {
		n.t.Fatalf("native overflow outcome differs: status=%d read=%v close=%v response=%s", response.StatusCode, readErr, closeErr, raw)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		n.t.Fatal(err)
	}
	for _, member := range []string{"param", "reason", "item_index", "item_kind", "item_key", "item_issue_id"} {
		if _, present := members[member]; present {
			n.t.Fatal("native generic failure unexpectedly carried typed item validation", member)
		}
	}
}

func (n *b11Native) metadataDelta(before, after b11Image, from, to, want string) {
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
			prior, next := n.edge(before, from, to), n.edge(after, from, to)
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
			if len(added) != 1 || b11Text(n.t, added[0]["op"]) != "dep_add" || b11Text(n.t, added[0]["issue_id"]) != from || b11Text(n.t, added[0]["actor"]) != "b11-native" {
				n.t.Fatal("metadata journal delta differs", added)
			}
			var dep struct{ Kind, Target, Metadata string }
			if json.Unmarshal([]byte(b11Text(n.t, added[0]["dep_json"])), &dep) != nil || dep.Kind != "related" || dep.Target != to || dep.Metadata != want {
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

// Emit the existing lexicon registry's exact AST bindings on RunsOn. Only the
// three immutable native protocol literals are selected; no method is exempted.
func b11ProtocolBinding(t *testing.T, root string) {
	t.Helper()
	path := "spec/cardb11/native_test.go"
	source, err := os.ReadFile(filepath.Join(os.Getenv("PLUG_SOURCE_MODULE_ROOT"), path))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	wanted := map[string]bool{
		"32d6f0a5c7dd84ad27ec70dc3497538db281818520ce79b73eebf16c7672ee47": true,
		"91f880d51578af7df6289315f0b8e9e7e5c44e86b33f48900c26f031a0cc6f75": true,
		"93602e05a1bff1bd127187c713caaeab338848eb4a6cba051123440a912cdf26": true,
	}
	var out strings.Builder
	fmt.Fprintf(&out, "\t%q: {\n", path)
	count := 0
	for _, declaration := range file.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if !ok || (method.Name.Name != "metadataAPI" && method.Name.Name != "metadataDelta") {
			continue
		}
		start, end := fset.Position(method.Pos()).Offset, fset.Position(method.End()).Offset
		fmt.Fprintf(&out, "\t\t{name: %q, kind: \"func\", contextSHA256: %q, literals: map[int]string{\n", method.Name.Name, hashBytes(source[start:end]))
		ast.Inspect(method, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil || !wanted[hashBytes([]byte(value))] {
				return true
			}
			fmt.Fprintf(&out, "\t\t\t%d: %q,\n", fset.Position(literal.Pos()).Offset-start, hashBytes([]byte(value)))
			count++
			return true
		})
		out.WriteString("\t\t}},\n")
	}
	out.WriteString("\t},\n")
	if count != 3 {
		t.Fatalf("native protocol AST sites=%d want=3", count)
	}
	if err := os.WriteFile(filepath.Join(root, "native-literal-binding.txt"), []byte(out.String()), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestB11NativePublicMetadataComparisonDomains(t *testing.T) {
	// Expectations derive independently from native JCS, SQL JSON conversion,
	// and the journal's string-valued EventDep.Metadata. Request spelling and
	// stored SQL spelling are deliberately distinct observations.
	cases := []struct {
		Name, Initial, InitialStored, Incoming, Stored string
		Equal, RepeatDuplicate                         bool
	}{
		{"wide-rounding", `{"n":9007199254740993}`, `{"n":9007199254740992}`, `{"n":9007199254740992}`, `{"n":9007199254740992}`, true, false},
		{"wide-distinct", `{"n":9007199254740993}`, `{"n":9007199254740992}`, `{"n":9007199254740994}`, `{"n":9007199254740994}`, false, false},
		{"duplicate-fallback", `{"x":2}`, `{"x":2}`, `{"x":1,"x":2}`, `{"x":2}`, false, true},
	}
	overflows := []string{`{"n":1e999999999}`, `{"n":10e999999998}`}
	// Every process, including the unchanged nested source builder, inherits
	// the worker-safe prefix. Native child environments carry it explicitly too.
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	root := b11Output(t, "build")
	b11ProtocolBinding(t, root)
	r := &nativeRecorder{t: t, root: root}
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	for _, key := range []string{"PLUG_B11_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatalf("missing actual run binding %s", key)
		}
	}
	actual := strings.TrimSpace(string(r.buildCommand(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if actual != os.Getenv("PLUG_B11_REQUEST_SHA") {
		t.Fatal("candidate request mismatch")
	}
	r.buildCommand(checkout, "candidate-tree-parents", "/usr/bin/git", []string{"show", "-s", "--format=%H %T %P", "HEAD"})
	raw, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(raw) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("canonical native protocol differs")
	}
	var protocol struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(raw, &protocol) != nil || protocol.ExporterLabel == "" {
		t.Fatal("canonical native build target absent")
	}
	r.target = protocol.ExporterLabel
	b11JSON(t, filepath.Join(root, "run-binding.json"), map[string]any{
		"candidate": actual, "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"),
		"workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"),
		"native_source": pinnedSource, "expectations": cases, "qualification": "PUBLIC_OPERATIONAL_RECORDING_ONLY",
		"overflow_requests": overflows, "overflow_status": http.StatusInternalServerError, "overflow_code": "internal",
		"native_history_credit": false,
	})
	binary := r.buildCanonicalSource(pinnedSource)
	for _, twin := range []string{"a", "b"} {
		t.Run("native_"+twin, func(t *testing.T) {
			n := b11NewNative(t, binary, twin)
			for i := range cases {
				from, to := fmt.Sprintf("b11-%d", i*2+1), fmt.Sprintf("b11-%d", i*2+2)
				for _, id := range []string{from, to} {
					n.command("create-"+id, false, "create", "--id", id, "--type", "task", "--title", id, "--json")
				}
				n.command("initial-edge-"+from, false, "dep", "add", from, to, "--type", "related", "--json")
			}
			apply, stop := n.metadataAPI("first-open")
			for i, test := range cases {
				from, to := fmt.Sprintf("b11-%d", i*2+1), fmt.Sprintf("b11-%d", i*2+2)
				initialBefore := n.image(test.Name + "-initial-before")
				wantID := nativeDependencyID(from, to)
				if got := b11Text(t, n.edge(initialBefore, from, to)["id"]); got != wantID {
					t.Fatalf("native initial endpoint UUID differs: %q want %q", got, wantID)
				}
				apply(test.Name+"-initial", from, to, test.Initial)
				before := n.image(test.Name + "-before")
				n.metadataDelta(initialBefore, before, from, to, test.Initial)
				if got := b11Text(t, n.edge(before, from, to)["id"]); got != wantID {
					t.Fatalf("native public re-add changed endpoint UUID: %q want %q", got, wantID)
				}
				if got := b11Text(t, n.edge(before, from, to)["metadata"]); got != test.InitialStored {
					t.Fatalf("native initial SQL metadata differs: %q want=%q", got, test.InitialStored)
				}
				apply(test.Name+"-readd", from, to, test.Incoming)
				after := n.image(test.Name + "-after")
				if test.Equal {
					if !reflect.DeepEqual(before, after) {
						t.Fatal("native semantic re-add changed complete current table image", test.Name)
					}
				} else {
					n.metadataDelta(before, after, from, to, test.Incoming)
				}
				if got := b11Text(t, n.edge(after, from, to)["id"]); got != wantID {
					t.Fatalf("native metadata re-add changed endpoint UUID: %q want %q", got, wantID)
				}
				if got := b11Text(t, n.edge(after, from, to)["metadata"]); got != test.Stored {
					t.Fatalf("native SQL metadata readback differs: %q want=%q", got, test.Stored)
				}
				if test.RepeatDuplicate {
					// Stored x2 still differs by the JCS byte fallback from the
					// repeated duplicate request: it writes/journals again.
					apply(test.Name+"-repeat-duplicate", from, to, test.Incoming)
					repeated := n.image(test.Name + "-repeat-after")
					n.metadataDelta(after, repeated, from, to, test.Incoming)
					if got := b11Text(t, n.edge(repeated, from, to)["metadata"]); got != test.Stored {
						t.Fatalf("native repeated duplicate SQL metadata differs: %q", got)
					}
				}
				b11JSON(t, filepath.Join(n.root, test.Name+"-outcome.json"), map[string]any{
					"initial_request": test.Initial, "initial_stored": test.InitialStored, "incoming_request": test.Incoming, "equal": test.Equal,
					"stored": test.Stored, "before_edge": n.edge(before, from, to), "after_edge": n.edge(after, from, to),
					"native_response_changed": false, "native_history_credit": false,
				})
			}
			for i, value := range overflows {
				label := fmt.Sprintf("overflow-%d", i)
				before := n.image(label + "-before")
				n.metadataOverflow(label, "b11-1", "b11-2", value)
				n.equal(before, label+"-after")
			}
			before := n.image("before-close")
			stop()
			reopened, closeReopened := n.metadataAPI("sql-reopen")
			for i, test := range cases {
				from, to := fmt.Sprintf("b11-%d", i*2+1), fmt.Sprintf("b11-%d", i*2+2)
				// Native has no request receipt here: re-add the canonical
				// committed SQL value, not an invalid-JCS request spelling.
				reopened(test.Name+"-reopen-canonical-readd", from, to, test.Stored)
			}
			n.equal(before, "after-reopen-replays")
			closeReopened()
		})
	}
}

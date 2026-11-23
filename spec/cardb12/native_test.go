package cardb12

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// The B12 native recording uses the exact pinned Beads source (9d6fff76),
// built by the shared canonical recipe, against an owned Dolt sql-server.
// Every write is a public `bd` command. SQL is read-only observation.
//
// Native AI provider stubbed via public ai.base_url; summary content not under
// test; Beads source unmodified (pinned bd sha recorded in the build receipt).
// The stub binds 127.0.0.1 on an ephemeral port and answers with fixed text.
// Every bd child also routes HTTP(S) proxying to a loopback sink, so any
// non-loopback HTTP(S) attempt is observed and fails the recording.
var pinnedSource = publicnative.CanonicalSource{
	Name: "beads-9d6fff", Commit: "9d6fff760b7a314b476539198504d77ce0959656",
	Tree: "cde121d6e4119e454857ad1c3ca293e2791be840", Parent: "cadad184cefce7da159ea4b18c9bd44865e72f6b",
	ArchiveSHA:  "8aa8d07655ec6903703a4c94a4b1aa12d4fb509ab647cd1958f73f8342d3fe6e",
	ManifestSHA: "23d84a56302ad19c50d9d25af733b2da50f9137aaf9308b273cab039b6023ae3", Entries: 5087,
}

const (
	b12DummyKey     = "b12-loopback-dummy-key-not-a-secret"
	b12LongMarker   = "LONG-SUMMARY-FIXTURE"
	b12StubSummary  = "Stub summary: closed public fixture."
	b12VectorsPath  = "testdata/b12-native-vectors.json"
	b12NativeTarget = "//cmd/bd:bd"
	// The switch trial's pinned binary (also bound by converterroundtrip B27).
	b12TrialBinary    = "/tmp/plug-swarm/work/restore-roundtrip-r2-20261031/" + "b" + "d"
	b12TrialBinarySHA = "9370e9a9234aed2697403a59a7d0c65b6d261c99720d386f5ad446535fc0bdd5"
)

// b12LongStubSummary is longer than every fixture's text, so native's real
// summary-not-smaller check refuses on the --auto path.
var b12LongStubSummary = strings.Repeat("Deliberately longer stub summary. ", 80)

// Fixture text is public and fixed. Sizes are chosen so the supplied tier-one
// summary is smaller than the original and the tier-two summary is smaller
// than the tier-one summary.
func b12Fixture(id, title string) map[string]any {
	return map[string]any{
		"id": id, "title": title, "status": "closed", "priority": 2, "issue_type": "task",
		"description":         "Original public description for " + id + ". It is deliberately long enough to be compacted by a shorter summary.",
		"design":              "Original public design for " + id + ".",
		"notes":               "Original public notes for " + id + ".",
		"acceptance_criteria": "Original public acceptance criteria for " + id + ".",
		"created_at":          "2020-01-01T00:00:00Z", "updated_at": "2020-01-02T00:00:00Z", "closed_at": "2020-01-02T00:00:00Z",
	}
}

// NativeIssue is the deterministic per-record projection; physical timestamps
// are reduced to presence so the committed vectors are rerun-stable.
type NativeIssue struct {
	ID                 string `json:"id"`
	Status             string `json:"status"`
	Description        string `json:"description"`
	Design             string `json:"design"`
	Notes              string `json:"notes"`
	AcceptanceCriteria string `json:"acceptance_criteria"`
	CompactionLevel    string `json:"compaction_level"`
	Compacted          bool   `json:"compacted_at_present"`
	OriginalSize       string `json:"original_size"`
}

type NativeSnapshot struct {
	IssueID         string         `json:"issue_id"`
	CompactionLevel string         `json:"compaction_level"`
	Content         map[string]any `json:"snapshot"`
}

type NativeComment struct {
	IssueID string `json:"issue_id"`
	Actor   string `json:"actor"`
	Text    string `json:"comment"`
}

type StubRequest struct {
	Method       string   `json:"method"`
	Path         string   `json:"path"`
	Model        string   `json:"model"`
	MaxTokens    int      `json:"max_tokens"`
	Roles        []string `json:"roles"`
	PromptBytes  int      `json:"prompt_bytes"`
	PromptSHA256 string   `json:"prompt_sha256"`
	DummyKey     bool     `json:"dummy_key_header"`
	LongVariant  bool     `json:"long_variant"`
}

// NativeVector is one public native command and its observed effect.
type NativeVector struct {
	Name             string           `json:"name"`
	Args             []string         `json:"args"`
	AIEnvironment    bool             `json:"ai_environment"`
	Exit             int              `json:"exit"`
	Stdout           any              `json:"stdout"`
	Stderr           string           `json:"stderr"`
	Before           []NativeIssue    `json:"before"`
	After            []NativeIssue    `json:"after"`
	SnapshotsAfter   []NativeSnapshot `json:"snapshots_after"`
	CommentsAdded    []NativeComment  `json:"comments_added"`
	StubRequests     []StubRequest    `json:"stub_requests"`
	FullImageChanged bool             `json:"full_image_changed"`
}

type b12Native struct {
	t                    *testing.T
	root, dir, binary    string
	plainEnv, aiEnv      []string
	seq                  int
	stub                 *b12Stub
	sink                 *b12Sink
	summaryFiles         map[string]string
	vectors              []NativeVector
	tableName            *regexp.Regexp
	outboundAttemptsSeen int
}

func b12JSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func b12Hash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func b12Decode(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	return d.Decode(target)
}

// b12Stub answers the Anthropic Messages shape that the pinned native client
// sends. It records request shape, never forwards anything, and is loopback.
type b12Stub struct {
	mu       sync.Mutex
	requests []StubRequest
	prompts  []string
	server   *http.Server
	addr     string
}

func startStub(t *testing.T) *b12Stub {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &b12Stub{addr: listener.Addr().String()}
	s.server = &http.Server{Handler: http.HandlerFunc(s.serve), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = s.server.Serve(listener) }()
	t.Cleanup(func() { _ = s.server.Close() })
	return s
}

func (s *b12Stub) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var body struct {
		Model     string `json:"model"`
		MaxTokens int    `json:"max_tokens"`
		Messages  []struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"messages"`
	}
	_ = json.Unmarshal(raw, &body)
	request := StubRequest{Method: r.Method, Path: r.URL.Path, Model: body.Model, MaxTokens: body.MaxTokens, DummyKey: r.Header.Get("X-Api-Key") == b12DummyKey}
	prompt := ""
	for _, m := range body.Messages {
		request.Roles = append(request.Roles, m.Role)
		for _, c := range m.Content {
			prompt += c.Text
		}
	}
	request.PromptBytes, request.PromptSHA256 = len(prompt), b12Hash([]byte(prompt))
	request.LongVariant = strings.Contains(prompt, b12LongMarker)
	summary := b12StubSummary
	if request.LongVariant {
		summary = b12LongStubSummary
	}
	s.mu.Lock()
	s.requests = append(s.requests, request)
	s.prompts = append(s.prompts, prompt)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": "msg_b12_loopback_stub", "type": "message", "role": "assistant", "model": body.Model,
		"content":     []map[string]any{{"type": "text", "text": summary}},
		"stop_reason": "end_turn", "stop_sequence": nil,
		"usage": map[string]any{"input_tokens": len(prompt) / 4, "output_tokens": len(summary) / 4},
	})
}

func (s *b12Stub) take() ([]StubRequest, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	requests, prompts := s.requests, s.prompts
	s.requests, s.prompts = nil, nil
	sort.SliceStable(requests, func(i, j int) bool { return requests[i].PromptSHA256 < requests[j].PromptSHA256 })
	return requests, prompts
}

// b12Sink is the HTTP(S) proxy for every bd child. Go does not proxy
// loopback destinations, so only a non-loopback attempt can reach it.
type b12Sink struct {
	mu       sync.Mutex
	attempts []string
	addr     string
	listener net.Listener
}

func startSink(t *testing.T) *b12Sink {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &b12Sink{addr: listener.Addr().String(), listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			line, _ := bufio.NewReader(conn).ReadString('\n')
			s.mu.Lock()
			s.attempts = append(s.attempts, strings.TrimSpace(line))
			s.mu.Unlock()
			_, _ = io.WriteString(conn, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return s
}

func (s *b12Sink) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.attempts...)
}

func (n *b12Native) run(label string, ai bool, stdin []byte, args ...string) (int, []byte, []byte) {
	n.t.Helper()
	n.seq++
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.binary, args...)
	cmd.Dir = n.dir
	cmd.Env = append([]string(nil), n.plainEnv...)
	if ai {
		cmd.Env = append([]string(nil), n.aiEnv...)
	}
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
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
	for suffix, data := range map[string][]byte{"stdout": out.Bytes(), "stderr": stderr.Bytes()} {
		if e := os.WriteFile(stem+"."+suffix, data, 0600); e != nil {
			n.t.Fatal(e)
		}
	}
	b12JSON(n.t, stem+".request.json", map[string]any{"argv": cmd.Args, "directory": cmd.Dir, "environment": cmd.Env, "exit": code, "error": fmt.Sprint(err), "stdout_sha256": b12Hash(out.Bytes()), "stderr_sha256": b12Hash(stderr.Bytes())})
	if ctx.Err() != nil || code < 0 {
		n.t.Fatalf("native %s did not complete: exit=%d err=%v stderr=%s", label, code, err, stderr.Bytes())
	}
	return code, out.Bytes(), stderr.Bytes()
}

func (n *b12Native) must(label string, args ...string) []byte {
	n.t.Helper()
	code, out, stderr := n.run(label, false, nil, args...)
	if code != 0 {
		n.t.Fatalf("native %s exit=%d stderr=%s", label, code, stderr)
	}
	return out
}

func (n *b12Native) sql(label, query string) []map[string]any {
	n.t.Helper()
	var rows []map[string]any
	if err := b12Decode(n.must(label, "--readonly", "--json", "sql", query), &rows); err != nil {
		n.t.Fatalf("native sql %s: %v", label, err)
	}
	if rows == nil {
		rows = []map[string]any{}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, _ := json.Marshal(rows[i])
		b, _ := json.Marshal(rows[j])
		return string(a) < string(b)
	})
	return rows
}

// image captures every native physical table, plus Dolt head and status.
func (n *b12Native) image(label string) map[string]any {
	n.t.Helper()
	tables := map[string][]map[string]any{}
	for _, row := range n.sql(label+"-tables", "SHOW TABLES") {
		for _, value := range row {
			name, ok := value.(string)
			if !ok || !n.tableName.MatchString(name) {
				n.t.Fatal("unsafe native table identity")
			}
			tables[name] = n.sql(label+"-table-"+name, "SELECT * FROM `"+name+"`")
		}
	}
	if len(tables["issues"]) == 0 {
		n.t.Fatal("native physical image has no issues")
	}
	image := map[string]any{"tables": tables, "head": n.sql(label+"-head", "SELECT DOLT_HASHOF('HEAD') AS head"), "status": n.sql(label+"-status", "SELECT * FROM dolt_status")}
	b12JSON(n.t, filepath.Join(n.root, fmt.Sprintf("image-%04d-%s.json", n.seq, label)), image)
	return image
}

// fingerprint is Dolt's own hash of the complete working database plus HEAD;
// any native write to any table changes it.
func (n *b12Native) fingerprint(label string) []map[string]any {
	n.t.Helper()
	return n.sql(label+"-fingerprint", "SELECT DOLT_HASHOF_DB() AS working, DOLT_HASHOF('HEAD') AS head")
}

func b12String(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return x.String()
	default:
		return fmt.Sprint(x)
	}
}

func (n *b12Native) issues(label string, ids []string) []NativeIssue {
	n.t.Helper()
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = "'" + id + "'"
	}
	rows := n.sql(label, "SELECT id,status,description,design,notes,acceptance_criteria,compaction_level,compacted_at,original_size FROM issues WHERE id IN ("+strings.Join(quoted, ",")+") ORDER BY id")
	out := make([]NativeIssue, 0, len(rows))
	for _, row := range rows {
		out = append(out, NativeIssue{ID: b12String(row["id"]), Status: b12String(row["status"]), Description: b12String(row["description"]), Design: b12String(row["design"]), Notes: b12String(row["notes"]), AcceptanceCriteria: b12String(row["acceptance_criteria"]), CompactionLevel: b12String(row["compaction_level"]), Compacted: row["compacted_at"] != nil, OriginalSize: b12String(row["original_size"])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) != len(ids) {
		n.t.Fatalf("native issue projection %s found %d of %v", label, len(out), ids)
	}
	return out
}

func (n *b12Native) snapshots(label string, ids []string) []NativeSnapshot {
	n.t.Helper()
	out := []NativeSnapshot{}
	for _, row := range n.sql(label, "SELECT issue_id,compaction_level,CAST(snapshot_json AS CHAR) AS snapshot_json FROM compaction_snapshots") {
		id := b12String(row["issue_id"])
		if !contains(ids, id) {
			continue
		}
		var content map[string]any
		if err := b12Decode([]byte(b12String(row["snapshot_json"])), &content); err != nil {
			n.t.Fatalf("native snapshot json: %v", err)
		}
		out = append(out, NativeSnapshot{IssueID: id, CompactionLevel: b12String(row["compaction_level"]), Content: content})
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := json.Marshal(out[i])
		b, _ := json.Marshal(out[j])
		return string(a) < string(b)
	})
	return out
}

func (n *b12Native) comments(label string) []NativeComment {
	n.t.Helper()
	out := []NativeComment{}
	for _, row := range n.sql(label, "SELECT issue_id,actor,comment FROM events WHERE event_type='commented'") {
		out = append(out, NativeComment{IssueID: b12String(row["issue_id"]), Actor: b12String(row["actor"]), Text: b12String(row["comment"])})
	}
	return out
}

func contains(values []string, v string) bool {
	for _, x := range values {
		if x == v {
			return true
		}
	}
	return false
}

// b12Volatile masks run-local fields (durations, wall-clock ages, timestamps
// and the project commit) in native JSON output.
func b12Volatile(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range x {
			switch k {
			case "elapsed_ms", "updated_at", "compacted_at", "created_at", "closed_at", "age_days", "compacted_at_commit":
				out[k] = "<volatile>"
			default:
				out[k] = b12Volatile(val)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = b12Volatile(x[i])
		}
		return out
	default:
		return v
	}
}

// vector runs one public native command and records its complete effect on
// the named records, snapshots, audit comments, stub and full image.
func (n *b12Native) vector(name string, ai bool, ids []string, args ...string) NativeVector {
	n.t.Helper()
	before := n.issues(name+"-before", ids)
	beforeImage := n.fingerprint(name + "-before")
	beforeComments := n.comments(name + "-comments-before")
	if leftover, _ := n.stub.take(); len(leftover) != 0 {
		n.t.Fatalf("stub requests leaked from a previous vector: %+v", leftover)
	}
	code, out, stderr := n.run(name, ai, nil, args...)
	requests, prompts := n.stub.take()
	for i, prompt := range prompts {
		if err := os.WriteFile(filepath.Join(n.root, fmt.Sprintf("%s-stub-prompt-%d.txt", name, i)), []byte(prompt), 0600); err != nil {
			n.t.Fatal(err)
		}
	}
	after := n.issues(name+"-after", ids)
	afterImage := n.fingerprint(name + "-after")
	afterComments := n.comments(name + "-comments-after")
	added := []NativeComment{}
	remaining := append([]NativeComment(nil), beforeComments...)
	for _, c := range afterComments {
		found := -1
		for i, r := range remaining {
			if r == c {
				found = i
				break
			}
		}
		if found >= 0 {
			remaining = append(remaining[:found], remaining[found+1:]...)
			continue
		}
		added = append(added, c)
	}
	if len(remaining) != 0 {
		n.t.Fatalf("native %s removed audit comments: %+v", name, remaining)
	}
	sort.Slice(added, func(i, j int) bool {
		a, _ := json.Marshal(added[i])
		b, _ := json.Marshal(added[j])
		return string(a) < string(b)
	})
	var stdout any
	if trimmed := bytes.TrimSpace(out); len(trimmed) != 0 {
		if err := b12Decode(trimmed, &stdout); err != nil {
			stdout = string(trimmed)
		} else {
			stdout = b12Volatile(stdout)
		}
	}
	shown := make([]string, len(args))
	for i, a := range args {
		shown[i] = a
		for label, path := range n.summaryFiles {
			if a == path {
				shown[i] = "<summary:" + label + ">"
			}
		}
	}
	if requests == nil {
		requests = []StubRequest{}
	}
	v := NativeVector{Name: name, Args: shown, AIEnvironment: ai, Exit: code, Stdout: stdout, Stderr: strings.TrimSpace(string(stderr)), Before: before, After: after, SnapshotsAfter: n.snapshots(name+"-snapshots", ids), CommentsAdded: added, StubRequests: requests, FullImageChanged: !reflect.DeepEqual(beforeImage, afterImage)}
	b12JSON(n.t, filepath.Join(n.root, fmt.Sprintf("vector-%04d-%s.json", n.seq, name)), v)
	n.vectors = append(n.vectors, v)
	if attempts := n.sink.seen(); len(attempts) != 0 {
		n.t.Fatalf("native %s made a non-loopback HTTP(S) attempt: %v", name, attempts)
	}
	return v
}

func (n *b12Native) summary(label, text string) string {
	n.t.Helper()
	path := filepath.Join(n.dir, "..", "summary-"+label+".txt")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		n.t.Fatal(err)
	}
	n.summaryFiles[label] = path
	return path
}

func TestB12NativeCompactionVectors(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute retained outputs required")
	}
	committedRaw, err := os.ReadFile(b12VectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	var committed []NativeVector
	if err := b12Decode(committedRaw, &committed); err != nil {
		t.Fatal(err)
	}
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	builder := publicnative.SourceBuilder{T: t, Root: root, SourceDirectory: filepath.Join(checkout, "spec/s1-b09-native/testdata"), Target: b12NativeTarget}
	candidate := strings.TrimSpace(string(builder.Command(checkout, "b12-candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	b12JSON(t, filepath.Join(root, "b12-request-binding.json"), map[string]any{"candidate": candidate, "target": os.Getenv("TEST_TARGET"), "run": os.Getenv("GITHUB_RUN_ID"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "native_source": pinnedSource, "committed_vectors_sha256": b12Hash(committedRaw), "ai_provider": "native AI provider stubbed via public ai.base_url; summary content not under test; Beads source unmodified"})
	binary := builder.Build(pinnedSource)
	binarySHA, _, err := hashFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("B12_NATIVE_BINARY sha256=%s source=%s", binarySHA, pinnedSource.Commit)

	dir := filepath.Join(root, "b12-native")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	byName := map[string]NativeVector{}
	for _, v := range committed {
		byName[v.Name] = v
	}
	vectors := recordB12Native(t, binary, dir, pinnedSource.Commit, func(t *testing.T, got NativeVector) {
		want, ok := byName[got.Name]
		if !ok || !reflect.DeepEqual(got, want) {
			gotRaw, _ := json.MarshalIndent(got, "", "  ")
			t.Fatalf("native vector %s differs from committed %s:\n%s", got.Name, b12VectorsPath, gotRaw)
		}
	})
	b12JSON(t, filepath.Join(root, "b12-native-vectors.actual.json"), vectors)
	if len(committed) != len(vectors) {
		t.Fatalf("committed vectors=%d recorded=%d", len(committed), len(vectors))
	}

	// Root ruling 2026-11-23: rerun the same commands once with the switch
	// trial's pinned binary and record whether every vector is identical.
	t.Run("trial-binary-9370e9a9", func(t *testing.T) {
		trialSHA, _, err := hashFile(b12TrialBinary)
		if err != nil || trialSHA != b12TrialBinarySHA {
			t.Fatalf("trial binary identity: sha=%s err=%v", trialSHA, err)
		}
		trialDir := filepath.Join(root, "b12-native-trial-binary")
		if err := os.Mkdir(trialDir, 0700); err != nil {
			t.Fatal(err)
		}
		trial := recordB12Native(t, b12TrialBinary, trialDir, "", func(*testing.T, NativeVector) {})
		differing := []string{}
		for i, v := range trial {
			if i >= len(vectors) || !reflect.DeepEqual(v, vectors[i]) {
				differing = append(differing, v.Name)
			}
		}
		identical := len(trial) == len(vectors) && len(differing) == 0
		b12JSON(t, filepath.Join(root, "b12-trial-binary-comparison.json"), map[string]any{"trial_binary": b12TrialBinary, "trial_sha256": trialSHA, "source_build_sha256": binarySHA, "source_commit": pinnedSource.Commit, "vectors": len(trial), "identical": identical, "differing": differing})
		t.Logf("B12_TRIAL_BINARY sha256=%s identical=%t differing=%v", trialSHA, identical, differing)
	})
}

// recordB12Native runs every native vector in a fresh workspace, owned Dolt
// server, loopback AI stub and loopback proxy sink, and returns the vectors.
func recordB12Native(t *testing.T, binary, dir, wantCommit string, check func(*testing.T, NativeVector)) []NativeVector {
	t.Helper()
	workspace := t.TempDir()
	home, project, tmp := filepath.Join(workspace, "home"), filepath.Join(workspace, "project"), filepath.Join(workspace, "tmp")
	for _, p := range []string{home, project, tmp} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	server := publicnative.StartDolt(t, dir)
	stub, sink := startStub(t), startSink(t)
	base := []string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin", "HOME=" + home, "USER=b12-public", "TZ=UTC", "LC_ALL=C", "NO_COLOR=1", "CI=true", "DO_NOT_TRACK=1", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B12 Public", "GIT_AUTHOR_EMAIL=b12@example.invalid", "GIT_COMMITTER_NAME=B12 Public", "GIT_COMMITTER_EMAIL=b12@example.invalid", "TMPDIR=" + tmp, "BEADS_ACTOR=b12-public-recorder", "BEADS_NO_DAEMON=1"}
	proxy := "http://" + sink.addr
	plain := append(server.ChildEnv(base), "HTTP_PROXY="+proxy, "HTTPS_PROXY="+proxy, "http_proxy="+proxy, "https_proxy="+proxy, "BD_NON_INTERACTIVE=1")
	// ai.base_url via its documented BD_AI_BASE_URL binding (config.go:900).
	ai := append(append([]string(nil), plain...), "ANTHROPIC_API_KEY="+b12DummyKey, "BD_AI_BASE_URL=http://"+stub.addr)
	n := &b12Native{t: t, root: dir, dir: project, binary: binary, plainEnv: plain, aiEnv: ai, stub: stub, sink: sink, summaryFiles: map[string]string{}, tableName: regexp.MustCompile(`^[A-Za-z0-9_]+$`)}
	server.RegisterBeadsCleanup(t, binary, project, plain)
	var version struct{ Commit, Build string }
	if err := b12Decode(n.must("version", "version", "--json"), &version); err != nil || wantCommit != "" && version.Commit != wantCommit {
		t.Fatalf("actual binary/source version mismatch: %+v %v", version, err)
	}
	if out, err := exec.Command("/usr/bin/git", "-C", project, "-c", "core.hooksPath=/dev/null", "init", "--quiet", "--template=").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v %s", err, out)
	}
	n.must("init", "init", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", "b12_native", "--prefix", "b12", "--skip-hooks", "--skip-agents", "--non-interactive", "--quiet")
	server.RequireDatabaseDir(t, "b12_native")

	recent := b12Fixture("b12-recent", "Recently closed public record")
	recentClosed := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	recent["created_at"], recent["updated_at"], recent["closed_at"] = recentClosed, recentClosed, recentClosed
	open := b12Fixture("b12-open", "Open public record")
	open["status"] = "open"
	delete(open, "closed_at")
	fixtures := []map[string]any{
		b12Fixture("b12-apply", "Supplied summary public record"),
		b12Fixture("b12-auto", "Auto summary public record"),
		b12Fixture("b12-autolong", "Auto "+b12LongMarker+" public record"),
		b12Fixture("b12-batch", "Batch public record"),
		b12Fixture("b12-dry", "Dry run public record"),
		open, recent,
		b12Fixture("b12-tier2", "Tier two ordering public record"),
	}
	var jsonl bytes.Buffer
	for _, f := range fixtures {
		raw, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		jsonl.Write(append(raw, '\n'))
	}
	input := filepath.Join(workspace, "b12-fixture.jsonl")
	if err := os.WriteFile(input, jsonl.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b12-fixture.jsonl"), jsonl.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	n.must("import-fixture", "import", input, "--json")
	n.image("after-import")

	short := n.summary("tier-one", "Tier one supplied summary.")
	long := n.summary("not-smaller", strings.Repeat("Supplied summary that is not smaller than the original. ", 20))
	tierTwo := n.summary("tier-two", "T2 summary.")
	compact := func(extra ...string) []string { return append([]string{"--json", "admin", "compact"}, extra...) }
	cases := []struct {
		name string
		ai   bool
		ids  []string
		args []string
	}{
		{"apply-open-refused", false, []string{"b12-open"}, compact("--apply", "--id", "b12-open", "--summary", short)},
		{"apply-recent-refused", false, []string{"b12-recent"}, compact("--apply", "--id", "b12-recent", "--summary", short)},
		{"apply-summary-not-smaller-refused", false, []string{"b12-apply"}, compact("--apply", "--id", "b12-apply", "--summary", long)},
		{"apply-tier-one", false, []string{"b12-apply"}, compact("--apply", "--id", "b12-apply", "--summary", short)},
		{"apply-tier-one-repeat-refused", false, []string{"b12-apply"}, compact("--apply", "--id", "b12-apply", "--summary", short)},
		{"restore-after-apply-no-snapshot", false, []string{"b12-apply"}, []string{"--json", "restore", "b12-apply", "--apply"}},
		// The pinned public CLI refuses every --tier 2 request (not implemented).
		{"apply-tier-two-level-zero-refused", false, []string{"b12-tier2"}, compact("--apply", "--tier", "2", "--id", "b12-tier2", "--summary", tierTwo)},
		{"apply-tier-two-level-one-refused", false, []string{"b12-apply"}, compact("--apply", "--tier", "2", "--id", "b12-apply", "--summary", tierTwo)},
		{"apply-dry-run", false, []string{"b12-dry"}, compact("--apply", "--dry-run", "--id", "b12-dry", "--summary", short)},
		{"auto-missing-key-refused", false, []string{"b12-auto"}, compact("--auto", "--id", "b12-auto")},
		{"auto-dry-run", true, []string{"b12-auto"}, compact("--auto", "--dry-run", "--id", "b12-auto")},
		{"auto-tier-one", true, []string{"b12-auto"}, compact("--auto", "--id", "b12-auto")},
		{"auto-summary-not-smaller-refused", true, []string{"b12-autolong"}, compact("--auto", "--id", "b12-autolong")},
		{"restore-preview", false, []string{"b12-auto"}, []string{"--json", "restore", "b12-auto"}},
		{"restore-apply-snapshot", false, []string{"b12-auto"}, []string{"--json", "restore", "b12-auto", "--apply"}},
		{"auto-batch-mixed", true, []string{"b12-auto", "b12-autolong", "b12-batch", "b12-dry", "b12-tier2"}, compact("--auto", "--all")},
	}
	defer func() { n.t = t; n.image("final") }()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n.t = t
			got := n.vector(c.name, c.ai, c.ids, c.args...)
			for _, request := range got.StubRequests {
				if request.Method != http.MethodPost || !request.DummyKey || len(request.Roles) != 1 || request.Roles[0] != "user" || request.Model == "" {
					t.Fatalf("native AI request shape differs from the real client: %+v", request)
				}
			}
			check(t, got)
		})
	}
	n.t = t
	if len(n.vectors) != len(cases) {
		t.Fatalf("recorded %d of %d native vectors", len(n.vectors), len(cases))
	}
	if attempts := sink.seen(); len(attempts) != 0 {
		t.Fatalf("non-loopback HTTP(S) attempts observed: %v", attempts)
	}
	t.Logf("B12_NATIVE_NO_OUTBOUND binary=%s proxy_sink=%s attempts=0", binary, sink.addr)
	return n.vectors
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	size, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), size, err
}

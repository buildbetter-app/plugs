package s1b21b24

// Native acceptance is a separate manual workload. The original PR299 recorder
// and its managed-proxied profile remain historical source and are not inputs.
import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/publicnative"
)

var nativeRecordingID = flag.String("native_recording", "", "one fresh accepted418 recording: recording1 or recording2")

// The two HTTP callers may finish in either order; complete bytes are retained
// under predetermined distinct names, independent of the winner.
type nativeHTTPReceipt struct {
	Label, Method, URL, StartedUTC, EndedUTC string
	Request, Response                        json.RawMessage
	Status                                   int
	TransportError                           string
}
type nativeLockedBuffer struct {
	sync.Mutex
	data bytes.Buffer
}

func (b *nativeLockedBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.data.Write(p)
}
func (b *nativeLockedBuffer) Bytes() []byte {
	b.Lock()
	defer b.Unlock()
	return append([]byte(nil), b.data.Bytes()...)
}

type nativeHTTPServer struct {
	t              *testing.T
	root, base     string
	cmd            *exec.Cmd
	stdout, stderr nativeLockedBuffer
	done           chan error
	stop           sync.Once
}

func nativeStartHTTP(t *testing.T, root, binary, dir string, env []string) *nativeHTTPServer {
	t.Helper()
	s := &nativeHTTPServer{t: t, root: root, done: make(chan error, 1)}
	s.cmd = exec.Command(binary, "serve", "--addr", "127.0.0.1:0")
	s.cmd.Dir, s.cmd.Env = dir, env // The actual ChildEnv result, unchanged.
	pipe, err := s.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	s.cmd.Stderr = &s.stderr
	started := time.Now().UTC()
	if err = s.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	nativeWriteJSON(t, filepath.Join(root, "http-start.json"), map[string]any{"argv": s.cmd.Args, "cwd": dir, "environment": env, "pid": s.cmd.Process.Pid, "started_utc": started})
	first := make(chan string, 1)
	go func() {
		scan := bufio.NewScanner(io.TeeReader(pipe, &s.stdout))
		if scan.Scan() {
			first <- scan.Text()
		}
		for scan.Scan() {
		}
		close(first)
	}()
	go func() { s.done <- s.cmd.Wait() }()
	t.Cleanup(func() { s.Stop() })
	select {
	case line := <-first:
		address, ok := strings.CutPrefix(line, "bd serve: listening on http://")
		if !ok {
			t.Fatalf("native serve did not announce listener: %q", line)
		}
		host, port, e := net.SplitHostPort(strings.TrimSpace(address))
		n, _ := strconv.Atoi(port)
		if e != nil || host != "127.0.0.1" || n < 1 || n > 65535 {
			t.Fatalf("native serve invalid bound address: %q", address)
		}
		s.base = "http://" + strings.TrimSpace(address)
	case <-time.After(90 * time.Second):
		t.Fatal("native HTTP startup timed out")
	}
	nativeWriteJSON(t, filepath.Join(root, "http-ready.json"), map[string]any{"url": s.base, "pid": s.cmd.Process.Pid, "utc": time.Now().UTC()})
	return s
}
func (s *nativeHTTPServer) Stop() {
	s.stop.Do(func() {
		err := s.cmd.Process.Signal(syscall.SIGTERM)
		var wait error
		select {
		case wait = <-s.done:
		case <-time.After(15 * time.Second):
			_ = s.cmd.Process.Kill()
			wait = <-s.done
		}
		nativeWriteRecordingBytes(s.t, filepath.Join(s.root, "http-stdout.txt"), s.stdout.Bytes())
		nativeWriteRecordingBytes(s.t, filepath.Join(s.root, "http-stderr.txt"), s.stderr.Bytes())
		nativeWriteJSON(s.t, filepath.Join(s.root, "http-stop.json"), map[string]any{"pid": s.cmd.Process.Pid, "signal_error": fmt.Sprint(err), "wait_error": fmt.Sprint(wait), "exited": s.cmd.ProcessState != nil && s.cmd.ProcessState.Exited(), "utc": time.Now().UTC()})
		if err != nil || wait != nil {
			s.t.Errorf("native serve shutdown: %v / %v", err, wait)
		}
	})
}
func (s *nativeHTTPServer) request(label, method, path string, body any) nativeHTTPReceipt {
	data, err := json.Marshal(body)
	if err != nil {
		s.t.Fatal(err)
	}
	receipt := nativeHTTPReceipt{Label: label, Method: method, URL: s.base + path, StartedUTC: time.Now().UTC().Format(time.RFC3339Nano), Request: data}
	req, err := http.NewRequest(method, receipt.URL, bytes.NewReader(data))
	if err != nil {
		s.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Minute}).Do(req)
	if err != nil {
		receipt.TransportError = err.Error()
	} else {
		receipt.Status = response.StatusCode
		receipt.Response, err = io.ReadAll(io.LimitReader(response.Body, 32<<20))
		closeErr := response.Body.Close()
		if err != nil || closeErr != nil {
			receipt.TransportError = fmt.Sprintf("read=%v close=%v", err, closeErr)
		}
	}
	receipt.EndedUTC = time.Now().UTC().Format(time.RFC3339Nano)
	nativeWriteJSON(s.t, filepath.Join(s.root, "http-"+label+".json"), receipt)
	return receipt
}
func nativeRequireHTTP(t *testing.T, r nativeHTTPReceipt, status int, code string) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(r.Response, &body); err != nil {
		t.Fatalf("native HTTP %s invalid response: %s / %v", r.Label, r.Response, err)
	}
	if r.TransportError != "" || r.Status != status || code != "" && body["code"] != code {
		t.Fatalf("native HTTP %s: status=%d want=%d code=%v want=%s error=%s body=%s", r.Label, r.Status, status, body["code"], code, r.TransportError, r.Response)
	}
	return body
}
func nativeWriteJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	nativeWriteRecordingBytes(t, path, append(b, '\n'))
}
func nativeRunfile(t *testing.T, name string) []byte {
	t.Helper()
	for _, p := range []string{filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "spec/s1-b21-b24/native_testdata", name), filepath.Join("native_testdata", name)} {
		if b, e := os.ReadFile(p); e == nil {
			return b
		}
	}
	t.Fatalf("native runfile unavailable: %s", name)
	return nil
}
func nativeFlatEnvelope(raw []byte) []byte {
	return append(append([]byte(`{"schema_version":1,"data":`), bytes.TrimSpace(raw)...), '}')
}
func nativeItems(body map[string]any) []any { v, _ := body["items"].([]any); return v }
func nativeRequireGuard(t *testing.T, body map[string]any, index int, kind, id, member, expected string) {
	t.Helper()
	if body["param"] != fmt.Sprintf("items[%d].%s.%s", index, kind, member) || body["item_index"] != float64(index) || body["item_kind"] != kind || body["item_issue_id"] != id || body[member] != expected {
		t.Fatalf("native typed guard response: %v", body)
	}
	// The public refusal deliberately does not export observed row state.
	for _, field := range []string{"actual_version", "actual_status", "actual_assignee", "current_version", "current_revision", "current_status", "current_assignee", "issue_id"} {
		if _, present := body[field]; present {
			t.Fatalf("native refusal unexpectedly exports %s: %v", field, body)
		}
	}
}
func nativeID(s string) string          { return project + "-" + s }
func nativeRef(s string) map[string]any { return map[string]any{"id": nativeID(s)} }
func nativeCreate(s, title string) any {
	return map[string]any{"kind": "create", "create": map[string]any{"id": nativeID(s), "title": title, "issue_type": "task"}}
}
func nativeUpdate(ref any, patch map[string]any) any {
	return map[string]any{"kind": "update", "update": map[string]any{"target": ref, "patch": patch}}
}
func nativeBatch(items ...any) map[string]any {
	return map[string]any{"actor": "s1-native-recorder", "items": items}
}

// These are the actual lifecycle endpoints, rather than two batch updates.
// Match the canonical native widths: sixteen closes, then four updates and
// four closes, all released against one previously observed opaque token.
func nativeRecordLifecycleRaces(t *testing.T, root string, server *nativeHTTPServer,
	apply func(*testing.T, string, ...any) map[string]any,
	sql func(*testing.T, string, string) []map[string]any,
	census func(*testing.T, string) map[string]any) {
	t.Helper()
	apply(t, "b22-race-seed", nativeCreate("racecc", "native close race"), nativeCreate("raceuc", "native mixed race"), nativeCreate("plugracecc", "imported close race"), nativeCreate("plugraceuc", "imported mixed race"))
	for _, tc := range []struct {
		name, target string
		racers       int
		mixed        bool
	}{{"close-close", "racecc", 16, false}, {"update-close", "raceuc", 8, true}} {
		if !t.Run("B22-native-lifecycle-"+tc.name, func(t *testing.T) {
			prefix := "b22-" + tc.name
			id := nativeID(tc.target)
			rows := sql(t, prefix+"-seed-row", "SELECT * FROM issues WHERE id='"+id+"'")
			if len(rows) != 1 || rows[0]["status"] != "open" {
				t.Fatal("native lifecycle race requires one genuinely open seed")
			}
			seed := rows[0]
			token := fmt.Sprint(seed["row_lock"])
			if _, err := strconv.ParseInt(token, 10, 64); err != nil {
				t.Fatal("native opaque token is not its exact decimal string", err)
			}
			before := census(t, prefix+"-race-initial")
			methods, paths, kinds := make([]string, tc.racers), make([]string, tc.racers), make([]string, tc.racers)
			bodies := make([]map[string]any, tc.racers)
			for i := range bodies {
				methods[i], paths[i], kinds[i] = "POST", "/v0/beads/issues/"+id+":close", "close"
				bodies[i] = map[string]any{"actor": "s1-native-recorder", "expected_version": token, "reason": fmt.Sprintf("%s-close-%d", prefix, i)}
				if tc.mixed && i%2 == 0 {
					methods[i], paths[i], kinds[i] = "PATCH", "/v0/beads/issues/"+id, "update"
					bodies[i] = map[string]any{"actor": "s1-native-recorder", "expected_version": token, "patch": map[string]any{"description": fmt.Sprintf("%s-update-%d", prefix, i)}}
				}
			}
			responses := make([]nativeHTTPReceipt, tc.racers)
			start := make(chan struct{})
			var ready, done sync.WaitGroup
			ready.Add(tc.racers)
			done.Add(tc.racers)
			for i := range responses {
				go func(i int) {
					defer done.Done()
					ready.Done()
					<-start
					responses[i] = server.request(fmt.Sprintf("%s-racer-%02d", prefix, i), methods[i], paths[i], bodies[i])
				}(i)
			}
			ready.Wait()
			close(start)
			done.Wait()
			winner := -1
			var winningBody map[string]any
			for i, response := range responses {
				if response.Status == 200 {
					if winner != -1 {
						t.Fatal("native lifecycle CAS admitted multiple winners")
					}
					winner, winningBody = i, nativeRequireHTTP(t, response, 200, "")
					if kinds[i] == "close" && winningBody["already_closed"] != false || kinds[i] == "update" && winningBody["changed"] != true {
						t.Fatal("native race winner must change the row, never an idempotent close")
					}
				} else {
					nativeRequireLifecycleVersionRefusal(t, response, token)
				}
			}
			if winner < 0 {
				t.Fatal("native lifecycle CAS admitted no winner")
			}
			landed := census(t, prefix+"-race-landed")
			rows = sql(t, prefix+"-winner-row", "SELECT * FROM issues WHERE id='"+id+"'")
			if len(rows) != 1 {
				t.Fatal("native lifecycle race lost its target")
			}
			row := rows[0]
			winningToken, ok := winningBody["revision"].(string)
			if !ok || winningToken == token || winningToken != fmt.Sprint(row["row_lock"]) {
				t.Fatal("native response revision must be the actual SQL row_lock decimal string")
			}
			if kinds[winner] == "close" {
				if row["status"] != "closed" || row["close_reason"] != bodies[winner]["reason"] || !nativeEqualJSON(row["description"], seed["description"]) {
					t.Fatal("native close winner mixed a losing field/reason")
				}
			} else if row["status"] != seed["status"] || !nativeEqualJSON(row["close_reason"], seed["close_reason"]) || row["description"] != fmt.Sprintf("%s-update-%d", prefix, winner) {
				t.Fatal("native update winner mixed a losing close/field")
			}
			nativeRequireLifecycleDelta(t, before, landed, id, kinds[winner], fmt.Sprint(bodies[winner]["reason"]))
			// Every losing verb is retried with the same old token, under a full
			// physical census. In close/close this is also stale-before-noop.
			for i := range responses {
				if i == winner {
					continue
				}
				label := fmt.Sprintf("%s-stale-%02d", prefix, i)
				state := census(t, label+"-before")
				nativeRequireLifecycleVersionRefusal(t, server.request(label, methods[i], paths[i], bodies[i]), token)
				if !reflect.DeepEqual(state, census(t, label+"-after")) {
					t.Fatal("native stale lifecycle refusal changed a physical/history plane")
				}
			}
			nativeWriteJSON(t, filepath.Join(root, prefix+"-race.json"), map[string]any{"racers": tc.racers, "kinds": kinds, "expected_version": token, "winner": winner, "winning_revision": winningToken, "seed": seed, "final": row, "responses": responses, "history_delta": 1, "version_delta": 1, "native_stored_idempotency_replay": "not a native lifecycle capability; stale guard replays refuse conservatively"})
		}) {
			return
		}
	}
}

func nativeRequireLifecycleVersionRefusal(t *testing.T, response nativeHTTPReceipt, token string) {
	t.Helper()
	body := nativeRequireHTTP(t, response, 409, "precondition_failed")
	if body["param"] != "expected_version" || body["expected_version"] != token {
		t.Fatal("native lifecycle refusal lost its actual expected token", body)
	}
	for _, field := range []string{"actual_version", "current_version", "current_revision", "currentRevision", "item_index", "item_kind", "item_issue_id"} {
		if _, ok := body[field]; ok {
			t.Fatal("native lifecycle refusal fabricated a non-exported field", field)
		}
	}
}

func nativeRequireLifecycleDelta(t *testing.T, before, after map[string]any, id, kind, reason string) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatal("native lifecycle race changed the physical table set")
	}
	mutable := map[string]bool{"issues": true, "events": true, "issue_versions": true, "bd_events_journal": true, "bd_events_seq": true, "local_metadata": true, "ready_issues": true, "@dolt_log": true}
	for table, initial := range before {
		if !mutable[table] && !reflect.DeepEqual(initial, after[table]) {
			t.Fatal("native lifecycle race changed an unrelated physical plane", table)
		}
	}
	for _, table := range []string{"events", "issue_versions", "bd_events_journal", "@dolt_log"} {
		initial, a := before[table].([]map[string]any)
		landed, b := after[table].([]map[string]any)
		if !a || !b || len(landed) != len(initial)+1 {
			t.Fatal("native lifecycle race must add exactly one event/version/journal/history row", table)
		}
		old := map[string]bool{}
		for _, row := range initial {
			old[nativeCanonical(row)] = true
		}
		added := 0
		for _, row := range landed {
			if old[nativeCanonical(row)] {
				delete(old, nativeCanonical(row))
				continue
			}
			added++
			if table != "@dolt_log" && row["issue_id"] != id {
				t.Fatal("native lifecycle race wrote another issue's history", table)
			}
			if table == "events" && kind == "close" && (row["new_value"] != reason || row["old_value"] != "" || row["comment"] != nil) {
				t.Fatal("native close event did not preserve only its winning reason")
			}
			if table == "events" && row["event_type"] != map[string]string{"close": "closed", "update": "updated"}[kind] {
				t.Fatal("native lifecycle human event does not identify its winning verb")
			}
			if table == "bd_events_journal" && row["op"] != kind {
				t.Fatal("native lifecycle journal does not identify its winning verb")
			}
		}
		if len(old) != 0 || added != 1 {
			t.Fatal("native lifecycle race rewrote existing history", table)
		}
	}
	// The clone-local journal counter shares the mutation transaction. A
	// losing allocator must not burn a sequence number.
	seqBefore, a := before["bd_events_seq"].([]map[string]any)
	seqAfter, b := after["bd_events_seq"].([]map[string]any)
	if !a || !b || len(seqBefore) != 1 || len(seqAfter) != 1 || fmt.Sprint(seqBefore[0]["id"]) != "0" || fmt.Sprint(seqAfter[0]["id"]) != "0" {
		t.Fatal("native journal must retain its single physical counter")
	}
	oldSeq, err := strconv.ParseInt(fmt.Sprint(seqBefore[0]["next_seq"]), 10, 64)
	if err != nil || fmt.Sprint(seqAfter[0]["next_seq"]) != strconv.FormatInt(oldSeq+1, 10) {
		t.Fatal("native lifecycle race burned or duplicated a journal sequence")
	}
	oldJournal := map[string]bool{}
	for _, row := range before["bd_events_journal"].([]map[string]any) {
		oldJournal[nativeCanonical(row)] = true
	}
	newJournal := []map[string]any{}
	for _, row := range after["bd_events_journal"].([]map[string]any) {
		if !oldJournal[nativeCanonical(row)] {
			newJournal = append(newJournal, row)
		}
	}
	if len(newJournal) != 1 || fmt.Sprint(newJournal[0]["seq"]) != strconv.FormatInt(oldSeq+1, 10) || newJournal[0]["issue_id"] != id || newJournal[0]["op"] != kind {
		t.Fatal("the newly added native journal row must be exactly the winning counter allocation")
	}
	// Close checks touch exactly two documented dependency-coordination
	// cells, even when the target has no edges. Preserve all other metadata.
	metadata := func(rows any) map[string]any {
		out := map[string]any{}
		for _, row := range rows.([]map[string]any) {
			key, ok := row["key"].(string)
			if !ok || out[key] != nil {
				t.Fatal("native local metadata key is invalid or duplicated")
			}
			out[key] = row["value"]
		}
		return out
	}
	oldMetadata, newMetadata := metadata(before["local_metadata"]), metadata(after["local_metadata"])
	if kind == "close" {
		shard := nativeHashBytes([]byte(id))[:3]
		for _, tier := range []string{"dependencies", "wisp_dependencies"} {
			key := "dependency-coordination/v1/" + tier + "/" + shard
			value, exists := newMetadata[key]
			if !exists || reflect.DeepEqual(value, oldMetadata[key]) {
				t.Fatal("native close did not rewrite its exact coordination cell", key)
			}
			if _, err := strconv.ParseInt(fmt.Sprint(value), 10, 64); err != nil {
				t.Fatal("native coordination token is not its decimal string", err)
			}
			delete(oldMetadata, key)
			delete(newMetadata, key)
		}
	}
	if !reflect.DeepEqual(oldMetadata, newMetadata) {
		t.Fatal("native lifecycle race changed unrelated local metadata")
	}
	initial, landed := before["issues"].([]map[string]any), after["issues"].([]map[string]any)
	// The pinned ready_issues relation is a SQL view of i.*, not an
	// independent stored table. This fresh unlinked open target starts in
	// it; only its winning close may remove it, and a winning update must
	// expose the exact final issue image. No unrelated ready row may change.
	readyBefore := before["ready_issues"].([]map[string]any)
	readyAfter := after["ready_issues"].([]map[string]any)
	withoutTarget := func(rows []map[string]any) ([]map[string]any, []map[string]any) {
		other, target := []map[string]any{}, []map[string]any{}
		for _, row := range rows {
			if row["id"] == id {
				target = append(target, row)
			} else {
				other = append(other, row)
			}
		}
		return other, target
	}
	oldOthers, oldReady := withoutTarget(readyBefore)
	newOthers, newReady := withoutTarget(readyAfter)
	_, oldIssue := withoutTarget(initial)
	_, newIssue := withoutTarget(landed)
	if len(oldReady) != 1 || !reflect.DeepEqual(oldReady, oldIssue) || !reflect.DeepEqual(oldOthers, newOthers) {
		t.Fatal("native lifecycle race changed an unrelated ready projection")
	}
	if kind == "close" && len(newReady) != 0 || kind == "update" && !reflect.DeepEqual(newReady, newIssue) {
		t.Fatal("native ready projection does not equal only the winning lifecycle delta")
	}
	if len(initial) != len(landed) {
		t.Fatal("native lifecycle race changed issue cardinality")
	}
	rows := map[string]string{}
	for _, row := range initial {
		if row["id"] != id {
			rows[fmt.Sprint(row["id"])] = nativeCanonical(row)
		}
	}
	for _, row := range landed {
		if row["id"] != id && rows[fmt.Sprint(row["id"])] != nativeCanonical(row) {
			t.Fatal("native lifecycle race changed an unrelated issue")
		}
		if row["id"] != id {
			continue
		}
		var seed map[string]any
		for _, candidate := range initial {
			if candidate["id"] == id {
				seed = candidate
			}
		}
		if seed == nil || len(seed) != len(row) {
			t.Fatal("native lifecycle winner changed its column set")
		}
		changed := map[string]bool{"updated_at": true, "row_lock": true, "current_revision": true}
		if kind == "close" {
			for _, field := range []string{"status", "close_reason", "closed_at", "closed_by_session"} {
				changed[field] = true
			}
		} else {
			changed["description"] = true
		}
		for field, old := range seed {
			if !changed[field] && !reflect.DeepEqual(old, row[field]) {
				t.Fatal("native lifecycle winner changed an unrelated field", field)
			}
		}
		oldVersion, err := strconv.ParseInt(fmt.Sprint(seed["current_revision"]), 10, 64)
		if err != nil || fmt.Sprint(row["current_revision"]) != strconv.FormatInt(oldVersion+1, 10) {
			t.Fatal("native lifecycle winner must mint exactly one local version")
		}
	}
}

func TestNativeB21B24Acceptance(t *testing.T) {
	if *nativeRecordingID != "recording1" && *nativeRecordingID != "recording2" {
		t.Fatal("explicit independent native_recording is required")
	}
	root := nativeMakeRecordingRoot(t)
	t.Logf("NATIVE_B21_B24_OUTPUT %s", root)
	var profile nativeRecordingProfile
	if err := json.Unmarshal(nativeRunfile(t, "profile.json"), &profile); err != nil || profile.Schema != "plug-public-native/1" {
		t.Fatal("invalid native profile", err)
	}
	var protocol nativeRecordingFixture
	if err := json.Unmarshal(nativeRunfile(t, "protocol.json"), &protocol); err != nil || protocol.SourceCommit != nativeBeadsCommit || protocol.SourceTree != nativeBeadsTree || len(protocol.Commands) == 0 {
		t.Fatal("invalid native protocol", err)
	}
	nativeWriteRecordingBytes(t, filepath.Join(root, "profile.json"), nativeRunfile(t, "profile.json"))
	nativeWriteRecordingBytes(t, filepath.Join(root, "protocol.json"), nativeRunfile(t, "protocol.json"))
	artifact := nativeVerifyPublicArtifact(t, profile)
	buildInfo := nativeRecordBuildInfo(t, root, artifact, profile)
	server := publicnative.StartDolt(t, root)
	for _, corpus := range []string{"role-only", "exchange"} {
		if !t.Run(corpus, func(t *testing.T) {
			corpusRoot := filepath.Join(root, corpus)
			if err := os.Mkdir(corpusRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			nativeRecordCorpus(t, corpusRoot, server, artifact, protocol, buildInfo, corpus)
		}) {
			return
		}
	}
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	nativeWriteJSON(t, filepath.Join(root, "recording-scope.json"), map[string]any{
		"recording": *nativeRecordingID, "corpora": []string{"role-only", "exchange"},
		"role_only":                  "full physical B21 metadata and ephemeral issue corpus retained; native export refuses; no replay claim",
		"exchange":                   "separately initialized physical database with supported operations; exact archive replay only",
		"whole_native_corpus_replay": false,
	})
}

// No unsupported role state is deleted or filtered into an export. Each corpus
// has a fresh workspace, physical database, exact ChildEnv and HTTP lifecycle.
func nativeRecordCorpus(t *testing.T, root string, server *publicnative.Server, artifact nativePublicArtifact, protocol nativeRecordingFixture, buildInfo []byte, corpus string) {
	t.Helper()
	work := nativeMakeRecordingDirectory(t, server.Root, corpus+"-beads-work-")
	if err := os.MkdirAll(filepath.Join(work, "home", ".config"), 0700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(work, "native-pinned")
	nativeCopyPinned(t, artifact.Binary, binary, artifact.SHA256, artifact.Size)
	env := server.ChildEnv(nativeRecordingEnv(server.DataRoot)(work))
	var commands []nativeCommandReceipt
	defer func() { nativeWriteCommandReceipts(t, root, commands) }()
	run := func(t *testing.T, label string, args ...string) []byte {
		return nativeRunCommand(t, root, &commands, label, corpus, work, env, append([]string{binary}, args...), nil, 0)
	}
	nativeRunCommand(t, root, &commands, "git-init", corpus, work, env, []string{"git", "-c", "init.defaultBranch=main", "init", "--quiet", "--template="}, nil, 0)
	run(t, "version", "version", "--json")
	database := "s1_native_21_24_" + *nativeRecordingID + "_" + strings.ReplaceAll(corpus, "-", "_") + "_" + strconv.Itoa(os.Getpid())
	run(t, "init", "init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", project, "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive")
	physical := server.RequireDatabaseDir(t, database)
	nativeWriteJSON(t, filepath.Join(root, "database-binding.json"), map[string]any{"database": database, "physical_root": physical, "server_root": server.DataRoot, "port": server.Port, "pid": server.PID, "child_env": env})
	run(t, "disable-auto-export", "config", "set", "export.auto", "false")
	run(t, "enable-versions", "config", "set", "versioned-history.enabled", "true")
	run(t, "enable-journal", "config", "set", "events-journal", "true")
	run(t, "config", "config", "set", protocol.ConfigKey, "native-21-24")
	run(t, "memory", "--json", "remember", "native B21-B24 memory", "--key", protocol.MemoryKey)
	run(t, "settle-initial", "dolt", "commit")
	httpServer := nativeStartHTTP(t, root, binary, work, env)
	apply := func(t *testing.T, label string, items ...any) map[string]any {
		return nativeRequireHTTP(t, httpServer.request(label, "POST", "/v0/beads/issues:batchApply", nativeBatch(items...)), 200, "")
	}
	sql := func(t *testing.T, label, query string) []map[string]any {
		var rows []map[string]any
		raw := run(t, label, "--readonly", "sql", query, "--json")
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&rows); err != nil {
			t.Fatalf("native SQL %s: %v %s", label, err, raw)
		}
		return rows
	}
	history := func(t *testing.T, label string) int {
		rows := sql(t, label, "SELECT COUNT(*) AS n FROM dolt_log")
		n, e := strconv.Atoi(fmt.Sprint(rows[0]["n"]))
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	// Enumerate every physical user table. Refusal conservation covers source
	// state, auxiliary rows, versions, events, configuration, and the Dolt head.
	census := func(t *testing.T, label string) map[string]any {
		result := map[string]any{}
		for i, row := range sql(t, label+"-tables", "SHOW TABLES") {
			if len(row) != 1 {
				t.Fatal("ambiguous table census")
			}
			for _, raw := range row {
				table, ok := raw.(string)
				if !ok || strings.Trim(table, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != "" {
					t.Fatalf("unsafe table %v", raw)
				}
				rows := sql(t, fmt.Sprintf("%s-table-%03d", label, i), "SELECT * FROM `"+table+"`")
				sort.Slice(rows, func(i, j int) bool { return nativeCanonical(rows[i]) < nativeCanonical(rows[j]) })
				result[table] = rows
			}
		}
		result["@dolt_log"] = sql(t, label+"-log", "SELECT commit_hash, committer, email, date, message FROM dolt_log ORDER BY commit_hash")
		nativeWriteJSON(t, filepath.Join(root, label+"-census.json"), result)
		return result
	}
	refuse := func(t *testing.T, label string, items []any, status int, code string) map[string]any {
		before := census(t, label+"-before")
		body := nativeRequireHTTP(t, httpServer.request(label, "POST", "/v0/beads/issues:batchApply", nativeBatch(items...)), status, code)
		after := census(t, label+"-after")
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("native %s refusal changed physical state", label)
		}
		return body
	}

	t.Run("B21-declaration-backrefs-and-one-history", func(t *testing.T) {
		before := history(t, "batch-history-before")
		create := map[string]any{"kind": "create", "create": map[string]any{"key": "new", "id": nativeID("anchor"), "title": "first", "issue_type": "task"}}
		closed := map[string]any{"kind": "create", "create": map[string]any{"key": "closed", "id": nativeID("closednow"), "title": "closed in declaration order", "issue_type": "task"}}
		body := apply(t, "ordered", create, nativeUpdate(map[string]any{"key": "new"}, map[string]any{"title": "declared-last"}), closed, map[string]any{"kind": "close", "close": map[string]any{"target": map[string]any{"key": "closed"}, "reason": "same ordered transaction"}}, nativeCreate("guardrow", "guard target"), nativeCreate("guardbl", "guard blocker"), nativeCreate("casrow", "original CAS"), nativeCreate("alphaa", "Needle alpha"), nativeCreate("betaaa", "Needle beta"), nativeCreate("gammaa", "plain gamma"), nativeCreate("untimed", "untimed"), nativeCreate("timedaa", "timed"), nativeCreate("partiala", "partial first"), nativeCreate("partialb", "partial last"), nativeCreate("spawner", "spawner"))
		keys, _ := body["keys"].(map[string]any)
		if keys["new"] != nativeID("anchor") || keys["closed"] != nativeID("closednow") || len(nativeItems(body)) != 15 {
			t.Fatalf("native declaration/backref response: %v", body)
		}
		rows := sql(t, "ordered-readback", "SELECT title FROM issues WHERE id='"+nativeID("anchor")+"'")
		if len(rows) != 1 || rows[0]["title"] != "declared-last" || history(t, "batch-history-after") != before+1 {
			t.Fatal("native ordered batch did not commit one declared-order act")
		}
		rows = sql(t, "ordered-close-readback", "SELECT status,close_reason FROM issues WHERE id='"+nativeID("closednow")+"'")
		if len(rows) != 1 || rows[0]["status"] != "closed" || rows[0]["close_reason"] != "same ordered transaction" {
			t.Fatal("native ordered ApplyBatch close did not land")
		}
	})
	if t.Failed() {
		return
	}
	t.Run("B21-late-unknown-forward-and-guard-conservation", func(t *testing.T) {
		refuse(t, "late-refusal", []any{nativeUpdate(nativeRef("anchor"), map[string]any{"title": "must rollback"}), nativeUpdate(nativeRef("missing"), map[string]any{"title": "absent"})}, 404, "not_found")
		refuse(t, "forward-refusal", []any{nativeUpdate(map[string]any{"key": "later"}, map[string]any{"title": "absent"}), map[string]any{"kind": "create", "create": map[string]any{"key": "later", "id": nativeID("futurex"), "title": "future", "issue_type": "task"}}}, 400, "invalid_argument")
		refuse(t, "unknown-close-field", []any{nativeUpdate(nativeRef("anchor"), map[string]any{"title": "must rollback"}), map[string]any{"kind": "close", "close": map[string]any{"target": nativeRef("anchor"), "override_guard": true}}}, 400, "invalid_argument")
		apply(t, "close-policy-seed", map[string]any{"kind": "dep_add", "dep_add": map[string]any{"source": nativeRef("guardrow"), "target": nativeRef("guardbl"), "type": "blocks"}})
		policy := refuse(t, "close-policy-refusal", []any{nativeUpdate(nativeRef("anchor"), map[string]any{"title": "must rollback"}), map[string]any{"kind": "close", "close": map[string]any{"target": nativeRef("guardrow")}}}, 409, "not_closable")
		blockers, _ := policy["blockers"].([]any)
		if policy["item_index"] != float64(1) || policy["item_kind"] != "close" || policy["item_issue_id"] != nativeID("guardrow") || len(blockers) != 1 {
			t.Fatalf("native close-policy typed refusal: %v", policy)
		}
		guarded := func(member, expected string, patch map[string]any) any {
			return map[string]any{"kind": "update", "update": map[string]any{"target": nativeRef("guardrow"), member: expected, "patch": patch}}
		}
		status := refuse(t, "as-modified-status", []any{nativeUpdate(nativeRef("guardrow"), map[string]any{"status": "in_progress"}), guarded("expected_status", "open", map[string]any{"description": "must rollback"})}, 409, "precondition_failed")
		nativeRequireGuard(t, status, 1, "update", nativeID("guardrow"), "expected_status", "open")
		assignee := refuse(t, "as-modified-assignee", []any{nativeUpdate(nativeRef("guardrow"), map[string]any{"assignee": "s1-native-recorder"}), guarded("expected_assignee", "", map[string]any{"description": "must rollback"})}, 409, "precondition_failed")
		nativeRequireGuard(t, assignee, 1, "update", nativeID("guardrow"), "expected_assignee", "")
		token := fmt.Sprint(sql(t, "as-modified-version-token", "SELECT row_lock FROM issues WHERE id='"+nativeID("guardrow")+"'")[0]["row_lock"])
		refuse(t, "as-modified-version-shape", []any{nativeUpdate(nativeRef("guardrow"), map[string]any{"description": "must rollback"}), guarded("expected_version", token, map[string]any{"title": "unusable mid-request token"})}, 400, "invalid_argument")
		apply(t, "as-modified-guards-accept", nativeUpdate(nativeRef("guardrow"), map[string]any{"status": "in_progress", "assignee": "s1-native-recorder"}), map[string]any{"kind": "update", "update": map[string]any{"target": nativeRef("guardrow"), "expected_status": "in_progress", "expected_assignee": "s1-native-recorder", "patch": map[string]any{"description": "guards saw preceding mutation"}}})
	})
	if t.Failed() {
		return
	}
	t.Run("B21-boundary-1000-and-noop-history", func(t *testing.T) {
		ops := make([]any, 1000)
		for i := range ops {
			ops[i] = nativeUpdate(nativeRef("anchor"), map[string]any{"title": "declared-last"})
		}
		before := history(t, "noop-before")
		body := apply(t, "boundary1000", ops...)
		if len(nativeItems(body)) != 1000 || history(t, "noop-after") != before {
			t.Fatal("native no-op boundary/history")
		}
		for _, raw := range nativeItems(body) {
			if raw.(map[string]any)["changed"] != false {
				t.Fatal("native noop marked changed")
			}
		}
		refuse(t, "boundary1001", append(ops, ops[0]), 400, "invalid_argument")
	})
	if t.Failed() {
		return
	}
	if corpus == "role-only" {
		t.Run("B21-metadata-delta-despite-changed-false-and-wisp", func(t *testing.T) {
			dep := func(gate string) any {
				return map[string]any{"kind": "dep_add", "dep_add": map[string]any{"source": nativeRef("gammaa"), "target": nativeRef("spawner"), "type": "waits-for", "metadata": map[string]any{"gate": gate}}}
			}
			apply(t, "gate-seed", dep("all-children"))
			before := history(t, "gate-history-before")
			result := apply(t, "gate-rewrite", dep("any-children"))
			rows := sql(t, "gate-readback", "SELECT metadata FROM dependencies WHERE issue_id='"+nativeID("gammaa")+"' AND depends_on_issue_id='"+nativeID("spawner")+"'")
			if len(nativeItems(result)) != 1 || nativeItems(result)[0].(map[string]any)["changed"] != false || len(rows) != 1 || !strings.Contains(fmt.Sprint(rows[0]["metadata"]), "any-children") || history(t, "gate-history-after") != before+1 {
				t.Fatal("native durable metadata counterexample lost")
			}
			before = history(t, "ephemeral-before")
			w := map[string]any{"kind": "create", "create": map[string]any{"id": nativeID("wisprow"), "title": "wisp only", "issue_type": "task", "ephemeral": true}}
			apply(t, "ephemeral-only", w)
			if history(t, "ephemeral-after") != before {
				t.Fatal("native ephemeral issue batch minted durable history")
			}
		})
		if t.Failed() {
			return
		}
	}
	t.Run("B21-native-partial-close-is-distinct", func(t *testing.T) {
		body := nativeRequireHTTP(t, httpServer.request("partial-close", "POST", "/v0/beads/issues:batchClose", map[string]any{"actor": "s1-native-recorder", "session": "native-partial", "items": []any{map[string]any{"id": nativeID("partiala"), "reason": "first"}, map[string]any{"id": nativeID("missing")}, map[string]any{"id": nativeID("partialb"), "reason": "last"}}}), 200, "")
		outcomes, _ := body["outcomes"].([]any)
		if len(outcomes) != 3 || outcomes[1].(map[string]any)["code"] != "not_found" {
			t.Fatalf("native partial-close outcomes: %v", body)
		}
		rows := sql(t, "partial-close-readback", "SELECT id,status FROM issues WHERE id IN ('"+nativeID("partiala")+"','"+nativeID("partialb")+"') ORDER BY id")
		if len(rows) != 2 || rows[0]["status"] != "closed" || rows[1]["status"] != "closed" {
			t.Fatal("native partial survivors did not commit")
		}
		before := history(t, "partial-idempotent-before")
		nativeRequireHTTP(t, httpServer.request("partial-idempotent", "POST", "/v0/beads/issues:batchClose", map[string]any{"actor": "s1-native-recorder", "items": []any{map[string]any{"id": nativeID("partiala")}, map[string]any{"id": nativeID("partialb")}}}), 200, "")
		if history(t, "partial-idempotent-after") != before {
			t.Fatal("native all-idempotent close wrote history")
		}
	})
	if t.Failed() {
		return
	}
	t.Run("B22-real-concurrent-same-token-CAS", func(t *testing.T) {
		rows := sql(t, "cas-token", "SELECT row_lock FROM issues WHERE id='"+nativeID("casrow")+"'")
		token := fmt.Sprint(rows[0]["row_lock"])
		makeBody := func(field string) any {
			return nativeBatch(map[string]any{"kind": "update", "update": map[string]any{"target": nativeRef("casrow"), "expected_version": token, "patch": map[string]any{field: "winner-" + field}}})
		}
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		ready.Add(2)
		done.Add(2)
		responses := make([]nativeHTTPReceipt, 2)
		fields := []string{"title", "description"}
		for i, field := range fields {
			go func(i int, field string) {
				defer done.Done()
				ready.Done()
				<-start
				responses[i] = httpServer.request("cas-"+field, "POST", "/v0/beads/issues:batchApply", makeBody(field))
			}(i, field)
		}
		ready.Wait()
		close(start)
		done.Wait()
		winner, loser := -1, -1
		for i, r := range responses {
			if r.Status == 200 {
				if winner != -1 {
					t.Fatal("two native CAS winners")
				}
				winner = i
			} else {
				loser = i
				nativeRequireGuard(t, nativeRequireHTTP(t, r, 409, "precondition_failed"), 0, "update", nativeID("casrow"), "expected_version", token)
			}
		}
		if winner < 0 || loser < 0 {
			t.Fatal("native CAS did not produce one winner and loser")
		}
		rows = sql(t, "cas-readback", "SELECT title,description,row_lock FROM issues WHERE id='"+nativeID("casrow")+"'")
		if rows[0][fields[winner]] != "winner-"+fields[winner] || rows[0][fields[loser]] == "winner-"+fields[loser] || fmt.Sprint(rows[0]["row_lock"]) == token {
			t.Fatal("native CAS merged losing disjoint column")
		}
		before := census(t, "cas-stale-before")
		nativeRequireGuard(t, nativeRequireHTTP(t, httpServer.request("cas-stale", "POST", "/v0/beads/issues:batchApply", makeBody(fields[loser])), 409, "precondition_failed"), 0, "update", nativeID("casrow"), "expected_version", token)
		if !reflect.DeepEqual(before, census(t, "cas-stale-after")) {
			t.Fatal("native stale CAS changed any physical plane")
		}
		closedToken := fmt.Sprint(sql(t, "closed-token", "SELECT row_lock FROM issues WHERE id='"+nativeID("closednow")+"'")[0]["row_lock"])
		if token == closedToken {
			t.Fatal("distinct native rows unexpectedly share revision token")
		}
		refusal := refuse(t, "expected-before-idempotent-close", []any{map[string]any{"kind": "close", "close": map[string]any{"target": nativeRef("closednow"), "expected_version": token, "force": true}}}, 409, "precondition_failed")
		nativeRequireGuard(t, refusal, 0, "close", nativeID("closednow"), "expected_version", token)
		beforeHistory := history(t, "guarded-idempotent-before")
		result := apply(t, "guarded-idempotent-close", map[string]any{"kind": "close", "close": map[string]any{"target": nativeRef("closednow"), "expected_version": closedToken}})
		if len(nativeItems(result)) != 1 || nativeItems(result)[0].(map[string]any)["changed"] != false || history(t, "guarded-idempotent-after") != beforeHistory {
			t.Fatal("native matching-token idempotent close changed history")
		}
	})
	if t.Failed() {
		return
	}
	nativeRecordLifecycleRaces(t, root, httpServer, apply, sql, census)
	if t.Failed() {
		return
	}
	nativeRecordViewsAndDefer(t, root, work, binary, corpus, env, &commands, run, sql, census)
	if t.Failed() {
		return
	}
	httpServer.Stop()
	run(t, "settle-export", "dolt", "commit")
	if corpus == "role-only" {
		// This is an actual supported-profile refusal, not a filtered archive.
		// The full physical census retains both the waits-for edge and ephemeral issue.
		before := census(t, "role-export-refusal-before")
		ephemeralPresent, edgePresent := false, false
		if rows, ok := before["wisps"].([]map[string]any); ok {
			for _, row := range rows {
				ephemeralPresent = ephemeralPresent || row["id"] == nativeID("wisprow")
			}
		}
		if rows, ok := before["dependencies"].([]map[string]any); ok {
			for _, row := range rows {
				edgePresent = edgePresent || row["issue_id"] == nativeID("gammaa") && row["depends_on_issue_id"] == nativeID("spawner") && row["type"] == "waits-for" && strings.Contains(fmt.Sprint(row["metadata"]), "any-children")
			}
		}
		if !ephemeralPresent || !edgePresent {
			t.Fatal("native role export census lost the unsupported ephemeral issue or waits-for state")
		}
		inventoryPath := filepath.Join(root, "role-inventory.json")
		run(t, "role-inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventoryPath)
		inventory, err := os.ReadFile(inventoryPath)
		if err != nil {
			t.Fatal(err)
		}
		if err = nativeValidateInventory(inventory, protocol); err != nil {
			t.Fatal(err)
		}
		archivePath := filepath.Join(root, "refused-role.tar")
		nativeRunCommand(t, root, &commands, "role-export-refusal", corpus, work, env, []string{binary, "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "-o", archivePath}, nil, 1)
		last := commands[len(commands)-1]
		stderr, err := os.ReadFile(last.StderrPath)
		if err != nil || !strings.Contains(string(stderr), "unsupported nonempty family wisp") {
			t.Fatalf("native role export did not report its actual ephemeral issue family boundary: %v %s", err, stderr)
		}
		if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
			t.Fatalf("refused native role export published output: %v", err)
		}
		if !reflect.DeepEqual(before, census(t, "role-export-refusal-after")) {
			t.Fatal("native role export refusal changed physical state")
		}
		nativeWriteJSON(t, filepath.Join(root, "provenance.json"), map[string]any{
			"schema": "plug.s1-b21-b24.native-role-only.v1", "recording": *nativeRecordingID, "corpus": corpus,
			"source_commit": nativeBeadsCommit, "source_tree": nativeBeadsTree, "build_run": artifact.BuildRun,
			"binary_sha256": artifact.SHA256, "manifest_sha256": artifact.ManifestSHA, "build_info_sha256": nativeHashBytes(buildInfo),
			"inventory_sha256": nativeHashBytes(inventory), "export": "ACTUAL_SUPPORTED_PROFILE_REFUSAL",
			"unsupported_preserved": []string{"waits-for metadata edge", "active ephemeral issue and sidecars"},
			"role_corpus_replay":    false, "whole_native_corpus_replay": false,
		})
		nativeRecordHistoryOffWake(t, root, run, sql, census)
		return
	}
	historyOff := nativeRecordHistoryOffWake(t, root, run, sql, census)
	finalCensus := census(t, "exchange-final")
	if !nativeEqualJSON(historyOff.WokenRow, nativeHistoryOffSourceRow(t, finalCensus, historyOff.Target)) {
		t.Fatal("actual full history-off source row changed before canonical export")
	}
	// Independent source observations PRECEDE inventory and archive generation.
	ordinaryPath := filepath.Join(root, "source.jsonl")
	run(t, "ordinary-export", "--readonly", "export", "--include-memories", "--output", ordinaryPath)
	ordinary, err := os.ReadFile(ordinaryPath)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := nativeProjectJSONL(ordinary, protocol.ProjectionFields, protocol.StatusCategories)
	if err != nil {
		t.Fatal(err)
	}
	nativeWriteRecordingBytes(t, filepath.Join(root, "source-projection.json"), projected)
	settingsRaw := run(t, "settings-readback", "--json", "config", "list")
	settings, err := nativeReadPublicConfigSettings(settingsRaw)
	if err != nil {
		t.Fatal(err)
	}
	// ListSettings excludes kv.* by contract. Compare every remaining stored
	// row to the independently recorded full physical census. In particular,
	// an actual stored schema_version row cannot disappear as protocol control.
	cliSettings, physicalSettings := map[string]string{}, map[string]string{}
	for _, observation := range settings {
		cliSettings[observation.Key] = observation.Value.(string)
	}
	configRows, ok := finalCensus["config"].([]map[string]any)
	if !ok {
		t.Fatal("native final census has no config table")
	}
	for _, row := range configRows {
		key, keyOK := row["key"].(string)
		value, valueOK := row["value"].(string)
		if !keyOK || !valueOK || key == "" {
			t.Fatal("native physical config row is not a string key/value")
		}
		if !strings.HasPrefix(key, "kv.") {
			physicalSettings[key] = value
		}
	}
	if !reflect.DeepEqual(cliSettings, physicalSettings) {
		t.Fatalf("native stored config census differs from CLI settings: cli=%v physical=%v", cliSettings, physicalSettings)
	}
	nativeWriteJSON(t, filepath.Join(root, "stored-config-comparison.json"), map[string]any{"cli": cliSettings, "physical": physicalSettings, "transport_control": map[string]any{"schema_version": 1}, "kv_partition": "native ListSettings excludes kv.*; user memories are independently projected from ordinary JSONL"})
	metadataRaw := run(t, "metadata-readback", "--readonly", "sql", "SELECT `key`, value FROM metadata ORDER BY `key`", "--json")
	metadata, err := nativeReadPublicMetadata(nativeFlatEnvelope(metadataRaw))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := nativeExpectPublicConfig(append(settings, metadata...), projected)
	if err != nil {
		t.Fatal(err)
	}
	nativeWriteJSON(t, filepath.Join(root, "independent-config-expectation.json"), expected)
	inventoryPath := filepath.Join(root, "inventory.json")
	run(t, "inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventoryPath)
	inventory, err := os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = nativeValidateInventory(inventory, protocol); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(root, "source.tar")
	run(t, "archive", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "-o", archivePath)
	archive, err := dumpformat.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	nativeAssertHistoryOffArchive(t, archive, historyOff)
	nativeAssertPublicConfigArchive(t, archive, inventoryPath, expected)
	archiveProjection, err := nativeProjectArchive(archive, protocol.ProjectionFields)
	if err != nil {
		t.Fatal(err)
	}
	nativeWriteRecordingBytes(t, filepath.Join(root, "archive-projection.json"), archiveProjection)
	if !bytes.Equal(projected, archiveProjection) {
		t.Fatal("native ordinary JSONL/archive semantic projections differ")
	}
	reverseExpectation, err := nativeProjectPublicReverseExpectation(projected, archive.Records("bead"), protocol.ReverseArchivedFields)
	if err != nil {
		t.Fatal(err)
	}
	nativeWriteRecordingBytes(t, filepath.Join(root, "reverse-expectation.json"), reverseExpectation)
	nativeReplayAll(t, root, archivePath, reverseExpectation, expected, protocol, historyOff)
	nativeWriteJSON(t, filepath.Join(root, "provenance.json"), map[string]any{"schema": "plug.s1-b21-b24.native-exchange.v1", "recording": *nativeRecordingID, "corpus": corpus, "role_only_corpus_replay": false, "whole_native_corpus_replay": false, "source_commit": nativeBeadsCommit, "source_tree": nativeBeadsTree, "build_run": artifact.BuildRun, "binary_sha256": artifact.SHA256, "manifest_sha256": artifact.ManifestSHA, "build_info_sha256": nativeHashBytes(buildInfo), "ordinary_sha256": nativeHashBytes(ordinary), "archive_sha256": archive.SHA256(), "projection_sha256": nativeHashBytes(projected), "import_history_mode": "snapshot", "source_history_claim": false, "original_credit": "Nikhil Shinday; PR299 comparator, tests, recorder/profile/extractor preserved; e44 native helper source contributors"})
}

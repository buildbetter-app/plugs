package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	_ "modernc.org/sqlite"
)

type plugServerProcess struct {
	cmd      *exec.Cmd
	endpoint string
	stderr   bytes.Buffer
}

func startPlugServer(t *testing.T, dir string, local bool) *plugServerProcess {
	t.Helper()
	args := []string{"server"}
	if local {
		args = append(args, "--local")
	}
	cmd := exec.Command(plugBinary(t), args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PLUG_SERVER=", "PLUG_TOKEN=")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &plugServerProcess{cmd: cmd}
	cmd.Stderr = &process.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("server did not print endpoint: %v; stderr=%s", err, process.stderr.String())
	}
	fields := strings.Fields(line)
	if len(fields) != 5 || fields[0] != "plug" || fields[1] != "server" || fields[2] != "listening" || fields[3] != "on" || !strings.HasPrefix(fields[4], "http://127.0.0.1:") {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("unexpected server announcement %q", line)
	}
	process.endpoint = fields[4]
	return process
}

func (p *plugServerProcess) stop(t *testing.T) {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 0 {
				t.Fatalf("server shutdown: %v; stderr=%s", err, p.stderr.String())
			}
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
		t.Fatal("server did not shut down")
	}
	p.cmd = nil
}

func remoteCommand(t *testing.T, input string, endpoint string, args ...string) (string, string, error) {
	t.Helper()
	args = append([]string{"--server", endpoint, "--project", "acceptance", "--agent", "acceptance", "--json"}, args...)
	return runPlug(t, input, args...)
}

func TestExecutableSQLiteServerRemoteLifecycleAndRestart(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".plug"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".plug", "config.yaml"), []byte("driver: sqlite\ndsn: server.db\nlisten: 127.0.0.1:0\nproject: acceptance\nui: false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	server := startPlugServer(t, dir, false)
	t.Cleanup(func() { server.stop(t) })
	firstProcess := server.cmd
	firstPID := firstProcess.Process.Pid
	endpoint := server.endpoint
	commands := "[\"create\",\"--title\",\"first\"]\n[\"ready\"]\n[\"update\",\"@last\",\"--title\",\"updated\"]\n[\"close\",\"@last\",\"--reason\",\"done\"]\n"
	out, errOut, err := remoteCommand(t, commands, endpoint, "session")
	if err != nil {
		t.Fatalf("remote lifecycle: %v stdout=%s stderr=%s", err, out, errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 {
		t.Fatalf("lifecycle results=%d: %s", len(lines), out)
	}
	created, err := wire.DecodeJSON([]byte(lines[0]))
	if err != nil {
		t.Fatal(err)
	}
	id := created.(map[string]any)["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)["id"].(string)

	dbPath := filepath.Join(dir, "server.db")
	beforeCreate := sqliteRestartSnapshot(t, dbPath)
	params := map[string]any{
		"type": "issue", "fields": map[string]any{"title": "restart receipt"},
		"_meta": sqliteRestartMeta("e53f53fd768849b298b6d743b8b6f2112"),
	}
	first := sqliteRestartRPC(t, endpoint, "", "bead.create", params)
	originalResult := first.result(t)
	if first.Meta.Replayed {
		t.Fatal("first keyed write was marked replayed")
	}
	var receipt model.Commit
	if err := json.Unmarshal(originalResult, &receipt); err != nil || len(receipt.Results) != 1 || receipt.Results[0].Ref.ID == "" {
		t.Fatalf("keyed commit=%s err=%v", originalResult, err)
	}
	keyedID := receipt.Results[0].Ref.ID
	beforeRestart := sqliteRestartSnapshot(t, dbPath)
	for _, table := range []string{"objects", "records", "versions", "changesets", "replays"} {
		if beforeRestart.Counts[table] != beforeCreate.Counts[table]+1 {
			t.Fatalf("keyed create added %d %s rows; want one", beforeRestart.Counts[table]-beforeCreate.Counts[table], table)
		}
	}
	if beforeRestart.Head != receipt.Revision.String() || beforeRestart.Events != beforeCreate.Events+1 {
		t.Fatalf("keyed write head=%s events=%d receipt=%s", beforeRestart.Head, beforeRestart.Events, originalResult)
	}
	historyParams := map[string]any{"asOf": map[string]any{"revision": receipt.Revision, "incarnation": beforeRestart.Incarnation}, "limit": 1000}
	history := sqliteRestartRPC(t, endpoint, "", "history.query", historyParams).result(t)
	oldSession := sqliteRestartOpenSession(t, endpoint)
	sqliteRestartRPC(t, endpoint, "", "presence.set", map[string]any{"sessionId": oldSession, "focus": "bead/" + keyedID}).result(t)
	if rows := sqliteRestartPresence(t, endpoint); len(rows) != 1 || rows[0].SessionID != oldSession || rows[0].Focus != "bead/"+keyedID {
		t.Fatalf("pre-restart presence=%+v", rows)
	}
	subscription, oldCursor := sqliteRestartSubscribe(t, endpoint, oldSession, "")
	stream := sqliteRestartAttach(t, endpoint, oldSession, subscription)
	opening := stream.next(t)
	if opening.Cursor != oldCursor {
		t.Fatal("stream did not acknowledge the returned signed cursor")
	}
	stream.close()
	if after := sqliteRestartSnapshot(t, dbPath); !reflect.DeepEqual(beforeRestart, after) {
		t.Fatal("collaboration changed durable graph/history/receipts")
	}
	t.Logf("before restart: pid=%d canonical_result=%s head=%s rows=%v events=%d signed_cursor=%s", firstPID, originalResult, beforeRestart.Head, beforeRestart.Counts, beforeRestart.Events, oldCursor)
	server.stop(t)
	if firstProcess.ProcessState == nil || !firstProcess.ProcessState.Exited() || firstProcess.ProcessState.ExitCode() != 0 {
		t.Fatal("first OS process was not waited with exit 0")
	}

	server = startPlugServer(t, dir, false)
	t.Cleanup(func() { server.stop(t) })
	secondProcess := server.cmd
	secondPID := secondProcess.Process.Pid
	if secondPID <= 0 || secondPID == firstPID {
		t.Fatalf("restart did not launch a different OS process: %d -> %d", firstPID, secondPID)
	}
	t.Logf("process restart: stopped/waited pid=%d exit=0; new pid=%d; same SQLite database", firstPID, secondPID)
	endpoint = server.endpoint
	out, errOut, err = remoteCommand(t, "", endpoint, "get", id)
	if err != nil {
		t.Fatalf("restart get: %v stdout=%s stderr=%s", err, out, errOut)
	}
	value, err := wire.DecodeJSON([]byte(strings.TrimSpace(out)))
	if err != nil {
		t.Fatal(err)
	}
	bead := value.(map[string]any)["bead"].(map[string]any)
	if bead["status"] != "done" || bead["fields"].(map[string]any)["title"] != "updated" {
		t.Fatalf("restart state=%v", bead)
	}
	if after := sqliteRestartSnapshot(t, dbPath); !reflect.DeepEqual(beforeRestart, after) {
		t.Fatal("restart changed durable graph/history/receipts")
	}
	replay := sqliteRestartRPC(t, endpoint, "", "bead.create", params)
	if replayedResult := replay.result(t); !replay.Meta.Replayed || !bytes.Equal(originalResult, replayedResult) {
		t.Fatalf("restart replay did not retain exact result/ID/revision: replayed=%v result=%s want=%s", replay.Meta.Replayed, replayedResult, originalResult)
	}
	if after := sqliteRestartSnapshot(t, dbPath); !reflect.DeepEqual(beforeRestart, after) {
		t.Fatal("keyed replay added or changed graph/log/version/event/receipt state")
	}
	changed := map[string]any{"type": "issue", "fields": map[string]any{"title": "changed payload"}, "_meta": params["_meta"]}
	refused := sqliteRestartRPC(t, endpoint, "", "bead.create", changed)
	if refused.Error == nil || refused.Error.Data.Code != "idempotency_key_reused" || refused.Error.Data.Retryable || len(refused.Result) != 0 {
		t.Fatalf("changed keyed payload was not refused: %+v", refused)
	}
	if after := sqliteRestartSnapshot(t, dbPath); !reflect.DeepEqual(beforeRestart, after) {
		t.Fatal("refused changed payload mutated durable state")
	}
	if after := sqliteRestartRPC(t, endpoint, "", "history.query", historyParams).result(t); !bytes.Equal(history, after) {
		t.Fatalf("history changed across restart/replay/refusal: before=%s after=%s", history, after)
	}
	t.Logf("receipt restart PASS: replayed=true exact result/ID/revision; changed payload=idempotency_key_reused; head=%s rows=%v events=%d unchanged", beforeRestart.Head, beforeRestart.Counts, beforeRestart.Events)
	if rows := sqliteRestartPresence(t, endpoint); len(rows) != 0 {
		t.Fatalf("old ephemeral presence survived restart: %+v", rows)
	}
	oldPresence := sqliteRestartRPC(t, endpoint, "", "presence.set", map[string]any{"sessionId": oldSession, "focus": "bead/" + keyedID})
	if oldPresence.Error == nil || oldPresence.Error.Data.Code != "not_found" {
		t.Fatalf("old ephemeral session survived restart: %+v", oldPresence)
	}
	newSession := sqliteRestartOpenSession(t, endpoint)
	sqliteRestartRPC(t, endpoint, "", "presence.set", map[string]any{"sessionId": newSession, "focus": "bead/" + keyedID}).result(t)
	if rows := sqliteRestartPresence(t, endpoint); len(rows) != 1 || rows[0].SessionID != newSession {
		t.Fatalf("new collaboration session was not admitted: %+v", rows)
	}
	t.Log("collaboration restart: old presence absent, old session=not_found, new session admitted")
	resumed := sqliteRestartRPC(t, endpoint, newSession, "event.subscribe", map[string]any{"filter": map[string]any{}, "from": oldCursor})
	if resumed.Error != nil {
		// Retain a real acceptance RED without losing the independent live-delta
		// observation. A typed refusal is not a canonical terminal event.gap.
		t.Errorf("old cursor neither resumed nor returned a contract-valid terminal gap: %v", resumed.Error)
		subscription, _ = sqliteRestartSubscribe(t, endpoint, newSession, "")
	} else {
		var value struct {
			SubscriptionID string `json:"subscriptionId"`
			Cursor         string `json:"cursor"`
		}
		if err := json.Unmarshal(resumed.result(t), &value); err != nil || value.SubscriptionID == "" || value.Cursor == "" {
			t.Fatalf("restart subscription=%s err=%v", resumed.Result, err)
		}
		subscription = value.SubscriptionID
	}
	stream = sqliteRestartAttach(t, endpoint, newSession, subscription)
	opening = stream.next(t)
	if len(opening.Events) == 1 && opening.Events[0].Control != nil && opening.Events[0].Control.Type == model.EventGap {
		gap := opening.Events[0].Control
		if gap.Cursor != opening.Cursor {
			t.Fatal("terminal gap cursor does not match its envelope")
		}
		stream.eof(t)
		stream.close()
		t.Logf("old cursor restart: canonical terminal event.gap=%+v", gap.Details)
		subscription, _ = sqliteRestartSubscribe(t, endpoint, newSession, "")
		stream = sqliteRestartAttach(t, endpoint, newSession, subscription)
		opening = stream.next(t)
	}
	defer stream.close()
	updated := sqliteRestartRPC(t, endpoint, "", "bead.update", map[string]any{
		"id": map[string]any{"id": keyedID}, "fields": map[string]any{"title": "live after restart"},
		"_meta": sqliteRestartMeta("45b44a061aeb4ba6a128fe2e640255b4"),
	})
	var live model.Commit
	if err := json.Unmarshal(updated.result(t), &live); err != nil {
		t.Fatal(err)
	}
	delta := stream.next(t)
	if len(delta.Events) != 1 || delta.Events[0].Control == nil || delta.Events[0].Control.Type != model.EventWatchDelta {
		t.Fatalf("expected an actual live watch.delta: %+v", delta)
	}
	details, ok := delta.Events[0].Control.Details.(model.WatchDeltaDetails)
	if !ok || details.SubscriptionID != subscription || details.FromCursor != opening.Cursor || details.ToCursor != delta.Cursor || len(details.Events) != 1 {
		t.Fatalf("live delta cursor continuity=%+v", details)
	}
	event := details.Events[0].Durable
	if event == nil || event.Type != model.EventBeadUpdated || event.Resource != "bead/"+keyedID || event.Revision != live.Revision {
		t.Fatalf("live delta does not describe the real post-restart write: %+v", details.Events)
	}
	current := sqliteRestartRPC(t, endpoint, "", "bead.get", map[string]any{"id": map[string]any{"id": keyedID}}).result(t)
	if !bytes.Contains(current, []byte(`"live after restart"`)) {
		t.Fatalf("new session's live event lacks current readable state: %s", current)
	}
	t.Logf("current admitted session live delta PASS: pid=%d resource=%s revision=%s from/to cursors continuous", secondPID, event.Resource, event.Revision)
	stream.close()
	sqliteRestartRPC(t, endpoint, "", "session.close", map[string]any{"sessionId": newSession}).result(t)
	server.stop(t)
	if secondProcess.ProcessState == nil || !secondProcess.ProcessState.Exited() || secondProcess.ProcessState.ExitCode() != 0 {
		t.Fatal("second OS process was not waited with exit 0")
	}
	t.Logf("owned process cleanup: both PIDs %d and %d waited with exit 0; disposable SQLite fixture removed by t.TempDir", firstPID, secondPID)
}

type sqliteRestartResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *wire.Error     `json:"error,omitempty"`
	Meta    struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta,omitempty"`
}

func (r sqliteRestartResponse) result(t *testing.T) json.RawMessage {
	t.Helper()
	if r.Error != nil || len(r.Result) == 0 {
		t.Fatalf("RPC did not return a result: error=%v result=%s", r.Error, r.Result)
	}
	return r.Result
}

func sqliteRestartMeta(key string) map[string]any {
	meta := map[string]any{"project": "acceptance", "agent": "acceptance"}
	if key != "" {
		meta["idempotencyKey"] = key
	}
	return meta
}

func sqliteRestartRPC(t *testing.T, endpoint, session, method string, params map[string]any) sqliteRestartResponse {
	t.Helper()
	p := make(map[string]any, len(params)+1)
	for key, value := range params {
		p[key] = value
	}
	if _, ok := p["_meta"]; !ok {
		p["_meta"] = sqliteRestartMeta("")
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": p})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/rpc/v1", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if session != "" {
		req.Header.Set("Plug-Session", session)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s HTTP RPC: %v", method, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("%s HTTP status=%d body=%s err=%v", method, response.StatusCode, raw, err)
	}
	var value sqliteRestartResponse
	if err := wire.DecodeInto(raw, &value); err != nil || value.JSONRPC != "2.0" || value.ID != 1 {
		t.Fatalf("%s response=%s err=%v", method, raw, err)
	}
	return value
}

func sqliteRestartOpenSession(t *testing.T, endpoint string) string {
	t.Helper()
	var session struct {
		SessionID string `json:"sessionId"`
	}
	raw := sqliteRestartRPC(t, endpoint, "", "session.open", nil).result(t)
	if err := json.Unmarshal(raw, &session); err != nil || session.SessionID == "" {
		t.Fatalf("session.open=%s err=%v", raw, err)
	}
	return session.SessionID
}

func sqliteRestartPresence(t *testing.T, endpoint string) []struct {
	SessionID string `json:"sessionId"`
	Focus     string `json:"focus"`
} {
	t.Helper()
	var value struct {
		Presence []struct {
			SessionID string `json:"sessionId"`
			Focus     string `json:"focus"`
		} `json:"presence"`
	}
	raw := sqliteRestartRPC(t, endpoint, "", "presence.list", nil).result(t)
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value.Presence
}

func sqliteRestartSubscribe(t *testing.T, endpoint, session, from string) (string, string) {
	t.Helper()
	var value struct {
		SubscriptionID string `json:"subscriptionId"`
		Cursor         string `json:"cursor"`
	}
	raw := sqliteRestartRPC(t, endpoint, session, "event.subscribe", map[string]any{"filter": map[string]any{}, "from": from}).result(t)
	if err := json.Unmarshal(raw, &value); err != nil || value.SubscriptionID == "" || value.Cursor == "" {
		t.Fatalf("event.subscribe=%s err=%v", raw, err)
	}
	return value.SubscriptionID, value.Cursor
}

type sqliteRestartStream struct {
	body    io.ReadCloser
	decoder *json.Decoder
	cancel  context.CancelFunc
}

type sqliteRestartFrame struct {
	SubscriptionID string        `json:"subscriptionId"`
	Events         []model.Event `json:"events"`
	Cursor         string        `json:"cursor"`
}

func sqliteRestartAttach(t *testing.T, endpoint, session, subscription string) *sqliteRestartStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	body, err := json.Marshal(map[string]string{"subscriptionId": subscription})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/watch/v1", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-Session", session)
	req.Header.Set("Plug-Agent", "acceptance")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("watch HTTP: %v", err)
	}
	stream := &sqliteRestartStream{body: response.Body, decoder: json.NewDecoder(response.Body), cancel: cancel}
	t.Cleanup(stream.close)
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("watch status=%d type=%s", response.StatusCode, response.Header.Get("Content-Type"))
	}
	return stream
}

func (s *sqliteRestartStream) close() { s.cancel(); _ = s.body.Close() }

func (s *sqliteRestartStream) next(t *testing.T) sqliteRestartFrame {
	t.Helper()
	var value sqliteRestartFrame
	if err := s.decoder.Decode(&value); err != nil || value.SubscriptionID == "" || value.Cursor == "" {
		t.Fatalf("watch frame=%+v err=%v", value, err)
	}
	return value
}

func (s *sqliteRestartStream) eof(t *testing.T) {
	t.Helper()
	var extra json.RawMessage
	if err := s.decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("terminal gap did not end the actual stream: extra=%s err=%v", extra, err)
	}
}

type sqliteRestartState struct {
	Head, Incarnation string
	Counts            map[string]int
	Digests           map[string]string
	Events            int
}

// Observe the executable's actual database without opening a second service,
// acquiring a lease, supplying signing keys, or changing any stored value.
// Only the active lease is excluded: its epoch/holder must change on restart.
func sqliteRestartSnapshot(t *testing.T, path string) sqliteRestartState {
	t.Helper()
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	state := sqliteRestartState{Counts: map[string]int{}, Digests: map[string]string{}}
	if err := tx.QueryRowContext(ctx, "SELECT head, incarnation FROM meta WHERE singleton=1").Scan(&state.Head, &state.Incarnation); err != nil {
		t.Fatal(err)
	}
	state.Head = strings.TrimLeft(state.Head, "0")
	if state.Head == "" {
		state.Head = "0"
	}
	for _, table := range []string{"meta", "objects", "records", "versions", "key_history", "blobs", "changesets", "replays", "checkpoints", "core_sessions", "allocator_states"} {
		rows, err := tx.QueryContext(ctx, "SELECT * FROM "+table+" ORDER BY 1, 2")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var values [][]string
		for rows.Next() {
			cells := make([][]byte, len(columns))
			pointers := make([]any, len(columns))
			for i := range cells {
				pointers[i] = &cells[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			row := make([]string, len(cells))
			for i, value := range cells {
				row[i] = fmt.Sprintf("%x", value)
				if value == nil {
					row[i] = "null"
				}
			}
			values = append(values, row)
			if table == "changesets" {
				var changeset model.Changeset
				if err := json.Unmarshal(cells[1], &changeset); err != nil {
					t.Fatal(err)
				}
				if changeset.Events == nil {
					state.Events += len(changeset.Changes)
				} else {
					state.Events += len(changeset.Events)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		state.Counts[table], state.Digests[table] = len(values), fmt.Sprintf("%x", sha256.Sum256(raw))
	}
	return state
}

func TestExecutableLocalServerIsDisposableAndWarns(t *testing.T) {
	dir := t.TempDir()
	server := startPlugServer(t, dir, true)
	t.Cleanup(func() { server.stop(t) })
	endpoint := server.endpoint
	out, errOut, err := remoteCommand(t, "", endpoint, "create", "--title", "ephemeral")
	if err != nil || out == "" {
		t.Fatalf("local create: %v stdout=%s stderr=%s", err, out, errOut)
	}
	if !strings.Contains(server.stderr.String(), "memory storage is disposable") {
		// The warning is written during startup and may arrive just after the
		// endpoint line; give the process a short scheduling window.
		time.Sleep(20 * time.Millisecond)
	}
	server.stop(t)
	server = startPlugServer(t, dir, true)
	t.Cleanup(func() { server.stop(t) })
	endpoint = server.endpoint
	out, _, err = remoteCommand(t, "", endpoint, "ready")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.DecodeJSON([]byte(strings.TrimSpace(out)))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.(map[string]any)["items"].([]any)) != 0 {
		t.Fatalf("local restart retained data: %s", out)
	}
}

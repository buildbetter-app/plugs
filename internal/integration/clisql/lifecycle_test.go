package clisql

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func plugBinary(t *testing.T) string {
	t.Helper()
	path := os.Getenv("PLUG_BINARY")
	if path == "" {
		t.Fatal("Bazel must supply the built plug executable")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), path)
	}
	return path
}

func runCLI(t *testing.T, server, token, input string, args ...string) (string, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, plugBinary(t), args...)
	cmd.Env = append(os.Environ(), "PLUG_SERVER="+server, "PLUG_TOKEN="+token)
	cmd.Stdin = strings.NewReader(input)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("built plug timed out: %v; stderr=%s", ctx.Err(), stderr.String())
	}
	return stdout.String(), stderr.String(), err
}

func commandInput(t *testing.T, commands ...[]string) string {
	t.Helper()
	var input strings.Builder
	for _, command := range commands {
		data, err := json.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		input.Write(data)
		input.WriteByte('\n')
	}
	return input.String()
}

func decodeCLI(t *testing.T, output string) []map[string]any {
	t.Helper()
	var results []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		value, err := wire.DecodeJSON([]byte(line))
		if err != nil {
			t.Fatalf("CLI output was not JSON: %v (%q)", err, line)
		}
		object, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("CLI result is not an object: %#v", value)
		}
		results = append(results, object)
	}
	return results
}

func resultRevision(t *testing.T, result map[string]any) string {
	t.Helper()
	revision, ok := result["revision"].(json.Number)
	if !ok {
		t.Fatalf("result has no numeric revision: %#v", result)
	}
	return revision.String()
}

func resultID(t *testing.T, result map[string]any) string {
	t.Helper()
	rows, ok := result["results"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("write result has no one operation result: %#v", result)
	}
	row, ok := rows[0].(map[string]any)
	if !ok {
		t.Fatalf("operation result is not an object: %#v", rows[0])
	}
	ref, ok := row["ref"].(map[string]any)
	if !ok {
		t.Fatalf("operation result has no ref: %#v", row)
	}
	id, ok := ref["id"].(string)
	if !ok || id == "" {
		t.Fatalf("operation result has no concrete id: %#v", ref)
	}
	return id
}

func assertEmptyWarnings(t *testing.T, result map[string]any) {
	t.Helper()
	warnings, ok := result["warnings"].([]any)
	if !ok || len(warnings) != 0 {
		t.Fatalf("committed result warning shape changed: %#v", result["warnings"])
	}
}

func TestBuiltRemoteCLIReopensSQLiteAndPostgres(t *testing.T) {
	principal := model.PrincipalRef{Kind: "user", ID: "user:cli-operator"}
	for _, kind := range sqlKinds(t) {
		t.Run(kind.name, func(t *testing.T) {
			dsn := sqliteDSN(t)
			if kind.name == "postgres" {
				dsn = postgresDatabaseURL(t)
			}
			provider := authnapikey.New(authnapikey.Config{})
			issued, err := provider.Issue(principal, time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			server := openSQLServer(t, kind, dsn, provider)
			defer server.close(t)

			createKey := strings.Repeat("a", 64)
			commands := commandInput(t,
				[]string{"ready"},
				[]string{"create", "--title", "Original", "--idempotency-key", createKey},
				[]string{"claim", "@last"},
				[]string{"update", "@last", "--title", "Updated"},
				[]string{"comment", "@last", "--body", "Evidence"},
				[]string{"close", "@last", "--reason", "Finished"},
				[]string{"history", "--as-of", "@revision", "--id", "@last"},
				[]string{"get", "@last"},
			)
			// The first run selects the remote server through PLUG_SERVER. It is a
			// separate process, so @last and @revision exercise the real CLI session.
			out, errOut, err := runCLI(t, server.url, issued.Token, commands, "--project", "sql-e2e", "--agent", "cli-agent", "--json", "session")
			if err != nil {
				t.Fatalf("remote CLI lifecycle failed: %v\nstdout=%s\nstderr=%s", err, out, errOut)
			}
			if errOut != "" {
				t.Fatalf("successful SQL CLI lifecycle wrote diagnostics: %s", errOut)
			}
			results := decodeCLI(t, out)
			if len(results) != 8 || len(results[0]["items"].([]any)) != 0 {
				t.Fatalf("unexpected lifecycle result count/readiness: %s", out)
			}
			id := resultID(t, results[1])
			if resultRevision(t, results[1]) != "1" || resultRevision(t, results[2]) != "2" || resultRevision(t, results[3]) != "3" || resultRevision(t, results[4]) != "4" || resultRevision(t, results[5]) != "5" {
				t.Fatalf("CLI lifecycle revisions changed: %s", out)
			}
			for _, result := range results[1:6] {
				assertEmptyWarnings(t, result)
			}
			if results[6]["asOf"].(map[string]any)["revision"] != json.Number("5") || results[7]["bead"].(map[string]any)["revision"] != json.Number("5") {
				t.Fatalf("history/current result revision changed: %s", out)
			}
			if got := results[7]["bead"].(map[string]any)["status"]; got != "done" {
				t.Fatalf("CLI get did not see terminal state: %#v", got)
			}

			// A second request through the actual HTTP endpoint proves the response
			// replay envelope independently of the CLI client's inner result JSON.
			replay := postRPC(t, server.url, issued.Token, map[string]any{
				"jsonrpc": "2.0", "id": "replay", "method": "bead.create",
				"params": map[string]any{
					"type": "issue", "fields": map[string]any{"title": "Original"},
					"_meta": map[string]any{"project": "sql-e2e", "agent": "cli-agent", "idempotencyKey": createKey},
				},
			})
			if replay.status != http.StatusOK || replay.value["_meta"] == nil {
				t.Fatalf("actual RPC replay response missing HTTP success metadata: %#v", replay.value)
			}
			if meta, ok := replay.value["_meta"].(map[string]any); !ok || meta["replayed"] != true {
				t.Fatalf("actual RPC replay metadata changed: %#v", replay.value["_meta"])
			}
			if result, ok := replay.value["result"].(map[string]any); !ok || resultRevision(t, result) != "1" || resultID(t, result) != id {
				t.Fatalf("actual RPC replay did not return original result: %#v", replay.value["result"])
			}

			// Close both the listener and the SQL store, then open a fresh handle.
			server.close(t)
			server.reopen(t)
			defer server.close(t)
			// --server must win over a deliberately unusable PLUG_SERVER value.
			freshOut, freshErr, freshRunErr := runCLI(t, "http://127.0.0.1:1", issued.Token, "", "--server", server.url, "--project", "sql-e2e", "--agent", "cli-agent", "--json", "get", id)
			if freshRunErr != nil || freshErr != "" {
				t.Fatalf("fresh-handle CLI flag selection failed: %v %s %s", freshRunErr, freshOut, freshErr)
			}
			fresh := decodeCLI(t, freshOut)
			if len(fresh) != 1 || fresh[0]["bead"].(map[string]any)["status"] != "done" || fresh[0]["bead"].(map[string]any)["fields"].(map[string]any)["title"] != "Updated" {
				t.Fatalf("fresh SQL handle lost current state: %s", freshOut)
			}
			historyOut, historyErr, historyRunErr := runCLI(t, server.url, issued.Token, "", "--project", "sql-e2e", "--agent", "cli-agent", "--json", "history", "--as-of", "5", "--id", id)
			if historyRunErr != nil || historyErr != "" {
				t.Fatalf("fresh-handle history failed: %v %s %s", historyRunErr, historyOut, historyErr)
			}
			history := decodeCLI(t, historyOut)
			if len(history) != 1 || len(history[0]["items"].([]any)) != 1 || history[0]["items"].([]any)[0].(map[string]any)["status"] != "done" {
				t.Fatalf("fresh SQL handle lost history: %s", historyOut)
			}

			reader := server.reader(t)
			ref := model.RefByID(id, model.Natural64(0))
			records, err := reader.Get(context.Background(), []driver.Ref{ref})
			if err != nil || len(records) != 1 {
				t.Fatalf("fresh SQL reader did not retain record: %v %v", records, err)
			}
			var document struct {
				Status   string         `json:"status"`
				Fields   map[string]any `json:"fields"`
				Comments []struct {
					Body      string             `json:"body"`
					Principal model.PrincipalRef `json:"principal"`
				} `json:"comments"`
			}
			if err := json.Unmarshal(records[0].Body, &document); err != nil {
				t.Fatal(err)
			}
			if document.Fields["status"] != "done" || document.Fields["title"] != "Updated" || len(document.Comments) != 2 || document.Comments[0].Body != "Evidence" || document.Comments[1].Body != "Finished" {
				t.Fatalf("fresh persisted document lost lifecycle data: %+v", document)
			}
			for _, comment := range document.Comments {
				if !reflect.DeepEqual(comment.Principal, principal) {
					t.Fatalf("comment caller identity was not stored: %+v", comment.Principal)
				}
			}
			changesets, err := reader.Changesets(context.Background(), nil, 20)
			if err != nil || len(changesets) != 5 {
				t.Fatalf("fresh SQL changesets=%d err=%v", len(changesets), err)
			}
			for _, changeset := range changesets {
				if !reflect.DeepEqual(changeset.Principal, principal) || changeset.IdempotencyKey == "" {
					t.Fatalf("stored caller or idempotency attribution changed: %+v", changeset)
				}
			}
			encoded, _ := json.Marshal(changesets)
			if bytes.Contains(encoded, []byte(issued.Token)) || bytes.Contains(encoded, []byte(issued.ID)) {
				t.Fatal("credential material reached durable changesets")
			}

			// The configured server refuses a credential-free CLI request without
			// leaking the bearer token or its key ID in diagnostics.
			unauthOut, unauthErr, unauthRunErr := runCLI(t, server.url, "", "", "--project", "sql-e2e", "--json", "ready")
			if unauthRunErr == nil || unauthOut != "" || !strings.Contains(unauthErr, "unauthenticated") || strings.Contains(unauthErr, issued.Token) || strings.Contains(unauthErr, issued.ID) {
				t.Fatalf("credential-free CLI diagnostics changed: out=%q err=%q run=%v", unauthOut, unauthErr, unauthRunErr)
			}

			unknown := postRPC(t, server.url, issued.Token, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "bead.unknown", "params": map[string]any{}})
			if unknown.status != http.StatusOK || unknown.value["result"] != nil || unknown.value["error"] == nil {
				t.Fatalf("unknown RPC method was not refused: %#v", unknown.value)
			}
			if e, ok := unknown.value["error"].(map[string]any); !ok || e["data"].(map[string]any)["code"] != "method_not_found" {
				t.Fatalf("unknown RPC method error changed: %#v", unknown.value["error"])
			}
		})
	}
}

type rpcReply struct {
	status int
	value  map[string]any
}

func postRPC(t *testing.T, server, token string, request map[string]any) rpcReply {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server+httpjson.Path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Plug-RPC", "1")
	httpRequest.Header.Set("Content-Type", "application/json")
	if token != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	value, err := wire.DecodeJSON(data)
	if err != nil {
		t.Fatalf("RPC response was not JSON: %v (%s)", err, data)
	}
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("RPC response was not an object: %#v", value)
	}
	return rpcReply{status: response.StatusCode, value: object}
}

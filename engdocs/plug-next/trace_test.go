package plugnext_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

//go:embed cli-rpc-lifecycle-trace.json
var capturedTrace []byte

type trace struct {
	FixedCoreClock    time.Time         `json:"fixedCoreClock"`
	RPCRequest        json.RawMessage   `json:"rpcRequest"`
	RPCResponse       json.RawMessage   `json:"rpcResponse"`
	RPCReplayResponse json.RawMessage   `json:"rpcReplayResponse"`
	CLICommands       [][]string        `json:"cliCommands"`
	CLIResults        []json.RawMessage `json:"cliResults"`
}

func sameJSON(t *testing.T, got, want []byte) bool {
	t.Helper()
	var left, right any
	if json.Unmarshal(got, &left) != nil || json.Unmarshal(want, &right) != nil {
		t.Fatal("invalid JSON in executable documentation")
	}
	return reflect.DeepEqual(left, right)
}

// This executable documentation uses the real HTTP/service/Writer/store path.
// Bodies contain only synthetic records. Credentials and headers are never logged.
func TestDisposableCLIAndRPCTrace(t *testing.T) {
	for _, mode := range []string{"anonymous", "bearer"} {
		t.Run(mode, func(t *testing.T) { runTrace(t, mode == "bearer") })
	}
}

func runTrace(t *testing.T, authenticated bool) {
	t.Helper()
	var capture trace
	if err := json.Unmarshal(capturedTrace, &capture); err != nil {
		t.Fatal(err)
	}
	if capture.FixedCoreClock.IsZero() || capture.FixedCoreClock.Location() != time.UTC {
		t.Fatal("executable expectations require an explicit nonzero UTC core clock")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "docs-example", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(context.Background())
	workflow := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(workflow),
		pipeline.WithClock(func() time.Time { return capture.FixedCoreClock }),
		pipeline.WithIDGenerator(func() (string, error) { return "pl-guide-1", nil }))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, service.New(store, lease, writer, service.WithOntology(workflow))); err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	config := rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "docs", OntologyRevision: model.Natural64(1)}
	authConfig := authn.Config{}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	token, keyID := "", ""
	keys := authnapikey.New(authnapikey.Config{})
	if authenticated {
		by = model.PrincipalRef{Kind: "service", ID: "service:docs-example"}
		issued, err := keys.Issue(by, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		token, keyID = issued.Token, issued.ID
		authConfig.Provider = keys
		config.AuthMode = "authenticated"
	}
	authenticator, err := authn.New(authConfig)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, config)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, authenticator, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	httpServer, err := handler.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve() }()
	defer func() {
		drain, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := httpServer.Shutdown(drain); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	}()
	endpoint := "http://" + httpServer.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	call := func(request string, status int) map[string]json.RawMessage {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+httpjson.Path, strings.NewReader(request))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Plug-RPC", "1")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal("local HTTP call failed")
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil || response.StatusCode != status {
			t.Fatalf("HTTP status=%d, want=%d", response.StatusCode, status)
		}
		t.Logf("RPC request: %s\nRPC response: %s", request, body)
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		return envelope
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, capture.RPCRequest); err != nil {
		t.Fatal(err)
	}
	request := compact.String()
	first := call(request, http.StatusOK)
	second := call(request, http.StatusOK)
	if first["error"] != nil || second["error"] != nil || !bytes.Equal(first["result"], second["result"]) || !strings.Contains(string(second["_meta"]), `"replayed":true`) {
		t.Fatal("replay did not preserve the successful result and transient metadata")
	}
	for _, pair := range []struct {
		got  map[string]json.RawMessage
		want json.RawMessage
	}{{first, capture.RPCResponse}, {second, capture.RPCReplayResponse}} {
		got, err := json.Marshal(pair.got)
		if err != nil || !sameJSON(t, got, pair.want) {
			t.Fatal("captured RPC body differs from the real response")
		}
	}
	binary := os.Getenv("PLUG_BINARY")
	if binary == "" {
		t.Fatal("Bazel must supply the real plug executable")
	}
	if !filepath.IsAbs(binary) {
		binary = filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), binary)
	}
	var input strings.Builder
	for _, command := range capture.CLICommands {
		line, err := json.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		input.Write(line)
		input.WriteByte('\n')
	}
	input.WriteString("[\"close\",\"pl-guide-1\",\"--reason\",\"Done\",\"--expect\",\"2\",\"--idempotency-key\",\"875ba243-c607-4944-a94c-cb41e32163f1\"]\n[\"ready\"]\n")
	commands := input.String()
	cmd := exec.CommandContext(ctx, binary, "--server", endpoint, "--project", "docs", "--agent", "docs-guide", "--json", "session")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "PLUG_SERVER=") && !strings.HasPrefix(entry, "PLUG_TOKEN=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "PLUG_TOKEN="+token)
	cmd.Stdin = strings.NewReader(commands)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		detail := stderr.String()
		if token != "" {
			detail = strings.ReplaceAll(detail, token, "<redacted>")
		}
		t.Fatalf("documented CLI session failed: %v; %s", err, detail)
	}
	t.Logf("CLI commands:\n%sCLI stdout:\n%s", commands, &stdout)
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 4 || stderr.Len() != 0 || !strings.Contains(lines[0], `"revision":2`) || !strings.Contains(lines[1], `"title":"Original"`) || !strings.Contains(lines[2], `"revision":3`) || !strings.Contains(lines[3], `"items":[]`) {
		t.Fatal("documented CLI trace changed")
	}
	if len(capture.CLIResults) != 2 {
		t.Fatal("capture must contain update and historical-get results")
	}
	for i, want := range capture.CLIResults {
		if !sameJSON(t, []byte(lines[i]), want) {
			t.Fatal("captured CLI JSON differs from the real executable")
		}
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	changes, err := reader.Changesets(ctx, nil, 10)
	if err != nil || len(changes) != 3 {
		t.Fatal("CLI/RPC did not retain exactly three real changesets")
	}
	for _, change := range changes {
		if !reflect.DeepEqual(change.Principal, by) {
			t.Fatal("admitted identity did not reach the real changeset")
		}
	}
	t.Logf("stored changesets=3, admitted principal=%s", by.ID)
	if authenticated {
		if !keys.Revoke(keyID) {
			t.Fatal("key revocation failed")
		}
		refused := call(request, http.StatusUnauthorized)
		if !strings.Contains(string(refused["error"]), `"code":"unauthenticated"`) {
			t.Fatal("revoked credential did not refuse before replay")
		}
	}
}

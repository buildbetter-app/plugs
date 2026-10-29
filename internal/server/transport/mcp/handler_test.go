package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type warningPost struct{ calls atomic.Int64 }

func (p *warningPost) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls.Add(1)
	return errors.New("fixture post failure")
}

type fixture struct {
	handler *mcp.Handler
	rpc     *rpc.Server
	post    *warningPost
	store   *memory.Store
	lease   driver.ActiveLease
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "mcp-fixture", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	post := &warningPost{}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "mcp-warning", Subscriber: post}))
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	rpcServer, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "mcp-test"})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := mcp.New(rpcServer, authenticator, mcp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{handler: handler, rpc: rpcServer, post: post, store: store, lease: lease}
}

func call(t *testing.T, f *fixture, body string) (int, []byte) {
	t.Helper()
	return callHandler(t, f.handler, body)
}

func callHandler(t *testing.T, handler *mcp.Handler, body string) (int, []byte) {
	return callHandlerWithHeaders(t, handler, body, nil)
}

func callHandlerWithHeaders(t *testing.T, handler *mcp.Handler, body string, headers map[string]string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://localhost/mcp", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:31000"
	req.Header.Set("Plug-RPC", "1")
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder.Code, recorder.Body.Bytes()
}

func decode(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var value map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		t.Fatalf("invalid JSON %q: %v", body, err)
	}
	return value
}

func errorCode(t *testing.T, body []byte) string {
	t.Helper()
	value := decode(t, body)
	errorValue, ok := value["error"].(map[string]any)
	if !ok {
		t.Fatalf("missing protocol error: %s", body)
	}
	data := errorValue["data"].(map[string]any)
	return data["code"].(string)
}

func TestMCPInitializeAndToolProjection(t *testing.T) {
	f := newFixture(t)
	status, body := call(t, f, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}}`)
	if status != http.StatusOK {
		t.Fatalf("initialize status=%d body=%s", status, body)
	}
	result := decode(t, body)["result"].(map[string]any)
	if result["protocolVersion"] != "2025-03-26" {
		t.Fatalf("protocol version=%v", result["protocolVersion"])
	}
	capabilities := result["capabilities"].(map[string]any)
	if capabilities["tools"].(map[string]any)["listChanged"] != true {
		t.Fatal("tools/list_changed capability missing")
	}
	status, body = call(t, f, `{"jsonrpc":"2.0","id":"list","method":"tools/list","params":{}}`)
	if status != http.StatusOK {
		t.Fatalf("tools/list status=%d body=%s", status, body)
	}
	tools := decode(t, body)["result"].(map[string]any)["tools"].([]any)
	seen := map[string]bool{}
	for _, raw := range tools {
		seen[raw.(map[string]any)["name"].(string)] = true
	}
	for _, name := range []string{"bead_ready", "bead_create", "bead_close", "plug_call", "plug_describe"} {
		if !seen[name] {
			t.Fatalf("tools/list omitted %q: %v", name, seen)
		}
	}
	if seen["bead_get"] {
		t.Fatal("non-agent-core method was exposed as a direct tool")
	}
}

func TestMCPCanonicalResultsWarningsAndErrors(t *testing.T) {
	f := newFixture(t)
	status, body := call(t, f, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"bead_create","arguments":{"type":"issue","fields":{"title":"MCP"}}}}`)
	if status != http.StatusOK {
		t.Fatalf("tools/call status=%d body=%s", status, body)
	}
	result := decode(t, body)["result"].(map[string]any)
	if result["isError"] == true {
		t.Fatalf("create returned tool error: %s", body)
	}
	content := result["content"].([]any)
	if len(content) != 1 || content[0].(map[string]any)["type"] != "text" {
		t.Fatalf("unexpected MCP content: %s", body)
	}
	var canonical map[string]any
	if err := json.Unmarshal([]byte(content[0].(map[string]any)["text"].(string)), &canonical); err != nil {
		t.Fatal(err)
	}
	warnings := canonical["warnings"].([]any)
	if len(warnings) != 1 || warnings[0].(map[string]any)["code"] != "post_subscriber_failed" {
		t.Fatalf("canonical warnings not surfaced: %s", content[0].(map[string]any)["text"])
	}
	if f.post.calls.Load() != 1 {
		t.Fatalf("post subscriber calls=%d", f.post.calls.Load())
	}

	status, body = call(t, f, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"bead_close","arguments":{"id":{"id":"missing"},"reason":"no such bead"}}}`)
	if status != http.StatusOK {
		t.Fatalf("error call status=%d body=%s", status, body)
	}
	result = decode(t, body)["result"].(map[string]any)
	if result["isError"] != true {
		t.Fatalf("canonical domain error was not a tool error: %s", body)
	}
	var canonicalError map[string]any
	if err := json.Unmarshal([]byte(result["content"].([]any)[0].(map[string]any)["text"].(string)), &canonicalError); err != nil {
		t.Fatal(err)
	}
	if canonicalError["data"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("domain error changed: %s", body)
	}
}

func TestMCPRejectsMalformedAndUnsupportedInvocations(t *testing.T) {
	f := newFixture(t)
	status, body := call(t, f, `{`)
	if status != http.StatusOK || errorCode(t, body) != "parse_error" {
		t.Fatalf("malformed request status=%d body=%s", status, body)
	}
	status, body = call(t, f, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"unknown_tool","arguments":{}}}`)
	if status != http.StatusOK || errorCode(t, body) != "method_not_found" {
		t.Fatalf("unknown tool status=%d body=%s", status, body)
	}
	status, body = call(t, f, `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"bead_create","arguments":{"type":"issue","fields":{},"principal":"caller"}}}`)
	if status != http.StatusOK {
		t.Fatalf("unsupported argument status=%d body=%s", status, body)
	}
	toolResult := decode(t, body)["result"].(map[string]any)
	if toolResult["isError"] != true || !strings.Contains(toolResult["content"].([]any)[0].(map[string]any)["text"].(string), `"code":"invalid_params"`) {
		t.Fatalf("unsupported argument was not rejected with canonical error: %s", body)
	}
	status, body = call(t, f, `{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`)
	if status != http.StatusAccepted || len(body) != 0 {
		t.Fatalf("notification status=%d body=%q", status, body)
	}
	status, body = call(t, f, `{"jsonrpc":"2.0","id":7,"method":"tools/list","params":{"cursor":"unsupported"}}`)
	if status != http.StatusOK || errorCode(t, body) != "invalid_params" {
		t.Fatalf("unsupported cursor status=%d body=%s", status, body)
	}
}

func TestMCPUsesCanonicalBytesForDirectComparison(t *testing.T) {
	f := newFixture(t)
	request := `{"type":"issue","fields":{"title":"same bytes"},"_meta":{"idempotencyKey":"550e8400-e29b-41d4-a716-446655440000"}}`
	status, body := call(t, f, `{"jsonrpc":"2.0","id":"mcp","method":"tools/call","params":{"name":"bead_create","arguments":`+request+`}}`)
	if status != http.StatusOK {
		t.Fatal(body)
	}
	text := decode(t, body)["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
	canonical := f.rpc.Dispatch(mcp.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}), []byte(`{"jsonrpc":"2.0","id":"canonical","method":"bead.create","params":`+request+`}`))
	var canonicalEnvelope map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &canonicalEnvelope); err != nil {
		t.Fatal(err)
	}
	if string(canonicalEnvelope["result"]) != text {
		t.Fatalf("MCP changed canonical result bytes\nMCP: %s\nRPC: %s", text, canonicalEnvelope["result"])
	}
}

func TestMCPBatchesRequestsAndAcceptsNotificationsWith202(t *testing.T) {
	f := newFixture(t)
	status, body := call(t, f, `[`+
		`{"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"batch","version":"1"}}},`+
		`{"jsonrpc":"2.0","id":"ping","method":"ping","params":{}},`+
		`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`+
		`]`)
	if status != http.StatusOK {
		t.Fatalf("batch status=%d body=%s", status, body)
	}
	var responses []map[string]any
	if err := json.Unmarshal(body, &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 || responses[0]["id"] != "init" || responses[1]["id"] != "ping" {
		t.Fatalf("batch responses=%s", body)
	}
	status, body = call(t, f, `{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`)
	if status != http.StatusAccepted || len(body) != 0 {
		t.Fatalf("notification status=%d body=%q", status, body)
	}
}

func TestMCPPreservesCanonicalReplayMetadataAndCallerKey(t *testing.T) {
	f := newFixture(t)
	key := "550e8400-e29b-41d4-a716-446655440000"
	body := `{"jsonrpc":"2.0","id":"replay","method":"tools/call","params":{"name":"bead_create","_meta":{"idempotencyKey":"` + key + `","agent":"caller"},"arguments":{"type":"issue","fields":{"title":"replayed"}}}}`
	status, first := call(t, f, body)
	if status != http.StatusOK {
		t.Fatalf("first call status=%d body=%s", status, first)
	}
	status, second := call(t, f, body)
	if status != http.StatusOK {
		t.Fatalf("replay status=%d body=%s", status, second)
	}
	outer := decode(t, second)
	meta, ok := outer["_meta"].(map[string]any)
	if !ok || meta["replayed"] != true {
		t.Fatalf("canonical replay metadata was not preserved: %s", second)
	}
	firstText := decode(t, first)["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"]
	secondText := outer["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"]
	if firstText != secondText {
		t.Fatalf("replay changed canonical result\nfirst=%s\nsecond=%s", firstText, secondText)
	}
}

func TestMCPInitializeValidatesRequiredParameters(t *testing.T) {
	f := newFixture(t)
	for _, body := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"only"}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":"future","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}}`,
	} {
		status, response := call(t, f, body)
		if status != http.StatusOK || errorCode(t, response) != "invalid_params" {
			t.Fatalf("invalid initialize status=%d body=%s", status, response)
		}
	}
}

func TestMCPEmitsToolsListChangedWhenVisibleHashChanges(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "mcp-list-change", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(ctx) })
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	domain := service.New(store, lease, pipeline.New(store, lease, pipeline.WithOntology(ontology)), service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	var allowReady atomic.Bool
	allowReady.Store(true)
	rpcServer, err := rpc.New(registry, rpc.Config{
		Incarnation:    meta.Incarnation,
		DefaultProject: "mcp-test",
		Visible: func(_ context.Context, _ model.PrincipalRef, method wire.MethodDesc) (bool, error) {
			return method.Name != "bead.ready" || allowReady.Load(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	notifications := make(chan []byte, 1)
	handler, err := mcp.New(rpcServer, authenticator, mcp.Config{OnNotification: func(_ context.Context, _ model.PrincipalRef, data []byte) { notifications <- data }})
	if err != nil {
		t.Fatal(err)
	}
	status, body := callHandler(t, handler, `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	if status != http.StatusOK {
		t.Fatalf("first tools/list status=%d body=%s", status, body)
	}
	allowReady.Store(false)
	status, body = callHandler(t, handler, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	if status != http.StatusOK {
		t.Fatalf("second tools/list status=%d body=%s", status, body)
	}
	select {
	case notification := <-notifications:
		if string(notification) != `{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}` {
			t.Fatalf("unexpected list-change notification: %s", notification)
		}
	case <-time.After(time.Second):
		t.Fatal("tools/list_changed notification was not emitted")
	}
}

func TestMCPCancelledNotificationCancelsActiveCall(t *testing.T) {
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	var once sync.Once
	if err := registry.Register("bead.ready", func(ctx context.Context, _ rpc.Call) (any, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return nil, &model.Error{Data: model.ErrorData{Code: "busy", Hint: "request canceled"}}
	}); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "mcp-cancel", DefaultProject: "mcp-test"})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := mcp.New(server, authenticator, mcp.Config{})
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan struct {
		status int
		body   []byte
	}, 1)
	go func() {
		status, body := callHandler(t, handler, `{"jsonrpc":"2.0","id":"active","method":"tools/call","params":{"name":"bead_ready","arguments":{}}}`)
		result <- struct {
			status int
			body   []byte
		}{status: status, body: body}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("canonical MCP call did not start")
	}

	status, body := callHandler(t, handler, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"active","reason":"fixture"}}`)
	if status != http.StatusAccepted || len(body) != 0 {
		t.Fatalf("cancellation notification status=%d body=%q", status, body)
	}
	select {
	case got := <-result:
		if got.status != http.StatusOK {
			t.Fatalf("canceled call status=%d body=%s", got.status, got.body)
		}
		toolResult := decode(t, got.body)["result"].(map[string]any)
		if toolResult["isError"] != true || !strings.Contains(toolResult["content"].([]any)[0].(map[string]any)["text"].(string), `"code":"busy"`) {
			t.Fatalf("canceled call did not preserve canonical busy error: %s", got.body)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled canonical MCP call did not finish")
	}
}

func TestMCPCancellationIsScopedToSession(t *testing.T) {
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 2)
	if err := registry.Register("bead.ready", func(ctx context.Context, _ rpc.Call) (any, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, &model.Error{Data: model.ErrorData{Code: "busy", Hint: "request canceled"}}
	}); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: "mcp-scoped-cancel", DefaultProject: "mcp-test"})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := mcp.New(server, authenticator, mcp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"jsonrpc":"2.0","id":"same","method":"tools/call","params":{"name":"bead_ready","arguments":{}}}`
	results := make(chan struct {
		session string
		status  int
		body    []byte
	}, 2)
	for _, session := range []string{"session-a", "session-b"} {
		session := session
		go func() {
			status, response := callHandlerWithHeaders(t, handler, body, map[string]string{"MCP-Session-Id": session})
			results <- struct {
				session string
				status  int
				body    []byte
			}{session: session, status: status, body: response}
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("both session calls did not start")
		}
	}
	status, response := callHandlerWithHeaders(t, handler, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"same"}}`, map[string]string{"MCP-Session-Id": "session-a"})
	if status != http.StatusAccepted || len(response) != 0 {
		t.Fatalf("session cancellation status=%d body=%q", status, response)
	}
	select {
	case result := <-results:
		if result.session != "session-a" || result.status != http.StatusOK {
			t.Fatalf("wrong session canceled first: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("session-a call did not cancel")
	}
	select {
	case result := <-results:
		t.Fatalf("session-b was canceled by session-a: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	status, response = callHandlerWithHeaders(t, handler, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"same"}}`, map[string]string{"MCP-Session-Id": "session-b"})
	if status != http.StatusAccepted || len(response) != 0 {
		t.Fatalf("second session cancellation status=%d body=%q", status, response)
	}
	select {
	case result := <-results:
		if result.session != "session-b" || result.status != http.StatusOK {
			t.Fatalf("session-b result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("session-b call did not cancel")
	}
}

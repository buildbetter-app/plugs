package mcp_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type notificationProvider struct {
	principals map[string]model.PrincipalRef
}

func (p notificationProvider) Authenticate(_ context.Context, token string) (model.PrincipalRef, error) {
	principal, ok := p.principals[token]
	if !ok {
		return model.PrincipalRef{}, authn.ErrUnauthenticated
	}
	return principal, nil
}

type notificationFixture struct {
	handler *mcp.Handler
	allow   atomic.Bool
	server  *httptest.Server
	store   *memory.Store
	lease   driver.ActiveLease
}

func newNotificationFixture(t *testing.T) *notificationFixture {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "mcp-notification-fixture", driver.LeaseTTL)
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
	fixture := &notificationFixture{store: store, lease: lease}
	fixture.allow.Store(true)
	rpcServer, err := rpc.New(registry, rpc.Config{
		Incarnation:    meta.Incarnation,
		DefaultProject: "mcp-test",
		Visible: func(_ context.Context, _ model.PrincipalRef, method wire.MethodDesc) (bool, error) {
			return method.Name != "bead.ready" || fixture.allow.Load(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{Provider: notificationProvider{principals: map[string]model.PrincipalRef{
		"alpha": {Kind: "user", ID: "user:alpha"},
		"beta":  {Kind: "user", ID: "user:beta"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.handler, err = mcp.New(rpcServer, authenticator, mcp.Config{})
	if err != nil {
		t.Fatal(err)
	}
	fixture.server = httptest.NewServer(fixture.handler)
	t.Cleanup(func() {
		_ = fixture.handler.Close()
		fixture.server.Close()
	})
	return fixture
}

func notificationRequest(t *testing.T, ctx context.Context, client *http.Client, method, url, token, session string, body io.Reader) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Authorization", "Bearer "+token)
	if session != "" {
		request.Header.Set("MCP-Session-Id", session)
	}
	if method == http.MethodGet {
		request.Header.Set("Accept", "text/event-stream")
	} else {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func initializeHTTPNotificationSession(t *testing.T, client *http.Client, endpoint, token string) string {
	t.Helper()
	body := strings.NewReader(`{"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}}`)
	response := notificationRequest(t, context.Background(), client, http.MethodPost, endpoint, token, "", body)
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("MCP-Protocol-Version") != "2025-03-26" {
		t.Fatalf("initialize status=%d protocol=%q body=%s", response.StatusCode, response.Header.Get("MCP-Protocol-Version"), data)
	}
	if !json.Valid(data) {
		t.Fatalf("initialize response is not JSON: %s", data)
	}
	session := response.Header.Get("MCP-Session-Id")
	if session == "" {
		t.Fatal("initialize did not issue MCP-Session-Id")
	}
	return session
}

type sseReader struct {
	body     io.ReadCloser
	messages chan []byte
	done     chan struct{}
}

func newSSEReader(body io.ReadCloser) *sseReader {
	reader := &sseReader{body: body, messages: make(chan []byte, 1), done: make(chan struct{})}
	go func() {
		defer close(reader.done)
		defer close(reader.messages)
		scanner := bufio.NewScanner(body)
		var data []byte
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				data = append(data[:0], strings.TrimPrefix(line, "data: ")...)
				continue
			}
			if line == "" && len(data) != 0 {
				reader.messages <- append([]byte(nil), data...)
				data = data[:0]
			}
		}
	}()
	return reader
}

func (r *sseReader) close() {
	_ = r.body.Close()
}

func (r *sseReader) next(t *testing.T) []byte {
	t.Helper()
	select {
	case message := <-r.messages:
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for SSE notification")
		return nil
	}
}

func (r *sseReader) noMessage(t *testing.T) {
	t.Helper()
	select {
	case message := <-r.messages:
		t.Fatalf("unexpected SSE notification: %s", message)
	case <-time.After(100 * time.Millisecond):
	}
}

func (r *sseReader) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(time.Second):
		t.Fatal("SSE stream did not close")
	}
}

func TestMCPNotificationStreamsAreAuthenticatedAndSessionScoped(t *testing.T) {
	fixture := newNotificationFixture(t)
	client := fixture.server.Client()
	endpoint := fixture.server.URL + mcp.Path
	sessionA := initializeHTTPNotificationSession(t, client, endpoint, "alpha")
	sessionB := initializeHTTPNotificationSession(t, client, endpoint, "alpha")

	streamAResponse := notificationRequest(t, context.Background(), client, http.MethodGet, endpoint, "alpha", sessionA, nil)
	if streamAResponse.StatusCode != http.StatusOK || streamAResponse.Header.Get("Content-Type") != "text/event-stream" || streamAResponse.Header.Get("MCP-Protocol-Version") != "2025-03-26" {
		data, _ := io.ReadAll(streamAResponse.Body)
		t.Fatalf("stream A status=%d headers=%v body=%s", streamAResponse.StatusCode, streamAResponse.Header, data)
	}
	streamA := newSSEReader(streamAResponse.Body)
	streamBResponse := notificationRequest(t, context.Background(), client, http.MethodGet, endpoint, "alpha", sessionB, nil)
	if streamBResponse.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(streamBResponse.Body)
		t.Fatalf("stream B status=%d body=%s", streamBResponse.StatusCode, data)
	}
	streamB := newSSEReader(streamBResponse.Body)
	defer streamA.close()
	defer streamB.close()

	fixture.allow.Store(false)
	ping := `{"jsonrpc":"2.0","id":2,"method":"ping"}`
	response := notificationRequest(t, context.Background(), client, http.MethodPost, endpoint, "alpha", sessionA, strings.NewReader(ping))
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !json.Valid(data) {
		t.Fatalf("finite ping status=%d body=%s err=%v", response.StatusCode, data, err)
	}
	if got := streamA.next(t); string(got) != `{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}` {
		t.Fatalf("unexpected session A notification: %s", got)
	}
	streamB.noMessage(t)

	// A missing or foreign principal/session cannot attach to the existing
	// stream, and the normal protocol header remains on each refusal.
	missing := notificationRequest(t, context.Background(), client, http.MethodGet, endpoint, "alpha", "", nil)
	missingBody, _ := io.ReadAll(missing.Body)
	missing.Body.Close()
	if missing.StatusCode != http.StatusBadRequest || missing.Header.Get("MCP-Protocol-Version") != "2025-03-26" || !strings.Contains(string(missingBody), `"code":"invalid_request"`) {
		t.Fatalf("missing session status=%d headers=%v body=%s", missing.StatusCode, missing.Header, missingBody)
	}
	foreign := notificationRequest(t, context.Background(), client, http.MethodGet, endpoint, "beta", sessionA, nil)
	foreignBody, _ := io.ReadAll(foreign.Body)
	foreign.Body.Close()
	if foreign.StatusCode != http.StatusNotFound || !strings.Contains(string(foreignBody), `"code":"not_found"`) {
		t.Fatalf("foreign principal status=%d body=%s", foreign.StatusCode, foreignBody)
	}

	// Cancellation releases the first stream; reconnecting with the same
	// authenticated session receives a later hash change without replaying the
	// earlier notification.
	streamA.close()
	streamA.waitClosed(t)
	streamAResponse = notificationRequest(t, context.Background(), client, http.MethodGet, endpoint, "alpha", sessionA, nil)
	if streamAResponse.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(streamAResponse.Body)
		t.Fatalf("reconnect status=%d body=%s", streamAResponse.StatusCode, data)
	}
	streamA = newSSEReader(streamAResponse.Body)
	fixture.allow.Store(true)
	response = notificationRequest(t, context.Background(), client, http.MethodPost, endpoint, "alpha", sessionA, strings.NewReader(ping))
	_, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if got := streamA.next(t); string(got) != `{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}` {
		t.Fatalf("unexpected reconnected notification: %s", got)
	}
	_ = fixture.handler.Close()
	streamA.waitClosed(t)
	streamB.waitClosed(t)
}

func TestMCPNotificationStreamRequiresEventStreamAccept(t *testing.T) {
	fixture := newNotificationFixture(t)
	client := fixture.server.Client()
	endpoint := fixture.server.URL + mcp.Path
	session := initializeHTTPNotificationSession(t, client, endpoint, "alpha")
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Authorization", "Bearer alpha")
	request.Header.Set("MCP-Session-Id", session)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusNotAcceptable || !strings.Contains(string(body), `"code":"invalid_request"`) {
		t.Fatalf("missing Accept status=%d body=%s", response.StatusCode, body)
	}
}

func TestMCPNotificationStreamCancellationClosesRequest(t *testing.T) {
	fixture := newNotificationFixture(t)
	client := fixture.server.Client()
	endpoint := fixture.server.URL + mcp.Path
	session := initializeHTTPNotificationSession(t, client, endpoint, "alpha")
	ctx, cancel := context.WithCancel(context.Background())
	response := notificationRequest(t, ctx, client, http.MethodGet, endpoint, "alpha", session, nil)
	reader := newSSEReader(response.Body)
	cancel()
	reader.waitClosed(t)
}

func TestMCPNotificationFiniteResponseStillWorksWithoutStream(t *testing.T) {
	fixture := newNotificationFixture(t)
	client := fixture.server.Client()
	endpoint := fixture.server.URL + mcp.Path
	session := initializeHTTPNotificationSession(t, client, endpoint, "alpha")
	fixture.allow.Store(false)
	response := notificationRequest(t, context.Background(), client, http.MethodPost, endpoint, "alpha", session, strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"ping"}`))
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("finite SSE status=%d type=%q body=%s err=%v", response.StatusCode, response.Header.Get("Content-Type"), body, err)
	}
	if !bytes.Contains(body, []byte(`"method":"notifications/tools/list_changed"`)) || !bytes.Contains(body, []byte(`"id":2`)) {
		t.Fatalf("finite response omitted notification or ping: %s", body)
	}
}

func TestMCPNotificationStreamClosesOnHandlerClose(t *testing.T) {
	fixture := newNotificationFixture(t)
	client := fixture.server.Client()
	endpoint := fixture.server.URL + mcp.Path
	session := initializeHTTPNotificationSession(t, client, endpoint, "alpha")
	response := notificationRequest(t, context.Background(), client, http.MethodGet, endpoint, "alpha", session, nil)
	reader := newSSEReader(response.Body)
	if err := fixture.handler.Close(); err != nil {
		t.Fatal(err)
	}
	reader.waitClosed(t)
	if err := fixture.handler.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMCPNotificationStreamRejectsWrongProtocolVersion(t *testing.T) {
	fixture := newNotificationFixture(t)
	client := fixture.server.Client()
	endpoint := fixture.server.URL + mcp.Path
	session := initializeHTTPNotificationSession(t, client, endpoint, "alpha")
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Authorization", "Bearer alpha")
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("MCP-Session-Id", session)
	request.Header.Set("MCP-Protocol-Version", "2099-01-01")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), `"code":"invalid_request"`) {
		t.Fatalf("wrong protocol status=%d body=%s", response.StatusCode, body)
	}
}

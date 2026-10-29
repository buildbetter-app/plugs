package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

func TestHTTPPostsUnchangedBytesWithCanonicalHeaders(t *testing.T) {
	request := []byte(`{"jsonrpc":"2.0","id":9007199254740993,"method":"server.hello"}`)
	response := []byte(`{"jsonrpc":"2.0","id":9007199254740993,"result":{"unchanged":true}}`)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.RequestURI != "/rpc/v1" || !bytes.Equal(body, request) {
			t.Error("request method, exact route or bytes changed")
		}
		if r.Header.Get("Plug-RPC") != "1" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json" || r.Header.Get("Authorization") != "Bearer test-credential" {
			t.Error("missing canonical RPC/media/bearer headers")
		}
		if r.Header.Get("Cookie") != "" || r.Header.Get("Origin") != "" || r.Header.Get("Idempotency-Key") != "" || r.Header.Get("Accept-Encoding") != "" {
			t.Error("unexpected cookies, browser origin, HTTP retry key or compression")
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write(response)
	}))
	defer server.Close()
	for _, path := range []string{"", "/", "/rpc/v1"} {
		transport, err := NewHTTP(HTTPConfig{Server: server.URL + path, Token: "test-credential"})
		if err != nil {
			t.Fatal(err)
		}
		got, err := transport.RoundTrip(context.Background(), request)
		transport.Close()
		if err != nil || !bytes.Equal(got, response) {
			t.Fatalf("HTTP changed response bytes: %s %v", got, err)
		}
	}
	if calls.Load() != 3 {
		t.Fatal("unexpected transport retries")
	}
}

func TestHTTPRejectsUnsafeConfigurationWithoutEchoingCredentials(t *testing.T) {
	for _, server := range []string{
		"", "127.0.0.1:1234", "ftp://localhost", "http://example.invalid", "http://127.0.0.1.evil.invalid",
		"https://user:private-credential@example.invalid", "https://example.invalid/?token=private-credential",
		"https://example.invalid/?", "https://example.invalid/#private-credential", "https://example.invalid/#",
		"https://example.invalid/rpc/v1/", "https://example.invalid/rpc/%761", "https://example.invalid/other",
		"https://example.invalid:", "https://example.invalid:0", "https://example.invalid:65536",
		"https://[::1%25zone]", " https://example.invalid", "https://example.invalid\\other",
	} {
		if _, err := NewHTTP(HTTPConfig{Server: server, Token: "private-credential"}); err == nil || strings.Contains(err.Error(), "private-credential") {
			t.Fatalf("unsafe server configuration accepted or exposed: %q", server)
		}
	}
	for _, cfg := range []HTTPConfig{
		{Server: "http://localhost", Token: "private-credential\r\nHeader: value"},
		{Server: "http://localhost", Token: "private-credential,other"},
		{Server: "http://localhost", Token: strings.Repeat("x", 4097)},
		{Server: "http://localhost", Timeout: -1},
		{Server: "http://localhost", MaxResponseBytes: -1},
	} {
		if _, err := NewHTTP(cfg); err == nil || strings.Contains(err.Error(), "private-credential") {
			t.Fatal("invalid HTTP configuration accepted or exposed")
		}
	}
}

func TestHTTPNeverFollowsRedirectsOrRetriesWrites(t *testing.T) {
	var targetCalls, sourceCalls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		t.Error("redirect target received a request")
	}))
	defer target.Close()
	for _, status := range []int{301, 302, 303, 307, 308} {
		for _, sameOrigin := range []bool{false, true} {
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sourceCalls.Add(1)
				if r.URL.Path != "/rpc/v1" {
					t.Error("same-origin redirect was followed")
					return
				}
				location := target.URL + "/private-credential"
				if sameOrigin {
					location = "/private-credential"
				}
				http.Redirect(w, r, location, status)
			}))
			transport, err := NewHTTP(HTTPConfig{Server: source.URL, Token: "private-credential"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = transport.RoundTrip(context.Background(), []byte(`{"method":"bead.create"}`))
			transport.Close()
			source.Close()
			if err == nil || strings.Contains(err.Error(), "private-credential") {
				t.Fatal("redirect succeeded or exposed Location/credential")
			}
		}
	}
	if sourceCalls.Load() != 10 || targetCalls.Load() != 0 {
		t.Fatalf("automatic follow/retry: source=%d target=%d", sourceCalls.Load(), targetCalls.Load())
	}
}

func TestHTTPResponseBoundsMediaAndSafeFailures(t *testing.T) {
	for _, mode := range []string{"large", "chunked-large", "truncated", "media", "encoding", "headers", "status", "reflected-error"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch mode {
				case "large":
					w.Header().Set("Content-Length", "1024")
				case "chunked-large":
					w.(http.Flusher).Flush()
				case "truncated":
					w.Header().Set("Content-Length", "1024")
					_, _ = w.Write([]byte("short"))
					return
				case "media":
					w.Header().Set("Content-Type", "text/plain")
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				case "headers":
					w.Header().Set("X-Huge", strings.Repeat("x", 64<<10))
				case "status":
					w.WriteHeader(http.StatusBadGateway)
				case "reflected-error":
					_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"plug-1","error":{"code":-32001,"message":"private-credential"}}`))
					return
				}
				_, _ = io.WriteString(w, strings.Repeat("private-credential", 80))
			}))
			defer server.Close()
			limit := int64(128)
			if mode == "truncated" || mode == "reflected-error" {
				limit = 2048
			}
			transport, err := NewHTTP(HTTPConfig{Server: server.URL, Token: "private-credential", MaxResponseBytes: limit})
			if err != nil {
				t.Fatal(err)
			}
			defer transport.Close()
			_, err = transport.RoundTrip(context.Background(), []byte(`{}`))
			if err == nil || strings.Contains(err.Error(), "private-credential") {
				t.Fatal("unsafe response accepted or body/credential exposed")
			}
		})
	}
}

func TestHTTPCancellationAndTimeoutInterruptResponseRead(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "timeout"}[timeout], func(t *testing.T) {
			started, canceled := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.(http.Flusher).Flush()
				close(started)
				<-r.Context().Done()
				close(canceled)
			}))
			defer server.Close()
			budget := 5 * time.Second
			if timeout {
				budget = 100 * time.Millisecond
			}
			transport, err := NewHTTP(HTTPConfig{Server: server.URL, Timeout: budget})
			if err != nil {
				t.Fatal(err)
			}
			defer transport.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := transport.RoundTrip(ctx, []byte(`{}`)); done <- err }()
			<-started
			want := context.DeadlineExceeded
			if !timeout {
				want = context.Canceled
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("cancellation identity lost: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP read ignored cancellation/deadline")
			}
			select {
			case <-canceled:
			case <-time.After(2 * time.Second):
				t.Fatal("server did not observe disconnected request")
			}
		})
	}
}

func TestHTTPCanonicalAdmissionErrorsKeepTypedCatalogData(t *testing.T) {
	cat, _ := wire.DefaultCatalog()
	for _, code := range []string{"unauthenticated", "token_expired", "authn_unavailable", "forbidden", "rate_limited"} {
		t.Run(code, func(t *testing.T) {
			remote := cat.NewError(code, "safe guidance", nil)
			remote.Data.TraceID = "test-trace"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": nil, "error": remote})
			}))
			defer server.Close()
			transport, err := NewHTTP(HTTPConfig{Server: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			defer transport.Close()
			c, _ := New(transport, "local", "test")
			_, err = c.Call(context.Background(), "bead.ready", nil, "")
			var got *wire.Error
			if !errors.As(err, &got) || got.Data.Code != code || got.Data.Retryable != remote.Data.Retryable || got.Data.TraceID != remote.Data.TraceID || got.Data.Hint != remote.Data.Hint {
				t.Fatalf("admission error lost catalog identity: %v", err)
			}
		})
	}
}

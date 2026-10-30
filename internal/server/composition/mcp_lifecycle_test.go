package composition

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type mcpReadGate struct {
	entered, canceled, released chan struct{}
	err                         error
}

// Keep the real SQLite snapshot open until canonical write preflight observes
// cancellation. The ordinary writer must close it on every exit path.
type mcpLifecycleStore struct {
	driver.Store
	gate    atomic.Pointer[mcpReadGate]
	readers atomic.Int64
}

func (s *mcpLifecycleStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	s.readers.Add(1)
	return &mcpLifecycleReader{Reader: r, owner: s}, nil
}

type mcpLifecycleReader struct {
	driver.Reader
	owner *mcpLifecycleStore
	gate  *mcpReadGate
	once  sync.Once
}

func (r *mcpLifecycleReader) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	if gate := r.owner.gate.Swap(nil); gate != nil {
		r.gate = gate
		close(gate.entered)
		<-ctx.Done()
		gate.err = ctx.Err()
		close(gate.canceled)
		return nil, ctx.Err()
	}
	return r.Reader.IdempotencyByKey(ctx, principal, agent, method, key)
}

func (r *mcpLifecycleReader) Close() error {
	err := r.Reader.Close()
	r.once.Do(func() {
		r.owner.readers.Add(-1)
		if r.gate != nil {
			close(r.gate.released)
		}
	})
	return err
}

func awaitMCPWork(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("composed MCP work did not release its resources")
	}
}

func TestComposedMCPCancellationAndCloseReleaseCanonicalWork(t *testing.T) {
	for _, mode := range []string{"notification", "client", "close"} {
		t.Run(mode, func(t *testing.T) {
			var observed *mcpLifecycleStore
			config := Config{
				Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "mcp-cancel.db"),
				Project: "mcp-cancel", Address: DefaultAddress,
				StoreFactory: func(ctx context.Context, _ string, config driver.Config) (driver.Store, error) {
					store, err := (sqlite.Driver{}).Open(ctx, config)
					if err != nil {
						return nil, err
					}
					observed = &mcpLifecycleStore{Store: store}
					return observed, nil
				},
			}
			f := startRuntime(t, config)
			status, headers, body := mcpRequest(t, f, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"lifecycle","version":"1"}}}`, nil)
			if status != http.StatusOK || headers.Get("MCP-Session-Id") == "" {
				t.Fatalf("composed MCP initialization failed: status=%d body=%s", status, body)
			}
			session := headers.Get("MCP-Session-Id")
			before, err := f.r.Store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			gate := &mcpReadGate{entered: make(chan struct{}), canceled: make(chan struct{}), released: make(chan struct{})}
			observed.gate.Store(gate)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+f.server.Addr().String()+"/mcp", bytes.NewBufferString(`{"jsonrpc":"2.0","id":"active","method":"tools/call","params":{"name":"bead_create","arguments":{"type":"issue","fields":{"title":"must not commit"}}}}`))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Plug-RPC", "1")
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("MCP-Session-Id", session)
			type response struct {
				status int
				body   []byte
				err    error
			}
			finished := make(chan response, 1)
			go func() {
				r, err := http.DefaultClient.Do(request)
				if err != nil {
					finished <- response{err: err}
					return
				}
				defer r.Body.Close()
				body, err := io.ReadAll(r.Body)
				finished <- response{r.StatusCode, body, err}
			}()
			awaitMCPWork(t, gate.entered)
			switch mode {
			case "notification":
				status, _, body := mcpRequest(t, f, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"active"}}`, map[string]string{"MCP-Session-Id": session})
				if status != http.StatusAccepted || len(body) != 0 {
					t.Fatalf("composed cancellation status=%d body=%s", status, body)
				}
			case "client":
				cancel()
			case "close":
				stop, stopCancel := context.WithCancel(context.Background())
				stopCancel()
				if err := f.r.Close(stop); !errors.Is(err, context.Canceled) {
					t.Fatalf("composed forced close err=%v", err)
				}
				f.transport.Close()
				f.r = nil // Close's expected missed-drain error is already checked.
				select {
				case err := <-f.done:
					if !errors.Is(err, http.ErrServerClosed) {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("composed listener survived Close")
				}
			}
			awaitMCPWork(t, gate.canceled)
			awaitMCPWork(t, gate.released)
			if !errors.Is(gate.err, context.Canceled) {
				t.Fatal("canonical work reached its deadline before cancellation")
			}
			select {
			case reply := <-finished:
				if mode == "notification" {
					if reply.err != nil || reply.status != http.StatusOK {
						t.Fatalf("canceled MCP write did not preserve its canonical refusal: status=%d err=%v body=%s", reply.status, reply.err, reply.body)
					}
					tool, _ := mcpObject(t, reply.body)["result"].(map[string]any)
					if tool["isError"] != true {
						t.Fatal("canceled MCP write reported a successful tool result")
					}
				} else if reply.err == nil {
					t.Fatal("canceled connection reported success")
				}
			case <-time.After(time.Second):
				t.Fatal("canceled MCP request retained work")
			}
			if observed.readers.Load() != 0 {
				t.Fatal("canceled canonical work retained a SQLite snapshot")
			}
			if f.r != nil {
				after, err := f.r.Store.Meta(context.Background())
				if err != nil || after.Head != before.Head {
					t.Fatal("canceled MCP work committed a write")
				}
				status, _, body := mcpRequest(t, f, `{"jsonrpc":"2.0","id":"active","method":"tools/call","params":{"name":"bead_ready","arguments":{"limit":1}}}`, map[string]string{"MCP-Session-Id": session})
				if status != http.StatusOK || mcpObject(t, body)["error"] != nil {
					t.Fatal("MCP cancellation retained the active request slot")
				}
				if tool, _ := mcpObject(t, body)["result"].(map[string]any); tool == nil || tool["isError"] == true {
					t.Fatal("MCP cancellation prevented a later canonical call")
				}
				f.stop(t)
			}
			if _, err := observed.Store.Meta(context.Background()); err == nil {
				t.Fatal("Runtime.Close retained its store")
			}
			config.StoreFactory = nil
			restarted := startRuntime(t, config)
			after, err := restarted.r.Store.Meta(context.Background())
			if err != nil || after.Head != before.Head {
				t.Fatal("restart exposed a canceled MCP write or retained lease")
			}
			status, _, body = mcpRequest(t, restarted, `{"jsonrpc":"2.0","id":1,"method":"ping","params":{}}`, map[string]string{"MCP-Session-Id": session})
			if status != http.StatusNotFound || mcpErrorCode(t, body) != "not_found" {
				t.Fatal("Runtime.Close retained MCP session authority")
			}
		})
	}
}

// Model only BUILD-36's closable-handler seam. All canonical MCP behavior in
// the tests above still runs through the existing adapter and real SQLite.
type closableMCPFixture struct {
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
	closes  atomic.Int64
}

func (h *closableMCPFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_ = http.NewResponseController(w).Flush()
	close(h.entered)
	select {
	case <-h.closed:
	case <-r.Context().Done():
	}
}

func (h *closableMCPFixture) Close() error {
	h.closes.Add(1)
	h.once.Do(func() { close(h.closed) })
	return nil
}

func TestComposedCloseReleasesMCPHandlerBeforeHTTPDrain(t *testing.T) {
	runtime, err := Open(context.Background(), Config{Driver: "memory", Address: DefaultAddress})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	handler := &closableMCPFixture{entered: make(chan struct{}), closed: make(chan struct{})}
	runtime.mcp = handler
	server, err := runtime.Listen(context.Background(), DefaultAddress, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://" + server.Addr().String() + "/mcp")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	awaitMCPWork(t, handler.entered)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		t.Fatalf("MCP handler held HTTP drain open: %v", err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal("MCP handler did not drain its connected response")
	}
	if err := runtime.Close(ctx); err != nil || handler.closes.Load() != 1 {
		t.Fatal("MCP handler close did not share Runtime's single owner")
	}
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("composed listener survived handler drain")
	}
}

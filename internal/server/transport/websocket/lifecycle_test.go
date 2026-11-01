package websocket_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/websocket"
)

func TestHandlerCloseCancelsAndJoinsDispatchBeforeReturning(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	f := newFixture(t, websocket.Config{}, func(registry *rpc.Registry) {
		if err := registry.Register("bead.get", func(ctx context.Context, _ rpc.Call) (any, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil, ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	connection := f.connect(t, f.http.URL)
	defer connection.close()
	if err := connection.write(1, []byte(`{"jsonrpc":"2.0","id":1,"method":"bead.get","params":{"id":{"id":"pl-ws"}}}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not start")
	}
	done := make(chan error, 1)
	go func() { done <- f.handler.Close() }()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel dispatch")
	}
	select {
	case <-done:
		t.Fatal("Close returned while dispatch still held its resources")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not join dispatch")
	}
	// Closing is idempotent, and no new upgrade reaches authentication/RPC.
	if err := f.handler.Close(); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	f.handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, f.http.URL+websocket.Path, nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed handler status=%d", response.Code)
	}
}

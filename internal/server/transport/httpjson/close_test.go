package httpjson

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// pausedCloseListener keeps active sockets open at the first listener-close
// boundary, making a response racing with cancellation observable without
// depending on scheduler timing.
type pausedCloseListener struct {
	net.Listener
	closing chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (l *pausedCloseListener) Close() error {
	l.once.Do(func() {
		close(l.closing)
		<-l.resume
	})
	return l.Listener.Close()
}

func TestCloseClosesConnectionsBeforeCancelingHandlers(t *testing.T) {
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	rpcServer, err := rpc.New(registry, rpc.Config{Incarnation: "close-test", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(rpcServer, authenticator, Config{})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan context.Context, 1)
	handlerDone := make(chan struct{})
	server, err := handler.ListenHandler(context.Background(), "127.0.0.1:0", nil, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- r.Context()
		<-r.Context().Done()
		// Use the actual admitted RPC handler to attempt its cancellation
		// refusal. Forced close must prevent these response bytes reaching the
		// peer even when the handler cooperates immediately.
		handler.ServeHTTP(w, r)
		close(handlerDone)
	}))
	if err != nil {
		t.Fatal(err)
	}
	listener := &pausedCloseListener{Listener: server.listener, closing: make(chan struct{}), resume: make(chan struct{})}
	server.listener = listener
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(listener.resume) }) }
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve() }()
	t.Cleanup(func() {
		release()
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-serveDone:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("Serve error=%v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("forced close retained Serve")
		}
	})
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	request, err := http.NewRequest(http.MethodPost, "http://"+server.Addr().String()+Path, strings.NewReader(`{"jsonrpc":"2.0","id":"close","method":"server.hello","params":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Plug-RPC", "1")
	requestDone := make(chan error, 1)
	go func() {
		response, err := client.Do(request)
		if err == nil {
			_, err = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
		requestDone <- err
	}()
	var requestContext context.Context
	select {
	case requestContext = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not enter the admitted server")
	}
	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	select {
	case <-listener.closing:
	case <-time.After(3 * time.Second):
		t.Fatal("forced close did not reach listener closure")
	}
	if err := requestContext.Err(); err != nil {
		t.Fatal("forced close canceled an active handler while its response connection remained open")
	}
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("forced close retained connections")
	}
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("forced close did not cancel handler work")
	}
	select {
	case err := <-requestDone:
		if err == nil {
			t.Fatal("forced close delivered the canceled RPC response")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("forced close retained the HTTP client")
	}
}

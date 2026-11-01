package transports_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

// This decorator keeps the real memory driver and core read path. Its only
// controlled delay makes the ordering of connection drain and store close
// observable, including after the request has already been canceled.
type drainingStore struct {
	driver.Store
	readers   atomic.Int64
	block     atomic.Bool
	early     atomic.Bool
	entered   chan struct{}
	canceled  chan struct{}
	release   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (s *drainingStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	reader, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	s.readers.Add(1)
	return &drainingReader{Reader: reader, owner: s}, nil
}

func (s *drainingStore) Close() error {
	s.closeOnce.Do(func() {
		s.early.Store(s.readers.Load() != 0)
		s.closeErr = s.Store.Close()
		close(s.closed)
	})
	return s.closeErr
}

type drainingReader struct {
	driver.Reader
	owner *drainingStore
	once  sync.Once
	err   error
}

func (r *drainingReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	if r.owner.block.Load() {
		close(r.owner.entered)
		<-ctx.Done()
		close(r.owner.canceled)
		<-r.owner.release
		return nil, ctx.Err()
	}
	return r.Reader.Get(ctx, refs)
}

func (r *drainingReader) Close() error {
	r.once.Do(func() {
		r.err = r.Reader.Close()
		r.owner.readers.Add(-1)
	})
	return r.err
}

func productionFixture(t *testing.T, authenticated bool) (*fixture, *composition.Runtime, *drainingStore) {
	t.Helper()
	store := &drainingStore{Store: memory.New(), entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	config := composition.Config{Driver: "memory", Project: "i3", StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil }}
	f := &fixture{principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}}
	if authenticated {
		f.principal = model.PrincipalRef{Kind: "user", ID: "user:i3-composition"}
		f.keys = authnapikey.New(authnapikey.Config{})
		key, err := f.keys.Issue(f.principal, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		f.token, f.keyID = key.Token, key.ID
		config.AuthMode = "authenticated"
		config.Auth = authn.Config{Provider: f.keys}
	}
	runtime, err := composition.Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	server, err := runtime.Listen(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	t.Cleanup(func() {
		_ = runtime.Close(context.Background())
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("production listener shutdown: %v", err)
		}
	})
	f.rpc, f.auth, f.url = runtime.RPC, runtime.Authenticator, "http://"+server.Addr().String()
	f.client = &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
	t.Cleanup(f.client.CloseIdleConnections)
	f.initializeMCP(t)
	return f, runtime, store
}

func TestProductionListenerMountsWebSocketAndDrainsBeforeStoreClose(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprintf("api_key=%t", authenticated), func(t *testing.T) {
			f, runtime, store := productionFixture(t, authenticated)
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(store.release) }) })
			create := request(t, "production", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "production WebSocket route"}, "_meta": map[string]any{"idempotencyKey": "550e8400-e29b-41d4-a716-446655440000"}})
			created := success(t, f.call(t, "websocket", create))
			var commit struct {
				Results []struct{ Ref json.RawMessage }
			}
			if err := json.Unmarshal(created, &commit); err != nil || len(commit.Results) != 1 {
				t.Fatalf("WebSocket create: %s (%v)", created, err)
			}
			get := request(t, "read", "bead.get", map[string]any{"id": commit.Results[0].Ref})
			var want json.RawMessage
			for _, transport := range transports {
				got := success(t, f.call(t, transport, get))
				if want != nil && !bytes.Equal(want, got) {
					t.Fatalf("production %s changed canonical read bytes", transport)
				}
				want = got
			}
			connection, _, status := f.connect(t, func(r *http.Request) { r.Header.Set("Origin", "http://attacker.invalid") })
			connection.Close()
			if status != http.StatusForbidden {
				t.Fatalf("production WebSocket hostile origin: %d", status)
			}

			connection, _, status = f.connect(t, nil)
			defer connection.Close()
			if status != http.StatusSwitchingProtocols {
				t.Fatalf("production upgrade: %d", status)
			}
			store.block.Store(true)
			writeFrame(t, connection, get)
			select {
			case <-store.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("core read did not acquire its store reader")
			}
			closed := make(chan error, 1)
			go func() { closed <- runtime.Close(context.Background()) }()
			select {
			case <-store.canceled:
			case <-time.After(2 * time.Second):
				t.Fatal("runtime close did not cancel the upgraded request")
			}
			select {
			case <-store.closed:
				t.Fatal("runtime closed the store before the WebSocket reader returned")
			default:
			}
			releaseOnce.Do(func() { close(store.release) })
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("runtime close did not join the WebSocket handler")
			}
			if store.early.Load() || store.readers.Load() != 0 {
				t.Fatal("store closed with active WebSocket readers")
			}
		})
	}
}

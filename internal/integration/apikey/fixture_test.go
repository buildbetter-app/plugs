package apikey_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/apikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type observedStore struct {
	*memory.Store
	reads atomic.Int64
}

func (s *observedStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	s.reads.Add(1)
	return s.Store.Read(ctx, view)
}

type fixture struct {
	store  *observedStore
	keys   *authnapikey.Provider
	clock  atomic.Int64
	cancel context.CancelFunc
	client *http.Client
	url    string
}

func newFixture(t *testing.T, installed func() bool) *fixture {
	t.Helper()
	f := &fixture{store: &observedStore{Store: memory.New()}}
	t.Cleanup(func() { _ = f.store.Close() })
	lease, err := f.store.Lease(context.Background(), "apikey-integration", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	f.clock.Store(time.Now().UnixNano())
	f.keys = authnapikey.New(authnapikey.Config{
		Now:                    func() time.Time { return time.Unix(0, f.clock.Load()) },
		AuthorizationInstalled: installed,
	})
	handler, err := apikey.New(context.Background(), apikey.Config{
		Store: f.store, Lease: lease, Keys: f.keys, DefaultProject: "apikey-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	t.Cleanup(cancel)
	server, err := handler.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.url = "http://" + server.Addr().String()
	f.client = &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	t.Cleanup(func() {
		f.client.CloseIdleConnections()
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("API-key HTTP server did not stop")
		}
	})
	return f
}

func (f *fixture) issue(t *testing.T, principal model.PrincipalRef) authnapikey.IssuedKey {
	t.Helper()
	issued, err := f.keys.Issue(principal, time.Unix(0, f.clock.Load()).Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return issued
}

type httpReply struct {
	status int
	header http.Header
	body   []byte
	value  map[string]any
}

func (f *fixture) call(t *testing.T, token, method string, params map[string]any, change func(*http.Request)) httpReply {
	t.Helper()
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 7, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequest(http.MethodPost, f.url+httpjson.Path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Plug-RPC", "1")
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if change != nil {
		change(r)
	}
	response, err := f.client.Do(r)
	if err != nil {
		t.Fatal("API-key HTTP round trip failed")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal("API-key HTTP response read failed")
	}
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatal("API-key HTTP response was not JSON")
	}
	return httpReply{status: response.StatusCode, header: response.Header, body: body, value: value}
}

func success(t *testing.T, reply httpReply) map[string]any {
	t.Helper()
	result, ok := reply.value["result"].(map[string]any)
	if reply.status != http.StatusOK || reply.value["error"] != nil || !ok {
		t.Fatalf("API-key call did not return a successful result (HTTP %d)", reply.status)
	}
	return result
}

func code(t *testing.T, reply httpReply, status int, want string) {
	t.Helper()
	err, ok := reply.value["error"].(map[string]any)
	if reply.status != status || !ok || reply.value["result"] != nil {
		t.Fatalf("API-key refusal did not return an error (HTTP %d)", reply.status)
	}
	data, ok := err["data"].(map[string]any)
	if !ok || data["code"] != want {
		t.Fatalf("API-key refusal did not return %s", want)
	}
}

func writeParams(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	params["_meta"] = map[string]any{"idempotencyKey": hex.EncodeToString(id[:]), "agent": "unverified-label"}
	return params
}

func (f *fixture) metadata(t *testing.T) driver.Meta {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

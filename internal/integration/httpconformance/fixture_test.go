package httpconformance_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
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

const (
	project = "http-conformance"

	principalKind = "service"
	principalID   = "service:http-conformance"
)

// observedStore wraps the real memory driver only to make the no-write proof
// independent of implementation details: every Writer transaction is counted
// at the storage boundary, while reads and changesets remain real driver calls.
type observedStore struct {
	*memory.Store
	writes atomic.Int64
}

func (s *observedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes.Add(1)
	return s.Store.Write(ctx, lease)
}

type fixture struct {
	store    *observedStore
	lease    driver.ActiveLease
	keys     *authnapikey.Provider
	token    string
	server   *httpjson.Server
	client   *http.Client
	endpoint string
	done     chan error
}

type reply struct {
	status int
	body   []byte
	header http.Header
}

type durableState struct {
	head       model.Revision
	changesets int
	writes     int64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	store := &observedStore{Store: memory.New()}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error("close memory store:", err)
		}
	})
	lease, err := store.Lease(ctx, "http-conformance", driver.LeaseTTL)
	if err != nil {
		t.Fatal("acquire memory lease:", err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error("release memory lease:", err)
		}
	})
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal("read memory metadata:", err)
	}

	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal("create RPC registry:", err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal("bind real service:", err)
	}

	now := time.Date(2026, 10, 29, 15, 0, 0, 0, time.UTC)
	keys := authnapikey.New(authnapikey.Config{Now: func() time.Time { return now }})
	issued, err := keys.Issue(model.PrincipalRef{Kind: principalKind, ID: principalID}, now.Add(time.Hour))
	if err != nil {
		t.Fatal("issue fixture credential:", err)
	}
	authenticator, err := authn.New(authn.Config{Provider: keys})
	if err != nil {
		t.Fatal("create authenticator:", err)
	}
	rpcServer, err := rpc.New(registry, rpc.Config{
		Incarnation:      meta.Incarnation,
		DefaultProject:   project,
		AuthMode:         "authenticated",
		OntologyRevision: model.Natural64(1),
	})
	if err != nil {
		t.Fatal("create RPC server:", err)
	}
	handler, err := httpjson.New(rpcServer, authenticator, httpjson.Config{})
	if err != nil {
		t.Fatal("create HTTP handler:", err)
	}
	server, err := handler.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal("listen admitted HTTP server:", err)
	}
	f := &fixture{
		store:    store,
		lease:    lease,
		keys:     keys,
		token:    issued.Token,
		server:   server,
		client:   &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}},
		endpoint: "http://" + server.Addr().String() + httpjson.Path,
		done:     make(chan error, 1),
	}
	go func() { f.done <- server.Serve() }()
	t.Cleanup(func() {
		f.client.CloseIdleConnections()
		if err := server.Close(); err != nil {
			t.Error("close HTTP server:", err)
		}
		select {
		case err := <-f.done:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Error("HTTP server:", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("HTTP server did not stop")
		}
	})
	return f
}

func (f *fixture) post(t *testing.T, body string) reply {
	return f.postWithToken(t, body, f.token)
}

func (f *fixture) postWithToken(t *testing.T, body, token string) reply {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, f.endpoint, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal("build HTTP request:", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := f.client.Do(req)
	if err != nil {
		t.Fatal("HTTP round trip:", err)
	}
	defer response.Body.Close()
	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal("read HTTP response:", err)
	}
	return reply{status: response.StatusCode, body: bodyBytes, header: response.Header.Clone()}
}

func (f *fixture) state(t *testing.T) durableState {
	t.Helper()
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal("read durable metadata:", err)
	}
	reader, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal("open durable snapshot:", err)
	}
	defer reader.Close()
	changesets, err := reader.Changesets(context.Background(), nil, 1000)
	if err != nil {
		t.Fatal("read durable changesets:", err)
	}
	return durableState{head: meta.Head, changesets: len(changesets), writes: f.store.writes.Load()}
}

func (f *fixture) assertNoMutation(t *testing.T, before durableState) {
	t.Helper()
	after := f.state(t)
	if after != before {
		t.Fatalf("refused call mutated durable state: before=%+v after=%+v", before, after)
	}
}

func decode(t *testing.T, body []byte) any {
	t.Helper()
	value, err := wire.DecodeJSON(body)
	if err != nil {
		t.Fatalf("response is not strict JSON: %v (%s)", err, body)
	}
	return value
}

func object(t *testing.T, body []byte) map[string]any {
	t.Helper()
	value := decode(t, body)
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("response is not an object: %s", body)
	}
	return result
}

func resultObject(t *testing.T, body []byte) map[string]any {
	t.Helper()
	envelope := object(t, body)
	if envelope["error"] != nil {
		t.Fatalf("RPC call returned an error: %s", body)
	}
	result, ok := envelope["result"].(map[string]any)
	if !ok {
		t.Fatalf("RPC result is not an object: %s", body)
	}
	return result
}

func errorCode(t *testing.T, body []byte) string {
	t.Helper()
	envelope := object(t, body)
	errObject, ok := envelope["error"].(map[string]any)
	if !ok {
		t.Fatalf("response has no RPC error: %s", body)
	}
	data, ok := errObject["data"].(map[string]any)
	if !ok {
		t.Fatalf("RPC error has no data: %s", body)
	}
	code, ok := data["code"].(string)
	if !ok {
		t.Fatalf("RPC error has no string code: %s", body)
	}
	return code
}

func mustResultRefID(t *testing.T, body []byte) string {
	t.Helper()
	result := resultObject(t, body)
	rows, ok := result["results"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("create result has no single result row: %s", body)
	}
	row, ok := rows[0].(map[string]any)
	if !ok {
		t.Fatalf("create result row is not an object: %s", body)
	}
	ref, ok := row["ref"].(map[string]any)
	if !ok {
		t.Fatalf("create result row has no ref: %s", body)
	}
	id, ok := ref["id"].(string)
	if !ok || id == "" {
		t.Fatalf("create result ref has no id: %s", body)
	}
	return id
}

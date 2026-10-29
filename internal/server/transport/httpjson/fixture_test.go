package httpjson_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type observedStore struct {
	*memory.Store
	reads, writes atomic.Int64
	readers       atomic.Int64
}

func (s *observedStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	s.reads.Add(1)
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	s.readers.Add(1)
	return &observedReader{Reader: r, owner: s}, nil
}
func (s *observedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes.Add(1)
	return s.Store.Write(ctx, lease)
}

type observedReader struct {
	driver.Reader
	owner *observedStore
	once  sync.Once
}

func (r *observedReader) Close() error {
	err := r.Reader.Close()
	r.once.Do(func() { r.owner.readers.Add(-1) })
	return err
}

type observedService struct {
	rpc.Service
	creates atomic.Int64
}

func (s *observedService) CreateWithOutcome(ctx context.Context, p model.PrincipalRef, req service.CreateRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	s.creates.Add(1)
	return s.Service.CreateWithOutcome(ctx, p, req, meta)
}

type warningPost struct{ calls atomic.Int64 }

func (p *warningPost) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls.Add(1)
	return errors.New("fixture post subscriber unavailable")
}

type fixture struct {
	rpc       *rpc.Server
	auth      *authn.Authenticator
	provider  *authnapikey.Provider
	token     string
	keyID     string
	keyClock  atomic.Int64
	principal model.PrincipalRef
	store     *observedStore
	domain    *observedService
	lease     driver.ActiveLease
	post      *warningPost
	server    *httpjson.Server
	client    *http.Client
	url       string
	baseCtx   context.Context
	cancel    context.CancelFunc
}

func newFixture(t *testing.T, authenticated, warnings bool, cfg httpjson.Config, extra ...pipeline.Option) *fixture {
	t.Helper()
	f := &fixture{}
	f.baseCtx, f.cancel = context.WithCancel(context.Background())
	t.Cleanup(f.cancel)
	store := memory.New()
	f.store = &observedStore{Store: store}
	t.Cleanup(func() {
		if f.store.readers.Load() != 0 {
			t.Error("HTTP work retained store readers")
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	var err error
	f.lease, err = store.Lease(context.Background(), "http-fixture", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	opts := []pipeline.Option{pipeline.WithOntology(ontology)}
	f.post = &warningPost{}
	if warnings {
		opts = append(opts, pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "http-warning", Subscriber: f.post}))
	}
	opts = append(opts, extra...)
	writer := pipeline.New(f.store, f.lease, opts...)
	f.domain = &observedService{Service: service.New(f.store, f.lease, writer, service.WithOntology(ontology))}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.domain); err != nil {
		t.Fatal(err)
	}
	authCfg := authn.Config{}
	rpcCfg := rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "http-test", OntologyRevision: model.Natural64(1)}
	f.principal = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	if authenticated {
		f.principal = model.PrincipalRef{Kind: "service", ID: "service:http-client"}
		f.keyClock.Store(time.Now().UnixNano())
		f.provider = authnapikey.New(authnapikey.Config{Now: func() time.Time { return time.Unix(0, f.keyClock.Load()) }})
		issued, err := f.provider.Issue(f.principal, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		f.token, f.keyID = issued.Token, issued.ID
		authCfg.Provider = f.provider
		rpcCfg.AuthMode = "authenticated"
	}
	f.auth, err = authn.New(authCfg)
	if err != nil {
		t.Fatal(err)
	}
	f.rpc, err = rpc.New(registry, rpcCfg)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(f.rpc, f.auth, cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.server, err = handler.Listen(f.baseCtx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.url = "http://" + f.server.Addr().String()
	f.client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
	done := make(chan error, 1)
	go func() { done <- f.server.Serve() }()
	t.Cleanup(func() {
		f.client.CloseIdleConnections()
		if err := f.server.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("HTTP Serve did not exit")
		}
	})
	return f
}

func (f *fixture) request(t *testing.T, data []byte) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodPost, f.url+httpjson.Path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Plug-RPC", "1")
	if f.token != "" {
		r.Header.Set("Authorization", "Bearer "+f.token)
	}
	return r
}

type httpResult struct {
	status int
	header http.Header
	body   []byte
}

func (f *fixture) send(t *testing.T, r *http.Request) httpResult {
	t.Helper()
	response, err := f.client.Do(r)
	if err != nil {
		t.Fatal("HTTP request failed:", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return httpResult{response.StatusCode, response.Header, data}
}

func message(t *testing.T, method string, params any, id json.RawMessage) []byte {
	t.Helper()
	v := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
	if id != nil {
		v["id"] = id
	}
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeParams(t *testing.T, params map[string]any) map[string]any {
	t.Helper()
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	params["_meta"] = map[string]any{"idempotencyKey": hex.EncodeToString(key[:]), "agent": "unverified-client-label"}
	return params
}

func decoded(t *testing.T, data []byte) map[string]any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v map[string]any
	if err := d.Decode(&v); err != nil {
		t.Fatal("invalid JSON response:", err)
	}
	return v
}

func result(t *testing.T, r httpResult) map[string]any {
	t.Helper()
	if r.status != http.StatusOK {
		t.Fatalf("HTTP status %d: %s", r.status, r.body)
	}
	v := decoded(t, r.body)
	if v["error"] != nil {
		t.Fatalf("RPC error: %s", r.body)
	}
	out, ok := v["result"].(map[string]any)
	if !ok {
		t.Fatalf("missing object result: %s", r.body)
	}
	return out
}

func (f *fixture) call(t *testing.T, method string, params any) httpResult {
	t.Helper()
	return f.send(t, f.request(t, message(t, method, params, json.RawMessage(`9007199254740993`))))
}

func createdRef(t *testing.T, commit map[string]any) map[string]any {
	t.Helper()
	rows, ok := commit["results"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatal("missing create result")
	}
	return rows[0].(map[string]any)["ref"].(map[string]any)
}

func requireCode(t *testing.T, r httpResult, status int, code string) {
	t.Helper()
	if r.status != status {
		t.Fatalf("status=%d want=%d: %s", r.status, status, r.body)
	}
	var v struct {
		Error *struct {
			Code int `json:"code"`
			Data struct {
				Code    string `json:"code"`
				TraceID string `json:"traceId"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(r.body, &v); err != nil {
		t.Fatal(err)
	}
	if v.Error == nil || v.Error.Data.Code != code || v.Error.Data.TraceID == "" {
		t.Fatalf("unexpected error envelope: %s", r.body)
	}
}

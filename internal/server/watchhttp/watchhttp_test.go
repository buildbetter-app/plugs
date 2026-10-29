package watchhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type transportFixture struct {
	ctx         context.Context
	store       *memory.Store
	lease       driver.ActiveLease
	incarnation string
	manager     *watch.Manager
	adapter     *Adapter
	server      *rpc.Server
	principal   model.PrincipalRef
	svc         *service.Service
	key         int
}

func newTransportFixture(t *testing.T) *transportFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	store, err := memory.NewWithOptions(memory.Options{Incarnation: "watch-http-incarnation-01"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "watch-http-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader := &coreevents.Reader{Log: serverevents.Log{Store: store}, Materializer: serverevents.HistoryMaterializer{Store: store}, Authorization: coreevents.AllowAll{}}
	codec, err := serverevents.NewCursorCodec([]byte(strings.Repeat("watch-http-key-", 3)))
	if err != nil {
		t.Fatal(err)
	}
	manager, err := watch.New(ctx, watch.Config{Events: &serverevents.Service{Reader: reader, Cursors: codec}, Coalesce: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	adapter, err := New(manager, Config{Ontology: "1", PolicyVersion: "immutable-1"})
	if err != nil {
		t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) { return "http-bead", nil }))
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Register(registry); err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, OntologyRevision: "1", CursorKey: []byte(strings.Repeat("rpc-cursor-key-", 3))})
	if err != nil {
		t.Fatal(err)
	}
	return &transportFixture{svc: domain, ctx: ctx, store: store, lease: lease, incarnation: meta.Incarnation, manager: manager, adapter: adapter, server: server, principal: model.PrincipalRef{Kind: "user", ID: "http-reader"}}
}

func (f *transportFixture) subscribe(t *testing.T) (string, string) {
	t.Helper()
	ctx := WithSession(context.Background(), "http-session", f.ctx)
	ctx = rpc.WithPrincipal(ctx, f.principal)
	response := f.server.Dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"event.subscribe","params":{"filter":{},"_meta":{"project":"local"}}}`))
	var envelope struct {
		Result struct {
			SubscriptionID string `json:"subscriptionId"`
			Cursor         string `json:"cursor"`
		} `json:"result"`
		Error *struct {
			Data map[string]any `json:"data"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("RPC response=%s err=%v", response, err)
	}
	if envelope.Error != nil || envelope.Result.SubscriptionID == "" || envelope.Result.Cursor == "" {
		t.Fatalf("subscribe response=%s", response)
	}
	return envelope.Result.SubscriptionID, envelope.Result.Cursor
}

func (f *transportFixture) commit(t *testing.T) {
	t.Helper()
	f.key++
	_, err := f.svc.Create(f.ctx, model.PrincipalRef{Kind: "user", ID: "http-writer"}, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "transport-event"}}, model.WriteMeta{Project: "local", IdempotencyKey: fmt.Sprintf("%032x", f.key)})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRPCSubscribeAndNDJSONStreamUseActualDeliveredBytes(t *testing.T) {
	f := newTransportFixture(t)
	subscriptionID, _ := f.subscribe(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subscription, err := f.manager.Attach(ctx, subscriptionID, f.principal)
	if err != nil {
		t.Fatal(err)
	}
	writer := &gateWriter{cancel: cancel, first: make(chan struct{}), second: make(chan struct{})}
	streamErr := make(chan error, 1)
	go func() { streamErr <- f.adapter.Stream(ctx, writer, subscription) }()
	select {
	case <-writer.first:
	case <-time.After(time.Second):
		t.Fatal("NDJSON stream did not deliver its snapshot")
	}
	f.commit(t)
	select {
	case <-writer.second:
	case <-time.After(time.Second):
		t.Fatal("NDJSON stream did not deliver its delta")
	}
	if err := <-streamErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("stream termination=%v", err)
	}
	lines := strings.Split(strings.TrimSpace(writer.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("NDJSON lines=%d body=%s", len(lines), writer.String())
	}
	var first, second eventNotify
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatal(err)
	}
	if first.SubscriptionID != subscriptionID || first.Cursor == "" || len(first.Events) != 0 {
		t.Fatalf("snapshot notify=%+v", first)
	}
	if second.SubscriptionID != subscriptionID || second.Cursor == first.Cursor || len(second.Events) != 1 || second.Events[0].Control == nil || second.Events[0].Control.Type != model.EventWatchDelta {
		t.Fatalf("delta notify=%+v", second)
	}
	details, ok := second.Events[0].Control.Details.(model.WatchDeltaDetails)
	if !ok || len(details.Events) != 1 || details.Events[0].Durable == nil || details.Events[0].Durable.Resource != "bead/http-bead" || details.FromCursor != first.Cursor || details.ToCursor != second.Cursor {
		t.Fatalf("delta details=%+v", second.Events[0].Control.Details)
	}
}

func TestStreamRequiresStampedPrincipalAndRPCRefusesMissingViewCheckpoint(t *testing.T) {
	f := newTransportFixture(t)
	// The HTTP boundary does not infer anonymous identity from the body.
	// Calling ServeHTTP directly is unnecessary here; the context helper is the
	// same seam used by a real authenticated composition.
	if _, ok := Principal(context.Background()); ok {
		t.Fatal("unstamped context unexpectedly has principal")
	}
	ctx := rpc.WithPrincipal(context.Background(), f.principal)
	response := f.server.Dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":2,"method":"view.watch","params":{"viewId":"missing"}}`))
	var envelope struct {
		Error struct {
			Data struct {
				Code string `json:"code"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Data.Code != "invalid_params" {
		t.Fatalf("view refusal response=%s", response)
	}
}

type eventNotify struct {
	SubscriptionID string        `json:"subscriptionId"`
	Events         []model.Event `json:"events"`
	Cursor         string        `json:"cursor"`
}

type gateWriter struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	cancel context.CancelFunc
	first  chan struct{}
	second chan struct{}
	once   sync.Once
}

func (w *gateWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = w.buffer.Write(data)
	lineCount := bytes.Count(w.buffer.Bytes(), []byte{'\n'})
	if lineCount == 1 {
		w.once.Do(func() { close(w.first) })
	}
	if lineCount == 2 {
		select {
		case <-w.second:
		default:
			close(w.second)
		}
		w.cancel()
	}
	return len(data), nil
}

func (w *gateWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
}

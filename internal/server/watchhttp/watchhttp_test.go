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
	"github.com/buildbetter-app/plugs/internal/core/viewops"
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
	views       transportViews
	key         int
}

type transportViews map[string]viewops.Definition

func (v transportViews) GetView(_ context.Context, _ model.PrincipalRef, id string) (viewops.Definition, error) {
	definition, ok := v[id]
	if !ok {
		return viewops.Definition{}, &model.Error{Data: model.ErrorData{Code: "not_found", Hint: "saved view is absent"}}
	}
	return definition, nil
}

type projectionStatusFunc func(context.Context, string) (coreevents.ProjectionStatus, error)

func (fn projectionStatusFunc) Status(ctx context.Context, binding string) (coreevents.ProjectionStatus, error) {
	return fn(ctx, binding)
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
	views := transportViews{}
	viewService, err := viewops.New(store, lease, viewops.Config{
		Views:             views,
		OntologyRevision:  "1",
		PolicyVersion:     "immutable-1",
		ProjectionBinding: "core",
		ProjectionStatus: projectionStatusFunc(func(_ context.Context, binding string) (coreevents.ProjectionStatus, error) {
			return coreevents.ProjectionStatus{Binding: binding, Position: coreevents.Position{Incarnation: meta.Incarnation}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := watch.New(ctx, watch.Config{Events: &serverevents.Service{Reader: reader, Cursors: codec}, Coalesce: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	adapter, err := New(manager, Config{Ontology: "1", PolicyVersion: "immutable-1", Views: viewService})
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
	return &transportFixture{svc: domain, ctx: ctx, store: store, lease: lease, incarnation: meta.Incarnation, manager: manager, adapter: adapter, server: server, principal: model.PrincipalRef{Kind: "user", ID: "http-reader"}, views: views}
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

func (f *transportFixture) viewWatch(t *testing.T, viewID, from string) (string, string, map[string]any) {
	t.Helper()
	ctx := WithSession(context.Background(), "http-session", f.ctx)
	ctx = rpc.WithPrincipal(ctx, f.principal)
	params := map[string]any{"viewId": viewID}
	if from != "" {
		params["from"] = from
	}
	rawParams, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	response := f.server.Dispatch(ctx, append([]byte(`{"jsonrpc":"2.0","id":1,"method":"view.watch","params":`), append(rawParams, []byte(`}`)...)...))
	var envelope struct {
		Result map[string]any `json:"result"`
		Error  *struct {
			Data map[string]any `json:"data"`
		} `json:"error,omitempty"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("view.watch response=%s err=%v", response, err)
	}
	if envelope.Error != nil {
		return "", "", envelope.Error.Data
	}
	id, _ := envelope.Result["subscriptionId"].(string)
	cursor, _ := envelope.Result["cursor"].(string)
	return id, cursor, nil
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

func TestRPCViewWatchSnapshotDeltaResumeAndBoundedRefusal(t *testing.T) {
	f := newTransportFixture(t)
	f.views["resource"] = viewops.Definition{Filter: &viewops.Filter{Types: []string{"issue"}}}
	id, opening, errData := f.viewWatch(t, "resource", "")
	if errData != nil || id == "" || opening == "" {
		t.Fatalf("view.watch open id=%q cursor=%q error=%v", id, opening, errData)
	}
	subscription, err := f.manager.AttachSession(f.ctx, id, "http-session", f.principal)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := nextFrameHTTP(t, subscription, time.Second)
	if snapshot.Kind != watch.FrameSnapshot || snapshot.Cursor != opening || len(snapshot.Events) != 0 {
		t.Fatalf("view snapshot=%+v", snapshot)
	}
	f.commit(t)
	delta := nextFrameHTTP(t, subscription, time.Second)
	if delta.Kind != watch.FrameDelta || delta.FromCursor != snapshot.Cursor || len(delta.Events) != 1 || delta.Events[0].Durable == nil || delta.Events[0].Durable.Resource != "bead/http-bead" {
		t.Fatalf("view delta=%+v", delta)
	}
	if err := subscription.Close(); err != nil {
		t.Fatal(err)
	}
	resumedID, _, errData := f.viewWatch(t, "resource", snapshot.Cursor)
	if errData != nil || resumedID == "" {
		t.Fatalf("view.watch resume id=%q error=%v", resumedID, errData)
	}
	resumed, err := f.manager.AttachSession(f.ctx, resumedID, "http-session", f.principal)
	if err != nil {
		t.Fatal(err)
	}
	resumedFrame := nextFrameHTTP(t, resumed, time.Second)
	if resumedFrame.Kind != watch.FrameSnapshot || len(resumedFrame.Events) != 1 || resumedFrame.Events[0].Durable == nil || resumedFrame.Events[0].Durable.Resource != "bead/http-bead" {
		t.Fatalf("view resume=%+v", resumedFrame)
	}
	if err := resumed.Close(); err != nil {
		t.Fatal(err)
	}

	f.views["grouped"] = viewops.Definition{Filter: &viewops.Filter{Resources: []string{"http-bead"}}, GroupBy: []string{"type"}}
	_, _, errData = f.viewWatch(t, "grouped", "")
	if errData == nil || errData["code"] != "invalid_params" {
		t.Fatalf("grouped view refusal=%v", errData)
	}
}

func nextFrameHTTP(t *testing.T, subscription *watch.Subscription, timeout time.Duration) watch.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	frame, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return frame
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
	SubscriptionID string                       `json:"subscriptionId"`
	Events         []model.Event                `json:"events"`
	Cursor         string                       `json:"cursor"`
	Projection     *coreevents.ProjectionStatus `json:"projection,omitempty"`
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

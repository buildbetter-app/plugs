package cardb20

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Harness wiring is retained from the existing S1 B20 public RPC fixture.
// This package extends that fixture across all engines without editing its owner.
const b20CursorKey = "cardb20-same-process-event-key-1"

type eventProvider struct{ events *serverevents.Service }

func (p eventProvider) List(ctx context.Context, req service.EventProviderRequest) (service.EventPage, error) {
	page, err := p.events.List(ctx, serverevents.Request{
		Principal: req.Principal,
		Filter: coreevents.Filter{
			Types:     req.Filter.Types,
			Resources: req.Filter.Resources,
		},
		After: req.After,
		Limit: req.Limit,
	})
	return service.EventPage{Events: page.Events, Cursor: page.Cursor}, err
}

type runtime struct {
	ctx       context.Context
	cancel    context.CancelFunc
	store     driver.Store
	lease     driver.ActiveLease
	server    *rpc.Server
	manager   *watch.Manager
	principal model.PrincipalRef
	requests  int
	once      sync.Once
	closeErr  error
}

func newRuntime(t *testing.T, store driver.Store, live bool, id string) *runtime {
	return newRuntimePolicy(t, store, live, id, false)
}

func newRuntimePolicy(t *testing.T, store driver.Store, live bool, id string, installed bool) *runtime {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	lease, err := store.Lease(ctx, "cardb20-runtime", driver.LeaseTTL)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	issued := false
	ontology := service.NewOntology(nil)
	options := []pipeline.Option{
		pipeline.WithOntology(ontology),
		pipeline.WithIDGenerator(func() (string, error) {
			if issued {
				return "", fmt.Errorf("unexpected second ID request")
			}
			issued = true
			return id, nil
		}),
	}
	authorization, policy, checker := installEventPolicy(t, ctx, store, lease, installed)
	if checker != nil {
		options = append(options, pipeline.WithAuthorizer(checker), pipeline.WithEventContext(checker))
	}
	writer := pipeline.New(store, lease, options...)
	reader := &coreevents.Reader{
		Log:           serverevents.Log{Store: store},
		Materializer:  serverevents.HistoryMaterializer{Store: store},
		Authorization: authorization,
	}
	codec, err := serverevents.NewCursorCodec([]byte(b20CursorKey))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	events := &serverevents.Service{Reader: reader, Cursors: codec}
	domain := service.New(store, lease, writer,
		service.WithOntology(ontology), service.WithAuthorizationInstalled(func() bool { return installed }), service.WithEventProvider(eventProvider{events: events}))
	registry, err := rpc.NewRegistry()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var manager *watch.Manager
	if live {
		manager, err = watch.New(ctx, watch.Config{
			Events: events, Coalesce: time.Millisecond,
			IDGenerator: subscriptionIDs(id),
		})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		adapter, adapterErr := watchhttp.New(manager, watchhttp.Config{
			Ontology: model.Natural64(1), PolicyVersion: policy,
		})
		if adapterErr != nil {
			cancel()
			t.Fatal(adapterErr)
		}
		if err = adapter.Register(registry); err != nil {
			cancel()
			t.Fatal(err)
		}
	}
	if err = rpc.BindService(registry, domain); err != nil {
		cancel()
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{
		DefaultProject: "cardb20", Incarnation: meta.Incarnation,
		OntologyRevision: model.Natural64(1), CursorKey: []byte(b20CursorKey),
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	r := &runtime{ctx: ctx, cancel: cancel, store: store, lease: lease, server: server,
		manager: manager, principal: model.PrincipalRef{Kind: "user", ID: "cardb20-reader"}}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func (r *runtime) Close() error {
	r.once.Do(func() {
		if r.manager != nil {
			r.closeErr = r.manager.Close()
		}
		r.cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		r.closeErr = errors.Join(r.closeErr, r.lease.Release(ctx), r.store.Close())
	})
	return r.closeErr
}

type response struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	} `json:"error,omitempty"`
}

func (r *runtime) call(t *testing.T, session, method string, params map[string]any, write bool) response {
	t.Helper()
	r.requests++
	copy := make(map[string]any, len(params)+1)
	for key, value := range params {
		copy[key] = value
	}
	meta := map[string]any{"project": "cardb20", "agent": "cardb20-recorder"}
	if write {
		meta["idempotencyKey"] = fmt.Sprintf("%032x", r.requests)
	}
	copy["_meta"] = meta
	request, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": r.requests, "method": method, "params": copy,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := rpc.WithPrincipal(r.ctx, r.principal)
	if session != "" {
		ctx = watchhttp.WithSession(ctx, session, r.ctx)
	}
	raw := r.server.Dispatch(ctx, request)
	var envelope response
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("%s response %s: %v", method, raw, err)
	}
	return envelope
}

func success[T any](t *testing.T, method string, envelope response) T {
	t.Helper()
	if envelope.Error != nil {
		t.Fatalf("%s refused with %s", method, envelope.Error.Data.Code)
	}
	var value T
	if err := json.Unmarshal(envelope.Result, &value); err != nil {
		t.Fatalf("decode %s result %s: %v", method, envelope.Result, err)
	}
	return value
}

func nextFrame(t *testing.T, subscription *watch.Subscription) watch.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	frame, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

type subscriptionResult struct {
	SubscriptionID string `json:"subscriptionId"`
	Cursor         string `json:"cursor"`
}

type eventPage struct {
	Events []model.Event `json:"events"`
	Cursor string        `json:"cursor"`
}

func subscriptionIDs(id string) func() (string, error) {
	count := 0
	return func() (string, error) { count++; return fmt.Sprintf("sub-%s-%d", id, count), nil }
}

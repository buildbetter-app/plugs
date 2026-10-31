package collabwatch_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

var (
	alice = model.PrincipalRef{Kind: "user", ID: "user:alice"}
	bob   = model.PrincipalRef{Kind: "user", ID: "user:bob"}
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// serviceBackedPolicy is a deliberately small host policy used only by this
// integration package. Every resource permission check first reads the
// current bead through the real core service; readable then models a current
// grant that can be revoked without changing the durable store.
type serviceBackedPolicy struct {
	service     *service.Service
	presence    atomic.Bool
	readable    atomic.Bool
	serviceGets atomic.Int32
}

func newServiceBackedPolicy() *serviceBackedPolicy {
	policy := &serviceBackedPolicy{}
	policy.presence.Store(true)
	policy.readable.Store(true)
	return policy
}

func (p *serviceBackedPolicy) AdmitPresence(context.Context, model.PrincipalRef, string) (bool, error) {
	return p.presence.Load(), nil
}

func (p *serviceBackedPolicy) current(ctx context.Context, by model.PrincipalRef, bead service.Bead) (bool, error) {
	if p.service == nil {
		return false, errors.New("service-backed policy has no service")
	}
	if _, err := p.service.Get(ctx, by, service.GetRequest{ID: model.RefByID(bead.ID, model.Natural64(0))}); err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, nil
	}
	p.serviceGets.Add(1)
	return p.readable.Load(), nil
}

func (p *serviceBackedPolicy) CanRead(ctx context.Context, by model.PrincipalRef, bead service.Bead) (bool, error) {
	return p.current(ctx, by, bead)
}

func (p *serviceBackedPolicy) Read(ctx context.Context, by model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	id, ok := strings.CutPrefix(event.Resource, "bead/")
	if !ok || id == "" {
		return coreevents.Visibility{}, nil
	}
	allowed, err := p.current(ctx, by, service.Bead{ID: id, Project: "project-a"})
	if err != nil {
		return coreevents.Visibility{}, err
	}
	if !allowed {
		return coreevents.Visibility{}, nil
	}
	return coreevents.Visibility{Readable: true, Before: event.Before, After: event.After}, nil
}

func (p *serviceBackedPolicy) ReadResource(ctx context.Context, by model.PrincipalRef, resource model.AuditResource) (coreevents.ResourceVisibility, error) {
	if resource.Kind != "bead" {
		return coreevents.ResourceVisibility{}, nil
	}
	allowed, err := p.current(ctx, by, service.Bead{ID: resource.Ref.ID, Project: "project-a"})
	return coreevents.ResourceVisibility{Readable: allowed}, err
}

type fixture struct {
	ctx     context.Context
	cancel  context.CancelFunc
	store   *memory.Store
	lease   driver.ActiveLease
	clock   *testClock
	writer  *pipeline.Writer
	svc     *service.Service
	collab  *collab.Manager
	watch   *watch.Manager
	adapter *watchhttp.Adapter
	server  *rpc.Server

	mu     sync.Mutex
	nextID string
	key    int
}

// presencePollGate holds one snapshot poll across session.close and then
// returns the source's expected not_found result. This models the production
// race where the session map is removed before the watch worker observes its
// canceled lifetime.
type presencePollGate struct {
	source      watch.PresenceSource
	calls       atomic.Int32
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func (g *presencePollGate) Snapshot(ctx context.Context, principal model.PrincipalRef, agent, sessionID, project string) ([]byte, error) {
	if g.calls.Add(1) == 2 {
		g.startedOnce.Do(func() { close(g.started) })
		<-g.release
		return nil, &model.Error{Data: model.ErrorData{Code: "not_found"}}
	}
	return g.source.Snapshot(ctx, principal, agent, sessionID, project)
}

func (g *presencePollGate) Release() {
	g.releaseOnce.Do(func() { close(g.release) })
}

func newFixture(t *testing.T) *fixture {
	return newConfiguredFixture(t, nil)
}

func newPolicyFixture(t *testing.T) (*fixture, *serviceBackedPolicy) {
	policy := newServiceBackedPolicy()
	return newConfiguredFixture(t, policy), policy
}

func newConfiguredFixture(t *testing.T, policy *serviceBackedPolicy) *fixture {
	return newConfiguredFixtureWithLog(t, policy, nil)
}

// newConfiguredFixtureWithLog keeps the acceptance harness on the real
// memory/service/RPC path while allowing a test to present one authoritative
// log-head boundary (for example, a takeover incarnation) to the watch
// manager. The wrapper still delegates scans and materialization to the real
// store.
func newConfiguredFixtureWithLog(t *testing.T, policy *serviceBackedPolicy, wrapLog func(coreevents.Log) coreevents.Log) *fixture {
	return newConfiguredFixtureWithLogAndPresence(t, policy, wrapLog, nil)
}

func newConfiguredFixtureWithPresence(t *testing.T, policy *serviceBackedPolicy, wrapPresence func(watch.PresenceSource) watch.PresenceSource) *fixture {
	return newConfiguredFixtureWithLogAndPresence(t, policy, nil, wrapPresence)
}

func newConfiguredFixtureWithLogAndPresence(t *testing.T, policy *serviceBackedPolicy, wrapLog func(coreevents.Log) coreevents.Log, wrapPresence func(watch.PresenceSource) watch.PresenceSource) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	f := &fixture{ctx: ctx, cancel: cancel, store: memory.New(), clock: &testClock{t: time.Date(2026, 10, 29, 15, 0, 0, 0, time.UTC)}}
	t.Cleanup(cancel)
	t.Cleanup(func() { _ = f.store.Close() })

	var err error
	f.lease, err = f.store.Lease(ctx, "collabwatch-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.lease.Release(context.Background()) })

	ontology := service.NewOntology(nil)
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(ontology),
		pipeline.WithClock(f.clock.now),
		pipeline.WithIDGenerator(func() (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.nextID == "" {
				return "", errors.New("test ID was not selected")
			}
			return f.nextID, nil
		}),
	)
	f.svc = service.New(f.store, f.lease, f.writer, service.WithOntology(ontology), service.WithClock(f.clock.now))
	if policy != nil {
		policy.service = f.svc
	}
	var readPolicy collab.ReadPolicy
	if policy != nil {
		readPolicy = policy
	}
	f.collab, err = collab.New(ctx, collab.Config{
		Service:                f.svc,
		Lease:                  f.lease,
		Clock:                  f.clock.now,
		AuthorizationInstalled: func() bool { return policy != nil },
		Policy:                 readPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.collab.Close() })

	meta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var eventLog coreevents.Log = serverevents.Log{Store: f.store}
	if wrapLog != nil {
		eventLog = wrapLog(eventLog)
	}
	reader := &coreevents.Reader{
		Log:           eventLog,
		Materializer:  serverevents.HistoryMaterializer{Store: f.store},
		Authorization: coreevents.AllowAll{},
	}
	if policy != nil {
		reader.Authorization = policy
	}
	codec, err := serverevents.NewCursorCodec([]byte(strings.Repeat("collabwatch-cursor-key-", 2)))
	if err != nil {
		t.Fatal(err)
	}
	events := &serverevents.Service{Reader: reader, Cursors: codec}
	presence := watch.PresenceSource(f.collab)
	if wrapPresence != nil {
		presence = wrapPresence(presence)
	}
	f.watch, err = watch.New(ctx, watch.Config{Events: events, Presence: presence, Coalesce: 5 * time.Millisecond, MaxLag: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.watch.Close() })
	adapter, err := watchhttp.New(f.watch, watchhttp.Config{Ontology: model.Natural64(1), PolicyVersion: "policy-1"})
	if err != nil {
		t.Fatal(err)
	}
	f.adapter = adapter
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.svc); err != nil {
		t.Fatal(err)
	}
	if err := collab.Bind(registry, f.collab); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Register(registry); err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{
		Incarnation:      meta.Incarnation,
		DefaultProject:   "project-a",
		OntologyRevision: model.Natural64(1),
		CursorKey:        []byte(strings.Repeat("collabwatch-rpc-key-", 2)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// presenceHTTPFixture is a disposable trusted-host composition around the
// existing RPC dispatcher and watch NDJSON adapter. The handler injects the
// principal/session lifetime as a host would; request metadata never supplies
// either authority.
type presenceHTTPFixture struct {
	principal model.PrincipalRef
	sessionID string
	lifetime  context.Context
	server    *httptest.Server
	client    *http.Client
}

func newPresenceHTTPFixture(t *testing.T, f *fixture, principal model.PrincipalRef, sessionID string) *presenceHTTPFixture {
	t.Helper()
	h := &presenceHTTPFixture{principal: principal, sessionID: sessionID}
	if sessionID != "" {
		lifetime, err := f.collab.SessionContext(f.ctx, collab.Caller{Principal: principal}, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		h.lifetime = lifetime
	}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := rpc.WithPrincipal(r.Context(), h.principal)
		ctx = watchhttp.WithPrincipal(ctx, h.principal)
		if h.sessionID != "" && h.lifetime != nil {
			ctx = watchhttp.WithSession(ctx, h.sessionID, h.lifetime)
		}
		switch r.URL.Path {
		case "/rpc/v1":
			if r.Method != http.MethodPost {
				w.Header().Set("Allow", http.MethodPost)
				http.Error(w, "use POST", http.StatusMethodNotAllowed)
				return
			}
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
			if err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			defer r.Body.Close()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(f.server.Dispatch(ctx, body))
		case watchhttp.DefaultPath:
			f.adapter.ServeHTTP(w, r.WithContext(ctx))
		default:
			http.NotFound(w, r)
		}
	}))
	h.client = h.server.Client()
	t.Cleanup(func() { h.close() })
	return h
}

func (h *presenceHTTPFixture) close() {
	if h == nil {
		return
	}
	if h.client != nil {
		h.client.CloseIdleConnections()
	}
	if h.server != nil {
		h.server.Close()
	}
}

func (h *presenceHTTPFixture) call(t *testing.T, method string, params map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, h.server.URL+"/rpc/v1", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := h.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(response.Body)
		t.Fatalf("HTTP RPC %s status=%d body=%s", method, response.StatusCode, raw)
	}
	var value map[string]any
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatalf("decode HTTP RPC %s: %v", method, err)
	}
	return value
}

type presenceHTTPNotify struct {
	SubscriptionID string        `json:"subscriptionId"`
	Events         []model.Event `json:"events"`
	Cursor         string        `json:"cursor"`
}

type presenceHTTPStream struct {
	response *http.Response
	cancel   context.CancelFunc
	decoder  *json.Decoder
}

func (h *presenceHTTPFixture) openPresenceWatch(t *testing.T, from string) (*presenceHTTPStream, presenceHTTPNotify) {
	t.Helper()
	params := map[string]any{
		"sessionId": h.sessionID,
		"_meta":     map[string]any{"project": "project-a"},
	}
	if from != "" {
		params["from"] = from
	}
	response := h.call(t, "presence.watch", params)
	opened := result(t, response)
	subscriptionID, ok := opened["subscriptionId"].(string)
	if !ok || subscriptionID == "" {
		t.Fatalf("presence.watch response=%v", response)
	}
	cursor, ok := opened["cursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("presence.watch cursor=%v", opened)
	}
	streamContext, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(streamContext, http.MethodPost, h.server.URL+watchhttp.DefaultPath, strings.NewReader(fmt.Sprintf(`{"subscriptionId":%q}`, subscriptionID)))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	streamResponse, err := h.client.Do(request)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if streamResponse.StatusCode != http.StatusOK || streamResponse.Header.Get("Content-Type") != "application/x-ndjson" {
		body, _ := io.ReadAll(streamResponse.Body)
		_ = streamResponse.Body.Close()
		t.Fatalf("presence stream status=%d content-type=%q body=%s", streamResponse.StatusCode, streamResponse.Header.Get("Content-Type"), body)
	}
	stream := &presenceHTTPStream{response: streamResponse, cancel: cancel, decoder: json.NewDecoder(bufio.NewReader(streamResponse.Body))}
	initial := stream.next(t)
	if initial.SubscriptionID != subscriptionID || initial.Cursor != cursor {
		t.Fatalf("presence HTTP snapshot=%+v opened=%v", initial, opened)
	}
	return stream, initial
}

func (s *presenceHTTPStream) next(t *testing.T) presenceHTTPNotify {
	t.Helper()
	type decoded struct {
		value presenceHTTPNotify
		err   error
	}
	resultCh := make(chan decoded, 1)
	go func() {
		var value presenceHTTPNotify
		resultCh <- decoded{value: value, err: s.decoder.Decode(&value)}
	}()
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("decode presence HTTP stream: %v", result.err)
		}
		return result.value
	case <-time.After(3 * time.Second):
		s.close()
		t.Fatal("timed out waiting for presence HTTP stream frame")
		return presenceHTTPNotify{}
	}
}

func (s *presenceHTTPStream) waitEOF(t *testing.T) {
	t.Helper()
	type decoded struct{ err error }
	resultCh := make(chan decoded, 1)
	go func() {
		var value presenceHTTPNotify
		resultCh <- decoded{err: s.decoder.Decode(&value)}
	}()
	select {
	case result := <-resultCh:
		if !errors.Is(result.err, io.EOF) {
			t.Fatalf("presence HTTP stream ended with %v", result.err)
		}
	case <-time.After(3 * time.Second):
		s.close()
		t.Fatal("timed out waiting for presence HTTP stream cancellation")
	}
}

func (s *presenceHTTPStream) close() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.response != nil {
		_ = s.response.Body.Close()
	}
}

func assertPresenceCursor(t *testing.T, principal model.PrincipalRef, token string) coreevents.Position {
	t.Helper()
	codec, err := serverevents.NewCursorCodec([]byte(strings.Repeat("collabwatch-cursor-key-", 2)))
	if err != nil {
		t.Fatal(err)
	}
	binding, err := serverevents.BindingFor("presence.watch", coreevents.Filter{Resources: []string{"presence/project-a"}}, principal, model.Natural64(1), "policy-1")
	if err != nil {
		t.Fatal(err)
	}
	position, err := codec.Decode(token, binding)
	if err != nil {
		t.Fatalf("presence cursor is not signed/bound: %v", err)
	}
	return position
}

func (f *fixture) call(t *testing.T, principal model.PrincipalRef, method string, params map[string]any) map[string]any {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	ctx := rpc.WithPrincipal(f.ctx, principal)
	reply, err := inprocess.New(f.server).RoundTrip(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	value, err := wire.DecodeJSON(reply)
	if err != nil {
		t.Fatalf("decode %s: %v (%s)", method, err, reply)
	}
	response, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s response is not an object: %v", method, value)
	}
	return response
}

func (f *fixture) session(t *testing.T, principal model.PrincipalRef) string {
	t.Helper()
	response := f.call(t, principal, "session.open", map[string]any{"_meta": map[string]any{"project": "project-a"}})
	return result(t, response)["sessionId"].(string)
}

func (f *fixture) watchContext(t *testing.T, principal model.PrincipalRef, sessionID string) context.Context {
	t.Helper()
	lifetime, err := f.collab.SessionContext(f.ctx, collab.Caller{Principal: principal}, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := rpc.WithPrincipal(f.ctx, principal)
	return watchhttp.WithSession(ctx, sessionID, lifetime)
}

func (f *fixture) subscribe(t *testing.T, principal model.PrincipalRef, sessionID, from string) *watch.Subscription {
	t.Helper()
	ctx := f.watchContext(t, principal, sessionID)
	params := map[string]any{"filter": map[string]any{}, "_meta": map[string]any{"project": "project-a"}}
	if from != "" {
		params["from"] = from
	}
	response := f.callContext(t, ctx, principal, "event.subscribe", params)
	id := result(t, response)["subscriptionId"].(string)
	subscription, err := f.watch.AttachSession(f.ctx, id, sessionID, principal)
	if err != nil {
		t.Fatal(err)
	}
	return subscription
}

func (f *fixture) subscribeResponse(t *testing.T, principal model.PrincipalRef, sessionID string, from string) map[string]any {
	t.Helper()
	ctx := f.watchContext(t, principal, sessionID)
	params := map[string]any{"filter": map[string]any{}, "_meta": map[string]any{"project": "project-a"}}
	if from != "" {
		params["from"] = from
	}
	return f.callContext(t, ctx, principal, "event.subscribe", params)
}

func (f *fixture) presenceWatch(t *testing.T, principal model.PrincipalRef, sessionID, from string) *watch.Subscription {
	t.Helper()
	ctx := f.watchContext(t, principal, sessionID)
	params := map[string]any{"sessionId": sessionID, "_meta": map[string]any{"project": "project-a"}}
	if from != "" {
		params["from"] = from
	}
	response := f.callContext(t, ctx, principal, "presence.watch", params)
	id := result(t, response)["subscriptionId"].(string)
	subscription, err := f.watch.AttachSession(f.ctx, id, sessionID, principal)
	if err != nil {
		t.Fatal(err)
	}
	return subscription
}

func (f *fixture) presenceWatchResponse(t *testing.T, principal model.PrincipalRef, sessionID, from string) map[string]any {
	t.Helper()
	ctx := f.watchContext(t, principal, sessionID)
	params := map[string]any{"sessionId": sessionID, "_meta": map[string]any{"project": "project-a"}}
	if from != "" {
		params["from"] = from
	}
	return f.callContext(t, ctx, principal, "presence.watch", params)
}

func (f *fixture) callContext(t *testing.T, ctx context.Context, principal model.PrincipalRef, method string, params map[string]any) map[string]any {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	ctx = rpc.WithPrincipal(ctx, principal)
	reply, err := inprocess.New(f.server).RoundTrip(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	value, err := wire.DecodeJSON(reply)
	if err != nil {
		t.Fatalf("decode %s: %v (%s)", method, err, reply)
	}
	response, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s response is not an object: %v", method, value)
	}
	return response
}

func result(t *testing.T, response map[string]any) map[string]any {
	t.Helper()
	value, ok := response["result"].(map[string]any)
	if !ok {
		t.Fatalf("RPC failure: %v", response)
	}
	return value
}

func errorCode(t *testing.T, response map[string]any) string {
	t.Helper()
	errorValue, ok := response["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected RPC error: %v", response)
	}
	data, _ := errorValue["data"].(map[string]any)
	code, _ := data["code"].(string)
	return code
}

func (f *fixture) writeKey() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.key++
	return fmt.Sprintf("%032x", f.key)
}

func (f *fixture) create(t *testing.T, id string) {
	t.Helper()
	f.mu.Lock()
	f.nextID = id
	f.mu.Unlock()
	response := f.call(t, alice, "bead.create", map[string]any{
		"type":   "issue",
		"fields": map[string]any{"title": "collaboration watch fixture"},
		"_meta":  map[string]any{"project": "project-a", "idempotencyKey": f.writeKey()},
	})
	created := result(t, response)["results"].([]any)
	if len(created) != 1 {
		t.Fatalf("create results=%v", created)
	}
	ref := created[0].(map[string]any)["ref"].(map[string]any)
	if ref["id"] != id {
		t.Fatalf("created ID=%v want %s", ref["id"], id)
	}
}

func (f *fixture) update(t *testing.T, id, title string) {
	t.Helper()
	response := f.call(t, alice, "bead.update", map[string]any{
		"id":     map[string]any{"id": id},
		"fields": map[string]any{"title": title},
		"_meta":  map[string]any{"project": "project-a", "idempotencyKey": f.writeKey()},
	})
	_ = result(t, response)
}

func (f *fixture) claim(t *testing.T, id string) {
	t.Helper()
	response := f.call(t, alice, "bead.claim", map[string]any{
		"id":    map[string]any{"id": id},
		"_meta": map[string]any{"project": "project-a", "idempotencyKey": f.writeKey()},
	})
	_ = result(t, response)
}

func (f *fixture) deleteBead(t *testing.T, id string) {
	t.Helper()
	_, err := f.writer.Apply(f.ctx, alice, model.ChangeSet{Ops: []model.Op{{
		Kind:   model.OpDelete,
		Delete: &model.DeleteOp{Target: model.RefByID(id, model.Natural64(0))},
	}}}, model.WriteMeta{Method: "bead.delete", Project: "project-a", IdempotencyKey: f.writeKey()})
	if err != nil {
		t.Fatal(err)
	}
}

func presenceRows(t *testing.T, response map[string]any) []any {
	t.Helper()
	return result(t, response)["presence"].([]any)
}

func nextFrame(t *testing.T, sub *watch.Subscription) watch.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	frame, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func nextUntilResources(t *testing.T, sub *watch.Subscription, want ...string) (string, map[string]bool) {
	t.Helper()
	seen := map[string]bool{}
	wanted := map[string]bool{}
	for _, resource := range want {
		wanted[resource] = true
	}
	var cursor string
	for len(seen) < len(wanted) {
		frame := nextFrame(t, sub)
		cursor = frame.Cursor
		for _, event := range frame.Events {
			if event.Durable != nil && wanted[event.Durable.Resource] {
				seen[event.Durable.Resource] = true
			}
		}
	}
	return cursor, seen
}

func durableState(t *testing.T, f *fixture) []byte {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Query(f.ctx, driver.Query{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	changesets, err := reader.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal([]any{meta, rows, changesets})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPresenceWatchCloseDuringInFlightSnapshotReturnsClosed(t *testing.T) {
	gate := &presencePollGate{started: make(chan struct{}), release: make(chan struct{})}
	defer gate.Release()
	f := newConfiguredFixtureWithPresence(t, nil, func(source watch.PresenceSource) watch.PresenceSource {
		gate.source = source
		return gate
	})
	sessionID := f.session(t, alice)
	subscription := f.presenceWatch(t, alice, sessionID, "")
	_ = nextFrame(t, subscription)

	select {
	case <-gate.started:
	case <-time.After(3 * time.Second):
		t.Fatal("presence watch did not reach the in-flight snapshot gate")
	}
	if _, err := f.callSessionClose(t, alice, sessionID); err != nil {
		t.Fatal(err)
	}
	gate.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := subscription.Next(ctx); !errors.Is(err, watch.ErrSubscriptionClosed) {
		t.Fatalf("session close during presence poll returned %v", err)
	}
	waitForWatchCount(t, f, sessionID, 0)
}

func TestPresenceAndWatchShareRPCSessionLifetimeAndPreserveDurableState(t *testing.T) {
	f := newFixture(t)
	sessionID := f.session(t, alice)
	subscription := f.subscribe(t, alice, sessionID, "")
	snapshot := nextFrame(t, subscription)
	if snapshot.Kind != watch.FrameSnapshot || snapshot.Cursor == "" || len(snapshot.Events) != 0 {
		t.Fatalf("initial watch snapshot=%+v", snapshot)
	}
	presenceSubscription := f.presenceWatch(t, alice, sessionID, "")
	presenceSnapshot := nextFrame(t, presenceSubscription)
	if presenceSnapshot.Kind != watch.FrameSnapshot || presenceSnapshot.Cursor == "" || len(presenceSnapshot.Events) != 0 {
		t.Fatalf("initial presence snapshot=%+v", presenceSnapshot)
	}

	f.create(t, "presence-watch-bead")
	beforePresence := durableState(t, f)
	set := f.call(t, alice, "presence.set", map[string]any{
		"sessionId": sessionID,
		"focus":     "bead/presence-watch-bead",
		"_meta":     map[string]any{"project": "project-a"},
	})
	setPresence := result(t, set)
	if setPresence["focus"] != "bead/presence-watch-bead" {
		t.Fatalf("presence.set=%v", setPresence)
	}
	rows := presenceRows(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-a"}))
	if len(rows) != 1 {
		t.Fatalf("presence.list=%v", rows)
	}
	if _, seen := nextUntilResources(t, subscription, "bead/presence-watch-bead"); !seen["bead/presence-watch-bead"] {
		t.Fatal("watch did not receive the actual service-created bead")
	}
	presenceDelta := nextFrame(t, presenceSubscription)
	if presenceDelta.Kind != watch.FrameDelta || presenceDelta.Cursor == presenceSnapshot.Cursor || len(presenceDelta.Events) != 0 {
		t.Fatalf("presence delta=%+v", presenceDelta)
	}
	wireEvents := presenceDelta.WireEvents()
	if len(wireEvents) != 1 || wireEvents[0].Control == nil || wireEvents[0].Control.Type != model.EventWatchDelta {
		t.Fatalf("presence delta wire events=%+v", wireEvents)
	}
	details, ok := wireEvents[0].Control.Details.(model.WatchDeltaDetails)
	if !ok || len(details.Events) != 0 || details.FromCursor != presenceSnapshot.Cursor || details.ToCursor != presenceDelta.Cursor {
		t.Fatalf("presence delta details=%+v", wireEvents[0].Control.Details)
	}
	if afterPresence := durableState(t, f); string(beforePresence) != string(afterPresence) {
		t.Fatal("presence lifecycle changed durable rows, history, or head")
	}

	methods := result(t, f.call(t, alice, "rpc.methods", nil))["methods"].([]any)
	foundPresenceWatch := false
	for _, method := range methods {
		if method == "presence.watch" {
			foundPresenceWatch = true
		}
	}
	if !foundPresenceWatch {
		t.Fatal("bound presence.watch was not advertised")
	}

	if _, err := f.callSessionClose(t, alice, sessionID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := subscription.Next(ctx); !errors.Is(err, watch.ErrSubscriptionClosed) {
		t.Fatalf("session close did not revoke watch: %v", err)
	}
	if _, err := presenceSubscription.Next(ctx); !errors.Is(err, watch.ErrSubscriptionClosed) {
		t.Fatalf("session close did not revoke presence watch: %v", err)
	}
	if rows := presenceRows(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-a"})); len(rows) != 0 {
		t.Fatalf("closed session remained visible: %v", rows)
	}
}

func TestPresenceAndWatchUseCurrentServiceBackedAccess(t *testing.T) {
	f, policy := newPolicyFixture(t)
	f.create(t, "service-policy-bead")
	sessionID := f.session(t, alice)
	result(t, f.call(t, alice, "presence.set", map[string]any{
		"sessionId": sessionID,
		"focus":     "bead/service-policy-bead",
		"_meta":     map[string]any{"project": "project-a"},
	}))

	subscription := f.subscribe(t, alice, sessionID, "")
	snapshot := nextFrame(t, subscription)
	if snapshot.Kind != watch.FrameSnapshot || len(snapshot.Events) != 1 || snapshot.Events[0].Durable == nil {
		t.Fatalf("policy-backed snapshot=%+v", snapshot)
	}

	f.update(t, "service-policy-bead", "visible update")
	visible := nextFrame(t, subscription)
	if visible.Kind != watch.FrameDelta || len(visible.Events) != 1 || visible.Events[0].Durable == nil {
		t.Fatalf("policy-backed visible delta=%+v", visible)
	}

	beforeMask := durableState(t, f)
	policy.readable.Store(false)
	rows := presenceRows(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-a"}))
	if len(rows) != 1 {
		t.Fatalf("current-denial presence rows=%v", rows)
	}
	row := rows[0].(map[string]any)
	if _, found := row["focus"]; found {
		t.Fatalf("current service-backed denial leaked focus: %v", row)
	}
	if afterMask := durableState(t, f); string(beforeMask) != string(afterMask) {
		t.Fatal("presence permission read changed durable rows or history")
	}

	f.update(t, "service-policy-bead", "denied update")
	denied := nextFrame(t, subscription)
	if denied.Kind != watch.FrameDelta {
		t.Fatalf("current-denial watch frame=%+v", denied)
	}
	visibilityLost := false
	for _, event := range denied.Events {
		if event.Durable != nil {
			t.Fatalf("denied watch delivered durable event: %+v", denied)
		}
		if event.Control != nil && event.Control.Type == model.EventVisibilityLost {
			visibilityLost = true
		}
	}
	if !visibilityLost {
		t.Fatalf("current service-backed denial did not revoke delivery: %+v", denied)
	}
	if policy.serviceGets.Load() == 0 {
		t.Fatal("permission checks did not read through the actual service")
	}

	if _, err := f.callSessionClose(t, alice, sessionID); err != nil {
		t.Fatal(err)
	}
	if rows := presenceRows(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-a"})); len(rows) != 0 {
		t.Fatalf("closed policy-backed session remained visible: %v", rows)
	}
}

func (f *fixture) callSessionClose(t *testing.T, principal model.PrincipalRef, sessionID string) (map[string]any, error) {
	t.Helper()
	response := f.call(t, principal, "session.close", map[string]any{"sessionId": sessionID})
	if _, ok := response["error"]; ok {
		return response, fmt.Errorf("session.close failed: %v", response)
	}
	return response, nil
}

func TestPresenceListMasksResourcesAfterCurrentServiceAccessChanges(t *testing.T) {
	f := newFixture(t)
	f.create(t, "presence-mask-bead")
	f.claim(t, "presence-mask-bead")
	sessionID := f.session(t, alice)
	_ = result(t, f.call(t, alice, "presence.set", map[string]any{
		"sessionId": sessionID,
		"focus":     "bead/presence-mask-bead",
		"workingOn": []string{"bead/presence-mask-bead"},
		"_meta":     map[string]any{"project": "project-a"},
	}))
	f.deleteBead(t, "presence-mask-bead")
	rows := presenceRows(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-a"}))
	if len(rows) != 1 {
		t.Fatalf("presence rows=%v", rows)
	}
	row := rows[0].(map[string]any)
	if _, found := row["focus"]; found {
		t.Fatalf("deleted focus leaked through current access: %v", row)
	}
	if _, found := row["workingOn"]; found {
		t.Fatalf("deleted claimed resource leaked through current access: %v", row)
	}
}

func TestPresenceExpiryRevokesWatchLifetime(t *testing.T) {
	f := newFixture(t)
	sessionID := f.session(t, alice)
	subscription := f.subscribe(t, alice, sessionID, "")
	_ = nextFrame(t, subscription)
	f.clock.advance(collab.SessionTTL)
	if rows := presenceRows(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-a"})); len(rows) != 0 {
		t.Fatalf("expired session remained visible: %v", rows)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := subscription.Next(ctx); !errors.Is(err, watch.ErrSubscriptionClosed) {
		t.Fatalf("expiry did not revoke watch lifetime: %v", err)
	}
}

func TestPresenceWatchReconnectsAndSharesTheSessionWatchCap(t *testing.T) {
	f := newFixture(t)
	sessionID := f.session(t, alice)
	for i := 0; i < watch.MaxWatchesPerSession/2; i++ {
		if _, ok := f.subscribeResponse(t, alice, sessionID, "")["result"]; !ok {
			t.Fatalf("event watch %d was refused", i)
		}
	}
	for i := 0; i < watch.MaxWatchesPerSession/2; i++ {
		if _, ok := f.presenceWatchResponse(t, alice, sessionID, "")["result"]; !ok {
			t.Fatalf("presence watch %d was refused", i)
		}
	}
	if code := errorCode(t, f.presenceWatchResponse(t, alice, sessionID, "")); code != "too_many_sessions" {
		t.Fatalf("mixed watch cap code=%s", code)
	}
	if _, err := f.callSessionClose(t, alice, sessionID); err != nil {
		t.Fatal(err)
	}

	f2 := newFixture(t)
	firstSession := f2.session(t, alice)
	subscription := f2.presenceWatch(t, alice, firstSession, "")
	initial := nextFrame(t, subscription)
	if initial.Kind != watch.FrameSnapshot || initial.Cursor == "" {
		t.Fatalf("presence initial snapshot=%+v", initial)
	}
	f2.create(t, "presence-reconnect-bead")
	_ = result(t, f2.call(t, alice, "presence.set", map[string]any{
		"sessionId": firstSession,
		"focus":     "bead/presence-reconnect-bead",
		"_meta":     map[string]any{"project": "project-a"},
	}))
	changed := nextFrame(t, subscription)
	if changed.Kind != watch.FrameDelta || changed.Cursor == initial.Cursor || len(changed.Events) != 0 {
		t.Fatalf("presence changed delta=%+v", changed)
	}
	if _, err := f2.callSessionClose(t, alice, firstSession); err != nil {
		t.Fatal(err)
	}

	secondSession := f2.session(t, alice)
	reconnected := f2.presenceWatch(t, alice, secondSession, changed.Cursor)
	resume := nextFrame(t, reconnected)
	if resume.Kind != watch.FrameSnapshot || resume.Cursor == changed.Cursor {
		t.Fatalf("presence reconnect did not advance cursor: from=%s frame=%+v", changed.Cursor, resume)
	}
	_ = result(t, f2.call(t, alice, "presence.set", map[string]any{
		"sessionId": secondSession,
		"focus":     "bead/presence-reconnect-bead",
		"_meta":     map[string]any{"project": "project-a"},
	}))
	live := nextFrame(t, reconnected)
	if live.Kind != watch.FrameDelta || live.Cursor == resume.Cursor || len(live.Events) != 0 {
		t.Fatalf("reconnected presence delta=%+v", live)
	}
	wire := live.WireEvents()
	if len(wire) != 1 || wire[0].Control == nil || wire[0].Control.Type != model.EventWatchDelta {
		t.Fatalf("reconnected presence wire delta=%+v", wire)
	}
	details, ok := wire[0].Control.Details.(model.WatchDeltaDetails)
	if !ok || details.FromCursor != resume.Cursor || details.ToCursor != live.Cursor || len(details.Events) != 0 {
		t.Fatalf("reconnected presence delta details=%+v", wire[0].Control.Details)
	}
}

func TestWatchSessionCapAndReconnectFromCursorUseRealRPCBoundary(t *testing.T) {
	f := newFixture(t)
	sessionID := f.session(t, alice)
	for i := 0; i < watch.MaxWatchesPerSession; i++ {
		response := f.subscribeResponse(t, alice, sessionID, "")
		_ = result(t, response)
	}
	if code := errorCode(t, f.subscribeResponse(t, alice, sessionID, "")); code != "too_many_sessions" {
		t.Fatalf("watch cap code=%s", code)
	}

	// A fresh fixture proves cursor resume independently of quota cleanup. The
	// cursor is signed by event.subscribe and is accepted by a new session with
	// the same principal and binding.
	f2 := newFixture(t)
	session1 := f2.session(t, alice)
	sub1 := f2.subscribe(t, alice, session1, "")
	first := nextFrame(t, sub1)
	f2.create(t, "resume-before-close")
	cursor, seen := nextUntilResources(t, sub1, "bead/resume-before-close")
	if !seen["bead/resume-before-close"] || cursor == first.Cursor {
		t.Fatalf("initial watch cursor did not advance: first=%s current=%s", first.Cursor, cursor)
	}
	if _, err := f2.callSessionClose(t, alice, session1); err != nil {
		t.Fatal(err)
	}
	// This event is committed while no watch is alive and must be found by the
	// reconnecting session's from cursor.
	f2.create(t, "resume-after-close")
	session2 := f2.session(t, alice)
	sub2 := f2.subscribe(t, alice, session2, cursor)
	resume := nextFrame(t, sub2)
	if resume.Cursor == cursor {
		t.Fatalf("reconnect did not advance from cursor: %+v", resume)
	}
	foundAfterClose := false
	for _, event := range resume.Events {
		if event.Durable != nil && event.Durable.Resource == "bead/resume-after-close" {
			foundAfterClose = true
		}
	}
	if !foundAfterClose {
		t.Fatalf("reconnect snapshot omitted committed event: %+v", resume)
	}
	f2.create(t, "resume-live")
	_, seen = nextUntilResources(t, sub2, "bead/resume-live")
	if !seen["bead/resume-live"] {
		t.Fatal("reconnected watch did not receive a live delta")
	}
}

func requirePresenceHTTPDelta(t *testing.T, notify presenceHTTPNotify, from string) string {
	t.Helper()
	if notify.Cursor == "" || len(notify.Events) != 1 || notify.Events[0].Control == nil || notify.Events[0].Control.Type != model.EventWatchDelta {
		t.Fatalf("presence HTTP delta=%+v", notify)
	}
	details, ok := notify.Events[0].Control.Details.(model.WatchDeltaDetails)
	if !ok || details.FromCursor != from || details.ToCursor != notify.Cursor || len(details.Events) != 0 {
		t.Fatalf("presence HTTP delta details=%+v", notify.Events[0].Control.Details)
	}
	return notify.Cursor
}

func waitPresenceWatchCount(t *testing.T, manager *watch.Manager, sessionID string, want int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if manager.WatchCount(sessionID) == want {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("presence HTTP watch count=%d want %d", manager.WatchCount(sessionID), want)
		}
	}
}

func TestPresenceWatchHTTPAcceptance(t *testing.T) {
	t.Run("create-mask-reconnect-and-refuse-before-allocation", func(t *testing.T) {
		f, policy := newPolicyFixture(t)
		f.create(t, "http-presence-mask")
		session1 := f.session(t, alice)

		// A request without the trusted session lifetime is refused before the
		// manager allocates a watch or any durable state can change.
		beforeRefusal := durableState(t, f)
		refuser := newPresenceHTTPFixture(t, f, alice, "")
		refused := refuser.call(t, "presence.watch", map[string]any{
			"sessionId": session1,
			"_meta":     map[string]any{"project": "project-a"},
		})
		if code := errorCode(t, refused); code != "invalid_params" {
			t.Fatalf("missing trusted session refusal code=%s response=%v", code, refused)
		}
		if got := f.watch.WatchCount(session1); got != 0 {
			t.Fatalf("refused presence.watch retained %d watches", got)
		}
		if afterRefusal := durableState(t, f); string(beforeRefusal) != string(afterRefusal) {
			t.Fatal("refused presence.watch changed durable state")
		}

		h1 := newPresenceHTTPFixture(t, f, alice, session1)
		stream1, initial := h1.openPresenceWatch(t, "")
		initialPosition := assertPresenceCursor(t, alice, initial.Cursor)
		if initialPosition.Incarnation == "" {
			t.Fatalf("initial presence cursor has no incarnation: %+v", initialPosition)
		}

		beforeSet := durableState(t, f)
		set := h1.call(t, "presence.set", map[string]any{
			"sessionId": session1,
			"focus":     "bead/http-presence-mask",
			"_meta":     map[string]any{"project": "project-a"},
		})
		if got := result(t, set)["focus"]; got != "bead/http-presence-mask" {
			t.Fatalf("HTTP presence.set=%v", set)
		}
		if afterSet := durableState(t, f); string(beforeSet) != string(afterSet) {
			t.Fatal("HTTP presence.set changed durable state")
		}
		visible := presenceRows(t, h1.call(t, "presence.list", map[string]any{"project": "project-a"}))
		if len(visible) != 1 || visible[0].(map[string]any)["focus"] != "bead/http-presence-mask" {
			t.Fatalf("visible HTTP presence=%v", visible)
		}
		setDelta := stream1.next(t)
		setCursor := requirePresenceHTTPDelta(t, setDelta, initial.Cursor)
		_ = assertPresenceCursor(t, alice, setCursor)

		beforeMask := durableState(t, f)
		policy.readable.Store(false)
		bobHTTP := newPresenceHTTPFixture(t, f, bob, "")
		masked := presenceRows(t, bobHTTP.call(t, "presence.list", map[string]any{"project": "project-a"}))
		if len(masked) != 1 {
			t.Fatalf("masked HTTP presence=%v", masked)
		}
		maskedRow := masked[0].(map[string]any)
		if _, found := maskedRow["focus"]; found {
			t.Fatalf("HTTP presence mask leaked focus=%v", maskedRow)
		}
		if afterMask := durableState(t, f); string(beforeMask) != string(afterMask) {
			t.Fatal("HTTP presence masking changed durable state")
		}
		maskDelta := stream1.next(t)
		maskCursor := requirePresenceHTTPDelta(t, maskDelta, setCursor)
		_ = assertPresenceCursor(t, alice, maskCursor)

		closed := h1.call(t, "session.close", map[string]any{"sessionId": session1})
		if _, ok := closed["error"]; ok {
			t.Fatalf("HTTP session.close failed: %v", closed)
		}
		stream1.waitEOF(t)
		waitPresenceWatchCount(t, f.watch, session1, 0)

		policy.readable.Store(true)
		session2 := f.session(t, alice)
		h2 := newPresenceHTTPFixture(t, f, alice, session2)
		stream2, resumed := h2.openPresenceWatch(t, maskCursor)
		resumedPosition := assertPresenceCursor(t, alice, resumed.Cursor)
		if resumed.Cursor == maskCursor || resumedPosition.Incarnation != initialPosition.Incarnation {
			t.Fatalf("HTTP presence reconnect cursor=%+v from=%s", resumed, maskCursor)
		}
		setAgain := h2.call(t, "presence.set", map[string]any{
			"sessionId": session2,
			"focus":     "bead/http-presence-mask",
			"_meta":     map[string]any{"project": "project-a"},
		})
		if got := result(t, setAgain)["focus"]; got != "bead/http-presence-mask" {
			t.Fatalf("HTTP reconnected presence.set=%v", setAgain)
		}
		live := stream2.next(t)
		liveCursor := requirePresenceHTTPDelta(t, live, resumed.Cursor)
		_ = assertPresenceCursor(t, alice, liveCursor)
		stream2.close()
	})

	t.Run("expiry-cancels-http-stream", func(t *testing.T) {
		f := newFixture(t)
		sessionID := f.session(t, alice)
		h := newPresenceHTTPFixture(t, f, alice, sessionID)
		stream, initial := h.openPresenceWatch(t, "")
		if initial.Cursor == "" {
			t.Fatal("expiry fixture returned an empty cursor")
		}
		f.clock.advance(collab.SessionTTL)
		if rows := presenceRows(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-a"})); len(rows) != 0 {
			t.Fatalf("expired HTTP presence remained visible=%v", rows)
		}
		stream.waitEOF(t)
		waitPresenceWatchCount(t, f.watch, sessionID, 0)
	})

	t.Run("incarnation-gap-is-a-terminal-http-control", func(t *testing.T) {
		f1 := newFixture(t)
		f1.create(t, "http-presence-gap")
		session1 := f1.session(t, alice)
		h1 := newPresenceHTTPFixture(t, f1, alice, session1)
		stream1, initial := h1.openPresenceWatch(t, "")
		set := h1.call(t, "presence.set", map[string]any{
			"sessionId": session1,
			"focus":     "bead/http-presence-gap",
			"_meta":     map[string]any{"project": "project-a"},
		})
		if _, ok := set["result"]; !ok {
			t.Fatalf("gap fixture presence.set=%v", set)
		}
		delta := stream1.next(t)
		oldCursor := requirePresenceHTTPDelta(t, delta, initial.Cursor)
		oldPosition := assertPresenceCursor(t, alice, oldCursor)
		stream1.close()
		if _, err := f1.callSessionClose(t, alice, session1); err != nil {
			t.Fatal(err)
		}

		f2 := newFixture(t)
		oldMeta, err := f1.store.Meta(f1.ctx)
		if err != nil {
			t.Fatal(err)
		}
		newMeta, err := f2.store.Meta(f2.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if oldMeta.Incarnation == newMeta.Incarnation || oldPosition.Incarnation != oldMeta.Incarnation {
			t.Fatalf("fixtures did not provide distinct incarnations: old=%+v new=%+v cursor=%+v", oldMeta, newMeta, oldPosition)
		}
		session2 := f2.session(t, alice)
		h2 := newPresenceHTTPFixture(t, f2, alice, session2)
		gapStream, gap := h2.openPresenceWatch(t, oldCursor)
		if len(gap.Events) != 1 || gap.Events[0].Control == nil || gap.Events[0].Control.Type != model.EventGap {
			t.Fatalf("HTTP incarnation gap=%+v", gap)
		}
		details, ok := gap.Events[0].Control.Details.(model.EventGapDetails)
		if !ok || details.FromIncarnation != oldMeta.Incarnation || details.ToIncarnation != newMeta.Incarnation {
			t.Fatalf("HTTP incarnation gap details=%+v old=%s new=%s", gap.Events[0].Control.Details, oldMeta.Incarnation, newMeta.Incarnation)
		}
		if gap.Cursor == oldCursor {
			t.Fatal("incarnation gap did not issue a new signed cursor")
		}
		_ = assertPresenceCursor(t, alice, gap.Cursor)
		gapStream.waitEOF(t)
		waitPresenceWatchCount(t, f2.watch, session2, 0)
	})
}

package epicprogress_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const (
	epics    = "ep6-epics"
	visible  = "ep6-visible"
	hidden   = "ep6-hidden"
	oldEpics = "ep6-old-epics"
	newEpics = "ep6-new-epics"
)

var projects = []string{epics, visible, hidden, oldEpics, newEpics}

func ref(id string) model.Ref             { return model.RefByID(id, model.Natural64(0)) }
func projectRef(project string) model.Ref { return ref("project-" + project) }
func agent(id string) model.PrincipalRef {
	return model.PrincipalRef{Kind: "agent", ID: "agent:" + id, Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:" + id}}
}

type endpoint struct {
	url    string
	client *http.Client
	server *httpjson.Server
	done   chan error
	once   sync.Once
}

func listen(t *testing.T, handler *httpjson.Handler) *endpoint {
	t.Helper()
	s, err := handler.Listen(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	e := &endpoint{url: "http://" + s.Addr().String() + httpjson.Path, server: s,
		client: &http.Client{Timeout: 65 * time.Second, Transport: &http.Transport{Proxy: nil}}, done: make(chan error, 1)}
	go func() { e.done <- s.Serve() }()
	t.Cleanup(func() { e.close(t) })
	return e
}

func (e *endpoint) close(t *testing.T) {
	t.Helper()
	e.once.Do(func() {
		e.client.CloseIdleConnections()
		if err := e.server.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-e.done:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("epic progress HTTP listener did not stop")
		}
	})
}

type reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
	status int
	raw    []byte
}

// Observe only the real service's error before RPC canonicalizes its message.
// The same request, outcome and error pass through unchanged; there is no
// second write, retry, policy hook or substitute authorization decision.
type observedService struct {
	*authzrpc.GuardedService
	t        *testing.T
	sequence atomic.Uint64
}

// Deadline diagnosis only: retain the concrete SQLite store's optional
// capabilities and the actual reader's allocator capability. No read is
// cached, batched, skipped or supplied by the observer; writes stay untouched.
type observedSQLiteStore struct{ *sqlite.Store }

func (s *observedSQLiteStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return r, err
	}
	allocator, ok := r.(driver.AllocatorReader)
	if !ok {
		return r, nil
	}
	if trace, ok := ctx.Value(deadlineTraceKey{}).(*deadlineReadTrace); ok {
		trace.mu.Lock()
		trace.head = r.Snapshot().At.String()
		trace.mu.Unlock()
	}
	return &observedSQLiteReader{Reader: r, AllocatorReader: allocator}, nil
}

func (s *observedSQLiteStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	started := time.Now()
	tx, err := s.Store.Write(ctx, lease)
	recordObservedOperation(ctx, "write_admission", 0, started, err)
	return tx, err
}

type observedSQLiteReader struct {
	driver.Reader
	driver.AllocatorReader
}

type deadlineTraceKey struct{}

type deadlineReadTrace struct {
	mu         sync.Mutex
	passes     []deadlineReadPass
	head       string
	operations map[string]deadlineOperation
}

type deadlineOperation struct {
	calls, rows, errors int
	wall, max           time.Duration
	stack               string
}

func recordObservedOperation(ctx context.Context, name string, rows int, started time.Time, err error) {
	trace, ok := ctx.Value(deadlineTraceKey{}).(*deadlineReadTrace)
	if !ok {
		return
	}
	elapsed := time.Since(started)
	stack := ""
	if elapsed >= 20*time.Millisecond || err != nil {
		var pcs [24]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		var names []string
		for {
			frame, more := frames.Next()
			if strings.Contains(frame.Function, "github.com/buildbetter-app/plugs/internal/") {
				names = append(names, frame.Function)
			}
			if !more {
				break
			}
		}
		stack = strings.Join(names, ";")
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if trace.operations == nil {
		trace.operations = map[string]deadlineOperation{}
	}
	op := trace.operations[name]
	op.calls++
	op.rows += rows
	op.wall += elapsed
	if err != nil {
		op.errors++
	}
	if elapsed > op.max {
		op.max, op.stack = elapsed, stack
	}
	trace.operations[name] = op
}

func (r *observedSQLiteReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	started := time.Now()
	page, err := r.Reader.Query(ctx, query)
	recordObservedOperation(ctx, "query", len(page.Items), started, err)
	return page, err
}

func (r *observedSQLiteReader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	started := time.Now()
	rows, err := r.Reader.DerivedRows(ctx, table, prefix)
	recordObservedOperation(ctx, "derived", len(rows), started, err)
	return rows, err
}

type deadlineReadPass struct {
	started, deadline   time.Time
	firstRead, lastRead time.Time
	getCalls, getRefs   int
	observeCalls, keys  int
	records, misses     int
	sentinels, facts    int
	errors              int
	readWall, maxRead   time.Duration
}

// CallerSource is reached inside each real checker invocation. Keep its
// default binding unchanged and mark only contexts of observed 100-op writes.
// All source calls and authorization decisions are delegated unchanged.
func observedCaller(ctx context.Context, principal model.PrincipalRef) (authz.Caller, error) {
	if trace, ok := ctx.Value(deadlineTraceKey{}).(*deadlineReadTrace); ok {
		trace.mu.Lock()
		deadline, _ := ctx.Deadline()
		trace.passes = append(trace.passes, deadlineReadPass{started: time.Now(), deadline: deadline})
		trace.mu.Unlock()
	}
	return authzrpc.DefaultCallerSource(model.Natural64(1))(ctx, principal)
}

// Reads using the marked checker context or its shorter nested rule context
// belong to that invocation. The enclosing Writer's two-second context and
// all later phases have later deadlines and are deliberately not counted.
func beginObservedRead(ctx context.Context) (*deadlineReadTrace, int, time.Time) {
	trace, ok := ctx.Value(deadlineTraceKey{}).(*deadlineReadTrace)
	if !ok {
		return nil, -1, time.Time{}
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, -1, time.Time{}
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	for i := len(trace.passes) - 1; i >= 0; i-- {
		pass := trace.passes[i]
		if !deadline.After(pass.deadline) && !deadline.Before(pass.started) {
			return trace, i, time.Now()
		}
	}
	return nil, -1, time.Time{}
}

func (r *observedSQLiteReader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	trace, pass, started := beginObservedRead(ctx)
	rows, err := r.Reader.Get(ctx, refs)
	if trace != nil {
		ended := time.Now()
		trace.mu.Lock()
		p := &trace.passes[pass]
		p.getCalls++
		p.getRefs += len(refs)
		p.recordRead(started, ended, err)
		trace.mu.Unlock()
	}
	return rows, err
}

func (r *observedSQLiteReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	trace, pass, started := beginObservedRead(ctx)
	states, err := r.Reader.Observe(ctx, keys)
	if trace != nil {
		ended := time.Now()
		trace.mu.Lock()
		p := &trace.passes[pass]
		p.observeCalls++
		p.keys += len(keys)
		for _, key := range keys {
			switch key.Kind {
			case driver.LockRecord:
				p.records++
			case driver.LockSentinel:
				p.sentinels++
			case driver.LockReadFact:
				p.facts++
			}
		}
		for _, state := range states {
			if state.Key.Kind == driver.LockRecord && !state.Present {
				p.misses++
			}
		}
		p.recordRead(started, ended, err)
		trace.mu.Unlock()
	}
	return states, err
}

func (p *deadlineReadPass) recordRead(started, ended time.Time, err error) {
	if p.firstRead.IsZero() {
		p.firstRead = started
	}
	p.lastRead = ended
	elapsed := ended.Sub(started)
	p.readWall += elapsed
	if elapsed > p.maxRead {
		p.maxRead = elapsed
	}
	if err != nil {
		p.errors++
	}
}

func (trace *deadlineReadTrace) log(t *testing.T, failed bool) {
	trace.mu.Lock()
	passes := append([]deadlineReadPass(nil), trace.passes...)
	head := trace.head
	operations := make(map[string]deadlineOperation, len(trace.operations))
	for name, op := range trace.operations {
		operations[name] = op
	}
	trace.mu.Unlock()
	for name, op := range operations {
		t.Logf("EP6 passive native operation: head=%s operation=%s calls=%d rows=%d errors=%d wall_ms=%.3f max_ms=%.3f stack=%s", head, name, op.calls, op.rows, op.errors, float64(op.wall)/float64(time.Millisecond), float64(op.max)/float64(time.Millisecond), op.stack)
	}
	for i, p := range passes {
		span := time.Duration(0)
		if !p.firstRead.IsZero() {
			span = p.lastRead.Sub(p.firstRead)
		}
		t.Logf("EP6 passive SQLite authz reads: failed=%t pass=%d initial_window_ms=%.3f get_calls=%d get_refs=%d observe_calls=%d keys=%d record_keys=%d record_misses=%d sentinels=%d read_facts=%d errors=%d read_wall_ms=%.3f max_read_ms=%.3f read_span_ms=%.3f", failed, i+1, float64(p.deadline.Sub(p.started))/float64(time.Millisecond), p.getCalls, p.getRefs, p.observeCalls, p.keys, p.records, p.misses, p.sentinels, p.facts, p.errors, float64(p.readWall)/float64(time.Millisecond), float64(p.maxRead)/float64(time.Millisecond), float64(span)/float64(time.Millisecond))
	}
}

var _ rpc.Service = (*observedService)(nil)

func (s *observedService) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	started := time.Now()
	sequence := s.sequence.Add(1)
	var trace *deadlineReadTrace
	if len(cs.Ops) == 100 {
		trace = &deadlineReadTrace{}
		ctx = context.WithValue(ctx, deadlineTraceKey{}, trace)
	}
	var out port.WriteOutcome
	var err error
	pprof.Do(ctx, pprof.Labels("ep6.write", fmt.Sprint(sequence)), func(call context.Context) {
		out, err = s.GuardedService.ApplyWithOutcome(call, by, cs, meta)
	})
	elapsed := time.Since(started)
	if trace != nil && err != nil {
		trace.log(s.t, true)
	}
	var domain *model.Error
	if errors.As(err, &domain) && (domain.Data.Code == "authz_unavailable" || domain.Data.Code == "busy") {
		s.t.Logf("EP6 real service refusal: code=%s message=%q operations=%d elapsed_ms=%.3f write=%d", domain.Data.Code, domain.Message, len(cs.Ops), float64(elapsed)/float64(time.Millisecond), sequence)
	}
	return out, err
}

// call crosses the real loopback listener and authentication provider. Tokens
// remain request-local and are never copied into durable evidence.
func (e *endpoint) call(t *testing.T, token, method string, params map[string]any) reply {
	t.Helper()
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "ep6", "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, e.url, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := e.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		t.Fatal(err)
	}
	var out reply
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("invalid HTTP/RPC reply: %s: %v", raw, err)
	}
	out.status, out.raw = response.StatusCode, raw
	return out
}

func result(t *testing.T, r reply) map[string]any {
	t.Helper()
	if r.status != http.StatusOK || r.Error != nil || len(r.Result) == 0 {
		t.Fatalf("RPC did not succeed: HTTP %d %s", r.status, r.raw)
	}
	var out map[string]any
	d := json.NewDecoder(bytes.NewReader(r.Result))
	d.UseNumber()
	if err := d.Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func requireCode(t *testing.T, r reply, code model.ErrorCode) {
	t.Helper()
	if r.Error == nil || r.Error.Data.Code != code || len(r.Result) != 0 || r.Meta.Replayed {
		t.Fatalf("expected pure %s refusal: HTTP %d %s", code, r.status, r.raw)
	}
}

type fixture struct {
	store     driver.Store
	lease     driver.ActiveLease
	keys      *authnapikey.Provider
	facts     *authzrpc.MutableFacts
	base      authzfixed.Facts
	app       *authzrpc.Composition
	api       *endpoint
	lifecycle *endpoint
	sequence  int
	owner     string
	stopRenew context.CancelFunc
	renewDone chan struct{}
	once      sync.Once
}

func openStore(t *testing.T, backend, path string) (driver.Store, driver.ActiveLease) {
	t.Helper()
	var store driver.Store
	var err error
	if backend == "memory" {
		store = memory.New()
	} else {
		store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
		if err == nil {
			store = &observedSQLiteStore{Store: store.(*sqlite.Store)}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(context.Background(), "ep6-qualification", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	return store, lease
}

func newFixture(t *testing.T, backend, path string, progress bool) *fixture {
	t.Helper()
	return newFixtureMode(t, backend, path, progress, true)
}

func newFixtureMode(t *testing.T, backend, path string, progress, seedOntology bool) *fixture {
	t.Helper()
	store, lease := openStore(t, backend, path)
	f := &fixture{store: store, lease: lease, renewDone: make(chan struct{})}
	t.Cleanup(func() { f.close(t) })
	ctx, stop := context.WithCancel(context.Background())
	f.stopRenew = stop
	go func() {
		defer close(f.renewDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil && ctx.Err() == nil {
					t.Errorf("active lease renewal: %v", err)
					return
				}
			}
		}
	}()
	initial, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: initial.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.Get(ctx, []model.Ref{ref("ontology/ep6")})
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		if !seedOntology {
			t.Fatal("restored store lost its protected ontology; reopen must never repair source state")
		}
		body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
		if err != nil {
			t.Fatal(err)
		}
		// Only the protected ontology fixture uses the storage conformance port.
		// Every bead, parent, move and status is written through the real Writer.
		_, err = conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref("ontology/ep6"), Kind: "ontology", Op: model.OpCreate, Body: body}})
		if err != nil {
			t.Fatal(err)
		}
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: ref("authority")}
	f.base.Namespace = ns
	for _, project := range projects {
		f.base.Projects = append(f.base.Projects, authzfixed.Project{Ref: projectRef(project), Name: project, InstallationID: "fixed", Active: true})
	}
	for user, allowed := range map[string][]string{"owner": projects, "mover": {visible, newEpics}, "child-only": {visible}, "parent-only": {epics}} {
		for _, project := range allowed {
			f.base.Grants = append(f.base.Grants, authz.Grant{Subject: authz.SubjectRef{Kind: "user", ID: "user:" + user}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: projectRef(project)}, Role: "involved"})
		}
	}
	f.facts, err = authzrpc.NewMutableFacts(f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.keys = authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
	f.app, err = authzrpc.New(ctx, authzrpc.Config{Store: store, Lease: lease, Provider: f.keys, Facts: f.facts, Namespace: ns, DefaultProject: epics, Hierarchy: true, Caller: observedCaller})
	if err != nil {
		t.Fatal(err)
	}
	vc := viewops.Config{OntologyRevision: model.Natural64(1), AuthorizeRead: func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
		_, err := f.app.Service.Get(ctx, by, service.GetRequest{ID: resource.Ref})
		return err
	}}
	if progress {
		vc.EpicProgress, err = viewrpc.NewEpicProgressConfig(f.app.Checker, service.StaticOntologySource(ref("ontology/ep6")))
		if err != nil {
			t.Fatal(err)
		}
	}
	views, err := viewops.New(store, lease, vc)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, &observedService{GuardedService: f.app.Service, t: t}); err != nil {
		t.Fatal(err)
	}
	if err := viewrpc.BindWithConfig(registry, views, viewrpc.Config{CursorKey: []byte("ep6-disposable-cursor-key-0123456789")}); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, AuthMode: "authenticated", OntologyRevision: model.Natural64(1), DefaultProject: epics,
		CurrentIncarnation: func(ctx context.Context) (string, error) { m, e := store.Meta(ctx); return m.Incarnation, e }})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{Provider: f.keys, AuthorizationInstalled: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, admission, httpjson.Config{RequestTimeout: 60 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	f.api = listen(t, handler)
	f.owner = f.issue(t, agent("owner")).Token
	return f
}

func (f *fixture) close(t *testing.T) {
	t.Helper()
	f.once.Do(func() {
		if f.lifecycle != nil {
			f.lifecycle.close(t)
		}
		if f.api != nil {
			f.api.close(t)
		}
		if f.stopRenew != nil {
			f.stopRenew()
			<-f.renewDone
		}
		if err := f.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
}

// lifecycleFixture is a separately admitted anonymous loopback host over the
// owned fixture store. The unchanged fixed policy does not grant lifecycle
// verbs. This host creates status observations through the real slot/ontology
// Writer; it never supplies hierarchy writes, count responses or a caller grant.
func lifecycleFixture(t *testing.T, store driver.Store, lease driver.ActiveLease) *endpoint {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	resolver := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(resolver))
	domain := service.New(store, lease, writer, service.WithOntology(resolver))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, AuthMode: "anonymous", DefaultProject: epics,
		CurrentIncarnation: func(ctx context.Context) (string, error) { m, e := store.Meta(ctx); return m.Incarnation, e }})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return listen(t, handler)
}

func (f *fixture) issue(t *testing.T, p model.PrincipalRef) authnapikey.IssuedKey {
	t.Helper()
	key, err := f.keys.Issue(p, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (f *fixture) write(t *testing.T, token, method, project string, params map[string]any) reply {
	t.Helper()
	f.sequence++
	params["_meta"] = map[string]any{"project": project, "agent": "ep6-qualifier", "idempotencyKey": fmt.Sprintf("%032x", f.sequence), "return": "full"}
	return f.api.call(t, token, method, params)
}

func committed(t *testing.T, r reply) model.Commit {
	t.Helper()
	result(t, r)
	var out model.Commit
	if err := json.Unmarshal(r.Result, &out); err != nil || len(out.Results) == 0 {
		t.Fatalf("write has no committed results: %s: %v", r.Result, err)
	}
	return out
}

func (f *fixture) create(t *testing.T, project, title string, parent *model.Ref) model.Ref {
	t.Helper()
	params := map[string]any{"type": "issue", "fields": map[string]any{"title": title}}
	if parent != nil {
		params["parent"] = *parent
	}
	return committed(t, f.write(t, f.owner, "bead.create", project, params)).Results[0].Ref
}

func progressParams(parent model.Ref, project string) map[string]any {
	return map[string]any{"definition": map[string]any{"filter": map[string]any{"query": "parent=" + parent.ID}, "fields": []string{}, "groupBy": []string{"epic-progress"}}, "_meta": map[string]any{"project": project}}
}

func (f *fixture) head(t *testing.T) string {
	t.Helper()
	m, e := f.store.Meta(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	return m.Head.String()
}

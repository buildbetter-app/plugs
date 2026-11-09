// Independent browser fixture: no shared EP6 helper or production-policy changes.
package epicauthbrowser_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/buildbetter-app/plugs/internal/core/authz"
	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
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
	"github.com/buildbetter-app/plugs/internal/server/collab"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/server/webui"
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
	return listenRoot(t, handler, handler)
}

func listenRoot(t *testing.T, handler *httpjson.Handler, root http.Handler) *endpoint {
	t.Helper()
	s, err := handler.ListenHandler(context.Background(), "127.0.0.1:0", nil, root)
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
	t           *testing.T
	mu          sync.Mutex
	agents      map[string]string
	fullBatches int
}

var _ rpc.Service = (*observedService)(nil)

func (s *observedService) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	started := time.Now()
	out, err := s.GuardedService.ApplyWithOutcome(ctx, by, cs, meta)
	if err == nil {
		s.mu.Lock()
		if len(cs.Ops) == 100 {
			s.fullBatches++
		}
		if strings.HasPrefix(by.ID, "agent:ep6-") {
			if by.Sponsor == nil {
				s.t.Error("authenticated agent lost sponsor")
			} else {
				s.agents[by.ID] = by.Sponsor.ID
			}
		}
		s.mu.Unlock()
	}
	var domain *model.Error
	if errors.As(err, &domain) && domain.Data.Code == "authz_unavailable" {
		s.t.Logf("EP6 real service refusal: code=%s message=%q operations=%d elapsed_ms=%.3f", domain.Data.Code, domain.Message, len(cs.Ops), float64(time.Since(started))/float64(time.Millisecond))
	}
	return out, err
}

// Preserve the lifecycle service's actual refusal before RPC canonicalizes it.
// The collector observes the unchanged Writer; no request, deadline, outcome or
// error is replaced, and diagnostics run only after the real service returns.
type observedLifecycleService struct {
	*service.Service
	t      *testing.T
	phases *pipeline.Collector
	store  driver.Store
}

func (s *observedLifecycleService) CloseWithOutcome(ctx context.Context, by model.PrincipalRef, req service.CloseRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	started := time.Now()
	out, err := s.Service.CloseWithOutcome(ctx, by, req, meta)
	if err != nil {
		code, message := model.ErrorCode(""), err.Error()
		var domain *model.Error
		if errors.As(err, &domain) {
			code, message = domain.Data.Code, domain.Message
		}
		phases, _ := json.Marshal(s.phases.Snapshot())
		s.t.Logf("EP6_LIFECYCLE_REFUSAL method=bead.close code=%s message=%q elapsed_ms=%.3f phases=%s", code, message, float64(time.Since(started))/float64(time.Millisecond), phases)
		s.observeSourceIndexes(ctx, req.ID)
	}
	return out, err
}

// These bounded driver-interface reads happen only after the failed write.
// They neither initialize index coverage nor substitute facts for its guards.
func (s *observedLifecycleService) observeSourceIndexes(ctx context.Context, target model.Ref) {
	current, err := s.store.Meta(ctx)
	if err != nil {
		s.t.Logf("EP6_LIFECYCLE_SOURCE_INDEX snapshot_meta_error=%v", err)
		return
	}
	reader, err := s.store.Read(ctx, driver.View{Incarnation: current.Incarnation})
	if err != nil {
		s.t.Logf("EP6_LIFECYCLE_SOURCE_INDEX snapshot_read_error=%v", err)
		return
	}
	defer reader.Close()
	if indexed, ok := reader.(driver.HierarchyChildrenReader); ok {
		_, _, err := indexed.HierarchyChildren(ctx, []model.Ref{target}, 0)
		s.t.Logf("EP6_LIFECYCLE_SOURCE_INDEX hierarchy_children_error=%v", err)
	}
	if indexed, ok := reader.(driver.InverseSourceLinksReader); ok {
		_, _, err := indexed.InverseSourceLinks(ctx, []model.Ref{target}, []driver.IncomingLinkKind{driver.IncomingLinkParentChild}, nil, 0)
		s.t.Logf("EP6_LIFECYCLE_SOURCE_INDEX inverse_source_links_error=%v", err)
	}
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
	observed  *observedService
	trace     *traceLog
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

// Native restore targets bypass newFixtureMode. Keep the existing lease TTL
// while renewing through the upload, publication, and close sequence.
func openRenewingTarget(t *testing.T, backend, path string) (driver.Store, driver.ActiveLease, func()) {
	t.Helper()
	store, lease := openStore(t, backend, path)
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	var renewalErr error
	renewals := 0
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil {
					if ctx.Err() == nil || !errors.Is(err, context.Canceled) {
						renewalErr = err
					}
					return
				}
				renewals++
			}
		}
	}()
	var once sync.Once
	closeTarget := func() {
		once.Do(func() {
			stop()
			<-done
			if renewalErr != nil {
				t.Errorf("native target lease renewal: %v", renewalErr)
			}
			t.Logf("native target lease renewals=%d error=%v", renewals, renewalErr)
			if err := lease.Release(context.Background()); err != nil {
				t.Error(err)
			}
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(closeTarget)
	return store, lease, closeTarget
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
	f.trace = &traceLog{}
	provider := &recordingProvider{Provider: f.keys}
	f.app, err = authzrpc.New(ctx, authzrpc.Config{Store: store, Lease: lease, Provider: provider, Facts: f.facts, Namespace: ns, DefaultProject: epics, Hierarchy: true})
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
	f.observed = &observedService{GuardedService: f.app.Service, t: t, agents: map[string]string{}}
	if err := rpc.BindService(registry, f.observed); err != nil {
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
	admission, err := authn.New(authn.Config{Provider: provider, AuthorizationInstalled: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpjson.New(server, admission, httpjson.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ui, err := webui.New(webui.Config{Project: epics})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ui/", ui)
	mux.Handle("/rpc/v1", f.trace.handler(handler))
	f.api = listenRoot(t, handler, mux)
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
	writer, phases := pipeline.NewWithTelemetry(store, lease, pipeline.WithOntology(resolver))
	domain := service.New(store, lease, writer, service.WithOntology(resolver))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, &observedLifecycleService{Service: domain, t: t, phases: phases, store: store}); err != nil {
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

// Observers delegate once to the real provider/transport and retain no bearer.
// The request and response bodies below are actual native bytes, not fixtures.
type traceKey struct{}
type nativeTrace struct {
	ID             string          `json:"id"`
	Method         string          `json:"method"`
	Params         json.RawMessage `json:"params"`
	Principal      string          `json:"principal"`
	Kind           string          `json:"kind"`
	Status         int             `json:"status"`
	Response       json.RawMessage `json:"response"`
	ResponseSHA256 string          `json:"responseSha256"`
	Cancelled      bool            `json:"cancelled,omitempty"`
}
type traceLog struct {
	mu   sync.Mutex
	rows []nativeTrace
}
type recordingProvider struct{ *authnapikey.Provider }

func (p *recordingProvider) Authenticate(ctx context.Context, token string) (model.PrincipalRef, error) {
	by, err := p.Provider.Authenticate(ctx, token)
	if row, ok := ctx.Value(traceKey{}).(*nativeTrace); ok && err == nil {
		row.Principal = by.ID
		row.Kind = by.Kind
	}
	return by, err
}

type traceWriter struct {
	http.ResponseWriter
	body   bytes.Buffer
	status int
}

func (w *traceWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *traceWriter) Flush()                      { _ = http.NewResponseController(w.ResponseWriter).Flush() }
func (w *traceWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *traceWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	w.body.Write(data)
	return w.ResponseWriter.Write(data)
}
func (log *traceLog) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only this disposable composition has an observer. Leave native admission,
		// authorization, deadlines, Writer bodies and errors completely unchanged.
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read refused", 400)
			return
		}
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(raw))
		row := nativeTrace{}
		_ = json.Unmarshal(raw, &row)
		capture := &traceWriter{ResponseWriter: w}
		next.ServeHTTP(capture, r.WithContext(context.WithValue(r.Context(), traceKey{}, &row)))
		if !strings.HasPrefix(row.ID, "web-ui-") && !strings.HasPrefix(row.ID, "chrome-probe-") {
			return
		}
		row.Status = capture.status
		row.Cancelled = r.Context().Err() != nil
		row.Response = append(json.RawMessage(nil), capture.body.Bytes()...)
		sum := sha256.Sum256(row.Response)
		row.ResponseSHA256 = hex.EncodeToString(sum[:])
		log.mu.Lock()
		defer log.mu.Unlock()
		log.rows = append(log.rows, row)
	})
}
func (log *traceLog) snapshot() []nativeTrace {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]nativeTrace(nil), log.rows...)
}

// These additional host seams are confined to the background-auth regression.
// Each saved-view, presence and event decision delegates to the unchanged fixed
// checker through the actual service. No native response or provider is faked.
type backgroundPolicy struct {
	f      *fixture
	parent model.Ref
}

func (p *backgroundPolicy) allowed(ctx context.Context, by model.PrincipalRef, id model.Ref) (bool, error) {
	_, err := p.f.app.Service.Get(ctx, by, service.GetRequest{ID: id})
	var domain *model.Error
	if errors.As(err, &domain) && (domain.Data.Code == "not_found" || domain.Data.Code == "forbidden") {
		return false, nil
	}
	return err == nil, err
}

func (p *backgroundPolicy) GetView(ctx context.Context, by model.PrincipalRef, id string) (viewops.Definition, error) {
	ok, err := p.allowed(ctx, by, p.parent)
	if err != nil {
		return viewops.Definition{}, err
	}
	if !ok || id != "background-home" {
		return viewops.Definition{}, &model.Error{Data: model.ErrorData{Code: "not_found"}}
	}
	budget := model.Natural64(25)
	return viewops.Definition{Filter: &viewops.Filter{Resources: []string{p.parent.ID}}, Fields: []string{"id", "type", "project", "status", "revision", "fields.title"}, Sort: []string{"id"}, BudgetTokens: &budget}, nil
}

func (p *backgroundPolicy) AdmitPresence(ctx context.Context, by model.PrincipalRef, project string) (bool, error) {
	if project != epics {
		return false, nil
	}
	return p.allowed(ctx, by, p.parent)
}

func (p *backgroundPolicy) CanRead(ctx context.Context, by model.PrincipalRef, bead service.Bead) (bool, error) {
	return p.allowed(ctx, by, ref(bead.ID))
}

func (p *backgroundPolicy) Read(ctx context.Context, by model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	id, ok := strings.CutPrefix(event.Resource, "bead/")
	if !ok {
		return coreevents.Visibility{}, nil
	}
	readable, err := p.allowed(ctx, by, ref(id))
	return coreevents.Visibility{Readable: readable, Before: event.Before, After: event.After}, err
}

// A gate delays exactly one real request before admission, or holds its actual
// reply after native allocation. Contexts, deadlines, principals and native
// response bytes are unchanged. It supplies explicit ordering, never a retry.
type requestGate struct {
	mu        sync.Mutex
	method    string
	holdReply bool
	entered   chan struct{}
	release   chan struct{}
	done      chan struct{}
	session   string
	cancelled atomic.Bool
}

func (g *requestGate) arm(method string) { g.armAt(method, false) }

func (g *requestGate) armAt(method string, holdReply bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.method, g.entered, g.release, g.done = method, make(chan struct{}), make(chan struct{}), make(chan struct{})
	g.holdReply, g.session = holdReply, ""
	g.cancelled.Store(false)
}

func (g *requestGate) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := "watch.attach"
		if r.URL.Path == httpjson.Path {
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read refused", 400)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var call struct {
				Method string `json:"method"`
			}
			_ = json.Unmarshal(raw, &call)
			method = call.Method
		}
		g.mu.Lock()
		var entered, release, done chan struct{}
		holdReply := false
		if g.method == method && g.entered != nil {
			g.method, g.session = "", r.Header.Get("Plug-Session")
			entered, release, done, holdReply = g.entered, g.release, g.done, g.holdReply
		}
		g.mu.Unlock()
		var reply *httptest.ResponseRecorder
		if holdReply {
			reply = httptest.NewRecorder()
			next.ServeHTTP(reply, r)
			if method == "session.open" {
				var envelope struct {
					Result collab.Session `json:"result"`
				}
				_ = json.Unmarshal(reply.Body.Bytes(), &envelope)
				g.mu.Lock()
				g.session = envelope.Result.SessionID
				g.mu.Unlock()
			}
		}
		if release != nil {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				g.cancelled.Store(true)
				<-release
			}
			if r.Context().Err() != nil {
				g.cancelled.Store(true)
			}
			defer close(done)
		}
		if reply != nil {
			for name, values := range reply.Header() {
				w.Header()[name] = append([]string(nil), values...)
			}
			w.WriteHeader(reply.Code)
			_, _ = w.Write(reply.Body.Bytes())
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (g *requestGate) wait(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("actual native request did not reach gate")
	}
}

func (g *requestGate) unblock() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.release != nil {
		select {
		case <-g.release:
		default:
			close(g.release)
		}
	}
}

type streamRecord struct {
	Request   string `json:"request"`
	Principal string `json:"principal"`
	Status    int    `json:"status"`
	Body      string `json:"body"`
	Cancelled bool   `json:"cancelled"`
}

type backgroundAuthHost struct {
	f        *fixture
	gate     requestGate
	sessions *collab.Manager
	watches  *watch.Manager
	active   atomic.Int64
	mu       sync.Mutex
	streams  []streamRecord
}

func newBackgroundAuthHost(t *testing.T, f *fixture, parent model.Ref) *backgroundAuthHost {
	t.Helper()
	h := &backgroundAuthHost{f: f}
	t.Cleanup(h.gate.unblock)
	policy := &backgroundPolicy{f: f, parent: parent}
	views, err := viewops.New(f.store, f.lease, viewops.Config{Views: policy, AuthorizeRead: func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, _ string) error {
		_, err := f.app.Service.Get(ctx, by, service.GetRequest{ID: resource.Ref})
		return err
	}, OntologyRevision: model.Natural64(1), PolicyVersion: "fixed-background-regression"})
	if err != nil {
		t.Fatal(err)
	}
	h.sessions, err = collab.New(context.Background(), collab.Config{Service: f.app.Service, Lease: f.lease, AuthorizationInstalled: func() bool { return true }, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.sessions.Close() })
	codec, err := serverevents.NewCursorCodec([]byte("background-auth-disposable-event-cursor-key"))
	if err != nil {
		t.Fatal(err)
	}
	events := &serverevents.Service{Reader: &coreevents.Reader{Log: serverevents.Log{Store: f.store}, Materializer: serverevents.HistoryMaterializer{Store: f.store}, Authorization: policy}, Cursors: codec}
	h.watches, err = watch.New(context.Background(), watch.Config{Events: events})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.watches.Close() })
	adapter, err := watchhttp.New(h.watches, watchhttp.Config{Views: views, Ontology: model.Natural64(1), PolicyVersion: "fixed-background-regression"})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, bind := range []func() error{func() error { return rpc.BindService(registry, f.observed) }, func() error { return viewrpc.Bind(registry, views) }, func() error { return collab.Bind(registry, h.sessions) }, func() error { return adapter.Register(registry) }} {
		if err := bind(); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{AuthMode: "authenticated", Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), DefaultProject: epics})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := authn.New(authn.Config{Provider: &recordingProvider{Provider: f.keys}, AuthorizationInstalled: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	bridge := func(ctx context.Context, by model.PrincipalRef, id, agent string) (context.Context, error) {
		lifetime, err := h.sessions.SessionContext(ctx, collab.Caller{Principal: by, Agent: agent}, id)
		if err != nil {
			return nil, err
		}
		return watchhttp.WithSession(ctx, id, lifetime), nil
	}
	transport, err := httpjson.New(server, admission, httpjson.Config{SessionBridge: bridge})
	if err != nil {
		t.Fatal(err)
	}
	assets := map[string][]byte{"index.html": []byte(`<!doctype html><title>Authentication regression frame</title>`)}
	digest, err := webui.FrameBundleDigest(assets)
	if err != nil {
		t.Fatal(err)
	}
	ui, err := webui.New(webui.Config{Project: epics, HomeViewID: "background-home", WatchPath: watchhttp.DefaultPath, Frames: []webui.FrameBundle{{ID: "auth-probe", Assets: assets, Digest: digest, Grant: webui.BridgeGrant{Project: epics, Methods: []string{"bead.get"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ui/", ui)
	mux.Handle(httpjson.Path, f.trace.handler(h.gate.handler(transport)))
	// Match the real shared host's authentication/session bridge. No stream
	// status or body is injected; 401 is produced by actual key admission and
	// 404 by an actually closed collaboration session.
	stream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		record := streamRecord{Request: string(raw)}
		capture := &traceWriter{ResponseWriter: w}
		h.active.Add(1)
		defer func() {
			record.Status = capture.status
			record.Body = capture.body.String()
			record.Cancelled = r.Context().Err() != nil
			h.mu.Lock()
			h.streams = append(h.streams, record)
			h.mu.Unlock()
			h.active.Add(-1)
		}()
		identity, err := admission.AdmitHTTP(r)
		if err != nil {
			http.Error(capture, "unauthenticated", http.StatusUnauthorized)
			return
		}
		record.Principal = identity.Principal().ID
		ctx, err := bridge(r.Context(), identity.Principal(), r.Header.Get("Plug-Session"), r.Header.Get("Plug-Agent"))
		if err != nil {
			http.Error(capture, "session not found", http.StatusNotFound)
			return
		}
		adapter.ServeHTTP(capture, r.WithContext(watchhttp.WithPrincipal(ctx, identity.Principal())))
	})
	mux.Handle(watchhttp.DefaultPath, h.gate.handler(stream))
	f.api.close(t)
	f.api = listenRoot(t, transport, mux)
	return h
}

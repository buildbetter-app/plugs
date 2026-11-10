package epicprogress_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
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
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
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
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
	status int
	raw    []byte
}

// logRefusal keeps the complete canonical transport error beside the caller
// phase. The diagnostic is test-only; it does not change the request,
// retry, authorization, or assertion path.
func logRefusal(t *testing.T, r reply) {
	t.Helper()
	if r.Error == nil {
		return
	}
	data, err := json.Marshal(r.Error.Data)
	if err != nil {
		t.Logf("EP6 RPC refusal: code=%d message=%q data=<marshal error: %v>", r.Error.Code, r.Error.Message, err)
		return
	}
	t.Logf("EP6 RPC refusal: code=%d message=%q data=%s", r.Error.Code, r.Error.Message, data)
}

// Observe only the real service's error before RPC canonicalizes its message.
// The same request, outcome and error pass through unchanged; there is no
// second write, retry, policy hook or substitute authorization decision.
type observedService struct {
	*authzrpc.GuardedService
	t        *testing.T
	observer *pipeline.Collector
}

var _ rpc.Service = (*observedService)(nil)

func (s *observedService) ApplyWithOutcome(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (port.WriteOutcome, error) {
	before := s.observer.Snapshot()
	started := time.Now()
	out, err := s.GuardedService.ApplyWithOutcome(ctx, by, cs, meta)
	elapsed := time.Since(started)
	var domain *model.Error
	if errors.As(err, &domain) && domain.Data.Code == "authz_unavailable" {
		s.t.Logf("EP6 real service refusal: code=%s message=%q operations=%d elapsed_ms=%.3f", domain.Data.Code, domain.Message, len(cs.Ops), float64(time.Since(started))/float64(time.Millisecond))
	}
	if len(cs.Ops) >= 100 || err != nil {
		after := s.observer.Snapshot()
		lanes := map[pipeline.Phase]float64{}
		for phase, samples := range after.LaneMillis {
			for result, millis := range samples {
				lanes[phase] += millis - before.LaneMillis[phase][result]
			}
		}
		preMS := 0.0
		for _, phase := range []pipeline.Phase{pipeline.PhaseAdmission, pipeline.PhaseReplay, pipeline.PhasePrepare, pipeline.PhasePre, pipeline.PhaseGraph, pipeline.PhaseClaims, pipeline.PhaseChanges, pipeline.PhaseAudit} {
			preMS += lanes[phase]
		}
		code := model.ErrorCode("")
		if domain != nil {
			code = domain.Data.Code
		}
		// The fixed 20k workload makes these service calls sequential. Commit
		// includes projection preparation, lock/staging and the driver's Commit
		// under the unchanged 2s preCtx; the lane sum is not elapsed deadline.
		encoded, _ := json.Marshal(lanes)
		s.t.Logf("write_timing label=ep6-real-service operations=%d pre_phase_lanes_ms=%.3f commit_ms=%.3f wall_ms=%.3f error_code=%q lanes_ms=%s", len(cs.Ops), preMS, lanes[pipeline.PhaseCommit], float64(elapsed)/float64(time.Millisecond), code, encoded)
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
		logRefusal(t, r)
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
	logRefusal(t, r)
	if r.Error == nil || r.Error.Data.Code != code || len(r.Result) != 0 || r.Meta.Replayed {
		t.Fatalf("expected pure %s refusal: HTTP %d %s", code, r.status, r.raw)
	}
}

type readObserver struct {
	queries            atomic.Uint64
	hierarchyChildren  atomic.Uint64
	inverseSourceLinks atomic.Uint64
}

func (o *readObserver) reset() {
	o.queries.Store(0)
	o.hierarchyChildren.Store(0)
	o.inverseSourceLinks.Store(0)
}

func (o *readObserver) queryCount() uint64 {
	return o.queries.Load()
}

func (o *readObserver) hierarchyCount() uint64 {
	return o.hierarchyChildren.Load()
}

func (o *readObserver) inverseCount() uint64 {
	return o.inverseSourceLinks.Load()
}

type observedReadStore struct {
	store    viewops.ReadStore
	observer *readObserver
}

func (s *observedReadStore) Meta(ctx context.Context) (driver.Meta, error) {
	return s.store.Meta(ctx)
}

func (s *observedReadStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	reader, err := s.store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &observedReader{Reader: reader, observer: s.observer}, nil
}

type observedReader struct {
	driver.Reader
	observer *readObserver
}

func (r *observedReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	r.observer.queries.Add(1)
	return r.Reader.Query(ctx, query)
}

func (r *observedReader) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.observer.hierarchyChildren.Add(1)
	reader, ok := r.Reader.(driver.HierarchyChildrenReader)
	if !ok {
		return nil, false, driver.ErrUnsupported
	}
	return reader.HierarchyChildren(ctx, parents, limit)
}

func (r *observedReader) HierarchyChildrenExcluding(ctx context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.observer.hierarchyChildren.Add(1)
	reader, ok := r.Reader.(driver.HierarchyChildrenExcludingReader)
	if !ok {
		return nil, false, driver.ErrUnsupported
	}
	return reader.HierarchyChildrenExcluding(ctx, parents, excluded, limit)
}

func (r *observedReader) InverseSourceLinks(ctx context.Context, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.observer.inverseSourceLinks.Add(1)
	reader, ok := r.Reader.(driver.InverseSourceLinksReader)
	if !ok {
		return nil, false, driver.ErrUnsupported
	}
	return reader.InverseSourceLinks(ctx, targets, kinds, excluded, limit)
}

// readerOnly intentionally exposes no optional hierarchy interface. It lets
// the regression compare the indexed result with the existing scan path.
type readerOnly struct{ driver.Reader }

type unindexedReadStore struct{ store viewops.ReadStore }

func (s *unindexedReadStore) Meta(ctx context.Context) (driver.Meta, error) {
	return s.store.Meta(ctx)
}

func (s *unindexedReadStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	reader, err := s.store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return readerOnly{Reader: reader}, nil
}

type fixture struct {
	store      driver.Store
	lease      driver.ActiveLease
	keys       *authnapikey.Provider
	facts      *authzrpc.MutableFacts
	base       authzfixed.Facts
	app        *authzrpc.Composition
	api        *endpoint
	lifecycle  *endpoint
	views      *viewops.Service
	viewConfig viewops.Config
	reads      *readObserver
	sequence   int
	owner      string
	stopRenew  context.CancelFunc
	renewDone  chan struct{}
	once       sync.Once
}

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		switch name {
		case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
		default:
			cmd.Env = append(cmd.Env, value)
		}
	}
	return cmd.CombinedOutput()
}

func ownedPostgres(t *testing.T) string {
	t.Helper()
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	name := "plug-ep6-progress-" + hex.EncodeToString(random[:])
	out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_ep6", "-p", "127.0.0.1::5432", postgresImage)
	if err != nil {
		t.Fatalf("owned PostgreSQL start failed: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := docker("rm", "-f", name); err != nil {
			t.Errorf("owned PostgreSQL cleanup failed: %v %s", err, out)
		}
	})
	out, err = docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatalf("owned PostgreSQL address failed: %v %s", err, out)
	}
	address := strings.TrimSpace(string(out))
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		t.Fatalf("owned PostgreSQL must bind only loopback: %q %v", address, err)
	}
	dsn := "postgres://postgres@" + address + "/plug_ep6?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for db.PingContext(ctx) != nil {
		select {
		case <-ctx.Done():
			t.Fatal("owned PostgreSQL did not become ready")
		case <-time.After(30 * time.Millisecond):
		}
	}
	return dsn
}

func openStore(t *testing.T, backend, path string) (driver.Store, driver.ActiveLease) {
	t.Helper()
	var store driver.Store
	var err error
	switch backend {
	case "memory":
		store = memory.New()
	case "sqlite":
		store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: path})
	case "postgres":
		store, err = postgres.Open(context.Background(), driver.Config{DSN: ownedPostgres(t)})
	default:
		t.Fatalf("unknown backend %q", backend)
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

// Native restore targets bypass newFixtureMode. Own the same renewal cadence
// until close, including upload, publication, and the subsequent indexed write.
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
	reads := &readObserver{}
	f := &fixture{store: store, lease: lease, reads: reads, renewDone: make(chan struct{})}
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
	observer := pipeline.NewCollector()
	f.app, err = authzrpc.New(ctx, authzrpc.Config{Store: store, Lease: lease, Provider: f.keys, Facts: f.facts, Namespace: ns, DefaultProject: epics, Hierarchy: true, WriterObserver: observer})
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
	f.viewConfig = vc
	f.views, err = viewops.New(&observedReadStore{store: store, observer: reads}, lease, vc)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, &observedService{GuardedService: f.app.Service, t: t, observer: observer}); err != nil {
		t.Fatal(err)
	}
	if err := viewrpc.BindWithConfig(registry, f.views, viewrpc.Config{CursorKey: []byte("ep6-disposable-cursor-key-0123456789")}); err != nil {
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

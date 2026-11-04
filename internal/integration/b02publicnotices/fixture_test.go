package b02publicnotices_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/ontologyrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	blockerProject   = "b02-blocker-project"
	dependentProject = "b02-dependent-project"
)

var sourceRef = model.Ref{ID: "ontology/b02-public"}
var caller = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

type reply struct {
	Result json.RawMessage `json:"result"`
	Error  *wire.Error     `json:"error"`
	Meta   struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

// This port adapter only forwards to the production cursor service, Reader
// and HistoryMaterializer. It does not construct events or ontology decisions.
type eventProvider struct{ service serverevents.Service }

func (p eventProvider) List(ctx context.Context, req service.EventProviderRequest) (service.EventPage, error) {
	page, err := p.service.List(ctx, serverevents.Request{
		Principal: req.Principal,
		Filter:    coreevents.Filter{Types: req.Filter.Types, Resources: req.Filter.Resources},
		After:     req.After,
		Limit:     req.Limit,
	})
	return service.EventPage{Events: page.Events, Cursor: page.Cursor}, err
}

type fixture struct {
	ctx      context.Context
	engine   engine
	store    driver.Store
	lease    driver.ActiveLease
	api      *httptest.Server
	registry *rpc.Registry
	source   driver.Record
	digest   string
	nextID   int
	nextKey  int
}

func newFixture(t *testing.T, e engine) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, engine: e}
	f.open(t)
	t.Cleanup(func() {
		if f.api != nil {
			f.api.CloseClientConnections()
			f.api.Close()
		}
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	// Only the bootstrap protected source is a fixture. Project overlays,
	// beads, links and lifecycle changes must all pass through public RPC.
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "b02-ontology-bootstrap"}}, []conformance.FixtureChange{{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: body}})
	if err != nil {
		t.Fatal(err)
	}
	f.compose(t)
	db := e.db(t)
	defer db.Close()
	if e.name == "sqlite_wal" {
		var mode string
		if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
			t.Fatalf("file-backed SQLite WAL: mode=%q err=%v", mode, err)
		}
	}
	return f
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	f.store, err = f.engine.storage.Open(f.ctx, driver.Config{DSN: f.engine.dsn})
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "b02-public-notices", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) meta(t *testing.T) driver.Meta {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func (f *fixture) read(t *testing.T) driver.Reader {
	t.Helper()
	meta := f.meta(t)
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (f *fixture) loadSource(t *testing.T) ontology.Snapshot {
	t.Helper()
	r := f.read(t)
	rows, err := r.Get(f.ctx, []model.Ref{sourceRef})
	closeErr := r.Close()
	if err != nil || closeErr != nil || len(rows) != 1 || rows[0].Kind != "ontology" {
		t.Fatalf("load durable ontology: rows=%+v err=%v close=%v", rows, err, closeErr)
	}
	f.source = rows[0]
	source, err := service.DecodeOntologySource(f.source.Body)
	if err != nil {
		t.Fatal(err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	f.digest = compiled.Digest()
	return compiled
}

func (f *fixture) compose(t *testing.T) {
	t.Helper()
	compiled := f.loadSource(t)
	normalizer := service.NewOntology(compiled.Effective)
	writer := pipeline.New(f.store, f.lease,
		pipeline.WithOntology(normalizer),
		pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(sourceRef))),
		pipeline.WithIDGenerator(func() (string, error) {
			ids := []string{"pl-b02-blocker", "pl-b02-ordinary", "pl-b02-conditional-z", "pl-b02-conditional-a"}
			if f.nextID >= len(ids) {
				return "", fmt.Errorf("unexpected extra bead create")
			}
			id := ids[f.nextID]
			f.nextID++
			return id, nil
		}),
	)
	core, err := ontologyops.New(f.store, f.lease, ontologyops.Config{
		Source: service.StaticOntologySource(sourceRef), ValidatingPre: ontologyops.NoValidatingPre(),
		ProtectedApplier: writer, OntologyRefresher: normalizer,
	})
	if err != nil {
		t.Fatal(err)
	}
	cursors, err := serverevents.NewCursorCodec([]byte("b02-public-test-cursor-signing-key-0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	events := serverevents.Service{Reader: &coreevents.Reader{
		Log: serverevents.Log{Store: f.store}, Materializer: serverevents.HistoryMaterializer{Store: f.store}, Authorization: coreevents.AllowAll{},
	}, Cursors: cursors}
	domain := service.New(f.store, f.lease, writer, service.WithOntology(normalizer), service.WithEventProvider(eventProvider{events}))
	f.registry, err = rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(f.registry, domain); err != nil {
		t.Fatal(err)
	}
	if err := ontologyrpc.Bind(f.registry, core); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(f.registry, rpc.Config{Incarnation: f.meta(t).Incarnation, DefaultProject: dependentProject, AuthMode: "anonymous", OntologyRevision: f.source.Revision})
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
	f.api = httptest.NewServer(handler)
}

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	before, epoch := f.meta(t), f.lease.Epoch()
	f.api.CloseClientConnections()
	f.api.Close()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.open(t)
	if after := f.meta(t); !reflect.DeepEqual(before, after) || f.lease.Epoch() <= epoch {
		t.Fatalf("reopen changed metadata or failed to fence old lease: before=%+v after=%+v epoch=%d->%d", before, after, epoch, f.lease.Epoch())
	}
	f.compose(t)
}

func jsonBytes(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func equalJSON(t *testing.T, got, want any) {
	t.Helper()
	if !bytes.Equal(jsonBytes(t, got), jsonBytes(t, want)) {
		t.Fatalf("got %s\nwant %s", jsonBytes(t, got), jsonBytes(t, want))
	}
}

func sameRef(a, b model.Ref) bool {
	return a.IsID() && b.IsID() && a.ID == b.ID && a.Branch.String() == b.Branch.String()
}

func (f *fixture) call(t *testing.T, method string, params map[string]any) reply {
	t.Helper()
	body := jsonBytes(t, map[string]any{"jsonrpc": "2.0", "id": "b02", "method": method, "params": params})
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.api.URL+"/rpc/v1", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Plug-RPC", "1")
	response, err := f.api.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		t.Fatal(err)
	}
	var result reply
	if err := json.Unmarshal(raw, &result); err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("%s HTTP %d: %s err=%v", method, response.StatusCode, raw, err)
	}
	if result.Error == nil {
		descriptor, ok := f.registry.Catalog().Descriptor(method)
		if !ok {
			t.Fatalf("missing canonical descriptor for %s", method)
		}
		if err := f.registry.Catalog().ValidateResult(descriptor, result.Result); err != nil {
			t.Fatalf("%s invalid public result %s: %v", method, result.Result, err)
		}
	} else if err := f.registry.Catalog().ValidateError(result.Error); err != nil {
		t.Fatalf("%s invalid public refusal: %+v err=%v", method, result.Error, err)
	}
	return result
}

func decodeResult(t *testing.T, result reply, into any) {
	t.Helper()
	if result.Error != nil {
		t.Fatalf("public RPC refused: %+v", result.Error)
	}
	if err := json.Unmarshal(result.Result, into); err != nil {
		t.Fatalf("decode public result %s: %v", result.Result, err)
	}
}

func (f *fixture) result(t *testing.T, method string, params map[string]any, into any) {
	t.Helper()
	decodeResult(t, f.call(t, method, params), into)
}

func (f *fixture) writeParams(project string, params map[string]any) map[string]any {
	f.nextKey++
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": fmt.Sprintf("%032x", f.nextKey)}
	return params
}

func (f *fixture) write(t *testing.T, method, project string, params map[string]any) model.Commit {
	t.Helper()
	var commit model.Commit
	f.result(t, method, f.writeParams(project, params), &commit)
	// The public commit DTO omits incarnation; bind that through the real
	// store metadata and persisted changeset instead of inventing a field.
	if commit.Revision.Validate() != nil || commit.Revision.IsZero() || len(commit.Results) == 0 {
		t.Fatalf("missing real public write commit: %+v", commit)
	}
	return commit
}

// Install actual protected overlays through public plan/apply and its typed
// anonymous presence check. The runtime OntologyRefresher is shared with the
// ordinary Writer and service; tests never inject a second category resolver.
func (f *fixture) install(t *testing.T, project string, document ontology.Document) {
	t.Helper()
	r := f.read(t)
	rows, err := r.Get(f.ctx, []model.Ref{sourceRef})
	closeErr := r.Close()
	if err != nil || closeErr != nil || len(rows) != 1 {
		t.Fatalf("source before plan: rows=%+v err=%v close=%v", rows, err, closeErr)
	}
	expect := []model.Precondition{model.PreconditionByRevision(sourceRef, rows[0].Revision)}
	var plan ontologyops.PlanResult
	f.result(t, "ontology.plan", map[string]any{"project": project, "document": document, "expect": expect}, &plan)
	if !plan.Changes.Ready || !plan.RequiresHuman || plan.PlanDigest == "" || plan.Changes.Source.Ref.ID != sourceRef.ID || plan.Changes.Source.Revision != rows[0].Revision {
		t.Fatalf("public overlay plan lacks exact source binding: %+v", plan)
	}
	commit := f.write(t, "ontology.apply", project, map[string]any{
		"planDigest": plan.PlanDigest, "expect": expect,
		"humanPresence": map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest},
	})
	var installed ontologyops.Result
	f.result(t, "ontology.get", map[string]any{"project": project}, &installed)
	if installed.Document == nil || installed.Revision != commit.Revision || installed.Drifted || len(installed.Problems) != 0 {
		t.Fatalf("publicly installed catalog is absent or drifted: %+v", installed)
	}
	for key, status := range document.Statuses {
		equalJSON(t, installed.Document.Statuses[key], status)
	}
	for key, workflow := range document.Workflows {
		equalJSON(t, installed.Document.Workflows[key], workflow)
	}
	f.loadSource(t) // retain the durable audit pin; do not replace the runtime
	// Refresh evidence comes from the next ordinary RPC write, not Compile.
}

type retained struct {
	Record   driver.Record
	Versions []driver.Version
}

func (f *fixture) retained(t *testing.T, refs []model.Ref) map[string]retained {
	t.Helper()
	r := f.read(t)
	defer r.Close()
	result := map[string]retained{}
	for _, ref := range refs {
		rows, err := r.Get(f.ctx, []model.Ref{ref})
		if err != nil || len(rows) != 1 {
			t.Fatalf("record %s: %+v err=%v", ref.ID, rows, err)
		}
		versions, err := r.Versions(f.ctx, ref, driver.PageReq{})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("retained history %s: %+v err=%v", ref.ID, versions, err)
		}
		result[ref.ID] = retained{rows[0], versions.Items}
	}
	return result
}

func (f *fixture) receipt(t *testing.T, method string, params map[string]any) *driver.IdempotencyRecord {
	t.Helper()
	principal, err := driver.PrincipalKey(caller)
	if err != nil {
		t.Fatal(err)
	}
	r := f.read(t)
	defer r.Close()
	key := params["_meta"].(map[string]any)["idempotencyKey"].(string)
	entry, err := r.IdempotencyByKey(f.ctx, principal, "", method, key)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func (f *fixture) changeset(t *testing.T, revision model.Revision) model.Changeset {
	t.Helper()
	sets, err := (serverevents.Log{Store: f.store}).Scan(f.ctx, coreevents.Position{Incarnation: f.meta(t).Incarnation, Revision: revision}, 1)
	if err != nil || len(sets) != 1 || sets[0].Revision != revision {
		t.Fatalf("persisted changeset %s: %+v err=%v", revision, sets, err)
	}
	if err := sets[0].ValidateEvents(); err != nil {
		t.Fatal(err)
	}
	return sets[0]
}

func (f *fixture) log(t *testing.T) []model.Changeset {
	t.Helper()
	r := f.read(t)
	defer r.Close()
	sets, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil || len(sets) == 1000 {
		t.Fatalf("complete small fixture log unavailable: count=%d err=%v", len(sets), err)
	}
	return sets
}

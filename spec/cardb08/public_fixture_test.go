package cardb20

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

const b08Project = "b08-public"

var b08CatalogRef = model.RefByID("ontology/b08-public-acceptance", model.Natural64(0))
var b08Actor = model.PrincipalRef{Kind: "user", ID: "b08-owner"}

func b08JSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func b08Save(t *testing.T, root, name string, value any) {
	t.Helper()
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), b08JSON(t, value), 0600); err != nil {
		t.Fatal(err)
	}
}

func b08Root(t *testing.T, name string) string {
	t.Helper()
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute original evidence directory required")
	}
	root = filepath.Join(root, name)
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	return root
}

// Only ontology configuration is seeded. All subjects, relationships,
// attachments, lifecycle actions and receipts use the public JSON-RPC bytes.
func b08Catalog() ontology.Document {
	d := ontology.DefaultDocument()
	d.Statuses["b08-queued"] = ontology.Status{Name: "Queued", Category: ontology.Active}
	d.Statuses["b08-finished"] = ontology.Status{Name: "Finished", Category: ontology.Done, Outcome: ontology.Success}
	w := d.Workflows["issue"]
	w.Slots = map[ontology.Slot]string{ontology.Claim: "in_progress", ontology.Close: "b08-finished", ontology.Reopen: "b08-queued", ontology.Fail: "failed", ontology.Cancel: "canceled", ontology.Defer: "backlog"}
	w.Statuses = append(w.Statuses, "b08-queued", "b08-finished")
	w.Initial = "b08-queued"
	w.Transitions = append(w.Transitions,
		ontology.Rule{To: "b08-queued", Guards: []ontology.Guard{{Kind: ontology.Permission}}},
		ontology.Rule{To: "b08-finished", Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}},
	)
	d.Workflows["b08-flow"] = w
	base := d.Types["issue"]
	base.Workflow = "b08-flow"
	base.Fields["proof"] = ontology.Field{Type: "string"}
	d.Types["b08-item"] = base
	permitted := w
	permitted.Transitions = append([]ontology.Rule(nil), w.Transitions...)
	last := len(permitted.Transitions) - 1
	permitted.Transitions[last].Guards = nil
	for _, guard := range w.Transitions[last].Guards {
		if guard.Kind != ontology.NoOpenChildren {
			permitted.Transitions[last].Guards = append(permitted.Transitions[last].Guards, guard)
		}
	}
	d.Workflows["b08-child-permitted"] = permitted
	permittedType := base
	permittedType.Workflow = "b08-child-permitted"
	d.Types["b08-child-permitted"] = permittedType
	for _, extra := range []struct {
		Name  string
		Guard ontology.Guard
	}{
		{"required", ontology.Guard{Kind: ontology.FieldsRequired, Fields: []string{"proof"}}},
		{"evidence", ontology.Guard{Kind: ontology.EvidenceRequired, Kinds: []string{"link"}}},
		{"gate", ontology.Guard{Kind: ontology.GatesResolved, Kinds: []string{"b08-ci"}}},
	} {
		flow := w
		flow.Transitions = append([]ontology.Rule(nil), w.Transitions...)
		last := len(flow.Transitions) - 1
		flow.Transitions[last].Guards = append(append([]ontology.Guard(nil), flow.Transitions[last].Guards...), extra.Guard)
		d.Workflows["b08-"+extra.Name] = flow
		kind := base
		kind.Workflow = "b08-" + extra.Name
		d.Types["b08-"+extra.Name] = kind
	}
	d.GateKinds = map[string]ontology.GateKind{"b08-ci": {Resolver: "b08-ci-resolver"}}
	return d
}

type b08Reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Message string `json:"message"`
		Data    struct {
			Code    string         `json:"code"`
			Details map[string]any `json:"details"`
		} `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

type b08Call struct {
	Method, Key string
	Principal   model.PrincipalRef
}

type b08Fixture struct {
	t                         *testing.T
	ctx                       context.Context
	engine, dsn, root, nextID string
	store                     driver.Store
	lease                     driver.ActiveLease
	clock                     *testclock.Clock
	server                    *rpc.Server
	calls                     []b08Call
	seq                       int
	permissionUnavailable     bool
	writerOptions             []pipeline.Option
}

func newB08Fixture(t *testing.T, engine, root string, permissionUnavailable bool, writerOptions ...pipeline.Option) *b08Fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	f := &b08Fixture{t: t, ctx: ctx, engine: engine, root: root, permissionUnavailable: permissionUnavailable, writerOptions: writerOptions, clock: testclock.New(t, time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC))}
	if engine == "sqlite" {
		f.dsn = filepath.Join(t.TempDir(), "b08.sqlite")
	}
	if engine == "postgres" {
		f.dsn = f.clock.BindPostgres(t, testclock.PostgresDSN(t))
	}
	f.open(true)
	t.Cleanup(f.close)
	return f
}

func (f *b08Fixture) open(seed bool) {
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: f.clock.Now})
	case "sqlite":
		f.store, err = (sqlite.Driver{Clock: f.clock.Now}).Open(f.ctx, driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = postgres.Open(f.ctx, driver.Config{DSN: f.dsn})
	default:
		f.t.Fatal("unknown real engine", f.engine)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "b08-public", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	if seed {
		body, err := service.MarshalOntologySource(service.OntologySource{Base: b08Catalog()})
		if err != nil {
			f.t.Fatal(err)
		}
		if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: b08CatalogRef, Kind: "ontology", Op: model.OpCreate, Body: body}}); err != nil {
			f.t.Fatal(err)
		}
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	rows, err := r.Get(f.ctx, []model.Ref{b08CatalogRef})
	closeErr := r.Close()
	if err != nil || closeErr != nil || len(rows) != 1 {
		f.t.Fatalf("stored ontology: %v %v", err, closeErr)
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		f.t.Fatal(err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		f.t.Fatal(problems)
	}
	var options []service.OntologyOption
	if f.permissionUnavailable {
		// This is the documented installed-but-unavailable adapter control,
		// not a fabricated permission result or a foreign-holder inference.
		options = append(options, service.WithTransitionPermissionObserver(nil))
	}
	o := service.NewOntology(compiled.Effective, options...)
	writerOptions := []pipeline.Option{pipeline.WithOntology(o), pipeline.WithClock(f.clock.Now), pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(b08CatalogRef))), pipeline.WithIDGenerator(func() (string, error) {
		if f.nextID == "" {
			return "", fmt.Errorf("missing declared B08 source ID")
		}
		id := f.nextID
		f.nextID = ""
		return id, nil
	})}
	writerOptions = append(writerOptions, f.writerOptions...)
	w := pipeline.New(f.store, f.lease, writerOptions...)
	serviceOptions := []service.Option{service.WithOntology(o), service.WithClock(f.clock.Now)}
	capability, err := driver.RequireWorkflowCheckpointStore(f.store, f.lease)
	if err != nil {
		f.t.Fatal(err)
	}
	state, err := pluginhost.NewWorkflowRuntimeState(capability, f.lease)
	if err != nil {
		f.t.Fatal(err)
	}
	marker, ok := state.(interface{ Persistent() bool })
	wantLifetime := driver.CheckpointLifetimeRestart
	if f.engine == "memory" {
		wantLifetime = driver.CheckpointLifetimeProcess
	}
	if !ok || marker.Persistent() != (f.engine != "memory") || capability.WorkflowCheckpointLifetime() != wantLifetime {
		f.t.Fatal("real workflow adapter has incorrect lifetime/persistence metadata")
	}
	provider, err := workflowplugin.New(state)
	if err != nil {
		f.t.Fatal(err)
	}
	provider.WithClock(f.clock.Now).WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
	serviceOptions = append(serviceOptions, service.WithWorkflow(provider))
	b08Save(f.t, f.root, "actual-workflow-capability.json", map[string]any{"engine": f.engine, "lifetime": capability.WorkflowCheckpointLifetime(), "persistent": marker.Persistent(), "namespace": "plugin/workflow/default/durable", "ruling": "17:14Z supersedes earlier memory refusal; process lifetime earns no restart credit"})
	svc := service.New(f.store, f.lease, w, serviceOptions...)
	reg, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := rpc.BindService(reg, svc); err != nil {
		f.t.Fatal(err)
	}
	f.server, err = rpc.New(reg, rpc.Config{DefaultProject: b08Project, Incarnation: meta.Incarnation, OntologyRevision: rows[0].Revision, CursorKey: []byte("b08-public-acceptance-cursor-key-0001")})
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *b08Fixture) close() {
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			f.t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}

func (f *b08Fixture) call(label, method string, params map[string]any, wantCode string) b08Reply {
	f.t.Helper()
	return f.callAs(b08Actor, label, method, params, wantCode)
}

func (f *b08Fixture) callAs(by model.PrincipalRef, label, method string, params map[string]any, wantCode string) b08Reply {
	f.t.Helper()
	f.seq++
	sum := sha256.Sum256([]byte("b08/" + label))
	key := hex.EncodeToString(sum[:])
	f.calls = append(f.calls, b08Call{Method: method, Key: key, Principal: by})
	body := map[string]any{}
	for k, v := range params {
		body[k] = v
	}
	body["_meta"] = map[string]any{"project": b08Project, "agent": "b08-recorder", "idempotencyKey": key}
	request := b08JSON(f.t, map[string]any{"jsonrpc": "2.0", "id": label, "method": method, "params": body})
	untouched := append([]byte(nil), request...)
	response := f.server.Dispatch(rpc.WithPrincipal(f.ctx, by), request)
	if !bytes.Equal(untouched, request) {
		f.t.Fatal("public dispatcher changed caller bytes")
	}
	b08Save(f.t, f.root, fmt.Sprintf("%03d-%s.rpc.json", f.seq, label), map[string]any{"request": json.RawMessage(request), "response": json.RawMessage(response), "application_clock": f.clock.Now()})
	var out b08Reply
	if err := json.Unmarshal(response, &out); err != nil {
		f.t.Fatal(err)
	}
	code := ""
	if out.Error != nil {
		code = out.Error.Data.Code
	}
	if code != wantCode {
		f.t.Fatalf("%s/%s code=%q want=%q original=%s", label, method, code, wantCode, response)
	}
	return out
}

func (f *b08Fixture) create(id, kind string, fields map[string]any, parent *model.Ref) model.Ref {
	f.t.Helper()
	f.nextID = "b08-" + id
	if fields == nil {
		fields = map[string]any{}
	}
	fields["title"] = id
	params := map[string]any{"type": kind, "fields": fields}
	if parent != nil {
		params["parent"] = parent
	}
	var commit model.Commit
	out := f.call("create-"+id, "bead.create", params, "")
	if err := json.Unmarshal(out.Result, &commit); err != nil || len(commit.Results) != 1 {
		f.t.Fatalf("public create: %s %v", out.Result, err)
	}
	expected := "b08-" + id
	if parent != nil {
		// Parented creates use the parent's actual persisted child counter,
		// independently of the host's top-level ID generator.
		expected = parent.ID + ".1"
	}
	if commit.Results[0].Ref.ID != expected {
		f.t.Fatalf("public create ID=%q want=%q", commit.Results[0].Ref.ID, expected)
	}
	return commit.Results[0].Ref
}

type b08Receipt driver.IdempotencyRecord

// A CommitNoop receipt has no history entry. Preserve every receipt field,
// representing the exactly zero Changeset as null rather than inventing its
// kind. A partially populated changeset still passes through typed marshaling
// and must fail if invalid.
func (r b08Receipt) MarshalJSON() ([]byte, error) {
	var changeset *model.Changeset
	if !reflect.DeepEqual(r.Changeset, model.Changeset{}) {
		changeset = &r.Changeset
	}
	return json.Marshal(struct {
		RequestHash     string
		Commit          model.Commit
		Changeset       *model.Changeset
		ResponsePending bool
	}{r.RequestHash, r.Commit, changeset, r.ResponsePending})
}

type b08State struct {
	Meta       driver.Meta
	Records    []driver.Record
	Versions   map[string][]driver.Version
	History    []model.Changeset
	Derived    map[driver.DerivedTable][]driver.DerivedRow
	Checkpoint driver.Checkpoint
	Receipts   map[string]b08Receipt
}

func (f *b08Fixture) state() b08State {
	f.t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	s := b08State{Meta: meta, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[string]b08Receipt{}}
	page, err := r.Query(f.ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		f.t.Fatalf("complete finite record census: %v", err)
	}
	s.Records = page.Items
	sort.Slice(s.Records, func(i, j int) bool { return s.Records[i].Ref.ID < s.Records[j].Ref.ID })
	for _, row := range s.Records {
		versions, err := r.Versions(f.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			f.t.Fatalf("complete versions: %v", err)
		}
		s.Versions[row.Ref.ID] = versions.Items
	}
	s.History, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil || len(s.History) >= 1000 {
		f.t.Fatalf("complete finite history: %v", err)
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		s.Derived[table], err = r.DerivedRows(f.ctx, table, "")
		if err != nil {
			f.t.Fatal(err)
		}
	}
	for _, call := range f.calls {
		principal, err := driver.PrincipalKey(call.Principal)
		if err != nil {
			f.t.Fatal(err)
		}
		receipt, err := r.IdempotencyByKey(f.ctx, principal, "b08-recorder", call.Method, call.Key)
		if err != nil {
			f.t.Fatal(err)
		}
		if receipt != nil {
			s.Receipts[call.Method+"/"+call.Key] = b08Receipt(*receipt)
		}
	}
	if cp, ok := f.store.(driver.CheckpointStore); ok {
		s.Checkpoint, err = cp.LoadCheckpoint(f.ctx, "plugin/workflow/default/durable")
		if err != nil {
			f.t.Fatal(err)
		}
	}
	return s
}

func (f *b08Fixture) document(id model.Ref) pipeline.Document {
	f.t.Helper()
	for _, row := range f.state().Records {
		if row.Ref.ID == id.ID {
			d, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				f.t.Fatal(err)
			}
			return d
		}
	}
	f.t.Fatal("missing original record", id.ID)
	return pipeline.Document{}
}

func (f *b08Fixture) conserved(label, method string, params map[string]any, code, guard string) {
	f.t.Helper()
	before := f.state()
	out := f.call(label, method, params, code)
	after := f.state()
	if !bytes.Equal(b08JSON(f.t, before), b08JSON(f.t, after)) {
		f.t.Fatalf("refusal %s changed records/history/versions/derived/receipts/checkpoint", label)
	}
	if guard != "" {
		raw := b08JSON(f.t, out.Error.Data.Details["violations"])
		var violations []ontology.Violation
		if json.Unmarshal(raw, &violations) != nil {
			f.t.Fatal("missing typed guard violations")
		}
		found := false
		for _, v := range violations {
			found = found || string(v.Guard) == guard
		}
		if !found {
			f.t.Fatalf("refusal missing guard %s: %s", guard, raw)
		}
	}
	b08Save(f.t, f.root, label+"-conservation.json", map[string]any{"before": before, "after": after, "code": code, "guard": guard})
}

func (f *b08Fixture) reopen(label string) {
	f.t.Helper()
	before := f.state()
	if f.engine != "memory" {
		f.close()
		f.open(false)
		if !bytes.Equal(b08JSON(f.t, before), b08JSON(f.t, f.state())) {
			f.t.Fatal("physical SQL reopen changed complete declared state")
		}
	}
	b08Save(f.t, f.root, label+"-readback.json", map[string]any{"engine": f.engine, "sql_physically_reopened": f.engine != "memory", "state": f.state()})
}

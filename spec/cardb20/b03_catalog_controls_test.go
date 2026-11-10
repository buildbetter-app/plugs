package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/ontologyrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// Card-specific controls reuse existing canonical build/PostgreSQL support.
// No native producer, receiver/importer, claim or authorization call is made.
const (
	b03Active  = "awaiting_review"
	b03WIP     = "qa_testing"
	b03Done    = "archived"
	b03Frozen  = "on_hold"
	b03Catalog = "awaiting_review:active,qa_testing:wip,archived:done,on_hold:frozen"
)

var b03Categories = map[string]ontology.Category{
	b03Active: ontology.Active, b03WIP: ontology.WIP, b03Done: ontology.Done, b03Frozen: ontology.Frozen,
}

type b03Point struct {
	Phase    string            `json:"phase"`
	Status   string            `json:"status"`
	Category ontology.Category `json:"category"`
	Ready    bool              `json:"ready"`
	Created  time.Time         `json:"created"`
	Started  *time.Time        `json:"started"`
	Closed   *time.Time        `json:"closed"`
}

type b03Public struct {
	ctx       context.Context
	store     driver.Store
	lease     driver.ActiveLease
	source    model.Ref
	transport *inprocess.Transport
	open      func() (driver.Store, error)
	sequence  int
	exchanges []map[string]any
	stop      context.CancelFunc
	done      chan error
}

func b03Write(t *testing.T, name string, value any) {
	t.Helper()
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute undeclared outputs directory required")
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, name+".json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func b03Document(extra int) ontology.Document {
	d := ontology.DefaultDocument()
	for key, category := range b03Categories {
		s := ontology.Status{Name: key, Category: category}
		if category == ontology.Done {
			s.Outcome = ontology.Success
		}
		d.Statuses[key] = s
		workflow := "b03-" + key
		d.Types[workflow] = ontology.Type{Name: workflow, Workflow: workflow, Fields: map[string]ontology.Field{"title": {Type: "string"}}}
		d.Workflows[workflow] = ontology.Workflow{Statuses: []string{b03Active, b03WIP, b03Done, b03Frozen}, Initial: key,
			Slots: map[ontology.Slot]string{ontology.Close: b03Done, ontology.Reopen: b03Active, ontology.Defer: b03Frozen},
			Transitions: []ontology.Rule{
				{To: b03Done, Guards: []ontology.Guard{{Kind: ontology.NotTemplate}, {Kind: ontology.NoOpenChildren}, {Kind: ontology.NoOpenBlockers}, {Kind: ontology.Permission}}},
				{To: b03Active, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
				{To: b03Frozen, Guards: []ontology.Guard{{Kind: ontology.Permission}}},
			},
		}
	}
	for i := 0; i < extra; i++ {
		key := fmt.Sprintf("catalog_%02d", i)
		d.Statuses[key] = ontology.Status{Name: key, Category: ontology.Active}
	}
	return d
}

func b03NewPublic(t *testing.T, engine string) *b03Public {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	f := &b03Public{ctx: ctx, source: model.Ref{ID: "ontology/b03-public-nonempty"}}
	switch engine {
	case "memory":
		f.open = func() (driver.Store, error) {
			return memory.NewWithOptions(memory.Options{StoreID: "b03-public", Incarnation: "b03-nonempty"})
		}
	case "sqlite":
		path := filepath.Join(t.TempDir(), "b03.db")
		f.open = func() (driver.Store, error) { return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path}) }
	case "postgres":
		dsn := startPostgres(t)
		f.open = func() (driver.Store, error) { return (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn}) }
	default:
		t.Fatal(engine)
	}
	f.start(t)
	t.Cleanup(func() { f.close(t); b03Write(t, "b03-plug-"+engine+"-exchanges", f.exchanges) })
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(ctx, f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "b03-bootstrap"}, Time: time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)}, []conformance.FixtureChange{{Ref: f.source, Kind: "ontology", Op: model.OpCreate, Body: body, ChangedPaths: []string{"base"}}})
	if err != nil {
		t.Fatal(err)
	}
	f.bind(t)
	return f
}

func (f *b03Public) start(t *testing.T) {
	t.Helper()
	var err error
	f.store, err = f.open()
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "b03-public-host", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	f.stop = cancel
	f.done = make(chan error, 1)
	lease, done := f.lease, f.done
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil {
					if ctx.Err() != nil {
						err = nil
					}
					done <- err
					return
				}
			}
		}
	}()
}
func (f *b03Public) close(t *testing.T) {
	t.Helper()
	if f.store == nil {
		return
	}
	f.stop()
	if err := <-f.done; err != nil {
		t.Error(err)
	}
	if err := f.store.Close(); err != nil {
		t.Error(err)
	}
	f.store = nil
}
func (f *b03Public) reader(t *testing.T) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (f *b03Public) row(t *testing.T, ref model.Ref) driver.Record {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	rows, err := r.Get(f.ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("read %s: %v", ref.ID, err)
	}
	return rows[0]
}
func (f *b03Public) bind(t *testing.T) {
	t.Helper()
	source, err := service.DecodeOntologySource(f.row(t, f.source).Body)
	if err != nil {
		t.Fatal(err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	workflow := service.NewOntology(compiled.Effective)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(workflow), pipeline.WithProtection(service.NewCatalogProtection(service.StaticOntologySource(f.source))))
	domain := service.New(f.store, f.lease, writer, service.WithOntology(workflow))
	core, err := ontologyops.New(f.store, f.lease, ontologyops.Config{Source: service.StaticOntologySource(f.source), ValidatingPre: ontologyops.NoValidatingPre(), ProtectedApplier: writer, OntologyRefresher: workflow})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err = ontologyrpc.Bind(registry, core); err != nil {
		t.Fatal(err)
	}
	if err = rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "b03", OntologyRevision: f.row(t, f.source).Revision})
	if err != nil {
		t.Fatal(err)
	}
	f.transport = inprocess.New(server)
}

func (f *b03Public) call(t *testing.T, method string, params map[string]any, write bool, want string, into any) json.RawMessage {
	t.Helper()
	if write {
		f.sequence++
		params["_meta"] = map[string]any{"project": "b03", "agent": "b03-public", "idempotencyKey": fmt.Sprintf("%032x", f.sequence)}
	}
	req, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": fmt.Sprint(f.sequence), "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.transport.RoundTrip(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	f.exchanges = append(f.exchanges, map[string]any{"request": string(req), "response": string(raw)})
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Data struct {
				Code string `json:"code"`
			} `json:"data"`
		} `json:"error"`
	}
	if err = json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	code := ""
	if out.Error != nil {
		code = out.Error.Data.Code
	}
	if code != want {
		t.Fatalf("%s got code=%s want=%s response=%s", method, code, want, raw)
	}
	if into != nil {
		if err = json.Unmarshal(out.Result, into); err != nil {
			t.Fatalf("%s: %v", method, err)
		}
	}
	return out.Result
}
func (f *b03Public) install(t *testing.T, d ontology.Document) {
	t.Helper()
	var plan ontologyops.PlanResult
	f.call(t, "ontology.plan", map[string]any{"document": d}, false, "", &plan)
	if !plan.Changes.Ready || len(plan.Violations) != 0 || len(plan.PlanDigest) != 64 {
		t.Fatalf("not a ready actual plan: %+v", plan)
	}
	f.call(t, "ontology.apply", map[string]any{"planDigest": plan.PlanDigest, "expect": plan.Changes.Expect, "humanPresence": map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest}}, true, "", nil)
	f.catalog(t, d)
}
func (f *b03Public) catalog(t *testing.T, want ontology.Document) {
	t.Helper()
	var got ontologyops.Result
	f.call(t, "ontology.get", map[string]any{"project": "b03"}, false, "", &got)
	if got.Drifted || got.Document == nil || !reflect.DeepEqual(*got.Document, want) {
		t.Fatalf("installed catalog does not equal exact planned definitions: %+v", got)
	}
}
func (f *b03Public) snapshot(t *testing.T) []byte {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := f.reader(t)
	defer r.Close()
	rows, err := r.Query(f.ctx, driver.Query{Limit: 1000})
	if err != nil || rows.NextCursor != nil {
		t.Fatalf("incomplete record snapshot: %v", err)
	}
	log, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(struct {
		Meta driver.Meta
		Rows []driver.Record
		Log  []model.Changeset
	}{meta, rows.Items, log})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func (f *b03Public) point(t *testing.T, ref model.Ref, phase, status string, ready bool) b03Point {
	t.Helper()
	var bead struct {
		Bead struct {
			Status string `json:"status"`
		} `json:"bead"`
	}
	f.call(t, "bead.get", map[string]any{"id": ref}, false, "", &bead)
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		Next string `json:"nextCursor"`
	}
	f.call(t, "bead.ready", map[string]any{"limit": 1000, "_meta": map[string]any{"project": "b03"}}, false, "", &page)
	found := false
	for _, b := range page.Items {
		found = found || b.ID == ref.ID
	}
	doc, err := pipeline.DecodeDocument(f.row(t, ref).Body)
	if err != nil || doc.CreatedAt == nil || doc.Fields["status"] != status || bead.Bead.Status != status || found != ready || page.Next != "" {
		t.Fatalf("%s exact public lifecycle/readiness mismatch: %+v %v", phase, doc, err)
	}
	return b03Point{phase, status, b03Categories[status], found, *doc.CreatedAt, doc.StartedAt, doc.ClosedAt}
}
func b03PlugCatalog(t *testing.T, engine string) []b03Point {
	t.Helper()
	f := b03NewPublic(t, engine)
	// Plug adopts canonical immutable keys and no native 50-status limit.
	for _, n := range []int{50, 51} {
		d := b03Document(n - 4)
		f.install(t, d)
		b03Write(t, fmt.Sprintf("b03-plug-%s-boundary-%d", engine, n), d)
	}
	d := b03Document(47)
	// Case-distinct keys remain distinct under Plug's adopted catalog contract.
	d.Statuses["Awaiting_review"] = ontology.Status{Name: "Awaiting_review", Category: ontology.Frozen}
	f.install(t, d)
	invalid := []struct {
		name     string
		document any
		code     string
	}{
		{"blank-key", json.RawMessage(`{"statuses":{"":{"name":"blank","category":"active"}}}`), "invalid_params"},
		{"duplicate-key", json.RawMessage(`{"statuses":{"dup":{"name":"one","category":"active"},"dup":{"name":"two","category":"frozen"}}}`), "invalid_request"},
		{"invalid-category", json.RawMessage(`{"statuses":{"bad":{"name":"bad","category":"unknown"}}}`), "invalid_params"},
		{"null-statuses", json.RawMessage(`{"statuses":null}`), "invalid_params"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			before := f.snapshot(t)
			f.call(t, "ontology.plan", map[string]any{"document": tc.document}, false, tc.code, nil)
			if !bytes.Equal(before, f.snapshot(t)) {
				t.Fatal("refused catalog changed records, revisions or journal")
			}
			f.catalog(t, d)
		})
	}
	var primary model.Ref
	var points []b03Point
	for _, key := range []string{b03Active, b03WIP, b03Done, b03Frozen} {
		var commit model.Commit
		f.call(t, "bead.create", map[string]any{"type": "b03-" + key, "fields": map[string]any{"title": "literal category " + key}}, true, "", &commit)
		if len(commit.Results) != 1 {
			t.Fatal(commit)
		}
		ref := commit.Results[0].Ref
		p := f.point(t, ref, "initial-"+key, key, key == b03Active)
		points = append(points, p)
		if key == b03Active {
			primary = ref
		}
	}
	beforeUnknown := f.snapshot(t)
	f.call(t, "bead.update", map[string]any{"id": primary, "fields": map[string]any{"status": "unconfigured"}}, true, "ontology_use_slot_verb", nil)
	if !bytes.Equal(beforeUnknown, f.snapshot(t)) {
		t.Fatal("refused ordinary status patch changed durable image")
	}
	created := points[0]
	step := func(method, phase, status string, ready bool) b03Point {
		f.call(t, method, map[string]any{"id": primary}, true, "", nil)
		p := f.point(t, primary, phase, status, ready)
		points = append(points, p)
		return p
	}
	closed := step("bead.close", "close", b03Done, false)
	repeated := step("bead.close", "repeat-close", b03Done, false)
	if closed.Closed == nil || closed.Closed.IsZero() || !reflect.DeepEqual(closed.Closed, repeated.Closed) {
		t.Fatal("first close timestamp not retained")
	}
	reopened := step("bead.reopen", "reopen", b03Active, true)
	step("bead.defer", "defer", b03Frozen, false)
	step("bead.reopen", "ready-again", b03Active, true)
	if reopened.Closed != nil {
		t.Fatal("reopen retained closed timestamp")
	}
	for _, p := range points[4:] {
		if !p.Created.Equal(created.Created) || !reflect.DeepEqual(p.Started, created.Started) {
			t.Fatal("ordinary lifecycle rewrote creation/started timestamp")
		}
	}
	if engine != "memory" {
		before := f.snapshot(t)
		f.close(t)
		f.start(t)
		f.bind(t)
		if !bytes.Equal(before, f.snapshot(t)) {
			t.Fatal("physical SQL reopen changed stored records or journal")
		}
		f.catalog(t, d)
		points = append(points, f.point(t, primary, "sql-reopen", b03Active, true))
	}
	b03Write(t, "b03-plug-"+engine+"-lifecycle", map[string]any{"complete": !t.Failed(), "engine": engine, "points": points, "literal_categories": b03Categories, "deviations": []string{"Plug canonical catalog accepts 51 additional status definitions", "Plug case-distinct immutable keys coexist", "Plug close/reopen use configured archived/awaiting_review workflow slots", "Plug default readiness includes custom active"}, "qualification": "ACTUAL_PUBLIC_PLUG_OUTCOMES_NOT_NATIVE_INPUT"})
	return points
}

func TestB03PublicNonemptyCatalogControls(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b03PlugCatalog(t, engine) })
	}
}

package b03catalog_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/ontologyrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var caller = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

const agent = "b03-catalog-author"

var projects = []string{"base-project", "east", "west"}
var allEngines = []string{"memory", "sqlite", "postgres"}

type requestScope struct{ Method, Key string }

type fixture struct {
	ctx       context.Context
	engine    engine
	store     driver.Store
	lease     driver.ActiveLease
	core      *ontologyops.Service
	domain    *service.Service
	registry  *rpc.Registry
	transport *inprocess.Transport
	source    model.Ref
	requests  []requestScope
	sequence  uint64
	stopRenew context.CancelFunc
	renewDone chan error
}

func newFixture(t *testing.T, e engine) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, engine: e, source: model.Ref{ID: "ontology/b03-catalog"}}
	f.open(t)
	t.Cleanup(func() { f.close(t) })
	// Only the initial built-in source is bootstrapped. All custom definitions,
	// overlays, archival and ordinary rows below enter through public byte RPC.
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	f.seedSource(t, body, model.OpCreate)
	if err := f.bindStored(); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	f.store, err = f.engine.storage.Open(f.ctx, driver.Config{DSN: f.engine.dsn})
	if err != nil {
		t.Fatalf("open %s: %v", f.engine.name, err)
	}
	f.lease, err = f.store.Lease(f.ctx, "b03-catalog-host", driver.LeaseTTL)
	if err != nil {
		_ = f.store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	f.stopRenew, f.renewDone = cancel, make(chan error, 1)
	lease, done := f.lease, f.renewDone
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
						done <- nil
					} else {
						done <- err
					}
					return
				}
			}
		}
	}()
}

func (f *fixture) close(t *testing.T) {
	t.Helper()
	if f.store == nil {
		return
	}
	f.stopRenew()
	if err := <-f.renewDone; err != nil {
		t.Errorf("active lease renewal failed: %v", err)
	}
	if err := f.store.Close(); err != nil {
		t.Error(err)
	}
	f.store = nil
}

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	if f.engine.name == "memory" {
		t.Fatal("a fresh memory store cannot prove durable reopen")
	}
	f.close(t)
	f.open(t)
	// This explicitly connected host reloads only durable bytes. It does not
	// claim production composition.Runtime startup wiring or cached snapshots.
	if err := f.bindStored(); err != nil {
		t.Fatalf("reload installed source after real close/open: %v", err)
	}
}

func (f *fixture) bindStored() error {
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		return err
	}
	at := meta.Head
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		return err
	}
	rows, readErr := r.Get(f.ctx, []model.Ref{f.source})
	closeErr := r.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(rows) != 1 || rows[0].Kind != "ontology" || rows[0].Ref.ID != f.source.ID {
		return fmt.Errorf("installed protected source is absent or has the wrong kind")
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		return err
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		return fmt.Errorf("installed source does not compile: %+v", problems)
	}
	workflow := service.NewOntology(compiled.Effective)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(workflow), pipeline.WithOntologyAudit(service.NewOntologyAudit(service.StaticOntologySource(f.source))))
	domain := service.New(f.store, f.lease, writer, service.WithOntology(workflow))
	core, err := ontologyops.New(f.store, f.lease, ontologyops.Config{
		Source: service.StaticOntologySource(f.source), ValidatingPre: ontologyops.NoValidatingPre(),
		ProtectedApplier: writer, OntologyRefresher: workflow,
	})
	if err != nil {
		return err
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		return err
	}
	if err := ontologyrpc.Bind(registry, core); err != nil {
		return err
	}
	if err := rpc.BindService(registry, domain); err != nil {
		return err
	}
	server, err := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "base-project", OntologyRevision: rows[0].Revision})
	if err != nil {
		return err
	}
	f.core, f.domain, f.registry, f.transport = core, domain, registry, inprocess.New(server)
	return nil
}

// Deliberate corruption is a storage fault fixture, never custom-catalog
// producer acceptance. It is used only to assert malformed-source refusal.
func (f *fixture) seedSource(t *testing.T, body []byte, op model.OpKind) {
	t.Helper()
	_, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{
		Principal: model.PrincipalRef{Kind: "system", ID: "b03-bootstrap-or-corruption"},
		Time:      time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC),
	}, []conformance.FixtureChange{{Ref: f.source, Kind: "ontology", Op: op, Body: body, ChangedPaths: []string{"base", "overlays"}}})
	if err != nil {
		t.Fatal(err)
	}
}

type response struct {
	Result json.RawMessage `json:"result"`
	Error  *wire.Error     `json:"error"`
	Meta   struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

func (f *fixture) call(t *testing.T, method string, params any) response {
	t.Helper()
	raw := marshal(t, map[string]any{"jsonrpc": "2.0", "id": "b03-catalog", "method": method, "params": params})
	data, err := f.transport.RoundTrip(f.ctx, raw)
	if err != nil {
		t.Fatalf("%s transport: %v", method, err)
	}
	var out response
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("%s envelope: %s: %v", method, data, err)
	}
	if out.Error == nil {
		descriptor, exists := f.registry.Catalog().Descriptor(method)
		if !exists {
			t.Fatalf("%s missing canonical descriptor", method)
		}
		if err := f.registry.Catalog().ValidateResult(descriptor, out.Result); err != nil {
			t.Fatalf("%s noncanonical result %s: %v", method, out.Result, err)
		}
	} else if len(out.Result) != 0 {
		t.Fatalf("%s refusal also returned a result: %s", method, data)
	} else if err := f.registry.Catalog().ValidateError(out.Error); err != nil {
		t.Fatalf("%s noncanonical refusal: %+v: %v", method, out.Error, err)
	}
	return out
}

func (f *fixture) result(t *testing.T, method string, params any, into any) response {
	t.Helper()
	out := f.call(t, method, params)
	if out.Error != nil {
		t.Fatalf("%s refused: %s", method, marshal(t, out.Error))
	}
	if into != nil {
		if err := json.Unmarshal(out.Result, into); err != nil {
			t.Fatalf("%s result %s: %v", method, out.Result, err)
		}
	}
	return out
}

func (f *fixture) params(method, project string, values map[string]any) map[string]any {
	f.sequence++
	key := fmt.Sprintf("%032x", f.sequence)
	f.requests = append(f.requests, requestScope{method, key})
	values["_meta"] = map[string]any{"project": project, "agent": agent, "idempotencyKey": key}
	return values
}

func (f *fixture) refuse(t *testing.T, method string, params any, code model.ErrorCode) response {
	t.Helper()
	before := f.snapshot(t)
	out := f.call(t, method, params)
	if out.Error == nil || out.Error.Data.Code != string(code) {
		t.Errorf("%s refusal = %+v, want %s; result=%s", method, out.Error, code, out.Result)
	}
	equal(t, method+" refusal conserves durable state and installed views", before, f.snapshot(t))
	return out
}

func (f *fixture) create(t *testing.T, project, typ string) (model.Ref, map[string]any, response) {
	t.Helper()
	params := f.params("bead.create", project, map[string]any{"type": typ, "fields": map[string]any{"title": "catalog proof"}})
	var commit model.Commit
	out := f.result(t, "bead.create", params, &commit)
	if len(commit.Results) != 1 {
		t.Fatalf("create did not return exactly one row: %+v", commit)
	}
	return commit.Results[0].Ref, params, out
}

func (f *fixture) status(t *testing.T, ref model.Ref, want string) {
	t.Helper()
	got, err := f.domain.Get(f.ctx, caller, service.GetRequest{ID: ref})
	if err != nil || got.Bead.Status != want {
		t.Fatalf("installed status: want=%s got=%+v error=%v", want, got, err)
	}
	var public struct {
		Bead struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"bead"`
	}
	f.result(t, "bead.get", map[string]any{"id": ref}, &public)
	if public.Bead.ID != ref.ID || public.Bead.Status != want {
		t.Fatalf("public installed status differs from domain view: %+v", public)
	}
}

func (f *fixture) ready(t *testing.T, project string, ref model.Ref, want bool) {
	t.Helper()
	page, err := f.domain.Ready(f.ctx, caller, service.ReadyRequest{Project: project, Limit: 1000})
	if err != nil || page.Next != nil {
		t.Fatalf("incomplete installed ready view: %+v %v", page, err)
	}
	found := false
	for _, bead := range page.Items {
		found = found || bead.ID == ref.ID
	}
	if found != want {
		t.Fatalf("ready membership for %s/%s: want=%t got=%t", project, ref.ID, want, found)
	}
	var public struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		NextCursor string `json:"nextCursor"`
	}
	f.result(t, "bead.ready", map[string]any{"limit": 1000, "_meta": map[string]any{"project": project}}, &public)
	found = false
	for _, bead := range public.Items {
		found = found || bead.ID == ref.ID
	}
	if public.NextCursor != "" || found != want {
		t.Fatalf("public ready membership for %s/%s: want=%t got=%+v", project, ref.ID, want, public)
	}
}

func (f *fixture) plan(t *testing.T, project string, doc ontology.Document) ontologyops.PlanResult {
	t.Helper()
	before := f.snapshot(t)
	source := record(t, before, f.source)
	var plan ontologyops.PlanResult
	f.result(t, "ontology.plan", map[string]any{
		"project": project, "document": doc,
		"expect": []model.Precondition{model.PreconditionByRevision(f.source, source.Revision)},
	}, &plan)
	equal(t, "public plan is read only", before, f.snapshot(t))
	if !plan.Changes.Ready || !plan.RequiresHuman || len(plan.Violations) != 0 || len(plan.PlanDigest) != 64 ||
		plan.BaseRevision != before.Meta.Head || plan.Changes.StoreID != before.Meta.StoreID || plan.Changes.Incarnation != before.Meta.Incarnation ||
		plan.Changes.Source.Ref.ID != f.source.ID || plan.Changes.Source.Revision != source.Revision || plan.Changes.Source.BodyHash != hash(source.Body) || len(plan.Changes.Source.Digest) != 64 {
		t.Fatalf("plan lacks actual ready source/head bindings: %+v", plan)
	}
	rows := map[string]driver.Record{}
	for _, row := range before.Records {
		if row.Kind == "bead" {
			rows[row.Ref.ID] = row
		}
	}
	if len(plan.Changes.Migration.Rows) != len(rows) {
		t.Fatalf("plan omitted actual rows: got=%d want=%d", len(plan.Changes.Migration.Rows), len(rows))
	}
	for _, row := range plan.Changes.Migration.Rows {
		actual, exists := rows[row.Before.State.ID]
		if !exists || actual.Revision != row.Before.Revision {
			t.Fatalf("plan returned a duplicate, invented, or stale row: %+v", row)
		}
		delete(rows, row.Before.State.ID)
	}
	return plan
}

func (f *fixture) applyParams(plan ontologyops.PlanResult) map[string]any {
	return f.params("ontology.apply", plan.Changes.Project, map[string]any{
		"planDigest": plan.PlanDigest, "expect": plan.Changes.Expect,
		"humanPresence": map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest},
	})
}

func (f *fixture) install(t *testing.T, project string, doc ontology.Document) (ontologyops.PlanResult, map[string]any) {
	t.Helper()
	plan := f.plan(t, project, doc)
	params := f.applyParams(plan)
	f.apply(t, plan, params)
	return plan, params
}

func (f *fixture) apply(t *testing.T, plan ontologyops.PlanResult, params map[string]any) model.Commit {
	t.Helper()
	before := f.snapshot(t)
	var commit model.Commit
	f.result(t, "ontology.apply", params, &commit)
	after := f.snapshot(t)
	head, err := before.Meta.Head.Uint64()
	if err != nil || commit.Revision != model.Natural64(head+1) || after.Meta.Head != commit.Revision || len(after.Log) != len(before.Log)+1 {
		t.Fatalf("apply did not publish exactly one durable revision: %+v %v", commit, err)
	}
	source := record(t, after, f.source)
	want, err := service.MarshalOntologySource(plan.Changes.Document)
	if err != nil || source.Revision != commit.Revision || !bytes.Equal(source.Body, want) {
		t.Fatalf("apply did not persist exact planned source: got=%s want=%s error=%v", source.Body, want, err)
	}
	last := after.Log[len(after.Log)-1]
	if last.Revision != commit.Revision || last.Kind != model.ChangesetMigration || last.Principal.Kind != caller.Kind || last.Principal.ID != caller.ID {
		t.Fatalf("migration revision or applier attribution lost: %+v", last)
	}
	for _, change := range last.Changes {
		if record(t, after, change.Ref).Revision != commit.Revision {
			t.Fatalf("source and row changes did not share one revision: %+v", last)
		}
	}
	scope := f.requests[len(f.requests)-1]
	receipt := after.Receipts[scope.Method+"/"+scope.Key]
	if receipt == nil || receipt.ResponsePending || receipt.RequestHash == "" {
		t.Fatal("apply did not persist a final digest-bound receipt")
	}
	equal(t, "apply receipt and public response", receipt.Commit, commit)
	return commit
}

type durableState struct {
	Meta         driver.Meta
	Records      []driver.Record
	Versions     map[string][]driver.Version
	Log          []model.Changeset
	Derived      map[driver.DerivedTable][]driver.DerivedRow
	Observations []driver.LockState
	Allocators   map[string]*driver.OfflineAllocator
	Ready        map[string]service.BeadPage
	Views        map[string]service.GetResult
	Receipts     map[string]*driver.IdempotencyRecord
	ByKey        map[string]*model.Changeset
}

func (f *fixture) snapshot(t *testing.T) durableState {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	at := meta.Head
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Limit: 1000})
	if err != nil || page.AsOf != at {
		t.Fatalf("incomplete record inventory: %+v %v", page, err)
	}
	inventory := append([]driver.Record(nil), page.Items...)
	for page.NextCursor != nil {
		page, err = r.Query(f.ctx, driver.Query{Limit: 1000, Cursor: page.NextCursor})
		if err != nil || page.AsOf != at || len(page.Items) == 0 {
			t.Fatalf("record inventory did not advance at pinned head: %+v %v", page, err)
		}
		inventory = append(inventory, page.Items...)
		if len(inventory) > ontologymigration.MaxAffectedRows+1 {
			t.Fatal("record inventory exceeds bounded migration rows plus source")
		}
	}
	seen := map[string]bool{}
	for _, row := range inventory {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &row.Ref})
		if err != nil || seen[key] {
			t.Fatalf("duplicate or invalid record in complete inventory: %+v %v", row.Ref, err)
		}
		seen[key] = true
	}
	s := durableState{Meta: meta, Records: inventory, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{},
		Allocators: map[string]*driver.OfflineAllocator{}, Ready: map[string]service.BeadPage{}, Views: map[string]service.GetResult{},
		Receipts: map[string]*driver.IdempotencyRecord{}, ByKey: map[string]*model.Changeset{}}
	keys := map[string]driver.LockKey{}
	addKey := func(key driver.LockKey) {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			t.Fatal(err)
		}
		keys[encoded] = key
	}
	for _, key := range []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock} {
		addKey(key)
	}
	for _, row := range s.Records {
		ref := row.Ref
		addKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		versions, err := r.Versions(f.ctx, ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("incomplete versions for %s: %v", ref.ID, err)
		}
		s.Versions[ref.ID] = versions.Items
		if row.Kind == "bead" {
			bead, err := driver.NewBeadSentinel(ref.ID)
			if err != nil {
				t.Fatal(err)
			}
			addKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
			s.Views[ref.ID], err = f.domain.Get(f.ctx, caller, service.GetRequest{ID: ref})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		s.Derived[table], err = r.DerivedRows(f.ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range s.Derived[table] {
			addKey(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}})
		}
	}
	allocators, ok := r.(driver.AllocatorReader)
	if !ok {
		t.Fatal("store lacks allocator observations needed by rollback proof")
	}
	for _, project := range projects {
		value, err := allocators.LoadAllocator(f.ctx, project)
		if err != nil && !errors.Is(err, driver.ErrNotFound) {
			t.Fatal(err)
		}
		s.Allocators[project] = nil
		if err == nil {
			s.Allocators[project] = &value
		}
		lock, err := driver.AllocatorLock(project)
		if err != nil {
			t.Fatal(err)
		}
		addKey(lock)
		for _, typ := range []string{"issue", "delivery", "handoff-item"} {
			ready, err := driver.NewReadySentinel(project, typ)
			if err != nil {
				t.Fatal(err)
			}
			addKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
		}
		s.Ready[project], err = f.domain.Ready(f.ctx, caller, service.ReadyRequest{Project: project, Limit: 1000})
		if err != nil || s.Ready[project].Next != nil {
			t.Fatalf("incomplete project ready view: %v", err)
		}
	}
	encodedKeys := make([]string, 0, len(keys))
	for key := range keys {
		encodedKeys = append(encodedKeys, key)
	}
	sort.Strings(encodedKeys)
	ordered := make([]driver.LockKey, 0, len(keys))
	for _, key := range encodedKeys {
		ordered = append(ordered, keys[key])
	}
	s.Observations, err = r.Observe(f.ctx, ordered)
	if err != nil {
		t.Fatal(err)
	}
	s.Log, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil || len(s.Log) >= 1000 {
		t.Fatalf("incomplete changeset inventory: %v", err)
	}
	principal, err := driver.PrincipalKey(caller)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range f.requests {
		key := scope.Method + "/" + scope.Key
		s.Receipts[key], err = r.IdempotencyByKey(f.ctx, principal, agent, scope.Method, scope.Key)
		if err != nil {
			t.Fatal(err)
		}
		s.ByKey[key], err = r.ChangesetByKey(f.ctx, principal, agent, scope.Method, scope.Key)
		if err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func record(t *testing.T, state durableState, ref model.Ref) driver.Record {
	t.Helper()
	for _, row := range state.Records {
		if row.Ref.ID == ref.ID && row.Ref.Branch.String() == ref.Branch.String() {
			return row
		}
	}
	t.Fatalf("missing complete stored record %s", ref.ID)
	return driver.Record{}
}

func hash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func marshal(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func equal(t *testing.T, label string, before, after any) {
	t.Helper()
	a, b := marshal(t, before), marshal(t, after)
	if !bytes.Equal(a, b) {
		t.Fatalf("%s:\nbefore=%s\nafter=%s", label, a, b)
	}
}

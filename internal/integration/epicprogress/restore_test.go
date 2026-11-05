package epicprogress_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var presence = map[string]any{"mode": "presence_check", "challenge": "ep6-disposable-challenge", "proof": "ep6-test-verifier"}

// Admin is a separate, admitted ANONYMOUS LOOPBACK endpoint over the borrowed
// store/lease. An API-key User is never promoted to an interactive admin. The
// verifier is a fixture-only D32 authority, not a production presence claim.
func admin(t *testing.T, store driver.Store, lease driver.ActiveLease) *endpoint {
	t.Helper()
	ctx := context.Background()
	meta, e := store.Meta(ctx)
	if e != nil {
		t.Fatal(e)
	}
	verifier := restore.PresenceVerifierFunc(func(_ context.Context, method, digest string, p restore.Presence) error {
		if method == "" || digest == "" || p["challenge"] != presence["challenge"] || p["proof"] != presence["proof"] {
			return fmt.Errorf("ep6 test presence binding refused")
		}
		return nil
	})
	registry, e := rpc.NewRegistry()
	if e != nil {
		t.Fatal(e)
	}
	if e = rpc.BindAdmin(registry, restore.NewManager(store, lease, verifier)); e != nil {
		t.Fatal(e)
	}
	server, e := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: epics, AuthMode: "anonymous", CurrentIncarnation: func(ctx context.Context) (string, error) { m, e := store.Meta(ctx); return m.Incarnation, e }})
	if e != nil {
		t.Fatal(e)
	}
	admission, e := authn.New(authn.Config{})
	if e != nil {
		t.Fatal(e)
	}
	handler, e := httpjson.New(server, admission, httpjson.Config{})
	if e != nil {
		t.Fatal(e)
	}
	return listen(t, handler)
}

func dumpHead(t *testing.T, e *endpoint) ([]byte, *dumpformat.Archive) {
	t.Helper()
	var data bytes.Buffer
	cursor, dumpID := "", ""
	for page := 0; page < 4096; page++ {
		params := map[string]any{"history": "head", "pageSize": 65536, "humanPresence": presence}
		if cursor != "" {
			params["cursor"] = cursor
		}
		value := result(t, e.call(t, "", "admin.dump", params))
		id, ok := value["dumpId"].(string)
		if !ok || id == "" {
			t.Fatal("dump has no identity")
		}
		if dumpID != "" && id != dumpID {
			t.Fatal("dump changed identity while paging")
		}
		dumpID = id
		chunk, err := base64.StdEncoding.DecodeString(value["data"].(string))
		if err != nil {
			t.Fatal(err)
		}
		data.Write(chunk)
		if value["done"] == true {
			archive, err := dumpformat.Read(bytes.NewReader(data.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			return data.Bytes(), archive
		}
		cursor = dumpID + ":" + value["nextPage"].(json.Number).String()
	}
	t.Fatal("dump did not finish within the admitted page bound")
	return nil, nil
}

func restoreHead(t *testing.T, e *endpoint, data []byte, archive *dumpformat.Archive) string {
	t.Helper()
	// Keep the public envelope unchanged while retaining the exact failed
	// phase in test.log before result's caller-level fatal attribution.
	call := func(phase, method string, params map[string]any) map[string]any {
		t.Helper()
		t.Logf("NATIVE_RESTORE_RPC phase=%s boundary=begin method=%s", phase, method)
		reply := e.call(t, "", method, params)
		t.Logf("NATIVE_RESTORE_RPC phase=%s boundary=returned method=%s http=%d reply=%s", phase, method, reply.status, reply.raw)
		return result(t, reply)
	}
	raw, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err = d.Decode(&manifest); err != nil {
		t.Fatal(err)
	}
	meta := func(n int) map[string]any {
		return map[string]any{"project": epics, "idempotencyKey": fmt.Sprintf("ep6-restore-%032x", n)}
	}
	begin := call("begin", "admin.restore.begin", map[string]any{"manifest": manifest, "history": "head", "humanPresence": presence, "_meta": meta(0)})
	id := begin["restoreId"].(string)
	for page, offset := 0, 0; offset < len(data); page++ {
		end := offset + 65536
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		sum := sha256.Sum256(chunk)
		call(fmt.Sprintf("chunk/%d", page), "admin.restore.chunk", map[string]any{"restoreId": id, "page": page, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": hex.EncodeToString(sum[:]), "humanPresence": presence, "_meta": meta(page + 1)})
		offset = end
	}
	sum := sha256.Sum256(raw)
	finish := call("finish", "admin.restore.finish", map[string]any{"restoreId": id, "manifestSha256": hex.EncodeToString(sum[:]), "humanPresence": presence, "_meta": meta(1000000)})
	if finish["state"] != "complete" {
		t.Fatalf("restore incomplete: %v", finish)
	}
	incarnation, ok := finish["incarnation"].(string)
	if !ok || incarnation == "" {
		t.Fatal("restore omitted the new incarnation")
	}
	return incarnation
}

func TestEpicProgressSurvivesNativeRestoreAndSQLiteReopen(t *testing.T) {
	dir := t.TempDir()
	source := newFixture(t, "sqlite", filepath.Join(dir, "source.db"), true)
	g := source.graph(t)
	sourceAllocators := persistedAllocatorSnapshot(t, source.store)
	viewer := source.viewer(t, epics, visible)
	before := inspectProgress(t, source.api.call(t, viewer, "view.run", progressParams(g.parent, epics)), map[string]int{"total": 2, "status:todo": 1, "status:done": 1}, 1)
	beforeImages := persistedImages(t, source.store)
	if len(beforeImages) != 7 {
		t.Fatalf("source census has %d images, want six beads and one protected ontology", len(beforeImages))
	}
	for _, id := range []model.Ref{g.parent, g.open, g.done, g.secret, g.grandchild, g.unrelated, ref("ontology/ep6")} {
		if _, exists := beforeImages[id.ID]; !exists {
			t.Fatalf("source census omitted fixture identity %s", id.ID)
		}
	}
	beforeClosure := persistedClosure(t, source.store)
	if len(beforeClosure) != 11 {
		t.Fatalf("source hierarchy closure has %d rows, want 11 for six beads", len(beforeClosure))
	}
	sourceAdmin := admin(t, source.store, source.lease)
	data, archive := dumpHead(t, sourceAdmin)
	allocatorMember, ok := archive.NativeMember(dumpformat.NativeAllocatorMember)
	expectedAllocatorBytes, err := driver.EncodeAllocatorSnapshot(sourceAllocators)
	if err != nil || !ok || !bytes.Equal(allocatorMember, expectedAllocatorBytes) {
		t.Fatalf("paged admin dump lost exact retained allocator member: %v", err)
	}
	sourceAdmin.close(t)
	source.close(t)
	if archive.Manifest().History != "head" {
		t.Fatal("fixture silently requested another history mode")
	}
	path := filepath.Join(dir, "restored.db")
	target, lease := openStore(t, "sqlite", path)
	t.Cleanup(func() { _ = lease.Release(context.Background()); _ = target.Close() })
	// Do not construct authorization facts or seed ontology in the EMPTY target.
	// Restore must establish all source state through its admitted native path.
	targetAdmin := admin(t, target, lease)
	incarnation := restoreHead(t, targetAdmin, data, archive)
	restoredAllocators := persistedAllocatorSnapshot(t, target)
	expectedAllocators, err := sourceAllocators.Detached()
	if err != nil {
		t.Fatal(err)
	}
	expectedAllocators.Incarnation = incarnation
	expectedAllocators.At = model.Natural64(1)
	expectedAllocators.CoverageFloor = model.Natural64(1)
	for i := range expectedAllocators.Inventory.Parents {
		expectedAllocators.Inventory.Parents[i].Incarnation = incarnation
	}
	requireAllocatorBytes(t, expectedAllocators, restoredAllocators)
	if !reflect.DeepEqual(persistedImages(t, target), beforeImages) {
		t.Fatal("native restore changed source bead or protected ontology images")
	}
	if restored := persistedClosure(t, target); !reflect.DeepEqual(restored, beforeClosure) {
		t.Fatalf("native restore changed canonical hierarchy closure: got %d rows, want %d", len(restored), len(beforeClosure))
	}
	targetAdmin.close(t)
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if incarnation == before.asOf["incarnation"] {
		t.Fatal("restore retained the source incarnation")
	}
	// A fresh host/checker/key ring over the reopened physical SQLite file is
	// essential: no in-memory parent/count/authority cache may satisfy this read.
	reopened := newFixtureMode(t, "sqlite", path, true, false)
	requireAllocatorBytes(t, restoredAllocators, persistedAllocatorSnapshot(t, reopened.store))
	requireReadOnlyRestoredFacts(t, reopened, []model.Ref{g.parent, g.open, g.done, g.secret, g.grandchild, g.unrelated})
	if !reflect.DeepEqual(persistedImages(t, reopened.store), beforeImages) {
		t.Fatal("physical reopen changed restored source images")
	}
	if !reflect.DeepEqual(persistedClosure(t, reopened.store), beforeClosure) {
		t.Fatal("physical reopen changed restored canonical hierarchy closure")
	}
	read := reopened.viewer(t, epics, visible)
	after := inspectProgress(t, reopened.api.call(t, read, "view.run", progressParams(g.parent, epics)), before.counts, 1)
	if after.asOf["incarnation"] != incarnation {
		t.Fatal("reopened service lost restored incarnation")
	}
	old := progressParams(g.parent, epics)
	old["asOf"] = before.asOf
	requireCode(t, reopened.api.call(t, read, "view.run", old), "incarnation_changed")
	// Restored hierarchy must still participate in actual future graph writes:
	// a descendant cycle refuses atomically, then a valid detach updates counts.
	head := reopened.head(t)
	beforeRefusal := persistedAllocatorSnapshot(t, reopened.store)
	cycle := model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: g.parent, Parent: &g.grandchild}}}}
	requireCode(t, reopened.write(t, reopened.owner, "tx.apply", epics, map[string]any{"changeSet": cycle}), "ontology_guard_failed")
	if reopened.head(t) != head {
		t.Fatal("restored cycle refusal changed head")
	}
	requireAllocatorBytes(t, beforeRefusal, persistedAllocatorSnapshot(t, reopened.store))
	detach := model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: g.done}}}}
	committed(t, reopened.write(t, reopened.owner, "tx.apply", visible, map[string]any{"changeSet": detach}))
	if rows := len(persistedClosure(t, reopened.store)); rows != 10 {
		t.Fatalf("restored detach left %d hierarchy closure rows, want 10", rows)
	}
	inspectProgress(t, reopened.api.call(t, read, "view.run", after.refresh), map[string]int{"total": 1, "status:todo": 1}, 1)
	continued := reopened.create(t, visible, "continued after native restore", &g.parent)
	if continued.ID != g.parent.ID+".4" {
		t.Fatalf("native restore/reopen reused consumed child number: %+v", continued)
	}
	inspectProgress(t, reopened.api.call(t, read, "view.run", progressParams(g.parent, epics)), map[string]int{"total": 2, "status:todo": 2}, 1)
	continuedAllocators := persistedAllocatorSnapshot(t, reopened.store)
	reopened.close(t)
	durable := newFixtureMode(t, "sqlite", path, true, false)
	requireAllocatorBytes(t, continuedAllocators, persistedAllocatorSnapshot(t, durable.store))
	durableRead := durable.viewer(t, epics, visible)
	inspectProgress(t, durable.api.call(t, durableRead, "view.run", progressParams(g.parent, epics)), map[string]int{"total": 2, "status:todo": 2}, 1)
	t.Logf("EP6 native head restore, physical SQLite close/reopen, current policy, cycle refusal, and count refresh proven at restored incarnation %s", incarnation)
}

// A native restore intentionally has no current-incarnation host facts source.
// New must leave that absence intact while ordinary current authorization works.
// Explicit startup establishes core selection coverage, then an explicit Replace
// establishes the independently complete authority domain in its own transaction.
func requireReadOnlyRestoredFacts(t *testing.T, f *fixture, refs []model.Ref) {
	t.Helper()
	ctx := context.Background()
	keys, err := authzfixed.SelectionRows(f.base.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	read := func() driver.Reader {
		t.Helper()
		r, err := f.store.Read(ctx, driver.View{Incarnation: f.base.Namespace.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	ordinary := func(r driver.Reader) []bool {
		t.Helper()
		binding, err := f.app.Checker.BindAuthorization(ctx, r, model.PrincipalRef{Kind: "user", ID: "restored-reader"}, authz.Read, authz.Bead, "bead.list")
		if err != nil {
			t.Fatal(err)
		}
		allowed, err := binding.Check(ctx, refs)
		if err != nil || len(allowed) != len(refs) {
			t.Fatal("ordinary restored authorization unavailable:", err)
		}
		for _, allow := range allowed {
			if !allow {
				t.Fatal("ordinary unscoped user read changed after restore")
			}
		}
		return allowed
	}
	r := read()
	for _, key := range []driver.DerivedRowKey{keys.Source, keys.Grants, keys.Projects, keys.Teams} {
		states, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}})
		if err != nil || len(states) != 1 || states[0].Present || !states[0].Revision.IsZero() {
			t.Fatal("New published absent restored facts selection domain:", err)
		}
	}
	before := ordinary(r)
	_ = r.Close()
	if err := pipeline.BuildSourceIndexes(ctx, f.store, f.lease); err != nil {
		t.Fatal("explicit restored core startup:", err)
	}
	probe := func(r driver.Reader, domain bool) (driver.SelectionResult, error) {
		t.Helper()
		// The real fixed read policy for an uncaveated User is true. Membership
		// independently probes the complete grants domain, including its emptiness.
		binding, err := f.app.Checker.BindAuthorization(ctx, r, model.PrincipalRef{Kind: "user", ID: "restored-reader"}, authz.Read, authz.Bead, "bead.list")
		if err != nil {
			t.Fatal(err)
		}
		filter, err := binding.Filter()
		if err != nil {
			t.Fatal(err)
		}
		identity, err := filter.Binding()
		if err != nil {
			t.Fatal(err)
		}
		acl, err := driver.NewReadFact(authz.ACLDependency(f.base.Namespace).Key)
		if err != nil {
			t.Fatal(err)
		}
		states, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &keys.Source}})
		if err != nil || len(states) != 1 {
			t.Fatal("restored source observation:", err)
		}
		controls, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockReadFact, Fact: acl}})
		if err != nil || len(controls) != 1 {
			t.Fatal("restored ACL observation:", err)
		}
		states = append(states, controls...)
		sources := make([]driver.SelectionSource, len(states))
		for i, state := range states {
			sources[i] = driver.SelectionSource{Key: state.Key, Revision: state.Revision, Present: state.Present}
			if state.Present && state.Derived != nil {
				sources[i].Digest = driver.SelectionDigest(state.Derived.Value)
			}
		}
		at := *r.Snapshot().At
		truth := driver.SelectionExpr{Op: driver.SelectionTrue}
		plan := driver.SelectionPlan{Incarnation: f.base.Namespace.Incarnation, Branch: model.Natural64(0), ContentRevision: at, AuthorityRevision: at, ContentCommitted: true, Membership: truth, Authorization: truth, Mode: driver.SelectionCountMode}
		if domain {
			plan.Membership = driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: keys.Grants, Kind: driver.SelectionScopedGrantFact}}
		}
		plan.Boundary, err = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{Version: 1, PreparedBinding: identity, Incarnation: plan.Incarnation, Branch: plan.Branch, AuthorityRevision: at, Sources: sources, Chain: driver.SelectionChainCompiled, Authorization: truth, Host: driver.SelectionHostAbsent})
		if err != nil {
			t.Fatal(err)
		}
		return r.(driver.SelectionReader).Select(ctx, plan)
	}
	r = read()
	if got, err := probe(r, false); err != nil || got.Total != uint64(len(refs)) {
		t.Fatalf("explicit startup did not establish independent core coverage: %+v %v", got, err)
	}
	if _, err := probe(r, true); !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatal("absent facts domain was certified for selection:", err)
	}
	_ = r.Close()
	if err := f.facts.Replace(f.base); err != nil {
		t.Fatal("explicit restored authority publication:", err)
	}
	r = read()
	if got, err := probe(r, true); err != nil || got.Total != uint64(len(refs)) {
		t.Fatalf("explicit Replace did not establish facts selection: %+v %v", got, err)
	}
	if !reflect.DeepEqual(before, ordinary(r)) {
		t.Fatal("explicit facts publication changed ordinary authorization")
	}
	_ = r.Close()
	current := persistedAllocatorSnapshot(t, f.store)
	fresh, err := authzrpc.NewMutableFacts(f.base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authzrpc.New(ctx, authzrpc.Config{Store: f.store, Lease: f.lease, Provider: f.keys, Facts: fresh, Namespace: f.base.Namespace, DefaultProject: epics, Hierarchy: true}); err != nil {
		t.Fatal(err)
	}
	requireAllocatorBytes(t, current, persistedAllocatorSnapshot(t, f.store))
}

// TestEpicProgressNativeRestoreReopenDetachUsesIndexedHierarchy exercises the
// actual paged native dump/restore path at the 20,000-bead workload. The
// reopened Writer must use the certified direct-parent source index rather than
// enumerating every bead during the unchanged two-second pre-phase.
func TestEpicProgressNativeRestoreReopenDetachUsesIndexedHierarchy(t *testing.T) {
	dir := t.TempDir()
	source := newFixture(t, "sqlite", filepath.Join(dir, "source-large.db"), true)
	_, child := buildLargeEpic(t, source)
	sourceAdmin := admin(t, source.store, source.lease)
	data, archive := dumpHead(t, sourceAdmin)
	if !archive.Native() {
		t.Fatal("large fixture dump was not native")
	}
	sourceAdmin.close(t)
	source.close(t)

	path := filepath.Join(dir, "restored-large.db")
	target, lease, closeTarget := openRenewingTarget(t, "sqlite", path)
	targetAdmin := admin(t, target, lease)
	incarnation := restoreHead(t, targetAdmin, data, archive)
	targetAdmin.close(t)
	closeTarget()

	reopened := newFixtureMode(t, "sqlite", path, true, false)
	reads := reopened.reads
	reads.reset()
	writer := pipeline.New(&observedWriteStore{Store: reopened.store, observer: reads}, reopened.lease, pipeline.WithOntology(service.NewOntology(nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "operator"}, model.ChangeSet{Ops: []model.Op{{
		Kind:     model.OpReparent,
		Reparent: &model.ReparentOp{Target: child},
	}}}, model.WriteMeta{Project: visible, Method: "tx.apply", AgentLabel: "native-restore-detach", IdempotencyKey: "00000000000000000000000000000011"})
	if err != nil {
		t.Fatalf("native restore/reopen detach failed: %v (queries=%d parent_reads=%d inverse_reads=%d)", err, reads.queryCount(), reads.hierarchyCount(), reads.inverseCount())
	}
	if reads.queryCount() != 0 {
		t.Fatalf("native restore/reopen detach used whole-bead Query fallback: queries=%d hierarchy_reads=%d inverse_reads=%d", reads.queryCount(), reads.hierarchyCount(), reads.inverseCount())
	}
	if reads.hierarchyCount() == 0 || reads.inverseCount() == 0 {
		t.Fatalf("native restore/reopen detach did not use bounded source reads: hierarchy=%d inverse=%d", reads.hierarchyCount(), reads.inverseCount())
	}
	if rows := len(persistedClosure(t, reopened.store)); rows != 20049 {
		t.Fatalf("native restore/reopen detach left %d closure rows, want 20049", rows)
	}
	t.Logf("native restore/reopen detach indexed: incarnation=%s whole-bead queries=%d hierarchy_reads=%d inverse_reads=%d", incarnation, reads.queryCount(), reads.hierarchyCount(), reads.inverseCount())
}

func TestEpicProgressNativeRestoreDetachUsesIndexedHierarchyAcrossBackends(t *testing.T) {
	for _, backend := range []string{"memory", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			dir := t.TempDir()
			source := newFixture(t, backend, filepath.Join(dir, "source-large.db"), true)
			_, child := buildLargeEpic(t, source)
			sourceAdmin := admin(t, source.store, source.lease)
			data, archive := dumpHead(t, sourceAdmin)
			if !archive.Native() {
				t.Fatal("large fixture dump was not native")
			}
			sourceAdmin.close(t)
			source.close(t)

			target, lease, closeTarget := openRenewingTarget(t, backend, filepath.Join(dir, "restored-large.db"))
			targetAdmin := admin(t, target, lease)
			incarnation := restoreHead(t, targetAdmin, data, archive)
			targetAdmin.close(t)

			var targetStore driver.Store = target
			var targetLease driver.ActiveLease = lease
			if backend == "sqlite" {
				closeTarget()
				reopened := newFixtureMode(t, backend, filepath.Join(dir, "restored-large.db"), true, false)
				targetStore, targetLease = reopened.store, reopened.lease
			}

			reads := &readObserver{}
			writer := pipeline.New(&observedWriteStore{Store: targetStore, observer: reads}, targetLease, pipeline.WithOntology(service.NewOntology(nil)))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "operator"}, model.ChangeSet{Ops: []model.Op{{
				Kind:     model.OpReparent,
				Reparent: &model.ReparentOp{Target: child},
			}}}, model.WriteMeta{Project: visible, Method: "tx.apply", AgentLabel: "native-restore-detach", IdempotencyKey: "00000000000000000000000000000012"}); err != nil {
				t.Fatalf("native restore detach failed: %v (queries=%d hierarchy_reads=%d inverse_reads=%d)", err, reads.queryCount(), reads.hierarchyCount(), reads.inverseCount())
			}
			if reads.queryCount() != 0 {
				t.Fatalf("native restore detach used whole-bead Query fallback: queries=%d hierarchy_reads=%d inverse_reads=%d", reads.queryCount(), reads.hierarchyCount(), reads.inverseCount())
			}
			if reads.hierarchyCount() == 0 || reads.inverseCount() == 0 {
				t.Fatalf("native restore detach did not use bounded source reads: hierarchy=%d inverse=%d", reads.hierarchyCount(), reads.inverseCount())
			}
			if rows := len(persistedClosure(t, targetStore)); rows != 20049 {
				t.Fatalf("native restore detach left %d closure rows, want 20049", rows)
			}
			t.Logf("native %s restore detach indexed: incarnation=%s whole-bead queries=%d hierarchy_reads=%d inverse_reads=%d", backend, incarnation, reads.queryCount(), reads.hierarchyCount(), reads.inverseCount())
		})
	}
}

type observedWriteStore struct {
	driver.Store
	observer *readObserver
}

func (s *observedWriteStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	reader, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &observedReader{Reader: reader, observer: s.observer}, nil
}

func (s *observedWriteStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &observedWriteTx{WriteTx: tx, observer: s.observer}, nil
}

type observedWriteTx struct {
	driver.WriteTx
	observer *readObserver
}

func (t *observedWriteTx) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	t.observer.queries.Add(1)
	return t.WriteTx.Query(ctx, query)
}
func (t *observedWriteTx) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	t.observer.hierarchyChildren.Add(1)
	return t.WriteTx.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, parents, limit)
}
func (t *observedWriteTx) HierarchyChildrenExcluding(ctx context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	t.observer.hierarchyChildren.Add(1)
	return t.WriteTx.(driver.HierarchyChildrenExcludingReader).HierarchyChildrenExcluding(ctx, parents, excluded, limit)
}
func (t *observedWriteTx) InverseSourceLinks(ctx context.Context, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	t.observer.inverseSourceLinks.Add(1)
	return t.WriteTx.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, targets, kinds, excluded, limit)
}
func (t *observedWriteTx) DerivedRowsForPrefixes(ctx context.Context, table driver.DerivedTable, prefixes []string) ([]driver.DerivedRow, error) {
	return t.WriteTx.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, table, prefixes)
}
func (t *observedWriteTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	return t.WriteTx.(driver.GraphSourceWriter).PutGraphRecord(ctx, record, parent, links)
}

// Derived hierarchy state is part of a usable restore: count reads can succeed
// from source bodies even when the next real Writer refuses missing closure.
// Compare every canonical key/value before host composition and after reopen;
// the native restore owner must rebuild it atomically, never this fixture.
func persistedClosure(t *testing.T, store driver.Store) map[string]string {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	images := make(map[string]string, len(rows))
	for _, row := range rows {
		if _, _, _, err := pipeline.DecodeAncestorRow(row); err != nil {
			t.Fatalf("persisted hierarchy closure is not canonical: %v", err)
		}
		if _, exists := images[row.Key]; exists {
			t.Fatal("duplicate persisted hierarchy closure key")
		}
		images[row.Key] = hex.EncodeToString(row.Value)
	}
	return images
}

// Enumerate both the complete bead census and the exact protected ontology
// before any fixture construction can seed or normalize a restored store.
func persistedImages(t *testing.T, store driver.Store) map[string]any {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out := map[string]any{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range page.Items {
			if _, exists := out[row.Ref.ID]; exists {
				t.Fatal("duplicate persisted source identity")
			}
			var value map[string]any
			d := json.NewDecoder(bytes.NewReader(row.Body))
			d.UseNumber()
			if err := d.Decode(&value); err != nil {
				t.Fatal(err)
			}
			out[row.Ref.ID] = value
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	rows, err := r.Get(ctx, []model.Ref{ref("ontology/ep6")})
	if err != nil || len(rows) != 1 || rows[0].Kind != "ontology" {
		t.Fatalf("protected source missing from persisted census: %v", err)
	}
	var ontology map[string]any
	d := json.NewDecoder(bytes.NewReader(rows[0].Body))
	d.UseNumber()
	if err := d.Decode(&ontology); err != nil {
		t.Fatal(err)
	}
	out["ontology/ep6"] = ontology
	return out
}

func persistedAllocatorSnapshot(t *testing.T, store driver.Store) driver.AllocatorSnapshot {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	capability, ok := r.(driver.AllocatorSnapshotReader)
	if !ok {
		t.Fatal("epicprogress store lacks native allocator snapshot")
	}
	snapshot, err := capability.AllocatorSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func requireAllocatorBytes(t *testing.T, want, got driver.AllocatorSnapshot) {
	t.Helper()
	a, err := driver.EncodeAllocatorSnapshot(want)
	if err != nil {
		t.Fatal(err)
	}
	b, err := driver.EncodeAllocatorSnapshot(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("epicprogress native allocator continuation changed:\nwant %s\n got %s", a, b)
	}
}

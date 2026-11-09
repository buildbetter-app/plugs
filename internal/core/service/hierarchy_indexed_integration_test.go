package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These are real Writer/driver controls. The scan oracle opens its own reader
// at the Writer's exact pinned revision: its reads must not enlarge the actual
// Writer read set. In particular it is never installed in the absence races.
type w8ScanSnapshot struct {
	driver.Reader
	queries int
	images  int
}

func (r *w8ScanSnapshot) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.queries++
	page, err := r.Reader.Query(ctx, q)
	r.images += len(page.Items)
	return page, err
}

// Embedding the complete optional interface preserves canonical, excluding,
// inverse and closure capabilities. A SnapshotReader-only counting wrapper
// would itself force the fallback and make the indexed comparison vacuous.
type w8IndexedSnapshot struct {
	port.HierarchySnapshotReader
	mode           string
	queries        int
	membership     int
	images         int
	sources        map[string]int
	more           bool
	canonicalCalls int
	inverseCalls   int
}

func (r *w8IndexedSnapshot) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.queries++
	return r.HierarchySnapshotReader.Query(ctx, q)
}

func (r *w8IndexedSnapshot) members(rows []driver.Record) {
	r.membership++
	r.images += len(rows)
	for _, row := range rows {
		r.sources[row.Ref.ID]++
	}
}

func (r *w8IndexedSnapshot) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	return r.HierarchyChildrenExcluding(ctx, parents, nil, limit)
}

func (r *w8IndexedSnapshot) HierarchyChildrenExcluding(ctx context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.canonicalCalls++
	if r.mode == "canonical-unavailable" {
		return nil, false, driver.ErrUnsupported
	}
	// A real bounded driver page produces More. This injects a smaller
	// capability budget, without changing the production guard's 5000 limit.
	if r.mode == "bounded-page" {
		limit = min(limit, 1)
	}
	rows, more, err := r.HierarchySnapshotReader.HierarchyChildrenExcluding(ctx, parents, excluded, limit)
	r.members(rows)
	r.more = r.more || more
	return rows, more, err
}

func (r *w8IndexedSnapshot) InverseSourceLinks(ctx context.Context, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.inverseCalls++
	if r.mode == "inverse-unavailable" {
		return nil, false, driver.ErrUnsupported
	}
	if r.mode == "bounded-page" {
		limit = min(limit, 1)
	}
	rows, more, err := r.HierarchySnapshotReader.InverseSourceLinks(ctx, targets, kinds, excluded, limit)
	r.members(rows)
	r.more = r.more || more
	return rows, more, err
}

type w8DifferentialOntology struct {
	*Ontology
	t          *testing.T
	store      driver.Store
	mode       string
	calls      int
	queries    int
	images     int
	repeated   bool
	oracleRows int
}

func w8ErrorImage(t *testing.T, err error) []byte {
	t.Helper()
	if err == nil {
		return nil
	}
	var typed *model.Error
	if !errors.As(err, &typed) {
		t.Fatalf("untyped ontology result: %T %v", err, err)
	}
	return mustJSON(t, typed)
}

func (o *w8DifferentialOntology) Validate(ctx context.Context, p port.ProposedChange, reader port.SnapshotReader) error {
	o.t.Helper()
	hierarchy, ok := reader.(port.HierarchySnapshotReader)
	if !ok || reader.Snapshot().At == nil || reader.Snapshot().At.String() != p.BaseRevision.String() {
		o.t.Fatal("actual Writer did not supply the pinned hierarchy snapshot")
	}
	actual := &w8IndexedSnapshot{HierarchySnapshotReader: hierarchy, mode: o.mode, sources: map[string]int{}}
	err := o.Ontology.Validate(ctx, p, actual)
	// The oracle has no hierarchy capability, so the unmodified Ontology
	// validator takes its complete paginated scan path on a separate reader.
	raw, openErr := o.store.Read(context.Background(), reader.Snapshot())
	if openErr != nil {
		o.t.Fatal(openErr)
	}
	defer raw.Close()
	if !bytes.Equal(mustJSON(o.t, raw.Snapshot()), mustJSON(o.t, reader.Snapshot())) {
		o.t.Fatal("oracle and Writer snapshot identities differ")
	}
	oracle := &w8ScanSnapshot{Reader: raw}
	oracleErr := o.Ontology.Validate(context.Background(), p, oracle)
	if !bytes.Equal(w8ErrorImage(o.t, err), w8ErrorImage(o.t, oracleErr)) {
		o.t.Fatalf("indexed/oracle full outcome differs: indexed=%v oracle=%v", err, oracleErr)
	}
	if oracle.queries == 0 || oracle.images == 0 {
		o.t.Fatal("complete graph oracle did not execute")
	}
	if o.mode == "" && actual.queries != 0 {
		o.t.Fatalf("certified indexed validation performed %d full graph queries", actual.queries)
	}
	if o.mode != "" && actual.queries == 0 {
		o.t.Fatal("unavailable/exhausted membership did not execute the conservative scan fallback")
	}
	if o.mode == "bounded-page" && !actual.more {
		o.t.Fatal("bounded fallback did not observe a real driver More page")
	}
	if o.mode == "inverse-unavailable" && (actual.canonicalCalls == 0 || actual.membership == 0 || actual.inverseCalls == 0) {
		o.t.Fatal("inverse-only injected failure did not execute canonical membership first")
	}
	o.calls++
	o.queries += actual.queries
	o.images += actual.images
	o.repeated = o.repeated || actual.images > len(actual.sources)
	o.oracleRows += oracle.images
	return err
}

func w8Create(t *testing.T, f *recoveryFixture, title string, parent *model.Ref, project string) model.Ref {
	t.Helper()
	key := meta(t)
	if project != "" {
		key.Project = project
	}
	commit, err := f.svc.Create(context.Background(), anonymous, CreateRequest{Type: "issue", Parent: parent, Fields: map[string]any{"title": title}}, key)
	if err != nil || len(commit.Results) != 1 {
		t.Fatalf("production create %s: %v", title, err)
	}
	return commit.Results[0].Ref
}

func w8Link(t *testing.T, f *recoveryFixture, from, to model.Ref, kind string, metadata *model.LinkMetadata) {
	t.Helper()
	if _, err := f.svc.Link(context.Background(), anonymous, LinkRequest{From: from, To: to, Kind: kind, Metadata: metadata}, meta(t)); err != nil {
		t.Fatal(err)
	}
}

func w8Slot(t *testing.T, f *recoveryFixture, ref model.Ref, method, project string) {
	t.Helper()
	key := meta(t)
	key.Method = method
	if project != "" {
		key.Project = project
	}
	if _, err := f.writer.Apply(context.Background(), anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{}}}}}, key); err != nil {
		t.Fatal(err)
	}
}

func w8InstallOntology(f *recoveryFixture, o *Ontology) {
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return f.now }))
	f.svc = New(f.store, f.lease, f.writer, WithOntology(o), WithClock(func() time.Time { return f.now }))
}

type w8DurableState struct {
	Stored     i1State
	Meta       driver.Meta
	Derived    map[driver.DerivedTable][]driver.DerivedRow
	Predicates []driver.LockState
}

func w8Capture(t *testing.T, f *recoveryFixture, key model.WriteMeta) w8DurableState {
	t.Helper()
	ctx := context.Background()
	result := w8DurableState{Stored: i1StoredState(t, f, key), Derived: map[driver.DerivedTable][]driver.DerivedRow{}}
	var err error
	result.Meta, err = f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(ctx, result.Stored.View)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	keys := map[string]driver.LockKey{}
	add := func(k driver.LockKey) {
		encoded, err := driver.EncodeLockKey(k)
		if err != nil {
			t.Fatal(err)
		}
		keys[encoded] = k
	}
	for _, k := range []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock} {
		add(k)
	}
	for _, row := range result.Stored.Records {
		ref := row.Ref
		add(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		for _, constructor := range []func(string) (driver.Sentinel, error){driver.NewBeadSentinel, driver.NewClosureSentinel} {
			sentinel, err := constructor(ref.ID)
			if err != nil {
				t.Fatal(err)
			}
			add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		}
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		result.Derived[table] = rows
		for _, row := range rows {
			add(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}})
		}
	}
	encoded := make([]string, 0, len(keys))
	for key := range keys {
		encoded = append(encoded, key)
	}
	sort.Strings(encoded)
	ordered := make([]driver.LockKey, 0, len(keys))
	for _, key := range encoded {
		ordered = append(ordered, keys[key])
	}
	result.Predicates, err = r.Observe(ctx, ordered)
	if err != nil || len(result.Predicates) != len(ordered) {
		t.Fatalf("incomplete durable predicates: %v", err)
	}
	return result
}

func w8SameState(t *testing.T, before, after w8DurableState) {
	t.Helper()
	if !bytes.Equal(mustJSON(t, before), mustJSON(t, after)) {
		t.Fatal("refusal/replay changed records, versions, log/events, head, receipt, predicates or one of nine derived tables")
	}
}

func w8AssertLifecycleCommit(t *testing.T, f *recoveryFixture, key model.WriteMeta, before w8DurableState, target model.Ref, method string, out port.WriteOutcome) w8DurableState {
	t.Helper()
	after := w8Capture(t, f, key)
	if out.Replayed || len(out.Commit.Results) != 1 || !sameResolvedRef(out.Commit.Results[0].Ref, target) || out.Commit.Results[0].Revision.String() != out.Commit.Revision.String() || after.Meta.Head.String() != out.Commit.Revision.String() || after.Meta.Head.String() == before.Meta.Head.String() {
		t.Fatal("lifecycle result/target/current head/commit revisions do not identify one new write")
	}
	if after.Stored.Receipt == nil || after.Stored.Receipt.ResponsePending || after.Stored.Receipt.RequestHash == "" || after.Stored.Receipt.Changeset.Revision.String() != out.Commit.Revision.String() || !bytes.Equal(mustJSON(t, after.Stored.Receipt.Commit), mustJSON(t, out.Commit)) {
		t.Fatal("matching request scope did not retain the finalized exact commit receipt")
	}
	if len(after.Stored.Log) != len(before.Stored.Log)+1 || len(after.Stored.Records) != len(before.Stored.Records) {
		t.Fatal("lifecycle did not append exactly one changeset while preserving the corpus")
	}
	entry := after.Stored.Log[len(after.Stored.Log)-1]
	if entry.Revision.String() != out.Commit.Revision.String() || len(entry.Changes) != 1 || !sameResolvedRef(entry.Changes[0].Ref, target) {
		t.Fatal("new log entry does not identify exactly the requested target")
	}
	previous := map[string]driver.Record{}
	for _, row := range before.Stored.Records {
		previous[row.Ref.ID] = row
	}
	found := 0
	for _, row := range after.Stored.Records {
		prior, exists := previous[row.Ref.ID]
		if !exists {
			t.Fatal("lifecycle introduced an unexpected row")
		}
		if row.Ref.ID != target.ID {
			if !bytes.Equal(mustJSON(t, prior), mustJSON(t, row)) || !bytes.Equal(mustJSON(t, before.Stored.Versions[row.Ref.ID]), mustJSON(t, after.Stored.Versions[row.Ref.ID])) {
				t.Fatal("lifecycle changed a control row or its history")
			}
			continue
		}
		found++
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			t.Fatal(err)
		}
		slot := ontology.Close
		if method == "bead.claim" {
			slot = ontology.Claim
		}
		status, ok := f.svc.ontology.effective(doc.Project).SlotStatus(doc.Type, slot)
		if !ok || !sameResolvedRef(row.Ref, target) || doc.ID != target.ID || doc.Fields["status"] != status || row.Revision.String() != out.Commit.Revision.String() || !bytes.Equal(row.Body, out.Commit.Results[0].Record) {
			t.Fatal("current target status/body/result/revision is wrong")
		}
		if slot == ontology.Claim {
			if doc.Lease == nil || doc.Lease.Principal.Kind != anonymous.Kind || doc.Lease.Principal.ID != anonymous.ID || doc.Lease.Agent != key.AgentLabel || !doc.Lease.ExpiresAt.Equal(f.now.Add(5*time.Minute)) || doc.Lease.TtlMs.String() != "300000" || doc.Lease.Epoch.String() != model.Natural64(f.lease.Epoch()).String() || out.Commit.LeaseExpiresAt == nil || !out.Commit.LeaseExpiresAt.Equal(doc.Lease.ExpiresAt) {
				t.Fatal("current claim lost its actual bounded holder lease")
			}
		} else if doc.Lease != nil {
			t.Fatal("close retained a live lease")
		}
		versions := after.Stored.Versions[target.ID]
		if len(versions) != len(before.Stored.Versions[target.ID])+1 || versions[len(versions)-1].Revision.String() != out.Commit.Revision.String() || versions[len(versions)-1].Deleted {
			t.Fatal("target did not append one live version at the commit revision")
		}
	}
	if found != 1 {
		t.Fatal("lifecycle current target is absent or duplicated")
	}
	return after
}

func TestIndexedOntologyRealWriterMatchesCompleteGraph(t *testing.T) {
	type scenario struct {
		name     string
		method   string
		code     model.ErrorCode
		repeated bool
		prepare  func(*testing.T, *recoveryFixture) model.Ref
	}
	var cases []scenario
	for _, kind := range []string{"blocks", "conditional-blocks"} {
		for _, slot := range []string{"open", "bead.close", "bead.fail"} {
			code := model.ErrorCode("ontology_guard_failed")
			if kind == "blocks" && (slot == "bead.close" || slot == "bead.fail") || kind == "conditional-blocks" && slot == "bead.fail" {
				code = ""
			}
			cases = append(cases, scenario{name: kind + "/" + slot, method: "bead.claim", code: code, prepare: func(t *testing.T, f *recoveryFixture) model.Ref {
				blocker := w8Create(t, f, "blocker", nil, "")
				target := w8Create(t, f, "dependent", nil, "")
				w8Link(t, f, target, blocker, kind, nil)
				if slot != "open" {
					w8Slot(t, f, blocker, slot, "")
				}
				return target
			}})
		}
	}
	for _, membership := range []string{"canonical", "legacy", "both"} {
		for _, terminal := range []bool{false, true} {
			code := model.ErrorCode("ontology_guard_failed")
			if terminal {
				code = ""
			}
			cases = append(cases, scenario{name: fmt.Sprintf("children/%s/terminal=%t", membership, terminal), method: "bead.close", code: code, repeated: membership == "both", prepare: func(t *testing.T, f *recoveryFixture) model.Ref {
				parent := w8Create(t, f, "parent", nil, "")
				var canonical *model.Ref
				if membership != "legacy" {
					canonical = &parent
				}
				child := w8Create(t, f, "child", canonical, "")
				if membership != "canonical" {
					w8Link(t, f, child, parent, "parent-child", nil)
				}
				if terminal {
					w8Slot(t, f, child, "bead.close", "")
				}
				return parent
			}})
		}
	}
	for _, gate := range []string{"nil", "all-children", "any-children", "also_blocks"} {
		for _, terminalChild := range []bool{false, true} {
			code := model.ErrorCode("ontology_guard_failed")
			// The second child stays open. Only explicit any-children can
			// accept one terminal child; all/nil still require all children.
			if gate == "any-children" && terminalChild {
				code = ""
			}
			cases = append(cases, scenario{name: fmt.Sprintf("waits-for/%s/one-terminal=%t", gate, terminalChild), method: "bead.claim", code: code, prepare: func(t *testing.T, f *recoveryFixture) model.Ref {
				spawner := w8Create(t, f, "spawner", nil, "")
				first := w8Create(t, f, "first child", &spawner, "")
				w8Create(t, f, "second child", &spawner, "")
				if terminalChild {
					w8Slot(t, f, first, "bead.close", "")
				}
				waiter := w8Create(t, f, "waiter", nil, "")
				var metadata *model.LinkMetadata
				if gate != "nil" {
					metadata = &model.LinkMetadata{Gate: gate}
					if gate == "also_blocks" {
						metadata = &model.LinkMetadata{Gate: "any-children", AlsoBlocks: true}
					}
				}
				w8Link(t, f, waiter, spawner, "waits-for", metadata)
				return waiter
			}})
		}
	}
	for _, blocked := range []bool{false, true} {
		code := model.ErrorCode("")
		if blocked {
			code = "ontology_guard_failed"
		}
		cases = append(cases, scenario{name: fmt.Sprintf("ancestor/blocker=%t", blocked), method: "bead.claim", code: code, prepare: func(t *testing.T, f *recoveryFixture) model.Ref {
			root := w8Create(t, f, "ancestor", nil, "")
			parent := w8Create(t, f, "parent", &root, "")
			leaf := w8Create(t, f, "leaf", &parent, "")
			if blocked {
				w8Link(t, f, root, w8Create(t, f, "ancestor blocker", nil, ""), "blocks", nil)
			}
			return leaf
		}})
	}
	for _, gate := range []string{"nil", "all-children", "any-children", "also_blocks"} {
		for _, terminalSpawner := range []bool{false, true} {
			code := model.ErrorCode("")
			if gate == "also_blocks" && !terminalSpawner {
				code = "ontology_guard_failed"
			}
			cases = append(cases, scenario{name: fmt.Sprintf("waits-for-empty/%s/spawner-terminal=%t", gate, terminalSpawner), method: "bead.claim", code: code, prepare: func(t *testing.T, f *recoveryFixture) model.Ref {
				spawner := w8Create(t, f, "empty spawner", nil, "")
				if terminalSpawner {
					w8Slot(t, f, spawner, "bead.close", "")
				}
				waiter := w8Create(t, f, "waiter", nil, "")
				var metadata *model.LinkMetadata
				if gate != "nil" {
					metadata = &model.LinkMetadata{Gate: gate}
					if gate == "also_blocks" {
						metadata = &model.LinkMetadata{Gate: "all-children", AlsoBlocks: true}
					}
				}
				w8Link(t, f, waiter, spawner, "waits-for", metadata)
				return waiter
			}})
		}
	}
	for _, method := range []string{"bead.close", "bead.fail"} {
		code := model.ErrorCode("ontology_guard_failed")
		if method == "bead.fail" {
			code = ""
		}
		cases = append(cases, scenario{name: "blocker-own-project/" + method, method: "bead.claim", code: code, prepare: func(t *testing.T, f *recoveryFixture) model.Ref {
			doc, base := ontology.DefaultDocument(), ontology.Default()
			closeStatus, _ := base.SlotStatus("issue", ontology.Close)
			failStatus, _ := base.SlotStatus("issue", ontology.Fail)
			closed, failed := doc.Statuses[closeStatus], doc.Statuses[failStatus]
			closed.Outcome, failed.Outcome = ontology.Failure, ontology.Success
			doc.Statuses[closeStatus], doc.Statuses[failStatus] = closed, failed
			workflow := doc.Workflows["issue"]
			workflow.Slots[ontology.Close], workflow.Slots[ontology.Fail] = failStatus, closeStatus
			doc.Workflows["issue"] = workflow
			compiled, problems := ontology.Compile(doc, nil, nil)
			if len(problems) != 0 {
				t.Fatal(problems)
			}
			o := NewOntology(func(project string) ontology.Effective {
				if project == "upstream" {
					return compiled.Effective(project)
				}
				return base
			})
			w8InstallOntology(f, o)
			blocker := w8Create(t, f, "custom outcome blocker", nil, "upstream")
			target := w8Create(t, f, "local dependent", nil, "")
			w8Link(t, f, target, blocker, "conditional-blocks", nil)
			w8Slot(t, f, blocker, method, "upstream")
			return target
		}})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
				target := tc.prepare(t, f)
				o := &w8DifferentialOntology{Ontology: f.svc.ontology, t: t, store: f.store}
				writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return f.now }))
				svc := New(f.store, f.lease, writer, WithOntology(o.Ontology), WithClock(func() time.Time { return f.now }))
				key := meta(t)
				key.Method = tc.method
				before := w8Capture(t, f, key)
				var out port.WriteOutcome
				var err error
				if tc.method == "bead.claim" {
					out, err = svc.ClaimWithOutcome(context.Background(), anonymous, ClaimRequest{ID: target}, key)
				} else {
					out, err = svc.CloseWithOutcome(context.Background(), anonymous, CloseRequest{ID: target}, key)
				}
				if tc.code != "" {
					wantCode(t, err, tc.code)
					w8SameState(t, before, w8Capture(t, f, key))
				} else {
					if err != nil {
						t.Fatal(err)
					}
					w8AssertLifecycleCommit(t, f, key, before, target, tc.method, out)
				}
				if o.calls != 1 || o.queries != 0 || o.oracleRows == 0 || tc.repeated && !o.repeated {
					t.Fatalf("nonvacuous differential calls=%d actualQueries=%d membershipImages=%d repeated=%t oracleRows=%d", o.calls, o.queries, o.images, o.repeated, o.oracleRows)
				}
			})
		})
	}
}

func TestIndexedOntologyRealFallbackPreservesRefusalAndAllow(t *testing.T) {
	// Unavailable modes inject independent capability failures; they do not
	// claim that these production-published stores are actually unbuilt.
	for _, mode := range []string{"canonical-unavailable", "inverse-unavailable", "bounded-page"} {
		for _, terminal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/terminal=%t", mode, terminal), func(t *testing.T) {
				recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
					parent := w8Create(t, f, "parent", nil, "")
					for i := 0; i < 2; i++ {
						child := w8Create(t, f, fmt.Sprintf("child %d", i), &parent, "")
						if terminal {
							w8Slot(t, f, child, "bead.close", "")
						}
					}
					o := &w8DifferentialOntology{Ontology: f.svc.ontology, t: t, store: f.store, mode: mode}
					writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(func() time.Time { return f.now }))
					svc := New(f.store, f.lease, writer, WithOntology(o.Ontology), WithClock(func() time.Time { return f.now }))
					key := meta(t)
					key.Method = "bead.close"
					before := w8Capture(t, f, key)
					out, err := svc.CloseWithOutcome(context.Background(), anonymous, CloseRequest{ID: parent}, key)
					if terminal {
						if err != nil {
							t.Fatal(err)
						}
						w8AssertLifecycleCommit(t, f, key, before, parent, "bead.close", out)
					} else {
						wantCode(t, err, "ontology_guard_failed")
						w8SameState(t, before, w8Capture(t, f, key))
					}
					if o.calls != 1 || o.queries == 0 {
						t.Fatal("real fallback validation was not exercised")
					}
				})
			})
		}
	}
}

// The stale attempt only needs Lock; Head must never be reached. The corrected
// positive and replay controls use the original concrete store, so this narrow
// spy cannot hide an optional publication, allocator or checkpoint capability.
type w8BeforeHeadStore struct {
	driver.Store
	driver.ResponseFinalizer
	driver.CheckpointStore
	writes atomic.Int32
	heads  atomic.Int32
}

func (s *w8BeforeHeadStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes.Add(1)
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &w8BeforeHeadTx{WriteTx: tx, heads: &s.heads}, nil
}

type w8BeforeHeadTx struct {
	driver.WriteTx
	heads *atomic.Int32
}

type w8Post struct{ calls atomic.Int32 }

func (p *w8Post) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	p.calls.Add(1)
	return nil
}

// No scan oracle is called here. This records only the membership calls made
// by the real guard; an additional test read would mask a missing read-set
// observation and could turn a broken absent-membership guard into a pass.
type w8AbsenceOntology struct {
	*Ontology
	t                      *testing.T
	calls, empty, occupied int
}

func (o *w8AbsenceOntology) Validate(ctx context.Context, p port.ProposedChange, reader port.SnapshotReader) error {
	hierarchy, ok := reader.(port.HierarchySnapshotReader)
	if !ok {
		o.t.Fatal("absence guard lost indexed snapshot capability")
	}
	indexed := &w8IndexedSnapshot{HierarchySnapshotReader: hierarchy, sources: map[string]int{}}
	err := o.Ontology.Validate(ctx, p, indexed)
	o.calls++
	if indexed.queries != 0 || indexed.membership == 0 {
		o.t.Fatal("absence probe did not execute indexed-only membership")
	}
	if indexed.images == 0 {
		o.empty++
	} else {
		o.occupied++
	}
	return err
}

func (tx *w8BeforeHeadTx) Head(ctx context.Context) (model.Revision, error) {
	tx.heads.Add(1)
	return tx.WriteTx.Head(ctx)
}

func TestIndexedOntologyRetriesAbsentChildBeforeHead(t *testing.T) {
	for _, membership := range []string{"canonical", "legacy"} {
		t.Run(membership, func(t *testing.T) {
			recoveryStores(t, func(t *testing.T, f *recoveryFixture) {
				ctx := context.Background()
				spawner := w8Create(t, f, "empty spawner", nil, "")
				waiter := w8Create(t, f, "waiter", nil, "")
				w8Link(t, f, waiter, spawner, "waits-for", &model.LinkMetadata{Gate: "all-children"})
				key := meta(t)
				key.Method = "bead.claim"
				var calls atomic.Int32
				var child model.Ref
				var interfered w8DurableState
				post := &w8Post{}
				probe := &w8AbsenceOntology{Ontology: f.svc.ontology, t: t}
				spy := &w8BeforeHeadStore{Store: f.store, ResponseFinalizer: f.store.(driver.ResponseFinalizer), CheckpointStore: f.store.(driver.CheckpointStore)}
				writer := pipeline.New(spy, f.lease, pipeline.WithOntology(probe), pipeline.WithClock(func() time.Time { return f.now }),
					pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "observe-real-post", Subscriber: post}),
					pipeline.WithValidators(pipeline.ValidatorBinding{ID: "insert-previously-absent-child", Validator: recoveryValidator(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
						if calls.Add(1) != 1 {
							return port.Verdict{}, fmt.Errorf("stale proposal reached validator again")
						}
						// Use the independent host context/Writer, after the indexed
						// guard allowed an empty relation, before outer Lock/Head.
						var parent *model.Ref
						if membership == "canonical" {
							parent = &spawner
						}
						child = w8Create(t, f, "concurrent new child", parent, "")
						if membership == "legacy" {
							w8Link(t, f, child, spawner, "parent-child", nil)
						}
						interfered = w8Capture(t, f, key)
						return port.Verdict{Allow: true}, nil
					})}))
				guarded := New(f.store, f.lease, writer, WithOntology(f.svc.ontology), WithClock(func() time.Time { return f.now }))
				_, err := guarded.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: waiter}, key)
				wantCode(t, err, "ontology_guard_failed")
				if calls.Load() != 1 || spy.writes.Load() != 1 || spy.heads.Load() != 0 || interfered.Stored.Receipt != nil || post.calls.Load() != 0 || probe.calls != 2 || probe.empty != 1 || probe.occupied != 1 {
					t.Fatalf("absent membership did not invalidate tracked locks before Head: validators=%d tx=%d heads=%d", calls.Load(), spy.writes.Load(), spy.heads.Load())
				}
				w8SameState(t, interfered, w8Capture(t, f, key))
				w8Slot(t, f, child, "bead.close", "")
				// The successful control uses the concrete production driver and
				// all its optional transaction capabilities, with the same post.
				before := w8Capture(t, f, key)
				positiveWriter := pipeline.New(f.store, f.lease, pipeline.WithOntology(f.svc.ontology), pipeline.WithClock(func() time.Time { return f.now }), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "observe-real-post", Subscriber: post}))
				positive := New(f.store, f.lease, positiveWriter, WithOntology(f.svc.ontology), WithClock(func() time.Time { return f.now }))
				out, err := positive.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: waiter}, key)
				if err != nil || out.Replayed || out.Commit.LeaseExpiresAt == nil {
					t.Fatalf("refused-key corrected claim: %+v %v", out, err)
				}
				committed := w8Capture(t, f, key)
				if committed.Stored.Receipt == nil || committed.Stored.Receipt.ResponsePending || post.calls.Load() != 1 || len(committed.Stored.Log) != len(before.Stored.Log)+1 || len(committed.Stored.Versions[waiter.ID]) != len(before.Stored.Versions[waiter.ID])+1 {
					t.Fatal("claim receipt missing or pending")
				}
				found := 0
				for _, row := range committed.Stored.Records {
					if row.Ref.ID != waiter.ID {
						continue
					}
					found++
					doc, err := pipeline.DecodeDocument(row.Body)
					claimStatus, _ := f.svc.ontology.effective("local").SlotStatus("issue", ontology.Claim)
					if err != nil || doc.Fields["status"] != claimStatus || doc.Lease == nil || row.Revision.String() != out.Commit.Revision.String() {
						t.Fatal("successful claim's current row/body/revision is wrong")
					}
				}
				if found != 1 {
					t.Fatal("successful claim's current row is absent or duplicated")
				}
				replay, err := positive.ClaimWithOutcome(ctx, anonymous, ClaimRequest{ID: waiter}, key)
				if err != nil || !replay.Replayed || !bytes.Equal(mustJSON(t, out.Commit), mustJSON(t, replay.Commit)) {
					t.Fatalf("corrected claim exact replay: %v", err)
				}
				w8SameState(t, committed, w8Capture(t, f, key))
				if post.calls.Load() != 1 {
					t.Fatal("exact replay reran post")
				}
				t.Log("W8 absent child PASS: canonical/legacy insertion changes the retried guard; stale attempt stops before Head; no target write or receipt; same key commits once and replays")
			})
		})
	}
}

package batchsql_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The profile deliberately omits Children. The optional restored records have
// a direct .8 and a grandchild .8.2, independently of the stored counter.
func seedChildAllocatorFixture(t *testing.T, f *fixture, profile driver.OfflineAllocator, observed bool) {
	t.Helper()
	if profile.Children != nil || profile.Validate() != nil {
		t.Fatal("fixture must be a valid profile with omitted child counters")
	}
	seedChildFixture(t, f, &profile, observed)
}

func seedChildFixture(t *testing.T, f *fixture, profile *driver.OfflineAllocator, observed bool, ids ...string) {
	t.Helper()
	seedChildSourceFixture(t, f, profile, observed, false, ids...)
}

func seedChildSourceFixture(t *testing.T, f *fixture, profile *driver.OfflineAllocator, observed, typed bool, ids ...string) {
	t.Helper()
	var locks driver.LockSet
	if profile != nil {
		key, err := driver.AllocatorLock(profile.Prefix)
		if err != nil {
			t.Fatal(err)
		}
		locks = append(locks, driver.LockRequest{Key: key, Mode: driver.LockUpdate})
	}
	var records []driver.Record
	var rows []driver.DerivedRow
	var sentinels []driver.Sentinel
	if typed {
		locks = append(locks, driver.LockRequest{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate})
		sentinels = append(sentinels, driver.SentinelGraphBlocking)
	}
	if observed {
		if len(ids) == 0 {
			ids = []string{"demo-root", "demo-root.8", "demo-root.8.2"}
		}
		for i, id := range ids {
			doc := pipeline.Document{ID: id, Type: "issue", Project: "demo", Fields: map[string]any{"title": id, "status": "todo"}}
			if i > 0 {
				parent := model.Ref{ID: ids[i-1]}
				doc.Parent = &parent
			}
			body, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			ref := model.Ref{ID: id}
			records = append(records, driver.Record{Ref: ref, Kind: "bead", Body: body})
			locks = append(locks, driver.LockRequest{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate})
			sentinel, err := driver.NewClosureSentinel(id)
			if err != nil {
				t.Fatal(err)
			}
			sentinels = append(sentinels, sentinel)
			beadSentinel, err := driver.NewBeadSentinel(id)
			if err != nil {
				t.Fatal(err)
			}
			sentinels = append(sentinels, beadSentinel)
			locks = append(locks, driver.LockRequest{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: beadSentinel}, Mode: driver.LockUpdate})
			locks = append(locks, driver.LockRequest{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}, Mode: driver.LockUpdate})
			for j := i; j >= 0; j-- {
				row, err := pipeline.AncestorRow(id, ids[j], uint64(i-j))
				if err != nil {
					t.Fatal(err)
				}
				rows = append(rows, row)
				locks = append(locks, driver.LockRequest{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}}, Mode: driver.LockUpdate})
			}
		}
	}
	sort.Slice(locks, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(locks[i].Key)
		b, _ := driver.EncodeLockKey(locks[j].Key)
		return a < b
	})
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	tx, err := f.store.Write(f.ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(f.ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(f.ctx); err != nil {
		t.Fatal(err)
	}
	meta, err := tx.(driver.OfflineRestoreTx).PrepareOfflineRestore(f.ctx, "child-allocator-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if profile != nil {
		if err = tx.(driver.AllocatorContinuation).RestoreAllocators(f.ctx, []driver.OfflineAllocator{*profile}); err != nil {
			t.Fatal(err)
		}
	}
	for _, record := range records {
		if typed {
			err = pipeline.PutSourceRecord(f.ctx, tx, record)
		} else {
			err = tx.Put(f.ctx, record)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(rows) > 0 {
		if err = tx.PutDerived(f.ctx, driver.DerivedAncestorClosure, nil, rows); err != nil {
			t.Fatal(err)
		}
		if err = tx.BumpSentinels(f.ctx, sentinels); err != nil {
			t.Fatal(err)
		}
	}
	changes := make([]model.ChangeRecord, len(records))
	for i, record := range records {
		changes[i] = model.ChangeRecord{Ref: record.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0)}
	}
	keys := make([]driver.LockKey, len(locks))
	for i := range locks {
		keys[i] = locks[i].Key
	}
	if _, err = tx.Commit(f.ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: model.PrincipalRef{Kind: "system", ID: "plug-converter"}, Method: "offline.restore", IdempotencyKey: "child-allocator-seed", RequestHash: strings.Repeat("a", 64), Changes: changes, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
}

// A restored fixture has an explicit floor. Observe all available live data
// and the complete log above that floor without requesting discarded history.
func childAllocatorDurableState(t *testing.T, f *fixture) durableState {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("complete child fixture query: %+v %v", page, err)
	}
	state := durableState{Meta: meta, Records: map[string]driver.Record{}, Versions: map[string][]driver.Version{}}
	for _, record := range page.Items {
		state.Records[record.Ref.ID] = record
		versions, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("complete child fixture versions: %+v %v", versions, err)
		}
		state.Versions[record.Ref.ID] = versions.Items
	}
	var cursor *driver.Cursor
	if !meta.HistoryFloor.IsZero() {
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor, Method: "changesets"}
	}
	state.Log, err = r.Changesets(f.ctx, cursor, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func childAllocatorState(t *testing.T, f *fixture) driver.OfflineAllocator {
	t.Helper()
	r := hierarchyReader(t, f)
	defer r.Close()
	state, err := r.(driver.AllocatorReader).LoadAllocator(f.ctx, "demo")
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// Recover only to report each engine/profile's reachable panic and inspect
// atomicity; a panic is always a test failure, never a successful refusal.
func childAllocatorApply(f *fixture, cs model.ChangeSet, meta model.WriteMeta) (out port.WriteOutcome, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("valid child create panicked: %v", value)
		}
	}()
	return f.writer.ApplyWithOutcome(f.ctx, batchPrincipal, cs, meta)
}

func assertChildAllocatorReplay(t *testing.T, f *fixture, cs model.ChangeSet, meta model.WriteMeta, commit model.Commit) {
	t.Helper()
	before, rows, inventory := childAllocatorDurableState(t, f), hierarchyRows(t, f), childInventory(t, f)
	got, err := childAllocatorApply(f, cs, meta)
	if err != nil || !got.Replayed || !reflect.DeepEqual(got.Commit, commit) {
		t.Fatalf("exact child replay: %+v err=%v want=%+v", got, err, commit)
	}
	requireSameState(t, before, childAllocatorDurableState(t, f))
	if !reflect.DeepEqual(rows, hierarchyRows(t, f)) {
		t.Fatal("replay changed closure rows")
	}
	if !reflect.DeepEqual(inventory, childInventory(t, f)) {
		t.Fatal("replay consumed allocator continuation")
	}
	if receipt := f.receipt(t, meta); receipt == nil || !reflect.DeepEqual(receipt.Commit, commit) {
		t.Fatalf("child receipt differs: %+v", receipt)
	}
}

func TestChildAllocatorOmittedCountersAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, mode := range []string{driver.AllocatorSequential, driver.AllocatorHash} {
				t.Run(mode, func(t *testing.T) {
					for _, observed := range []bool{false, true} {
						name := "same-parent-batch"
						if observed {
							name = "restored-direct-child"
						}
						t.Run(name, func(t *testing.T) {
							f := newFixture(t, engine)
							profile := driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential}
							if mode == driver.AllocatorHash {
								profile = driver.NewHashAllocator("demo", 3, 8, 0.25)
							}
							seedChildAllocatorFixture(t, f, profile, observed)
							parent, start := model.Ref{ID: "demo-root"}, 1
							if !observed {
								root := hierarchyApply(t, f, hierarchyCreate("parent", nil, "demo"))
								parent = root.Results[0].Ref
							} else {
								start = 9
							}
							if childAllocatorState(t, f).Children != nil {
								t.Fatal("fixture did not retain omitted counters")
							}
							cs := model.ChangeSet{Ops: []model.Op{hierarchyCreate("first", &parent, "demo"), hierarchyCreate("second", &parent, "demo"), hierarchyCreate("third", &parent, "demo")}}
							meta := f.meta("omitted-counters")
							before, rows := childAllocatorDurableState(t, f), hierarchyRows(t, f)
							out, err := childAllocatorApply(f, cs, meta)
							if err != nil {
								requireSameState(t, before, childAllocatorDurableState(t, f))
								if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || f.receipt(t, meta) != nil || childAllocatorState(t, f).Children != nil {
									t.Fatal("failed allocation partially published")
								}
								t.Fatal(err)
							}
							if out.Replayed || len(out.Commit.Results) != 3 {
								t.Fatalf("batch result=%+v", out)
							}
							for i, result := range out.Commit.Results {
								if result.Ref.ID != fmt.Sprintf("%s.%d", parent.ID, start+i) || result.Revision.String() != out.Commit.Revision.String() {
									t.Fatalf("child %d=%+v", i, result)
								}
							}
							if got := childAllocatorState(t, f); got.Children[parent.ID] != uint64(start+2) {
								t.Fatalf("durable child high-water=%+v", got)
							}
							verifyHierarchy(t, f)
							assertChildAllocatorReplay(t, f, cs, meta, out.Commit)
							if engine != "memory" {
								f.reopen(t)
								assertChildAllocatorReplay(t, f, cs, meta, out.Commit)
							}
							before, rows = childAllocatorDurableState(t, f), hierarchyRows(t, f)
							counter := childAllocatorState(t, f)
							badMeta := f.meta("late-failure")
							bad := model.ChangeSet{Ops: []model.Op{hierarchyCreate("rollback-a", &parent, "demo"), hierarchyCreate("rollback-b", &parent, "demo"), patchOp(model.Ref{ID: "missing-late"}, map[string]any{"title": "refuse"})}}
							_, err = childAllocatorApply(f, bad, badMeta)
							requireCode(t, err, "not_found")
							requireSameState(t, before, childAllocatorDurableState(t, f))
							if !reflect.DeepEqual(counter, childAllocatorState(t, f)) || !reflect.DeepEqual(rows, hierarchyRows(t, f)) || f.receipt(t, badMeta) != nil {
								t.Fatal("late refusal consumed counter, closure or receipt")
							}
							next := hierarchyApply(t, f, hierarchyCreate("continued", &parent, "demo"))
							if next.Results[0].Ref.ID != fmt.Sprintf("%s.%d", parent.ID, start+3) {
								t.Fatalf("continuation skipped or reused number: %+v", next)
							}
							t.Logf("%s/%s/%s: three exact IDs, replay, atomic refusal and durable continuation proven", engine, mode, name)
						})
					}
				})
			}
		})
	}
}

func childDelete(ref model.Ref) model.Op {
	return model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: ref}}
}

func TestChildAllocatorNoProfileNeverReusesDeletedNumberAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, project := range []string{"", "batch-local"} {
				name := "project-absent"
				if project != "" {
					name = "project-without-profile"
				}
				t.Run(name, func(t *testing.T) {
					f := newFixture(t, engine)
					rootMeta := f.meta("parent")
					rootMeta.Project = project
					root, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyCreate("parent", nil, project)}}, rootMeta)
					if err != nil {
						t.Fatal(err)
					}
					parent := root.Results[0].Ref
					if parent.ID != fmt.Sprintf("pl-batch-%s-001", engine) {
						t.Fatalf("top-level fallback changed: %+v", parent)
					}
					childCS := model.ChangeSet{Ops: []model.Op{hierarchyCreate("child", &parent, project)}}
					childMeta := f.meta("child")
					childMeta.Project = project
					first, err := childAllocatorApply(f, childCS, childMeta)
					if err != nil || len(first.Commit.Results) != 1 || first.Commit.Results[0].Ref.ID != parent.ID+".1" {
						t.Fatalf("first child=%+v err=%v", first, err)
					}
					child := first.Commit.Results[0].Ref
					assertChildAllocatorReplay(t, f, childCS, childMeta, first.Commit)
					hierarchyApply(t, f, childDelete(child))
					r := hierarchyReader(t, f)
					versions, err := r.Versions(f.ctx, child, driver.PageReq{Limit: 10})
					r.Close()
					if err != nil || len(versions.Items) != 2 || !versions.Items[1].Deleted {
						t.Fatalf("deletion did not retain tombstone: %+v err=%v", versions, err)
					}
					if engine != "memory" {
						f.reopen(t)
						assertChildAllocatorReplay(t, f, childCS, childMeta, first.Commit)
					}
					nextMeta := f.meta("after-deletion")
					nextMeta.Project = project
					next, err := childAllocatorApply(f, childCS, nextMeta)
					if err != nil || len(next.Commit.Results) != 1 || next.Commit.Results[0].Ref.ID != parent.ID+".2" {
						t.Fatalf("consumed child number reused after deletion/reopen: got=%+v err=%v want=%s.2", next, err, parent.ID)
					}
					assertChildAllocatorReplay(t, f, childCS, nextMeta, next.Commit)
					before, rows := childAllocatorDurableState(t, f), hierarchyRows(t, f)
					badMeta := f.meta("rollback")
					badMeta.Project = project
					bad := model.ChangeSet{Ops: []model.Op{hierarchyCreate("a", &parent, project), hierarchyCreate("b", &parent, project), patchOp(model.Ref{ID: "missing-late"}, map[string]any{"title": "refuse"})}}
					_, err = childAllocatorApply(f, bad, badMeta)
					requireCode(t, err, "not_found")
					requireSameState(t, before, childAllocatorDurableState(t, f))
					if f.receipt(t, badMeta) != nil || !reflect.DeepEqual(rows, hierarchyRows(t, f)) {
						t.Fatal("late refusal partially published")
					}
					batch := model.ChangeSet{Ops: []model.Op{hierarchyCreate("a", &parent, project), hierarchyCreate("b", &parent, project)}}
					batchMeta := f.meta("same-parent-batch")
					batchMeta.Project = project
					out, err := childAllocatorApply(f, batch, batchMeta)
					if err != nil || len(out.Commit.Results) != 2 || out.Commit.Results[0].Ref.ID != parent.ID+".3" || out.Commit.Results[1].Ref.ID != parent.ID+".4" {
						t.Fatalf("same-parent batch after refusal=%+v err=%v", out, err)
					}
					assertChildAllocatorReplay(t, f, batch, batchMeta, out.Commit)
					verifyHierarchy(t, f)
					if engine != "memory" {
						f.reopen(t)
						assertChildAllocatorReplay(t, f, batch, batchMeta, out.Commit)
					}
					last := hierarchyApply(t, f, hierarchyCreate("last", &parent, project))
					if last.Results[0].Ref.ID != parent.ID+".5" {
						t.Fatalf("reopened high-water lost: %+v", last)
					}
					// Child-only state must not install a project profile or reroute
					// the ordinary top-level ID generator.
					r = hierarchyReader(t, f)
					if project != "" {
						_, err = r.(driver.AllocatorReader).LoadAllocator(f.ctx, project)
						if !errors.Is(err, driver.ErrNotFound) {
							t.Fatalf("fallback seeded a project allocator: %v", err)
						}
					}
					r.Close()
					rootMeta = f.meta("another-root")
					rootMeta.Project = project
					root, err = f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyCreate("root", nil, project)}}, rootMeta)
					if err != nil || root.Results[0].Ref.ID != fmt.Sprintf("pl-batch-%s-002", engine) {
						t.Fatalf("child allocation changed top-level producer: %+v err=%v", root, err)
					}
					t.Logf("%s/%s: deleted .1 retained; .2-.5, replay, atomic refusal, SQL reopen and unchanged roots proven", engine, name)
				})
			}
		})
	}
}

var errChildAllocationDenied = errors.New("child allocation denied after preparation")
var errChildAllocationPut = errors.New("child allocation second Put failed")

type childAllocationDeny struct{}

func (childAllocationDeny) Authorize(_ context.Context, stage port.AuthorizationStage, _ port.ProposedChange, _ port.SnapshotReader) error {
	if stage == port.AuthorizeAfter {
		return errChildAllocationDenied
	}
	return nil
}

type childAllocationFailStore struct {
	driver.Store
	consumes, puts int
}

type childAllocationFailTx struct {
	driver.WriteTx
	driver.HashAllocatorContinuation
	driver.ParentCounterContinuation
	owner *childAllocationFailStore
}

func (s *childAllocationFailStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &childAllocationFailTx{WriteTx: tx, HashAllocatorContinuation: tx.(driver.HashAllocatorContinuation), ParentCounterContinuation: tx.(driver.ParentCounterContinuation), owner: s}, nil
}

func (t *childAllocationFailTx) AllocateChild(ctx context.Context, parent, expected string) error {
	err := t.HashAllocatorContinuation.AllocateChild(ctx, parent, expected)
	if err == nil {
		t.owner.consumes++
	}
	return err
}

func (t *childAllocationFailTx) ConsumeParentChild(ctx context.Context, parent, expected string) error {
	err := t.ParentCounterContinuation.ConsumeParentChild(ctx, parent, expected)
	if err == nil {
		t.owner.consumes++
	}
	return err
}

func (t *childAllocationFailTx) Put(ctx context.Context, record driver.Record) error {
	t.owner.puts++
	if t.owner.puts == 2 {
		return errChildAllocationPut
	}
	return t.WriteTx.Put(ctx, record)
}

func (t *childAllocationFailTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	t.owner.puts++
	if t.owner.puts == 2 {
		return errChildAllocationPut
	}
	return t.WriteTx.(driver.GraphSourceWriter).PutGraphRecord(ctx, record, parent, links)
}

func childInventory(t *testing.T, f *fixture) driver.AllocatorInventory {
	t.Helper()
	r := hierarchyReader(t, f)
	defer r.Close()
	inventory, err := r.(driver.AllocatorInventoryReader).AllocatorInventory(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return inventory
}

// Check the source index independently of closure rows and allocator inventory.
// In particular, an allocator-backed create must publish the exact bead image
// through the typed writer, and a rolled-back consume must leave no index row.
func assertChildAllocatorSourceIndex(t *testing.T, f *fixture, parent model.Ref, want ...model.Ref) {
	t.Helper()
	r := hierarchyReader(t, f)
	defer r.Close()
	indexed, ok := r.(driver.HierarchyChildrenReader)
	if !ok {
		t.Fatal("allocator fixture lacks the hierarchy source reader")
	}
	children, more, err := indexed.HierarchyChildren(f.ctx, []model.Ref{parent}, len(want))
	if err != nil || more || len(children) != len(want) {
		t.Fatalf("allocator child source index: children=%+v more=%v err=%v want=%+v", children, more, err, want)
	}
	expected := make(map[string]bool, len(want))
	for _, ref := range want {
		expected[ref.ID] = true
	}
	previous := ""
	for _, child := range children {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &child.Ref})
		if err != nil || !expected[child.Ref.ID] || previous != "" && key <= previous {
			t.Fatalf("unexpected, duplicate or unsorted allocator child source: %+v err=%v", child, err)
		}
		delete(expected, child.Ref.ID)
		previous = key
		stored, err := r.Get(f.ctx, []model.Ref{child.Ref})
		if err != nil || len(stored) != 1 || !reflect.DeepEqual(child, stored[0]) {
			t.Fatalf("allocator child index differs from exact published image: indexed=%+v stored=%+v err=%v", child, stored, err)
		}
		if doc := decodeDocument(t, child); doc.Parent == nil || doc.Parent.ID != parent.ID {
			t.Fatalf("allocator child index has a different parent: %+v", doc)
		}
	}
}

func TestChildAllocatorCertifiedSourcePublicationAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, mode := range []string{"no-profile", driver.AllocatorSequential, driver.AllocatorHash, driver.AllocatorRandom} {
			t.Run(engine+"/"+mode, func(t *testing.T) {
				f := newFixture(t, engine)
				var profile *driver.OfflineAllocator
				switch mode {
				case driver.AllocatorSequential:
					profile = &driver.OfflineAllocator{Prefix: "demo", Mode: mode}
				case driver.AllocatorHash:
					value := driver.NewHashAllocator("demo", 3, 8, 0.25)
					profile = &value
				case driver.AllocatorRandom:
					value := driver.NewRandomAllocator("demo")
					profile = &value
				}
				// Certify the root and restore its allocator in the same empty-
				// target transaction. The existing record-only fixtures above
				// deliberately retain their separate unbuilt-index coverage.
				seedChildSourceFixture(t, f, profile, true, true, "demo-root")
				parent := model.Ref{ID: "demo-root"}
				assertChildAllocatorSourceIndex(t, f, parent)
				cs := model.ChangeSet{Ops: []model.Op{hierarchyCreate("one", &parent, "demo"), hierarchyCreate("two", &parent, "demo")}}
				meta := f.meta("certified-children")
				out, err := childAllocatorApply(f, cs, meta)
				if err != nil || len(out.Commit.Results) != 2 {
					t.Fatalf("certified allocator publication: %+v err=%v", out, err)
				}
				first, second := out.Commit.Results[0].Ref, out.Commit.Results[1].Ref
				if mode != driver.AllocatorRandom && (first.ID != parent.ID+".1" || second.ID != parent.ID+".2") {
					t.Fatalf("certified allocator skipped initial counters: %+v", out)
				}
				assertChildAllocatorSourceIndex(t, f, parent, first, second)
				assertChildAllocatorReplay(t, f, cs, meta, out.Commit)
				hierarchyApply(t, f, childDelete(first))
				assertChildAllocatorSourceIndex(t, f, parent, second)

				before, rows, inventory := childAllocatorDurableState(t, f), hierarchyRows(t, f), childInventory(t, f)
				ordinary := f.writer
				fault := &childAllocationFailStore{Store: f.store}
				f.writer = pipeline.New(fault, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now))
				faultMeta := f.meta("certified-rollback")
				_, err = childAllocatorApply(f, cs, faultMeta)
				f.writer = ordinary
				if !errors.Is(err, errChildAllocationPut) || fault.consumes != 2 || fault.puts != 2 {
					t.Fatalf("certified rollback missed real consumes/typed Puts: consumes=%d puts=%d err=%v", fault.consumes, fault.puts, err)
				}
				requireSameState(t, before, childAllocatorDurableState(t, f))
				if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || !reflect.DeepEqual(inventory, childInventory(t, f)) || f.receipt(t, faultMeta) != nil {
					t.Fatal("certified rollback published closure, allocator or receipt")
				}
				assertChildAllocatorSourceIndex(t, f, parent, second)
				next := hierarchyApply(t, f, hierarchyCreate("three", &parent, "demo")).Results[0].Ref
				if mode != driver.AllocatorRandom && next.ID != parent.ID+".3" {
					t.Fatalf("certified rollback/deletion consumed or reused a counter: %+v", next)
				}
				assertChildAllocatorSourceIndex(t, f, parent, second, next)
				verifyHierarchy(t, f)
				if engine != "memory" {
					before, inventory = childAllocatorDurableState(t, f), childInventory(t, f)
					f.reopen(t)
					requireSameState(t, before, childAllocatorDurableState(t, f))
					if !reflect.DeepEqual(inventory, childInventory(t, f)) {
						t.Fatal("reopen changed certified allocator continuation")
					}
					assertChildAllocatorSourceIndex(t, f, parent, second, next)
				}
				t.Logf("%s/%s: allocator and certified child sources commit, replay, delete, roll back and reopen together", engine, mode)
			})
		}
	}
}

func TestChildAllocatorAtomicConsumptionAndConcurrentRetryAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, mode := range []string{"no-profile", driver.AllocatorSequential, driver.AllocatorHash} {
				t.Run(mode, func(t *testing.T) {
					f := newFixture(t, engine)
					project := ""
					if mode != "no-profile" {
						project = "demo"
						profile := driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential}
						if mode == driver.AllocatorHash {
							profile = driver.NewHashAllocator("demo", 3, 8, 0.25)
						}
						seedChildAllocatorFixture(t, f, profile, false)
					}
					parent := hierarchyApply(t, f, hierarchyCreate("parent", nil, project)).Results[0].Ref
					// A batch with no final graph change remains a receipt-only
					// no-op. These IDs are previews, never consumed allocations.
					noopCS := model.ChangeSet{Ops: []model.Op{hierarchyCreate("preview", &parent, project), childDelete(model.RefByKey("preview"))}}
					noopMeta := f.meta("receipt-only-preview")
					noopBefore, noopInventory := childAllocatorDurableState(t, f), childInventory(t, f)
					noop, err := childAllocatorApply(f, noopCS, noopMeta)
					if err != nil || noop.Commit.Revision.String() != noopBefore.Meta.Head.String() || len(noop.Commit.Results) != 2 {
						t.Fatalf("receipt-only child preview changed no-op semantics: %+v err=%v", noop, err)
					}
					requireSameState(t, noopBefore, childAllocatorDurableState(t, f))
					if !reflect.DeepEqual(noopInventory, childInventory(t, f)) {
						t.Fatal("receipt-only no-op consumed child preview")
					}
					assertChildAllocatorReplay(t, f, noopCS, noopMeta, noop.Commit)
					cs := model.ChangeSet{Ops: []model.Op{hierarchyCreate("one", &parent, project), hierarchyCreate("two", &parent, project)}}
					before, rows, inventory := childAllocatorDurableState(t, f), hierarchyRows(t, f), childInventory(t, f)
					assertUnchanged := func(t *testing.T, meta model.WriteMeta) {
						t.Helper()
						requireSameState(t, before, childAllocatorDurableState(t, f))
						if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || !reflect.DeepEqual(inventory, childInventory(t, f)) || f.receipt(t, meta) != nil {
							t.Fatal("refusal/dry-run published graph, counter or receipt")
						}
					}
					dryMeta := f.meta("dry-run")
					dryMeta.DryRun = true
					dry, err := childAllocatorApply(f, cs, dryMeta)
					if err != nil || !dry.Commit.WouldCommit || dry.Replayed {
						t.Fatalf("dry-run child preview=%+v err=%v", dry, err)
					}
					assertUnchanged(t, dryMeta)
					ordinary := f.writer
					f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithAuthorizer(childAllocationDeny{}))
					denyMeta := f.meta("authorization-refusal")
					_, err = childAllocatorApply(f, cs, denyMeta)
					if !errors.Is(err, errChildAllocationDenied) {
						t.Fatalf("after-authorization refusal=%v", err)
					}
					assertUnchanged(t, denyMeta)
					faultStore := &childAllocationFailStore{Store: f.store}
					f.writer = pipeline.New(faultStore, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now))
					faultMeta := f.meta("transaction-rollback")
					_, err = childAllocatorApply(f, cs, faultMeta)
					if !errors.Is(err, errChildAllocationPut) || faultStore.consumes != 2 || faultStore.puts != 2 {
						t.Fatalf("real consumption/late-Put rollback: consumes=%d puts=%d err=%v", faultStore.consumes, faultStore.puts, err)
					}
					assertUnchanged(t, faultMeta)
					f.writer = ordinary
					meta := f.meta("commit-after-refusals")
					out, err := childAllocatorApply(f, cs, meta)
					if err != nil || out.Commit.Results[0].Ref.ID != parent.ID+".1" || out.Commit.Results[1].Ref.ID != parent.ID+".2" {
						t.Fatalf("rollback consumed child numbers: %+v err=%v", out, err)
					}
					assertChildAllocatorReplay(t, f, cs, meta, out.Commit)

					// Hold one fully prepared .3 preview. A second Writer commits
					// .3, so the first must replan through the normal retry protocol.
					pause := &hierarchyPause{entered: make(chan struct{}), release: make(chan struct{})}
					concurrent := pipeline.New(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithValidators(pipeline.ValidatorBinding{ID: "child-allocation-race", Validator: pause}))
					one := model.ChangeSet{Ops: []model.Op{hierarchyCreate("next", &parent, project)}}
					pausedMeta := f.meta("stale-preview")
					type result struct {
						out port.WriteOutcome
						err error
					}
					done := make(chan result, 1)
					go func() {
						out, err := concurrent.ApplyWithOutcome(f.ctx, batchPrincipal, one, pausedMeta)
						done <- result{out, err}
					}()
					select {
					case <-pause.entered:
					case <-f.ctx.Done():
						t.Fatal("concurrent Writer never prepared")
					}
					winnerMeta := f.meta("concurrent-winner")
					winner, err := childAllocatorApply(f, one, winnerMeta)
					close(pause.release)
					if err != nil || winner.Commit.Results[0].Ref.ID != parent.ID+".3" {
						t.Fatalf("concurrent winner=%+v err=%v", winner, err)
					}
					var retried result
					select {
					case retried = <-done:
					case <-f.ctx.Done():
						t.Fatal("concurrent child allocation deadlocked")
					}
					if retried.err != nil || retried.out.Replayed || retried.out.Commit.Results[0].Ref.ID != parent.ID+".4" {
						t.Fatalf("stale child preview did not replan: %+v err=%v", retried.out, retried.err)
					}
					assertChildAllocatorReplay(t, f, one, winnerMeta, winner.Commit)
					assertChildAllocatorReplay(t, f, one, pausedMeta, retried.out.Commit)
					transient := model.ChangeSet{Ops: []model.Op{hierarchyCreate("temporary", &parent, project), childDelete(model.RefByKey("temporary")), hierarchyCreate("survivor", &parent, project)}}
					transientMeta := f.meta("ordered-create-delete-create")
					last, err := childAllocatorApply(f, transient, transientMeta)
					if err != nil || len(last.Commit.Results) != 3 || last.Commit.Results[0].Ref.ID != parent.ID+".5" || last.Commit.Results[2].Ref.ID != parent.ID+".6" {
						t.Fatalf("same-batch deleted preview reused: %+v err=%v", last, err)
					}
					assertChildAllocatorReplay(t, f, transient, transientMeta, last.Commit)
					continued := hierarchyApply(t, f, hierarchyCreate("after-transient", &parent, project))
					if continued.Results[0].Ref.ID != parent.ID+".7" {
						t.Fatalf("transient consumed number did not advance continuation: %+v", continued)
					}
					verifyHierarchy(t, f)
					t.Logf("%s/%s: dry-run, after-auth refusal, two real consumes rolled back after Put, stale-preview retry, ordered deletion and exact replay proven", engine, mode)
				})
			}
		})
	}
}

func TestChildAllocatorRetainedCountersRollbackAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, mode := range []string{"no-profile", driver.AllocatorSequential, driver.AllocatorHash} {
				t.Run(mode, func(t *testing.T) {
					f := newFixture(t, engine)
					project := ""
					if mode != "no-profile" {
						project = "demo"
						profile := driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential}
						if mode == driver.AllocatorHash {
							profile = driver.NewHashAllocator("demo", 3, 8, 0.25)
						}
						seedChildAllocatorFixture(t, f, profile, false)
					}
					parent := hierarchyApply(t, f, hierarchyCreate("parent", nil, project)).Results[0].Ref
					hierarchyApply(t, f, hierarchyCreate("committed-one", &parent, project))
					before, inventory := childAllocatorDurableState(t, f), childInventory(t, f)
					old := hierarchyReader(t, f)
					defer old.Close()
					ordinary := f.writer
					faultStore := &childAllocationFailStore{Store: f.store}
					f.writer = pipeline.New(faultStore, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now))
					cs := model.ChangeSet{Ops: []model.Op{hierarchyCreate("rollback-two", &parent, project), hierarchyCreate("rollback-three", &parent, project)}}
					meta := f.meta("retained-counter-rollback")
					_, err := childAllocatorApply(f, cs, meta)
					if !errors.Is(err, errChildAllocationPut) || faultStore.consumes != 2 || faultStore.puts != 2 {
						t.Fatalf("retained counter real consumption/rollback: consumes=%d puts=%d err=%v", faultStore.consumes, faultStore.puts, err)
					}
					requireSameState(t, before, childAllocatorDurableState(t, f))
					if actual := childInventory(t, f); !reflect.DeepEqual(inventory, actual) || f.receipt(t, meta) != nil {
						t.Fatalf("retained counter leaked through failed transaction: before=%+v after=%+v", inventory, actual)
					}
					if mode != "no-profile" {
						state, err := old.(driver.AllocatorReader).LoadAllocator(f.ctx, project)
						if err != nil || state.Children[parent.ID] != 1 {
							t.Fatalf("rollback mutated prior snapshot: %+v err=%v", state, err)
						}
					}
					f.writer = ordinary
					out := hierarchyApply(t, f, hierarchyCreate("after-rollback", &parent, project))
					if out.Results[0].Ref.ID != parent.ID+".2" {
						t.Fatalf("failed transaction consumed retained number: %+v", out)
					}
					t.Logf("%s/%s: retained .1 stayed detached after two real consumes/second-Put rollback; next .2", engine, mode)
				})
			}
		})
	}
}

func TestParentCounterSnapshotStagingAndLockGuardsAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			root := hierarchyApply(t, f, hierarchyCreate("parent", nil, ""))
			parent := root.Results[0].Ref
			old := hierarchyReader(t, f)
			defer old.Close()
			if _, err := old.(driver.ParentCounterReader).LoadParentCounter(f.ctx, parent.ID); !errors.Is(err, driver.ErrNotFound) {
				t.Fatalf("uninitialized counter=%v", err)
			}
			hierarchyApply(t, f, hierarchyCreate("child", &parent, ""))
			if _, err := old.(driver.ParentCounterReader).LoadParentCounter(f.ctx, parent.ID); !errors.Is(err, driver.ErrNotFound) {
				t.Fatalf("old snapshot observed new counter: %v", err)
			}
			before, inventory := childAllocatorDurableState(t, f), childInventory(t, f)
			if len(inventory.Projects) != 0 || len(inventory.Parents) != 1 || inventory.Parents[0].LastChild != 1 {
				t.Fatalf("typed counter inventory=%+v", inventory)
			}
			historical, err := f.store.Read(f.ctx, driver.View{Incarnation: before.Meta.Incarnation, At: &root.Revision})
			if err != nil {
				t.Fatal(err)
			}
			_, err = historical.(driver.ParentCounterReader).LoadParentCounter(f.ctx, parent.ID)
			historical.Close()
			if !errors.Is(err, driver.ErrNotFound) {
				t.Fatalf("historical snapshot observed a counter created after its head: %v", err)
			}
			counterKey, err := driver.ParentCounterLock(before.Meta.Incarnation, parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			candidate := model.Ref{ID: parent.ID + ".2"}
			recordKey := driver.LockKey{Kind: driver.LockRecord, Ref: &candidate}
			for _, scenario := range []string{"staging-and-rollback", "missing-counter-lock", "shared-counter-lock", "missing-candidate-lock", "shared-candidate-lock"} {
				t.Run(scenario, func(t *testing.T) {
					locks := driver.LockSet{{Key: recordKey, Mode: driver.LockUpdate}, {Key: counterKey, Mode: driver.LockUpdate}}
					switch scenario {
					case "missing-counter-lock":
						locks = locks[:1]
					case "shared-counter-lock":
						locks[1].Mode = driver.LockShare
					case "missing-candidate-lock":
						locks = locks[1:]
					case "shared-candidate-lock":
						locks[0].Mode = driver.LockShare
					}
					tx, err := f.store.Write(f.ctx, f.lease)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Close()
					if _, err = tx.Lock(f.ctx, locks); err != nil {
						t.Fatal(err)
					}
					if _, err = tx.Head(f.ctx); err != nil {
						t.Fatal(err)
					}
					counter := tx.(driver.ParentCounterContinuation)
					initial, err := counter.LoadParentCounter(f.ctx, parent.ID)
					if err != nil || initial.LastChild != 1 {
						t.Fatalf("transaction initial counter=%+v %v", initial, err)
					}
					err = counter.ConsumeParentChild(f.ctx, parent.ID, candidate.ID)
					if scenario == "staging-and-rollback" {
						if err != nil {
							t.Fatal(err)
						}
						staged, err := counter.LoadParentCounter(f.ctx, parent.ID)
						if err != nil || staged.LastChild != 2 {
							t.Fatalf("transaction missed staged counter: %+v %v", staged, err)
						}
						if err = counter.ConsumeParentChild(f.ctx, parent.ID, candidate.ID); !errors.Is(err, driver.ErrConflict) {
							t.Fatalf("duplicate consumption=%v", err)
						}
						cancelled, cancel := context.WithCancel(f.ctx)
						cancel()
						if _, err = counter.LoadParentCounter(cancelled, parent.ID); !errors.Is(err, context.Canceled) {
							t.Fatalf("cancelled counter read=%v", err)
						}
						if err = counter.ConsumeParentChild(cancelled, parent.ID, candidate.ID); !errors.Is(err, context.Canceled) {
							t.Fatalf("cancelled counter consume=%v", err)
						}
					} else if err == nil {
						t.Fatal("counter mutation accepted without both strongest Update locks")
					}
					if err = tx.Rollback(f.ctx); err != nil {
						t.Fatal(err)
					}
					if _, err = counter.LoadParentCounter(f.ctx, parent.ID); err == nil {
						t.Fatal("closed transaction read counter")
					}
					requireSameState(t, before, childAllocatorDurableState(t, f))
					if !reflect.DeepEqual(inventory, childInventory(t, f)) {
						t.Fatal("rollback/refusal published counter")
					}
				})
			}
		})
	}
}

func TestParentCounterReconcilesRestoredDirectChildrenAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seedChildFixture(t, f, nil, true)
			parent := model.Ref{ID: "demo-root"}
			if inventory := childInventory(t, f); len(inventory.Projects) != 0 || len(inventory.Parents) != 0 {
				t.Fatalf("record-only restore installed continuation: %+v", inventory)
			}
			cs := model.ChangeSet{Ops: []model.Op{hierarchyCreate("nine", &parent, "demo"), hierarchyCreate("ten", &parent, "demo")}}
			meta := f.meta("observed-no-profile")
			out, err := childAllocatorApply(f, cs, meta)
			if err != nil || len(out.Commit.Results) != 2 || out.Commit.Results[0].Ref.ID != "demo-root.9" || out.Commit.Results[1].Ref.ID != "demo-root.10" {
				t.Fatalf("no-profile direct-child repair: %+v err=%v", out, err)
			}
			assertChildAllocatorReplay(t, f, cs, meta, out.Commit)
			// Remove the restored grandchild first, then every direct child.
			// The retained counter, rather than live rows or history, owns .11.
			hierarchyApply(t, f, childDelete(model.Ref{ID: "demo-root.8.2"}), childDelete(model.Ref{ID: "demo-root.8"}), childDelete(out.Commit.Results[0].Ref), childDelete(out.Commit.Results[1].Ref))
			if engine != "memory" {
				f.reopen(t)
			}
			continued := hierarchyApply(t, f, hierarchyCreate("eleven", &parent, "demo"))
			if continued.Results[0].Ref.ID != "demo-root.11" {
				t.Fatalf("reconciled no-profile continuation reset: %+v", continued)
			}
			inventory := childInventory(t, f)
			if len(inventory.Projects) != 0 || len(inventory.Parents) != 1 || inventory.Parents[0].LastChild != 11 {
				t.Fatalf("parent-only durable high-water: %+v", inventory)
			}
			verifyHierarchy(t, f)
			t.Logf("%s: restored .8/.8.2 without profile reconciled .9/.10; all children deleted; continuation .11 with SQL reopen", engine)
		})
	}
}

func TestParentCounterTerminalValueAcrossStores(t *testing.T) {
	const priorID = "demo-root.18446744073709551614"
	const finalID = "demo-root.18446744073709551615"
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, scenario := range []string{"commit", "rollback-then-commit"} {
				t.Run(scenario, func(t *testing.T) {
					f := newFixture(t, engine)
					seedChildFixture(t, f, nil, true, "demo-root", priorID)
					parent := model.Ref{ID: "demo-root"}
					before, rows, inventory := childAllocatorDurableState(t, f), hierarchyRows(t, f), childInventory(t, f)
					if len(inventory.Projects) != 0 || len(inventory.Parents) != 0 {
						t.Fatalf("record-only restore installed continuation: %+v", inventory)
					}
					cs := model.ChangeSet{Ops: []model.Op{
						hierarchyCreate("terminal-child", &parent, "demo"),
						// A fresh root gives the rollback control two actual Puts
						// without patching a record-only restored ontology image.
						hierarchyCreate("atomic-peer", nil, "demo"),
					}}
					meta := f.meta("terminal-child")
					if scenario == "rollback-then-commit" {
						ordinary := f.writer
						fault := &childAllocationFailStore{Store: f.store}
						f.writer = pipeline.New(fault, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID))
						_, err := childAllocatorApply(f, cs, meta)
						requireSameState(t, before, childAllocatorDurableState(t, f))
						if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || !reflect.DeepEqual(inventory, childInventory(t, f)) || f.receipt(t, meta) != nil {
							t.Fatal("terminal consumption leaked through failed transaction")
						}
						if !errors.Is(err, errChildAllocationPut) || fault.consumes != 1 || fault.puts != 2 {
							t.Fatalf("terminal rollback never reached second Put: consumes=%d puts=%d err=%v", fault.consumes, fault.puts, err)
						}
						f.writer = ordinary
					}
					out, err := childAllocatorApply(f, cs, meta)
					if err != nil {
						requireSameState(t, before, childAllocatorDurableState(t, f))
						if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || !reflect.DeepEqual(inventory, childInventory(t, f)) || f.receipt(t, meta) != nil {
							t.Fatal("refused terminal create partially published")
						}
						t.Fatalf("Writer refused final valid child %s: %v", finalID, err)
					}
					if out.Replayed || len(out.Commit.Results) != 2 || out.Commit.Results[0].Ref.ID != finalID || out.Commit.Results[1].Ref.ID == finalID {
						t.Fatalf("terminal batch result=%+v", out)
					}
					state := childAllocatorDurableState(t, f)
					child, exists := state.Records[finalID]
					if !exists || len(state.Records) != len(before.Records)+2 || child.Revision.String() != out.Commit.Revision.String() || len(state.Versions[finalID]) != 1 || len(state.Log) != len(before.Log)+1 {
						t.Fatalf("terminal child publication: record=%+v records=%d versions=%d log=%d", child, len(state.Records), len(state.Versions[finalID]), len(state.Log))
					}
					if doc := decodeDocument(t, child); doc.Parent == nil || doc.Parent.ID != parent.ID {
						t.Fatalf("terminal child parent=%+v", doc.Parent)
					}
					peer, exists := state.Records[out.Commit.Results[1].Ref.ID]
					if !exists || peer.Revision.String() != out.Commit.Revision.String() || decodeDocument(t, peer).Fields["title"] != "atomic-peer" || decodeDocument(t, peer).Parent != nil {
						t.Fatalf("terminal batch peer not published atomically: %+v", peer)
					}
					inventory = childInventory(t, f)
					if len(inventory.Projects) != 0 || len(inventory.Parents) != 1 || inventory.Parents[0] != (driver.ParentCounter{Incarnation: state.Meta.Incarnation, Parent: parent.ID, LastChild: ^uint64(0)}) {
						t.Fatalf("terminal high-water not persisted: %+v", inventory)
					}
					verifyHierarchy(t, f)
					assertChildAllocatorReplay(t, f, cs, meta, out.Commit)
					assertExhausted := func(label string) {
						t.Helper()
						before, rows, inventory := childAllocatorDurableState(t, f), hierarchyRows(t, f), childInventory(t, f)
						meta := f.meta(label)
						_, err := childAllocatorApply(f, model.ChangeSet{Ops: []model.Op{hierarchyCreate("exhausted", &parent, "demo")}}, meta)
						if !errors.Is(err, driver.ErrUnsupported) {
							t.Fatalf("allocation after terminal child did not refuse: %v", err)
						}
						requireSameState(t, before, childAllocatorDurableState(t, f))
						if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || !reflect.DeepEqual(inventory, childInventory(t, f)) || f.receipt(t, meta) != nil {
							t.Fatal("terminal exhaustion partially published")
						}
					}
					assertExhausted("after-terminal-commit")
					hierarchyApply(t, f, childDelete(model.Ref{ID: priorID}), childDelete(model.Ref{ID: finalID}))
					before = childAllocatorDurableState(t, f)
					if len(before.Records) != 2 || !reflect.DeepEqual(inventory, childInventory(t, f)) {
						t.Fatal("all-child deletion lost terminal high-water")
					}
					if engine != "memory" {
						f.reopen(t)
						requireSameState(t, before, childAllocatorDurableState(t, f))
						if !reflect.DeepEqual(inventory, childInventory(t, f)) {
							t.Fatal("SQL reopen lost terminal high-water")
						}
					}
					assertChildAllocatorReplay(t, f, cs, meta, out.Commit)
					assertExhausted("after-deletion-reopen")
					verifyHierarchy(t, f)
					t.Logf("%s/%s: restored max-1 committed max atomically; terminal counter, exact replay, deletion/reopen and typed exhaustion proven", engine, scenario)
				})
			}
		})
	}
}

func TestChildAllocatorObservedExhaustionRefusesAtomicallyAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, mode := range []string{"no-profile", driver.AllocatorSequential, driver.AllocatorHash} {
				t.Run(mode, func(t *testing.T) {
					for _, suffix := range []string{"18446744073709551615", "18446744073709551616"} {
						t.Run(suffix, func(t *testing.T) {
							f := newFixture(t, engine)
							var profile *driver.OfflineAllocator
							switch mode {
							case driver.AllocatorSequential:
								profile = &driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential}
							case driver.AllocatorHash:
								value := driver.NewHashAllocator("demo", 3, 8, 0.25)
								profile = &value
							}
							seedChildFixture(t, f, profile, true, "demo-root", "demo-root."+suffix)
							before, inventory := childAllocatorDurableState(t, f), childInventory(t, f)
							parent := model.Ref{ID: "demo-root"}
							meta := f.meta("observed-exhaustion")
							out, err := childAllocatorApply(f, model.ChangeSet{Ops: []model.Op{hierarchyCreate("exhausted", &parent, "demo")}}, meta)
							if !errors.Is(err, driver.ErrUnsupported) {
								t.Fatalf("decimal direct-child exhaustion did not refuse: suffix=%s outcome=%+v err=%v", suffix, out, err)
							}
							requireSameState(t, before, childAllocatorDurableState(t, f))
							if !reflect.DeepEqual(inventory, childInventory(t, f)) || f.receipt(t, meta) != nil {
								t.Fatal("exhausted preview published continuation or receipt")
							}
						})
					}
				})
			}
		})
	}
}

type childNoHistoryStore struct{ driver.Store }
type childNoHistoryReader struct {
	driver.Reader
	driver.AllocatorReader
	driver.ParentCounterReader
}

func (s childNoHistoryStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return childNoHistoryReader{Reader: r, AllocatorReader: r.(driver.AllocatorReader), ParentCounterReader: r.(driver.ParentCounterReader)}, nil
}
func (childNoHistoryReader) Versions(context.Context, driver.Ref, driver.PageReq) (driver.Page[driver.Version], error) {
	return driver.Page[driver.Version]{}, driver.ErrTruncated{}
}
func (childNoHistoryReader) Changesets(context.Context, *driver.Cursor, int) ([]model.Changeset, error) {
	return nil, driver.ErrTruncated{}
}

func TestParentCounterContinuationWithoutAvailableHistoryAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			parent := hierarchyApply(t, f, hierarchyCreate("parent", nil, "")).Results[0].Ref
			child := hierarchyApply(t, f, hierarchyCreate("child", &parent, "")).Results[0].Ref
			hierarchyApply(t, f, childDelete(child))
			// History is deliberately unavailable to the production Writer.
			// This proves counter independence; it is not a public compaction run.
			f.writer = pipeline.New(childNoHistoryStore{f.store}, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now))
			next := hierarchyApply(t, f, hierarchyCreate("continued", &parent, ""))
			if next.Results[0].Ref.ID != parent.ID+".2" {
				t.Fatalf("continuation relied on compactable history: %+v", next)
			}
		})
	}
}

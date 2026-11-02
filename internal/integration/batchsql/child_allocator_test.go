package batchsql_test

import (
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
	key, err := driver.AllocatorLock(profile.Prefix)
	if err != nil {
		t.Fatal(err)
	}
	locks := driver.LockSet{{Key: key, Mode: driver.LockUpdate}}
	var records []driver.Record
	var rows []driver.DerivedRow
	var sentinels []driver.Sentinel
	if observed {
		ids := []string{"demo-root", "demo-root.8", "demo-root.8.2"}
		for i, id := range ids {
			doc := pipeline.Document{ID: id, Type: "issue", Project: "demo", Fields: map[string]any{"title": id}}
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
	if err = tx.(driver.AllocatorContinuation).RestoreAllocators(f.ctx, []driver.OfflineAllocator{profile}); err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err = tx.Put(f.ctx, record); err != nil {
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
	before, rows := childAllocatorDurableState(t, f), hierarchyRows(t, f)
	got, err := childAllocatorApply(f, cs, meta)
	if err != nil || !got.Replayed || !reflect.DeepEqual(got.Commit, commit) {
		t.Fatalf("exact child replay: %+v err=%v want=%+v", got, err, commit)
	}
	requireSameState(t, before, childAllocatorDurableState(t, f))
	if !reflect.DeepEqual(rows, hierarchyRows(t, f)) {
		t.Fatal("replay changed closure rows")
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

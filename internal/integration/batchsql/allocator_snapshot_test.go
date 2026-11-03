package batchsql_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Call consumption directly so Writer collision checks cannot hide missing
// engine locks, durable membership or transaction-local reservations.
func TestRestoredRandomAllocatorConsumptionGuardsAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seedChildAllocatorFixture(t, f, driver.NewRandomAllocator("demo"), false)
			root := hierarchyApply(t, f, hierarchyCreate("random-guard-root", nil, "demo")).Results[0].Ref
			child := hierarchyApply(t, f, hierarchyCreate("random-guard-child", &root, "demo")).Results[0].Ref
			for _, parent := range []string{"", root.ID} {
				kind := "root"
				if parent != "" {
					kind = "child"
				}
				for _, scenario := range []string{"rollback", "durable", "staged", "reserved", "missing-allocator-lock", "shared-allocator-lock", "missing-candidate-lock", "shared-candidate-lock", "cancelled", "malformed"} {
					t.Run(kind+"/"+scenario, func(t *testing.T) {
						prefix := "demo-"
						if parent != "" {
							prefix = parent + "."
						}
						candidate := prefix + strings.Repeat("1", 32)
						if scenario == "durable" {
							candidate = root.ID
							if parent != "" {
								candidate = child.ID
							}
						}
						if scenario == "malformed" {
							candidate = prefix + strings.Repeat("G", 32)
						}
						before := childAllocatorDurableState(t, f)
						image := observedAllocatorSnapshot(t, f, nil)
						ref := model.Ref{ID: candidate}
						allocator, err := driver.AllocatorLock("demo")
						if err != nil {
							t.Fatal(err)
						}
						locks := driver.LockSet{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate}, {Key: allocator, Mode: driver.LockUpdate}}
						switch scenario {
						case "missing-allocator-lock":
							locks = locks[:1]
						case "shared-allocator-lock":
							locks[1].Mode = driver.LockShare
						case "missing-candidate-lock":
							locks = locks[1:]
						case "shared-candidate-lock":
							locks[0].Mode = driver.LockShare
						}
						if scenario == "staged" {
							// The preceding Writer creates established ready coverage. Stage
							// the exact root/no-links image through the typed source port.
							locks = append(locks, driver.LockRequest{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate})
						}
						tx, err := f.store.Write(f.ctx, f.lease)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Close()
						facts, err := tx.Lock(f.ctx, locks)
						if err != nil {
							t.Fatal(err)
						}
						if _, err = tx.Head(f.ctx); err != nil {
							t.Fatal(err)
						}
						if scenario == "staged" {
							body, err := json.Marshal(map[string]any{"id": candidate, "type": "issue", "fields": map[string]any{"title": "must roll back"}})
							if err != nil {
								t.Fatal(err)
							}
							if err = tx.(driver.GraphSourceWriter).PutGraphRecord(f.ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
								t.Fatal(err)
							}
						}
						callCtx := f.ctx
						if scenario == "cancelled" {
							var cancel context.CancelFunc
							callCtx, cancel = context.WithCancel(f.ctx)
							cancel()
						}
						consume := func() error {
							port := tx.(driver.AllocatorContinuation)
							if parent == "" {
								return port.AllocateTopLevel(callCtx, "demo", candidate)
							}
							return port.AllocateChild(callCtx, parent, candidate)
						}
						if scenario == "reserved" {
							if err = consume(); err != nil {
								t.Fatalf("first previewed candidate was refused: %v", err)
							}
						}
						err = consume()
						if scenario == "rollback" {
							if err != nil {
								t.Fatalf("previewed candidate was reminted/refused: %v", err)
							}
						} else if err == nil {
							t.Fatalf("%s consumed a forbidden candidate", scenario)
						}
						if (scenario == "durable" || scenario == "staged" || scenario == "reserved") && !errors.Is(err, driver.ErrConflict) {
							t.Fatalf("collision was not typed conflict: %v", err)
						}
						if scenario == "cancelled" && !errors.Is(err, context.Canceled) {
							t.Fatalf("cancellation was masked: %v", err)
						}
						if err = tx.Close(); err != nil {
							t.Fatal(err)
						}
						requireSameState(t, before, childAllocatorDurableState(t, f))
						requireAllocatorSnapshotBytes(t, image, observedAllocatorSnapshot(t, f, nil))
						probe, err := f.store.Write(f.ctx, f.lease)
						if err != nil {
							t.Fatal(err)
						}
						defer probe.Close()
						after, err := probe.Lock(f.ctx, locks)
						if err != nil || !reflect.DeepEqual(facts, after) {
							t.Fatalf("rollback changed facts or retained locks: %v", err)
						}
					})
				}
			}
		})
	}
}

func observedAllocatorSnapshot(t *testing.T, f *fixture, at *model.Revision) driver.AllocatorSnapshot {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s, err := r.(driver.AllocatorSnapshotReader).AllocatorSnapshot(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	bound := r.Snapshot()
	if bound.At == nil || s.At.String() != bound.At.String() || s.Incarnation != bound.Incarnation || s.StoreID != meta.StoreID {
		t.Fatalf("unbound allocator snapshot: %+v %+v", s, bound)
	}
	inventory, err := r.(driver.AllocatorInventoryReader).AllocatorInventory(f.ctx)
	if err != nil || !reflect.DeepEqual(inventory, s.Inventory) {
		t.Fatalf("inventory disagrees: %+v %+v %v", inventory, s.Inventory, err)
	}
	for _, a := range s.Inventory.Projects {
		got, err := r.(driver.AllocatorReader).LoadAllocator(f.ctx, a.Prefix)
		if err != nil || !reflect.DeepEqual(got, a) {
			t.Fatalf("project getter disagrees: %+v %+v %v", got, a, err)
		}
		for key := range got.Children {
			got.Children[key] = 0
		}
		again, err := r.(driver.AllocatorReader).LoadAllocator(f.ctx, a.Prefix)
		if err != nil || !reflect.DeepEqual(again, a) {
			t.Fatal("project getter aliases its snapshot")
		}
	}
	for _, p := range s.Inventory.Parents {
		got, err := r.(driver.ParentCounterReader).LoadParentCounter(f.ctx, p.Parent)
		if err != nil || got != p {
			t.Fatalf("parent getter disagrees: %+v %+v %v", got, p, err)
		}
	}
	return s
}

func requireAllocatorSnapshotBytes(t *testing.T, want, got driver.AllocatorSnapshot) {
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
		t.Fatalf("snapshot bytes changed:\nwant %s\n got %s", a, b)
	}
}

func TestAllocatorSnapshotAtCapturedHeadAcrossThreeStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			zero := observedAllocatorSnapshot(t, f, nil)
			if !zero.At.IsZero() || len(zero.Inventory.Projects) != 0 || len(zero.Inventory.Parents) != 0 {
				t.Fatalf("fresh baseline: %+v", zero)
			}
			parent := hierarchyApply(t, f, hierarchyCreate("snapshot-root", nil, "")).Results[0].Ref
			captured := observedAllocatorSnapshot(t, f, nil)
			// Advance the actual engine on another goroutine between capture and
			// opening an explicitly historical reader, including the head-zero case.
			type result struct {
				commit model.Commit
				err    error
			}
			done := make(chan result, 1)
			go func() {
				c, e := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyCreate("snapshot-child", &parent, "")}}, f.meta("snapshot-concurrent"))
				done <- result{c, e}
			}()
			out := <-done
			if out.err != nil {
				t.Fatal(out.err)
			}
			child := out.commit.Results[0].Ref
			if child.ID != parent.ID+".1" {
				t.Fatalf("actual child identity: %+v", child)
			}
			withChild := observedAllocatorSnapshot(t, f, nil)
			if len(withChild.Inventory.Parents) != 1 || withChild.Inventory.Parents[0].LastChild != 1 {
				t.Fatalf("actual continuation: %+v", withChild)
			}
			hierarchyApply(t, f, childDelete(child))
			deleted := observedAllocatorSnapshot(t, f, nil)
			if len(deleted.Inventory.Parents) != 1 || deleted.Inventory.Parents[0].LastChild != 1 {
				t.Fatal("deletion discarded continuation")
			}
			second := hierarchyApply(t, f, hierarchyCreate("second-child", &parent, "")).Results[0].Ref
			if second.ID != parent.ID+".2" {
				t.Fatalf("deleted suffix reused: %+v", second)
			}
			for _, want := range []driver.AllocatorSnapshot{zero, captured, withChild, deleted} {
				at := want.At
				requireAllocatorSnapshotBytes(t, want, observedAllocatorSnapshot(t, f, &at))
			}
			if engine != "memory" {
				f.reopen(t)
				for _, want := range []driver.AllocatorSnapshot{zero, captured, withChild, deleted} {
					at := want.At
					requireAllocatorSnapshotBytes(t, want, observedAllocatorSnapshot(t, f, &at))
				}
			}
		})
	}
}

func TestAllocatorSnapshotProjectHistoryAcrossThreeStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seedChildAllocatorFixture(t, f, driver.OfflineAllocator{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 8}, false)
			seed := observedAllocatorSnapshot(t, f, nil)
			parent := hierarchyApply(t, f, hierarchyCreate("historical-project-root", nil, "demo")).Results[0].Ref
			if parent.ID != "demo-9" {
				t.Fatalf("real project allocation: %+v", parent)
			}
			root := observedAllocatorSnapshot(t, f, nil)
			child := hierarchyApply(t, f, hierarchyCreate("historical-project-child", &parent, "demo")).Results[0].Ref
			if child.ID != "demo-9.1" {
				t.Fatalf("real project child: %+v", child)
			}
			current := observedAllocatorSnapshot(t, f, nil)
			if current.Inventory.Projects[0].LastID != 9 || current.Inventory.Projects[0].Children[parent.ID] != 1 {
				t.Fatalf("project counter not published: %+v", current)
			}
			before := childAllocatorDurableState(t, f)
			meta := f.meta("allocator-history-rollback")
			_, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyCreate("unpublished", &parent, "demo"), patchOp(model.Ref{ID: "missing-for-rollback"}, map[string]any{"title": "refuse"})}}, meta)
			requireCode(t, err, "not_found")
			requireSameState(t, before, childAllocatorDurableState(t, f))
			if f.receipt(t, meta) != nil {
				t.Fatal("refusal wrote receipt")
			}
			requireAllocatorSnapshotBytes(t, current, observedAllocatorSnapshot(t, f, nil))
			for _, want := range []driver.AllocatorSnapshot{seed, root, current} {
				at := want.At
				requireAllocatorSnapshotBytes(t, want, observedAllocatorSnapshot(t, f, &at))
			}
			if engine != "memory" {
				f.reopen(t)
				for _, want := range []driver.AllocatorSnapshot{seed, root, current} {
					at := want.At
					requireAllocatorSnapshotBytes(t, want, observedAllocatorSnapshot(t, f, &at))
				}
			}
		})
	}
}

// This labelled migration-negative removes only the new internal journal from
// the test's private SQL database. The retained live counters are untouched;
// no old value is supplied by an adapter or reconstructed from a record ID.
func TestAllocatorSnapshotLegacyCoverageAcrossSQL(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			parent := hierarchyApply(t, f, hierarchyCreate("legacy-parent", nil, "")).Results[0].Ref
			hierarchyApply(t, f, hierarchyCreate("legacy-child", &parent, ""))
			before := observedAllocatorSnapshot(t, f, nil)
			name, dsn, table := "sqlite", f.path, "allocator_states"
			if engine == "postgres" {
				name, dsn, table = "pgx", f.dsn, "plug_postgres.allocator_states"
			}
			db, err := sql.Open(name, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.ExecContext(f.ctx, "DELETE FROM "+table+" WHERE prefix LIKE 'allocator-snapshot/%'"); err != nil {
				t.Fatal(err)
			}
			old := model.Natural64(1)
			meta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &old})
			if err != nil {
				t.Fatal(err)
			}
			_, err = r.(driver.AllocatorSnapshotReader).AllocatorSnapshot(f.ctx)
			_ = r.Close()
			var refusal *driver.NativeAllocatorSnapshotError
			if !errors.As(err, &refusal) || refusal.Reason != driver.NativeAllocatorCoverageUnavailable {
				t.Fatalf("legacy old head fabricated: %v", err)
			}
			legacy := observedAllocatorSnapshot(t, f, nil)
			if legacy.CoverageFloor.String() != before.At.String() {
				t.Fatalf("legacy coverage: %+v", legacy)
			}
			if !reflect.DeepEqual(legacy.Inventory, before.Inventory) {
				t.Fatal("legacy live counters changed")
			}
			hierarchyApply(t, f, hierarchyCreate("post-legacy", &parent, ""))
			at := legacy.At
			requireAllocatorSnapshotBytes(t, legacy, observedAllocatorSnapshot(t, f, &at))
			f.reopen(t)
			requireAllocatorSnapshotBytes(t, legacy, observedAllocatorSnapshot(t, f, &at))
		})
	}
}

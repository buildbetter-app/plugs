package converterroundtrip

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This matrix exercises actual checkpoint publication and core correspondence,
// not a memory persistence promise or a substitute for public B21 adoption.
func TestSourceCheckpointCapabilitiesAndAtomicSnapshotsAllDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			wantLifetime := driver.CheckpointLifetimeRestart
			if engine == "memory" {
				wantLifetime = driver.CheckpointLifetimeProcess
			}
			if capability.WorkflowCheckpointLifetime() != wantLifetime {
				t.Fatal("wrong actual store lifetime")
			}
			if engine == "memory" {
				if persistent, ok := store.(driver.PersistentCheckpointStore); ok && persistent.Persistent() {
					t.Fatal("process store falsely promises restart persistence")
				}
			}
			const namespace = "plugin/workflow/default/durable"
			meta, err := store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expect := driver.CoreSnapshotExpectation{StoreID: meta.StoreID, Incarnation: meta.Incarnation, Head: meta.Head}
			if _, err := capability.SaveCheckpointAtHead(ctx, lease, namespace, 0, []byte("initial"), expect); err != nil {
				t.Fatal(err)
			}
			before := reverseState(t, store)
			pinned, err := capability.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: meta.Incarnation}, []string{"missing", namespace})
			if err != nil {
				t.Fatal(err)
			}
			defer pinned.Close()
			if !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("snapshot guard wrote core state")
			}
			assertSourceCheckpoint(t, pinned, namespace, 1, "initial")
			if _, ok := pinned.(driver.AllocatorReader); !ok {
				t.Fatalf("%s snapshot dropped allocator capability", engine)
			}
			missing, err := pinned.SourcePlaneCheckpoint("missing")
			if err != nil || missing.Generation != 0 || missing.Payload != nil {
				t.Fatal("missing namespace was not pinned", err)
			}
			if _, err := pinned.SourcePlaneCheckpoint("unobserved"); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("unobserved namespace silently treated as absent", err)
			}
			// CommitNoop joins the checkpoint and final receipt with zero core
			// revision/history. An exact retry must not repeat its CAS.
			for attempt := 0; attempt < 2; attempt++ {
				tx, h := sourceCheckpointTx(t, store, lease, "active-only", nil)
				if err := tx.(driver.CheckpointWriteTx).StageCheckpoint(ctx, namespace, 1, []byte("active")); err != nil {
					t.Fatal(err)
				}
				if commit, err := tx.(driver.NoopCommitter).CommitNoop(ctx, h); err != nil || !commit.Revision.IsZero() {
					t.Fatal("active-only atomic receipt/replay", commit, err)
				}
				_ = tx.Close()
			}
			cp, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil || cp.Generation != 2 || string(cp.Payload) != "active" || !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("active-only commit/replay changed core or reapplied checkpoint", err)
			}
			// A failed mixed commit must not publish either plane or its receipt.
			ref := model.RefByID("source-checkpoint-mixed", model.Natural64(0))
			for _, fail := range []bool{true, false} {
				tx, h := sourceCheckpointTx(t, store, lease, "mixed", &ref)
				if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "config", Body: []byte(`{"changed":true}`)}); err != nil {
					t.Fatal(err)
				}
				if err := tx.(driver.CheckpointWriteTx).StageCheckpoint(ctx, namespace, 2, []byte("mixed")); err != nil {
					t.Fatal(err)
				}
				if fail {
					h.RequestHash = "rejected-mixed"
					h.Changes = nil
					if _, err := tx.Commit(ctx, h); err == nil {
						t.Fatal("invalid mixed changeset accepted")
					}
				} else if entry, err := tx.Commit(ctx, h); err != nil || entry.Revision.String() != "1" || len(entry.Changes) != 1 {
					t.Fatal("mixed checkpoint+core commit", entry, err)
				}
				_ = tx.Close()
				if fail {
					cp, err := capability.LoadCheckpoint(ctx, namespace)
					if err != nil || cp.Generation != 2 || string(cp.Payload) != "active" || !reflect.DeepEqual(before, reverseState(t, store)) {
						t.Fatal("rejected mixed commit published", err)
					}
				}
			}
			if _, err := capability.SaveCheckpointAtHead(ctx, lease, namespace, 3, []byte("stale"), expect); !errors.Is(err, driver.ErrCheckpointConflict) {
				t.Fatal("stale core snapshot allowed provider publication", err)
			}
			assertSourceCheckpoint(t, pinned, namespace, 1, "initial")
			if rows, err := pinned.Get(ctx, []driver.Ref{ref}); err != nil || len(rows) != 0 || !pinned.Snapshot().At.IsZero() {
				t.Fatal("pinned snapshot mixed graph generations", err)
			}
			if _, err := capability.ReadSourcePlaneSnapshot(ctx, pinned.Snapshot(), []string{namespace}); !errors.Is(err, driver.ErrCheckpointConflict) {
				t.Fatal("historical core joined to current provider", err)
			}
			_ = pinned.Close()
			store, lease = reopen() // SQL closes/reopens; memory keeps its process.
			capability, err = driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			meta, err = store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			current, err := capability.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: meta.Incarnation}, []string{namespace})
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			assertSourceCheckpoint(t, current, namespace, 3, "mixed")
			if rows, err := current.Get(ctx, []driver.Ref{ref}); err != nil || len(rows) != 1 || current.Snapshot().At.String() != "1" {
				t.Fatal("reopened current core/provider correspondence", err)
			}
			principal, _ := driver.PrincipalKey(restorePrincipal)
			for _, key := range []string{"active-only", "mixed"} {
				receipt, err := current.IdempotencyByKey(ctx, principal, "", "source-checkpoint-control", key)
				if err != nil || receipt == nil || receipt.RequestHash != "exact-request" {
					t.Fatal("atomic original receipt missing or changed after reopen", key, err)
				}
			}
		})
	}
}

func sourceCheckpointTx(t *testing.T, store driver.Store, lease driver.ActiveLease, key string, ref *model.Ref) (driver.WriteTx, driver.ChangesetHeader) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	h := driver.ChangesetHeader{Kind: model.ChangesetWrite, Incarnation: meta.Incarnation, Principal: restorePrincipal, Method: "source-checkpoint-control", IdempotencyKey: key, RequestHash: "exact-request"}
	var locks []driver.LockRequest
	if ref != nil {
		k := driver.LockKey{Kind: driver.LockRecord, Ref: ref}
		locks = []driver.LockRequest{{Key: k, Mode: driver.LockUpdate}}
		h.LockKeys = []driver.LockKey{k}
		h.Changes = []model.ChangeRecord{{Ref: *ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}}}
	}
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	return tx, h
}

func assertSourceCheckpoint(t *testing.T, snapshot driver.SourcePlaneSnapshot, namespace string, generation uint64, payload string) {
	t.Helper()
	value, err := snapshot.SourcePlaneCheckpoint(namespace)
	if err != nil || value.Generation != generation || string(value.Payload) != payload {
		t.Fatalf("snapshot checkpoint=%+v %v", value, err)
	}
	value.Payload[0] = '!'
	value, err = snapshot.SourcePlaneCheckpoint(namespace)
	if err != nil || !bytes.Equal(value.Payload, []byte(payload)) {
		t.Fatal("snapshot payload aliases caller", err)
	}
}

// The hook creates a real intervening publication before the adapter's
// certification transaction, proving it refuses torn observations.
type sourceSnapshotHookStore struct {
	driver.Store
	driver.CheckpointStore
	driver.HeadGuardedCheckpointStore
	beforeWrite func()
}

func (s *sourceSnapshotHookStore) Persistent() bool { return true }
func (s *sourceSnapshotHookStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if s.beforeWrite != nil {
		fn := s.beforeWrite
		s.beforeWrite = nil
		fn()
	}
	return s.Store.Write(ctx, lease)
}

func TestSourceCheckpointSQLSnapshotRejectsInterveningPublication(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, _ := openIdentityTarget(t, engine)
			cp := store.(driver.CheckpointStore)
			for _, kind := range []string{"checkpoint", "core"} {
				t.Run(kind, func(t *testing.T) {
					before := reverseState(t, store)
					hook := &sourceSnapshotHookStore{Store: store, CheckpointStore: cp, HeadGuardedCheckpointStore: store.(driver.HeadGuardedCheckpointStore)}
					hook.beforeWrite = func() {
						if kind == "core" {
							putOther(t, store, lease)
						} else if _, err := cp.SaveCheckpoint(ctx, lease, "workflow", 0, []byte("intervening")); err != nil {
							t.Fatal(err)
						}
					}
					adapter, err := driver.RequireWorkflowCheckpointStore(hook, lease)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := adapter.ReadSourcePlaneSnapshot(ctx, driver.View{Incarnation: before.Meta.Incarnation}, []string{"workflow"}); !errors.Is(err, driver.ErrCheckpointConflict) {
						t.Fatal("torn observations accepted", err)
					}
					after := reverseState(t, store)
					if kind == "checkpoint" && !reflect.DeepEqual(before, after) || kind == "core" && (after.Meta.Head.String() != "1" || len(after.Records) != 1) {
						t.Fatal("snapshot certification published an extra mutation")
					}
				})
			}
		})
	}
}

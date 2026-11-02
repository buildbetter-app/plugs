package s1b05b08

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB05RecordGuardedCheckpointConservesStateOnRefusal(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ref := claimForRace(t, f)
			before := f.state(t, ref)
			meta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			guard := f.store.(driver.RecordGuardedCheckpointStore)
			checkpoints := f.store.(driver.CheckpointStore)
			checkpointKey := "s1/guarded-record"
			saved, err := guard.SaveCheckpointIfRecord(f.ctx, f.lease, checkpointKey, 0, []byte("first"), ref, meta.Head, meta.Incarnation)
			if err != nil || saved.Generation != 1 {
				t.Fatalf("matching guard failed: %+v %v", saved, err)
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if _, err := guard.SaveCheckpointIfRecord(cancelled, f.lease, checkpointKey, 1, []byte("cancelled"), ref, meta.Head, meta.Incarnation); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled publication: %v", err)
			}
			if _, err := guard.SaveCheckpointIfRecord(f.ctx, f.lease, checkpointKey, 0, []byte("stale-generation"), ref, meta.Head, meta.Incarnation); !errors.Is(err, driver.ErrCheckpointConflict) {
				t.Fatalf("stale generation accepted: %v", err)
			}
			var changed driver.ErrIncarnationChanged
			if _, err := guard.SaveCheckpointIfRecord(f.ctx, f.lease, checkpointKey, 1, []byte("wrong-incarnation"), ref, meta.Head, "previous-incarnation"); !errors.As(err, &changed) {
				t.Fatalf("foreign incarnation accepted: %v", err)
			}
			if _, err := guard.SaveCheckpointIfRecord(f.ctx, f.lease, checkpointKey, 1, []byte("missing"), model.Ref{ID: "s1-absent"}, meta.Head, meta.Incarnation); !errors.Is(err, driver.ErrConflict) {
				t.Fatalf("missing record accepted: %v", err)
			}
			if after := f.state(t, ref); !reflect.DeepEqual(before, after) {
				t.Fatalf("checkpoint changed graph/history: before=%+v after=%+v", before, after)
			}
			if got := f.call(t, f.ctx, "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "new revision"}}, key("guard-update"), actor); got.Error != nil {
				t.Fatalf("record update failed: %s", got.Error.Data.Code)
			}
			if _, err := guard.SaveCheckpointIfRecord(f.ctx, f.lease, checkpointKey, 1, []byte("stale-record"), ref, meta.Head, meta.Incarnation); !errors.Is(err, driver.ErrConflict) {
				t.Fatalf("stale record accepted: %v", err)
			}
			if got, err := checkpoints.LoadCheckpoint(f.ctx, checkpointKey); err != nil || !reflect.DeepEqual(saved, got) {
				t.Fatalf("refused guard changed checkpoint: got=%+v err=%v", got, err)
			}
			f.reopen(t)
			if got, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, checkpointKey); err != nil || !reflect.DeepEqual(saved, got) {
				t.Fatalf("guarded checkpoint did not survive reopen: got=%+v err=%v", got, err)
			}
			latest, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			oldLease := f.lease
			if err := oldLease.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			var fenced driver.ErrFenced
			_, err = f.store.(driver.RecordGuardedCheckpointStore).SaveCheckpointIfRecord(f.ctx, oldLease, checkpointKey, 1, []byte("stale host"), ref, latest.Head, latest.Incarnation)
			if !errors.As(err, &fenced) {
				t.Fatalf("lost storage lease published metadata: %v", err)
			}
			if got, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, checkpointKey); err != nil || !reflect.DeepEqual(saved, got) {
				t.Fatalf("fenced guard changed checkpoint: got=%+v err=%v", got, err)
			}
		})
	}
}

func TestB05CheckpointGenerationIsRecheckedInsideCommit(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, initial := range []uint64{0, 1} {
				t.Run(model.Natural64(initial).String(), func(t *testing.T) {
					f := newFixture(t, engine)
					ref := f.create(t, "checkpoint-cas")
					checkpoints := f.store.(driver.CheckpointStore)
					checkpointKey := "s1/transaction-generation"
					if initial != 0 {
						if _, err := checkpoints.SaveCheckpoint(f.ctx, f.lease, checkpointKey, 0, []byte("initial")); err != nil {
							t.Fatal(err)
						}
					}
					before := f.state(t, ref)
					meta, err := f.store.Meta(f.ctx)
					if err != nil {
						t.Fatal(err)
					}
					tx, err := f.store.Write(f.ctx, f.lease)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Close()
					if _, err := tx.Lock(f.ctx, nil); err != nil {
						t.Fatal(err)
					}
					if _, err := tx.Head(f.ctx); err != nil {
						t.Fatal(err)
					}
					if err := tx.(driver.CheckpointGuardTx).ExpectCheckpoint(f.ctx, checkpointKey, initial); err != nil {
						t.Fatal(err)
					}
					newCheckpoint, err := checkpoints.SaveCheckpoint(f.ctx, f.lease, checkpointKey, initial, []byte("concurrent save"))
					if err != nil {
						t.Fatal(err)
					}
					_, err = tx.Commit(f.ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: anonymous})
					if !errors.Is(err, driver.ErrCheckpointConflict) {
						t.Fatalf("changed generation was not rejected in real Commit: %v", err)
					}
					if err := tx.Rollback(f.ctx); err != nil {
						t.Fatal(err)
					}
					if after := f.state(t, ref); !reflect.DeepEqual(before, after) {
						t.Fatalf("guard conflict changed graph/history: before=%+v after=%+v", before, after)
					}
					if got, err := checkpoints.LoadCheckpoint(f.ctx, checkpointKey); err != nil || !reflect.DeepEqual(newCheckpoint, got) {
						t.Fatalf("rollback undid independent checkpoint: got=%+v err=%v", got, err)
					}
				})
			}
		})
	}
}

type checkpointOnlyStore struct {
	driver.Store
	driver.CheckpointStore
}

type unguardedTransactionStore struct {
	driver.Store
	driver.CheckpointStore
}
type unguardedTransaction struct{ driver.WriteTx }

func (s unguardedTransactionStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return unguardedTransaction{tx}, nil
}

func TestB05MissingAtomicCapabilitiesFailClosed(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ref := claimForRace(t, f)
			before := f.state(t, ref)
			store := checkpointOnlyStore{f.store, f.store.(driver.CheckpointStore)}
			writer := pipeline.New(store, f.lease, pipeline.WithClock(func() time.Time { return f.now }))
			_, err := writer.Heartbeat(f.ctx, anonymous, ref, model.WriteMeta{Project: project, Agent: actor, IdempotencyKey: key("unsupported-heartbeat")})
			requireErrorCode(t, err, "internal_error")
			if got := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(ref.ID)); got.Generation != 0 {
				t.Fatal("unsupported heartbeat wrote an overlay")
			}
			f.now = f.now.Add(16 * time.Minute)
			withoutGuard := unguardedTransactionStore{f.store, f.store.(driver.CheckpointStore)}
			writer = pipeline.New(withoutGuard, f.lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithClock(func() time.Time { return f.now }))
			_, err = writer.ApplyClaimRelease(f.ctx, model.PrincipalRef{Kind: "system", ID: "core"}, ref, "", model.WriteMeta{Project: project, IdempotencyKey: key("unsupported-reclaim")})
			if !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("unsupported transaction did not fail closed: %v", err)
			}
			if after := f.state(t, ref); !reflect.DeepEqual(before, after) {
				t.Fatalf("unsupported capability changed durable state: before=%+v after=%+v", before, after)
			}
		})
	}
}

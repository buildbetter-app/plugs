package s1b05b08

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These adapters pause actual producer calls immediately before forwarding to
// the real driver's publication. They never manufacture a successful outcome.
type reclaimRaceStore struct {
	driver.Store
	driver.CheckpointStore
	overlayKey    string
	overlayPaused chan struct{}
	overlayResume chan struct{}
	commitPaused  chan struct{}
	commitResume  chan struct{}
	overlayOnce   sync.Once
	commitOnce    sync.Once
}

func (s *reclaimRaceStore) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, generation uint64, payload []byte) (driver.Checkpoint, error) {
	if key == s.overlayKey {
		s.overlayOnce.Do(func() {
			close(s.overlayPaused)
			select {
			case <-s.overlayResume:
			case <-ctx.Done():
			}
		})
	}
	return s.CheckpointStore.SaveCheckpoint(ctx, lease, key, generation, payload)
}

func (s *reclaimRaceStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &reclaimRaceTx{WriteTx: tx, store: s}, nil
}

type reclaimRaceTx struct {
	driver.WriteTx
	store *reclaimRaceStore
}

func (tx *reclaimRaceTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	if header.Method == "bead.release" && tx.store.commitPaused != nil {
		tx.store.commitOnce.Do(func() {
			close(tx.store.commitPaused)
			select {
			case <-tx.store.commitResume:
			case <-ctx.Done():
			}
		})
	}
	return tx.WriteTx.Commit(ctx, header)
}

func awaitRace[T any](t *testing.T, ctx context.Context, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-ctx.Done():
		t.Fatalf("real producer did not reach the required boundary: %v", ctx.Err())
		var zero T
		return zero
	}
}

type heartbeatRaceResult struct {
	outcome port.HeartbeatOutcome
	err     error
}

func pausedHeartbeat(t *testing.T, f *fixture, ref model.Ref, pauseCommit bool) (*reclaimRaceStore, <-chan heartbeatRaceResult, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	t.Cleanup(cancel)
	store := &reclaimRaceStore{
		Store: f.store, CheckpointStore: f.store.(driver.CheckpointStore), overlayKey: heartbeatCheckpointKey(ref.ID),
		overlayPaused: make(chan struct{}), overlayResume: make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-store.overlayResume:
		default:
			close(store.overlayResume)
		}
	})
	if pauseCommit {
		store.commitPaused, store.commitResume = make(chan struct{}), make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-store.commitResume:
			default:
				close(store.commitResume)
			}
		})
	}
	// The heartbeat starts while its claim is live. Its clock is immutable,
	// independently of the later recovery actor's observation.
	heartbeatNow := f.now.Add(4 * time.Minute)
	writer := pipeline.New(store, f.lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithClock(func() time.Time { return heartbeatNow }))
	svc := service.New(store, f.lease, writer)
	results := make(chan heartbeatRaceResult, 1)
	go func() {
		outcome, err := svc.HeartbeatWithOutcome(ctx, anonymous, service.HeartbeatRequest{ID: ref}, model.WriteMeta{Project: project, Agent: actor, AgentLabel: actor, IdempotencyKey: key("paused-heartbeat")})
		results <- heartbeatRaceResult{outcome, err}
	}()
	awaitRace(t, ctx, store.overlayPaused)
	return store, results, ctx
}

func claimForRace(t *testing.T, f *fixture) model.Ref {
	t.Helper()
	ref := f.create(t, "race-claim")
	if got := f.call(t, f.ctx, "bead.claim", map[string]any{"id": ref}, key("race-claim"), actor); got.Error != nil {
		t.Fatalf("claim refused: %s", got.Error.Data.Code)
	}
	return ref
}

func TestB05ReclaimRechecksHeartbeatAtRealCommit(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ref := claimForRace(t, f)
			store, heartbeat, ctx := pausedHeartbeat(t, f, ref, true)
			// Original expiry is 00:05; at 00:16 it is strictly beyond the
			// ten-minute grace. The in-flight heartbeat fixes expiry 00:09.
			f.now = f.now.Add(16 * time.Minute)
			writer := pipeline.New(store, f.lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithClock(func() time.Time { return f.now }))
			svc := service.New(store, f.lease, writer)
			reclaimed := make(chan error, 1)
			go func() {
				_, err := svc.ReleaseWithOutcome(ctx, model.PrincipalRef{Kind: "system", ID: "core"}, service.ReleaseRequest{ID: ref}, model.WriteMeta{Project: project, IdempotencyKey: key("race-reclaim")})
				reclaimed <- err
			}()
			awaitRace(t, ctx, store.commitPaused)
			close(store.overlayResume)
			beat := awaitRace(t, ctx, heartbeat)
			if beat.err != nil || beat.outcome.LeaseExpiresAt.IsZero() {
				t.Fatalf("authentic heartbeat did not publish: %+v %v", beat.outcome, beat.err)
			}
			before := f.state(t, ref)
			close(store.commitResume)
			err := awaitRace(t, ctx, reclaimed)
			var refusal *model.Error
			if !errors.As(err, &refusal) || refusal.Data.Code != "lease_expired" {
				t.Fatalf("reclaim ignored completed heartbeat/grace: err=%v", err)
			}
			if after := f.state(t, ref); !reflect.DeepEqual(before, after) {
				t.Fatalf("refused reclaim changed graph/history: before=%+v after=%+v", before, after)
			}
			row, err := f.service.Get(f.ctx, anonymous, service.GetRequest{ID: ref})
			if err != nil || row.Lease == nil || !row.Lease.ExpiresAt.Equal(beat.outcome.LeaseExpiresAt) {
				t.Fatalf("heartbeat claim not retained: row=%+v err=%v", row, err)
			}
		})
	}
}

func TestB05LateHeartbeatCannotPublishAfterReclaim(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ref := claimForRace(t, f)
			store, heartbeat, ctx := pausedHeartbeat(t, f, ref, false)
			f.now = f.now.Add(16 * time.Minute)
			_, err := f.service.ReleaseWithOutcome(ctx, model.PrincipalRef{Kind: "system", ID: "core"}, service.ReleaseRequest{ID: ref}, model.WriteMeta{Project: project, IdempotencyKey: key("reclaim-before-heartbeat")})
			if err != nil {
				t.Fatal(err)
			}
			before := f.state(t, ref)
			close(store.overlayResume)
			beat := awaitRace(t, ctx, heartbeat)
			var refusal *model.Error
			if !errors.As(beat.err, &refusal) || refusal.Data.Code != "lease_not_held" {
				t.Fatalf("late heartbeat succeeded for reclaimed claim: %+v err=%v", beat.outcome, beat.err)
			}
			if after := f.state(t, ref); !reflect.DeepEqual(before, after) {
				t.Fatalf("late heartbeat changed graph/history: before=%+v after=%+v", before, after)
			}
			if cp := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(ref.ID)); cp.Generation != 0 {
				t.Fatalf("late heartbeat published an orphan overlay: %+v", cp)
			}
			f.reopen(t)
			row, err := f.service.Get(f.ctx, anonymous, service.GetRequest{ID: ref})
			if err != nil || row.Lease != nil || row.Bead.Status != "todo" {
				t.Fatalf("reclaimed state did not survive reopen: row=%+v err=%v", row, err)
			}
		})
	}
}

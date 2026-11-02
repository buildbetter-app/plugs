package s1b05b08

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type heartbeatTestAuthorizer struct {
	mu         sync.Mutex
	stages     []port.AuthorizationStage
	denyBefore bool
	denyReplay bool
	hiddenID   string
}

func (a *heartbeatTestAuthorizer) Authorize(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, _ port.SnapshotReader) error {
	a.mu.Lock()
	a.stages = append(a.stages, stage)
	a.mu.Unlock()
	if len(proposal.Changes) != 1 {
		return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
	}
	id := proposal.Changes[0].Resource.Ref.ID
	if stage == port.AuthorizeBefore && (a.denyBefore || a.hiddenID == id) {
		return &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "current heartbeat permission denied"}}
	}
	if stage == port.AuthorizeReplay && a.denyReplay {
		return &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "heartbeat replay permission revoked"}}
	}
	return nil
}

func installHeartbeatService(f *fixture, authorizer port.Authorizer, installed func() bool) {
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(service.NewOntology(nil)),
		pipeline.WithClock(func() time.Time { return f.now }),
		pipeline.WithAuthorizer(authorizer),
	)
	options := []service.Option{
		service.WithOntology(service.NewOntology(nil)),
		service.WithClock(func() time.Time { return f.now }),
	}
	if installed != nil {
		options = append(options, service.WithAuthorizationInstalled(installed))
	}
	f.service = service.New(f.store, f.lease, f.writer, options...)
}

func heartbeatCheckpointKey(id string) string {
	digest := sha256.Sum256([]byte("plug/lease/" + id))
	return "plug/lease/" + hex.EncodeToString(digest[:])
}

func heartbeatReceiptCheckpointKey(principal, agent, method, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(principal + "\x00" + agent + "\x00" + method + "\x00" + idempotencyKey))
	return "plug/heartbeat/" + hex.EncodeToString(digest[:])
}

func loadHeartbeatCheckpoint(t *testing.T, f *fixture, key string) driver.Checkpoint {
	t.Helper()
	store, ok := f.store.(driver.CheckpointStore)
	if !ok {
		t.Fatal("fixture store does not expose checkpoint metadata")
	}
	value, err := store.LoadCheckpoint(f.ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func requireErrorCode(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s", code)
	}
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != code {
		t.Fatalf("error=%v want code=%s", err, code)
	}
}

func TestHeartbeatAuthorizationDenialRevocationAndHiddenNoWrite(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) { testHeartbeatAuthorizationNoWrite(t, engine) })
	}
}

func testHeartbeatAuthorizationNoWrite(t *testing.T, engine string) {
	f := newFixture(t, engine)
	id := f.create(t, "authorization target")
	second := f.create(t, "hidden target")
	for _, ref := range []model.Ref{id, second} {
		claim := f.call(t, f.ctx, "bead.claim", map[string]any{"id": ref}, key("auth-claim-"+ref.ID), actor)
		if claim.Error != nil {
			t.Fatalf("claim %s refused: %s", ref.ID, claim.Error.Data.Code)
		}
	}
	auth := &heartbeatTestAuthorizer{denyBefore: true}
	installHeartbeatService(f, auth, func() bool { return true })
	meta := model.WriteMeta{Project: project, Agent: actor, AgentLabel: actor, Method: "bead.heartbeat", IdempotencyKey: key("auth-denied")}
	before := f.state(t, id)
	if _, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta); err == nil {
		t.Fatal("denied heartbeat succeeded")
	} else {
		requireErrorCode(t, err, "forbidden")
	}
	if got := f.state(t, id); got.Head != before.Head || got.Versions != before.Versions || got.Changes != before.Changes || string(got.Record) != string(before.Record) {
		t.Fatalf("denied heartbeat changed graph: before=%+v after=%+v", before, got)
	}
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	if cp := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(id.ID)); cp.Generation != 0 || len(cp.Payload) != 0 {
		t.Fatalf("denied heartbeat wrote lease checkpoint: %+v", cp)
	}
	if cp := loadHeartbeatCheckpoint(t, f, heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", meta.IdempotencyKey)); cp.Generation != 0 || len(cp.Payload) != 0 {
		t.Fatalf("denied heartbeat wrote receipt checkpoint: %+v", cp)
	}

	auth.denyBefore = false
	auth.denyReplay = false
	meta.IdempotencyKey = key("auth-replay")
	_, err = f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta)
	if err != nil {
		t.Fatalf("authorized heartbeat refused: %v", err)
	}
	beforeReplay := f.state(t, id)
	leaseKey := heartbeatCheckpointKey(id.ID)
	receiptKey := heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", meta.IdempotencyKey)
	beforeLease := loadHeartbeatCheckpoint(t, f, leaseKey)
	beforeReceipt := loadHeartbeatCheckpoint(t, f, receiptKey)
	auth.denyBefore = true
	revokedMeta := meta
	revokedMeta.IdempotencyKey = key("auth-revoked-fresh")
	_, err = f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, revokedMeta)
	requireErrorCode(t, err, "forbidden")
	if cp := loadHeartbeatCheckpoint(t, f, heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", revokedMeta.IdempotencyKey)); cp.Generation != 0 || len(cp.Payload) != 0 {
		t.Fatalf("revoked fresh heartbeat wrote receipt: %+v", cp)
	}
	auth.denyBefore = false
	auth.denyReplay = true
	if _, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta); err == nil {
		t.Fatal("revoked replay succeeded")
	} else {
		requireErrorCode(t, err, "forbidden")
	}
	if got := f.state(t, id); got.Head != beforeReplay.Head || got.Versions != beforeReplay.Versions || got.Changes != beforeReplay.Changes || string(got.Record) != string(beforeReplay.Record) {
		t.Fatalf("revoked replay changed graph: before=%+v after=%+v", beforeReplay, got)
	}
	afterLease := loadHeartbeatCheckpoint(t, f, leaseKey)
	afterReceipt := loadHeartbeatCheckpoint(t, f, receiptKey)
	if afterLease.Generation != beforeLease.Generation || string(afterLease.Payload) != string(beforeLease.Payload) || afterReceipt.Generation != beforeReceipt.Generation || string(afterReceipt.Payload) != string(beforeReceipt.Payload) {
		t.Fatalf("revoked replay changed checkpoint metadata: before lease=%+v receipt=%+v after lease=%+v receipt=%+v", beforeLease, beforeReceipt, afterLease, afterReceipt)
	}

	auth.denyReplay = false
	auth.hiddenID = second.ID
	meta.IdempotencyKey = key("auth-hidden")
	beforeHidden := f.state(t, second)
	if _, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: second}, meta); err == nil {
		t.Fatal("hidden heartbeat succeeded")
	} else {
		requireErrorCode(t, err, "forbidden")
	}
	if got := f.state(t, second); got.Head != beforeHidden.Head || got.Versions != beforeHidden.Versions || got.Changes != beforeHidden.Changes || string(got.Record) != string(beforeHidden.Record) {
		t.Fatalf("hidden heartbeat changed graph: before=%+v after=%+v", beforeHidden, got)
	}
	if cp := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(second.ID)); cp.Generation != 0 || len(cp.Payload) != 0 {
		t.Fatalf("hidden heartbeat wrote lease checkpoint: %+v", cp)
	}
	if cp := loadHeartbeatCheckpoint(t, f, heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", meta.IdempotencyKey)); cp.Generation != 0 || len(cp.Payload) != 0 {
		t.Fatalf("hidden heartbeat wrote receipt checkpoint: %+v", cp)
	}
	// An absent row under the same visibility denial must not become a
	// not_found or lease-specific response either.
	auth.denyBefore = true
	_, err = f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: model.Ref{ID: "s1-missing"}}, meta)
	requireErrorCode(t, err, "forbidden")
	auth.denyBefore = false
	auth.hiddenID = id.ID
	meta.IdempotencyKey = key("auth-replay")
	_, err = f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta)
	requireErrorCode(t, err, "forbidden")
	if !reflect.DeepEqual(beforeLease, loadHeartbeatCheckpoint(t, f, leaseKey)) || !reflect.DeepEqual(beforeReceipt, loadHeartbeatCheckpoint(t, f, receiptKey)) {
		t.Fatal("hidden replay changed lease or receipt checkpoint")
	}
	auth.mu.Lock()
	stages := append([]port.AuthorizationStage(nil), auth.stages...)
	auth.mu.Unlock()
	wantStages := []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeBefore, port.AuthorizeBefore, port.AuthorizeBefore, port.AuthorizeReplay, port.AuthorizeBefore, port.AuthorizeBefore, port.AuthorizeBefore}
	if !reflect.DeepEqual(stages, wantStages) {
		t.Fatalf("authorization stages=%v", stages)
	}
	t.Logf("installed policy denied fresh, revoked, hidden, absent and replay requests; graph and checkpoint generations/bytes unchanged; stages=%v", stages)
}

func TestHeartbeatSameKeyDifferentTargetRefusesAndLeavesSecondTarget(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) { testHeartbeatSameKeyDifferentTarget(t, engine) })
	}
}

func testHeartbeatSameKeyDifferentTarget(t *testing.T, engine string) {
	f := newFixture(t, engine)
	first := f.create(t, "first target")
	second := f.create(t, "second target")
	for _, ref := range []model.Ref{first, second} {
		claim := f.call(t, f.ctx, "bead.claim", map[string]any{"id": ref}, key("same-key-claim-"+ref.ID), actor)
		if claim.Error != nil {
			t.Fatalf("claim %s refused: %s", ref.ID, claim.Error.Data.Code)
		}
	}
	f.now = f.now.Add(10 * time.Second)
	keyValue := key("same-target-key")
	baseMeta := model.WriteMeta{Project: project, Agent: actor, AgentLabel: actor, IdempotencyKey: keyValue}
	if _, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: first}, baseMeta); err != nil {
		t.Fatalf("first heartbeat refused: %v", err)
	}
	before := f.state(t, second)
	beforeLease := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(second.ID))
	secondCall := f.call(t, f.ctx, "bead.heartbeat", map[string]any{"id": second}, keyValue, actor)
	if secondCall.Error == nil || secondCall.Error.Data.Code != "idempotency_key_reused" {
		t.Fatalf("same-key changed-target result=%+v", secondCall)
	}
	after := f.state(t, second)
	afterLease := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(second.ID))
	if after.Head != before.Head || after.Versions != before.Versions || after.Changes != before.Changes || string(after.Record) != string(before.Record) || afterLease.Generation != beforeLease.Generation || string(afterLease.Payload) != string(beforeLease.Payload) {
		t.Fatalf("changed-target reuse mutated second target: before=%+v/%+v after=%+v/%+v", before, beforeLease, after, afterLease)
	}
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	receiptKey := heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", keyValue)
	beforeReceipt := loadHeartbeatCheckpoint(t, f, receiptKey)
	beforeFirstLease := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(first.ID))
	for _, parameter := range []string{"project", "return", "dryRun", "missingTarget"} {
		t.Run(parameter, func(t *testing.T) {
			meta := baseMeta
			target := first
			switch parameter {
			case "project":
				meta.Project = "different-project"
			case "return":
				meta.Return = "minimal"
			case "dryRun":
				meta.DryRun = true
			case "missingTarget":
				target = model.Ref{ID: "s1-missing"}
			}
			_, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: target}, meta)
			requireErrorCode(t, err, "idempotency_key_reused")
			if !reflect.DeepEqual(beforeReceipt, loadHeartbeatCheckpoint(t, f, receiptKey)) || !reflect.DeepEqual(beforeFirstLease, loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(first.ID))) || !reflect.DeepEqual(beforeLease, loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(second.ID))) || !reflect.DeepEqual(before, f.state(t, second)) {
				t.Fatal("changed-parameter reuse changed durable state")
			}
		})
	}
	t.Log("same scope/key with changed target, project, return or dryRun refused; graph, both lease checkpoints and stored receipt unchanged")
}

// delayedHeartbeatStore pauses a real checkpoint operation at a declared
// boundary. The winner publishes while the delayed request is in flight;
// reservation/publication schedules also force an actual driver CAS conflict.
type delayedHeartbeatKey struct{}
type delayedHeartbeatStore struct {
	driver.Store
	driver.CheckpointStore
	once      sync.Once
	phase     string
	paused    chan struct{}
	release   chan struct{}
	conflicts atomic.Int32
}

func (s *delayedHeartbeatStore) pause(ctx context.Context) error {
	s.once.Do(func() {
		close(s.paused)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	})
	return ctx.Err()
}

func (s *delayedHeartbeatStore) LoadCheckpoint(ctx context.Context, key string) (driver.Checkpoint, error) {
	value, err := s.CheckpointStore.LoadCheckpoint(ctx, key)
	if err == nil && s.phase == "lookup" && ctx.Value(delayedHeartbeatKey{}) == true && strings.HasPrefix(key, "plug/heartbeat/") {
		err = s.pause(ctx)
	}
	return value, err
}

func (s *delayedHeartbeatStore) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte) (driver.Checkpoint, error) {
	if ctx.Value(delayedHeartbeatKey{}) == true && strings.HasPrefix(key, "plug/heartbeat/") {
		var receipt struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(payload, &receipt); err != nil {
			return driver.Checkpoint{}, err
		}
		if s.phase == "reserve" && receipt.State == "reserved" || s.phase == "publish" && receipt.State == "published" {
			if err := s.pause(ctx); err != nil {
				return driver.Checkpoint{}, err
			}
		}
	}
	value, err := s.CheckpointStore.SaveCheckpoint(ctx, lease, key, expected, payload)
	if errors.Is(err, driver.ErrCheckpointConflict) && ctx.Value(delayedHeartbeatKey{}) == true {
		s.conflicts.Add(1)
	}
	return value, err
}

func TestHeartbeatConcurrentSameKeyReturnsStoredExpiry(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			for _, phase := range []string{"lookup", "reserve", "publish"} {
				t.Run(phase, func(t *testing.T) { testHeartbeatConcurrentSameKey(t, engine, phase) })
			}
		})
	}
}

func testHeartbeatConcurrentSameKey(t *testing.T, engine, phase string) {
	f := newFixture(t, engine)
	id := f.create(t, "interleaved heartbeat")
	claim := f.call(t, f.ctx, "bead.claim", map[string]any{"id": id}, key("interleaved-claim"), actor)
	if claim.Error != nil {
		t.Fatalf("claim refused: %s", claim.Error.Data.Code)
	}
	beforeGraph := f.state(t, id)
	store := &delayedHeartbeatStore{Store: f.store, CheckpointStore: f.store.(driver.CheckpointStore), phase: phase, paused: make(chan struct{}), release: make(chan struct{})}
	// Release the paused real read even if an assertion ends the test.
	var release sync.Once
	unblock := func() { release.Do(func() { close(store.release) }) }
	defer unblock()
	base := f.now.Add(10 * time.Second)
	newService := func(now time.Time) *service.Service {
		ontology := service.NewOntology(nil)
		writer := pipeline.New(store, f.lease, pipeline.WithOntology(ontology), pipeline.WithClock(func() time.Time { return now }))
		return service.New(store, f.lease, writer, service.WithOntology(ontology))
	}
	winner := newService(base)
	delayed := newService(base.Add(2 * time.Second))
	meta := model.WriteMeta{Project: project, Agent: actor, AgentLabel: actor, IdempotencyKey: key("interleaved-same-key")}
	type result struct {
		value port.HeartbeatOutcome
		err   error
	}
	ctx, cancel := context.WithCancel(context.WithValue(f.ctx, delayedHeartbeatKey{}, true))
	defer cancel()
	done := make(chan result, 1)
	go func() {
		value, err := delayed.HeartbeatWithOutcome(ctx, anonymous, service.HeartbeatRequest{ID: id}, meta)
		done <- result{value, err}
	}()
	select {
	case <-store.paused:
	case result := <-done:
		t.Fatalf("delayed heartbeat returned before its checkpoint pause: %+v", result)
	}
	first, err := winner.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta)
	wantExpiry := base.Add(5 * time.Minute)
	if phase == "publish" {
		// The delayed writer already reserved its later expiry before
		// the winner helps publish that exact response.
		wantExpiry = wantExpiry.Add(2 * time.Second)
	}
	if err != nil || !first.LeaseExpiresAt.Equal(wantExpiry) {
		t.Fatalf("winning heartbeat=%+v err=%v", first, err)
	}
	beforeLease := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(id.ID))
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	receiptKey := heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", meta.IdempotencyKey)
	beforeReceipt := loadHeartbeatCheckpoint(t, f, receiptKey)
	unblock()
	second := <-done
	if second.err != nil || !second.value.Replayed || !second.value.LeaseExpiresAt.Equal(first.LeaseExpiresAt) {
		t.Fatalf("interleaved replay=%+v err=%v winner=%+v", second.value, second.err, first)
	}
	afterLease := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(id.ID))
	if !reflect.DeepEqual(beforeLease, afterLease) {
		t.Fatalf("same-key loser rearmed overlay: before=%+v after=%+v", beforeLease, afterLease)
	}
	wantConflicts := int32(1)
	if phase == "lookup" {
		wantConflicts = 0
	}
	if store.conflicts.Load() != wantConflicts {
		t.Fatalf("real driver conflicts=%d want=%d", store.conflicts.Load(), wantConflicts)
	}
	receipt := loadHeartbeatCheckpoint(t, f, receiptKey)
	if !reflect.DeepEqual(beforeReceipt, receipt) {
		t.Fatalf("same-key loser rewrote receipt: before=%+v after=%+v", beforeReceipt, receipt)
	}
	var stored struct {
		LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
		State          string    `json:"state"`
	}
	if err := json.Unmarshal(receipt.Payload, &stored); err != nil || stored.State != "published" || !stored.LeaseExpiresAt.Equal(first.LeaseExpiresAt) {
		t.Fatalf("durable receipt=%s err=%v", receipt.Payload, err)
	}
	if engine == "sqlite" {
		f.reopen(t)
	}
	replay, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta)
	if err != nil || !replay.Replayed || !replay.LeaseExpiresAt.Equal(stored.LeaseExpiresAt) {
		t.Fatalf("durable retry=%+v err=%v stored=%s", replay, err, receipt.Payload)
	}
	if afterGraph := f.state(t, id); !reflect.DeepEqual(beforeGraph, afterGraph) {
		t.Fatalf("interleaved heartbeat changed graph: before=%+v after=%+v", beforeGraph, afterGraph)
	}
	t.Logf("schedule=%s winner=%s delayed-clock=%s stored=%s actualCASConflicts=%d overlayGeneration=%d unchanged; replay after reopen=%t", phase, first.LeaseExpiresAt, base.Add(2*time.Second), stored.LeaseExpiresAt, store.conflicts.Load(), afterLease.Generation, engine == "sqlite")
}

type interruptedHeartbeatStore struct {
	driver.Store
	driver.CheckpointStore
	phase string
	err   error
}

func (s *interruptedHeartbeatStore) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte) (driver.Checkpoint, error) {
	interrupt := s.phase == "overlay" && strings.HasPrefix(key, "plug/lease/")
	if s.phase == "publish" && strings.HasPrefix(key, "plug/heartbeat/") {
		var receipt struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(payload, &receipt); err != nil {
			return driver.Checkpoint{}, err
		}
		interrupt = receipt.State == "published"
	}
	if interrupt {
		return driver.Checkpoint{}, s.err
	}
	return s.CheckpointStore.SaveCheckpoint(ctx, lease, key, expected, payload)
}

func TestHeartbeatInterruptedPublicationKeepsReservedOutcome(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			for _, phase := range []string{"overlay", "publish"} {
				t.Run(phase, func(t *testing.T) {
					f := newFixture(t, engine)
					id := f.create(t, "interrupted heartbeat")
					if claim := f.call(t, f.ctx, "bead.claim", map[string]any{"id": id}, key("interrupted-claim"), actor); claim.Error != nil {
						t.Fatalf("claim refused: %s", claim.Error.Data.Code)
					}
					beforeGraph := f.state(t, id)
					f.now = f.now.Add(10 * time.Second)
					reservedAt := f.now
					injected := errors.New("checkpoint publication interrupted")
					store := &interruptedHeartbeatStore{Store: f.store, CheckpointStore: f.store.(driver.CheckpointStore), phase: phase, err: injected}
					writer := pipeline.New(store, f.lease, pipeline.WithClock(func() time.Time { return f.now }))
					svc := service.New(store, f.lease, writer)
					meta := model.WriteMeta{Project: project, Agent: actor, AgentLabel: actor, IdempotencyKey: key("interrupted-heartbeat")}
					if _, err := svc.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta); !errors.Is(err, injected) {
						t.Fatalf("interruption result=%v", err)
					}
					principal, err := driver.PrincipalKey(anonymous)
					if err != nil {
						t.Fatal(err)
					}
					receiptKey := heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", meta.IdempotencyKey)
					reserved := loadHeartbeatCheckpoint(t, f, receiptKey)
					var stored struct {
						State          string    `json:"state"`
						LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
					}
					if err := json.Unmarshal(reserved.Payload, &stored); err != nil || stored.State != "reserved" || !stored.LeaseExpiresAt.Equal(reservedAt.Add(5*time.Minute)) {
						t.Fatalf("lost immutable reservation: %s err=%v", reserved.Payload, err)
					}
					beforeOverlay := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(id.ID))
					f.now = f.now.Add(2 * time.Second)
					if engine == "sqlite" {
						f.reopen(t)
					}
					recovered, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta)
					if err != nil || !recovered.Replayed || !recovered.LeaseExpiresAt.Equal(stored.LeaseExpiresAt) {
						t.Fatalf("recovered=%+v err=%v reserved=%s", recovered, err, reserved.Payload)
					}
					afterOverlay := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(id.ID))
					var overlay struct {
						ExpiresAt     time.Time `json:"expiresAt"`
						LastHeartbeat time.Time `json:"lastHeartbeat"`
					}
					if err := json.Unmarshal(afterOverlay.Payload, &overlay); err != nil || !overlay.ExpiresAt.Equal(stored.LeaseExpiresAt) || !overlay.LastHeartbeat.Equal(reservedAt) {
						t.Fatalf("retry rearmed reserved overlay: %s err=%v", afterOverlay.Payload, err)
					}
					if phase == "publish" && !reflect.DeepEqual(beforeOverlay, afterOverlay) {
						t.Fatal("retry after partial publication rewrote overlay")
					}
					published := loadHeartbeatCheckpoint(t, f, receiptKey)
					if err := json.Unmarshal(published.Payload, &stored); err != nil || stored.State != "published" || !stored.LeaseExpiresAt.Equal(recovered.LeaseExpiresAt) {
						t.Fatalf("recovery did not publish stored response: %s err=%v", published.Payload, err)
					}
					if !reflect.DeepEqual(beforeGraph, f.state(t, id)) {
						t.Fatal("interruption/recovery changed graph")
					}
					t.Logf("interrupted %s publication retained generation=%d reservation; retry/reopen published original expiry=%s and heartbeat time=%s", phase, reserved.Generation, recovered.LeaseExpiresAt, overlay.LastHeartbeat)
				})
			}
		})
	}
}

func TestHeartbeatInstalledAuthorizationUnavailableFailsClosed(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			id := f.create(t, "unsupported installed policy")
			if claim := f.call(t, f.ctx, "bead.claim", map[string]any{"id": id}, key("unsupported-claim"), actor); claim.Error != nil {
				t.Fatalf("claim refused: %s", claim.Error.Data.Code)
			}
			before := f.state(t, id)
			meta := model.WriteMeta{Project: project, Agent: actor, AgentLabel: actor, IdempotencyKey: key("unsupported-policy")}
			installHeartbeatService(f, nil, func() bool { return true })
			_, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta)
			requireErrorCode(t, err, "authz_unavailable")
			if after := f.state(t, id); !reflect.DeepEqual(before, after) {
				t.Fatalf("unavailable auth changed graph: %+v", after)
			}
			if cp := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(id.ID)); cp.Generation != 0 {
				t.Fatalf("unavailable auth wrote lease: %+v", cp)
			}
			principal, err := driver.PrincipalKey(anonymous)
			if err != nil {
				t.Fatal(err)
			}
			if cp := loadHeartbeatCheckpoint(t, f, heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", meta.IdempotencyKey)); cp.Generation != 0 {
				t.Fatalf("unavailable auth wrote receipt: %+v", cp)
			}
			installHeartbeatService(f, nil, func() bool { return false })
			if _, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta); err != nil {
				t.Fatalf("explicit no-plugin heartbeat refused: %v", err)
			}
		})
	}
}

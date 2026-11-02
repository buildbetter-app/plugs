package s1b05b08

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	mu          sync.Mutex
	stages      []port.AuthorizationStage
	denyBefore  bool
	denyReplay  bool
	hiddenID    string
	beforeGate  chan struct{}
	gateRelease chan struct{}
	gateCount   atomic.Int32
}

func (a *heartbeatTestAuthorizer) Authorize(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, _ port.SnapshotReader) error {
	a.mu.Lock()
	a.stages = append(a.stages, stage)
	a.mu.Unlock()
	if stage == port.AuthorizeBefore && a.beforeGate != nil {
		if a.gateCount.Add(1) == 2 {
			close(a.gateRelease)
		}
		select {
		case <-a.gateRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
	f := newFixture(t, "memory")
	id := f.create(t, "authorization target")
	second := f.create(t, "hidden target")
	for _, ref := range []model.Ref{id, second} {
		claim := f.call(t, f.ctx, "bead.claim", map[string]any{"id": ref}, key("auth-claim-"+ref.ID), actor)
		if claim.Error != nil {
			t.Fatalf("claim %s refused: %s", ref.ID, claim.Error.Data.Code)
		}
	}
	auth := &heartbeatTestAuthorizer{denyBefore: true}
	installHeartbeatService(f, auth, nil)
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
	if cp := loadHeartbeatCheckpoint(t, f, heartbeatCheckpointKey(id)); cp.Generation != 0 || len(cp.Payload) != 0 {
		t.Fatalf("denied heartbeat wrote lease checkpoint: %+v", cp)
	}
	if cp := loadHeartbeatCheckpoint(t, f, heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", meta.IdempotencyKey)); cp.Generation != 0 || len(cp.Payload) != 0 {
		t.Fatalf("denied heartbeat wrote receipt checkpoint: %+v", cp)
	}

	auth.denyBefore = false
	auth.denyReplay = false
	meta.IdempotencyKey = key("auth-replay")
	first, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, meta)
	if err != nil {
		t.Fatalf("authorized heartbeat refused: %v", err)
	}
	beforeReplay := f.state(t, id)
	leaseKey := heartbeatCheckpointKey(id)
	receiptKey := heartbeatReceiptCheckpointKey(principal, actor, "bead.heartbeat", meta.IdempotencyKey)
	beforeLease := loadHeartbeatCheckpoint(t, f, leaseKey)
	beforeReceipt := loadHeartbeatCheckpoint(t, f, receiptKey)
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
	auth.mu.Lock()
	stages := append([]port.AuthorizationStage(nil), auth.stages...)
	auth.mu.Unlock()
	if len(stages) < 5 || stages[0] != port.AuthorizeBefore || stages[1] != port.AuthorizeBefore || stages[2] != port.AuthorizeBefore || stages[3] != port.AuthorizeReplay || stages[4] != port.AuthorizeBefore {
		t.Fatalf("authorization stages=%v", stages)
	}
	_ = first
}

func TestHeartbeatSameKeyDifferentTargetRefusesAndLeavesSecondTarget(t *testing.T) {
	f := newFixture(t, "memory")
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
	initial := f.call(t, f.ctx, "bead.heartbeat", map[string]any{"id": first}, keyValue, actor)
	if initial.Error != nil {
		t.Fatalf("first heartbeat refused: %s", initial.Error.Data.Code)
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
}

func TestHeartbeatConcurrentSameKeyReturnsStoredExpiry(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			id := f.create(t, "interleaved heartbeat")
			claim := f.call(t, f.ctx, "bead.claim", map[string]any{"id": id}, key("interleaved-claim"), actor)
			if claim.Error != nil {
				t.Fatalf("claim refused: %s", claim.Error.Data.Code)
			}
			auth := &heartbeatTestAuthorizer{beforeGate: make(chan struct{}), gateRelease: make(chan struct{})}
			var tick atomic.Int32
			base := f.now.Add(10 * time.Second)
			f.writer = pipeline.New(f.store, f.lease,
				pipeline.WithOntology(service.NewOntology(nil)),
				pipeline.WithClock(func() time.Time {
					if tick.Add(1) == 1 {
						return base
					}
					return base.Add(2 * time.Second)
				}),
				pipeline.WithAuthorizer(auth),
			)
			f.service = service.New(f.store, f.lease, f.writer, service.WithOntology(service.NewOntology(nil)), service.WithClock(func() time.Time { return f.now }))
			start := make(chan struct{})
			outcomes := make(chan port.HeartbeatOutcome, 2)
			errs := make(chan error, 2)
			for i := 0; i < 2; i++ {
				go func() {
					<-start
					outcome, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, model.WriteMeta{Project: project, Agent: actor, AgentLabel: actor, Method: "bead.heartbeat", IdempotencyKey: key("interleaved-same-key")})
					outcomes <- outcome
					errs <- err
				}()
			}
			close(start)
			var got [2]port.HeartbeatOutcome
			for i := range got {
				got[i] = <-outcomes
				if err := <-errs; err != nil {
					t.Fatalf("concurrent heartbeat %d failed: %v", i, err)
				}
			}
			if !got[0].LeaseExpiresAt.Equal(got[1].LeaseExpiresAt) {
				t.Fatalf("concurrent responses disagreed: %+v", got)
			}
			replay, err := f.service.HeartbeatWithOutcome(f.ctx, anonymous, service.HeartbeatRequest{ID: id}, model.WriteMeta{Project: project, Agent: actor, AgentLabel: actor, Method: "bead.heartbeat", IdempotencyKey: key("interleaved-same-key")})
			if err != nil || !replay.Replayed || !replay.LeaseExpiresAt.Equal(got[0].LeaseExpiresAt) {
				t.Fatalf("stored replay=%+v err=%v concurrent=%+v", replay, err, got)
			}
		})
	}
}

package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var claimRecoverySystem = model.PrincipalRef{Kind: "system", ID: "core"}

func TestExpiredClaimRecoveryReopensAndAttributesRealEvent(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id, _ := create(t, f, "recover me")
	claimMeta := meta(t)
	claimed, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, claimMeta)
	if err != nil {
		t.Fatal(err)
	}
	// The fixed default grace is twice the five-minute claim TTL. The strict
	// expiry-before-cutoff boundary therefore needs one nanosecond past 15m.
	f.now = f.now.Add(15*time.Minute + time.Nanosecond)
	reclaimMeta := meta(t)
	reclaim, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id, Reason: "worker expired"}, reclaimMeta)
	if err != nil || reclaim.Replayed {
		t.Fatalf("reclaim outcome=%+v err=%v", reclaim, err)
	}
	if reclaim.Commit.Revision.String() == claimed.Commit.Revision.String() {
		t.Fatal("reclaim did not publish a new revision")
	}
	got, err := f.svc.Get(ctx, claimRecoverySystem, GetRequest{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	initial, _ := f.svc.ontology.effective("local").InitialStatus("issue")
	if got.Bead.Status != initial || got.Lease != nil {
		t.Fatalf("recovered bead=%+v lease=%+v", got.Bead, got.Lease)
	}
	reader, err := f.store.Read(ctx, storeView(t, f))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	changesets, err := reader.Changesets(ctx, nil, 20)
	if err != nil || len(changesets) != 3 {
		t.Fatalf("changesets=%+v err=%v", changesets, err)
	}
	entry := changesets[2]
	if entry.Principal.Kind != claimRecoverySystem.Kind || entry.Principal.ID != claimRecoverySystem.ID || len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadLeaseReclaimed {
		t.Fatalf("recovery event=%+v", entry)
	}
	// The explicit release is idempotent through the real durable receipt.
	replay, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id, Reason: "worker expired"}, reclaimMeta)
	if err != nil || !replay.Replayed || replay.Commit.Revision.String() != reclaim.Commit.Revision.String() {
		t.Fatalf("reclaim replay=%+v err=%v", replay, err)
	}
	// After the recovery, a normal actual claim enters the normal claim slot.
	bobMeta := meta(t)
	bobMeta.AgentLabel = "bob"
	bobMeta.Agent = "bob"
	claimedAgain, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, bobMeta)
	if err != nil {
		t.Fatal(err)
	}
	if claimedAgain.LeaseTTL != 5*time.Minute {
		t.Fatalf("normal claim after recovery=%+v", claimedAgain)
	}
}

func TestClaimRecoveryHonorsLiveAndGraceBoundariesAndAuthorization(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id, _ := create(t, f, "boundary")
	claimMeta := meta(t)
	if _, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, claimMeta); err != nil {
		t.Fatal(err)
	}
	foreignMeta := meta(t)
	foreignMeta.AgentLabel = "bob"
	foreignMeta.Agent = "bob"
	_, err := f.svc.ReleaseWithOutcome(ctx, anonymous, ReleaseRequest{ID: id}, foreignMeta)
	wantCode(t, err, "lease_not_held")
	// Expiry alone and the full grace boundary are both refusals. The stored
	// claim remains unchanged because the refusal occurs before Writer.Apply.
	f.now = f.now.Add(5*time.Minute + time.Nanosecond)
	before, err := f.svc.Get(ctx, claimRecoverySystem, GetRequest{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, meta(t))
	wantCode(t, err, "lease_expired")
	f.now = f.now.Add(10*time.Minute - time.Nanosecond)
	_, err = f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, meta(t))
	wantCode(t, err, "lease_expired")
	after, err := f.svc.Get(ctx, claimRecoverySystem, GetRequest{ID: id})
	if err != nil || before.Bead.Revision.String() != after.Bead.Revision.String() || after.Lease == nil {
		t.Fatalf("refusal changed claim before=%+v after=%+v err=%v", before, after, err)
	}
}

func storeView(t *testing.T, f *fixture) (v driver.View) {
	t.Helper()
	m, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return driver.View{Incarnation: m.Incarnation}
}

type recoveryAuthorizer struct{ calls atomic.Int32 }

func (a *recoveryAuthorizer) Authorize(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error {
	a.calls.Add(1)
	return failure("forbidden", "ordinary callers denied")
}

func TestClaimRecoveryUsesCurrentServerEpochAfterFailover(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id, _ := create(t, f, "previous server claim")
	if _, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, meta(t)); err != nil {
		t.Fatal(err)
	}
	if err := f.lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	var err error
	f.lease, err = f.store.Lease(ctx, "replacement", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.lease.Release(ctx) })
	f.now = f.now.Add(15*time.Minute + time.Nanosecond)
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(f.svc.ontology), pipeline.WithClock(func() time.Time { return f.now }))
	f.svc = New(f.store, f.lease, f.writer, WithOntology(f.svc.ontology), WithClock(func() time.Time { return f.now }))
	if _, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, meta(t)); err != nil {
		t.Fatalf("active replacement cannot recover expired previous-epoch claim: %v", err)
	}
}

func TestClaimRecoveryCoreWorkNeverEntersAuthorizationChain(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	id, _ := create(t, f, "core invariant work")
	if _, err := f.svc.Claim(ctx, anonymous, ClaimRequest{ID: id}, meta(t)); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(15*time.Minute + time.Nanosecond)
	auth := &recoveryAuthorizer{}
	f.svc.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(f.svc.ontology), pipeline.WithClock(func() time.Time { return f.now }), pipeline.WithAuthorizer(auth))
	key := meta(t)
	if _, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, key); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.ReleaseWithOutcome(ctx, claimRecoverySystem, ReleaseRequest{ID: id}, key); err != nil {
		t.Fatal(err)
	}
	if auth.calls.Load() != 0 {
		t.Fatal("core recovery entered authorization chain")
	}
}

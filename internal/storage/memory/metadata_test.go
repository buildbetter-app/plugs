package memory

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestCheckpointOverflowAndCancellationPublishNothing(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	s.checkpoints["overflow"] = driver.Checkpoint{Generation: math.MaxUint64, Payload: []byte("retained")}
	if _, err := s.SaveCheckpoint(ctx, lease, "overflow", math.MaxUint64, []byte("replacement")); err == nil {
		t.Fatal("generation overflow accepted")
	}
	value, err := s.LoadCheckpoint(ctx, "overflow")
	if err != nil || value.Generation != math.MaxUint64 || string(value.Payload) != "retained" {
		t.Fatalf("overflow published %+v %v", value, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.SaveCheckpoint(cancelled, lease, "new", 0, []byte("bad")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	value, err = s.LoadCheckpoint(ctx, "new")
	if err != nil || value.Generation != 0 || value.Payload != nil {
		t.Fatal("cancelled checkpoint published")
	}
	meta, _ := s.Meta(ctx)
	if !meta.Head.IsZero() {
		t.Fatal("metadata failure changed graph head")
	}
}

func TestIdempotencyWindowAndAuthenticatedScope(t *testing.T) {
	ctx := context.Background()
	fixed := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	var elapsed atomic.Int64
	s, err := NewWithOptions(Options{Clock: func() time.Time { return fixed.Add(time.Duration(elapsed.Load())) }, StoreID: "clock-test", Incarnation: "clock-incarnation"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lease, err := s.Lease(ctx, "test", 72*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	firstRef := model.RefByID("pl-original", model.Natural64(0))
	response := model.Commit{Results: []model.OpResult{{Ref: firstRef}}, Warnings: []model.Warning{}}
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "user", ID: "u"}, AgentLabel: "label-a", Method: "bead.create", IdempotencyKey: "key", RequestHash: "hash", Response: response}
	first, err := conformance.Seed(ctx, s, lease, h, []conformance.FixtureChange{{Ref: firstRef, Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Results[0].Revision.IsZero() {
		t.Fatal("commit mutated caller response")
	}
	h.AgentLabel = "label-b"
	secondRef := model.RefByID("pl-unpublished", model.Natural64(0))
	replay, err := conformance.Seed(ctx, s, lease, h, []conformance.FixtureChange{{Ref: secondRef, Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}})
	if err != nil || !reflect.DeepEqual(replay, first) {
		t.Fatalf("authenticated agent label split scope: %+v %v", replay, err)
	}
	meta, _ := s.Meta(ctx)
	r, _ := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer r.Close()
	cached, err := r.IdempotencyByKey(ctx, "user:u", "any-label", "bead.create", "key")
	if err != nil || cached == nil || cached.Commit.Results[0].Ref.ID != firstRef.ID {
		t.Fatalf("cached=%+v %v", cached, err)
	}
	elapsed.Store(int64(driver.IdempotencyWindow - time.Nanosecond))
	cached, err = r.IdempotencyByKey(ctx, "user:u", "", "bead.create", "key")
	if err != nil || cached == nil {
		t.Fatal("dedup expired before 24h")
	}
	elapsed.Store(int64(driver.IdempotencyWindow))
	cached, err = r.IdempotencyByKey(ctx, "user:u", "", "bead.create", "key")
	if err != nil || cached != nil {
		t.Fatal("dedup did not expire at 24h")
	}
	h.Response = model.Commit{Results: []model.OpResult{{Ref: secondRef}}, Warnings: []model.Warning{}}
	h.RequestHash = "new-hash"
	second, err := conformance.Seed(ctx, s, lease, h, []conformance.FixtureChange{{Ref: secondRef, Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}})
	if err != nil || second.Revision.String() != "2" {
		t.Fatalf("expired key reuse=%+v %v", second, err)
	}
	r, _ = s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer r.Close()
	log, err := r.Changesets(ctx, nil, 100)
	if err != nil || len(log) != 2 || log[0].IdempotencyKey != "key" || log[1].IdempotencyKey != "key" {
		t.Fatalf("log lost retained keys=%v %v", log, err)
	}
}

func TestResponseFinalizerFencedAndAtomic(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID("pl-response", model.Natural64(0))
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "user", ID: "u"}, Method: "bead.create", IdempotencyKey: "key", RequestHash: "hash", Response: model.Commit{Results: []model.OpResult{{Ref: ref}}, Warnings: []model.Warning{{Code: "subscriber_skipped", Message: "initial"}}}}
	entry, err := conformance.Seed(ctx, s, lease, h, []conformance.FixtureChange{{Ref: ref, Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	other := New()
	defer other.Close()
	foreign, err := other.Lease(ctx, "foreign", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	warnings := []model.Warning{{Code: "post_subscriber_failed", Message: "post"}}
	var fenced driver.ErrFenced
	if err := s.FinalizeResponse(ctx, foreign, "user:u", "", "bead.create", "key", entry.Revision, warnings); !errors.As(err, &fenced) {
		t.Fatalf("foreign finalize=%v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := s.Lease(ctx, "takeover", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeResponse(ctx, lease, "user:u", "", "bead.create", "key", entry.Revision, warnings); !errors.As(err, &fenced) {
		t.Fatalf("stale finalize=%v", err)
	}
	meta, _ := s.Meta(ctx)
	r, _ := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer r.Close()
	cached, _ := r.IdempotencyByKey(ctx, "user:u", "", "bead.create", "key")
	if len(cached.Commit.Warnings) != 1 {
		t.Fatal("fenced finalize published warnings")
	}
	if err := s.FinalizeResponse(ctx, next, "user:u", "", "bead.create", "key", entry.Revision, warnings); err != nil {
		t.Fatal(err)
	}
	r, _ = s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer r.Close()
	cached, _ = r.IdempotencyByKey(ctx, "user:u", "", "bead.create", "key")
	if len(cached.Commit.Warnings) != 2 || cached.Commit.Warnings[0].Message != "initial" || cached.Commit.Warnings[1].Message != "post" {
		t.Fatalf("final response=%+v", cached)
	}
	if err := s.FinalizeResponse(ctx, next, "user:u", "", "missing", "key", entry.Revision, nil); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("missing scope=%v", err)
	}
	newMeta, _ := s.Meta(ctx)
	if newMeta.Head.String() != meta.Head.String() {
		t.Fatal("finalizer changed graph head")
	}
}

func TestOrdinaryResponsePreservesUnchangedRowRevision(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	oldRef, newRef := model.RefByID("pl-old", model.Natural64(0)), model.RefByID("pl-new", model.Natural64(0))
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: oldRef, Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	results := []model.OpResult{{Ref: oldRef, Revision: model.Natural64(1)}, {Ref: newRef, Revision: model.Natural64(2)}}
	h := driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "test"}, Method: "bead.batch", IdempotencyKey: "mixed", RequestHash: "digest", Response: model.Commit{Results: results, Warnings: []model.Warning{}}}
	if _, err := conformance.Seed(ctx, s, lease, h, []conformance.FixtureChange{{Ref: newRef, Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	meta, _ := s.Meta(ctx)
	r, _ := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer r.Close()
	response, err := r.IdempotencyByKey(ctx, "system:test", "", h.Method, h.IdempotencyKey)
	if err != nil || response == nil || response.Commit.Revision.String() != "2" || !reflect.DeepEqual(response.Commit.Results, results) {
		t.Fatalf("cached response changed row revisions: %+v %v", response, err)
	}
}

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) driver.Store { s := New(); t.Cleanup(func() { _ = s.Close() }); return s })
}

func TestConnectedStorageCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	l, err := s.Lease(ctx, "test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := s.Meta(ctx)
	ref := model.RefByID("pl-checkpoint", model.Natural64(0))
	key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
	sentinel, _ := driver.NewBeadSentinel(ref.ID)
	sentinelKey := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
	tx, err := s.Write(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, []driver.LockRequest{{Key: key, Mode: driver.LockUpdate}, {Key: sentinelKey, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{"type":"task","fields":{"title":"checkpoint"}}`)}); err != nil {
		t.Fatal(err)
	}
	if err = tx.BumpSentinels(ctx, []driver.Sentinel{sentinel}); err != nil {
		t.Fatal(err)
	}
	entry, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: m.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "anonymous", ID: ""}, LockKeys: []driver.LockKey{key, sentinelKey}, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}})
	if err != nil {
		t.Fatal(err)
	}
	if entry.Revision.String() != "1" {
		t.Fatalf("revision=%s", entry.Revision)
	}
	r, err := s.Read(ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	records, err := r.Get(ctx, []driver.Ref{ref})
	if err != nil || len(records) != 1 {
		t.Fatalf("Get=%v %v", records, err)
	}
	versions, err := r.Versions(ctx, ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 1 || !versions.Items[0].Base {
		t.Fatalf("Versions=%v %v", versions, err)
	}
	changes, err := r.Changesets(ctx, nil, 100)
	if err != nil || len(changes) != 1 {
		t.Fatalf("Changesets=%v %v", changes, err)
	}
}

func TestDeterministicOptions(t *testing.T) {
	ctx := context.Background()
	fixed := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	s, err := NewWithOptions(Options{Clock: func() time.Time { return fixed }, StoreID: "fixture-store", Incarnation: "fixture-incarnation"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m, err := s.Meta(ctx)
	if err != nil || m.StoreID != "fixture-store" || m.Incarnation != "fixture-incarnation" {
		t.Fatalf("Meta=%+v error=%v", m, err)
	}
	lease, err := s.Lease(ctx, "fixture", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Lock(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	entry, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: m.Incarnation, Kind: model.ChangesetDerived, Principal: model.PrincipalRef{Kind: "system", ID: "fixture"}})
	if err != nil || !entry.Time.Equal(fixed) {
		t.Fatalf("Commit=%+v error=%v", entry, err)
	}
	if _, err := NewWithOptions(Options{StoreID: "bad\x00id"}); err == nil {
		t.Fatal("invalid identity accepted")
	}
}

func TestStorageFixtureKeepsChangeOrderAndTime(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "fixture", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	fixed := time.Date(2026, 10, 1, 2, 3, 4, 0, time.UTC)
	h := driver.ChangesetHeader{Time: fixed, Principal: model.PrincipalRef{Kind: "system", ID: "fixture"}}
	changes := []conformance.FixtureChange{{Ref: model.RefByID("pl-z", model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: []byte(`{"value":1}`)}, {Ref: model.RefByID("pl-a", model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: []byte(`{"value":2}`)}}
	entry, err := conformance.Seed(ctx, s, lease, h, changes)
	if err != nil {
		t.Fatal(err)
	}
	if !entry.Time.Equal(fixed) || len(entry.Changes) != 2 || entry.Changes[0].Ref.ID != "pl-z" || entry.Changes[1].Ref.ID != "pl-a" {
		t.Fatalf("fixture=%+v", entry)
	}
	changes = []conformance.FixtureChange{{Ref: changes[0].Ref, Kind: "bead", Op: model.OpPatch, Body: []byte(`{"value":3}`)}}
	if err := conformance.SeedRollback(ctx, s, lease, h, changes); err != nil {
		t.Fatal(err)
	}
	m, _ := s.Meta(ctx)
	if m.Head.String() != "1" {
		t.Fatalf("rollback advanced head=%s", m.Head)
	}
	entry, err = conformance.Seed(ctx, s, lease, h, changes)
	if err != nil || entry.Changes[0].FromRevision.String() != "1" {
		t.Fatalf("patch fixture=%+v %v", entry, err)
	}
	changes[0].Op = model.OpDelete
	changes[0].Body = nil
	entry, err = conformance.Seed(ctx, s, lease, h, changes)
	if err != nil || entry.Changes[0].FromRevision.String() != "2" {
		t.Fatalf("delete fixture=%+v %v", entry, err)
	}
}

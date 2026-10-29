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

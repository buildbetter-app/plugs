package memory

import (
	"context"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestReaderCloseReleasesPinAndCanRaceReads(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID("pl-close", model.Natural64(0))
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	meta, _ := s.Meta(ctx)
	snapshot, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	failures := make(chan string, 16)
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for j := 0; j < 20; j++ {
				records, err := snapshot.Get(ctx, []driver.Ref{ref})
				if err == nil && (len(records) != 1 || string(records[0].Body) != `{}`) {
					failures <- "corrupt read racing close"
					return
				}
			}
		}()
	}
	close(start)
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
	r := snapshot.(*reader)
	if r.state != nil || r.now != nil {
		t.Fatal("closed reader retained its pinned state/clock")
	}
	if view := snapshot.Snapshot(); view.At == nil || view.At.String() != "1" || view.Incarnation != meta.Incarnation {
		t.Fatalf("copied snapshot metadata=%+v", view)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreCloseReleasesLeaseAndTransactionPins(t *testing.T) {
	ctx := context.Background()
	s := New()
	lease, err := s.Lease(ctx, "test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Lock(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	internal := tx.(*transaction)
	if internal.base != nil || internal.records != nil || internal.acquired != nil {
		t.Fatal("closed transaction retained snapshot/staged data/locks")
	}
	if tx.Snapshot().At == nil {
		t.Fatal("closed transaction lost copied snapshot metadata")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lease.Lost():
	default:
		t.Fatal("store close retained active lease")
	}
	if s.state != nil || s.checkpoints != nil || s.active != nil || len(s.transactions) != 0 {
		t.Fatal("closed store retained handles/state")
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Meta(ctx); err == nil {
		t.Fatal("closed store admitted metadata read")
	}
	if _, err := s.Read(ctx, driver.View{}); err == nil {
		t.Fatal("closed store admitted reader")
	}
	if _, err := s.Write(ctx, lease); err == nil {
		t.Fatal("closed store admitted transaction")
	}
}

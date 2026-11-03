package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAllocatorSnapshotLegacyMemoryCoverageAndNoReadPublication(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "allocator-history-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	key, _ := driver.AllocatorLock("demo")
	commit := func(seed bool) {
		meta, _ := s.Meta(ctx)
		tx, err := s.Write(ctx, lease)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Close()
		if _, err := tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Head(ctx); err != nil {
			t.Fatal(err)
		}
		if seed {
			err = tx.(driver.AllocatorContinuation).RestoreAllocators(ctx, []driver.OfflineAllocator{{Prefix: "demo", Mode: driver.AllocatorSequential, LastID: 4}})
		} else {
			err = tx.(driver.AllocatorContinuation).AllocateTopLevel(ctx, "demo", "demo-5")
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetDerived, Principal: model.PrincipalRef{Kind: "system", ID: "allocator-history-test"}, LockKeys: []driver.LockKey{key}}); err != nil {
			t.Fatal(err)
		}
	}
	meta, _ := s.Meta(ctx)
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.(driver.AllocatorSnapshotReader).AllocatorSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	if s.state.allocatorHistory != nil {
		t.Fatal("Read published a fresh baseline")
	}
	commit(true)
	// Labelled legacy fixture: remove only the internal history while retaining
	// the project state produced by the real commit above.
	s.state.allocatorHistory = nil
	meta, _ = s.Meta(ctx)
	zero := model.Natural64(0)
	r, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.(driver.AllocatorSnapshotReader).AllocatorSnapshot(ctx)
	_ = r.Close()
	var refusal *driver.NativeAllocatorSnapshotError
	if !errors.As(err, &refusal) || refusal.Reason != driver.NativeAllocatorCoverageUnavailable {
		t.Fatalf("legacy history fabricated: %v", err)
	}
	r, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	want, err := r.(driver.AllocatorSnapshotReader).AllocatorSnapshot(ctx)
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if want.CoverageFloor.String() != "1" || want.Inventory.Projects[0].LastID != 4 {
		t.Fatalf("legacy state: %+v", want)
	}
	if s.state.allocatorHistory != nil {
		t.Fatal("Read wrote legacy baseline")
	}
	commit(false)
	at := model.Natural64(1)
	r, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := r.(driver.AllocatorSnapshotReader).AllocatorSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := driver.EncodeAllocatorSnapshot(want)
	b, _ := driver.EncodeAllocatorSnapshot(got)
	if string(a) != string(b) {
		t.Fatalf("first committed baseline lost exact prior state: %s %s", a, b)
	}
}

func TestNativeRestoreEmptyGuardIncludesParentAndJournalOnlyState(t *testing.T) {
	for _, kind := range []string{"parent-only", "journal-only"} {
		t.Run(kind, func(t *testing.T) {
			s := New()
			defer s.Close()
			if kind == "parent-only" {
				key, _ := driver.ParentCounterKey(s.state.meta.Incarnation, "orphan")
				s.state.parentCounters = map[string]driver.ParentCounter{key: {Incarnation: s.state.meta.Incarnation, Parent: "orphan", LastChild: 8}}
			} else {
				snapshot, err := driver.NewAllocatorSnapshot(s.state.meta, model.Natural64(0), model.Natural64(0), driver.AllocatorInventory{})
				if err != nil {
					t.Fatal(err)
				}
				s.state.allocatorHistory = &allocatorImage{snapshot: snapshot}
			}
			if err := s.restoreEmpty(); !errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
				t.Fatalf("%s target admitted: %v", kind, err)
			}
			if !s.state.meta.Head.IsZero() {
				t.Fatal("negative fixture changed the graph head")
			}
		})
	}
}

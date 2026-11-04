package memory

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance/sourceactivation"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

func TestSourceCoverageOntologyFirstLifecycle(t *testing.T) {
	s := New()
	defer s.Close()
	sourceactivation.OntologyFirst(t, s, sourceLease(t, s))
}

func TestSourceCoverageLegacyStartupUpgrade(t *testing.T) {
	s := New()
	defer s.Close()
	sourceactivation.LegacyUpgrade(t, s, sourceLease(t, s), func() {
		// The old ontology-only publisher left both domains unbuilt.
		next := s.state.fork()
		next.hierarchy = newHierarchyIndex(next.meta.Incarnation)
		next.sourceLinks = newSourceLinkIndex(next.meta.Incarnation)
		s.state = next
	})
	requireUnbuiltSelectionFamilies(t, s)
}

func TestSourceCoverageNativeOntologyFirst(t *testing.T) {
	s := New()
	defer s.Close()
	sourceactivation.NativeOntologyFirst(t, s, sourceLease(t, s))
}

func TestSourceCoverageIndependentStartupUpgrade(t *testing.T) {
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	sourceactivation.ParentOnlyUpgrade(t, s, lease)
	requireUnbuiltSelectionFamilies(t, s)
}

func requireUnbuiltSelectionFamilies(t *testing.T, s *Store) {
	t.Helper()
	for _, family := range selectionFamilies() {
		if err := (selectionView{state: s.state}).requireSelectionCoverage(family, mustSelectionHead(s.state)); !errors.Is(err, driver.ErrSelectionUnavailable) {
			t.Fatalf("optional refusal certified family %+v: %v", family, err)
		}
	}
}

func TestSelectionStartupRefusalKeepsGraphCoverage(t *testing.T) {
	for _, phase := range []string{"preparation", "page", "activation"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			s := New()
			defer s.Close()
			lease := sourceLease(t, s)
			ref := sourceRef("legacy-optional")
			key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "startup/" + ref.ID}
			tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &key}}, true, ref)
			if err := tx.Put(ctx, sourceRecord(ref.ID, `{}`)); err != nil {
				t.Fatal(err)
			}
			if phase == "activation" {
				if err := tx.PutDerived(ctx, key.Table, nil, []driver.DerivedRow{{Key: key.Key, Value: []byte("different existing logical value")}}); err != nil {
					t.Fatal(err)
				}
			}
			commitSourceTest(t, tx, header)
			before := s.state
			projection := startupSortProjection()
			switch phase {
			case "preparation":
				projection.Inputs = nil
			case "page":
				projection.Inputs = func(driver.Record) ([]driver.DerivedRowRange, error) { return nil, driver.ErrUnsupported }
			case "activation": // A valid complete page cannot rewrite existing logical bytes at final activation.
				projection.Decode = func(record driver.Record, _ []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
					row := startupBeadRow(record, before.meta.Incarnation)
					return []driver.DerivedSelectionRow{row}, nil
				}
			}
			err := s.BuildGraphSourceIndexes(ctx, lease, func(driver.Record) (driver.GraphSourceMetadata, error) { return driver.GraphSourceMetadata{}, nil }, projection)
			if !driver.IsSelectionBackfillRefused(err) {
				t.Fatalf("graph-completed optional refusal lost its explicit result: %v", err)
			}
			if !errors.Is(err, driver.ErrSelectionUnavailable) && !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("optional refusal lost: %v", err)
			}
			current := sourceReader(t, s, nil)
			if rows, more, err := current.HierarchyChildren(ctx, nil, 0); err != nil || len(rows) != 0 || more {
				t.Fatalf("Parent activation rolled back: %v", err)
			}
			if rows, more, err := current.InverseSourceLinks(ctx, nil, nil, nil, 0); err != nil || len(rows) != 0 || more {
				t.Fatalf("Links activation rolled back: %v", err)
			}
			if before.meta != s.state.meta || !reflect.DeepEqual(before.records, s.state.records) || !reflect.DeepEqual(before.log, s.state.log) {
				t.Fatal("optional refusal changed source/history/Head")
			}
			requireUnbuiltSelectionFamilies(t, s)
		})
	}
}

func TestSourceCoverageStartupGapRefuses(t *testing.T) {
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	a := sourceRef("pl-a")
	tx, h := beginSourceTest(t, s, lease, a)
	if err := tx.PutGraphRecord(context.Background(), sourceRecord(a.ID, `{}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	next := s.state.fork()
	next.meta.Head = model.Natural64(2)
	s.state = next
	sourceactivation.GapRefuses(t, s, lease)
}

func TestSourceCoverageBuildLeaseRenewal(t *testing.T) {
	for _, lose := range []bool{false, true} {
		name := "renewed"
		if lose {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			clock := testclock.New(t, time.Date(2037, 3, 29, 0, 0, 0, 0, time.UTC))
			s, err := NewWithOptions(Options{Clock: clock.Now})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			lease, err := s.Lease(context.Background(), "source-backfill", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			sourceactivation.RenewalBoundary(t, s, lease, func(delta time.Duration) { clock.Advance(t, delta) }, lose)

			if lose && (hierarchyReady(s.state.hierarchy) || sourceLinksReady(s.state.sourceLinks)) {
				t.Fatal("failed build published an immutable index root")
			}

		})
	}
}

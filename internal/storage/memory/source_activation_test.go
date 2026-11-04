package memory

import (
	"context"
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

package memory

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/conformance/sourceactivation"
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

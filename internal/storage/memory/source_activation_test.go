package memory

import (
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

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

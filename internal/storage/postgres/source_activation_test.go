package postgres

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/conformance/sourceactivation"
)

func TestSourceCoverageOntologyFirstLifecycle(t *testing.T) {
	s := openTest(t, freshDSN(t))
	sourceactivation.OntologyFirst(t, s, testLease(t, s))
}

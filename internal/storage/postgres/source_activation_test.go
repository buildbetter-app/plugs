package postgres

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/conformance/sourceactivation"
)

func TestSourceCoverageOntologyFirstLifecycle(t *testing.T) {
	s := openTest(t, freshDSN(t))
	sourceactivation.OntologyFirst(t, s, testLease(t, s))
}

func TestSourceCoverageLegacyStartupUpgrade(t *testing.T) {
	s := openTest(t, freshDSN(t))
	sourceactivation.LegacyUpgrade(t, s, testLease(t, s), func() {
		if _, err := s.db.ExecContext(context.Background(), `DELETE FROM plug_postgres.hierarchy_index_coverage; DELETE FROM plug_postgres.graph_link_index_coverage`); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSourceCoverageNativeOntologyFirst(t *testing.T) {
	s := openTest(t, freshDSN(t))
	sourceactivation.NativeOntologyFirst(t, s, testLease(t, s))
}

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/conformance/sourceactivation"
	"github.com/buildbetter-app/plugs/internal/testclock"
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
	// The real legacy graph has no provable selection closure. Graph activation
	// must commit without certifying any of its optional selection families.
	r := pgStartupRead(t, s)
	for _, family := range selectionFamilies() {
		requirePGFamily(t, r, family, r.at, false)
	}
}

func TestSourceCoverageNativeOntologyFirst(t *testing.T) {
	s := openTest(t, freshDSN(t))
	sourceactivation.NativeOntologyFirst(t, s, testLease(t, s))
}

func TestSourceCoverageIndependentStartupUpgrade(t *testing.T) {
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	sourceactivation.ParentOnlyUpgrade(t, s, lease)
	r := pgStartupRead(t, s)
	for _, family := range selectionFamilies() {
		requirePGFamily(t, r, family, r.at, false)
	}
}

func TestSourceCoverageStartupGapRefuses(t *testing.T) {
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	a := testRef("pl-a")
	tx, h := beginSourceTest(t, s, lease, a)
	if err := tx.PutGraphRecord(context.Background(), sourceRecord(a.ID, `{}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	if _, err := s.db.ExecContext(context.Background(), `UPDATE plug_postgres.meta SET head=2`); err != nil {
		t.Fatal(err)
	}
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
			s := openTest(t, clock.BindPostgres(t, freshDSN(t)))
			lease := testLease(t, s)
			sourceactivation.RenewalBoundary(t, s, lease, func(delta time.Duration) { clock.Advance(t, delta) }, lose)

			if lose {
				for _, table := range []string{"hierarchy_source_current", "hierarchy_source_history", "graph_link_source_current", "graph_link_source_history", "graph_link_current", "graph_link_history", "hierarchy_index_coverage", "graph_link_index_coverage"} {
					var count int
					if err := s.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM plug_postgres."+table).Scan(&count); err != nil || count != 0 {
						t.Fatalf("failed build retained %s rows=%d err=%v", table, count, err)
					}
				}
			}

		})
	}
}

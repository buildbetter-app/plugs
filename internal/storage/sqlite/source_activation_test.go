package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/conformance/sourceactivation"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

func TestSourceCoverageOntologyFirstLifecycle(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "ontology-first.db"))
	sourceactivation.OntologyFirst(t, s, getLease(t, ctx, s, driver.LeaseTTL))
}

func TestSourceCoverageLegacyStartupUpgrade(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "legacy-ontology-first.db"))
	sourceactivation.LegacyUpgrade(t, s, getLease(t, ctx, s, driver.LeaseTTL), func() {
		// At ontology-only Head there are no source rows. Simulate the old
		// publisher's absent coverage before admitting its untyped bead writes.
		if _, err := s.db.ExecContext(ctx, `DELETE FROM hierarchy_index_coverage; DELETE FROM graph_link_index_coverage`); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSourceCoverageNativeOntologyFirst(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "native-ontology-first.db"))
	sourceactivation.NativeOntologyFirst(t, s, getLease(t, ctx, s, driver.LeaseTTL))
}

func TestSourceCoverageIndependentStartupUpgrade(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "independent.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	sourceactivation.ParentOnlyUpgrade(t, s, lease)
}

func TestSourceCoverageStartupGapRefuses(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "independent.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	a := testRef("pl-a")
	tx, h := beginSourceTest(t, s, lease, a)
	if err := tx.PutGraphRecord(context.Background(), sourceRecord(a.ID, `{}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	if _, err := s.db.ExecContext(ctx, `UPDATE meta SET head=?`, revKey(2)); err != nil {
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
			s := clockStore(t, clock)
			lease := getLease(t, context.Background(), s, driver.LeaseTTL)
			sourceactivation.RenewalBoundary(t, s, lease, func(delta time.Duration) { clock.Advance(t, delta) }, lose)

			if lose {
				for _, table := range []string{"hierarchy_source_current", "hierarchy_source_history", "graph_link_source_current", "graph_link_source_history", "graph_link_current", "graph_link_history", "hierarchy_index_coverage", "graph_link_index_coverage"} {
					var count int
					if err := s.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
						t.Fatalf("failed build retained %s rows=%d err=%v", table, count, err)
					}
				}
			}

		})
	}
}

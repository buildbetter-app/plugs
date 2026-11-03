package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/conformance/sourceactivation"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
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

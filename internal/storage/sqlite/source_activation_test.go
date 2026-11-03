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

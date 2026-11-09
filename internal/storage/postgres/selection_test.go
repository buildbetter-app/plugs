package postgres

import (
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"testing"
)

// PostgreSQL now implements the same public typed selector oracle as memory.
// This replaces the old capability-absent assertion; malformed or unavailable
// metadata must still refuse without partial selection or partial staging.
func TestSelectionConformance(t *testing.T) { conformance.RunSelection(t, factory) }

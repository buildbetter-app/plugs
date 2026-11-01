package memory

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestStagedReadOverlay(t *testing.T) {
	conformance.RunStagedReadOverlay(t, func(*testing.T) driver.Store { return New() })
}

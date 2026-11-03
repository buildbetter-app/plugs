package memory

import (
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"testing"
)

func TestSelectionReference(t *testing.T) {
	conformance.RunSelection(t, func(*testing.T) driver.Store { return New() })
}

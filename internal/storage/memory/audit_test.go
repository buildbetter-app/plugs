package memory

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAuditConformance(t *testing.T) {
	conformance.RunAudit(t, func(t *testing.T) driver.Store { return New() })
}

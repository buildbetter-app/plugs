package historyverify

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
)

// Compile the actual verifier-to-planner dependency in this pure extraction
// layer. No raw projection constructor or checkpoint issuer is introduced.
func TestReceivingPlannerDependencyHasNoCallerConstructibleResult(t *testing.T) {
	var plan nativeprojection.Plan
	if plan.Built() { t.Fatal("zero receiving result acquired authority") }
	if _, err := plan.Snapshot(); err == nil { t.Fatal("unbuilt receiving result accepted") }
}

package composition

import (
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// newPipelineWriter is the BUILD-35 composition seam. BUILD-31 may continue
// to own durable OIDC/session construction and BUILD-33 may mount the existing
// MCP handler; neither needs to own, replace, or close this collector. The
// collector is in-process, has no goroutines, and follows Runtime.Close with
// the rest of the runtime because it owns no external resource.
func newPipelineWriter(store driver.Store, lease driver.ActiveLease, options ...pipeline.Option) (*pipeline.Writer, *pipeline.Collector) {
	return pipeline.NewWithTelemetry(store, lease, options...)
}

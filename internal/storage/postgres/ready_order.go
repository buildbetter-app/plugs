package postgres

import (
	"context"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
)

func (r *reader) SelectReadyOrder(ctx context.Context, p driver.ReadyOrderPlan) (driver.ReadyOrderResult, error) {
	if r.writer != nil {
		return driver.ReadyOrderResult{}, fmt.Errorf("%w: Ready transaction overlay is not certified", driver.ErrUnsupported)
	}
	return readyindex.Select(ctx, r, p)
}

var _ driver.ReadyOrderReader = (*reader)(nil)
var _ driver.ReadyOrderReader = (*transaction)(nil)

// A fresh adapter may enable only a coupled current-meta/source/Ready/authority
// proof in the same statement. A generic Selection generation alone is not a
// complete Ready certificate. Absence requires the exact transaction fallback.
func (r *reader) readyOrderPhysicalCertificateSQL() string { return "" }

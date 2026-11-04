package sqlite

import (
	"context"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
)

func (r *reader) SelectReadyOrder(ctx context.Context, p driver.ReadyOrderPlan) (driver.ReadyOrderResult, error) {
	if r.owner != nil {
		return driver.ReadyOrderResult{}, fmt.Errorf("%w: Ready transaction overlay is not certified", driver.ErrUnsupported)
	}
	return readyindex.Select(ctx, r, p)
}

var _ driver.ReadyOrderReader = (*reader)(nil)
var _ driver.ReadyOrderReader = (*transaction)(nil)

// This opt-in seam is consumed only by a fresh adapter which evaluates the
// predicate with current meta (alias m), Ready rows and authority in one SQL
// statement. The current source lacks the coupled physical certificate.
func (r *reader) readyOrderPhysicalCertificateSQL() string { return "" }

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
	certified, err := r.readyOrderSnapshotCertified(ctx, p)
	if err != nil {
		return driver.ReadyOrderResult{}, err
	}
	if !certified {
		return driver.ReadyOrderResult{}, fmt.Errorf("%w: Ready snapshot physical source coverage is unavailable", driver.ErrUnsupported)
	}
	return readyindex.Select(ctx, r, p)
}

// Through==Head alone does not certify source mutations made outside the
// maintained transaction. Enabling this requires a complete physical source,
// Ready-root and authority proof bound to this exact reader snapshot.
func (r *reader) readyOrderSnapshotCertified(ctx context.Context, _ driver.ReadyOrderPlan) (bool, error) {
	return false, ctx.Err()
}

var _ driver.ReadyOrderReader = (*reader)(nil)
var _ driver.ReadyOrderReader = (*transaction)(nil)

// This opt-in seam is consumed only by a fresh adapter which evaluates the
// predicate with current meta (alias m), Ready rows and authority in one SQL
// statement. The current source lacks the coupled physical certificate.
func (r *reader) readyOrderPhysicalCertificateSQL() string { return "" }

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
	certified, err := r.readyOrderSnapshotCertified(ctx, p)
	if err != nil {
		return driver.ReadyOrderResult{}, err
	}
	if !certified {
		return driver.ReadyOrderResult{}, fmt.Errorf("%w: Ready snapshot physical source coverage is unavailable", driver.ErrUnsupported)
	}
	return readyindex.Select(ctx, r, p)
}

// A generic Selection generation or a Ready marker cannot independently prove
// this snapshot's source, root and authority coverage. The physical publisher
// must supply that complete coupled proof before SQL selection is enabled.
func (r *reader) readyOrderSnapshotCertified(ctx context.Context, _ driver.ReadyOrderPlan) (bool, error) {
	return false, ctx.Err()
}

var _ driver.ReadyOrderReader = (*reader)(nil)
var _ driver.ReadyOrderReader = (*transaction)(nil)

// A fresh adapter may enable only a coupled current-meta/source/Ready/authority
// proof in the same statement. A generic Selection generation alone is not a
// complete Ready certificate. Absence requires the exact transaction fallback.
func (r *reader) readyOrderPhysicalCertificateSQL() string { return "" }

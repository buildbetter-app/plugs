package sqlite

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Select refuses until the separately scoped SQL executor and transactional
// projection maintenance exist. Query/DerivedRows are not an equivalent fallback.
// Transactions embed this reader and inherit the same refusal.
func (r *reader) Select(ctx context.Context, _ driver.SelectionPlan) (driver.SelectionResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.SelectionResult{}, err
	}
	return driver.SelectionResult{}, fmt.Errorf("%w: SQLite selection is not implemented", driver.ErrUnsupported)
}

var _ driver.SelectionReader = (*reader)(nil)
var _ driver.SelectionReader = (*transaction)(nil)

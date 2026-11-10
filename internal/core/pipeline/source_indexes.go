package pipeline

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// BuildSourceIndexes is startup maintenance, not part of a write's pre phase.
// It upgrades populated pre-index stores through the driver's atomic bounded
// backfill port. Hosts call it after acquiring/renewing their lease and before
// serving requests. An unavailable capability leaves ordinary fail-closed
// readers/fallbacks unchanged; incompatible data or a failed build is an error.
func BuildSourceIndexes(ctx context.Context, store driver.Store, lease driver.ActiveLease) error {
	builder, ok := store.(driver.GraphSourceIndexBuilder)
	if !ok {
		return nil
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		return err
	}
	decode, projection := sourceSelectionDecoder(meta.Incarnation)
	err = builder.BuildGraphSourceIndexes(ctx, lease, decode, projection)
	// Selection is optional. A legacy graph with no provable closure can still
	// activate its graph domains; its independent selection range stays unbuilt.
	// Only the explicit result proves graph completion. An unavailable cause
	// may also occur inside a fatal SQL/fence/cancellation failure.
	if driver.IsSelectionBackfillRefused(err) {
		return nil
	}
	return err
}

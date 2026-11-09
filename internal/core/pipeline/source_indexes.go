package pipeline

import (
	"context"
	"fmt"

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
	return builder.BuildGraphSourceIndexes(ctx, lease, func(record driver.Record) (driver.GraphSourceMetadata, error) {
		doc, err := DecodeDocument(record.Body)
		if err != nil || doc.ID != record.Ref.ID {
			return driver.GraphSourceMetadata{}, fmt.Errorf("%w: invalid core source document during index activation", driver.ErrUnsupported)
		}
		links, err := localSourceLinks(doc.Links)
		if err != nil {
			return driver.GraphSourceMetadata{}, err
		}
		metadata := driver.GraphSourceMetadata{Parent: doc.Parent, Links: links}
		return metadata, nil
	})
}

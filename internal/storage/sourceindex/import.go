package sourceindex

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// PrepareImport runs under the import's existing empty-target/Head fence,
// BEFORE any records are published. Complete final-image declarations certify
// only the new incarnation's current head, never its imported history.
func (d Dialect) PrepareImport(ctx context.Context, q SQL, current, next driver.Meta, records map[string]*driver.Record, identities map[string]driver.Record, overlay *Overlay, writable func() error, bumped bool) (Plan, error) {
	if len(overlay.Declarations) == 0 {
		return Plan{}, nil // legacy untyped import remains unbuilt
	}
	p, err := d.Prepare(ctx, q, current, next, true, records, identities, overlay, writable, bumped)
	if err != nil {
		return Plan{}, err
	}
	p.Parent.First, p.Links.First = p.At, p.At
	// Reuse the backfill row encoding to retain each imported source's actual
	// last-change revision. Coverage markers still start at H, not that revision.
	p.backfill = true
	p.Records = make(map[string]*driver.Record, len(records))
	for id, record := range records {
		if record != nil {
			image := *record
			p.Records[id] = &image
		}
	}
	return p, nil
}

// PublishImport is part of the SAME fenced SQL transaction as records, history,
// semantic fact, binding, receipt and head. No paged/backfill commits occur here.
func (p Plan) PublishImport(ctx context.Context, q SQL) error {
	if err := p.Publish(ctx, q); err != nil {
		return err
	}
	return p.ActivateBackfill(ctx, q)
}

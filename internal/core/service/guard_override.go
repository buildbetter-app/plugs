package service

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// ValidateForcedCloseReplay reobserves only transition authority on the
// current tracked snapshot. It never mutates a stored receipt, re-executes a
// close or turns override_guard into a permission observation.
func (o *Ontology) ValidateForcedCloseReplay(ctx context.Context, proposal port.ProposedChange, reader port.SnapshotReader) error {
	o.mu.RLock()
	configured := o.permissionConfigured
	o.mu.RUnlock()
	if !configured {
		return nil // the explicit no-authorization composition
	}
	seen := map[string]bool{}
	for _, change := range proposal.Changes {
		if !proposal.ForcesClose(change) || seen[change.Resource.Ref.ID] {
			continue
		}
		seen[change.Resource.Ref.ID] = true
		rows, err := reader.Get(ctx, []model.Ref{change.Resource.Ref})
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return failure("forbidden", "force replay target is unavailable for transition authorization")
		}
		doc, err := decodeDocument(rows[0].Body)
		if err != nil {
			return failure("internal_error", "invalid force replay target")
		}
		before := state(doc)
		after := before
		target, ok := o.effective(doc.Project).SlotStatus(doc.Type, ontology.Close)
		if !ok {
			return failure("invalid_params", "force replay close slot is unavailable")
		}
		after.Status = target
		facts := ontology.GuardFacts{}
		if err := o.observeTransitionGuards(ctx, proposal, change, &before, after, "bead.close", before.Status == target, nil, reader, &facts); err != nil {
			return err
		}
		if err := violationsError(ontology.ValidateTransitionPermission(facts)); err != nil {
			return err
		}
	}
	return nil
}

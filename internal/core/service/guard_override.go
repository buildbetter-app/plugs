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
	return o.validateCloseReplay(ctx, proposal, reader, true)
}

// ValidateCloseReplay reobserves current transition authority for ordinary
// and forced receipts alike, without rerunning mutation or policy guards.
func (o *Ontology) ValidateCloseReplay(ctx context.Context, proposal port.ProposedChange, reader port.SnapshotReader) error {
	return o.validateCloseReplay(ctx, proposal, reader, false)
}

func (o *Ontology) validateCloseReplay(ctx context.Context, proposal port.ProposedChange, reader port.SnapshotReader, forcedOnly bool) error {
	o.mu.RLock()
	configured := o.permissionConfigured
	o.mu.RUnlock()
	if !configured {
		return nil // the explicit no-authorization composition
	}
	seen := map[string]bool{}
	for _, change := range proposal.Changes {
		if (proposal.Meta.Method != "bead.close" && change.Op.Kind != model.OpClose) ||
			(forcedOnly && !proposal.ForcesClose(change)) || seen[change.Resource.Ref.ID] {
			continue
		}
		switch change.Resource.Kind {
		case "workflow_item":
			// The Writer admitted this provider-owned source identity and
			// rechecked its current project/method policy before this hook.
			// Its completed receipt is not an ordinary stored bead transition.
			continue
		case "bead":
		default:
			return failure("forbidden", "unsupported close replay resource")
		}
		seen[change.Resource.Ref.ID] = true
		rows, err := reader.Get(ctx, []model.Ref{change.Resource.Ref})
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return failure("forbidden", "close replay target is unavailable for transition authorization")
		}
		doc, err := decodeDocument(rows[0].Body)
		if err != nil {
			return failure("internal_error", "invalid close replay target")
		}
		before := state(doc)
		after := before
		target, ok := o.effective(doc.Project).SlotStatus(doc.Type, ontology.Close)
		if !ok {
			return failure("invalid_params", "close replay close slot is unavailable")
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

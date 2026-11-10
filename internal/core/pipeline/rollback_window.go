package pipeline

import (
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
)

// checkRollbackWindow runs after request normalization and all pre callbacks,
// while the proposed image is still private to the Writer. Only creates are
// guarded: updates/deletes of already-existing user content preserve ordinary
// lifecycle and CAS behavior, while operational kinds remain admissible.
func (w *Writer) checkRollbackWindow(p *prepared) error {
	if !w.cfg.rollbackWindow || p == nil {
		return nil
	}
	for _, change := range p.proposal.Changes {
		if change.Op.Kind != model.OpCreate {
			continue
		}
		classification := w.cfg.reverseConverter.ClassifyKind(change.Resource.Kind)
		if classification.Disposition == reverseconverter.KindUnrepresentable {
			return reverseconverter.RollbackWindowRefusal(change.Resource.Kind)
		}
	}
	return nil
}

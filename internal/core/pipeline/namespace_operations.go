package pipeline

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

// ApplyWithSourceOperations explicitly admits a trusted namespace participant.
// A legacy Writer that ignores context cannot silently omit that participant.
func (w *Writer) ApplyWithSourceOperations(ctx context.Context, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, prepare SourcePreparation, kind func(model.Ref) string) (port.WriteOutcome, error) {
	return w.ApplyWithOutcome(WithSourceOperations(ctx, prepare, kind), by, cs, meta)
}

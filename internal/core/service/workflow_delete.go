package service

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func ordinaryDeletes(cs model.ChangeSet) []model.Ref {
	var refs []model.Ref
	for _, op := range cs.Ops {
		if op.Kind == model.OpDelete && op.Delete != nil && op.Delete.Target.IsID() {
			refs = append(refs, op.Delete.Target)
		}
	}
	return refs
}

func ordinaryDeleteKind(ref model.Ref) string {
	if workflowRef(ref) {
		return "workflow:item"
	}
	return "bead"
}

func (s *Service) ordinaryDeletePreparation(by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, preview *map[int]model.OpResult) pipeline.SourcePreparation {
	return func(ctx context.Context, reader driver.Reader, prepare func(pipeline.SourceOperationHandler) error) (pipeline.SourcePublication, error) {
		planned, err := s.workflow.PrepareOrdinaryDelete(ctx, by, cs, meta, func(handle func(model.Op) (*workflowplugin.OrdinaryDeleteImage, error)) error {
			return prepare(func(_ context.Context, op model.Op, _ pipeline.SourceCoreImage) (model.Op, *pipeline.SourceOperation, error) {
				image, err := handle(op)
				if err != nil || image == nil {
					return op, nil, err
				}
				// The core receipt keeps identity only for ephemeral endpoints.
				// Full results come from the atomically joined plugin receipts.
				return op, &pipeline.SourceOperation{
					Change: port.Change{Resource: model.ResourceRef{Kind: "workflow:item", Ref: image.Ref}, Op: op, Before: image.Before, After: image.After, SetBy: map[string]string{"links": "request"}},
					Result: model.OpResult{Ref: image.Ref, Revision: image.Generation, SetBy: map[string]string{"links": "request"}},
				}, nil
			})
		})
		if err != nil {
			return pipeline.SourcePublication{}, err
		}
		*preview = planned.Results
		publication := pipeline.SourcePublication{Head: driver.CoreSnapshotExpectation{StoreID: planned.Namespace.StoreID, Incarnation: reader.Snapshot().Incarnation, Head: *reader.Snapshot().At}, Namespace: planned.Namespace}
		publication.FinalResults = make(map[int]model.OpResult, len(planned.Results))
		for position, result := range planned.Results {
			result.Record = nil
			publication.FinalResults[position] = result
		}
		if planned.State != nil {
			mutation, err := planned.State.Checkpoint(ctx)
			if err != nil {
				return pipeline.SourcePublication{}, err
			}
			publication.Mutation = &mutation
		}
		return publication, nil
	}
}

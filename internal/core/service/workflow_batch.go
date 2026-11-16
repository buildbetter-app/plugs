package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func ordinaryBatchUsesWorkflow(cs model.ChangeSet) bool {
	for _, op := range cs.Ops {
		if op.Validate() != nil {
			continue // The canonical writer reports malformed union arms.
		}
		if workflowRef(sourceOpTarget(op)) {
			return true
		}
		if (op.Kind == model.OpLink || op.Kind == model.OpUnlink) && op.Link != nil &&
			op.Link.TargetKind != model.LinkTargetExternal && workflowRef(op.Link.To) {
			return true
		}
	}
	return false
}

func ordinaryWorkflowKind(ref model.Ref) string {
	if workflowRef(ref) {
		return "workflow_item"
	}
	return ""
}

// prepareWorkflowRelation routes both endpoint directions through the existing
// provider, using the ordered core image rather than opening another reader.
// The original request remains the Writer's hash and receipt identity.
func (s *Service) prepareWorkflowRelation(ctx context.Context, staged *workflowplugin.Provider, by model.PrincipalRef, op model.Op, index int, cs model.ChangeSet, meta model.WriteMeta, graph map[string]storedBead, resolver *workflowCoreResolver, resolve func(model.Ref) (model.Ref, error)) (model.Op, *pipeline.SourceOperation, error) {
	kind, err := op.Link.TargetKind.Normalized()
	if err != nil {
		return model.Op{}, nil, failure("invalid_params", err.Error())
	}
	if kind == model.LinkTargetExternal {
		if workflowRef(op.Link.From) {
			return model.Op{}, nil, failure("unsupported", "workflow external targets require a typed workflow storage contract")
		}
		return op, nil, nil
	}
	to, err := resolve(op.Link.To)
	if err != nil {
		return model.Op{}, nil, err
	}
	link := *op.Link
	link.To = to
	op.Link = &link
	if !workflowRef(link.From) && !workflowRef(to) {
		return op, nil, nil
	}
	if meta.DryRun || len(cs.Expect) != 0 {
		return model.Op{}, nil, failure("unsupported", "mixed workflow relations do not support dryRun or expect")
	}
	for _, ref := range []model.Ref{link.From, to} {
		if workflowRef(ref) {
			continue
		}
		if !ref.IsID() || !ref.Branch.IsZero() {
			return model.Op{}, nil, failure("invalid_params", "mixed relations require resolved branch-zero endpoints")
		}
		row, exists := graph[ref.ID]
		if !exists {
			return model.Op{}, nil, failure("not_found", "ordinary workflow endpoint was not found in the staged project")
		}
		if row.doc.Project != meta.Project {
			return model.Op{}, nil, failure("forbidden", "workflow ordinary endpoint is outside the project")
		}
	}
	endpoint := link.From
	if !workflowRef(endpoint) {
		endpoint = to
	}
	before, err := staged.GetWithResolver(ctx, by, endpoint, meta, resolver)
	if err != nil {
		return model.Op{}, nil, err
	}
	operationMeta := meta
	digest := sha256.Sum256([]byte(meta.Method + "\x00" + meta.IdempotencyKey + "\x00" + strconv.Itoa(index)))
	operationMeta.IdempotencyKey = hex.EncodeToString(digest[:])
	request := workflowplugin.LinkRequest{From: link.From, To: to, Kind: link.LinkKind, Metadata: link.Metadata}
	var commit model.Commit
	if op.Kind == model.OpUnlink {
		operationMeta.Method = "bead.unlink"
		commit, err = staged.Unlink(ctx, by, request, operationMeta)
	} else {
		operationMeta.Method = "bead.link"
		commit, err = staged.Link(ctx, by, request, operationMeta)
	}
	if err != nil {
		return model.Op{}, nil, err
	}
	if len(commit.Results) != 1 || commit.Results[0].Ref != endpoint {
		return model.Op{}, nil, failure("internal_error", "workflow relation returned an invalid endpoint")
	}
	after, err := staged.GetWithResolver(ctx, by, endpoint, meta, resolver)
	if err != nil {
		return model.Op{}, nil, err
	}
	active := &pipeline.SourceOperation{Result: commit.Results[0], Change: port.Change{Resource: model.ResourceRef{Kind: "workflow_item", Ref: endpoint}, Op: op}}
	active.Change.Before, err = json.Marshal(before)
	if err != nil {
		return model.Op{}, nil, err
	}
	active.Change.After, err = json.Marshal(after)
	active.Result.Record = active.Change.After
	return op, active, err
}

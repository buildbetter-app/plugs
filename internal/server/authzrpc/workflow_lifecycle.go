package authzrpc

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func (s *GuardedService) WorkflowPromote(ctx context.Context, by model.PrincipalRef, req workflowplugin.PromoteRequest, meta model.WriteMeta) (workflowplugin.PromoteResult, error) {
	lifecycle, ok := s.delegate.(rpc.WorkflowLifecycleService)
	if !ok {
		return workflowplugin.PromoteResult{}, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
	}
	binding, err := s.workflowModifyAuthority(ctx, by, meta.Project)
	if err != nil {
		return workflowplugin.PromoteResult{}, err
	}
	out, err := lifecycle.WorkflowPromote(ctx, by, req, meta)
	if err != nil {
		return workflowplugin.PromoteResult{}, err
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return workflowplugin.PromoteResult{}, err
	}
	return out, nil
}
func (s *GuardedService) WorkflowDemote(ctx context.Context, by model.PrincipalRef, req workflowplugin.DemoteRequest, meta model.WriteMeta) (workflowplugin.DemoteResult, error) {
	lifecycle, ok := s.delegate.(rpc.WorkflowLifecycleService)
	if !ok {
		return workflowplugin.DemoteResult{}, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
	}
	binding, err := s.workflowModifyAuthority(ctx, by, meta.Project)
	if err != nil {
		return workflowplugin.DemoteResult{}, err
	}
	out, err := lifecycle.WorkflowDemote(ctx, by, req, meta)
	if err != nil {
		return workflowplugin.DemoteResult{}, err
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return workflowplugin.DemoteResult{}, err
	}
	return out, nil
}

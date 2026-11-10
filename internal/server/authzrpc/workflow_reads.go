package authzrpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// Workflow ownership is enforced by the provider. It does not replace the
// current project policy, whose resource is the immutable registry Ref.
func (c *Checker) workflowRead(ctx context.Context, caller authz.Caller, authority *slice, method, project string) (bool, error) {
	ref, found := authority.projectByName(project)
	if !found {
		return false, nil
	}
	decisions, err := c.chain.Check(ctx, authority, []authz.CheckRequest{{
		Caller: caller, Action: authz.Read,
		Resource: authz.Resource{Kind: authz.Project, Ref: ref}, Method: method,
	}})
	if err != nil {
		return false, authzError(err)
	}
	if len(decisions) != 1 {
		return false, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	return decisions[0].Effect == authz.Allow, nil
}

func (s *GuardedService) getWorkflow(ctx context.Context, by model.PrincipalRef, req service.GetRequest) (service.GetResult, error) {
	if req.Project == "" {
		return service.GetResult{}, notFound()
	}
	_, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return service.GetResult{}, err
	}
	defer closeReader()
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return service.GetResult{}, authzError(err)
	}
	authority, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return service.GetResult{}, &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	var allowed bool
	if s.sourceRuntimeInstalled() {
		allowed, err = s.sourceRead(ctx, caller, authority, req.ID, req.Project, "bead.get")
	} else {
		allowed, err = s.checker.workflowRead(ctx, caller, authority, "bead.get", req.Project)
	}
	if err != nil {
		return service.GetResult{}, err
	}
	if !allowed {
		return service.GetResult{}, notFound()
	}
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authority, snapshot)
	if err != nil {
		return service.GetResult{}, err
	}
	result, err := s.delegate.Get(ctx, by, req)
	if err != nil {
		return service.GetResult{}, err
	}
	if result.Bead.Project != req.Project {
		return service.GetResult{}, notFound()
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return service.GetResult{}, err
	}
	return result, nil
}

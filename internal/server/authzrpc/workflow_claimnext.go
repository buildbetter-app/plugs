package authzrpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

// workflowClaimAuthority resolves the current immutable project resource for
// workflow claims. Empty Method uses the adopted Modify/Project permit, just
// like the core claimNext terminal receipt; provider ownership is additional
// scope checking and is never a policy grant.
//
// The returned binding fences response publication. The provider's checkpoint
// CAS has no core ACL dependency input, so this adapter does not make policy
// revocation atomic with a workflow mutation between preflight and its save.
func (s *GuardedService) workflowClaimAuthority(ctx context.Context, by model.PrincipalRef, project string) (string, error) {
	_, snapshot, closeReader, err := s.current(ctx, nil)
	if err != nil {
		return "", err
	}
	defer closeReader()
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return "", authzError(err)
	}
	authority, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return "", &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	ref, found := authority.projectByName(project)
	if !found {
		return "", forbidden("workflow claim project is not authorized")
	}
	decisions, err := s.checker.chain.Check(ctx, authority, []authz.CheckRequest{{
		Caller: caller, Action: authz.Modify,
		Resource: authz.Resource{Kind: authz.Project, Ref: ref},
	}})
	if err != nil {
		return "", authzError(err)
	}
	if len(decisions) != 1 {
		return "", &model.Error{Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	if decisions[0].Effect != authz.Allow {
		return "", forbidden("workflow claim project is not authorized")
	}
	binding, err := s.checker.readAuthorityBinding(ctx, caller, authority, snapshot)
	if err != nil {
		return "", err
	}
	if err := s.checker.ValidateReadAuthorityBinding(ctx, by, binding); err != nil {
		return "", err
	}
	return binding, nil
}

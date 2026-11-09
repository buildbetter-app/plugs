package authzfixed

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/authz"
)

// OntologyRuleSet is an explicitly installed management policy. The ordinary
// finite CP7 component remains unchanged. Only a current authority-admin User
// may read the protected source or apply its exact named migration operation;
// the host separately requires live interactive authentication on apply.
type OntologyRuleSet struct{ namespace authz.Namespace }

func NewOntology(n authz.Namespace) (*OntologyRuleSet, error) {
	if err := n.Validate(); err != nil {
		return nil, err
	}
	return &OntologyRuleSet{namespace: n}, nil
}

func (r *OntologyRuleSet) Compile(req authz.ListRequest) (authz.FilterTemplate, error) {
	req, err := authz.FreezeList(req)
	if err != nil {
		return authz.FilterTemplate{}, err
	}
	permit := authz.FilterExpr{Op: authz.False}
	if req.ResourceKind == authz.Ontology &&
		(req.Action == authz.Read && req.Method == "" || req.Action == authz.ManageOntology && req.Method == "ontology.apply") {
		permit = authz.And(
			authz.FilterExpr{Op: authz.PrincipalKind, Kind: "user"},
			authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.AuthorityBinding, Role: "admin"},
		)
	}
	t := authz.FilterTemplate{Namespace: r.namespace, Request: req, Permit: permit, Forbid: authz.FilterExpr{Op: authz.False}}
	return t, t.Validate()
}

func (r *OntologyRuleSet) Eval(ctx context.Context, s authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	decisions := make([]authz.Decision, len(requests))
	for i, req := range requests {
		t, err := r.Compile(authz.ListRequest{Caller: req.Caller, Action: req.Action, ResourceKind: req.Resource.Kind, Method: req.Method})
		if err != nil {
			return nil, err
		}
		// The chain has already prepared the caller and common dependencies.
		// An unrelated operation cannot receive a permit or forbid here. Avoid
		// repeating those reads in a bounded event-context snapshot.
		if t.Permit.Op == authz.False && t.Forbid.Op == authz.False {
			decisions[i].Effect = authz.NoDecision
			continue
		}
		allowed, err := t.Match(ctx, s, req.Resource)
		if err != nil {
			return nil, err
		}
		decisions[i].Effect = authz.NoDecision
		if allowed {
			decisions[i] = authz.Decision{Effect: authz.Allow, Reasons: []string{"ontology.authority-admin"}}
		}
	}
	return decisions, nil
}

var _ authz.RuleSet = (*OntologyRuleSet)(nil)

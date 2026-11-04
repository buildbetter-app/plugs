// Package authzfixed implements only the adopted finite CP7 policy component.
// Protected management, authentication, storage and query execution are hosts.
package authzfixed

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/authz"
)

type RuleSet struct{ namespace authz.Namespace }

func New(n authz.Namespace) (*RuleSet, error) {
	if err := n.Validate(); err != nil {
		return nil, err
	}
	return &RuleSet{namespace: n}, nil
}

// Compile publishes the same finite predicate used by Eval. Modify on Bead is
// a membership adapter ONLY for the three named existing-bead verbs. Before
// any actual write, the host must also call authz.ValidateBeadWrite on its
// stored/prepared images and retain ontology, setter and transaction guards.
// Empty Method exposes only the three exact CP7 action/resource pairs.
func (r *RuleSet) Compile(req authz.ListRequest) (authz.FilterTemplate, error) {
	prepared, err := authz.PrepareRequest(req)
	if err != nil {
		return authz.FilterTemplate{}, err
	}
	return r.CompilePrepared(prepared)
}

func (r *RuleSet) CompilePrepared(prepared *authz.PreparedRequest) (authz.FilterTemplate, error) {
	req := prepared.Request()
	f := authz.FilterExpr{Op: authz.False}
	user := authz.FilterExpr{Op: authz.PrincipalKind, Kind: "user"}
	switch {
	case req.Action == authz.Read && req.ResourceKind == authz.Bead && readMethod(req.Method):
		f = user
	case req.Action == authz.ImportBulkHistory && req.ResourceKind == authz.History && req.Method == "":
		f = authz.And(user, authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.AuthorityBinding, Role: "admin"})
	case req.Action == authz.Modify && ((req.ResourceKind == authz.Project && req.Method == "") || (req.ResourceKind == authz.Bead && writeMethod(req.Method))):
		f = authz.And(authz.FilterExpr{Op: authz.PrincipalKind, Kind: "agent"}, authz.FilterExpr{Op: authz.SponsorKind, Kind: "user"}, authz.FilterExpr{Op: authz.ActiveProject}, authz.Or(
			authz.FilterExpr{Op: authz.GrantExists, Subject: authz.SponsorSubject, Scope: authz.ProjectBindingScope, Role: "involved"},
			authz.FilterExpr{Op: authz.TeamInvolvement, Subject: authz.SponsorSubject},
		))
	}
	t := authz.FilterTemplate{Namespace: r.namespace, Request: req, Permit: f, Forbid: authz.FilterExpr{Op: authz.False}}
	return prepared.Filter(t)
}

func readMethod(m string) bool {
	switch m {
	case "", "bead.get", "bead.list", "bead.ready", "bead.cycles", "view.run":
		return true
	}
	return false
}
func writeMethod(m string) bool {
	return m == "bead.claim" || m == "bead.claimNext" || m == "bead.update"
}

func (r *RuleSet) Eval(ctx context.Context, s authz.Slice, reqs []authz.CheckRequest) ([]authz.Decision, error) {
	return r.EvalPrepared(ctx, s, authz.PrepareChecks(reqs))
}

func (r *RuleSet) EvalPrepared(ctx context.Context, s authz.Slice, batch *authz.PreparedChecks) ([]authz.Decision, error) {
	out := make([]authz.Decision, batch.Len())
	filters := map[*authz.PreparedRequest]authz.FilterTemplate{}
	for i := range out {
		binding, err := batch.Binding(i)
		if err != nil {
			return nil, err
		}
		t, found := filters[binding]
		if !found {
			t, err = r.CompilePrepared(binding)
			if err != nil {
				return nil, err
			}
			filters[binding] = t
		}
		ok, err := batch.Match(ctx, s, i, t)
		if err != nil {
			return nil, err
		}
		out[i].Effect = authz.NoDecision
		if ok {
			out[i].Effect = authz.Allow
			out[i].Reasons = []string{"cp7." + string(t.Request.Action)}
		}
	}
	return out, nil
}

var _ authz.RuleSet = (*RuleSet)(nil)
var _ authz.PreparedRuleSet = (*RuleSet)(nil)
var _ authz.PreparedCompiler = (*RuleSet)(nil)

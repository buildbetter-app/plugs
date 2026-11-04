package authzfixed

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
)

// These are finite-policy controls, not native or authenticated runtime proof.
func TestOntologyManagementRequiresExactAuthorityAndOperation(t *testing.T) {
	management, err := NewOntology(ns())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, principal, method string
		action                  authz.Action
		kind                    authz.ResourceKind
		admin, active, allow    bool
	}{
		{"admin-apply", "user", "ontology.apply", authz.ManageOntology, authz.Ontology, true, true, true},
		{"admin-source-read", "user", "", authz.Read, authz.Ontology, true, true, true},
		{"no-current-grant", "user", "ontology.apply", authz.ManageOntology, authz.Ontology, false, true, false},
		{"revoked-context", "user", "ontology.apply", authz.ManageOntology, authz.Ontology, true, false, false},
		{"agent", "agent", "ontology.apply", authz.ManageOntology, authz.Ontology, false, true, false},
		{"service", "service", "ontology.apply", authz.ManageOntology, authz.Ontology, false, true, false},
		{"bead-grant-cannot-apply", "user", "ontology.apply", authz.ManageOntology, authz.Bead, true, true, false},
		{"modify-cannot-apply", "user", "ontology.apply", authz.Modify, authz.Ontology, true, true, false},
		{"wrong-method", "user", "tx.apply", authz.ManageOntology, authz.Ontology, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := caller(tc.principal, nil)
			s := newSlice(c)
			s.contextActive = tc.active
			if tc.admin {
				s.data.Grants = append(s.data.Grants, grant("user", c.Principal.ID, authz.AuthorityScope, ns().Authority.ID, "admin"))
			}
			q := authz.CheckRequest{Caller: c, Action: tc.action, Resource: authz.Resource{Kind: tc.kind, Ref: ref("ontology/native")}, Method: tc.method}
			chain := authz.Chain{Namespace: ns(), Rules: []authz.RuleSet{rules(t), management}}
			decisions, err := chain.Check(context.Background(), s, []authz.CheckRequest{q})
			if err != nil || len(decisions) != 1 {
				t.Fatalf("decision: %v %v", decisions, err)
			}
			if (decisions[0].Effect == authz.Allow) != tc.allow {
				t.Fatalf("unexpected protected permit: %+v", decisions[0])
			}
			if tc.kind == authz.Ontology && (!s.recorded[authz.ACLDependency(ns())] || !s.recorded[authz.ContextDependency(ns())]) {
				t.Fatal("current authorization generations were not observed")
			}
		})
	}
}

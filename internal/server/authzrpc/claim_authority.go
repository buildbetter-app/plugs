package authzrpc

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ClaimPolicyConfig installs a claim-slot transition policy. Exactly one
// source mode must be explicit. Source locates a real protected ontology row
// on the current decision reader; it supplies no document or permit. The
// immutable default mode is valid only while the shared runtime ontology
// still advertises its original built-in resolver.
type ClaimPolicyConfig struct {
	UseImmutableDefault bool
	Source              service.OntologySourceLocator
}

type claimRuleSet struct {
	namespace authz.Namespace
	shared    *service.Ontology
	config    ClaimPolicyConfig
	fixed     *authzfixed.RuleSet
}

func newClaimRuleSet(ns authz.Namespace, shared *service.Ontology, config ClaimPolicyConfig) (*claimRuleSet, error) {
	if shared == nil || config.UseImmutableDefault == (config.Source != nil) {
		return nil, fmt.Errorf("claim policy requires one explicit ontology source mode")
	}
	fixed, err := authzfixed.New(ns)
	if err != nil {
		return nil, err
	}
	if config.UseImmutableDefault {
		if _, ok := shared.ImmutableDefault(); !ok {
			return nil, fmt.Errorf("claim policy default differs from shared runtime ontology")
		}
	}
	return &claimRuleSet{namespace: ns, shared: shared, config: config, fixed: fixed}, nil
}

func (r *claimRuleSet) Compile(q authz.ListRequest) (authz.FilterTemplate, error) {
	q, err := authz.FreezeList(q)
	if err != nil {
		return authz.FilterTemplate{}, err
	}
	// Candidate listing keeps the existing installed policy's compiled
	// predicate. This rule grants no list/read/modify/close/override action.
	return authz.FilterTemplate{Namespace: r.namespace, Request: q, Permit: authz.FilterExpr{Op: authz.False}, Forbid: authz.FilterExpr{Op: authz.False}}, nil
}

func claimTransitionCheck(q authz.CheckRequest) bool {
	return q.Action == authz.Transition && q.Resource.Kind == authz.Bead &&
		(q.Method == "bead.claim" || q.Method == "bead.claimNext") &&
		authz.CanonicalRef(q.Resource.Ref) == nil &&
		authz.ValidID(q.Transition.Project) && authz.ValidID(q.Transition.Type) &&
		authz.ValidID(q.Transition.FromStatus) && authz.ValidID(q.Transition.ToStatus)
}

func (r *claimRuleSet) effective(ctx context.Context, s *slice, project string) (ontology.Effective, error) {
	// Record even the built-in resolver's ontology dependency on the exact
	// decision reader. Refresh invalidates default mode instead of silently
	// retaining a private copy of the earlier document.
	states, err := s.snapshot.Observe(ctx, []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: driver.SentinelOntology}})
	if err != nil {
		return ontology.Effective{}, err
	}
	if len(states) != 1 {
		return ontology.Effective{}, fmt.Errorf("claim ontology dependency unavailable")
	}
	if r.config.UseImmutableDefault {
		effective, ok := r.shared.ImmutableDefault()
		if !ok {
			return ontology.Effective{}, fmt.Errorf("immutable claim ontology was replaced")
		}
		return effective, nil
	}
	ref, err := r.config.Source(project)
	if err != nil {
		return ontology.Effective{}, err
	}
	if authz.CanonicalRef(ref) != nil || !(ref.ID == "ontology" || strings.HasPrefix(ref.ID, "ontology/")) {
		return ontology.Effective{}, fmt.Errorf("claim policy requires a protected ontology source")
	}
	rows, err := s.snapshot.Get(ctx, []model.Ref{ref})
	if err != nil {
		return ontology.Effective{}, err
	}
	if len(rows) != 1 || rows[0].Ref != ref || rows[0].Kind != "ontology" || rows[0].Revision.IsZero() {
		return ontology.Effective{}, fmt.Errorf("claim ontology source unavailable")
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		return ontology.Effective{}, err
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		return ontology.Effective{}, fmt.Errorf("claim ontology source does not compile: %v", problems)
	}
	return compiled.Effective(project), nil
}

func (r *claimRuleSet) Eval(ctx context.Context, source authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	out := make([]authz.Decision, len(requests))
	for i := range out {
		out[i].Effect = authz.NoDecision
	}
	for i, q := range requests {
		if !claimTransitionCheck(q) {
			continue
		}
		s, ok := source.(*slice)
		if !ok || s == nil || s.snapshot == nil || s.Namespace() != r.namespace {
			return nil, fmt.Errorf("claim policy snapshot unavailable")
		}
		rows, err := s.snapshot.Get(ctx, []model.Ref{q.Resource.Ref})
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 || rows[0].Ref != q.Resource.Ref || rows[0].Kind != "bead" {
			continue
		}
		doc, err := pipeline.DecodeDocument(rows[0].Body)
		if err != nil {
			return nil, err
		}
		from, ok := doc.Fields["status"].(string)
		if !ok || doc.ID != q.Resource.Ref.ID || doc.Project != q.Transition.Project || doc.Type != q.Transition.Type || from != q.Transition.FromStatus {
			continue
		}
		effective, err := r.effective(ctx, s, doc.Project)
		if err != nil {
			return nil, err
		}
		target, ok := effective.SlotStatus(doc.Type, ontology.Claim)
		if !ok || target != q.Transition.ToStatus || !effective.ReadyEligible(doc.Type, from) {
			continue
		}
		// This is an explicit claim-slot transition rule, conditioned on the
		// existing fixed policy's exact project/team/sponsor membership. It
		// never returns a Modify grant. The enclosing chain still evaluates
		// the original Transition caveats and every installed forbid.
		membership := q
		membership.Action = authz.Modify
		membership.Transition = authz.TransitionContext{}
		decisions, err := r.fixed.Eval(ctx, s, []authz.CheckRequest{membership})
		if err != nil {
			return nil, err
		}
		if len(decisions) != 1 {
			return nil, fmt.Errorf("claim membership decision unavailable")
		}
		if decisions[0].Effect == authz.Allow {
			out[i] = authz.Decision{Effect: authz.Allow, Reasons: []string{"claim_slot_transition"}}
		}
	}
	return out, nil
}

var _ authz.RuleSet = (*claimRuleSet)(nil)

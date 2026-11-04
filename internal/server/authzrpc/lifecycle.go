package authzrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// LifecyclePolicyConfig is trusted installation state, not a public policy
// selector. The host maintains Ref through its administrative storage path.
type LifecyclePolicyConfig struct{ Ref model.Ref }

// LifecyclePolicy is the body of a host-owned lifecycle_policy record. Each
// grant is exact: there are no wildcard principals, statuses, projects or
// actions. Public bead operations cannot write this record kind.
type LifecyclePolicy struct {
	Namespace authz.Namespace  `json:"namespace"`
	Grants    []LifecycleGrant `json:"grants"`
}

type LifecycleGrant struct {
	Principal  model.PrincipalRef `json:"principal"`
	Project    model.Ref          `json:"project"`
	Type       string             `json:"type"`
	FromStatus string             `json:"fromStatus"`
	ToStatus   string             `json:"toStatus"`
	Actions    []authz.Action     `json:"actions"`
	Effect     authz.Effect       `json:"effect"`
}

type lifecycleRuleSet struct {
	namespace authz.Namespace
	ref       model.Ref
}

func (r *lifecycleRuleSet) Compile(q authz.ListRequest) (authz.FilterTemplate, error) {
	// This rule grants only exact close checks. It adds no list/read/claim
	// permit and cannot widen the finite fixed component's compiled predicate.
	q, err := authz.FreezeList(q)
	if err != nil {
		return authz.FilterTemplate{}, err
	}
	return authz.FilterTemplate{Namespace: r.namespace, Request: q, Permit: authz.FilterExpr{Op: authz.False}, Forbid: authz.FilterExpr{Op: authz.False}}, nil
}

func closeCheck(q authz.CheckRequest) bool {
	return q.Resource.Kind == authz.Bead && (q.Method == "bead.close" || q.Method == "tx.apply") &&
		(q.Action == authz.Modify || q.Action == authz.Transition || q.Action == authz.OverrideGuard) &&
		authz.ValidID(q.Transition.Project) && authz.ValidID(q.Transition.Type) &&
		authz.ValidID(q.Transition.FromStatus) && authz.ValidID(q.Transition.ToStatus)
}

func (r *lifecycleRuleSet) Eval(ctx context.Context, source authz.Slice, requests []authz.CheckRequest) ([]authz.Decision, error) {
	out := make([]authz.Decision, len(requests))
	needed := false
	for i, q := range requests {
		out[i].Effect = authz.NoDecision
		needed = needed || closeCheck(q)
	}
	if !needed {
		return out, nil
	}
	s, ok := source.(*slice)
	if !ok || s == nil || s.snapshot == nil || s.Namespace() != r.namespace {
		return nil, fmt.Errorf("lifecycle policy snapshot unavailable")
	}
	// Get records both a hit's source revision and a missing policy on the
	// exact Writer reader. No cached or request-supplied policy is consulted.
	rows, err := s.snapshot.Get(ctx, []model.Ref{r.ref})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return out, nil
	}
	if len(rows) != 1 || rows[0].Kind != "lifecycle_policy" {
		return nil, fmt.Errorf("invalid lifecycle policy source")
	}
	var policy LifecyclePolicy
	if err := json.Unmarshal(rows[0].Body, &policy); err != nil {
		return nil, err
	}
	if policy.Namespace != r.namespace {
		return nil, fmt.Errorf("lifecycle policy namespace changed")
	}
	for _, grant := range policy.Grants {
		if grant.Principal.Kind != "user" || grant.Principal.Sponsor != nil || len(grant.Principal.Caveats) != 0 || !authz.ValidID(grant.Principal.ID) ||
			authz.CanonicalRef(grant.Project) != nil || !authz.ValidID(grant.Type) || !authz.ValidID(grant.FromStatus) || !authz.ValidID(grant.ToStatus) ||
			(grant.Effect != authz.Allow && grant.Effect != authz.Deny) || len(grant.Actions) == 0 {
			return nil, fmt.Errorf("invalid lifecycle grant")
		}
		for _, action := range grant.Actions {
			if action != authz.Modify && action != authz.Transition && action != authz.OverrideGuard {
				return nil, fmt.Errorf("invalid lifecycle action")
			}
		}
	}
	for i, q := range requests {
		if !closeCheck(q) || q.Caller.Principal.Kind != "user" {
			continue
		}
		binding, err := s.ProjectForBead(ctx, q.Resource.Ref)
		if err != nil {
			return nil, err
		}
		if !binding.Found || binding.Name != q.Transition.Project {
			continue
		}
		dep, err := authz.GrantDependency(r.namespace, authz.Scope{Kind: authz.ProjectScope, Ref: binding.Ref})
		if err != nil {
			return nil, err
		}
		if err := s.Record(ctx, dep); err != nil {
			return nil, err
		}
		active, err := s.ProjectActive(ctx, binding.Ref)
		if err != nil {
			return nil, err
		}
		if !active {
			continue
		}
		for _, grant := range policy.Grants {
			if !samePrincipal(grant.Principal, q.Caller.Principal) || grant.Project != binding.Ref || grant.Type != q.Transition.Type || grant.FromStatus != q.Transition.FromStatus || grant.ToStatus != q.Transition.ToStatus {
				continue
			}
			for _, action := range grant.Actions {
				if action == q.Action && out[i].Effect != authz.Deny {
					out[i] = authz.Decision{Effect: grant.Effect, Reasons: []string{"native_lifecycle_policy"}}
				}
			}
		}
	}
	return out, nil
}

func (c *Checker) closeContext(ctx context.Context, reader Snapshot, ref model.Ref) (authz.TransitionContext, error) {
	if c == nil || c.ontology == nil || reader == nil {
		return authz.TransitionContext{}, fmt.Errorf("close authority unavailable")
	}
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil {
		return authz.TransitionContext{}, err
	}
	if len(rows) != 1 || rows[0].Kind != "bead" {
		return authz.TransitionContext{}, fmt.Errorf("existing close target required")
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		return authz.TransitionContext{}, err
	}
	_, target, ok := c.ontology.CloseSlots(doc.Project, doc.Type)
	from, statusOK := doc.Fields["status"].(string)
	if !ok || !statusOK {
		return authz.TransitionContext{}, fmt.Errorf("close status unavailable")
	}
	return authz.TransitionContext{Project: doc.Project, Type: doc.Type, FromStatus: from, ToStatus: target}, nil
}

// ObserveTransitionPermission obtains an exact target-status decision from the
// installed chain using the caller's tracked Writer reader.
func (c *Checker) ObserveTransitionPermission(ctx context.Context, q service.TransitionPermissionRequest, reader port.SnapshotReader) (service.TransitionPermissionObservation, error) {
	observation := service.TransitionPermissionObservation{AuthzInstalled: true}
	if q.Action != string(authz.Transition) || q.Resource.Kind != "bead" || reader == nil {
		return observation, forbidden("invalid transition observation")
	}
	caller, err := c.callerFor(ctx, q.Principal)
	if err != nil {
		return observation, authzError(err)
	}
	s, err := c.load(ctx, reader, caller)
	if err != nil {
		return observation, authzError(err)
	}
	requests := []authz.CheckRequest{{Caller: caller, Action: authz.Transition, Resource: authz.Resource{Kind: authz.Bead, Ref: q.Resource.Ref}, Method: q.Method, Transition: authz.TransitionContext{Project: q.Project, Type: q.Type, FromStatus: q.FromStatus, ToStatus: q.ToStatus}}}
	decisions, err := c.chain.Check(ctx, s, requests)
	if err != nil {
		return observation, authzError(err)
	}
	if len(decisions) != 1 {
		return observation, forbidden("invalid transition decision")
	}
	allowed := decisions[0].Effect == authz.Allow
	observation.Permission = &allowed
	return observation, nil
}

// Lifecycle checks do not widen the existing CP7 Agent adapter. This narrow
// User-close guard verifies the stored/prepared images independently and
// rejects every permission-bearing reassignment and deputy mutation.
func validateLifecycleImage(before, after model.Record, by model.PrincipalRef, caller authz.Caller, setters map[string]string) error {
	if by.Kind != "user" || !samePrincipal(by, caller.Principal) || authz.CanonicalRef(before.Ref) != nil || before.Ref != after.Ref || before.Kind != "bead" || after.Kind != before.Kind || before.Revision.IsZero() || before.Body == nil || after.Body == nil {
		return forbidden("invalid lifecycle images or setter")
	}
	for _, owner := range setters {
		if owner != "request" && owner != "core" {
			return forbidden("lifecycle deputy mutation is unsupported")
		}
	}
	stableBefore, stableAfter := make(map[string]any), make(map[string]any)
	for key, value := range before.Body {
		stableBefore[key] = value
	}
	for key, value := range after.Body {
		stableAfter[key] = value
	}
	for _, key := range []string{"fields", "comments", "lease", "startedAt", "closedAt"} {
		delete(stableBefore, key)
		delete(stableAfter, key)
	}
	if !reflect.DeepEqual(stableBefore, stableAfter) {
		return forbidden("lifecycle reassignment is unsupported")
	}
	beforeFields, bok := before.Body["fields"].(map[string]any)
	afterFields, aok := after.Body["fields"].(map[string]any)
	if !bok || !aok {
		return forbidden("invalid lifecycle fields")
	}
	stableBefore, stableAfter = make(map[string]any), make(map[string]any)
	for key, value := range beforeFields {
		if key != "status" {
			stableBefore[key] = value
		}
	}
	for key, value := range afterFields {
		if key != "status" {
			stableAfter[key] = value
		}
	}
	if !reflect.DeepEqual(stableBefore, stableAfter) {
		return forbidden("lifecycle field mutation is unsupported")
	}
	return nil
}

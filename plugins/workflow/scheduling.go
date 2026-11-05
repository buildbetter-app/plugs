package workflow

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// OrdinaryParentFact comes only from one actual loaded checkpoint. Parent is
// the ordinary child's typed workflow parent; Blocked includes that parent's
// ancestry and the child's direct workflow dependencies. It is not a wire row.
type OrdinaryParentFact struct {
	Parent  *model.Ref
	Blocked bool
}

// ParentBlockerResolver lets the host follow a real ordinary ancestor chain
// against its pinned graph and this same checkpoint's typed parent lookup.
// The lookup never reloads state or re-enters the provider mutex.
type ParentBlockerResolver interface {
	IsParentBlocked(context.Context, model.PrincipalRef, model.WriteMeta, model.Ref, func(string) (OrdinaryParentFact, error)) (bool, error)
}

func itemParent(item itemState) (*model.Ref, error) {
	parent := cloneRef(item.Parent)
	for _, link := range item.Links {
		if link.Kind != "parent-child" {
			continue
		}
		if parent != nil && !sameRef(parent, &link.To) {
			return nil, fail("ontology_guard_failed", "workflow item has conflicting typed parents")
		}
		parent = cloneRef(&link.To)
	}
	return parent, nil
}

func itemAncestorsReady(ctx context.Context, value persisted, item itemState, by model.PrincipalRef, meta model.WriteMeta, resolver BlockerResolver) (bool, error) {
	r := parentReadiness{ctx: ctx, value: value, by: by, meta: meta, resolver: resolver, visiting: map[string]bool{item.ID: true}, done: map[string]bool{}}
	return r.ancestors(item)
}

// parentReadiness follows only parent edges in the loaded image. Ordinary
// bodies are resolved from one pinned host graph, never fetched recursively
// from the live service. The visiting set spans both planes and fails closed
// even for a corrupt mixed cycle; memoization retains only this evaluation.
type parentReadiness struct {
	ctx      context.Context
	value    persisted
	by       model.PrincipalRef
	meta     model.WriteMeta
	resolver BlockerResolver
	parents  map[string]model.Ref
	blocked  map[string]bool
	visiting map[string]bool
	done     map[string]bool
}

func (r *parentReadiness) ancestors(item itemState) (bool, error) {
	if err := r.ctx.Err(); err != nil {
		return false, err
	}
	parent, err := itemParent(item)
	if err != nil || parent == nil {
		return err == nil, err
	}
	if validateRef(*parent) != nil {
		return false, fail("internal_error", "invalid persisted workflow parent")
	}
	if workflowTarget(*parent) {
		return r.workflowParent(parent.ID)
	}
	if extended, ok := r.resolver.(ParentBlockerResolver); ok {
		blocked, err := extended.IsParentBlocked(r.ctx, r.by, r.meta, *parent, r.ordinary)
		return !blocked, err
	}
	// A resolver without the typed-parent lookup cannot prove mixed ancestry.
	return false, nil
}

func (r *parentReadiness) workflowParent(id string) (bool, error) {
	if err := r.ctx.Err(); err != nil {
		return false, err
	}
	if r.visiting[id] {
		return false, nil
	}
	if ready, ok := r.done[id]; ok {
		return ready, nil
	}
	ancestor, exists := r.value.Items[id]
	if !exists {
		return false, nil
	}
	blocked, err := itemBlockedWithResolver(r.ctx, r.value, ancestor, r.by, r.meta, r.resolver)
	if err != nil || blocked || ancestor.Status == "deferred" || ancestor.Status == "backlog" || ancestor.Status == "triage" {
		return false, err
	}
	r.visiting[id] = true
	ready, err := r.ancestors(ancestor)
	delete(r.visiting, id)
	if err == nil {
		r.done[id] = ready
	}
	return ready, err
}

func (r *parentReadiness) ordinary(id string) (OrdinaryParentFact, error) {
	if err := r.ctx.Err(); err != nil {
		return OrdinaryParentFact{}, err
	}
	if r.parents == nil {
		r.parents = map[string]model.Ref{}
		ids := []string{}
		for _, item := range r.value.Items {
			if err := r.ctx.Err(); err != nil {
				return OrdinaryParentFact{}, err
			}
			if r.meta.Project != "" && item.Project != r.meta.Project {
				continue
			}
			for _, link := range item.Inbound {
				ids = append(ids, link.From)
				if link.Kind != "parent-child" || workflowTarget(model.RefByID(link.From, model.Natural64(0))) {
					continue
				}
				if validateRef(link.To) != nil || link.To.ID != item.ID || !workflowTarget(link.To) || link.Metadata != nil || validateRef(model.RefByID(link.From, model.Natural64(0))) != nil {
					return OrdinaryParentFact{}, fail("internal_error", "invalid persisted mixed parent index")
				}
				if prior, exists := r.parents[link.From]; exists && !sameRef(&prior, &link.To) {
					return OrdinaryParentFact{}, fail("internal_error", "conflicting persisted mixed parents")
				}
				r.parents[link.From] = link.To
			}
		}
		r.blocked = ordinaryBlocked(r.value, ids, r.meta)
	}
	fact := OrdinaryParentFact{Blocked: r.blocked[id]}
	parent, exists := r.parents[id]
	if !exists {
		return fact, nil
	}
	fact.Parent = cloneRef(&parent)
	if r.visiting[id] {
		fact.Blocked = true
		return fact, nil
	}
	r.visiting[id] = true
	ready, err := r.workflowParent(parent.ID)
	delete(r.visiting, id)
	fact.Blocked = fact.Blocked || !ready
	return fact, err
}

package authz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// ProjectFilter is a request-local specialization of a complete FilterTemplate.
// Its project MUST come from current, verified authority, even for historical
// content. It is not a cached grant or a replacement for the host's remaining
// restrictions, current-row existence checks, parent projection, or final fence.
// In particular, a historical project name alone cannot authorize a moved bead.
type ProjectFilter struct {
	project ProjectBinding
	allowed bool
	refs    map[string]model.Ref // nil means unrestricted; empty means deny all
}

// ErrProjectFilterScope requires fallback or rebinding, not a denied bead.
// The complete policy may allow a candidate in another current project.
var ErrProjectFilterScope = errors.New("candidate outside bound current project")

// BindProject specializes the finite policy against one known current project.
// Every supported FilterExpr is constant within that project. Only refs caveats
// vary per bead; they become an exact intersection, never a project-wide permit.
// A check-only link must have been rejected by Chain.Compile before this call.
func (t FilterTemplate) BindProject(ctx context.Context, s Slice, project ProjectBinding) (ProjectFilter, error) {
	zero := ProjectFilter{}
	if err := ctx.Err(); err != nil {
		return zero, unavailable(err)
	}
	if err := t.Validate(); err != nil {
		return zero, unavailable(err)
	}
	if t.Request.ResourceKind != Bead {
		return zero, fmt.Errorf("%w: project specialization requires bead resources", ErrCheckOnly)
	}
	if !project.Found || !ValidID(project.Name) || CanonicalRef(project.Ref) != nil {
		return zero, unavailable(fmt.Errorf("project specialization requires a verified current project"))
	}
	out := ProjectFilter{project: project}
	if t.NoPlugins {
		if scoped(CheckRequest{Caller: t.Request.Caller}) {
			return zero, &Error{Code: "authz_not_installed"}
		}
		out.allowed = true
		return out, nil
	}
	// Copy mutable caller caveats before normalizing them. The bound filter
	// retains only owned refs, not the template, caller, Slice or effective facts.
	request, err := FreezeList(t.Request)
	if err != nil {
		return zero, unavailable(err)
	}
	q := CheckRequest{Caller: request.Caller, Action: request.Action, Method: request.Method,
		Resource: Resource{Kind: Project, Ref: project.Ref}}
	ok, err := prepare(ctx, s, t.Namespace, q)
	if err != nil || !ok {
		return out, err
	}
	// Project caveats and policy nodes use the exact project reference, not its
	// display name. Ref caveats are the only part not uniform across this scope.
	for p := &q.Caller.Principal; p != nil; p = p.Sponsor {
		for key, value := range p.Caveats {
			data, err := json.Marshal(value)
			if err != nil || string(data) == "null" {
				return out, nil
			}
			switch key {
			case "actions":
				var actions []string
				if json.Unmarshal(data, &actions) != nil || !slices.Contains(actions, string(q.Action)) {
					return out, nil
				}
			case "projects", "refs":
				var refs []model.Ref
				if json.Unmarshal(data, &refs) != nil {
					return out, nil
				}
				next := make(map[string]model.Ref, len(refs))
				for _, ref := range refs {
					if CanonicalRef(ref) != nil {
						return out, nil
					}
					next[ref.ID] = ref
				}
				if key == "projects" {
					if _, found := next[project.Ref.ID]; !found {
						return out, nil
					}
					continue
				}
				if out.refs != nil {
					for id := range next {
						if _, found := out.refs[id]; !found {
							delete(next, id)
						}
					}
				}
				out.refs = next
				if len(out.refs) == 0 {
					return out, nil
				}
			default:
				return out, nil
			}
		}
	}
	for _, restriction := range t.Restrictions {
		ok, err = evalExpr(ctx, s, q, restriction)
		if err != nil || !ok {
			if err != nil {
				err = unavailable(err)
			}
			return out, err
		}
	}
	deny, err := evalExpr(ctx, s, q, t.Forbid)
	if err != nil {
		return out, unavailable(err)
	}
	if deny {
		return out, nil
	}
	out.allowed, err = evalExpr(ctx, s, q, t.Permit)
	if err != nil {
		return zero, unavailable(err)
	}
	return out, nil
}

// Uniform reports whether every existing bead in the bound current project
// passes this template. Host restrictions and source coverage remain separate.
func (f ProjectFilter) Uniform() bool { return f.allowed && f.refs == nil }

// References returns an owned, bytewise ordered exact set and whether the
// template restricts refs. A denial is an explicitly restricted empty set.
func (f ProjectFilter) References() ([]model.Ref, bool) {
	if !f.allowed {
		return []model.Ref{}, true
	}
	if f.refs == nil {
		return nil, false
	}
	refs := make([]model.Ref, 0, len(f.refs))
	for _, ref := range f.refs {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b model.Ref) int { return strings.Compare(a.ID, b.ID) })
	return refs, true
}

// Allows checks an already verified CURRENT project mapping for one bead.
// Raw Ready count/pivot must be computed before applying this predicate.
func (f ProjectFilter) Allows(ctx context.Context, ref model.Ref, current ProjectBinding) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, unavailable(err)
	}
	if CanonicalRef(ref) != nil || strings.Contains(ref.ID, "/") {
		return false, nil
	}
	if !current.Found || current.Name != f.project.Name || CanonicalRef(current.Ref) != nil || current.Ref.ID != f.project.Ref.ID {
		return false, ErrProjectFilterScope
	}
	if !f.allowed {
		return false, nil
	}
	if f.refs == nil {
		return true, nil
	}
	_, found := f.refs[ref.ID]
	return found, nil
}

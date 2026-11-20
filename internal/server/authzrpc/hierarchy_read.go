package authzrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ParentDisclosure never contains a hidden parent's ID, project, title or
// status. The child must itself be readable before even Exists is disclosed.
type ParentDisclosure struct {
	Exists bool       `json:"exists"`
	Ref    *model.Ref `json:"ref,omitempty"`
}

// ParentProjectionAuthority contains projections for already visible children.
// A single CURRENT authority snapshot authorizes the complete batch against
// PINNED content; its binding must be validated again before publication.
type ParentProjectionAuthority struct {
	Parents       map[string]ParentDisclosure
	PolicyBinding string
}

func (c *Checker) AuthorizeParentProjections(ctx context.Context, principal model.PrincipalRef, content driver.Reader, children []model.Ref) (ParentProjectionAuthority, error) {
	if c == nil || content == nil || len(children) > listPostfilterCap {
		return ParentProjectionAuthority{}, hierarchyUnavailable()
	}
	authority, _, closeReader, err := (&GuardedService{checker: c, lease: c.lease}).current(ctx, nil)
	if err != nil {
		return ParentProjectionAuthority{}, err
	}
	defer closeReader()
	caller, err := c.callerFor(ctx, principal)
	if err != nil {
		return ParentProjectionAuthority{}, authzError(err)
	}
	s, err := c.load(ctx, authority, caller)
	if err != nil {
		return ParentProjectionAuthority{}, hierarchyUnavailable()
	}
	parents, err := c.parentProjections(ctx, caller, s, content, children)
	if err != nil {
		return ParentProjectionAuthority{}, err
	}
	binding, err := c.readAuthorityBinding(ctx, caller, s, authority)
	if err != nil {
		return ParentProjectionAuthority{}, err
	}
	if err := c.ValidateReadAuthorityBinding(ctx, principal, binding); err != nil {
		return ParentProjectionAuthority{}, err
	}
	return ParentProjectionAuthority{Parents: parents, PolicyBinding: binding}, nil
}

// AuthorizeParentProjection is the single-child form. Multi-item producers
// should use AuthorizeParentProjections once for their entire visible output.
func (c *Checker) AuthorizeParentProjection(ctx context.Context, principal model.PrincipalRef, content driver.Reader, child model.Ref) (ParentDisclosure, string, error) {
	result, err := c.AuthorizeParentProjections(ctx, principal, content, []model.Ref{child})
	if err != nil {
		return ParentDisclosure{}, "", err
	}
	return result.Parents[child.ID], result.PolicyBinding, nil
}

func (c *Checker) parentProjections(ctx context.Context, caller authz.Caller, authority *slice, content driver.Reader, children []model.Ref) (map[string]ParentDisclosure, error) {
	if content == nil || content.Snapshot().Incarnation != c.chain.Namespace.Incarnation {
		return nil, hierarchyUnavailable()
	}
	parents := make(map[string]ParentDisclosure, len(children))
	for _, child := range children {
		if hierarchyRef(child) != nil {
			return nil, hierarchyUnavailable()
		}
		if _, duplicate := parents[child.ID]; duplicate {
			return nil, hierarchyUnavailable()
		}
		allowed, err := c.hierarchyRead(ctx, caller, authority, child)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, notFound()
		}
		row, err := hierarchySource(ctx, content, child)
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, notFound()
		}
		target, _ := authz.HierarchyImage(*row)
		projection := ParentDisclosure{Exists: target.Parent != nil}
		if target.Parent != nil {
			// Historical content never carries an older read grant. A missing
			// or currently denied parent contributes existence only.
			current, err := hierarchySource(ctx, authority.snapshot, *target.Parent)
			if err != nil {
				return nil, err
			}
			if current != nil {
				allowed, err := c.hierarchyRead(ctx, caller, authority, *target.Parent)
				if err != nil {
					return nil, err
				}
				if allowed {
					ref := *target.Parent
					projection.Ref = &ref
				}
			}
		}
		parents[child.ID] = projection
	}
	return parents, nil
}

func projectBeadParent(bead *service.Bead, disclosure ParentDisclosure) {
	bead.Parent = ""
	bead.ParentHidden = disclosure.Exists && disclosure.Ref == nil
	if disclosure.Ref != nil {
		bead.Parent = disclosure.Ref.ID
	}
}

// ChildVisibility carries only readable child refs and the exact hidden
// remainder. Hidden identities and statuses never leave this boundary.
type ChildVisibility struct {
	Refs   []model.Ref   `json:"refs"`
	Hidden model.Natural `json:"hidden"`
}

// EpicProgressAuthority is a host result, not a wire extension. Its complete
// visible set and hidden remainder share one current authorization snapshot.
type EpicProgressAuthority struct {
	Visible       []model.Ref
	Hidden        model.Natural
	PolicyBinding string
}

// AuthorizeEpicProgress applies CURRENT authority to PINNED content. EP4/EP5
// must supply the complete direct-child query from content and bucket only the
// returned Visible refs using that same content reader. This opens one current
// authority reader, checks parent readability before exposing counts, and never
// exposes hidden statuses/IDs or treats historic grants as current authority.
func (c *Checker) AuthorizeEpicProgress(ctx context.Context, principal model.PrincipalRef, content driver.Reader, parent model.Ref, children []model.Ref) (EpicProgressAuthority, error) {
	if c == nil || content == nil {
		return EpicProgressAuthority{}, hierarchyUnavailable()
	}
	authority, _, closeReader, err := (&GuardedService{checker: c, lease: c.lease}).current(ctx, nil)
	if err != nil {
		return EpicProgressAuthority{}, err
	}
	defer closeReader()
	if content.Snapshot().Incarnation != authority.Snapshot().Incarnation || content.Snapshot().Incarnation != c.chain.Namespace.Incarnation {
		return EpicProgressAuthority{}, hierarchyUnavailable()
	}
	caller, err := c.callerFor(ctx, principal)
	if err != nil {
		return EpicProgressAuthority{}, authzError(err)
	}
	s, err := c.load(ctx, authority, caller)
	if err != nil {
		return EpicProgressAuthority{}, hierarchyUnavailable()
	}
	visibility, err := c.visibleChildren(ctx, caller, s, parent, children, content)
	if err != nil {
		return EpicProgressAuthority{}, err
	}
	binding, err := c.readAuthorityBinding(ctx, caller, s, authority)
	if err != nil {
		return EpicProgressAuthority{}, err
	}
	if err := c.ValidateReadAuthorityBinding(ctx, principal, binding); err != nil {
		return EpicProgressAuthority{}, err
	}
	return EpicProgressAuthority{Visible: visibility.Refs, Hidden: visibility.Hidden, PolicyBinding: binding}, nil
}

// ValidateReadAuthorityBinding performs a fresh final generation check, never
// a second independently filtered count. EP4 must call it with the returned
// PolicyBinding after status bucketing and immediately before publication.
// A changed context/policy fails the entire response; no partial count escapes.
func (c *Checker) ValidateReadAuthorityBinding(ctx context.Context, principal model.PrincipalRef, expected string) error {
	if c == nil || len(expected) != sha256.Size*2 {
		return hierarchyUnavailable()
	}
	current, _, closeReader, err := (&GuardedService{checker: c, lease: c.lease}).current(ctx, nil)
	if err != nil {
		return err
	}
	defer closeReader()
	binding, err := c.ReadAuthorityBinding(ctx, principal, current)
	if err != nil {
		return err
	}
	if binding != expected {
		return hierarchyUnavailable()
	}
	return nil
}

func (c *Checker) ParentDisclosure(ctx context.Context, principal model.PrincipalRef, child model.Ref, snapshot Snapshot) (ParentDisclosure, error) {
	if err := c.requireHierarchyRead(ctx, principal, child, snapshot); err != nil {
		return ParentDisclosure{}, err
	}
	value, err := hierarchySource(ctx, snapshot, child)
	if err != nil {
		return ParentDisclosure{}, err
	}
	if value == nil {
		return ParentDisclosure{}, notFound()
	}
	target, _ := authz.HierarchyImage(*value)
	if target.Parent == nil {
		return ParentDisclosure{}, nil
	}
	out := ParentDisclosure{Exists: true}
	parent, err := hierarchySource(ctx, snapshot, *target.Parent)
	if err != nil {
		return ParentDisclosure{}, err
	}
	if parent == nil {
		return out, nil
	}
	allowed, err := c.readDecision(ctx, snapshot, principal, "bead.get", *target.Parent)
	if err != nil {
		return ParentDisclosure{}, err
	}
	if allowed {
		ref := *target.Parent
		out.Ref = &ref
	}
	return out, nil
}

// VisibleChildren consumes the COMPLETE direct-child candidate set from one
// pinned hierarchy query. Its caller must reject truncated queries rather than
// publishing a short page/count. Every candidate is checked against actual
// source and its own read permission; duplicates and nonchildren fail closed.
// Query/closure membership reads must join the same tracked snapshot at the
// producer. EP5 performs status bucketing only for Refs, on that same snapshot.
func (c *Checker) VisibleChildren(ctx context.Context, principal model.PrincipalRef, parent model.Ref, children []model.Ref, snapshot Snapshot) (ChildVisibility, error) {
	caller, err := c.callerFor(ctx, principal)
	if err != nil {
		return ChildVisibility{}, authzError(err)
	}
	s, err := c.load(ctx, snapshot, caller)
	if err != nil {
		return ChildVisibility{}, hierarchyUnavailable()
	}
	return c.visibleChildren(ctx, caller, s, parent, children, snapshot)
}

func (c *Checker) visibleChildren(ctx context.Context, caller authz.Caller, authority *slice, parent model.Ref, children []model.Ref, content Snapshot) (ChildVisibility, error) {
	allowed, err := c.hierarchyRead(ctx, caller, authority, parent)
	if err != nil {
		return ChildVisibility{}, err
	}
	if !allowed {
		return ChildVisibility{}, notFound()
	}
	value, err := hierarchySource(ctx, content, parent)
	if err != nil {
		return ChildVisibility{}, err
	}
	if value == nil {
		return ChildVisibility{}, notFound()
	}
	out := ChildVisibility{Refs: []model.Ref{}, Hidden: model.Natural64(0)}
	seen := map[string]bool{}
	validChildren := make([]model.Ref, 0, len(children))
	for _, child := range children {
		if hierarchyRef(child) != nil || seen[child.ID] {
			return ChildVisibility{}, hierarchyUnavailable()
		}
		seen[child.ID] = true
		row, err := hierarchySource(ctx, content, child)
		if err != nil {
			return ChildVisibility{}, err
		}
		if row == nil {
			return ChildVisibility{}, hierarchyUnavailable()
		}
		target, _ := authz.HierarchyImage(*row)
		if target.Parent == nil || target.Parent.ID != parent.ID {
			return ChildVisibility{}, hierarchyUnavailable()
		}
		validChildren = append(validChildren, child)
	}
	childAllowed := make([]bool, len(validChildren))
	if c.canBatchHierarchyChecks() {
		childAllowed, err = c.hierarchyReadBatch(ctx, caller, authority, validChildren)
		if err != nil {
			return ChildVisibility{}, err
		}
	} else {
		for i, child := range validChildren {
			childAllowed[i], err = c.hierarchyRead(ctx, caller, authority, child)
			if err != nil {
				return ChildVisibility{}, err
			}
		}
	}
	for i, child := range validChildren {
		if childAllowed[i] {
			out.Refs = append(out.Refs, child)
		} else {
			hidden, _ := out.Hidden.Uint64()
			out.Hidden = model.Natural64(hidden + 1)
		}
	}
	return out, nil
}

func (c *Checker) hierarchyReadBatch(ctx context.Context, caller authz.Caller, authority authz.Slice, refs []model.Ref) ([]bool, error) {
	if len(refs) == 0 {
		return []bool{}, ctx.Err()
	}
	requests := make([]authz.CheckRequest, len(refs))
	for i, ref := range refs {
		requests[i] = authz.CheckRequest{Caller: caller, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: ref}, Method: "bead.get"}
	}
	decisions, err := c.chain.Check(ctx, authority, requests)
	if err != nil {
		return nil, authzError(err)
	}
	if len(decisions) != len(refs) {
		return nil, hierarchyUnavailable()
	}
	allowed := make([]bool, len(refs))
	for i, decision := range decisions {
		allowed[i] = decision.Effect == authz.Allow
	}
	return allowed, nil
}

func (c *Checker) hierarchyRead(ctx context.Context, caller authz.Caller, authority *slice, ref model.Ref) (bool, error) {
	decisions, err := c.chain.Check(ctx, authority, []authz.CheckRequest{{Caller: caller, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: ref}, Method: "bead.get"}})
	if err != nil {
		return false, authzError(err)
	}
	if len(decisions) != 1 {
		return false, hierarchyUnavailable()
	}
	return decisions[0].Effect == authz.Allow, nil
}

// ReadAuthorityBinding is an opaque continuation binding, never a permit.
// Hosts MUST call it with a fresh CURRENT authority snapshot on every page,
// compare with the cursor, then use one pinned data snapshot for filtering and
// counts. Reusing only an old data snapshot cannot detect revocation. Facts
// sources must track all their authoritative reads, as required by FactsSource.
func (c *Checker) ReadAuthorityBinding(ctx context.Context, principal model.PrincipalRef, snapshot Snapshot) (string, error) {
	caller, err := c.callerFor(ctx, principal)
	if err != nil {
		return "", authzError(err)
	}
	s, err := c.load(ctx, snapshot, caller)
	if err != nil {
		return "", hierarchyUnavailable()
	}
	return c.readAuthorityBinding(ctx, caller, s, snapshot)
}

func (c *Checker) readAuthorityBinding(ctx context.Context, caller authz.Caller, s *slice, snapshot Snapshot) (string, error) {
	viewSource, ok := snapshot.(interface{ Snapshot() driver.View })
	if !ok || viewSource.Snapshot().At == nil {
		return "", hierarchyUnavailable()
	}
	view := viewSource.Snapshot()
	contextFact, err := driver.NewReadFact(authz.ContextDependency(c.chain.Namespace).Key)
	if err != nil {
		return "", hierarchyUnavailable()
	}
	keys := []driver.LockKey{{Kind: driver.LockReadFact, Fact: contextFact}, driver.PluginsSentinelLock, driver.OntologySentinelLock}
	states := make([]driver.LockState, 0, len(keys))
	for _, key := range keys {
		values, err := snapshot.Observe(ctx, []driver.LockKey{key})
		if err != nil || len(values) != 1 {
			return "", hierarchyUnavailable()
		}
		states = append(states, values[0])
	}
	data, err := json.Marshal(struct {
		Version string
		View    driver.View
		Caller  authz.Caller
		Facts   any
		ACL     model.Revision
		States  []driver.LockState
	}{"ep2/read-scope/2", view, caller, s.facts, s.authorizationRevision, states})
	if err != nil {
		return "", hierarchyUnavailable()
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func (c *Checker) requireHierarchyRead(ctx context.Context, principal model.PrincipalRef, ref model.Ref, snapshot Snapshot) error {
	if snapshot == nil || hierarchyRef(ref) != nil {
		return notFound()
	}
	allowed, err := c.readDecision(ctx, snapshot, principal, "bead.get", ref)
	if err != nil {
		return err
	}
	if !allowed {
		return notFound()
	}
	return nil
}

func hierarchySource(ctx context.Context, snapshot Snapshot, ref model.Ref) (*model.Record, error) {
	if snapshot == nil || hierarchyRef(ref) != nil {
		return nil, hierarchyUnavailable()
	}
	rows, err := snapshot.Get(ctx, []model.Ref{ref})
	if err != nil {
		return nil, hierarchyUnavailable()
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) != 1 || rows[0].Kind != "bead" || hierarchyRef(rows[0].Ref) != nil || rows[0].Ref.ID != ref.ID || rows[0].Revision.IsZero() {
		return nil, hierarchyUnavailable()
	}
	value, err := hierarchyRaw(ref, rows[0].Revision, rows[0].Body)
	if err != nil {
		return nil, hierarchyUnavailable()
	}
	return &value, nil
}

func hierarchyRaw(ref model.Ref, revision model.Revision, raw []byte) (model.Record, error) {
	var body map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&body); err != nil {
		return model.Record{}, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return model.Record{}, hierarchyUnavailable()
	}
	value := model.Record{Ref: ref, Kind: "bead", Revision: revision, Body: body}
	if _, err := authz.HierarchyImage(value); err != nil {
		return model.Record{}, err
	}
	return value, nil
}

package authzrpc

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// projectionReadSlice reuses sources observed in this authority snapshot.
// Hits and misses come from a real batch Get; policy dependencies other than
// these exact records still pass through the original tracked slice.
type projectionReadSlice struct {
	*slice
	rows  map[string]driver.Record
	known map[string]bool
}

func (s *projectionReadSlice) Record(ctx context.Context, dep authz.Dependency) error {
	if dep.Kind == "record" && s.known[dep.Key] {
		return ctx.Err()
	}
	return s.slice.Record(ctx, dep)
}

func (s *projectionReadSlice) ProjectForBead(ctx context.Context, ref model.Ref) (authz.ProjectBinding, error) {
	if err := ctx.Err(); err != nil {
		return authz.ProjectBinding{}, err
	}
	if !s.known[ref.ID] {
		return s.slice.ProjectForBead(ctx, ref)
	}
	row, found := s.rows[ref.ID]
	if !found {
		return authz.ProjectBinding{}, nil
	}
	doc, err := pipeline.DecodeDocument(row.Body)
	if err != nil {
		return authz.ProjectBinding{}, err
	}
	if doc.Project == "" {
		return authz.ProjectBinding{}, nil
	}
	for _, project := range s.facts.Projects {
		if project.Name == doc.Project {
			return authz.ProjectBinding{Name: project.Name, Ref: project.Ref, Found: true}, nil
		}
	}
	return authz.ProjectBinding{Name: doc.Project}, nil
}

func projectionRows(refs []model.Ref, rows []driver.Record) (map[string]driver.Record, error) {
	wanted := make(map[string]model.Ref, len(refs))
	for _, ref := range refs {
		if hierarchyRef(ref) != nil {
			return nil, hierarchyUnavailable()
		}
		if _, duplicate := wanted[ref.ID]; duplicate {
			return nil, hierarchyUnavailable()
		}
		wanted[ref.ID] = ref
	}
	out := make(map[string]driver.Record, len(rows))
	for _, row := range rows {
		ref, requested := wanted[row.Ref.ID]
		if !requested || !driver.SelectionSameRef(ref, row.Ref) || row.Kind != "bead" || row.Revision.Validate() != nil || row.Revision.IsZero() {
			return nil, hierarchyUnavailable()
		}
		if _, duplicate := out[row.Ref.ID]; duplicate {
			return nil, hierarchyUnavailable()
		}
		out[row.Ref.ID] = row
	}
	return out, nil
}

func (s *projectionReadSlice) load(ctx context.Context, refs []model.Ref) error {
	missing := make([]model.Ref, 0, len(refs))
	for _, ref := range refs {
		if !s.known[ref.ID] {
			missing = append(missing, ref)
		}
	}
	if len(missing) == 0 {
		return ctx.Err()
	}
	rows, err := s.snapshot.Get(ctx, missing)
	if err != nil {
		return hierarchyUnavailable()
	}
	indexed, err := projectionRows(missing, rows)
	if err != nil {
		return err
	}
	for _, ref := range missing {
		s.known[ref.ID] = true
	}
	for id, row := range indexed {
		s.rows[id] = row
	}
	return nil
}

func sameProjectionSnapshot(content driver.Reader, authority Snapshot) bool {
	other, ok := authority.(interface{ Snapshot() driver.View })
	if !ok {
		return false
	}
	a, b := content.Snapshot(), other.Snapshot()
	return a.At != nil && b.At != nil && a.Incarnation == b.Incarnation && a.Branch.String() == b.Branch.String() && a.At.String() == b.At.String()
}

func (c *Checker) hierarchyReadBatch(ctx context.Context, caller authz.Caller, authority authz.Slice, refs []model.Ref, filter *authz.FilterTemplate) ([]bool, error) {
	if len(refs) == 0 {
		return []bool{}, ctx.Err()
	}
	allowed := make([]bool, len(refs))
	for i, ref := range refs {
		if filter != nil {
			var err error
			allowed[i], err = filter.Match(ctx, authority, authz.Resource{Kind: authz.Bead, Ref: ref})
			if err != nil {
				return nil, authzError(err)
			}
			continue
		}
		// A check-only component keeps its existing per-resource budget.
		// Batching source reads must not impose one shared component deadline
		// on all emitted items or turn a valid custom policy into a refusal.
		decisions, err := c.chain.Check(ctx, authority, []authz.CheckRequest{{Caller: caller, Action: authz.Read, Resource: authz.Resource{Kind: authz.Bead, Ref: ref}, Method: "bead.get"}})
		if err != nil {
			return nil, authzError(err)
		}
		if len(decisions) != 1 {
			return nil, hierarchyUnavailable()
		}
		allowed[i] = decisions[0].Effect == authz.Allow
	}
	return allowed, nil
}

// parentProjectionsFromRows consumes only already emitted item sources. When
// content and authority are the same pinned view it does not read those item
// bodies a second time. Parent sources are fetched once per distinct parent.
func (c *Checker) parentProjectionsFromRows(ctx context.Context, caller authz.Caller, authority *slice, content driver.Reader, rows []driver.Record) (map[string]ParentDisclosure, error) {
	children := make([]model.Ref, len(rows))
	for i, row := range rows {
		children[i] = row.Ref
	}
	return c.parentProjectionsBatch(ctx, caller, authority, content, children, rows)
}

func (c *Checker) parentProjectionsBatch(ctx context.Context, caller authz.Caller, authority *slice, content driver.Reader, children []model.Ref, supplied []driver.Record) (map[string]ParentDisclosure, error) {
	if content == nil || authority == nil || content.Snapshot().Incarnation != c.chain.Namespace.Incarnation {
		return nil, hierarchyUnavailable()
	}
	if _, err := projectionRows(children, nil); err != nil {
		return nil, err
	}
	current := &projectionReadSlice{slice: authority, rows: map[string]driver.Record{}, known: map[string]bool{}}
	contentRows := map[string]driver.Record(nil)
	if supplied != nil {
		var err error
		contentRows, err = projectionRows(children, supplied)
		if err != nil {
			return nil, err
		}
		if sameProjectionSnapshot(content, authority.snapshot) {
			for _, ref := range children {
				current.known[ref.ID] = true
			}
			for id, row := range contentRows {
				current.rows[id] = row
			}
		}
	}
	if err := current.load(ctx, children); err != nil {
		return nil, err
	}
	// Compile one complete parent-read policy for both source batches. A
	// component without a usable exact compiler retains its ordinary Check;
	// compilation failure never manufactures a denial or an authorization.
	var filter *authz.FilterTemplate
	if compiled, err := c.chain.Compile(authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "bead.get"}); err == nil {
		filter = &compiled
	}
	allowed, err := c.hierarchyReadBatch(ctx, caller, current, children, filter)
	if err != nil {
		return nil, err
	}
	for _, value := range allowed {
		if !value {
			return nil, notFound()
		}
	}
	if contentRows == nil {
		if sameProjectionSnapshot(content, authority.snapshot) {
			contentRows = current.rows
		} else {
			rows, err := content.Get(ctx, children)
			if err != nil {
				return nil, hierarchyUnavailable()
			}
			contentRows, err = projectionRows(children, rows)
			if err != nil {
				return nil, err
			}
		}
	}
	parents := make(map[string]ParentDisclosure, len(children))
	parentRefs := make([]model.Ref, 0)
	seenParents := map[string]bool{}
	targets := make(map[string]model.Ref, len(children))
	for _, child := range children {
		row, found := contentRows[child.ID]
		if !found {
			return nil, notFound()
		}
		value, err := hierarchyRaw(row.Ref, row.Revision, row.Body)
		if err != nil {
			return nil, hierarchyUnavailable()
		}
		target, _ := authz.HierarchyImage(value)
		parents[child.ID] = ParentDisclosure{Exists: target.Parent != nil}
		if target.Parent != nil {
			targets[child.ID] = *target.Parent
			if !seenParents[target.Parent.ID] {
				seenParents[target.Parent.ID] = true
				parentRefs = append(parentRefs, *target.Parent)
			}
		}
	}
	if err := current.load(ctx, parentRefs); err != nil {
		return nil, err
	}
	present := make([]model.Ref, 0, len(parentRefs))
	for _, ref := range parentRefs {
		if row, found := current.rows[ref.ID]; found {
			if _, err := hierarchyRaw(row.Ref, row.Revision, row.Body); err != nil {
				return nil, hierarchyUnavailable()
			}
			present = append(present, ref)
		}
	}
	parentAllowed, err := c.hierarchyReadBatch(ctx, caller, current, present, filter)
	if err != nil {
		return nil, err
	}
	visible := make(map[string]bool, len(present))
	for i, ref := range present {
		visible[ref.ID] = parentAllowed[i]
	}
	for child, parent := range targets {
		if visible[parent.ID] {
			ref := parent
			parents[child] = ParentDisclosure{Exists: true, Ref: &ref}
		}
	}
	return parents, nil
}

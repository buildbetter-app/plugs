package service

import (
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

func hierarchyGraphNeeded(proposal port.ProposedChange) bool {
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" {
			continue
		}
		if change.Op.Kind == model.OpReparent {
			return true
		}
		for _, raw := range [][]byte{change.Before, change.After} {
			if hasDocument(raw) {
				if doc, err := decodeDocument(raw); err == nil && doc.Parent != nil {
					return true
				}
			}
		}
	}
	return false
}

func (o *Ontology) noOpenChildren(parent pipeline.Document, graph map[string]storedBead) bool {
	for _, child := range graph {
		if child.doc.Parent == nil || child.doc.Parent.ID != parent.ID {
			continue
		}
		category, known := o.effective(child.doc.Project).Category(state(child.doc).Status)
		if !known || !category.Terminal() {
			return false
		}
	}
	return true
}

// An unchanged parent must not acquire a child which would invalidate its
// current relational guards. This rechecks only authoritative child/blocker
// facts for that status; it grants no transition or permission and never
// invents a status update on the parent. Its source and child phantoms are
// guarded by the same tracked final-graph query as ordinary lifecycle guards.
func (o *Ontology) validateHierarchyParents(proposal port.ProposedChange, graph map[string]storedBead) error {
	if graph == nil {
		return nil
	}
	parents := map[string]bool{}
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" {
			continue
		}
		for _, raw := range [][]byte{change.Before, change.After} {
			if !hasDocument(raw) {
				continue
			}
			doc, err := decodeDocument(raw)
			if err != nil {
				return err
			}
			if doc.Parent != nil {
				parents[doc.Parent.ID] = true
			}
		}
	}
	for id := range parents {
		parent, exists := graph[id]
		if !exists { // Final dangling-parent refusal belongs to the core planner.
			continue
		}
		doc := parent.doc
		effective := o.effective(doc.Project).Document()
		typ, exists := effective.Types[doc.Type]
		if !exists {
			return failure("ontology_guard_failed", "parent workflow is unavailable")
		}
		for _, transition := range effective.Workflows[typ.Workflow].Transitions {
			if transition.To != state(doc).Status {
				continue
			}
			for _, guard := range transition.Guards {
				switch guard.Kind {
				case ontology.NoOpenChildren:
					if !o.noOpenChildren(doc, graph) {
						return failure("ontology_guard_failed", "parent child guard is not satisfied")
					}
				case ontology.NoOpenBlockers:
					closed, err := o.noOpenBlockers(doc, graph)
					if err != nil {
						return err
					}
					if !closed {
						return failure("ontology_guard_failed", "parent blocker guard is not satisfied")
					}
				}
			}
		}
	}
	return nil
}

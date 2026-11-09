package service

import (
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

func hierarchyGraphNeeded(proposal port.ProposedChange) (bool, error) {
	for _, change := range proposal.Changes {
		if change.Resource.Kind != "bead" {
			continue
		}
		before, err := proposedImage(change.Before)
		if err != nil {
			return false, err
		}
		after, err := proposedImage(change.After)
		if err != nil {
			return false, err
		}
		if change.Op.Kind == model.OpReparent {
			return true, nil
		}
		if change.Op.Kind == model.OpLink || change.Op.Kind == model.OpUnlink {
			// Link mutations can change parent-child or blocker facts used by
			// lifecycle guards even when the explicit Parent field is stable.
			return true, nil
		}
		if before.parent != after.parent || before.hasDocument && after.hasDocument && (before.status != after.status || before.project != after.project || before.typ != after.typ) {
			return true, nil
		}
		if before.hasDocument && after.hasDocument && changeTouchesStatus(change) {
			return true, nil
		}
	}
	return false, nil
}

type proposalImage struct {
	hasDocument bool
	parent      string
	status      string
	project     string
	typ         string
}

func changeTouchesStatus(change port.Change) bool {
	for path := range change.SetBy {
		if path == "status" || path == "fields.status" {
			return true
		}
	}
	if change.Op.Patch != nil {
		if _, exists := change.Op.Patch.Fields["status"]; exists {
			return true
		}
	}
	return false
}

// proposedImage extracts only facts that can affect a relational guard. A
// missing image has no parent/status relation, so root creates/deletes remain
// on the no-scan path while introducing/removing a non-root relation does not.
// The parent key is canonical JSON, retaining nil/presence and branch identity
// under the strict document decoder.
func proposedImage(raw []byte) (proposalImage, error) {
	if !hasDocument(raw) {
		return proposalImage{}, nil
	}
	doc, err := decodeDocument(raw)
	if err != nil {
		return proposalImage{}, err
	}
	image := proposalImage{hasDocument: true, status: state(doc).Status, project: doc.Project, typ: doc.Type}
	if doc.Parent == nil {
		return image, nil
	}
	parent, err := json.Marshal(doc.Parent)
	if err != nil {
		return proposalImage{}, err
	}
	image.parent = string(parent)
	return image, nil
}

func (o *Ontology) noOpenChildren(parent pipeline.Document, graph map[string]storedBead) bool {
	for _, child := range graph {
		linked := false
		if child.doc.Parent != nil && child.doc.Parent.ID == parent.ID {
			linked = true
		}
		if !linked {
			for _, edge := range child.doc.Links {
				if edge.TargetKind != model.LinkTargetExternal && edge.Kind == "parent-child" && edge.To.ID == parent.ID {
					linked = true
					break
				}
			}
		}
		if !linked {
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

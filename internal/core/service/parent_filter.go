package service

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// parentFilter resolves membership on the same certified source snapshot as
// the other filters. Native ready selects transitive descendants; native JSON
// list selects direct children. Neither selection prunes ready's blocker graph.
func (s *Service) parentFilter(ctx context.Context, r driver.Reader, filter *Filter, transitive bool) (map[string]bool, error) {
	if filter == nil {
		return nil, nil
	}
	if filter.Parent != "" && s.workflowProvider() != nil {
		return nil, failure("unsupported", "parent filtering requires a complete mixed-plane selection capability")
	}
	if filter.Parent == "" {
		return nil, nil
	}
	ref := model.RefByID(filter.Parent, model.Natural64(0))
	if err := driver.ValidateHierarchyRef(ref); err != nil {
		return nil, failure("invalid_params", "filter.parent requires a nonempty branch-zero bead ID")
	}
	indexed, ok := r.(driver.HierarchyChildrenReader)
	if !ok {
		return nil, failure("unsupported", "parent filtering requires indexed hierarchy storage")
	}
	var selected map[string]bool
	var err error
	if transitive {
		selected, err = driver.DescendantFilter(ctx, indexed, ref)
	} else {
		selected, err = directParentChildren(ctx, indexed, ref)
	}
	if errors.Is(err, driver.ErrUnsupported) {
		return nil, failure("unsupported", "parent-filter hierarchy index coverage is unavailable")
	}
	return selected, mapped(err)
}

// mixedReadyParents keeps the source planes separate until visible candidates
// have been selected. A colliding durable row must not supply a workflow row's
// parent, and excluding workflow output must retain durable ancestry.
type mixedReadyParents struct {
	ordinary map[string]bool
	workflow map[string]bool
}

// mixedReadyParentFilter traverses the certified ordinary hierarchy index and
// complete typed parent facts from the already captured provider image. Source
// presence resolves identity; it never supplies ordinary parent membership.
func mixedReadyParentFilter(ctx context.Context, r driver.Reader, filter *Filter, snapshot workflowplugin.ReadySnapshot, graph map[string]storedBead, includeEphemeral bool) (mixedReadyParents, error) {
	if filter == nil || filter.Parent == "" {
		return mixedReadyParents{}, nil
	}
	root := model.RefByID(filter.Parent, model.Natural64(0))
	validRef := func(ref model.Ref) bool {
		if workflowRef(ref) {
			return validateWorkflowRef(ref) == nil
		}
		return driver.ValidateHierarchyRef(ref) == nil
	}
	if !validRef(root) {
		return mixedReadyParents{}, failure("invalid_params", "filter.parent requires a branch-zero bead or workflow item ID")
	}
	indexed, ok := r.(driver.HierarchyChildrenReader)
	if !ok {
		return mixedReadyParents{}, failure("unsupported", "parent filtering requires indexed hierarchy storage")
	}
	type node struct {
		id       string
		workflow bool
	}
	// Resolve every ID to the source that owns its effective namespace for this
	// read. An active workflow shadows a durable collision only when ephemeral
	// output is included. With that output excluded, a durable collision owns
	// the ID while a workflow-only node remains available as an ancestry bridge
	// for ordinary descendants. The resolver is used for both parent keys and
	// child identities so a source edge can never silently cross planes.
	sourceForID := func(id string) (node, bool) {
		_, active := snapshot.ActiveIDs[id]
		_, durable := graph[id]
		switch {
		case active && includeEphemeral:
			return node{id, true}, true
		case durable:
			return node{id, false}, true
		case active:
			return node{id, true}, true
		default:
			return node{}, false
		}
	}
	parentSource := func(ref model.Ref) (node, bool) {
		parent, ok := sourceForID(ref.ID)
		if !ok {
			return node{}, false
		}
		// A typed workflow parent remains a workflow edge. When a durable row
		// collides with that ID and workflow output is excluded, its durable
		// ancestry must not be guessed as the workflow ancestry.
		if workflowRef(ref) && !parent.workflow {
			return node{}, false
		}
		return parent, true
	}
	children := map[node][]node{}
	for id, parent := range snapshot.WorkflowParents {
		if parent == nil {
			continue
		}
		if !validRef(model.RefByID(id, model.Natural64(0))) || !validRef(*parent) {
			return mixedReadyParents{}, failure("internal_error", "invalid workflow parent-filter source")
		}
		child, childOK := sourceForID(id)
		parentNode, parentOK := parentSource(*parent)
		if !childOK || !child.workflow || !parentOK {
			// A colliding durable child owns the durable namespace when
			// ephemeral output is excluded; its workflow edge is not an
			// ordinary parent fact.
			continue
		}
		children[parentNode] = append(children[parentNode], child)
	}
	for id, fact := range snapshot.OrdinaryParents {
		if fact.Parent == nil {
			continue
		}
		if !validRef(model.RefByID(id, model.Natural64(0))) || !validRef(*fact.Parent) {
			return mixedReadyParents{}, failure("internal_error", "invalid ordinary mixed parent-filter source")
		}
		child, childOK := sourceForID(id)
		parentNode, parentOK := parentSource(*fact.Parent)
		if !childOK || child.workflow || !parentOK {
			// An active workflow row is the effective child when included;
			// its durable fact cannot supply a second parent route. The same
			// source check also keeps a workflow parent edge typed when the
			// durable-only projection is requested.
			continue
		}
		children[parentNode] = append(children[parentNode], child)
	}
	selected := mixedReadyParents{ordinary: map[string]bool{}, workflow: map[string]bool{}}
	rootNode, rootOK := sourceForID(root.ID)
	if !rootOK {
		return selected, nil
	}
	seen := map[node]bool{rootNode: true}
	pending := []node{rootNode}
	for len(pending) != 0 {
		if err := ctx.Err(); err != nil {
			return mixedReadyParents{}, err
		}
		end := min(len(pending), driver.HierarchyMaxParents)
		batch := pending[:end]
		pending = pending[end:]
		ordinary := make([]model.Ref, 0, len(batch))
		next := map[node]bool{}
		for _, parent := range batch {
			for _, child := range children[parent] {
				next[child] = true
			}
			if !parent.workflow {
				ordinary = append(ordinary, model.RefByID(parent.id, model.Natural64(0)))
			}
		}
		if len(ordinary) != 0 {
			rows, more, err := indexed.HierarchyChildren(ctx, ordinary, driver.HierarchyMaxLimit)
			if err != nil {
				return mixedReadyParents{}, mapped(err)
			}
			if more {
				return mixedReadyParents{}, mapped(driver.ErrDescendantFilterLimit{Limit: driver.HierarchyMaxLimit})
			}
			for _, row := range rows {
				if row.Kind != "bead" || driver.ValidateHierarchyRef(row.Ref) != nil {
					return mixedReadyParents{}, failure("unsupported", "invalid indexed parent-filter source")
				}
				child, childOK := sourceForID(row.Ref.ID)
				if !childOK || child.workflow {
					continue
				}
				if fact := snapshot.OrdinaryParents[row.Ref.ID]; fact.Parent != nil {
					// A typed workflow parent belongs to the checkpoint plane.
					// A durable source edge cannot provide another path to it.
					continue
				}
				next[child] = true
			}
		}
		for child := range next {
			if seen[child] {
				return mixedReadyParents{}, failure("unsupported", "cyclic or duplicate mixed parent-filter source")
			}
			seen[child] = true
			if child.workflow {
				selected.workflow[child.id] = true
			} else {
				selected.ordinary[child.id] = true
			}
			if len(seen)-1 > driver.SelectionOrdinaryCap {
				return mixedReadyParents{}, mapped(driver.ErrDescendantFilterLimit{Limit: driver.SelectionOrdinaryCap})
			}
			pending = append(pending, child)
		}
	}
	if err := ctx.Err(); err != nil {
		return mixedReadyParents{}, err
	}
	return selected, nil
}

func directParentChildren(ctx context.Context, indexed driver.HierarchyChildrenReader, parent model.Ref) (map[string]bool, error) {
	rows, more, err := indexed.HierarchyChildren(ctx, []model.Ref{parent}, driver.HierarchyMaxLimit)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if more {
		return nil, driver.ErrDescendantFilterLimit{Limit: driver.HierarchyMaxLimit}
	}
	selected := map[string]bool{}
	for _, row := range rows {
		if row.Kind != "bead" || driver.ValidateHierarchyRef(row.Ref) != nil || row.Ref.ID == parent.ID || selected[row.Ref.ID] {
			return nil, driver.ErrUnsupported
		}
		selected[row.Ref.ID] = true
	}
	return selected, nil
}

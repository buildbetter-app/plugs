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

// mixedReadyParentFilter traverses the certified ordinary hierarchy index and
// the complete typed parent facts from the already captured provider image.
// Neither ready eligibility nor any other filter prunes intermediate parents.
// Provider-only edges never become guessed ordinary rows or index entries.
func mixedReadyParentFilter(ctx context.Context, r driver.Reader, filter *Filter, snapshot workflowplugin.ReadySnapshot) (map[string]bool, error) {
	if filter == nil || filter.Parent == "" {
		return nil, nil
	}
	root := model.RefByID(filter.Parent, model.Natural64(0))
	validRef := func(ref model.Ref) bool {
		return driver.ValidateHierarchyRef(ref) == nil || validateWorkflowRef(ref) == nil
	}
	if !validRef(root) {
		return nil, failure("invalid_params", "filter.parent requires a branch-zero bead or workflow item ID")
	}
	indexed, ok := r.(driver.HierarchyChildrenReader)
	if !ok {
		return nil, failure("unsupported", "parent filtering requires indexed hierarchy storage")
	}
	children := map[string][]model.Ref{}
	add := func(id string, parent *model.Ref) error {
		if parent == nil {
			return nil
		}
		child := model.RefByID(id, model.Natural64(0))
		if !validRef(child) || !validRef(*parent) {
			return failure("internal_error", "invalid mixed parent-filter source")
		}
		children[parent.ID] = append(children[parent.ID], child)
		return nil
	}
	for id, parent := range snapshot.WorkflowParents {
		if err := add(id, parent); err != nil {
			return nil, err
		}
	}
	for id, fact := range snapshot.OrdinaryParents {
		if err := add(id, fact.Parent); err != nil {
			return nil, err
		}
	}
	selected := map[string]bool{}
	seen := map[string]bool{root.ID: true}
	pending := []model.Ref{root}
	for len(pending) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(len(pending), driver.HierarchyMaxParents)
		batch := pending[:end]
		pending = pending[end:]
		ordinary := make([]model.Ref, 0, len(batch))
		next := []model.Ref{}
		for _, parent := range batch {
			next = append(next, children[parent.ID]...)
			if driver.ValidateHierarchyRef(parent) == nil {
				ordinary = append(ordinary, parent)
			}
		}
		if len(ordinary) != 0 {
			rows, more, err := indexed.HierarchyChildren(ctx, ordinary, driver.HierarchyMaxLimit)
			if err != nil {
				return nil, mapped(err)
			}
			if more {
				return nil, mapped(driver.ErrDescendantFilterLimit{Limit: driver.HierarchyMaxLimit})
			}
			for _, row := range rows {
				if row.Kind != "bead" || driver.ValidateHierarchyRef(row.Ref) != nil {
					return nil, failure("unsupported", "invalid indexed parent-filter source")
				}
				next = append(next, row.Ref)
			}
		}
		for _, child := range next {
			if seen[child.ID] {
				return nil, failure("unsupported", "cyclic or duplicate mixed parent-filter source")
			}
			seen[child.ID], selected[child.ID] = true, true
			if len(selected) > driver.SelectionOrdinaryCap {
				return nil, mapped(driver.ErrDescendantFilterLimit{Limit: driver.SelectionOrdinaryCap})
			}
			pending = append(pending, child)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
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

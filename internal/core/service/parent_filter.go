package service

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// parentFilter resolves membership on the same certified source snapshot as
// the other filters. Native ready selects transitive descendants; native JSON
// list selects direct children. Neither selection prunes ready's blocker graph.
func (s *Service) parentFilter(ctx context.Context, r driver.Reader, filter *Filter, transitive bool) (map[string]bool, error) {
	if filter == nil {
		return nil, nil
	}
	if (filter.Parent != "" || filter.Priority != nil) && s.workflowProvider() != nil {
		return nil, failure("unsupported", "parent/priority filtering requires a complete mixed-plane selection capability")
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

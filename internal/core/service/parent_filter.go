package service

import (
	"context"
	"errors"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// parentFilter resolves the storage-owned transitive predicate on the same
// snapshot used for other membership facts. Ready must retain the complete
// dependency graph, but only matching descendants enter its ordered page.
func (s *Service) parentFilter(ctx context.Context, r driver.Reader, filter *Filter) (map[string]bool, error) {
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
	selected, err := driver.DescendantFilter(ctx, indexed, ref)
	if errors.Is(err, driver.ErrUnsupported) {
		return nil, failure("unsupported", "parent-filter hierarchy index coverage is unavailable")
	}
	return selected, mapped(err)
}

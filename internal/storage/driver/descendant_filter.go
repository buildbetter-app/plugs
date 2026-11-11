package driver

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// DescendantFilter compiles a transitive parent predicate from the reader's
// certified source index before the caller orders or pages its candidates.
// The root is excluded. Parentage is never inferred from ID spelling, generic
// Query results, or derived closure caches. Unknown roots produce an empty set.
// Unsupported coverage, overflow and corrupt cycles return no partial set.
func DescendantFilter(ctx context.Context, reader HierarchyChildrenReader, parent model.Ref) (map[string]bool, error) {
	if err := ValidateHierarchyRef(parent); err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, fmt.Errorf("%w: parent filter requires indexed hierarchy reads", ErrUnsupported)
	}
	pending := []model.Ref{parent}
	seen := map[string]bool{parent.ID: true}
	selected := map[string]bool{}
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(len(pending), HierarchyMaxParents)
		batch := pending[:end]
		pending = pending[end:]
		rows, more, err := reader.HierarchyChildren(ctx, batch, HierarchyMaxLimit)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if more {
			return nil, ErrDescendantFilterLimit{Limit: HierarchyMaxLimit}
		}
		for _, row := range rows {
			if row.Kind != "bead" {
				return nil, fmt.Errorf("%w: parent filter source is not a bead", ErrUnsupported)
			}
			if err := ValidateHierarchyRef(row.Ref); err != nil {
				return nil, err
			}
			if seen[row.Ref.ID] {
				return nil, fmt.Errorf("%w: cyclic or duplicate parent-filter source", ErrUnsupported)
			}
			seen[row.Ref.ID] = true
			selected[row.Ref.ID] = true
			if len(selected) > SelectionOrdinaryCap {
				return nil, ErrDescendantFilterLimit{Limit: SelectionOrdinaryCap}
			}
			pending = append(pending, row.Ref)
		}
	}
	return selected, nil
}

// ErrDescendantFilterLimit is a refusal, never a truncated membership/count.
type ErrDescendantFilterLimit struct{ Limit int }

func (e ErrDescendantFilterLimit) Error() string {
	return fmt.Sprintf("parent filter exceeds indexed descendant bound %d", e.Limit)
}
func (e ErrDescendantFilterLimit) driverMapping() (model.ErrorCode, map[string]any) {
	return "filter_too_broad", map[string]any{"filter": "parent", "limit": e.Limit}
}

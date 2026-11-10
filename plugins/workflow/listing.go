package workflow

import (
	"context"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// ListItems reads the caller's current items, including blocked and closed
// ones. Unlike load, this read never collects receipts or writes a checkpoint.
func (p *Provider) ListItems(ctx context.Context, by model.PrincipalRef, project string, meta model.WriteMeta) ([]ItemView, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, _, err := p.readState(ctx)
	if err != nil {
		return nil, err
	}
	if _, _, err := principalScope(by, effectiveAgent(by, meta)); err != nil {
		return nil, fail("invalid_params", "invalid principal")
	}
	ids := make([]string, 0, len(value.Items))
	for id := range value.Items {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	items := make([]ItemView, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		item := value.Items[id]
		if item.Project != project || p.authorized(item, by, meta) != nil {
			continue
		}
		items = append(items, p.itemViewIn(value, item))
	}
	return items, nil
}

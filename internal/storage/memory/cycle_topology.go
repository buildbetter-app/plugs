package memory

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"sort"
)

func (r *reader) CycleTopology(ctx context.Context, cursor *driver.Cursor, limit int) (driver.Page[driver.CycleTopologyEdge], error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	empty := driver.Page[driver.CycleTopologyEdge]{}
	if err := r.check(ctx); err != nil {
		return empty, err
	}
	at := model.Natural64(r.at)
	offset, err := driver.CycleTopologyOffset(cursor, r.incarnation, at, limit)
	if err != nil {
		return empty, err
	}
	root, err := r.state.sourceLinks.rootAt(r.incarnation, 0, r.at)
	if err != nil {
		return empty, err
	}
	type keyed struct {
		key  string
		edge driver.CycleTopologyEdge
	}
	all := []keyed{}
	seen := map[string]bool{}
	for key, source := range root.sources {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		for _, link := range source.links {
			target, err := recordKey(link.target)
			if err != nil {
				return empty, err
			}
			identity := key + "\x00" + link.kind + "\x00" + target
			if !seen[identity] {
				all = append(all, keyed{identity, driver.CycleTopologyEdge{From: source.record.Ref, To: link.target, Kind: link.kind}})
				seen[identity] = true
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].key < all[j].key })
	// Published membership roots already collapse identical kind/target facts.
	page := driver.Page[driver.CycleTopologyEdge]{Items: []driver.CycleTopologyEdge{}, AsOf: at}
	if offset > uint64(len(all)) {
		return empty, driver.ErrUnsupported
	}
	end := offset + uint64(limit)
	if end > uint64(len(all)) {
		end = uint64(len(all))
	}
	for _, value := range all[offset:end] {
		page.Items = append(page.Items, value.edge)
	}
	if end < uint64(len(all)) {
		page.NextCursor = driver.CycleTopologyCursor(r.incarnation, at, end)
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return page, nil
}

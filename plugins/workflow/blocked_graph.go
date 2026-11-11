package workflow

import (
	"context"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// BlockedGraph is one read-only checkpoint observation. Hidden workflow item
// bodies never leave the provider. Their incoming ordinary relations remain
// internal unknown facts so a blocked read can count, rather than disclose,
// an unreadable reason. This method performs no expiry cleanup or publication.
type BlockedGraph struct {
	Items     []ItemView
	Hidden    []BlockedGraphNode
	Relations []RelationView
	Revision  pluginapi.Revision
}

// BlockedGraphNode is an internal evaluation fact. The host never projects a
// hidden node, but must retain its topology and terminal state for child gates.
type BlockedGraphNode struct {
	ID, Project, Status string
	Parent              *model.Ref
}

func (p *Provider) BlockedGraph(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta) (BlockedGraph, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, _, err := principalScope(by, effectiveAgent(by, meta)); err != nil {
		return BlockedGraph{}, fail("invalid_params", "invalid principal")
	}
	value, revision, err := p.readState(ctx)
	if err != nil {
		return BlockedGraph{}, err
	}
	result := BlockedGraph{Revision: revision, Items: []ItemView{}, Relations: []RelationView{}}
	ids := make([]string, 0, len(value.Items))
	for id := range value.Items {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return BlockedGraph{}, err
		}
		item := value.Items[id]
		visible := p.authorized(item, by, meta) == nil
		if visible {
			result.Items = append(result.Items, p.itemViewIn(value, item))
		} else {
			result.Hidden = append(result.Hidden, BlockedGraphNode{ID: item.ID, Project: item.Project, Status: item.Status, Parent: cloneRef(item.Parent)})
		}
		for _, link := range item.Links {
			result.Relations = append(result.Relations, relationView(link))
		}
		// An inbound edge's subject is an ordinary bead. Its own read policy
		// is evaluated by the host on the same pinned core reader.
		for _, link := range item.Inbound {
			result.Relations = append(result.Relations, relationView(link))
		}
	}
	result.Relations = dedupeRelationViews(result.Relations)
	return result, nil
}

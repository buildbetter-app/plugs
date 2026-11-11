package service

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReadCyclesFrom lets the installed-policy adapter keep its content reader and
// authority fences while using the real service's compiled-in workflow port.
// The durable-only entry point remains usable by hosts without that port.
func (s *Service) ReadCyclesFrom(ctx context.Context, by model.PrincipalRef, reader driver.Reader, req CyclesRequest, readable func(context.Context, model.Ref) (bool, error)) (CyclesResult, error) {
	if !req.IncludeEphemeral {
		return ReadCycles(ctx, reader, req, readable)
	}
	limit, err := cycleLimit(req)
	if err != nil {
		return CyclesResult{}, err
	}
	if reader == nil || reader.Snapshot().At == nil {
		return CyclesResult{}, failure("invalid_params", "cycle reporting requires a pinned snapshot")
	}
	provider := s.workflowProvider()
	if provider == nil {
		return CyclesResult{}, ephemeralCycleCapabilityFailure()
	}
	graph, err := readCycleRows(ctx, reader)
	if err != nil {
		return CyclesResult{}, err
	}
	snapshot, err := provider.ReadCycleSnapshot(ctx, by, workflowMeta(by, "bead.cycles", req.Project, req.Agent))
	if err != nil {
		return CyclesResult{}, mapped(err)
	}
	view := reader.Snapshot()
	meta, err := s.store.Meta(ctx)
	if err != nil {
		return CyclesResult{}, mapped(err)
	}
	if snapshot.Head.StoreID != meta.StoreID || snapshot.Head.Incarnation != view.Incarnation || snapshot.Head.Head.String() != view.At.String() || snapshot.Head.Check(meta) != nil {
		return CyclesResult{}, mapped(driver.ErrCheckpointConflict)
	}
	for _, node := range snapshot.Nodes {
		if _, exists := graph[node.ID]; exists {
			return CyclesResult{}, failure("internal_error", "cycle graph has conflicting plane identities")
		}
		graph[node.ID] = cycleRow{project: node.Project, missing: !node.Readable}
	}
	for _, edge := range snapshot.Edges {
		row, exists := graph[edge.From.ID]
		if !exists {
			row.missing = true
		}
		row.links = append(row.links, pipeline.Link{To: edge.To, Kind: edge.Kind})
		graph[edge.From.ID] = row
	}
	return readCycleGraph(ctx, graph, req, limit, AsOf{Revision: *view.At, Incarnation: view.Incarnation}, readable)
}

package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// CycleNode describes membership separately from disclosure. Hidden items and
// their edges stay in the graph; they can never anchor a visible report.
type CycleNode struct {
	ID, Project string
	Readable    bool
}

// CycleSnapshot is a complete, unfiltered graph from one checked host image.
// Head and Namespace are retained even when no workflow items are present.
type CycleSnapshot struct {
	Nodes     []CycleNode
	Edges     []driver.CycleTopologyEdge
	Head      driver.CoreSnapshotExpectation
	Namespace driver.CheckpointExpectation
	Revision  pluginapi.Revision
}

// ReadCycleSnapshot uses the existing joint core/checkpoint read. Unlike Ready,
// this includes blocked, closed and foreign-owner membership, and never samples
// a clock, collects receipts, or publishes a checkpoint.
func (p *Provider) ReadCycleSnapshot(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta) (CycleSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var logical json.RawMessage
	var head driver.CoreSnapshotExpectation
	var namespace driver.CheckpointExpectation
	var err error
	if snapshot, ok := p.state.(interface {
		ReadWorkflowRuntimeCheckpoint(context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error)
	}); ok {
		logical, _, head, namespace, err = snapshot.ReadWorkflowRuntimeCheckpoint(ctx)
	} else if snapshot, ok := p.state.(interface {
		ReadSourceRuntimeCheckpoint(context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error)
	}); ok && p.source != nil {
		logical, _, head, namespace, err = snapshot.ReadSourceRuntimeCheckpoint(ctx)
	} else {
		return CycleSnapshot{}, driver.ErrUnsupported
	}
	if err != nil {
		return CycleSnapshot{}, err
	}
	if head.StoreID == "" || head.Incarnation == "" || namespace.StoreID != head.StoreID || namespace.Incarnation != head.Incarnation || driver.ValidateCheckpointKey(namespace.Key) != nil {
		return CycleSnapshot{}, driver.ErrCheckpointConflict
	}
	var value persisted
	if len(logical) != 0 {
		decoder := json.NewDecoder(bytes.NewReader(logical))
		decoder.UseNumber()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&value); err != nil {
			return CycleSnapshot{}, err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return CycleSnapshot{}, fail("internal_error", "workflow cycle image has trailing data")
		}
		if value.Items == nil || value.Runs == nil || value.Receipts == nil {
			return CycleSnapshot{}, fail("internal_error", "workflow cycle image is incomplete")
		}
	}
	if p.source != nil && (value.Source == nil || value.Source.Schema != SourceStateSchema || namespace.Key != p.source.Provider.Namespace) {
		return CycleSnapshot{}, driver.ErrCheckpointConflict
	}
	result, err := p.cycleGraph(ctx, value, by, meta)
	if err != nil {
		return CycleSnapshot{}, err
	}
	result.Head, result.Namespace = head, namespace
	return result, nil
}

// StagedCycleGraph reads only the caller's detached ordered-operation image.
// It is for graph validation inside the existing preparation participant.
func (p *Provider) StagedCycleGraph(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta) (CycleSnapshot, error) {
	state, ok := p.state.(*stagedState)
	if !ok {
		return CycleSnapshot{}, driver.ErrUnsupported
	}
	value, revision, err := p.readState(ctx)
	if err != nil {
		return CycleSnapshot{}, err
	}
	result, err := p.cycleGraph(ctx, value, by, meta)
	if err == nil && state.head != nil && state.namespace != nil {
		result.Head, result.Namespace, result.Revision = *state.head, *state.namespace, revision
	}
	return result, err
}

func (p *Provider) cycleGraph(ctx context.Context, value persisted, by model.PrincipalRef, meta model.WriteMeta) (CycleSnapshot, error) {
	result := CycleSnapshot{Nodes: []CycleNode{}, Edges: []driver.CycleTopologyEdge{}}
	ids := make([]string, 0, len(value.Items))
	for id := range value.Items {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return CycleSnapshot{}, err
		}
		item := value.Items[id]
		if id != item.ID || validateRef(model.RefByID(id, model.Natural64(0))) != nil || !workflowTarget(model.RefByID(id, model.Natural64(0))) || item.Project == "" {
			return CycleSnapshot{}, fail("internal_error", "invalid workflow cycle membership")
		}
		result.Nodes = append(result.Nodes, CycleNode{ID: id, Project: item.Project, Readable: p.authorized(item, by, meta) == nil})
		appendEdge := func(link relationState, inbound bool) error {
			from := model.RefByID(link.From, model.Natural64(0))
			if validateRef(from) != nil || validateRef(link.To) != nil || link.Kind == "" || (!inbound && link.From != id) || (inbound && link.To.ID != id) {
				return fail("internal_error", "invalid workflow cycle topology")
			}
			result.Edges = append(result.Edges, driver.CycleTopologyEdge{From: from, To: link.To, Kind: link.Kind})
			return nil
		}
		for _, link := range item.Links {
			if err := appendEdge(link, false); err != nil {
				return CycleSnapshot{}, err
			}
		}
		for _, link := range item.Inbound {
			if err := appendEdge(link, true); err != nil {
				return CycleSnapshot{}, err
			}
		}
		if item.Parent != nil {
			if validateRef(*item.Parent) != nil {
				return CycleSnapshot{}, fail("internal_error", "invalid workflow cycle parent")
			}
			result.Edges = append(result.Edges, driver.CycleTopologyEdge{From: model.RefByID(id, model.Natural64(0)), To: *item.Parent, Kind: "parent-child"})
		}
	}
	return result, ctx.Err()
}

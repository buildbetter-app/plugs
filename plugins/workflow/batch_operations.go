package workflow

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

func (p *Provider) checkpointAvailable() bool {
	if p.persistent {
		return true
	}
	marker, ok := p.state.(interface {
		WorkflowCheckpointLifetime() driver.CheckpointLifetime
	})
	return ok && marker.WorkflowCheckpointLifetime().Validate() == nil
}

// CanPrepareOrdinaryOperations identifies the host adapter that can detach a
// real ordinary graph/namespace observation. Persistence alone is insufficient;
// legacy durable-only providers keep their explicitly unsupported batch route.
func (p *Provider) CanPrepareOrdinaryOperations() bool {
	if p == nil || p.source != nil {
		return false
	}
	marker, ok := p.state.(interface {
		WorkflowCheckpointLifetime() driver.CheckpointLifetime
	})
	if !ok || marker.WorkflowCheckpointLifetime().Validate() != nil {
		return false
	}
	_, ok = p.state.(interface {
		ReadWorkflowRuntimeCheckpoint(context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error)
	})
	return ok
}

func (s *stagedState) WorkflowCheckpointLifetime() driver.CheckpointLifetime {
	if marker, ok := s.base.(interface {
		WorkflowCheckpointLifetime() driver.CheckpointLifetime
	}); ok {
		return marker.WorkflowCheckpointLifetime()
	}
	return ""
}

// PrepareOperations stages the same provider used by ordinary public tools.
// It publishes nothing: the existing Writer joins the detached checkpoint to
// core effects and the original public request's durable receipt.
func (p *Provider) PrepareOperations(ctx context.Context, apply func(*Provider) error) (PreparedState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if apply == nil || !p.CanPrepareOrdinaryOperations() {
		return PreparedState{}, driver.ErrUnsupported
	}
	if err := ctx.Err(); err != nil {
		return PreparedState{}, err
	}
	snapshot := p.state.(interface {
		ReadWorkflowRuntimeCheckpoint(context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error)
	})
	logical, revision, head, namespace, err := snapshot.ReadWorkflowRuntimeCheckpoint(ctx)
	if err != nil {
		return PreparedState{}, err
	}
	if namespace.StoreID != head.StoreID || namespace.Incarnation != head.Incarnation ||
		driver.ValidateCheckpointKey(namespace.Key) != nil {
		return PreparedState{}, driver.ErrCheckpointConflict
	}
	staged, state := p.staged()
	state.key, state.value, state.revision, state.loaded, state.head = stateKey, logical, revision, true, &head
	state.namespace = &namespace
	if err := apply(staged); err != nil {
		return PreparedState{}, err
	}
	if err := ctx.Err(); err != nil {
		return PreparedState{}, err
	}
	if !state.written {
		copy := *state.head
		return PreparedState{head: &copy}, nil
	}
	return state.prepared(), nil
}

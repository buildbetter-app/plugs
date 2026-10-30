package events

import (
	"context"
	"errors"
	"fmt"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ProjectionStatusConfig wires the durable sources used by the read-only
// projection status adapter. Checkpoints are the persisted delivery state;
// Log supplies the current incarnation; Lease prevents a deposed composition
// from presenting stale readiness to a view or watch.
type ProjectionStatusConfig struct {
	Checkpoints core.Checkpoints
	Log         core.Log
	Lease       driver.ActiveLease
	Registry    *core.ProjectionRegistry
}

// ProjectionStatusAdapter reads projection status from the same durable
// checkpoint source used by Dispatcher. It has no process-local status map and
// cannot acknowledge or advance a projection.
type ProjectionStatusAdapter struct {
	checkpoints core.Checkpoints
	log         core.Log
	lease       driver.ActiveLease
	registry    *core.ProjectionRegistry
}

// NewProjectionStatusReader creates the checkpoint-backed implementation of
// core.ProjectionStatusReader. The registry is optional for embedders that
// already validate binding ownership; when supplied it makes an unknown or
// non-projection binding fail closed before reading a checkpoint.
func NewProjectionStatusReader(config ProjectionStatusConfig) (*ProjectionStatusAdapter, error) {
	if config.Checkpoints == nil {
		return nil, errors.New("projection status requires durable checkpoints")
	}
	if config.Log == nil {
		return nil, errors.New("projection status requires an event log")
	}
	if config.Lease == nil {
		return nil, errors.New("projection status requires an active lease")
	}
	return &ProjectionStatusAdapter{
		checkpoints: config.Checkpoints,
		log:         config.Log,
		lease:       config.Lease,
		registry:    config.Registry,
	}, nil
}

// NewProjectionStatusAdapter is the positional constructor for hosts that
// already have the three durable delivery sources in hand.
func NewProjectionStatusAdapter(checkpoints core.Checkpoints, log core.Log, lease driver.ActiveLease, registry *core.ProjectionRegistry) (*ProjectionStatusAdapter, error) {
	return NewProjectionStatusReader(ProjectionStatusConfig{
		Checkpoints: checkpoints,
		Log:         log,
		Lease:       lease,
		Registry:    registry,
	})
}

var _ core.ProjectionStatusReader = (*ProjectionStatusAdapter)(nil)

// Status returns a detached, deterministic view of the persisted projection
// checkpoint. A missing checkpoint is represented by the current incarnation
// at revision/index zero. Persisted checkpoints must belong to the current
// incarnation; silently resetting them would make a view report readiness
// while the dispatcher still has work from another history.
func (a *ProjectionStatusAdapter) Status(ctx context.Context, binding string) (core.ProjectionStatus, error) {
	if a == nil || a.checkpoints == nil || a.log == nil || a.lease == nil {
		return core.ProjectionStatus{}, errors.New("projection status adapter is not initialized")
	}
	if err := driver.ValidateCheckpointKey(binding); err != nil {
		return core.ProjectionStatus{}, err
	}
	if a.registry != nil {
		registered, ok := a.registry.Get(binding)
		if !ok {
			return core.ProjectionStatus{}, fmt.Errorf("projection %q is not registered: %w", binding, core.ErrProjectionNotFound)
		}
		if registered.Class != core.Projection {
			return core.ProjectionStatus{}, fmt.Errorf("projection %q has class %q", binding, registered.Class)
		}
	}
	select {
	case <-a.lease.Lost():
		return core.ProjectionStatus{}, driver.ErrFenced{ExpectedEpoch: a.lease.Epoch()}
	default:
	}

	head, err := a.log.Head(ctx)
	if err != nil {
		return core.ProjectionStatus{}, err
	}
	if head.Incarnation == "" {
		return core.ProjectionStatus{}, errors.New("projection status event log has no incarnation")
	}
	if err := head.Revision.Validate(); err != nil {
		return core.ProjectionStatus{}, fmt.Errorf("projection status event log has an invalid head revision: %w", err)
	}
	state, generation, err := a.checkpoints.Load(ctx, binding)
	if err != nil {
		return core.ProjectionStatus{}, err
	}
	// A load can race lease loss. The checkpoint source remains the authority
	// for fencing writes, while this read refuses to turn the old holder's
	// snapshot into a successful readiness result.
	select {
	case <-a.lease.Lost():
		return core.ProjectionStatus{}, driver.ErrFenced{ExpectedEpoch: a.lease.Epoch()}
	default:
	}
	if generation == 0 {
		return core.ProjectionStatus{
			Binding:  binding,
			Position: core.Position{Incarnation: head.Incarnation},
		}, nil
	}
	if state.Class != core.Projection {
		return core.ProjectionStatus{}, fmt.Errorf("checkpoint %q belongs to %q delivery, not a projection", binding, state.Class)
	}
	if err := state.Position.Validate(); err != nil {
		return core.ProjectionStatus{}, fmt.Errorf("invalid projection checkpoint: %w", err)
	}
	if state.Position.Incarnation != head.Incarnation {
		return core.ProjectionStatus{}, &driver.ErrIncarnationChanged{From: state.Position.Incarnation, To: head.Incarnation}
	}
	if cmp, err := core.CompareNatural(state.Position.Revision, head.Revision); err != nil {
		return core.ProjectionStatus{}, fmt.Errorf("invalid projection checkpoint revision: %w", err)
	} else if cmp > 0 {
		return core.ProjectionStatus{}, futureCheckpointError(head, "checkpoint revision", state.Position.Revision)
	}
	if err := state.LagAcceptedThrough.Validate(); err != nil {
		return core.ProjectionStatus{}, fmt.Errorf("invalid projection lag watermark: %w", err)
	}
	if cmp, err := core.CompareNatural(state.LagAcceptedThrough, head.Revision); err != nil {
		return core.ProjectionStatus{}, fmt.Errorf("invalid projection lag watermark: %w", err)
	} else if cmp > 0 {
		return core.ProjectionStatus{}, futureCheckpointError(head, "lag-accepted-through watermark", state.LagAcceptedThrough)
	}
	status := core.ProjectionStatus{
		Binding:            binding,
		Position:           state.Position,
		Failure:            cloneFailure(state.Failure),
		Halted:             state.Halted,
		Stalled:            state.Stalled,
		LagAcceptedThrough: state.LagAcceptedThrough,
	}
	return core.NormalizeProjectionStatus(status, binding), nil
}

// futureCheckpointError keeps an impossible durable position visible as an
// explicit event boundary. Callers must not clamp it to the head or treat it
// as a healthy checkpoint, because either choice would fabricate readiness.
func futureCheckpointError(head core.Head, field string, value model.Revision) error {
	return fmt.Errorf("projection %s %s is beyond event log head %s: %w", field, value.String(), head.Revision.String(), &core.BoundaryError{
		Code:   "cursor_invalid",
		From:   head.Incarnation,
		Head:   head,
		Reason: "checkpoint",
	})
}

func cloneFailure(failure *core.Failure) *core.Failure {
	if failure == nil {
		return nil
	}
	copy := *failure
	return &copy
}

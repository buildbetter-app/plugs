package pluginhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// NewWorkflowRuntimeState uses the selected driver's actual checkpoint lifetime.
// Process checkpoints support ordinary memory workflows without advertising
// persistence across a process restart. Legacy durable-only clients still refuse.
func NewWorkflowRuntimeState(store driver.WorkflowCheckpointStore, lease driver.ActiveLease) (pluginapi.DurableState, error) {
	if store == nil || lease == nil || store.WorkflowCheckpointLifetime().Validate() != nil {
		return nil, driver.ErrUnsupported
	}
	d := newDurableAccess(store, lease, "workflow", "default")
	d.workflow = store
	return d, nil
}

func (d *durableAccess) WorkflowCheckpointLifetime() driver.CheckpointLifetime {
	if d == nil || d.workflow == nil {
		return ""
	}
	return d.workflow.WorkflowCheckpointLifetime()
}

// ReadWorkflowRuntimeCheckpoint detaches active state and both publication
// expectations from one real graph/namespace snapshot. Source origin validation
// remains owned by the admitted-source adapter.
func (d *durableAccess) ReadWorkflowRuntimeCheckpoint(ctx context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error) {
	if d.source != nil {
		return d.ReadSourceRuntimeCheckpoint(ctx)
	}
	var head driver.CoreSnapshotExpectation
	var expectation driver.CheckpointExpectation
	if err := d.available(ctx); err != nil {
		return nil, "", head, expectation, err
	}
	store := d.workflow
	if store == nil {
		return nil, "", head, expectation, driver.ErrUnsupported
	}
	metadata, ok := d.store.(interface {
		Meta(context.Context) (driver.Meta, error)
	})
	if !ok {
		return nil, "", head, expectation, driver.ErrUnsupported
	}
	meta, err := metadata.Meta(ctx)
	if err != nil {
		return nil, "", head, expectation, err
	}
	r, err := store.ReadSourcePlaneSnapshot(ctx, driver.View{Branch: model.Natural64(0), Incarnation: meta.Incarnation}, []string{d.key})
	if err != nil {
		return nil, "", head, expectation, err
	}
	defer r.Close()
	view := r.Snapshot()
	if view.At == nil || view.Incarnation != meta.Incarnation || !view.Branch.IsZero() {
		return nil, "", head, expectation, driver.ErrCheckpointConflict
	}
	head = driver.CoreSnapshotExpectation{StoreID: meta.StoreID, Incarnation: view.Incarnation, Head: *view.At}
	cp, err := r.SourcePlaneCheckpoint(d.key)
	if err != nil {
		return nil, "", head, expectation, err
	}
	namespace := durableNamespace{Values: map[string]durableValue{}}
	if cp.Generation != 0 {
		decoder := json.NewDecoder(bytes.NewReader(cp.Payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&namespace); err != nil {
			return nil, "", head, expectation, err
		}
		if decoder.Decode(new(any)) != io.EOF || namespace.Origin != nil {
			return nil, "", head, expectation, driver.ErrUnsupported
		}
		for key, value := range namespace.Values {
			if driver.ValidateCheckpointKey(key) != nil || value.Revision == 0 && len(value.Value) != 0 {
				return nil, "", head, expectation, fmt.Errorf("invalid workflow namespace value")
			}
		}
	}
	value := namespace.Values[sourceStateKey]
	expectation = driver.CheckpointExpectation{StoreID: head.StoreID, Incarnation: head.Incarnation, Key: d.key, Expected: cp.Generation}
	return append(json.RawMessage(nil), value.Value...), pluginapi.Natural64(value.Revision), head, expectation, nil
}

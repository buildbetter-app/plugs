package driver

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// CoreSnapshotExpectation binds a checkpoint-only publication to one complete
// core graph snapshot, including predicate reads such as waits-for children.
// The head check never advances the core graph or creates a durable receipt.
type CoreSnapshotExpectation struct {
	StoreID     StoreID
	Incarnation string
	Head        model.Revision
}

func (e CoreSnapshotExpectation) Check(meta Meta) error {
	if err := e.Head.Validate(); err != nil {
		return err
	}
	if e.StoreID == "" || e.Incarnation == "" || e.StoreID != meta.StoreID || e.Incarnation != meta.Incarnation || e.Head.String() != meta.Head.String() {
		return ErrCheckpointConflict
	}
	return nil
}

// HeadGuardedCheckpointStore atomically checks the observed core snapshot,
// namespace generation and live lease before publishing unversioned state.
// A changed graph or namespace returns ErrCheckpointConflict, publishing
// nothing. The CheckpointStore uncertain-publication contract also applies.
type HeadGuardedCheckpointStore interface {
	SaveCheckpointAtHead(context.Context, ActiveLease, string, uint64, []byte, CoreSnapshotExpectation) (Checkpoint, error)
}

// CheckpointExpectation binds a host-owned read to the exact store namespace.
// It fences a graph write without publishing or advancing the checkpoint.
type CheckpointExpectation struct {
	StoreID     StoreID
	Incarnation string
	Key         string
	Expected    uint64
}

// CheckpointMutation is prepared by a trusted host namespace adapter. It is
// data only: plugins never receive the transaction that publishes it.
type CheckpointMutation struct {
	StoreID     StoreID
	Incarnation string
	Key         string
	Expected    uint64
	Payload     []byte
}

// CheckpointWriteTx publishes an unversioned checkpoint CAS in the same
// transaction as the ordinary changeset. Any failure rolls back both planes.
// StageCheckpoint copies the input and rejects conflicting duplicate stages.
type CheckpointWriteTx interface {
	StageCheckpoint(context.Context, string, uint64, []byte) error
}

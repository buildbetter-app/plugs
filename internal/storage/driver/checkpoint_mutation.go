package driver

import "context"

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

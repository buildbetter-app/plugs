package driver

import "context"

// ReadyAuthoritySnapshot is a borrowed committed view of the transaction
// AFTER Head. It owns no connection and cannot close or mutate the writer.
// Implementations exclude all staged overlays from both source and fact reads.
type ReadyAuthoritySnapshot interface {
	Snapshot() View
	Get(context.Context, []Ref) ([]Record, error)
	Observe(context.Context, []LockKey) ([]LockState, error)
}

type FreshReadyAuthorityReader interface {
	FreshReadyAuthority(context.Context) (ReadyAuthoritySnapshot, error)
}

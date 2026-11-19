package driver

import (
	"bytes"
	"context"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// RequireWorkflowCheckpointStore discovers a real store capability, never a
// caller-supplied persistence flag. Process stores must explicitly implement
// the workflow lifetime marker. Existing restart-persistent stores can use
// their head/namespace guards to establish a joint snapshot without changing
// their ordinary transaction or checkpoint publication implementations.
func RequireWorkflowCheckpointStore(store Store, lease ActiveLease) (WorkflowCheckpointStore, error) {
	if lease == nil {
		return nil, ErrUnsupported
	}
	cp, ok := store.(CheckpointStore)
	if !ok {
		return nil, ErrUnsupported
	}
	head, ok := store.(HeadGuardedCheckpointStore)
	if !ok {
		return nil, ErrUnsupported
	}
	lifetime := CheckpointLifetime("")
	if marker, ok := store.(interface{ WorkflowCheckpointLifetime() CheckpointLifetime }); ok {
		lifetime = marker.WorkflowCheckpointLifetime()
	}
	persistent := false
	if marker, ok := store.(PersistentCheckpointStore); ok {
		persistent = marker.Persistent()
	}
	if lifetime == "" && persistent {
		lifetime = CheckpointLifetimeRestart
	}
	if lifetime.Validate() != nil || (lifetime == CheckpointLifetimeRestart && !persistent) {
		return nil, ErrUnsupported
	}
	if direct, ok := store.(WorkflowCheckpointStore); ok {
		return direct, nil
	}
	return &workflowCheckpointAdapter{Store: store, CheckpointStore: cp, HeadGuardedCheckpointStore: head, lease: lease, lifetime: lifetime, persistent: persistent}, nil
}

type workflowCheckpointAdapter struct {
	Store
	CheckpointStore
	HeadGuardedCheckpointStore
	lease      ActiveLease
	lifetime   CheckpointLifetime
	persistent bool
}

func (s *workflowCheckpointAdapter) WorkflowCheckpointLifetime() CheckpointLifetime {
	return s.lifetime
}
func (s *workflowCheckpointAdapter) Persistent() bool { return s.persistent }

// The copied core/checkpoint observations are certified together by one
// retry-free, lease-fenced Head/namespace-guard transaction. This is an atomic
// expectation over the COMPLETE snapshot, not two unrelated reads. The guard
// transaction has no key, receipt, mutation, checkpoint stage or head advance.
func (s *workflowCheckpointAdapter) ReadSourcePlaneSnapshot(ctx context.Context, view View, keys []string) (SourcePlaneSnapshot, error) {
	if len(keys) > SourcePlaneMaxListEntries {
		return nil, ErrUnsupported
	}
	for i, key := range keys {
		if ValidateCheckpointKey(key) != nil || (i > 0 && keys[i-1] >= key) {
			return nil, ErrCheckpointConflict
		}
	}
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = r.Close()
		}
	}()
	snapshot := r.Snapshot()
	if snapshot.At == nil {
		return nil, ErrUnsupported
	}
	checkpoints := make(map[string]Checkpoint, len(keys))
	for _, key := range keys {
		cp, err := s.LoadCheckpoint(ctx, key)
		if err != nil {
			return nil, err
		}
		cp.Payload = bytes.Clone(cp.Payload)
		checkpoints[key] = cp
	}
	tx, err := s.Store.Write(ctx, s.lease)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	guard, ok := tx.(CheckpointGuardTx)
	if !ok {
		return nil, ErrUnsupported
	}
	noop, ok := tx.(NoopCommitter)
	if !ok {
		return nil, ErrUnsupported
	}
	if _, err := tx.Lock(ctx, nil); err != nil {
		return nil, err
	}
	head, err := tx.Head(ctx)
	if err != nil {
		return nil, err
	}
	meta, err := s.Store.Meta(ctx)
	if err != nil {
		return nil, err
	}
	if meta.Incarnation != snapshot.Incarnation || head.String() != snapshot.At.String() || meta.Head.String() != head.String() {
		return nil, ErrCheckpointConflict
	}
	for _, key := range keys {
		if err := guard.ExpectCheckpoint(ctx, key, checkpoints[key].Generation); err != nil {
			return nil, err
		}
	}
	if _, err := noop.CommitNoop(ctx, ChangesetHeader{Kind: model.ChangesetWrite, Incarnation: meta.Incarnation, Principal: model.PrincipalRef{Kind: "system", ID: "source-plane-snapshot"}}); err != nil {
		return nil, err
	}
	keep = true
	checked := &checkedSourcePlaneSnapshot{Reader: r, checkpoints: checkpoints}
	// Optional reader capabilities belong to the pinned observation. Preserve
	// them only when the underlying reader actually provides them; an adapter
	// must never advertise a capability that it cannot satisfy.
	allocators, hasAllocators := r.(AllocatorReader)
	parents, hasParents := r.(HierarchyChildrenReader)
	links, hasLinks := r.(InverseSourceLinksReader)
	switch {
	case hasAllocators && hasParents && hasLinks:
		return &inverseHierarchyAllocatorSourcePlaneSnapshot{hierarchyAllocatorSourcePlaneSnapshot: &hierarchyAllocatorSourcePlaneSnapshot{allocatorSourcePlaneSnapshot: &allocatorSourcePlaneSnapshot{SourcePlaneSnapshot: checked, AllocatorReader: allocators}, HierarchyChildrenReader: parents}, InverseSourceLinksReader: links}, nil
	case hasAllocators && hasParents:
		return &hierarchyAllocatorSourcePlaneSnapshot{allocatorSourcePlaneSnapshot: &allocatorSourcePlaneSnapshot{SourcePlaneSnapshot: checked, AllocatorReader: allocators}, HierarchyChildrenReader: parents}, nil
	case hasAllocators && hasLinks:
		return &inverseAllocatorSourcePlaneSnapshot{allocatorSourcePlaneSnapshot: &allocatorSourcePlaneSnapshot{SourcePlaneSnapshot: checked, AllocatorReader: allocators}, InverseSourceLinksReader: links}, nil
	case hasParents && hasLinks:
		return &inverseHierarchySourcePlaneSnapshot{hierarchySourcePlaneSnapshot: &hierarchySourcePlaneSnapshot{SourcePlaneSnapshot: checked, HierarchyChildrenReader: parents}, InverseSourceLinksReader: links}, nil
	case hasAllocators:
		return &allocatorSourcePlaneSnapshot{SourcePlaneSnapshot: checked, AllocatorReader: allocators}, nil
	case hasParents:
		return &hierarchySourcePlaneSnapshot{SourcePlaneSnapshot: checked, HierarchyChildrenReader: parents}, nil
	case hasLinks:
		return &inverseLinksSourcePlaneSnapshot{SourcePlaneSnapshot: checked, InverseSourceLinksReader: links}, nil
	default:
		return checked, nil
	}
}

type checkedSourcePlaneSnapshot struct {
	Reader
	mu          sync.RWMutex
	checkpoints map[string]Checkpoint
	closed      bool
}

type allocatorSourcePlaneSnapshot struct {
	SourcePlaneSnapshot
	AllocatorReader
}

type hierarchySourcePlaneSnapshot struct {
	SourcePlaneSnapshot
	HierarchyChildrenReader
}

type inverseLinksSourcePlaneSnapshot struct {
	SourcePlaneSnapshot
	InverseSourceLinksReader
}

type hierarchyAllocatorSourcePlaneSnapshot struct {
	*allocatorSourcePlaneSnapshot
	HierarchyChildrenReader
}

type inverseAllocatorSourcePlaneSnapshot struct {
	*allocatorSourcePlaneSnapshot
	InverseSourceLinksReader
}

type inverseHierarchySourcePlaneSnapshot struct {
	*hierarchySourcePlaneSnapshot
	InverseSourceLinksReader
}

type inverseHierarchyAllocatorSourcePlaneSnapshot struct {
	*hierarchyAllocatorSourcePlaneSnapshot
	InverseSourceLinksReader
}

func (r *checkedSourcePlaneSnapshot) SourcePlaneCheckpoint(key string) (Checkpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return Checkpoint{}, ErrTxClosed
	}
	cp, ok := r.checkpoints[key]
	if !ok {
		return Checkpoint{}, ErrUnsupported
	}
	cp.Payload = bytes.Clone(cp.Payload)
	return cp, nil
}
func (r *checkedSourcePlaneSnapshot) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.checkpoints = nil
	return r.Reader.Close()
}

var _ WorkflowCheckpointStore = (*workflowCheckpointAdapter)(nil)

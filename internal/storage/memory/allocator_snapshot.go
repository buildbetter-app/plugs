package memory

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Each node is immutable and shares its predecessor. Reads never create a
// baseline; the first actual publication records its real pre-write image.
type allocatorImage struct {
	previous *allocatorImage
	snapshot driver.AllocatorSnapshot
}

func (s *state) currentAllocatorInventory() (driver.AllocatorInventory, error) {
	inventory := driver.AllocatorInventory{}
	for _, a := range s.allocators {
		inventory.Projects = append(inventory.Projects, a.Clone())
	}
	for key, p := range s.parentCounters {
		expected, err := driver.ParentCounterKey(s.meta.Incarnation, p.Parent)
		if err != nil || key != expected {
			return inventory, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorInventoryInvalid, fmt.Errorf("parent row key mismatch"))
		}
		inventory.Parents = append(inventory.Parents, p)
	}
	return inventory, nil
}

func (s *state) allocatorSnapshot(at uint64) (driver.AllocatorSnapshot, error) {
	for image := s.allocatorHistory; image != nil; image = image.previous {
		if image.snapshot.At.String() == model.Natural64(at).String() {
			if image.snapshot.StoreID != s.meta.StoreID || image.snapshot.Incarnation != s.meta.Incarnation {
				return driver.AllocatorSnapshot{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorSourceMismatch, nil)
			}
			return image.snapshot, nil
		}
	}
	if s.allocatorHistory == nil && s.meta.Head.String() == model.Natural64(at).String() {
		inventory, err := s.currentAllocatorInventory()
		if err != nil {
			return driver.AllocatorSnapshot{}, err
		}
		return driver.NewAllocatorSnapshot(s.meta, s.meta.Head, s.meta.Head, inventory)
	}
	return driver.AllocatorSnapshot{}, driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorCoverageUnavailable, nil)
}

func (s *state) publishAllocatorImage(previous *state, reset bool) error {
	var parent *allocatorImage
	floor := s.meta.Head
	if !reset {
		parent = previous.allocatorHistory
		if parent == nil {
			inventory, err := previous.currentAllocatorInventory()
			if err != nil {
				return err
			}
			baseline, err := driver.NewAllocatorSnapshot(previous.meta, previous.meta.Head, previous.meta.Head, inventory)
			if err != nil {
				return err
			}
			parent = &allocatorImage{snapshot: baseline}
		}
		floor = parent.snapshot.CoverageFloor
	}
	inventory, err := s.currentAllocatorInventory()
	if err != nil {
		return err
	}
	snapshot, err := driver.NewAllocatorSnapshot(s.meta, s.meta.Head, floor, inventory)
	if err != nil {
		return err
	}
	s.allocatorHistory = &allocatorImage{previous: parent, snapshot: snapshot}
	return nil
}

func (r *reader) AllocatorSnapshot(ctx context.Context) (driver.AllocatorSnapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return driver.AllocatorSnapshot{}, err
	}
	snapshot, err := r.state.allocatorSnapshot(r.at)
	if err != nil {
		return driver.AllocatorSnapshot{}, err
	}
	return snapshot.Detached()
}

func (t *transaction) nativeAllocatorLocks(binding driver.NativeAllocatorBinding) error {
	fence, err := driver.NativeAllocatorRestoreLock()
	if err != nil {
		return err
	}
	if err := t.writable(fence); err != nil {
		return driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorBindingRequired, err)
	}
	for _, prefix := range binding.Projects {
		key, err := driver.AllocatorLock(prefix)
		if err != nil {
			return err
		}
		if err := t.writable(key); err != nil {
			return driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorBindingRequired, err)
		}
	}
	return nil
}

func (t *transaction) RestoreNativeAllocators(ctx context.Context, snapshot driver.AllocatorSnapshot) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.nativeRestore == nil || t.restore == nil || !t.hasHead || t.nativeAllocators || len(t.records)+len(t.derived)+len(t.bumps)+len(t.blobs)+len(t.allocators)+len(t.parentCounters) != 0 {
		return driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorBindingRequired, nil)
	}
	if err := t.nativeRestore.AllocatorBinding.Match(snapshot); err != nil {
		return err
	}
	if err := t.nativeAllocatorLocks(t.nativeRestore.AllocatorBinding); err != nil {
		return err
	}
	if err := t.store.restoreEmpty(); err != nil {
		return err
	}
	copy, err := snapshot.Detached()
	if err != nil {
		return err
	}
	parents := map[string]driver.ParentCounter{}
	bumps := map[string]driver.LockKey{}
	fence, _ := driver.NativeAllocatorRestoreLock()
	bumps[keyID(fence)] = fence
	for _, a := range copy.Inventory.Projects {
		key, _ := driver.AllocatorLock(a.Prefix)
		bumps[keyID(key)] = key
	}
	for _, p := range copy.Inventory.Parents {
		p.Incarnation = t.restore.Incarnation
		key, err := driver.ParentCounterKey(p.Incarnation, p.Parent)
		if err != nil {
			return err
		}
		parents[key] = p
		lock, err := driver.ParentCounterLock(p.Incarnation, p.Parent)
		if err != nil {
			return err
		}
		bumps[keyID(lock)] = lock
	}
	for _, a := range copy.Inventory.Projects {
		t.allocators[a.Prefix] = a
	}
	t.parentCounters = parents
	for id, key := range bumps {
		t.bumps[id] = key
	}
	t.nativeAllocators = true
	return nil
}

var _ driver.AllocatorSnapshotReader = (*reader)(nil)
var _ driver.NativeAllocatorRestoreTx = (*transaction)(nil)

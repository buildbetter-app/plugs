package memory

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func allocatorKey(prefix string) (driver.LockKey, string, error) {
	key, err := driver.AllocatorLock(prefix)
	if err != nil {
		return driver.LockKey{}, "", err
	}
	id, err := driver.EncodeLockKey(key)
	if err != nil {
		return driver.LockKey{}, "", err
	}
	return key, id, nil
}

func (r *reader) LoadAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return driver.OfflineAllocator{}, err
	}
	if _, err := driver.AllocatorFact(prefix); err != nil {
		return driver.OfflineAllocator{}, err
	}
	snapshot, err := r.state.allocatorSnapshot(r.at)
	if err != nil {
		return driver.OfflineAllocator{}, err
	}
	var value driver.OfflineAllocator
	ok := false
	for _, candidate := range snapshot.Inventory.Projects {
		if candidate.Prefix == prefix {
			value, ok = candidate, true
			break
		}
	}
	if !ok {
		return driver.OfflineAllocator{}, driver.ErrNotFound
	}
	return value.Clone(), nil
}

func (t *transaction) LoadAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.OfflineAllocator{}, err
	}
	if _, err := driver.AllocatorFact(prefix); err != nil {
		return driver.OfflineAllocator{}, err
	}
	if value, ok := t.allocators[prefix]; ok {
		return value.Clone(), nil
	}
	value, ok := t.base.allocators[prefix]
	if !ok {
		return driver.OfflineAllocator{}, driver.ErrNotFound
	}
	return value.Clone(), nil
}

func (t *transaction) allocatorWritable(prefix string) (driver.LockKey, string, error) {
	if t.nativeRestore != nil {
		return driver.LockKey{}, "", driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorBindingRequired, nil)
	}
	key, id, err := allocatorKey(prefix)
	if err != nil {
		return driver.LockKey{}, "", err
	}
	if err := t.writable(key); err != nil {
		return driver.LockKey{}, "", err
	}
	return key, id, nil
}

func (t *transaction) stageAllocator(prefix string, value driver.OfflineAllocator) error {
	key, id, err := t.allocatorWritable(prefix)
	if err != nil {
		return err
	}
	t.allocators[prefix] = value.Clone()
	// The fact revision is the optimistic token used by ordinary writers.
	t.bumps[id] = key
	return nil
}

func (t *transaction) RestoreAllocators(ctx context.Context, values []driver.OfflineAllocator) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if err := driver.ValidateAllocatorSet(values); err != nil {
		return err
	}
	// Validate every existing prefix and every required update lock before
	// changing the transaction. A duplicate/conflicting input is no-write.
	for _, value := range values {
		if _, _, err := t.allocatorWritable(value.Prefix); err != nil {
			return err
		}
		if _, exists := t.base.allocators[value.Prefix]; exists {
			return fmt.Errorf("%w: allocator prefix %q already exists", driver.ErrConflict, value.Prefix)
		}
		if _, exists := t.allocators[value.Prefix]; exists {
			return fmt.Errorf("%w: allocator prefix %q was already staged", driver.ErrConflict, value.Prefix)
		}
	}
	for _, value := range values {
		if err := t.stageAllocator(value.Prefix, value); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) AllocateTopLevel(ctx context.Context, prefix, expected string) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	value, ok := t.allocators[prefix]
	if !ok {
		value, ok = t.base.allocators[prefix]
	}
	if !ok {
		return driver.ErrNotFound
	}
	if value.Mode == driver.AllocatorRandom {
		return t.consumeRandom(ctx, value, "", expected)
	}
	if value.Mode == driver.AllocatorHash {
		return fmt.Errorf("%w: hash allocation requires the content tuple", driver.ErrUnsupported)
	}
	if err := value.ValidateCandidate("", expected); err != nil {
		return err
	}
	if value.LastID == ^uint64(0) {
		return fmt.Errorf("%w: allocator counter exhausted", driver.ErrUnsupported)
	}
	value.LastID++
	return t.stageAllocator(prefix, value)
}

func (t *transaction) AllocateChild(ctx context.Context, parent, expected string) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	prefix := allocatorPrefix(t.allocators, t.base.allocators, parent)
	value, ok := t.allocators[prefix]
	if !ok {
		value, ok = t.base.allocators[prefix]
	}
	if !ok {
		return driver.ErrNotFound
	}
	if value.Mode == driver.AllocatorRandom {
		return t.consumeRandom(ctx, value, parent, expected)
	}
	child, err := value.ValidateChildCandidate(parent, expected)
	if err != nil {
		return err
	}
	current := value.Children[parent]
	if child <= current {
		return fmt.Errorf("%w: allocator child counter already consumed %s", driver.ErrConflict, expected)
	}
	ref := model.Ref{ID: expected}
	if _, _, err := t.allocatorWritable(value.Prefix); err != nil {
		return err
	}
	if err := t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}); err != nil {
		return err
	}
	id, err := recordKey(ref)
	if err != nil {
		return err
	}
	if _, staged := t.records[id]; staged || t.allocated[id] {
		return fmt.Errorf("%w: allocator candidate already staged or reserved", driver.ErrConflict)
	}
	if _, exists := t.base.kinds[id]; exists {
		return fmt.Errorf("%w: allocator candidate already exists", driver.ErrConflict)
	}
	value = value.Clone()
	if value.Children == nil {
		value.Children = make(map[string]uint64)
	}
	value.Children[parent] = child
	if err := t.stageAllocator(prefix, value); err != nil {
		return err
	}
	if t.allocated == nil {
		t.allocated = make(map[string]bool)
	}
	t.allocated[id] = true
	return nil
}

func (t *transaction) AllocateHashTopLevel(ctx context.Context, prefix string, input driver.HashInput, expected string) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	value, ok := t.allocators[prefix]
	if !ok {
		value, ok = t.base.allocators[prefix]
	}
	if !ok {
		return driver.ErrNotFound
	}
	if value.Mode != driver.AllocatorHash {
		return fmt.Errorf("%w: allocator is not hash mode", driver.ErrUnsupported)
	}
	if err := value.ValidateHashCandidate(input, expected); err != nil {
		return err
	}
	ref := model.Ref{ID: expected}
	if _, _, err := t.allocatorWritable(value.Prefix); err != nil {
		return err
	}
	if err := t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}); err != nil {
		return err
	}
	id, err := recordKey(ref)
	if err != nil {
		return err
	}
	if _, staged := t.records[id]; staged || t.allocated[id] {
		return fmt.Errorf("%w: allocator candidate already staged or reserved", driver.ErrConflict)
	}
	if _, exists := t.base.kinds[id]; exists {
		return fmt.Errorf("%w: allocator candidate already exists", driver.ErrConflict)
	}
	// Hash allocators have no monotonic counter, but advancing the allocator
	// fact still serializes candidate consumption and records the exact
	// configured policy in the same commit.
	if err := t.stageAllocator(prefix, value); err != nil {
		return err
	}
	if t.allocated == nil {
		t.allocated = make(map[string]bool)
	}
	t.allocated[id] = true
	return nil
}

var _ driver.AllocatorReader = (*reader)(nil)
var _ driver.AllocatorContinuation = (*transaction)(nil)
var _ driver.HashAllocatorContinuation = (*transaction)(nil)

func allocatorPrefix(staged, base map[string]driver.OfflineAllocator, parent string) string {
	prefix := ""
	for candidate := range staged {
		if strings.HasPrefix(parent, candidate+"-") && len(candidate) > len(prefix) {
			prefix = candidate
		}
	}
	for candidate := range base {
		if strings.HasPrefix(parent, candidate+"-") && len(candidate) > len(prefix) {
			prefix = candidate
		}
	}
	if prefix == "" {
		prefix = parent
	}
	return prefix
}

// consumeRandom validates an already-previewed ID under complete locks. It
// reserves IDs even before Put, and never advances source counters or remints.
func (t *transaction) consumeRandom(ctx context.Context, value driver.OfflineAllocator, parent, expected string) error {
	if err := value.ValidateCandidate(parent, expected); err != nil {
		return err
	}
	if _, _, err := t.allocatorWritable(value.Prefix); err != nil {
		return err
	}
	ref := model.Ref{ID: expected}
	if err := t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}); err != nil {
		return err
	}
	id, err := recordKey(ref)
	if err != nil {
		return err
	}
	if _, staged := t.records[id]; staged || t.allocated[id] {
		return fmt.Errorf("%w: allocator candidate already staged or reserved", driver.ErrConflict)
	}
	if _, exists := t.base.kinds[id]; exists {
		return fmt.Errorf("%w: allocator candidate already exists", driver.ErrConflict)
	}
	if err := t.stageAllocator(value.Prefix, value); err != nil {
		return err
	}
	if t.allocated == nil {
		t.allocated = make(map[string]bool)
	}
	t.allocated[id] = true
	return nil
}

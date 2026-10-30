package memory

import (
	"context"
	"fmt"
	"strings"

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
	value, ok := r.state.allocators[prefix]
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
	next, err := value.NextID()
	if err != nil {
		return err
	}
	if expected != next {
		return fmt.Errorf("%w: allocator next ID is %s, expected %s", driver.ErrConflict, next, expected)
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
	next, err := value.NextChildID(parent)
	if err != nil {
		return err
	}
	if expected != next {
		return fmt.Errorf("%w: allocator next child ID is %s, expected %s", driver.ErrConflict, next, expected)
	}
	current := value.Children[parent]
	if current == ^uint64(0) {
		return fmt.Errorf("%w: child counter exhausted", driver.ErrUnsupported)
	}
	if value.Children == nil {
		value.Children = make(map[string]uint64)
	}
	value.Children[parent] = current + 1
	return t.stageAllocator(prefix, value)
}

var _ driver.AllocatorReader = (*reader)(nil)
var _ driver.AllocatorContinuation = (*transaction)(nil)

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

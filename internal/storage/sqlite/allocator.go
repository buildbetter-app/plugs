package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

func loadAllocator(ctx context.Context, q queryer, prefix string) (driver.OfflineAllocator, error) {
	if _, err := driver.AllocatorFact(prefix); err != nil {
		return driver.OfflineAllocator{}, err
	}
	var data []byte
	err := q.QueryRowContext(ctx, `SELECT data FROM allocator_states WHERE prefix=?`, prefix).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.OfflineAllocator{}, driver.ErrNotFound
	}
	if err != nil {
		return driver.OfflineAllocator{}, err
	}
	var value driver.OfflineAllocator
	if err := json.Unmarshal(data, &value); err != nil {
		return driver.OfflineAllocator{}, err
	}
	if value.Prefix != prefix {
		return driver.OfflineAllocator{}, fmt.Errorf("sqlite: allocator prefix differs from row key")
	}
	if err := value.Validate(); err != nil {
		return driver.OfflineAllocator{}, err
	}
	return value.Clone(), nil
}

func (r *reader) LoadAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.OfflineAllocator{}, err
	}
	if r.owner != nil {
		if value, ok := r.owner.allocators[prefix]; ok {
			return value.Clone(), nil
		}
	}
	return loadAllocator(ctx, r.tx, prefix)
}

func (t *transaction) LoadAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.OfflineAllocator{}, err
	}
	if value, ok := t.allocators[prefix]; ok {
		return value.Clone(), nil
	}
	return loadAllocator(ctx, t.tx, prefix)
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
	t.bumps[id] = key
	return nil
}

func (t *transaction) RestoreAllocators(ctx context.Context, values []driver.OfflineAllocator) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if err := driver.ValidateAllocatorSet(values); err != nil {
		return err
	}
	for _, value := range values {
		if _, _, err := t.allocatorWritable(value.Prefix); err != nil {
			return err
		}
		if _, exists := t.allocators[value.Prefix]; exists {
			return fmt.Errorf("%w: allocator prefix %q was already staged", driver.ErrConflict, value.Prefix)
		}
		if _, err := loadAllocator(ctx, t.tx, value.Prefix); err == nil {
			return fmt.Errorf("%w: allocator prefix %q already exists", driver.ErrConflict, value.Prefix)
		} else if !errors.Is(err, driver.ErrNotFound) {
			return err
		}
	}
	for _, value := range values {
		if err := t.stageAllocator(value.Prefix, value); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) currentAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	if value, ok := t.allocators[prefix]; ok {
		return value.Clone(), nil
	}
	return loadAllocator(ctx, t.tx, prefix)
}

func (t *transaction) AllocateTopLevel(ctx context.Context, prefix, expected string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	value, err := t.currentAllocator(ctx, prefix)
	if err != nil {
		return err
	}
	next, err := value.NextID()
	if err != nil {
		return err
	}
	if expected != next {
		return fmt.Errorf("%w: allocator next ID is %s, expected %s", driver.ErrConflict, next, expected)
	}
	value.LastID++
	return t.stageAllocator(prefix, value)
}

func (t *transaction) AllocateChild(ctx context.Context, parent, expected string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	prefix, err := t.allocatorPrefix(ctx, parent)
	if err != nil {
		return err
	}
	value, err := t.currentAllocator(ctx, prefix)
	if err != nil {
		return err
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

func (t *transaction) allocatorPrefix(ctx context.Context, parent string) (string, error) {
	prefix := ""
	for candidate := range t.allocators {
		if strings.HasPrefix(parent, candidate+"-") && len(candidate) > len(prefix) {
			prefix = candidate
		}
	}
	rows, err := t.tx.QueryContext(ctx, `SELECT prefix FROM allocator_states`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var candidate string
		if err := rows.Scan(&candidate); err != nil {
			return "", err
		}
		if strings.HasPrefix(parent, candidate+"-") && len(candidate) > len(prefix) {
			prefix = candidate
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if prefix == "" {
		prefix = parent
	}
	return prefix, nil
}

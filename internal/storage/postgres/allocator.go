package postgres

import (
	"context"
	"database/sql"
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
	id, err := keyID(key)
	if err != nil {
		return driver.LockKey{}, "", err
	}
	return key, id, nil
}

func loadAllocator(ctx context.Context, q querier, prefix string) (driver.OfflineAllocator, error) {
	if _, err := driver.AllocatorFact(prefix); err != nil {
		return driver.OfflineAllocator{}, err
	}
	var raw []byte
	err := q.QueryRowContext(ctx, `SELECT data FROM plug_postgres.allocator_states WHERE prefix=$1`, prefix).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.OfflineAllocator{}, driver.ErrNotFound
	}
	if err != nil {
		return driver.OfflineAllocator{}, err
	}
	var value driver.OfflineAllocator
	if err := decode(raw, &value); err != nil {
		return driver.OfflineAllocator{}, err
	}
	if value.Prefix != prefix {
		return driver.OfflineAllocator{}, fmt.Errorf("postgres: allocator prefix differs from row key")
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
	if _, err := driver.AllocatorFact(prefix); err != nil {
		return driver.OfflineAllocator{}, err
	}
	if r.writer != nil {
		if a, ok := r.writer.allocators[prefix]; ok {
			return a.Clone(), nil
		}
		return loadAllocator(ctx, r.sql, prefix)
	}
	snapshot, err := r.allocatorSnapshot(ctx)
	if err != nil {
		return driver.OfflineAllocator{}, err
	}
	for _, a := range snapshot.Inventory.Projects {
		if a.Prefix == prefix {
			return a.Clone(), nil
		}
	}
	return driver.OfflineAllocator{}, driver.ErrNotFound
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
	return loadAllocator(ctx, t.sql, prefix)
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
	if err := value.Validate(); err != nil {
		return err
	}
	t.allocators[prefix] = value.Clone()
	// The allocator fact is published through the ordinary state history in the
	// same SQL transaction as the allocator row and any records it advances.
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

	// Validate the complete batch before changing any staged state. A duplicate
	// or existing prefix therefore cannot leave a partial restore behind.
	type stagedAllocator struct {
		value driver.OfflineAllocator
		key   driver.LockKey
		id    string
	}
	prepared := make([]stagedAllocator, 0, len(values))
	for _, value := range values {
		key, id, err := t.allocatorWritable(value.Prefix)
		if err != nil {
			return err
		}
		if _, exists := t.allocators[value.Prefix]; exists {
			return fmt.Errorf("%w: allocator prefix %q was already staged", driver.ErrConflict, value.Prefix)
		}
		if _, err := loadAllocator(ctx, t.sql, value.Prefix); err == nil {
			return fmt.Errorf("%w: allocator prefix %q already exists", driver.ErrConflict, value.Prefix)
		} else if !errors.Is(err, driver.ErrNotFound) {
			return err
		}
		prepared = append(prepared, stagedAllocator{value: value.Clone(), key: key, id: id})
	}
	for _, item := range prepared {
		t.allocators[item.value.Prefix] = item.value
		t.bumps[item.id] = item.key
	}
	return nil
}

func (t *transaction) currentAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	if value, ok := t.allocators[prefix]; ok {
		return value.Clone(), nil
	}
	return loadAllocator(ctx, t.sql, prefix)
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
	if value.Mode == driver.AllocatorRandom {
		return t.consumeRandom(ctx, value, "", expected)
	}
	if value.Mode == driver.AllocatorHash {
		return fmt.Errorf("%w: hash allocation requires the content tuple", driver.ErrUnsupported)
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
	ref := driver.Ref{ID: expected}
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
	var exists bool
	if err := t.sql.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM plug_postgres.records WHERE key=$1)`, []byte(id)).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%w: allocator candidate already exists", driver.ErrConflict)
	}
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	value, err := t.currentAllocator(ctx, prefix)
	if err != nil {
		return err
	}
	if value.Mode != driver.AllocatorHash {
		return fmt.Errorf("%w: allocator is not hash mode", driver.ErrUnsupported)
	}
	if err := value.ValidateHashCandidate(input, expected); err != nil {
		return err
	}
	ref := driver.Ref{ID: expected}
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
	var exists bool
	if err := t.sql.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM plug_postgres.records WHERE key=$1)`, []byte(id)).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%w: allocator candidate already exists", driver.ErrConflict)
	}
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

// Consume a previewed opaque candidate under the complete allocator/record
// locks. Never draw randomness again or advance a random profile's counters.
func (t *transaction) consumeRandom(ctx context.Context, value driver.OfflineAllocator, parent, expected string) error {
	if err := value.ValidateCandidate(parent, expected); err != nil {
		return err
	}
	if _, _, err := t.allocatorWritable(value.Prefix); err != nil {
		return err
	}
	ref := driver.Ref{ID: expected}
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
	var exists bool
	if err := t.sql.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM plug_postgres.records WHERE key=$1)`, []byte(id)).Scan(&exists); err != nil {
		return err
	}
	if exists {
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

func (t *transaction) allocatorPrefix(ctx context.Context, parent string) (string, error) {
	prefix := ""
	for candidate := range t.allocators {
		if strings.HasPrefix(parent, candidate+"-") && len(candidate) > len(prefix) {
			prefix = candidate
		}
	}
	// Journal identities are not project prefixes. They remain part of the
	// complete snapshot/occupancy checks, outside this live allocator lookup.
	rows, err := t.sql.QueryContext(ctx, `SELECT prefix FROM plug_postgres.allocator_states WHERE prefix NOT LIKE 'allocator-snapshot/%'`)
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

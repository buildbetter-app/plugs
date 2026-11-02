package memory

import (
	"context"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func parentCounterValue(staged, committed map[string]driver.ParentCounter, incarnation, parent string) (driver.ParentCounter, error) {
	key, err := driver.ParentCounterKey(incarnation, parent)
	if err != nil {
		return driver.ParentCounter{}, err
	}
	value, ok := staged[key]
	if !ok {
		value, ok = committed[key]
	}
	if !ok {
		return driver.ParentCounter{}, driver.ErrNotFound
	}
	return value, value.Validate(incarnation, parent)
}

func (r *reader) LoadParentCounter(ctx context.Context, parent string) (driver.ParentCounter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return driver.ParentCounter{}, err
	}
	if r.state.meta.Head.String() != model.Natural64(r.at).String() {
		return driver.ParentCounter{}, fmt.Errorf("%w: historical parent counters", driver.ErrUnsupported)
	}
	return parentCounterValue(nil, r.state.parentCounters, r.incarnation, parent)
}

func (t *transaction) LoadParentCounter(ctx context.Context, parent string) (driver.ParentCounter, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.ParentCounter{}, err
	}
	return parentCounterValue(t.parentCounters, t.base.parentCounters, t.incarnation, parent)
}

func (t *transaction) ConsumeParentChild(ctx context.Context, parent, expected string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.restore != nil || t.offlineHistory != nil {
		return fmt.Errorf("%w: parent counters are not an offline restore profile", driver.ErrUnsupported)
	}
	key, err := driver.ParentCounterLock(t.incarnation, parent)
	if err != nil {
		return err
	}
	if err := t.writable(key); err != nil {
		return err
	}
	ref := model.Ref{ID: expected}
	if err := t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}); err != nil {
		return err
	}
	value, err := parentCounterValue(t.parentCounters, t.base.parentCounters, t.incarnation, parent)
	if err == driver.ErrNotFound {
		value = driver.ParentCounter{Incarnation: t.incarnation, Parent: parent}
	} else if err != nil {
		return err
	}
	value, err = value.Consume(expected)
	if err != nil {
		return err
	}
	id, err := recordKey(ref)
	if err != nil {
		return err
	}
	if _, staged := t.records[id]; staged || t.allocated[id] {
		return fmt.Errorf("%w: child candidate already staged or reserved", driver.ErrConflict)
	}
	if _, exists := t.base.kinds[id]; exists {
		return fmt.Errorf("%w: child candidate already exists", driver.ErrConflict)
	}
	storageKey, _ := driver.ParentCounterKey(t.incarnation, parent)
	if t.parentCounters == nil {
		t.parentCounters = make(map[string]driver.ParentCounter)
	}
	t.parentCounters[storageKey] = value
	t.bumps[keyID(key)] = key
	if t.allocated == nil {
		t.allocated = make(map[string]bool)
	}
	t.allocated[id] = true
	return nil
}

func (r *reader) AllocatorInventory(ctx context.Context) (driver.AllocatorInventory, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return driver.AllocatorInventory{}, err
	}
	if r.state.meta.Head.String() != model.Natural64(r.at).String() {
		return driver.AllocatorInventory{}, fmt.Errorf("%w: historical allocator inventory", driver.ErrUnsupported)
	}
	inventory := driver.AllocatorInventory{}
	for _, value := range r.state.allocators {
		if err := value.Validate(); err != nil {
			return driver.AllocatorInventory{}, err
		}
		inventory.Projects = append(inventory.Projects, value.Clone())
	}
	for key, value := range r.state.parentCounters {
		expected, err := driver.ParentCounterKey(r.incarnation, value.Parent)
		if err != nil || expected != key {
			return driver.AllocatorInventory{}, fmt.Errorf("invalid parent counter inventory key")
		}
		if err := value.Validate(r.incarnation, value.Parent); err != nil {
			return driver.AllocatorInventory{}, err
		}
		inventory.Parents = append(inventory.Parents, value)
	}
	sort.Slice(inventory.Projects, func(i, j int) bool { return inventory.Projects[i].Prefix < inventory.Projects[j].Prefix })
	sort.Slice(inventory.Parents, func(i, j int) bool { return inventory.Parents[i].Parent < inventory.Parents[j].Parent })
	return inventory, nil
}

var _ driver.ParentCounterReader = (*reader)(nil)
var _ driver.ParentCounterContinuation = (*transaction)(nil)
var _ driver.AllocatorInventoryReader = (*reader)(nil)

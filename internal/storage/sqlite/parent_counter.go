package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func loadParentCounter(ctx context.Context, q queryer, incarnation, parent string) (driver.ParentCounter, error) {
	key, err := driver.ParentCounterKey(incarnation, parent)
	if err != nil {
		return driver.ParentCounter{}, err
	}
	var raw []byte
	err = q.QueryRowContext(ctx, `SELECT data FROM allocator_states WHERE prefix=?`, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return driver.ParentCounter{}, driver.ErrNotFound
	}
	if err != nil {
		return driver.ParentCounter{}, err
	}
	var value driver.ParentCounter
	if err := json.Unmarshal(raw, &value); err != nil {
		return driver.ParentCounter{}, err
	}
	return value, value.Validate(incarnation, parent)
}

func (r *reader) LoadParentCounter(ctx context.Context, parent string) (driver.ParentCounter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.ParentCounter{}, err
	}
	if r.owner != nil {
		if value, ok := r.owner.parentCounters[parent]; ok {
			return value, value.Validate(r.meta.Incarnation, parent)
		}
	} else if r.meta.Head.String() != model.Natural64(r.at).String() {
		return driver.ParentCounter{}, fmt.Errorf("%w: historical parent counters", driver.ErrUnsupported)
	}
	return loadParentCounter(ctx, r.tx, r.meta.Incarnation, parent)
}

func (t *transaction) LoadParentCounter(ctx context.Context, parent string) (driver.ParentCounter, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.ParentCounter{}, err
	}
	return t.currentParentCounter(ctx, parent)
}

func (t *transaction) currentParentCounter(ctx context.Context, parent string) (driver.ParentCounter, error) {
	if value, ok := t.parentCounters[parent]; ok {
		return value, value.Validate(t.meta.Incarnation, parent)
	}
	return loadParentCounter(ctx, t.tx, t.meta.Incarnation, parent)
}

func (t *transaction) ConsumeParentChild(ctx context.Context, parent, expected string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.restore != nil || t.offlineHistory != nil {
		return fmt.Errorf("%w: parent counters are not an offline restore profile", driver.ErrUnsupported)
	}
	key, err := driver.ParentCounterLock(t.meta.Incarnation, parent)
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
	value, err := t.currentParentCounter(ctx, parent)
	if errors.Is(err, driver.ErrNotFound) {
		value = driver.ParentCounter{Incarnation: t.meta.Incarnation, Parent: parent}
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
	var exists bool
	if err := t.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM objects WHERE key=?)`, id).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%w: child candidate already exists", driver.ErrConflict)
	}
	if t.parentCounters == nil {
		t.parentCounters = make(map[string]driver.ParentCounter)
	}
	t.parentCounters[parent] = value
	t.bumps[keyID(key)] = key
	if t.allocated == nil {
		t.allocated = make(map[string]bool)
	}
	t.allocated[id] = true
	return nil
}

func (t *transaction) publishParentCounters(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}) error {
	for parent, value := range t.parentCounters {
		if err := value.Validate(t.meta.Incarnation, parent); err != nil {
			return err
		}
		key, err := driver.ParentCounterKey(t.meta.Incarnation, parent)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO allocator_states(prefix,data) VALUES(?,?) ON CONFLICT(prefix) DO UPDATE SET data=excluded.data`, key, raw); err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) AllocatorInventory(ctx context.Context) (driver.AllocatorInventory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.AllocatorInventory{}, err
	}
	if r.owner != nil {
		return driver.AllocatorInventory{}, fmt.Errorf("%w: allocator inventory requires a read-only snapshot", driver.ErrUnsupported)
	}
	if r.meta.Head.String() != model.Natural64(r.at).String() {
		return driver.AllocatorInventory{}, fmt.Errorf("%w: historical allocator inventory", driver.ErrUnsupported)
	}
	rows, err := r.tx.QueryContext(ctx, `SELECT prefix,data FROM allocator_states ORDER BY prefix`)
	if err != nil {
		return driver.AllocatorInventory{}, err
	}
	defer rows.Close()
	inventory := driver.AllocatorInventory{}
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			return driver.AllocatorInventory{}, err
		}
		if err := inventory.AddRow(key, raw, r.meta.Incarnation); err != nil {
			return driver.AllocatorInventory{}, err
		}
	}
	return inventory, rows.Err()
}

var _ driver.ParentCounterReader = (*reader)(nil)
var _ driver.ParentCounterContinuation = (*transaction)(nil)
var _ driver.AllocatorInventoryReader = (*reader)(nil)

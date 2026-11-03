package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func loadParentCounter(ctx context.Context, q querier, incarnation, parent string) (driver.ParentCounter, error) {
	key, err := driver.ParentCounterKey(incarnation, parent)
	if err != nil {
		return driver.ParentCounter{}, err
	}
	var raw []byte
	err = q.QueryRowContext(ctx, `SELECT data FROM plug_postgres.allocator_states WHERE prefix=$1`, key).Scan(&raw)
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
	if _, err := driver.ParentCounterKey(r.meta.Incarnation, parent); err != nil {
		return driver.ParentCounter{}, err
	}
	if r.writer != nil {
		if value, ok := r.writer.parentCounters[parent]; ok {
			return value, value.Validate(r.meta.Incarnation, parent)
		}
		return loadParentCounter(ctx, r.sql, r.meta.Incarnation, parent)
	}
	snapshot, err := r.allocatorSnapshot(ctx)
	if err != nil {
		return driver.ParentCounter{}, err
	}
	for _, p := range snapshot.Inventory.Parents {
		if p.Parent == parent {
			return p, nil
		}
	}
	return driver.ParentCounter{}, driver.ErrNotFound
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
	return loadParentCounter(ctx, t.sql, t.meta.Incarnation, parent)
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
	if err := t.sql.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM plug_postgres.records WHERE key=$1)`, []byte(id)).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%w: child candidate already exists", driver.ErrConflict)
	}
	if t.parentCounters == nil {
		t.parentCounters = make(map[string]driver.ParentCounter)
	}
	t.parentCounters[parent] = value
	keyID, err := keyID(key)
	if err != nil {
		return err
	}
	t.bumps[keyID] = key
	if t.allocated == nil {
		t.allocated = make(map[string]bool)
	}
	t.allocated[id] = true
	return nil
}

func (t *transaction) publishParentCounters(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}) error {
	incarnation := t.meta.Incarnation
	if t.restore != nil {
		incarnation = t.restore.Incarnation
	}
	for parent, value := range t.parentCounters {
		if err := value.Validate(incarnation, parent); err != nil {
			return err
		}
		key, err := driver.ParentCounterKey(incarnation, parent)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO plug_postgres.allocator_states(prefix,data) VALUES ($1,$2) ON CONFLICT (prefix) DO UPDATE SET data=EXCLUDED.data`, key, raw); err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) AllocatorInventory(ctx context.Context) (driver.AllocatorInventory, error) {
	snapshot, err := r.AllocatorSnapshot(ctx)
	return snapshot.Inventory, err
}

var _ driver.ParentCounterReader = (*reader)(nil)
var _ driver.ParentCounterContinuation = (*transaction)(nil)
var _ driver.AllocatorInventoryReader = (*reader)(nil)

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func allocatorRows(ctx context.Context, q querier, liveOnly bool) (map[string][]byte, error) {
	query := `SELECT prefix,data FROM plug_postgres.allocator_states`
	if liveOnly {
		query += ` WHERE prefix NOT LIKE 'allocator-snapshot/%'`
	}
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string][]byte{}
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			return nil, err
		}
		result[key] = bytes.Clone(raw)
	}
	return result, rows.Err()
}

func (r *reader) allocatorSnapshot(ctx context.Context) (driver.AllocatorSnapshot, error) {
	if r.writer != nil {
		return driver.AllocatorSnapshot{}, fmt.Errorf("%w: allocator snapshot requires read-only view", driver.ErrUnsupported)
	}
	if r.allocatorImage != nil {
		return *r.allocatorImage, nil
	}
	rows, err := allocatorRows(ctx, r.sql, false)
	if err != nil {
		return driver.AllocatorSnapshot{}, err
	}
	snapshot, err := driver.MaterializeAllocatorJournal(r.meta, model.Natural64(r.at), rows)
	if err != nil {
		return driver.AllocatorSnapshot{}, err
	}
	r.allocatorImage = &snapshot
	return snapshot, nil
}

func (r *reader) AllocatorSnapshot(ctx context.Context) (driver.AllocatorSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.AllocatorSnapshot{}, err
	}
	snapshot, err := r.allocatorSnapshot(ctx)
	if err != nil {
		return driver.AllocatorSnapshot{}, err
	}
	return snapshot.Detached()
}

// The index and changed immutable values are written in the same fenced SQL
// transaction as graph/meta/receipt publication. Existing values are shared.
func (t *transaction) publishAllocatorJournal(ctx context.Context, q interface {
	querier
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, target driver.Meta, reset bool) error {
	invalid := func(err error) error {
		return driver.NewNativeAllocatorSnapshotError(driver.NativeAllocatorInventoryInvalid, err)
	}
	inventory := driver.AllocatorInventory{}
	floor := target.Head
	var previous driver.AllocatorSnapshot
	var baseline bool
	if !reset {
		rows, err := allocatorRows(ctx, q, true)
		if err != nil {
			return err
		}
		for key, raw := range rows {
			if err := inventory.AddRow(key, raw, t.meta.Incarnation); err != nil {
				return invalid(err)
			}
		}
		var index []byte
		err = q.QueryRowContext(ctx, `SELECT data FROM plug_postgres.allocator_states WHERE prefix=$1`, driver.AllocatorJournalIndexKey(t.meta.Incarnation, t.meta.Head)).Scan(&index)
		if errors.Is(err, sql.ErrNoRows) {
			var occupied bool
			if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM plug_postgres.allocator_states WHERE prefix LIKE 'allocator-snapshot/%')`).Scan(&occupied); err != nil {
				return err
			}
			if occupied {
				return invalid(fmt.Errorf("missing complete current allocator index"))
			}
			baseline = true
			floor = t.meta.Head
		} else if err != nil {
			return err
		} else {
			floor, err = driver.AllocatorJournalCoverage(t.meta, index)
			if err != nil {
				return invalid(err)
			}
		}
		previous, err = driver.NewAllocatorSnapshot(t.meta, t.meta.Head, floor, inventory)
		if err != nil {
			return err
		}
		if !baseline {
			entries, err := driver.AllocatorJournalEntries(previous)
			if err != nil {
				return err
			}
			if !bytes.Equal(entries[driver.AllocatorJournalIndexKey(t.meta.Incarnation, t.meta.Head)], index) {
				return invalid(fmt.Errorf("allocator live census differs from committed index"))
			}
		}
	}
	projects := map[string]driver.OfflineAllocator{}
	parents := map[string]driver.ParentCounter{}
	for _, a := range inventory.Projects {
		projects[a.Prefix] = a
	}
	for _, p := range inventory.Parents {
		parents[p.Parent] = p
	}
	for key, a := range t.allocators {
		projects[key] = a.Clone()
	}
	for key, p := range t.parentCounters {
		if err := p.Validate(target.Incarnation, key); err != nil {
			return invalid(err)
		}
		parents[key] = p
	}
	inventory = driver.AllocatorInventory{}
	for _, a := range projects {
		inventory.Projects = append(inventory.Projects, a)
	}
	for _, p := range parents {
		inventory.Parents = append(inventory.Parents, p)
	}
	next, err := driver.NewAllocatorSnapshot(target, target.Head, floor, inventory)
	if err != nil {
		return err
	}
	var entries map[string][]byte
	if reset {
		entries, err = driver.AllocatorJournalEntries(next)
	} else {
		entries, err = driver.AllocatorJournalDelta(previous, next)
	}
	if err != nil {
		return err
	}
	if baseline {
		initial, err := driver.AllocatorJournalEntries(previous)
		if err != nil {
			return err
		}
		for key, raw := range initial {
			entries[key] = raw
		}
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		raw := entries[key]
		result, err := q.ExecContext(ctx, `INSERT INTO plug_postgres.allocator_states(prefix,data) VALUES($1,$2) ON CONFLICT(prefix) DO NOTHING`, key, raw)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			var actual []byte
			if err := q.QueryRowContext(ctx, `SELECT data FROM plug_postgres.allocator_states WHERE prefix=$1`, key).Scan(&actual); err != nil {
				return err
			}
			if !bytes.Equal(actual, raw) {
				return invalid(fmt.Errorf("immutable allocator history collision"))
			}
		}
	}
	return nil
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
	t.mu.Lock()
	defer t.mu.Unlock()
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
	if err := t.restoreEmpty(ctx); err != nil {
		return err
	}
	copy, err := snapshot.Detached()
	if err != nil {
		return err
	}
	parents := map[string]driver.ParentCounter{}
	bumps := map[string]driver.LockKey{}
	addBump := func(key driver.LockKey) error {
		id, err := keyID(key)
		if err != nil {
			return err
		}
		bumps[id] = key
		return nil
	}
	fence, _ := driver.NativeAllocatorRestoreLock()
	if err := addBump(fence); err != nil {
		return err
	}
	for _, a := range copy.Inventory.Projects {
		key, err := driver.AllocatorLock(a.Prefix)
		if err != nil {
			return err
		}
		if err := addBump(key); err != nil {
			return err
		}
	}
	for _, p := range copy.Inventory.Parents {
		p.Incarnation = t.restore.Incarnation
		parents[p.Parent] = p
		key, err := driver.ParentCounterLock(p.Incarnation, p.Parent)
		if err != nil {
			return err
		}
		if err := addBump(key); err != nil {
			return err
		}
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

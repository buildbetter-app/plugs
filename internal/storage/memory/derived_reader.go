package memory

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (r *reader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if err := (driver.DerivedRowKey{Table: table, Key: "validation"}).Validate(); err != nil {
		return nil, err
	}
	rows := []driver.DerivedRow{}
	for _, history := range r.state.keyHistory {
		index := sort.Search(len(history), func(i int) bool { n, _ := history[i].Revision.Uint64(); return n > r.at }) - 1
		if index < 0 {
			continue
		}
		state := history[index]
		if state.Key.Kind == driver.LockDerived && state.Key.Derived.Table == table && state.Present && state.Derived != nil && strings.HasPrefix(state.Derived.Key, prefix) {
			rows = append(rows, driver.DerivedRow{Key: state.Derived.Key, Value: bytes.Clone(state.Derived.Value)})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return rows, nil
}

func (t *transaction) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	r, err := t.snapshotReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	rows, err := r.DerivedRows(ctx, table, prefix)
	if err != nil {
		return nil, err
	}
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return nil, err
	}
	byKey := map[string]driver.DerivedRow{}
	for _, row := range rows {
		byKey[row.Key] = row
	}
	for _, state := range t.derived {
		if state.Key.Derived.Table != table || !strings.HasPrefix(state.Key.Derived.Key, prefix) {
			continue
		}
		delete(byKey, state.Key.Derived.Key)
		if state.Present && state.Derived != nil {
			byKey[state.Derived.Key] = driver.DerivedRow{Key: state.Derived.Key, Value: bytes.Clone(state.Derived.Value)}
		}
	}
	rows = rows[:0]
	for _, row := range byKey {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return rows, nil
}

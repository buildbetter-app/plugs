package memory

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (r *reader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	return r.DerivedRowsForPrefixes(ctx, table, []string{prefix})
}

// DerivedRowsForPrefixes seeks only the requested ranges in the immutable
// historical key catalog, then resolves each selected key at the pinned view.
func (r *reader) DerivedRowsForPrefixes(ctx context.Context, table driver.DerivedTable, prefixes []string) ([]driver.DerivedRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	return r.state.derivedRowsForPrefixes(ctx, table, prefixes, r.at, nil, nil, nil)
}

func (t *transaction) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	return t.DerivedRowsForPrefixes(ctx, table, []string{prefix})
}

func (t *transaction) DerivedRowsForPrefixes(ctx context.Context, table driver.DerivedTable, prefixes []string) ([]driver.DerivedRow, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return nil, err
	}
	at, err := t.snapshotAt.Uint64()
	if err != nil {
		return nil, err
	}
	// Read the captured immutable base directly; building a snapshotReader
	// would clone unrelated record and derived-history maps for each call.
	return t.base.derivedRowsForPrefixes(ctx, table, prefixes, at, t.derived, nil, t.selectionSourceChanged)
}

// publishDerivedKeys merges new historical keys once per changed table.
// Deleted keys stay in the catalog so earlier readers can resolve them. Old
// catalog slices are never changed, including when fork shares their storage.
func (s *state) publishDerivedKeys(changes map[string]driver.LockState) {
	added := make(map[driver.DerivedTable][]string)
	for _, change := range changes {
		key := change.Key.Derived
		old := s.derivedKeys[key.Table]
		i := sort.SearchStrings(old, key.Key)
		if i == len(old) || old[i] != key.Key {
			added[key.Table] = append(added[key.Table], key.Key)
		}
	}
	for table, keys := range added {
		sort.Strings(keys)
		old := s.derivedKeys[table]
		merged := make([]string, 0, len(old)+len(keys))
		i := 0
		for _, key := range keys {
			for i < len(old) && old[i] < key {
				merged = append(merged, old[i])
				i++
			}
			if len(merged) == 0 || merged[len(merged)-1] != key {
				merged = append(merged, key)
			}
		}
		s.derivedKeys[table] = append(merged, old[i:]...)
	}
}

func derivedPrefixUnion(table driver.DerivedTable, prefixes []string) ([]string, error) {
	if err := (driver.DerivedRowKey{Table: table, Key: "validation"}).Validate(); err != nil {
		return nil, err
	}
	selected := append([]string(nil), prefixes...)
	for _, prefix := range selected {
		// Appending a non-space byte admits empty prefixes and preserves the
		// existing prefix validation, including opaque non-UTF8 bytes.
		if err := (driver.DerivedRowKey{Table: table, Key: prefix + "!"}).Validate(); err != nil {
			return nil, err
		}
	}
	sort.Strings(selected)
	union := selected[:0]
	for _, prefix := range selected {
		if len(union) == 0 || !strings.HasPrefix(prefix, union[len(union)-1]) {
			union = append(union, prefix)
		}
	}
	return union, nil
}

type derivedPrefixWork struct{ histories, staged int }

func (s *state) derivedRowsForPrefixes(ctx context.Context, table driver.DerivedTable, prefixes []string, at uint64, staged map[string]driver.LockState, work *derivedPrefixWork, sourceChanges ...func(driver.LockKey) bool) ([]driver.DerivedRow, error) {
	var sourceChanged func(driver.LockKey) bool
	if len(sourceChanges) != 0 {
		sourceChanged = sourceChanges[0]
	}
	union, err := derivedPrefixUnion(table, prefixes)
	if err != nil {
		return nil, err
	}
	rows := make([]driver.DerivedRow, 0)
	committed := make(map[string]driver.DerivedRow)
	if len(union) == 0 {
		return rows, nil
	}
	keys := s.derivedKeys[table]
	for _, prefix := range union {
		for i := sort.SearchStrings(keys, prefix); i < len(keys) && strings.HasPrefix(keys[i], prefix); i++ {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			key := keys[i]
			id := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: key}})
			_, shadowed := staged[id]
			if work != nil {
				work.histories++
			}
			history := s.keyHistory[id]
			var revisionErr error
			index := sort.Search(len(history), func(j int) bool {
				n, e := history[j].Revision.Uint64()
				if e != nil {
					revisionErr = e
				}
				return e != nil || n > at
			}) - 1
			if revisionErr != nil {
				if shadowed {
					continue
				}
				return nil, revisionErr
			}
			if len(history) == 0 {
				if shadowed {
					continue
				}
				return nil, fmt.Errorf("memory: missing indexed derived history")
			}
			if index < 0 {
				continue
			}
			row, present, e := selectedDerivedRow(history[index], table, key)
			if e != nil {
				if shadowed {
					continue
				}
				return nil, e
			}
			if present {
				if staged != nil {
					committed[key] = row.Clone()
				}
				if shadowed {
					continue
				}
				rows = append(rows, row)
			}
		}
	}
	for _, state := range staged {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if work != nil {
			work.staged++
		}
		if state.Key.Derived == nil || state.Key.Derived.Table != table {
			continue
		}
		key := state.Key.Derived.Key
		i := sort.Search(len(union), func(i int) bool { return union[i] > key }) - 1
		if i < 0 || !strings.HasPrefix(key, union[i]) {
			continue
		}
		row, present, e := selectedDerivedRow(state, table, key)
		if e != nil {
			return nil, e
		}
		if present {
			if row.Index == nil {
				if base, ok := committed[key]; ok && base.Index != nil && bytes.Equal(base.Value, row.Value) && (sourceChanged == nil || !sourceChanged(base.Index.Source.Key)) {
					row.Index = base.Clone().Index
				}
			}
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func selectedDerivedRow(state driver.LockState, table driver.DerivedTable, key string) (driver.DerivedRow, bool, error) {
	if state.Key.Kind != driver.LockDerived || state.Key.Derived == nil || state.Key.Derived.Table != table || state.Key.Derived.Key != key {
		return driver.DerivedRow{}, false, fmt.Errorf("memory: mismatched indexed derived key")
	}
	if err := state.Key.Validate(); err != nil {
		return driver.DerivedRow{}, false, err
	}
	if !state.Present {
		return driver.DerivedRow{}, false, nil
	}
	if state.Derived == nil || state.Derived.Key != key {
		return driver.DerivedRow{}, false, fmt.Errorf("memory: malformed present derived history row")
	}
	return state.Derived.Clone(), true, nil
}

var _ driver.DerivedPrefixReader = (*reader)(nil)
var _ driver.DerivedPrefixReader = (*transaction)(nil)

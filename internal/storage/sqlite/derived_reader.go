package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (r *reader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	return r.DerivedRowsForPrefixes(ctx, table, []string{prefix})
}

func (r *reader) DerivedRowsForPrefixes(ctx context.Context, table driver.DerivedTable, prefixes []string) ([]driver.DerivedRow, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if err := (driver.DerivedRowKey{Table: table, Key: "validation"}).Validate(); err != nil {
		return nil, err
	}
	selected := append([]string(nil), prefixes...)
	for _, prefix := range selected {
		if _, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: prefix + "!"}}); err != nil {
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
	byKey := map[string]driver.DerivedRow{}
	// Bound SQL parameters and expression depth without broadening the union.
	// All batches read the same pinned transaction and revision.
	const batchSize = 128
	for offset := 0; offset < len(union); offset += batchSize {
		batch := union[offset:min(offset+batchSize, len(union))]
		conditions := make([]string, 0, len(batch))
		args := make([]any, 0, 2*len(batch)+1)
		for _, prefix := range batch {
			start, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: prefix + "!"}})
			start = strings.TrimSuffix(start, "!")
			end := []byte(start)
			for i := len(end) - 1; i >= 0; i-- {
				if end[i] < 255 {
					end[i]++
					end = end[:i+1]
					break
				}
			}
			conditions = append(conditions, "(key>=? AND key<?)")
			args = append(args, start, string(end))
		}
		args = append(args, revKey(r.at))
		rows, err := r.tx.QueryContext(ctx, `SELECT h.data FROM key_history h JOIN
	 (SELECT key, MAX(rev) AS rev FROM key_history WHERE (`+strings.Join(conditions, " OR ")+`) AND rev<=? GROUP BY key) v
	 ON h.key=v.key AND h.rev=v.rev ORDER BY h.key`, args...)
		if err != nil {
			return nil, err
		}
		err = func() error {
			defer rows.Close()
			for rows.Next() {
				var raw []byte
				var state driver.LockState
				if err := rows.Scan(&raw); err != nil {
					return err
				}
				if err := json.Unmarshal(raw, &state); err != nil {
					return err
				}
				driver.ReportReadWork(ctx, driver.ReadPrefixStates, 1)
				driver.ReportReadWork(ctx, driver.ReadPrefixBytes, int64(len(raw)))
				if state.Present && state.Derived != nil {
					byKey[state.Derived.Key] = *state.Derived
				}
			}
			return rows.Err()
		}()
		if err != nil {
			return nil, err
		}
	}
	if r.owner != nil {
		committed := make(map[string]driver.DerivedRow, len(byKey))
		for key, row := range byKey {
			committed[key] = row.Clone()
		}
		for _, state := range r.owner.derived {
			key := state.Key.Derived.Key
			i := sort.Search(len(union), func(i int) bool { return union[i] > key }) - 1
			if state.Key.Derived.Table != table || i < 0 || !strings.HasPrefix(key, union[i]) {
				continue
			}
			delete(byKey, state.Key.Derived.Key)
			if state.Present && state.Derived != nil {
				row := state.Derived.Clone()
				if row.Index == nil {
					if base, ok := committed[key]; ok && base.Index != nil && bytes.Equal(base.Value, row.Value) && !r.owner.selectionSourceChanged(base.Index.Source.Key) {
						row.Index = base.Clone().Index
					}
				}
				byKey[row.Key] = row
			}
		}
	}
	result := make([]driver.DerivedRow, 0, len(byKey))
	for _, row := range byKey {
		result = append(result, row.Clone())
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

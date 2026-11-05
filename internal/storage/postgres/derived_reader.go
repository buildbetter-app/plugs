package postgres

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (r *reader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	start, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: prefix + "!"}})
	if err != nil {
		return nil, err
	}
	start = strings.TrimSuffix(start, "!")
	end := []byte(start)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 255 {
			end[i]++
			end = end[:i+1]
			break
		}
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT DISTINCT ON (key) value FROM plug_postgres.states
	 WHERE key>=$1 AND key<$2 AND revision<=$3 ORDER BY key, revision DESC`, []byte(start), end, decimal(r.at))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byKey := map[string]driver.DerivedRow{}
	for rows.Next() {
		var raw []byte
		var state driver.LockState
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := decode(raw, &state); err != nil {
			return nil, err
		}
		driver.ReportReadWork(ctx, driver.ReadPrefixStates, 1)
		driver.ReportReadWork(ctx, driver.ReadPrefixBytes, int64(len(raw)))
		if state.Present && state.Derived != nil {
			byKey[state.Derived.Key] = *state.Derived
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if r.writer != nil {
		for _, state := range r.writer.derived {
			if state.Key.Derived.Table != table || !strings.HasPrefix(state.Key.Derived.Key, prefix) {
				continue
			}
			delete(byKey, state.Key.Derived.Key)
			if state.Present && state.Derived != nil {
				byKey[state.Derived.Key] = *state.Derived
			}
		}
	}
	result := make([]driver.DerivedRow, 0, len(byKey))
	for _, row := range byKey {
		result = append(result, driver.DerivedRow{Key: row.Key, Value: bytes.Clone(row.Value)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

package postgres

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (r *reader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	return r.DerivedRowsForPrefixes(ctx, table, []string{prefix})
}

// The union is resolved in one pinned statement, then one final staged overlay.
// Selection conformance reads annotations through this existing optional seam.
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
	for _, p := range selected {
		if err := (driver.DerivedRowKey{Table: table, Key: p + "!"}).Validate(); err != nil {
			return nil, err
		}
	}
	sort.Strings(selected)
	union := selected[:0]
	for _, p := range selected {
		if len(union) == 0 || !strings.HasPrefix(p, union[len(union)-1]) {
			union = append(union, p)
		}
	}
	if len(union) == 0 {
		return []driver.DerivedRow{}, nil
	}
	lower, upper := make([][]byte, 0, len(union)), make([][]byte, 0, len(union))
	for _, prefix := range union {
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
		lower = append(lower, []byte(start))
		upper = append(upper, end)
	}
	rows, err := r.sql.QueryContext(ctx, `SELECT DISTINCT ON (key) value FROM plug_postgres.states WHERE revision<=$3 AND EXISTS(SELECT 1 FROM unnest($1::bytea[],$2::bytea[]) AS requested(lo,hi) WHERE key>=lo AND key<hi) ORDER BY key,revision DESC`, lower, upper, decimal(r.at))
	if err != nil {
		return nil, err
	}
	byKey := map[string]driver.DerivedRow{}
	for rows.Next() {
		var raw []byte
		var state driver.LockState
		if err = rows.Scan(&raw); err != nil {
			break
		}
		if err = decode(raw, &state); err != nil {
			break
		}
		driver.ReportReadWork(ctx, driver.ReadPrefixStates, 1)
		driver.ReportReadWork(ctx, driver.ReadPrefixBytes, int64(len(raw)))
		if state.Present && state.Derived != nil {
			byKey[state.Derived.Key] = *state.Derived
		}
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	if r.writer != nil {
		committed := make(map[string]driver.DerivedRow, len(byKey))
		for key, row := range byKey {
			committed[key] = row.Clone()
		}
		base := make([]driver.DerivedRow, 0, len(committed))
		for _, row := range committed {
			base = append(base, row)
		}
		if err := r.selectionAnnotations(ctx, base, table, true); err != nil {
			return nil, err
		}
		for _, row := range base {
			byKey[row.Key] = row
			committed[row.Key] = row.Clone()
		}
		for _, state := range r.writer.derived {
			if state.Key.Derived.Table != table {
				continue
			}
			matches := false
			for _, p := range union {
				if strings.HasPrefix(state.Key.Derived.Key, p) {
					matches = true
					break
				}
			}
			if !matches {
				continue
			}
			delete(byKey, state.Key.Derived.Key)
			if state.Present && state.Derived != nil {
				row := state.Derived.Clone()
				if row.Index == nil {
					if base, ok := committed[state.Derived.Key]; ok && base.Index != nil && bytes.Equal(base.Value, row.Value) && !r.writer.selectionSourceChanged(base.Index.Source.Key) {
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
	if r.writer == nil {
		if err := r.selectionAnnotations(ctx, result, table); err != nil {
			return nil, err
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

var _ driver.DerivedPrefixReader = (*reader)(nil)
var _ driver.DerivedPrefixReader = (*transaction)(nil)

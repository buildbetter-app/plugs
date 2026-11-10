package sourceindex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// BackfillPage receives the already-decoded source page inside its fenced page
// publication. Optional selection work must not turn graph-ready into an early
// return. This hook publishes staging rows only; activation belongs to the
// owning engine's final transaction/fence after EVERY source page completes.
type BackfillPage func(context.Context, SQL, []driver.Record, []driver.GraphSourceMetadata) error

// BackfillSelection supplies all three lifecycle steps. Clear discards earlier
// unpublished staging (including interrupted builds), Page never certifies, and
// Activate runs in the final fenced graph/selection activation transaction.
// Backfill isolates every optional SQL step. Unavailable/unsupported selection
// stops this lifecycle without blocking graph activation or certifying selection.
// Ready family history is never cleared; a refusal cannot publish partial markers.
type BackfillSelection struct {
	Clear    func(context.Context, BackfillPublication) error
	Page     BackfillPage
	Activate func(context.Context, SQL) error
	refusal  *error
}

// SelectionSourcePage reads one source page in the engine's pinned short-step
// snapshot. It does not consult graph readiness; a selection-only build must
// still enumerate every source. The caller owns the returned envelope bodies.
func (d Dialect) SelectionSourcePage(ctx context.Context, q SQL, after string) ([]driver.Record, error) {
	return d.sourcePage(ctx, q, after)
}

// SelectionRangePage reads ALL opaque ranges for one page in ONE statement.
// Only driver LockState envelopes are decoded here, never source document JSON.
// The source page and ranges must use the same snapshot/revision. Each range
// has a limit+1 and the statement has a total budget+1; overflow yields no rows.
func (d Dialect) SelectionRangePage(ctx context.Context, q SQL, at model.Revision, inputs []driver.DerivedRowRange, budget int) ([][]driver.DerivedRow, error) {
	if budget <= 0 || len(inputs) > driver.SelectionBackfillPageSize {
		return nil, fmt.Errorf("%w: invalid selection range page", driver.ErrSelectionUnavailable)
	}
	result := make([][]driver.DerivedRow, len(inputs))
	if len(inputs) == 0 {
		return result, nil
	}
	atNumber, err := at.Uint64()
	if err != nil {
		return nil, err
	}
	b := bindings{d: d}
	values := make([]string, len(inputs))
	for i, input := range inputs {
		if input.Max <= 0 || input.Max > budget {
			return nil, fmt.Errorf("%w: invalid selection range bound", driver.ErrSelectionUnavailable)
		}
		start, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: input.Table, Key: string(input.Prefix) + "!"}})
		if err != nil {
			return nil, err
		}
		start = strings.TrimSuffix(start, "!")
		end := []byte(start)
		for j := len(end) - 1; j >= 0; j-- {
			if end[j] < 255 {
				end[j]++
				end = end[:j+1]
				break
			}
		}
		fields := []string{b.add(i), b.add(d.key(start)), b.add(d.key(string(end))), b.add(input.Max)}
		if d == Postgres {
			for j, typ := range []string{"integer", "bytea", "bytea", "integer"} {
				fields[j] += "::" + typ
			}
		}
		values[i] = "(" + strings.Join(fields, ",") + ")"
	}
	var query string
	if d == Postgres {
		// Presence is a driver envelope field. Apply it after latest-version
		// selection, so an earlier live value never resurrects a tombstone.
		revision := b.add(at.String())
		query = `WITH requested(slot,lo,hi,maximum) AS (VALUES ` + strings.Join(values, ",") + `)
SELECT requested.slot, live.key, live.value FROM requested CROSS JOIN LATERAL (
 SELECT latest.key,latest.value FROM (
  SELECT DISTINCT ON (key) key,value FROM plug_postgres.states
  WHERE key>=requested.lo AND key<requested.hi AND revision<=` + revision + `::numeric ORDER BY key,revision DESC
 ) latest WHERE convert_from(latest.value,'UTF8')::jsonb->>'Present'='true'
 LIMIT requested.maximum+1
) live ORDER BY requested.slot LIMIT ` + b.add(budget+1)
	} else {
		revision := b.add(fmt.Sprintf("%020d", atNumber))
		newerRevision := b.add(fmt.Sprintf("%020d", atNumber))
		query = `WITH requested(slot,lo,hi,maximum) AS (VALUES ` + strings.Join(values, ",") + `), current AS (
 SELECT requested.slot,requested.maximum,h.key,h.data,
 ROW_NUMBER() OVER (PARTITION BY requested.slot ORDER BY h.key) AS n
 FROM requested JOIN key_history h ON h.key>=requested.lo AND h.key<requested.hi
 WHERE h.rev<=` + revision + ` AND NOT EXISTS (
  SELECT 1 FROM key_history newer WHERE newer.key=h.key AND newer.rev>h.rev AND newer.rev<=` + newerRevision + `
 ) AND json_extract(h.data,'$.Present')=1
)
SELECT slot,key,data FROM current WHERE n<=maximum+1 ORDER BY slot,key LIMIT ` + b.add(budget+1)
	}
	rows, err := q.QueryContext(ctx, query, b.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	total := 0
	for rows.Next() {
		var slot int
		var rawKey, raw []byte
		if err := rows.Scan(&slot, &rawKey, &raw); err != nil {
			return nil, err
		}
		total++
		if slot < 0 || slot >= len(inputs) || total > budget || len(result[slot]) >= inputs[slot].Max {
			return nil, fmt.Errorf("%w: selection range/page overflow", driver.ErrSelectionUnavailable)
		}
		var state driver.LockState
		if err := json.Unmarshal(raw, &state); err != nil {
			return nil, err
		}
		input := inputs[slot]
		encoded, keyErr := driver.EncodeLockKey(state.Key)
		revision, revisionErr := state.Revision.Uint64()
		if keyErr != nil || !bytes.Equal(rawKey, []byte(encoded)) || revisionErr != nil || revision > atNumber || !state.Present || state.Key.Kind != driver.LockDerived || state.Key.Derived == nil || state.Key.Derived.Table != input.Table || state.Derived == nil || state.Key.Derived.Key != state.Derived.Key || !bytes.HasPrefix([]byte(state.Derived.Key), input.Prefix) {
			return nil, fmt.Errorf("%w: selection range envelope mismatch", driver.ErrSelectionUnavailable)
		}
		result[slot] = append(result[slot], state.Derived.Clone())
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

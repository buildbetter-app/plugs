package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These are exact requested-key reads on Commit's existing connection. Each
// batch stays below SQLite's older 999-variable limit; no store census or
// retained-history range is substituted for a latest-version point lookup.
const readyOrderPhysicalReadBatch = 128

type readyOrderPhysicalImage struct {
	present bool
	data    []byte
}

func readyOrderReadPhysical(ctx context.Context, c *sql.Conn, keys []string, history bool) (map[string]readyOrderPhysicalImage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	images := make(map[string]readyOrderPhysicalImage, len(keys))
	for _, key := range keys {
		if _, duplicate := images[key]; duplicate {
			return nil, driver.ErrUnsupported
		}
		images[key] = readyOrderPhysicalImage{}
	}
	for start := 0; start < len(keys); start += readyOrderPhysicalReadBatch {
		end := min(start+readyOrderPhysicalReadBatch, len(keys))
		args := make([]any, end-start)
		for i, key := range keys[start:end] {
			args[i] = key
		}
		query := `WITH requested(key) AS (VALUES ` + strings.TrimSuffix(strings.Repeat("(?),", len(args)), ",") + `)
			SELECT requested.key, image.key IS NOT NULL, image.data FROM requested
			LEFT JOIN records image ON image.key=requested.key`
		if history {
			query = `WITH requested(key) AS (VALUES ` + strings.TrimSuffix(strings.Repeat("(?),", len(args)), ",") + `)
				SELECT requested.key, image.key IS NOT NULL, image.data FROM requested
				LEFT JOIN key_history image ON image.key=requested.key AND image.rev=(
				 SELECT rev FROM key_history WHERE key=requested.key ORDER BY rev DESC LIMIT 1)`
		}
		rows, err := c.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		seen := make(map[string]bool, len(args))
		for rows.Next() {
			var key string
			var image readyOrderPhysicalImage
			if err = rows.Scan(&key, &image.present, &image.data); err != nil {
				break
			}
			if _, requested := images[key]; !requested || seen[key] {
				err = driver.ErrUnsupported
				break
			}
			seen[key] = true
			images[key] = image
		}
		if err == nil {
			err = rows.Err()
		}
		closeErr := rows.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if len(seen) != len(args) {
			return nil, driver.ErrUnsupported
		}
	}
	return images, nil
}

func readyOrderReadStates(ctx context.Context, c *sql.Conn, keys []driver.LockKey) (map[string]driver.LockState, error) {
	ids := make([]string, len(keys))
	for i, key := range keys {
		ids[i] = keyID(key)
	}
	images, err := readyOrderReadPhysical(ctx, c, ids, true)
	if err != nil {
		return nil, err
	}
	states := make(map[string]driver.LockState, len(keys))
	for i, key := range keys {
		state := driver.LockState{Key: key, Revision: model.Natural64(0)}
		if image := images[ids[i]]; image.present {
			if json.Unmarshal(image.data, &state) != nil {
				return nil, driver.ErrUnsupported
			}
		}
		states[ids[i]] = state
	}
	return states, nil
}

// readyOrderAcquiredStates reuses the exact same-connection observations made
// while the complete lock census was acquired. A Ready publication's Before
// image is drawn from that census, so another latest-row query cannot observe
// a different state while the update locks are held. Transactions created by
// direct driver callers may not retain this cache; those callers keep the
// original bounded physical read below.
func (t *transaction) readyOrderAcquiredStates(keys []driver.LockKey) (map[string]driver.LockState, bool) {
	if len(keys) == 0 || len(t.acquiredStates) == 0 {
		return nil, false
	}
	states := make(map[string]driver.LockState, len(keys))
	for _, key := range keys {
		id := keyID(key)
		state, ok := t.acquiredStates[id]
		if !ok {
			return nil, false
		}
		states[id] = state
	}
	return states, true
}

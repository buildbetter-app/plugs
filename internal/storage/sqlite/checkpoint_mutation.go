package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"math"
	"sort"
)

func (t *transaction) StageCheckpoint(ctx context.Context, key string, expected uint64, payload []byte) error {
	if expected == math.MaxUint64 {
		return fmt.Errorf("checkpoint generation exhausted")
	}
	if err := t.ExpectCheckpoint(ctx, key, expected); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.checkpointWrites == nil {
		t.checkpointWrites = map[string]driver.Checkpoint{}
	}
	if prior, ok := t.checkpointWrites[key]; ok && (prior.Generation != expected+1 || !bytes.Equal(prior.Payload, payload)) {
		return driver.ErrCheckpointConflict
	}
	t.checkpointWrites[key] = driver.Checkpoint{Generation: expected + 1, Payload: append([]byte(nil), payload...)}
	return nil
}

func (t *transaction) publishCheckpoints(ctx context.Context, c *sql.Conn) error {
	keys := make([]string, 0, len(t.checkpointWrites))
	for key := range t.checkpointWrites {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := t.checkpointWrites[key]
		if _, err := c.ExecContext(ctx, `INSERT INTO checkpoints(key,generation,payload) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET generation=excluded.generation,payload=excluded.payload`, key, revKey(value.Generation), value.Payload); err != nil {
			return err
		}
	}
	return nil
}

var _ driver.CheckpointWriteTx = (*transaction)(nil)

package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Both checkpoint publication and guarded graph Commit take this same key
// lock. Row locks alone cannot protect the first save of an absent key.
func lockCheckpoint(ctx context.Context, tx *sql.Tx, key string) error {
	digest := sha256.Sum256([]byte("plug/checkpoint/" + key))
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(binary.BigEndian.Uint64(digest[:8])))
	return err
}

func (t *transaction) ExpectCheckpoint(ctx context.Context, key string, generation uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if _, err := checkpointKey(key); err != nil {
		return err
	}
	if t.checkpointGuards == nil {
		t.checkpointGuards = make(map[string]uint64)
	}
	if prior, exists := t.checkpointGuards[key]; exists && prior != generation {
		return driver.ErrCheckpointConflict
	}
	t.checkpointGuards[key] = generation
	return nil
}

func (t *transaction) checkCheckpointGuards(ctx context.Context) error {
	keys := make([]string, 0, len(t.checkpointGuards))
	for key := range t.checkpointGuards {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := lockCheckpoint(ctx, t.sql, key); err != nil {
			return err
		}
		var text string
		err := t.sql.QueryRowContext(ctx, `SELECT generation::text FROM plug_postgres.checkpoints WHERE key=$1`, []byte(key)).Scan(&text)
		var current uint64
		if err == nil {
			current, err = parseCounter(text)
		} else if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
		if err != nil {
			return err
		}
		if current != t.checkpointGuards[key] {
			return driver.ErrCheckpointConflict
		}
	}
	return nil
}

var _ driver.CheckpointGuardTx = (*transaction)(nil)

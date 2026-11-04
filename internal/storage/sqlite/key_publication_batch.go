package sqlite

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Three bindings per row keep every statement below the older 999-variable
// SQLite limit. Every row retains its original JSON envelope and row triggers;
// all batches run on Commit's same fenced immediate connection.
const keyPublicationBatchSize = 128

type keyPublicationBatch struct {
	args []any
}

func (b *keyPublicationBatch) add(ctx context.Context, c selectionPublicationExecutor, state driver.LockState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	b.args = append(b.args, keyID(state.Key), revKey(mustUint(state.Revision)), data)
	if len(b.args) == 3*keyPublicationBatchSize {
		return b.flush(ctx, c)
	}
	return nil
}

func (b *keyPublicationBatch) flush(ctx context.Context, c selectionPublicationExecutor) error {
	if len(b.args) == 0 {
		return nil
	}
	query := `INSERT INTO key_history(key,rev,data) VALUES ` +
		strings.TrimSuffix(strings.Repeat("(?,?,?),", len(b.args)/3), ",")
	if _, err := c.ExecContext(ctx, query, b.args...); err != nil {
		return err
	}
	b.args = b.args[:0]
	return nil
}

func publishDerivedKeys(ctx context.Context, c selectionPublicationExecutor, revision model.Revision, derived map[string]driver.LockState) error {
	var batch keyPublicationBatch
	for _, state := range derived {
		state.Revision = revision
		if err := batch.add(ctx, c, state); err != nil {
			return err
		}
	}
	return batch.flush(ctx, c)
}

func publishBumpKeys(ctx context.Context, c selectionPublicationExecutor, revision model.Revision, bumps map[string]driver.LockKey) error {
	var batch keyPublicationBatch
	for _, key := range bumps {
		state := driver.LockState{Key: key, Revision: revision, Present: true}
		if err := batch.add(ctx, c, state); err != nil {
			return err
		}
	}
	return batch.flush(ctx, c)
}

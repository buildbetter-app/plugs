package sqlite

import (
	"context"
	"database/sql"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (t *transaction) ExpectCheckpoint(ctx context.Context, key string, generation uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
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

// The immediate transaction also owns checkpoint publication, so observing
// an absent key here is as strong a guard as observing an existing row.
func (t *transaction) checkCheckpointGuards(ctx context.Context, c *sql.Conn) error {
	for key, generation := range t.checkpointGuards {
		current, err := loadCheckpoint(ctx, c, key)
		if err != nil {
			return err
		}
		if current.Generation != generation {
			return driver.ErrCheckpointConflict
		}
	}
	return nil
}

var _ driver.CheckpointGuardTx = (*transaction)(nil)

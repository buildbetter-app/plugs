package memory

import (
	"context"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (t *transaction) ExpectCheckpoint(ctx context.Context, key string, generation uint64) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
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

// Commit owns the same store mutex as SaveCheckpoint, including absent keys.
func (t *transaction) checkCheckpointGuards() error {
	for key, generation := range t.checkpointGuards {
		if t.store.checkpoints[key].Generation != generation {
			return driver.ErrCheckpointConflict
		}
	}
	return nil
}

var _ driver.CheckpointGuardTx = (*transaction)(nil)

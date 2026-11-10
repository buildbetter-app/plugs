package memory

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"math"

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

func (t *transaction) StageCheckpoint(ctx context.Context, key string, expected uint64, payload []byte) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.offlineHistory != nil && t.sourcePlaneImport == nil {
		return fmt.Errorf("%w: offline history does not publish checkpoint stages", driver.ErrUnsupported)
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return err
	}
	if expected == math.MaxUint64 {
		return fmt.Errorf("memory: checkpoint generation exhausted")
	}
	if guard, ok := t.checkpointGuards[key]; ok && guard != expected {
		return driver.ErrCheckpointConflict
	}
	if prior, ok := t.checkpointWrites[key]; ok && (prior.Generation != expected+1 || !bytes.Equal(prior.Payload, payload)) {
		return driver.ErrCheckpointConflict
	}
	value := driver.Checkpoint{Generation: expected + 1, Payload: bytes.Clone(payload)}
	if err := t.check(ctx); err != nil {
		return err
	}
	if t.checkpointGuards == nil {
		t.checkpointGuards = make(map[string]uint64)
	}
	if t.checkpointWrites == nil {
		t.checkpointWrites = make(map[string]driver.Checkpoint)
	}
	t.checkpointGuards[key] = expected
	t.checkpointWrites[key] = value
	return nil
}

// The caller owns the store mutex through final publication. Staged byte
// arrays are already detached; clone again so a completed transaction never
// retains writable aliases into committed checkpoint state.
func (t *transaction) prepareCheckpointPublication() map[string]driver.Checkpoint {
	if len(t.checkpointWrites) == 0 {
		return t.store.checkpoints
	}
	next := maps.Clone(t.store.checkpoints)
	for key, value := range t.checkpointWrites {
		value.Payload = bytes.Clone(value.Payload)
		next[key] = value
	}
	return next
}

var _ driver.CheckpointWriteTx = (*transaction)(nil)

package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *Store) LoadCheckpoint(ctx context.Context, key string) (driver.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return driver.Checkpoint{}, err
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return driver.Checkpoint{}, err
	}
	value := s.checkpoints[key]
	value.Payload = bytes.Clone(value.Payload)
	return value, nil
}

func (s *Store) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte) (driver.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return driver.Checkpoint{}, err
	}
	if err := s.checkLease(lease); err != nil {
		return driver.Checkpoint{}, err
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return driver.Checkpoint{}, err
	}
	current := s.checkpoints[key]
	if current.Generation != expected {
		return driver.Checkpoint{}, driver.ErrCheckpointConflict
	}
	if expected == math.MaxUint64 {
		return driver.Checkpoint{}, fmt.Errorf("memory: checkpoint generation exhausted")
	}
	value := driver.Checkpoint{Generation: expected + 1, Payload: bytes.Clone(payload)}
	// Copy before rechecking the deadline, since payloads may be large.
	if err := s.checkLease(lease); err != nil {
		return driver.Checkpoint{}, err
	}
	s.checkpoints[key] = value
	value.Payload = bytes.Clone(value.Payload)
	return value, nil
}

func (s *Store) FinalizeResponse(ctx context.Context, lease driver.ActiveLease, principal, agent, method, key string, revision model.Revision, warnings []model.Warning) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := s.checkLease(lease); err != nil {
		return err
	}
	if err := revision.Validate(); err != nil {
		return err
	}
	id := normalizedScope(principal, agent, method, key)
	current, ok := s.state.replays[id]
	if !ok || !s.now().Before(current.at.Add(driver.IdempotencyWindow)) {
		return driver.ErrNotFound
	}
	if current.value.Changeset.Revision.String() != revision.String() {
		return driver.ErrConflict
	}
	encoded, err := json.Marshal(warnings)
	if err != nil {
		return err
	}
	if current.finalized {
		previous, err := json.Marshal(current.post)
		if err != nil {
			return err
		}
		if !bytes.Equal(previous, encoded) {
			return driver.ErrConflict
		}
		return nil
	}
	current, err = copyReplay(current)
	if err != nil {
		return err
	}
	current.post, err = cloneJSON(warnings)
	if err != nil {
		return err
	}
	current.value.Commit.Warnings = append(current.value.Commit.Warnings, current.post...)
	current.finalized = true
	next := s.state.fork()
	next.replays[id] = current
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.state = next
	return nil
}

func copyReplay(value replay) (replay, error) {
	copy, err := cloneJSON(value.value)
	if err != nil {
		return replay{}, err
	}
	value.value = copy
	return value, nil
}

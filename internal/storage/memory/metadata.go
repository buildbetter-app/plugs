package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"

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
	return s.saveCheckpoint(ctx, lease, key, expected, payload)
}

func (s *Store) SaveCheckpointAtHead(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte, head driver.CoreSnapshotExpectation) (driver.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return driver.Checkpoint{}, err
	}
	if err := s.checkLease(lease); err != nil {
		return driver.Checkpoint{}, err
	}
	if err := head.Check(s.state.meta); err != nil {
		return driver.Checkpoint{}, err
	}
	return s.saveCheckpoint(ctx, lease, key, expected, payload)
}

func (*Store) WorkflowCheckpointLifetime() driver.CheckpointLifetime {
	return driver.CheckpointLifetimeProcess
}

type sourcePlaneSnapshot struct {
	*reader
	checkpoints map[string]driver.Checkpoint
}

// Core state and the complete requested namespace set are captured while
// holding the SAME publication mutex. Historical core plus current provider
// state is unsupported, including at a repeated zero graph head.
func (s *Store) ReadSourcePlaneSnapshot(ctx context.Context, view driver.View, keys []string) (driver.SourcePlaneSnapshot, error) {
	return s.readCheckpointSnapshot(ctx, view, keys, false)
}

func (s *Store) ReadCheckpointInventory(ctx context.Context, view driver.View) (driver.CheckpointInventory, error) {
	return s.readCheckpointSnapshot(ctx, view, nil, true)
}

func (s *Store) readCheckpointSnapshot(ctx context.Context, view driver.View, keys []string, all bool) (*sourcePlaneSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(keys) > driver.SourcePlaneMaxListEntries {
		return nil, driver.ErrUnsupported
	}
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if view.At != nil && view.At.String() != s.state.meta.Head.String() {
		return nil, driver.ErrCheckpointConflict
	}
	if all {
		if len(s.checkpoints) > driver.SourcePlaneMaxListEntries {
			return nil, driver.ErrUnsupported
		}
		keys = make([]string, 0, len(s.checkpoints))
		for key := range s.checkpoints {
			keys = append(keys, key)
		}
		sort.Strings(keys)
	}
	checkpoints := make(map[string]driver.Checkpoint, len(keys))
	totalBytes := 0
	for i, key := range keys {
		if err := driver.ValidateCheckpointKey(key); err != nil {
			return nil, err
		}
		if i > 0 && keys[i-1] >= key {
			return nil, driver.ErrCheckpointConflict
		}
		value := s.checkpoints[key]
		totalBytes += len(value.Payload)
		if all && totalBytes > driver.SourcePlaneMaxPlanBytes {
			return nil, driver.ErrUnsupported
		}
		value.Payload = bytes.Clone(value.Payload)
		checkpoints[key] = value
	}
	r, err := newReader(s.state, view, s.now)
	if err != nil {
		return nil, err
	}
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	return &sourcePlaneSnapshot{reader: r, checkpoints: checkpoints}, nil
}

func (r *sourcePlaneSnapshot) CheckpointKeys() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state == nil {
		return nil, driver.ErrTxClosed
	}
	if err := r.check(context.Background()); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(r.checkpoints))
	for key := range r.checkpoints {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (r *sourcePlaneSnapshot) SourcePlaneCheckpoint(key string) (driver.Checkpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(context.Background()); err != nil {
		return driver.Checkpoint{}, err
	}
	value, ok := r.checkpoints[key]
	if !ok {
		return driver.Checkpoint{}, driver.ErrUnsupported
	}
	value.Payload = bytes.Clone(value.Payload)
	return value, nil
}

func (r *sourcePlaneSnapshot) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = nil
	r.now = nil
	r.checkpoints = nil
	return nil
}

var _ driver.WorkflowCheckpointStore = (*Store)(nil)
var _ driver.CheckpointInventoryStore = (*Store)(nil)

func (s *Store) SaveCheckpointIfRecord(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte, ref driver.Ref, revision model.Revision, incarnation string) (driver.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return driver.Checkpoint{}, err
	}
	if err := s.checkLease(lease); err != nil {
		return driver.Checkpoint{}, err
	}
	if incarnation == "" || incarnation != s.state.meta.Incarnation {
		return driver.Checkpoint{}, driver.ErrIncarnationChanged{From: incarnation, To: s.state.meta.Incarnation}
	}
	id, err := recordKey(ref)
	if err != nil {
		return driver.Checkpoint{}, err
	}
	if err := revision.Validate(); err != nil {
		return driver.Checkpoint{}, err
	}
	row, present := s.state.records[id]
	if !present || row.Revision.String() != revision.String() {
		return driver.Checkpoint{}, driver.ErrConflict
	}
	return s.saveCheckpoint(ctx, lease, key, expected, payload)
}

func (s *Store) saveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, expected uint64, payload []byte) (driver.Checkpoint, error) {
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
	if err := s.check(ctx); err != nil {
		return driver.Checkpoint{}, err
	}
	if err := s.checkLease(lease); err != nil {
		return driver.Checkpoint{}, err
	}
	if err := s.withLeasePublication(lease, func() {
		s.checkpoints[key] = value
	}); err != nil {
		return driver.Checkpoint{}, err
	}
	value.Payload = bytes.Clone(value.Payload)
	return value, nil
}

var _ driver.RecordGuardedCheckpointStore = (*Store)(nil)

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
	if current.value.Commit.Revision.String() != revision.String() {
		return driver.ErrConflict
	}
	// Nil and empty post lists describe the same outcome, including a receipt
	// initially published final, whose private post outcome is empty.
	if warnings == nil {
		warnings = []model.Warning{}
	}
	encoded, err := json.Marshal(warnings)
	if err != nil {
		return err
	}
	if !current.value.ResponsePending {
		post := current.post
		if post == nil {
			post = []model.Warning{}
		}
		previous, err := json.Marshal(post)
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
	if err := s.check(ctx); err != nil {
		return err
	}
	current.value.Commit.Warnings = append(current.value.Commit.Warnings, current.post...)
	current.value.ResponsePending = false
	next := s.state.fork()
	next.replays[id] = current
	return s.withLeasePublication(lease, func() { s.state = next })
}

func copyReplay(value replay) (replay, error) {
	copy, err := copyIdempotency(value.value)
	if err != nil {
		return replay{}, err
	}
	value.value = copy
	return value, nil
}

func copyIdempotency(value driver.IdempotencyRecord) (driver.IdempotencyRecord, error) {
	response, err := cloneJSON(value.Commit)
	if err != nil {
		return driver.IdempotencyRecord{}, err
	}
	value.Commit = response
	// A no-op receipt has no changeset. The zero value is an absence marker,
	// not a model changeset to serialize or expose through the log API.
	if value.Changeset.Kind != "" {
		entry, err := cloneJSON(value.Changeset)
		if err != nil {
			return driver.IdempotencyRecord{}, err
		}
		value.Changeset = entry
	}
	return value, nil
}

package pipeline

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type readSet struct {
	mu     sync.Mutex
	states map[string]driver.LockState
}

type snapshotReader struct {
	reader          driver.Reader
	reads           *readSet
	mu              sync.Mutex
	points, queries int
	bounded         bool
}

var _ port.SnapshotReader = (*snapshotReader)(nil)

func (r *snapshotReader) Snapshot() driver.View { return r.reader.Snapshot() }
func (r *snapshotReader) child() *snapshotReader {
	return &snapshotReader{reader: r.reader, reads: r.reads, bounded: true}
}

func (r *snapshotReader) budget(points, queries int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.points += points
	r.queries += queries
	if r.bounded && (r.points > 32 || r.queries > 4) {
		return fmt.Errorf("subscriber snapshot read budget exceeded")
	}
	return nil
}

func (r *snapshotReader) observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	states, err := r.reader.Observe(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(states) != len(keys) {
		return nil, fmt.Errorf("driver omitted snapshot observations")
	}
	r.reads.mu.Lock()
	defer r.reads.mu.Unlock()
	for i, state := range states {
		key, err := driver.EncodeLockKey(keys[i])
		if err != nil {
			return nil, err
		}
		actual, err := driver.EncodeLockKey(state.Key)
		if err != nil || key != actual {
			return nil, fmt.Errorf("driver returned the wrong observation key")
		}
		r.reads.states[key] = state
	}
	return states, nil
}

func (r *snapshotReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := r.budget(len(keys), 0); err != nil {
		return nil, err
	}
	return r.observe(ctx, keys)
}

func (r *snapshotReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	if err := r.budget(len(refs), 0); err != nil {
		return nil, err
	}
	keys := map[string]driver.LockKey{}
	for _, ref := range refs {
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return nil, err
		}
		keys[encoded] = key
	}
	if _, err := r.observe(ctx, sortedKeys(keys)); err != nil {
		return nil, err
	}
	return r.reader.Get(ctx, refs)
}

// A general query conservatively depends on the whole graph predicate.
// Every supported pipeline write bumps graph/blocking. This catches inserts,
// deletes, filter changes and misses without an unbounded point-read budget.
func (r *snapshotReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	if err := r.budget(0, 1); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	if q.AsOf != nil && q.AsOf.String() != r.Snapshot().At.String() {
		return driver.Page[driver.Record]{}, fmt.Errorf("snapshot query cannot change revision")
	}
	if _, err := r.observe(ctx, []driver.LockKey{driver.GraphBlockingSentinelLock}); err != nil {
		return driver.Page[driver.Record]{}, err
	}
	return r.reader.Query(ctx, q)
}

func sortedKeys(keys map[string]driver.LockKey) []driver.LockKey {
	names := make([]string, 0, len(keys))
	for name := range keys {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]driver.LockKey, 0, len(keys))
	for _, name := range names {
		result = append(result, keys[name])
	}
	return result
}

func (r *snapshotReader) locks(writes map[string]*pending, sentinels []driver.Sentinel) (driver.LockSet, error) {
	r.reads.mu.Lock()
	defer r.reads.mu.Unlock()
	byKey := map[string]driver.LockRequest{}
	add := func(key driver.LockKey, mode driver.LockMode) error {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return err
		}
		if prior, ok := byKey[encoded]; ok && prior.Mode == driver.LockUpdate {
			return nil
		}
		byKey[encoded] = driver.LockRequest{Key: key, Mode: mode}
		return nil
	}
	for _, state := range r.reads.states {
		if err := add(state.Key, driver.LockShare); err != nil {
			return nil, err
		}
	}
	for _, p := range writes {
		ref := p.ref
		if err := add(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, driver.LockUpdate); err != nil {
			return nil, err
		}
	}
	for _, s := range sentinels {
		if err := add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: s}, driver.LockUpdate); err != nil {
			return nil, err
		}
	}
	names := make([]string, 0, len(byKey))
	for name := range byKey {
		names = append(names, name)
	}
	sort.Strings(names)
	locks := make(driver.LockSet, 0, len(names))
	for _, name := range names {
		locks = append(locks, byKey[name])
	}
	return locks, driver.ValidateLockSet(locks)
}

func (r *snapshotReader) check(states []driver.LockState) error {
	r.reads.mu.Lock()
	defer r.reads.mu.Unlock()
	seen := map[string]bool{}
	for _, state := range states {
		key, err := driver.EncodeLockKey(state.Key)
		if err != nil {
			return err
		}
		seen[key] = true
		if old, ok := r.reads.states[key]; ok && (old.Revision.String() != state.Revision.String() || old.Present != state.Present) {
			return errStale
		}
	}
	for key := range r.reads.states {
		if !seen[key] {
			return fmt.Errorf("driver omitted a required lock")
		}
	}
	return nil
}

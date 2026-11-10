package pipeline

import (
	"bytes"
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
	ordered := sortedKeys(keys)
	states, err := r.observe(ctx, ordered)
	if err != nil {
		return nil, err
	}
	// Use only this call's fresh observation, never a retained read-set value:
	// transaction readers may have a newer locked/staged view on a later call.
	// Presence (including a tombstone's nonzero revision) is authoritative;
	// drivers which omit a complete live Record still use their normal Get.
	values := make(map[string]driver.Record, len(states))
	fallback := make([]model.Ref, 0)
	for i, state := range states {
		if !state.Present {
			continue
		}
		encoded, _ := driver.EncodeLockKey(ordered[i])
		if state.Record != nil && state.Record.Kind != "" && len(state.Record.Body) != 0 {
			recordKey, encodeErr := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &state.Record.Ref})
			if encodeErr == nil && recordKey == encoded && state.Record.Revision.String() == state.Revision.String() {
				values[encoded] = *state.Record
				continue
			}
		}
		fallback = append(fallback, *ordered[i].Ref)
	}
	if len(fallback) != 0 {
		rows, err := r.reader.Get(ctx, fallback)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			encoded, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &row.Ref})
			if err != nil {
				return nil, err
			}
			values[encoded] = row
		}
	}
	result := make([]driver.Record, 0, len(refs))
	for _, ref := range refs {
		encoded, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		if row, exists := values[encoded]; exists {
			row.Body = bytes.Clone(row.Body)
			result = append(result, row)
		}
	}
	return result, nil
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

func (r *snapshotReader) locks(writes map[string]*pending, sentinels []driver.Sentinel, extra ...driver.LockKey) (driver.LockSet, error) {
	return r.locksForSelected(writes, sentinels, "", extra...)
}

// claimNextLocks changes only the one selected bead's lock mode. Its point
// read, a subscriber read, and the write may all declare that same key; the
// selected skip mode must survive their deduplication in the global order.
func (r *snapshotReader) claimNextLocks(writes map[string]*pending, sentinels []driver.Sentinel, extra ...driver.LockKey) (driver.LockSet, error) {
	if len(writes) != 1 {
		return nil, fmt.Errorf("claimNext requires exactly one selected write")
	}
	var selected string
	for _, pending := range writes {
		if pending.kind != "bead" {
			return nil, fmt.Errorf("claimNext selected write is not a bead")
		}
		ref := pending.ref
		var err error
		selected, err = driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		if err != nil {
			return nil, err
		}
	}
	return r.locksForSelected(writes, sentinels, selected, extra...)
}

func (r *snapshotReader) locksForSelected(writes map[string]*pending, sentinels []driver.Sentinel, selected string, extra ...driver.LockKey) (driver.LockSet, error) {
	r.reads.mu.Lock()
	defer r.reads.mu.Unlock()
	byKey := map[string]driver.LockRequest{}
	add := func(key driver.LockKey, mode driver.LockMode) error {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return err
		}
		if selected != "" && encoded == selected {
			mode = driver.LockSkipLocked
		}
		if prior, ok := byKey[encoded]; ok && (prior.Mode == driver.LockUpdate || prior.Mode == driver.LockSkipLocked) {
			return nil
		}
		byKey[encoded] = driver.LockRequest{Key: key, Mode: mode}
		return nil
	}
	// readSet.observe has already validated each state key and stored the exact
	// encoded identity. Reuse that identity here instead of re-encoding every
	// read during a large write's lock-plan construction. Update requests below
	// still replace a share entry through the same add() path.
	for encoded, state := range r.reads.states {
		if prior, ok := byKey[encoded]; ok && prior.Mode == driver.LockUpdate {
			continue
		}
		byKey[encoded] = driver.LockRequest{Key: state.Key, Mode: driver.LockShare}
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
	for _, key := range extra {
		if err := add(key, driver.LockUpdate); err != nil {
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

// Only the selected SKIP LOCKED key may be omitted. A driver dropping a
// required read/fact/sentinel/derived lock is still an error. This runs before
// Head or staging, so a skipped candidate cannot mint a receipt or revision.
func checkSelectedLocks(locks driver.LockSet, states []driver.LockState) error {
	seen := make(map[string]bool, len(states))
	wanted := make(map[string]driver.LockMode, len(locks))
	for _, request := range locks {
		key, err := driver.EncodeLockKey(request.Key)
		if err != nil {
			return err
		}
		wanted[key] = request.Mode
	}
	for _, state := range states {
		key, err := driver.EncodeLockKey(state.Key)
		if err != nil {
			return err
		}
		if _, ok := wanted[key]; !ok || seen[key] {
			return fmt.Errorf("driver returned an unexpected or duplicate lock")
		}
		seen[key] = true
	}
	missingSelected := 0
	for key, mode := range wanted {
		if seen[key] {
			continue
		}
		if mode != driver.LockSkipLocked {
			return fmt.Errorf("driver failed to acquire the complete required lock set")
		}
		missingSelected++
	}
	if missingSelected == 1 {
		return failure("write_conflict", "claimNext candidate is locked")
	}
	if missingSelected != 0 {
		return fmt.Errorf("driver omitted more than one selected lock")
	}
	return nil
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

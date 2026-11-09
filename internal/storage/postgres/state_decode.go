package postgres

import (
	"bytes"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	observedStateDecodeMaxBytes   = 8 << 20
	observedStateDecodeMaxEntries = 8192
)

// JSON decoding overlays the requested key when stored fields are absent.
// Keep its exact value, including Ref's selected arm and literal branch, in
// the identity. Equal raw bytes with different initial keys are not equivalent.
type observedStateDecodeKey struct {
	raw                string
	kind               driver.LockKeyKind
	fact               driver.ReadFact
	sentinel           driver.Sentinel
	ref                model.Ref
	derived            driver.DerivedRowKey
	hasRef, hasDerived bool
}

func observedStateKey(raw []byte, key driver.LockKey) observedStateDecodeKey {
	cache := observedStateDecodeKey{raw: string(raw), kind: key.Kind, fact: key.Fact, sentinel: key.Sentinel}
	if key.Ref != nil {
		cache.ref, cache.hasRef = *key.Ref, true
	}
	if key.Derived != nil {
		cache.derived, cache.hasDerived = *key.Derived, true
	}
	return cache
}

func cloneObservedState(state driver.LockState) driver.LockState {
	state.Key = copyLockKey(state.Key)
	if state.Record != nil {
		record := *state.Record
		record.Body = bytes.Clone(record.Body)
		state.Record = &record
	}
	if state.Derived != nil {
		// Inline annotations bypass this cache below. Preserve nil versus empty
		// ordinary values without invoking annotation normalization.
		row := *state.Derived
		row.Value = bytes.Clone(row.Value)
		state.Derived = &row
	}
	return state
}

// Called only after the current SQL row was successfully scanned. This caches
// decoding, never observations: SQL errors, current overlays and separately
// stored selection annotations still follow the original read path each time.
// The reader mutex protects the cache; Close discards it.
func (r *reader) decodeObservedState(raw []byte, key driver.LockKey) (driver.LockState, error) {
	identity := observedStateKey(raw, key)
	if state, ok := r.stateDecodeCache[identity]; ok {
		return cloneObservedState(state), nil
	}
	state := driver.LockState{Key: copyLockKey(key), Revision: model.Natural64(0)}
	if err := decode(raw, &state); err != nil {
		return driver.LockState{}, err
	}
	// Ordinary typed annotations are restored separately from their current
	// binary SQL rows. Legacy inline annotations still use the original JSON
	// result verbatim, including nil versus empty nested slices.
	if state.Derived != nil && state.Derived.Index != nil {
		return state, nil
	}
	weight := len(raw) + len(key.Kind) + len(key.Fact) + len(key.Sentinel) +
		len(identity.ref.ID) + len(identity.ref.Key) + len(identity.ref.Branch) +
		len(identity.derived.Table) + len(identity.derived.Key)
	if weight > observedStateDecodeMaxBytes {
		return state, nil
	}
	if len(r.stateDecodeCache) >= observedStateDecodeMaxEntries || r.stateDecodeBytes+weight > observedStateDecodeMaxBytes {
		r.stateDecodeCache = nil
		r.stateDecodeBytes = 0
	}
	if r.stateDecodeCache == nil {
		r.stateDecodeCache = make(map[observedStateDecodeKey]driver.LockState)
	}
	r.stateDecodeCache[identity] = state
	r.stateDecodeBytes += weight
	return cloneObservedState(state), nil
}

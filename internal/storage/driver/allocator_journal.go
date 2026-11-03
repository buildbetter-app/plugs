package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Internal immutable indexes share content-addressed values. They live in the
// existing allocator table, are never project profiles, and count as occupancy.
const AllocatorJournalNamespace = "allocator-snapshot/"
const allocatorJournalVersion = "plug-allocator-history/1"

type allocatorJournalRef struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type allocatorJournalIndex struct {
	Version       string                `json:"version"`
	StoreID       StoreID               `json:"store_id"`
	Incarnation   string                `json:"incarnation"`
	At            model.Revision        `json:"at"`
	CoverageFloor model.Revision        `json:"coverage_floor"`
	Digest        string                `json:"digest"`
	Rows          []allocatorJournalRef `json:"rows"`
}
type allocatorJournalValue struct {
	Key  string          `json:"key"`
	Data json.RawMessage `json:"data"`
}

func AllocatorJournalIndexKey(incarnation string, at model.Revision) string {
	return AllocatorJournalNamespace + "index/" + hex.EncodeToString([]byte(incarnation)) + "/" + at.String()
}
func journalValueKey(raw []byte) string {
	h := sha256.Sum256(raw)
	return AllocatorJournalNamespace + "value/" + hex.EncodeToString(h[:])
}

func AllocatorJournalEntries(s AllocatorSnapshot) (map[string][]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	digest, err := s.Digest()
	if err != nil {
		return nil, err
	}
	index := allocatorJournalIndex{allocatorJournalVersion, s.StoreID, s.Incarnation, s.At, s.CoverageFloor, digest, []allocatorJournalRef{}}
	entries := map[string][]byte{}
	add := func(key string, value any) error {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(allocatorJournalValue{key, raw})
		if err != nil {
			return err
		}
		id := journalValueKey(encoded)
		entries[id] = encoded
		index.Rows = append(index.Rows, allocatorJournalRef{key, id})
		return nil
	}
	for _, a := range s.Inventory.Projects {
		if err := add(a.Prefix, a); err != nil {
			return nil, err
		}
	}
	for _, p := range s.Inventory.Parents {
		key, err := ParentCounterKey(s.Incarnation, p.Parent)
		if err != nil {
			return nil, err
		}
		if err := add(key, p); err != nil {
			return nil, err
		}
	}
	sort.Slice(index.Rows, func(i, j int) bool { return index.Rows[i].Key < index.Rows[j].Key })
	raw, err := json.Marshal(index)
	if err != nil {
		return nil, err
	}
	entries[AllocatorJournalIndexKey(s.Incarnation, s.At)] = raw
	return entries, nil
}

// Unchanged values are not rewritten. An index still binds the complete
// logical image at every committed head, including allocator-free writes.
func AllocatorJournalDelta(previous, next AllocatorSnapshot) (map[string][]byte, error) {
	before, err := AllocatorJournalEntries(previous)
	if err != nil {
		return nil, err
	}
	after, err := AllocatorJournalEntries(next)
	if err != nil {
		return nil, err
	}
	for key, raw := range after {
		if old, ok := before[key]; ok && bytes.Equal(old, raw) {
			delete(after, key)
		}
	}
	return after, nil
}

func decodeJournalIndex(meta Meta, key string, raw []byte) (allocatorJournalIndex, error) {
	var v allocatorJournalIndex
	if err := strictAllocatorJSON(raw, &v); err != nil {
		return v, err
	}
	canonical, err := json.Marshal(v)
	if err != nil || !bytes.Equal(canonical, raw) {
		return v, fmt.Errorf("noncanonical allocator index")
	}
	if v.Version != allocatorJournalVersion || v.StoreID != meta.StoreID || v.Incarnation != meta.Incarnation || key != AllocatorJournalIndexKey(v.Incarnation, v.At) || !importDigest(v.Digest) || v.Rows == nil {
		return v, fmt.Errorf("allocator index source/key mismatch")
	}
	at, err := v.At.Uint64()
	if err != nil {
		return v, err
	}
	floor, err := v.CoverageFloor.Uint64()
	if err != nil {
		return v, err
	}
	head, err := meta.Head.Uint64()
	if err != nil {
		return v, err
	}
	if floor > at || at > head {
		return v, fmt.Errorf("allocator index coverage mismatch")
	}
	for i, row := range v.Rows {
		if row.Key == "" || i > 0 && v.Rows[i-1].Key >= row.Key || !strings.HasPrefix(row.Value, AllocatorJournalNamespace+"value/") || !importDigest(strings.TrimPrefix(row.Value, AllocatorJournalNamespace+"value/")) {
			return v, fmt.Errorf("invalid allocator index row")
		}
	}
	return v, nil
}

func AllocatorJournalCoverage(meta Meta, raw []byte) (model.Revision, error) {
	v, err := decodeJournalIndex(meta, AllocatorJournalIndexKey(meta.Incarnation, meta.Head), raw)
	return v.CoverageFloor, err
}

// Materialize validates every internal row before excluding it from the live
// census. No namespace-wide skip or latest-image fallback can prove history.
func MaterializeAllocatorJournal(meta Meta, at model.Revision, rows map[string][]byte) (AllocatorSnapshot, error) {
	invalid := func(err error) (AllocatorSnapshot, error) {
		return AllocatorSnapshot{}, NewNativeAllocatorSnapshotError(NativeAllocatorInventoryInvalid, err)
	}
	live := AllocatorInventory{}
	indexes := map[string]allocatorJournalIndex{}
	values := map[string]allocatorJournalValue{}
	for key, raw := range rows {
		switch {
		case strings.HasPrefix(key, AllocatorJournalNamespace+"index/"):
			v, err := decodeJournalIndex(meta, key, raw)
			if err != nil {
				return invalid(err)
			}
			indexes[key] = v
		case strings.HasPrefix(key, AllocatorJournalNamespace+"value/"):
			var v allocatorJournalValue
			if err := strictAllocatorJSON(raw, &v); err != nil {
				return invalid(err)
			}
			canonical, err := json.Marshal(v)
			if err != nil || !bytes.Equal(raw, canonical) || key != journalValueKey(raw) {
				return invalid(fmt.Errorf("allocator journal value checksum"))
			}
			probe := AllocatorInventory{}
			if err := probe.AddRow(v.Key, v.Data, meta.Incarnation); err != nil {
				return invalid(err)
			}
			values[key] = v
		case strings.HasPrefix(key, AllocatorJournalNamespace):
			return invalid(fmt.Errorf("unknown allocator journal row"))
		default:
			if err := live.AddRow(key, raw, meta.Incarnation); err != nil {
				return invalid(err)
			}
		}
	}
	var selected *AllocatorSnapshot
	ordered := make([]uint64, 0, len(indexes))
	var floor uint64
	for _, index := range indexes {
		at, _ := index.At.Uint64()
		f, _ := index.CoverageFloor.Uint64()
		if len(ordered) > 0 && f != floor {
			return invalid(fmt.Errorf("allocator coverage changes within incarnation"))
		}
		floor = f
		ordered = append(ordered, at)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	if len(ordered) > 0 {
		head, _ := meta.Head.Uint64()
		if ordered[0] != floor || ordered[len(ordered)-1] != head {
			return invalid(fmt.Errorf("incomplete allocator coverage endpoints"))
		}
		for i := 1; i < len(ordered); i++ {
			if ordered[i]-ordered[i-1] != 1 {
				return invalid(fmt.Errorf("allocator coverage gap"))
			}
		}
	}
	for key, index := range indexes {
		inventory := AllocatorInventory{}
		for _, ref := range index.Rows {
			value, ok := values[ref.Value]
			if !ok || value.Key != ref.Key {
				return invalid(fmt.Errorf("incomplete allocator index"))
			}
			if err := inventory.AddRow(value.Key, value.Data, index.Incarnation); err != nil {
				return invalid(err)
			}
		}
		s, err := NewAllocatorSnapshot(meta, index.At, index.CoverageFloor, inventory)
		if err != nil {
			return invalid(err)
		}
		digest, err := s.Digest()
		if err != nil || digest != index.Digest {
			return invalid(fmt.Errorf("allocator index image digest"))
		}
		if key == AllocatorJournalIndexKey(meta.Incarnation, meta.Head) {
			current, err := NewAllocatorSnapshot(meta, meta.Head, index.CoverageFloor, live)
			if err != nil {
				return invalid(err)
			}
			currentDigest, err := current.Digest()
			if err != nil || currentDigest != digest {
				return invalid(fmt.Errorf("allocator live census differs from complete index"))
			}
		}
		if key == AllocatorJournalIndexKey(meta.Incarnation, at) {
			selected = &s
		}
	}
	if selected != nil {
		return selected.Detached()
	}
	if len(indexes) == 0 && len(values) == 0 && at.String() == meta.Head.String() {
		return NewAllocatorSnapshot(meta, at, at, live)
	}
	return AllocatorSnapshot{}, NewNativeAllocatorSnapshotError(NativeAllocatorCoverageUnavailable, nil)
}

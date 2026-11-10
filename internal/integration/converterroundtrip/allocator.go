package converterroundtrip

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The manifest's hash_length describes admitted source IDs, including older
// IDs shorter than today's configured minimum. The config table carries the
// adaptive policy. Never infer that policy from an observed ID's width.
func importedAllocator(archive *dumpformat.Archive, project dumpformat.Project) (driver.OfflineAllocator, error) {
	a := driver.NewHashAllocator(project.Prefix, 3, 8, 0.25)
	seen := map[string]bool{}
	for _, record := range archive.Records("config") {
		if record.Source.Table != "config" || record.Project != "" && record.Project != project.Prefix {
			continue
		}
		key, _ := record.Source.Fields["key"].(string)
		if key != "min_hash_length" && key != "max_hash_length" && key != "max_collision_prob" {
			continue
		}
		if seen[key] {
			return driver.OfflineAllocator{}, fmt.Errorf("%w: duplicate allocator config %s for %s", ErrRestoreUnavailable, key, project.Prefix)
		}
		seen[key] = true
		value, ok := record.Source.Fields["value"].(string)
		if !ok {
			return driver.OfflineAllocator{}, fmt.Errorf("%w: allocator config %s is not text", ErrRestoreUnavailable, key)
		}
		if value == "" {
			continue
		}
		// Beads ignores a malformed numeric setting and retains its default.
		if key == "max_collision_prob" {
			if n, err := strconv.ParseFloat(value, 64); err == nil {
				a.MaxCollisionProb = n
			}
		} else if n, err := strconv.ParseUint(value, 10, 32); err == nil {
			if key == "min_hash_length" {
				a.MinHashLength = uint32(n)
			} else {
				a.HashLength, a.MaxHashLength = uint32(n), uint32(n)
			}
		}
	}
	a.LastID = project.LastID
	if len(project.ChildCounters) > 0 {
		a.Children = make(map[string]uint64, len(project.ChildCounters))
		for parent, n := range project.ChildCounters {
			a.Children[parent] = n
		}
	}
	// Keep the source spelling of every ID. Only the final decimal component
	// contributes to its immediate parent's counter, including leading zeroes.
	for _, record := range archive.Records("bead") {
		if record.Project != project.Prefix {
			continue
		}
		if dot := strings.LastIndexByte(record.ID, '.'); dot >= 0 {
			parent := record.ID[:dot]
			n, err := strconv.ParseUint(record.ID[dot+1:], 10, 64)
			if err != nil || n == 0 {
				return driver.OfflineAllocator{}, fmt.Errorf("%w: invalid source child counter", ErrRestoreUnavailable)
			}
			if a.Children == nil {
				a.Children = map[string]uint64{}
			}
			if n > a.Children[parent] {
				a.Children[parent] = n
			}
		}
	}
	if err := a.Validate(); err != nil {
		return driver.OfflineAllocator{}, fmt.Errorf("%w: source allocator %s: %v", ErrRestoreUnavailable, project.Prefix, err)
	}
	return a, nil
}

// Immutable import evidence retains the initial counter image. Later ordinary
// writes may only advance counters; they cannot change the admitted policy.
func allocatorContinues(seed, actual driver.OfflineAllocator) bool {
	if actual.LastID < seed.LastID {
		return false
	}
	for parent, n := range seed.Children {
		if actual.Children[parent] < n {
			return false
		}
	}
	seed.LastID, actual.LastID = 0, 0
	seed.Children, actual.Children = nil, nil
	return reflect.DeepEqual(seed, actual)
}

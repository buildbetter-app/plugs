package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// populationCache is an expendable Writer-local observation, never storage or
// provenance. Only requested, existing HASH profiles are retained, with one
// integer per prefix and no record bodies. A revision mismatch discards it.
type populationCache struct {
	mu          sync.Mutex
	generation  uint64
	incarnation string
	at          model.Revision
	counts      map[string]uint64
}

func populationMatches(prefix, id string) bool {
	tail, ok := strings.CutPrefix(id, prefix+"-")
	return ok && tail != "" && !strings.ContainsRune(tail, '.')
}

// Candidate discovery must not turn arbitrary ordinary IDs into invalid
// allocator lookups. Slashes and noncanonical whitespace cannot name profiles.
func populationPrefixes(id string) []string {
	var prefixes []string
	for i := 1; i < len(id); i++ {
		if id[i] != '-' || !populationMatches(id[:i], id) {
			continue
		}
		if _, err := driver.AllocatorFact(id[:i]); err == nil {
			prefixes = append(prefixes, id[:i])
		}
	}
	return prefixes
}
func (p *prepared) populationOverlay(prefix string, count uint64, inverse bool) (uint64, error) {
	for id, pending := range p.writes {
		if pending.kind != "bead" || !populationMatches(prefix, id) {
			continue
		}
		before := len(pending.before) != 0
		after := !pending.deleted
		if inverse {
			before, after = after, before
		}
		if before {
			if count == 0 {
				return 0, fmt.Errorf("allocator population underflow")
			}
			count--
		}
		if after {
			count++
		}
	}
	return count, nil
}
func (p *prepared) hashPopulation(ctx context.Context, prefix string) (uint64, error) {
	if p.population == nil {
		count, _, _, err := p.allocatorObservations(ctx, prefix, "")
		return count, err
	}
	// Preserve the old query's optimistic dependency even on a cache hit.
	if _, err := p.snapshot.observe(ctx, []driver.LockKey{driver.GraphBlockingSentinelLock}); err != nil {
		return 0, err
	}
	view := p.snapshot.Snapshot()
	cache := p.population
	cache.mu.Lock()
	if cache.counts == nil || cache.incarnation != view.Incarnation || cache.at.String() != view.At.String() {
		cache.incarnation = view.Incarnation
		cache.at = *view.At
		cache.counts = map[string]uint64{}
		cache.generation++
	}
	generation := cache.generation
	base, ok := cache.counts[prefix]
	cache.mu.Unlock()
	if ok {
		return p.populationOverlay(prefix, base, false)
	}
	// Keep the census implementation as the cold-path oracle, including its
	// staged replacement/deletion semantics. Undo its overlay before caching.
	count, _, _, err := p.allocatorObservations(ctx, prefix, "")
	if err != nil {
		return 0, err
	}
	base, err = p.populationOverlay(prefix, count, true)
	if err != nil {
		return 0, err
	}
	cache.mu.Lock()
	if cache.counts != nil && cache.generation == generation && cache.incarnation == view.Incarnation && cache.at.String() == view.At.String() {
		cache.counts[prefix] = base
	}
	cache.mu.Unlock()
	return count, nil
}

// Advance only after an acknowledged record commit and only from its actual
// precommit head. Any intervening external write invalidates the observation.
func (w *Writer) advancePopulation(p *prepared, head, next model.Revision) {
	cache := &w.population
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.incarnation != p.snapshot.Snapshot().Incarnation || cache.at.String() != head.String() {
		return
	}
	if cache.counts == nil || p.snapshot.Snapshot().At.String() != head.String() {
		cache.counts = nil
		cache.generation++
		return
	}
	deltas := map[string]int64{}
	for id := range p.changed {
		pending := p.writes[id]
		if pending == nil || pending.kind != "bead" {
			continue
		}
		delta := int64(0)
		if len(pending.before) != 0 {
			delta--
		}
		if !pending.deleted {
			delta++
		}
		if delta == 0 {
			continue
		}
		for _, prefix := range populationPrefixes(id) {
			if _, exists := cache.counts[prefix]; exists {
				deltas[prefix] += delta
			}
		}
	}
	for prefix, delta := range deltas {
		count := cache.counts[prefix]
		if delta < 0 {
			decrease := uint64(-delta)
			if decrease > count {
				cache.counts = nil
				cache.generation++
				return
			}
			count -= decrease
		} else {
			count += uint64(delta)
		}
		cache.counts[prefix] = count
	}
	cache.at = next
	cache.generation++
}

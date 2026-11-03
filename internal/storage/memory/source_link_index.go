package memory

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This coverage domain certifies complete outgoing link sets, independently
// of canonical Parent. The later incoming guard selector uses four known
// kinds, but the typed source image retains all supplied outgoing facts.
type sourceLinkIndex struct {
	incarnation string
	branch      uint64
	builtFrom   uint64
	through     uint64
	roots       []*sourceLinkRoot
}

type sourceLinkRoot struct {
	revision uint64
	sources  map[string]sourceLinkSource
	incoming map[sourceLinkBucket][]string // sorted source keys, no Body fetch
}

type sourceLinkSource struct {
	record driver.Record
	links  []sourceLink // presence in sources explicitly declares completeness
}

type sourceLink struct {
	kind   string
	target model.Ref
}

type sourceLinkBucket struct{ kind, targetKey string }

func newSourceLinkIndex(incarnation string) *sourceLinkIndex {
	return &sourceLinkIndex{incarnation: incarnation}
}

func newSourceLinkRoot(revision uint64) *sourceLinkRoot {
	return &sourceLinkRoot{revision: revision, sources: make(map[string]sourceLinkSource), incoming: make(map[sourceLinkBucket][]string)}
}

func (h *sourceLinkIndex) rootAt(incarnation string, branch, revision uint64) (*sourceLinkRoot, error) {
	if h == nil || h.incarnation != incarnation || h.branch != branch || len(h.roots) == 0 || revision < h.builtFrom || revision > h.through {
		return nil, fmt.Errorf("%w: outgoing link coverage unavailable", driver.ErrUnsupported)
	}
	i := sort.Search(len(h.roots), func(i int) bool { return h.roots[i].revision > revision })
	if i == 0 {
		return nil, fmt.Errorf("%w: outgoing link root unavailable", driver.ErrUnsupported)
	}
	return h.roots[i-1], nil
}

// certify requires the publication layer to prove complete typed initial
// coverage, then maintain every publication without a legacy-writer gap.
func (h *sourceLinkIndex) certify(root *sourceLinkRoot) (*sourceLinkIndex, error) {
	if h == nil || root == nil {
		return nil, fmt.Errorf("%w: missing outgoing link state", driver.ErrUnsupported)
	}
	n := *h
	if len(h.roots) == 0 {
		n.builtFrom = root.revision
	} else if h.through == ^uint64(0) || root.revision != h.through+1 {
		return nil, fmt.Errorf("%w: noncontiguous outgoing link coverage", driver.ErrUnsupported)
	}
	n.through = root.revision
	n.roots = append(slices.Clone(h.roots), root)
	return &n, nil
}

func sourceLinkKey(link sourceLink) (sourceLinkBucket, error) {
	if link.kind == "" || len(link.kind) > 32 || !utf8.ValidString(link.kind) || !link.target.IsID() || link.target.ID == "" {
		return sourceLinkBucket{}, fmt.Errorf("%w: invalid outgoing link metadata", driver.ErrUnsupported)
	}
	key, err := recordKey(link.target)
	return sourceLinkBucket{kind: link.kind, targetKey: key}, err
}

// apply never parses Body or mutates a published image/bucket. Selecting
// incoming membership later merges keys first, deduplicates and excludes keys
// before one global limit+1, and only then materializes selected source images.
func (r *sourceLinkRoot) apply(revision uint64, puts []sourceLinkSource, deletes []string) (*sourceLinkRoot, error) {
	if r == nil || revision < r.revision {
		return nil, fmt.Errorf("%w: invalid outgoing link root revision", driver.ErrUnsupported)
	}
	n := &sourceLinkRoot{revision: revision, sources: maps.Clone(r.sources), incoming: maps.Clone(r.incoming)}
	changed := make(map[sourceLinkBucket]bool)
	remove := func(key string) {
		old, ok := n.sources[key]
		if !ok {
			return
		}
		delete(n.sources, key)
		for _, link := range old.links {
			bucketKey, _ := sourceLinkKey(link)
			bucket := n.incoming[bucketKey]
			if !changed[bucketKey] {
				bucket = slices.Clone(bucket)
				changed[bucketKey] = true
			}
			i, found := slices.BinarySearch(bucket, key)
			if found {
				bucket = slices.Delete(bucket, i, i+1)
			}
			if len(bucket) == 0 {
				delete(n.incoming, bucketKey)
			} else {
				n.incoming[bucketKey] = bucket
			}
		}
	}
	for _, key := range deletes {
		remove(key)
	}
	for _, source := range puts {
		key, err := recordKey(source.record.Ref)
		if err != nil {
			return nil, err
		}
		at, err := source.record.Revision.Uint64()
		if err != nil || at > revision || source.record.Kind != "bead" || !source.record.Ref.IsID() || source.record.Ref.ID == "" {
			return nil, fmt.Errorf("%w: invalid outgoing source image", driver.ErrUnsupported)
		}
		buckets := make(map[sourceLinkBucket]bool)
		for _, link := range source.links {
			bucketKey, err := sourceLinkKey(link)
			if err != nil {
				return nil, err
			}
			buckets[bucketKey] = true
		}
		remove(key)
		source.record = copyRecord(source.record)
		source.links = slices.Clone(source.links)
		n.sources[key] = source
		for bucketKey := range buckets {
			bucket := n.incoming[bucketKey]
			if !changed[bucketKey] {
				bucket = slices.Clone(bucket)
				changed[bucketKey] = true
			}
			i, found := slices.BinarySearch(bucket, key)
			if !found {
				n.incoming[bucketKey] = slices.Insert(bucket, i, key)
			}
		}
	}
	return n, nil
}

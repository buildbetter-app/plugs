package memory

import (
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These private immutable values are representation primitives only. The
// publication layer must prove complete typed coverage under its Head fence
// before calling certify. No reader or writer capability is advertised here.
type hierarchyIndex struct {
	incarnation string
	branch      uint64
	builtFrom   uint64
	through     uint64
	roots       []*hierarchyRoot
}

type hierarchyRoot struct {
	revision uint64
	children map[string][]hierarchySource // ordered by canonical child record key
	sources  map[string]hierarchySource
}

type hierarchySource struct {
	parentKey string // explicit typed root is empty; missing sources are untyped
	record    driver.Record
}

func newHierarchyIndex(incarnation string) *hierarchyIndex {
	return &hierarchyIndex{incarnation: incarnation}
}

func newHierarchyRoot(revision uint64) *hierarchyRoot {
	return &hierarchyRoot{revision: revision, children: make(map[string][]hierarchySource), sources: make(map[string]hierarchySource)}
}

// rootAt never substitutes current membership for a missing historical root.
func (h *hierarchyIndex) rootAt(incarnation string, branch, revision uint64) (*hierarchyRoot, error) {
	if h == nil || h.incarnation != incarnation || h.branch != branch || len(h.roots) == 0 || revision < h.builtFrom || revision > h.through {
		return nil, fmt.Errorf("%w: hierarchy index coverage unavailable", driver.ErrUnsupported)
	}
	i := sort.Search(len(h.roots), func(i int) bool { return h.roots[i].revision > revision })
	if i == 0 {
		return nil, fmt.Errorf("%w: hierarchy index root unavailable", driver.ErrUnsupported)
	}
	return h.roots[i-1], nil
}

// certify is an explicit publication primitive, never an emptiness heuristic.
// Initial completeness is the caller's responsibility. Later publications must
// be contiguous, so an old writer cannot leave a gap and resume certification.
func (h *hierarchyIndex) certify(root *hierarchyRoot) (*hierarchyIndex, error) {
	if h == nil || root == nil {
		return nil, fmt.Errorf("%w: missing hierarchy index state", driver.ErrUnsupported)
	}
	n := *h
	if len(h.roots) == 0 {
		n.builtFrom = root.revision
	} else if h.through == ^uint64(0) || root.revision != h.through+1 {
		return nil, fmt.Errorf("%w: noncontiguous hierarchy coverage", driver.ErrUnsupported)
	}
	n.through = root.revision
	n.roots = append(slices.Clone(h.roots), root)
	return &n, nil
}

// apply makes a new root. It copies input bodies and only the changed parent
// buckets; unaffected images and buckets remain shared and immutable. The
// caller owns no mutable pointer into a published record image.
func (r *hierarchyRoot) apply(revision uint64, puts []hierarchySource, deletes []string) (*hierarchyRoot, error) {
	if r == nil || revision < r.revision {
		return nil, fmt.Errorf("%w: invalid hierarchy root revision", driver.ErrUnsupported)
	}
	n := &hierarchyRoot{revision: revision, children: maps.Clone(r.children), sources: maps.Clone(r.sources)}
	changed := make(map[string]bool)
	remove := func(key string) {
		old, ok := n.sources[key]
		if !ok {
			return
		}
		delete(n.sources, key)
		if old.parentKey == "" {
			return
		}
		bucket := n.children[old.parentKey]
		if !changed[old.parentKey] {
			bucket = slices.Clone(bucket)
			changed[old.parentKey] = true
		}
		i := sort.Search(len(bucket), func(i int) bool {
			child, _ := recordKey(bucket[i].record.Ref)
			return child >= key
		})
		if i < len(bucket) {
			bucket = slices.Delete(bucket, i, i+1)
		}
		if len(bucket) == 0 {
			delete(n.children, old.parentKey)
		} else {
			n.children[old.parentKey] = bucket
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
		if err != nil || at > revision || source.record.Kind != "bead" || source.parentKey == key || !source.record.Ref.IsID() {
			return nil, fmt.Errorf("%w: invalid hierarchy source image", driver.ErrUnsupported)
		}
		remove(key)
		source.record = copyRecord(source.record)
		n.sources[key] = source
		if source.parentKey == "" {
			continue
		}
		bucket := n.children[source.parentKey]
		if !changed[source.parentKey] {
			bucket = slices.Clone(bucket)
			changed[source.parentKey] = true
		}
		i := sort.Search(len(bucket), func(i int) bool {
			child, _ := recordKey(bucket[i].record.Ref)
			return child >= key
		})
		n.children[source.parentKey] = slices.Insert(bucket, i, source)
	}
	return n, nil
}

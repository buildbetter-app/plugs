package memory

import (
	"container/heap"
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Membership cursors touch keys only. Parent buckets contain immutable source
// references; reading Ref does not copy or interpret their Body.
type sourceKeyCursor struct {
	keys     []string
	parents  []hierarchySource
	position int
	key      string
	staged   bool
}

func (c *sourceKeyCursor) advance() (bool, error) {
	if c.keys != nil {
		if c.position >= len(c.keys) {
			return false, nil
		}
		c.key = c.keys[c.position]
	} else {
		if c.position >= len(c.parents) {
			return false, nil
		}
		var err error
		c.key, err = recordKey(c.parents[c.position].record.Ref)
		if err != nil {
			return false, err
		}
	}
	c.position++
	return true, nil
}

type sourceKeyHeap []*sourceKeyCursor

func (h sourceKeyHeap) Len() int           { return len(h) }
func (h sourceKeyHeap) Less(i, j int) bool { return h[i].key < h[j].key }
func (h sourceKeyHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *sourceKeyHeap) Push(v any)        { *h = append(*h, v.(*sourceKeyCursor)) }
func (h *sourceKeyHeap) Pop() any {
	last := len(*h) - 1
	v := (*h)[last]
	*h = (*h)[:last]
	return v
}

type sourceReadWork struct{ buckets, candidates, images int }

func canonicalSourceKeys(refs []model.Ref) map[string]bool {
	keys := make(map[string]bool, len(refs))
	for _, ref := range refs {
		key, _ := recordKey(ref) // request normalization validated every Ref
		keys[key] = true
	}
	return keys
}

// Merge each indexed prefix once. Caller exclusions override staged images;
// driver staging shadows the committed image before selection and lookahead.
// Duplicate inverse memberships never materialize duplicate source images.
func selectSourceKeys(ctx context.Context, cursors sourceKeyHeap, excluded map[string]bool, staged map[string]*driver.Record, limit int, work *sourceReadWork) ([]string, error) {
	h := make(sourceKeyHeap, 0, len(cursors))
	for _, cursor := range cursors {
		present, err := cursor.advance()
		if err != nil {
			return nil, err
		}
		if present {
			h = append(h, cursor)
		}
	}
	if work != nil {
		work.buckets += len(cursors)
	}
	heap.Init(&h)
	selected := make([]string, 0, limit+1)
	for len(h) != 0 && len(selected) < limit+1 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := h[0].key
		eligible := false
		for len(h) != 0 && h[0].key == key {
			cursor := heap.Pop(&h).(*sourceKeyCursor)
			if work != nil {
				work.candidates++
			}
			_, shadowed := staged[key]
			if !excluded[key] && (cursor.staged || !shadowed) {
				eligible = true
			}
			present, err := cursor.advance()
			if err != nil {
				return nil, err
			}
			if present {
				heap.Push(&h, cursor)
			}
		}
		if eligible {
			selected = append(selected, key)
		}
	}
	return selected, nil
}

func materializeSourceKeys(ctx context.Context, keys []string, at uint64, limit int, lookup func(string) (driver.Record, bool), work *sourceReadWork) ([]driver.Record, bool, error) {
	more := len(keys) > limit
	records := make([]driver.Record, 0, min(len(keys), limit))
	for i, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		image, found := lookup(key)
		if work != nil {
			work.images++
		}
		actual, err := recordKey(image.Ref)
		revision, revisionErr := image.Revision.Uint64()
		if !found || err != nil || revisionErr != nil || actual != key || revision > at || driver.ValidateHierarchyRecord(image, nil) != nil {
			return nil, false, fmt.Errorf("%w: malformed indexed source image", driver.ErrUnsupported)
		}
		if i < limit {
			records = append(records, copyRecord(image))
		}
	}
	return records, more, nil
}

func readHierarchySources(ctx context.Context, index *hierarchyIndex, incarnation string, at uint64, parents, excluded []model.Ref, limit int, tx *transaction, work *sourceReadWork) ([]driver.Record, bool, error) {
	root, err := index.rootAt(incarnation, 0, at)
	if err != nil {
		return nil, false, err
	}
	cursors := make(sourceKeyHeap, 0, len(parents)*2)
	for parent := range canonicalSourceKeys(parents) {
		if bucket := root.children[parent]; len(bucket) != 0 {
			cursors = append(cursors, &sourceKeyCursor{parents: bucket})
		}
		if tx != nil {
			if bucket := tx.stagedParents[parent]; len(bucket) != 0 {
				cursors = append(cursors, &sourceKeyCursor{keys: bucket, staged: true})
			}
		}
	}
	var staged map[string]*driver.Record
	if tx != nil {
		staged = tx.records
	}
	keys, err := selectSourceKeys(ctx, cursors, canonicalSourceKeys(excluded), staged, limit, work)
	if err != nil {
		return nil, false, err
	}
	return materializeSourceKeys(ctx, keys, at, limit, func(key string) (driver.Record, bool) {
		if image, shadowed := staged[key]; shadowed {
			if image == nil {
				return driver.Record{}, false
			}
			copy := *image
			copy.Revision = tx.base.keys[key].Revision
			return copy, true
		}
		source, found := root.sources[key]
		return source.record, found
	}, work)
}

func readInverseSources(ctx context.Context, index *sourceLinkIndex, incarnation string, at uint64, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int, tx *transaction, work *sourceReadWork) ([]driver.Record, bool, error) {
	root, err := index.rootAt(incarnation, 0, at)
	if err != nil {
		return nil, false, err
	}
	kindsSet := make(map[string]bool, len(kinds))
	for _, kind := range kinds {
		kindsSet[string(kind)] = true
	}
	cursors := make(sourceKeyHeap, 0)
	for target := range canonicalSourceKeys(targets) {
		for kind := range kindsSet {
			key := sourceLinkBucket{kind: kind, targetKey: target}
			if bucket := root.incoming[key]; len(bucket) != 0 {
				cursors = append(cursors, &sourceKeyCursor{keys: bucket})
			}
			if tx != nil {
				if bucket := tx.stagedIncoming[key]; len(bucket) != 0 {
					cursors = append(cursors, &sourceKeyCursor{keys: bucket, staged: true})
				}
			}
		}
	}
	var staged map[string]*driver.Record
	if tx != nil {
		staged = tx.records
	}
	keys, err := selectSourceKeys(ctx, cursors, canonicalSourceKeys(excluded), staged, limit, work)
	if err != nil {
		return nil, false, err
	}
	return materializeSourceKeys(ctx, keys, at, limit, func(key string) (driver.Record, bool) {
		if image, shadowed := staged[key]; shadowed {
			if image == nil {
				return driver.Record{}, false
			}
			copy := *image
			copy.Revision = tx.base.keys[key].Revision
			return copy, true
		}
		source, found := root.sources[key]
		return source.record, found
	}, work)
}

func (r *reader) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	return r.HierarchyChildrenExcluding(ctx, parents, nil, limit)
}

func (r *reader) HierarchyChildrenExcluding(ctx context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return nil, false, err
	}
	if _, err := driver.NormalizeHierarchyExcludingRequest(parents, excluded, limit); err != nil {
		return nil, false, err
	}
	return readHierarchySources(ctx, r.state.hierarchy, r.incarnation, r.at, parents, excluded, limit, nil, nil)
}

func (r *reader) InverseSourceLinks(ctx context.Context, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return nil, false, err
	}
	if _, err := driver.NormalizeInverseSourceLinksRequest(targets, kinds, excluded, limit); err != nil {
		return nil, false, err
	}
	return readInverseSources(ctx, r.state.sourceLinks, r.incarnation, r.at, targets, kinds, excluded, limit, nil, nil)
}

func (t *transaction) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	return t.HierarchyChildrenExcluding(ctx, parents, nil, limit)
}

func (t *transaction) HierarchyChildrenExcluding(ctx context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return nil, false, err
	}
	if _, err := driver.NormalizeHierarchyExcludingRequest(parents, excluded, limit); err != nil {
		return nil, false, err
	}
	at, err := t.snapshotAt.Uint64()
	if err != nil {
		return nil, false, err
	}
	return readHierarchySources(ctx, t.base.hierarchy, t.incarnation, at, parents, excluded, limit, t, nil)
}

func (t *transaction) InverseSourceLinks(ctx context.Context, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return nil, false, err
	}
	if _, err := driver.NormalizeInverseSourceLinksRequest(targets, kinds, excluded, limit); err != nil {
		return nil, false, err
	}
	at, err := t.snapshotAt.Uint64()
	if err != nil {
		return nil, false, err
	}
	return readInverseSources(ctx, t.base.sourceLinks, t.incarnation, at, targets, kinds, excluded, limit, t, nil)
}

var _ driver.HierarchyChildrenExcludingReader = (*reader)(nil)
var _ driver.HierarchyChildrenExcludingReader = (*transaction)(nil)
var _ driver.InverseSourceLinksReader = (*reader)(nil)
var _ driver.InverseSourceLinksReader = (*transaction)(nil)

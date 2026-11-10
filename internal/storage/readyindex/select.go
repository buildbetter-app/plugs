package readyindex

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/readyorder"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (t *tree) child(scope string, c driver.ReadyOrderChild) (*driver.ReadyOrderNode, error) {
	n, err := t.node(scope, c.Prefix)
	if err != nil {
		return nil, err
	}
	if n.Commitment != c.Commitment {
		return nil, unavailable("child commitment mismatch")
	}
	return n, nil
}
func (t *tree) atPrefix(scope string, n *driver.ReadyOrderNode, p string) (*driver.ReadyOrderNode, error) {
	if strings.HasPrefix(n.Prefix, p) {
		return n, nil
	}
	if !strings.HasPrefix(p, n.Prefix) {
		return nil, nil
	}
	for _, c := range n.Children {
		if strings.HasPrefix(c.Prefix, p) || strings.HasPrefix(p, c.Prefix) {
			next, err := t.child(scope, c)
			if err != nil {
				return nil, err
			}
			return t.atPrefix(scope, next, p)
		}
	}
	return nil, nil
}
func (t *tree) rangeKeys(scope string, n *driver.ReadyOrderNode, offset *uint64, take *int, out *[]driver.SelectionKey) error {
	if *take == 0 {
		return nil
	}
	if *offset >= n.Commitment.Count {
		*offset -= n.Commitment.Count
		return nil
	}
	if n.Leaf != nil {
		if *offset > 0 {
			*offset--
		} else {
			*out = append(*out, n.Leaf.Key)
			*take--
			if *take == 0 {
				return nil
			}
		}
	}
	for _, c := range n.Children {
		if *offset >= c.Commitment.Count {
			*offset -= c.Commitment.Count
			continue
		}
		next, err := t.child(scope, c)
		if err != nil {
			return err
		}
		if err = t.rangeKeys(scope, next, offset, take, out); err != nil {
			return err
		}
		if *take == 0 {
			return nil
		}
	}
	return nil
}
func (t *tree) rank(scope string, n *driver.ReadyOrderNode, rank uint64) (driver.SelectionKey, error) {
	if rank >= n.Commitment.Count {
		return driver.SelectionKey{}, unavailable("rank outside subtree")
	}
	take := 1
	out := []driver.SelectionKey{}
	if err := t.rangeKeys(scope, n, &rank, &take, &out); err != nil {
		return driver.SelectionKey{}, err
	}
	if len(out) != 1 {
		return driver.SelectionKey{}, unavailable("rank leaf missing")
	}
	return out[0], nil
}
func refSet(refs []model.Ref) (map[string]bool, error) {
	if len(refs) > 1000 {
		return nil, unavailable("finite reference scope exceeds 1000")
	}
	out := map[string]bool{}
	for _, ref := range refs {
		if driver.ValidateHierarchyRef(ref) != nil {
			return nil, unavailable("reference identity")
		}
		out[ref.ID] = true
	}
	return out, nil
}
func scopeMatch(s source, scope driver.ReadyOrderScope) bool {
	return (scope.Project == nil || *scope.Project == s.Project) && (scope.Type == nil || *scope.Type == s.Type)
}
func (t *tree) leaves(refs map[string]bool, scope driver.ReadyOrderScope) ([]driver.ReadyOrderLeaf, error) {
	ids := make([]string, 0, len(refs))
	for id := range refs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]driver.ReadyOrderLeaf, 0, len(ids))
	for _, id := range ids {
		key := sourceKey(id)
		_, cached := t.rows[key]
		var s source
		found, err := t.read(key, &s)
		if err != nil {
			return nil, err
		}
		// Descriptor reads, including absent references, count as thin leaves.
		if !cached {
			t.work.Leaves++
		}
		if !found || !scopeMatch(s, scope) || s.Leaf == nil {
			continue
		}
		if !s.Local || s.Leaf.Key.Ref.ID != id {
			return nil, unavailable("incompatible thin source")
		}
		n := driver.ReadyOrderNode{Prefix: driver.ReadyOrderKey(s.Leaf.Priority, id), Leaf: s.Leaf}
		if err = driver.SealReadyOrderNode(&n); err != nil {
			return nil, err
		}
		t.work.DigestBytes += n.DigestBytes()
		out = append(out, *s.Leaf)
	}
	sort.Slice(out, func(i, j int) bool {
		return driver.ReadyOrderKey(out[i].Priority, out[i].Key.Ref.ID) < driver.ReadyOrderKey(out[j].Priority, out[j].Key.Ref.ID)
	})
	return out, nil
}

// Select reads exact counted metadata at a pinned snapshot. Uniform dense
// pages skip whole subtrees by cardinality. Sparse authorization examines only
// its explicit references, while pivots come from the RAW group tree.
func Select(ctx context.Context, r Observer, p driver.ReadyOrderPlan) (result driver.ReadyOrderResult, err error) {
	t := newTree(ctx, r)
	defer func() { result.Work = t.work }()
	v := r.Snapshot()
	if v.At == nil || p.Incarnation != v.Incarnation || !v.Branch.IsZero() || p.ContentRevision.Validate() != nil || p.AuthorityRevision.Validate() != nil || p.ContentRevision.String() != v.At.String() || p.AuthorityRevision.String() != v.At.String() || p.Eligibility == "" || p.Limit < 1 || p.Limit > 1000 {
		return result, unavailable("plan binding")
	}
	if p.UniformAuthorization && p.AuthorizedRefs != nil || !p.UniformAuthorization && p.AuthorizedRefs == nil {
		return result, unavailable("incomplete authorization boundary")
	}
	var coverage marker
	present, e := t.read(markerKey, &coverage)
	if e != nil {
		return result, e
	}
	if !present || coverage.Version != 3 || coverage.Incarnation != v.Incarnation || coverage.Through.String() != v.At.String() || coverage.Unsafe != 0 {
		return result, unavailable("source, lease, heartbeat or nonlocal coverage")
	}
	// With a common genuine creation instant inside the recent 48-hour
	// bucket, main's (bucket, rank, creation, ID) order is exactly this
	// priority/ID tree. A missing clock, mixed instants, or an aged source
	// takes ordinary Ready instead of claiming an incompatible rank.
	if p.OrderAt.IsZero() || coverage.OrderUnsafe || coverage.OrderCount > 0 &&
		(coverage.OrderCreatedAt.IsZero() || p.OrderAt.Before(coverage.OrderCreatedAt) ||
			readyorder.Classify(0, coverage.OrderCreatedAt, p.OrderAt).Bucket != 0) {
		return result, unavailable("hybrid Ready order certificate")
	}
	global, e := t.node(scopeID(driver.ReadyOrderScope{}), "")
	if e != nil {
		return result, e
	}
	if global.Commitment.Count != coverage.OrderCount {
		return result, unavailable("hybrid Ready order count")
	}
	scope := scopeID(p.Scope)
	var header scopeHeader
	_, e = t.read(headerKey(scope), &header)
	if e != nil {
		return result, e
	}
	for binding, n := range header.Bindings {
		if n == 0 || binding != p.Eligibility {
			return result, unavailable("effective ontology changed")
		}
	}
	root, e := t.node(scope, "")
	if e != nil {
		return result, e
	}
	if p.RawRefs != nil || !p.UniformAuthorization {
		return selectFinite(t, p, scope, root)
	}
	result.Total = root.Commitment.Count
	result.Commitment = root.Commitment
	if p.Offset > result.Total {
		return result, fmt.Errorf("%w: Ready offset outside membership", driver.ErrUnsupported)
	}
	remaining := p.Limit
	offset := p.Offset
	if p.Seed == "" {
		e = t.rangeKeys(scope, root, &offset, &remaining, &result.Keys)
	} else {
		for priority := int64(0); priority <= 4 && remaining > 0; priority++ {
			group, err := t.atPrefix(scope, root, driver.ReadyOrderKey(priority, ""))
			if err != nil {
				return result, err
			}
			if group == nil {
				continue
			}
			count := group.Commitment.Count
			if offset >= count {
				offset -= count
				continue
			}
			pivot := driver.ReadyOrderPivot(p.Seed, priority, count)
			for _, span := range [][2]uint64{{pivot, count}, {0, pivot}} {
				length := span[1] - span[0]
				if offset >= length {
					offset -= length
					continue
				}
				start := span[0] + offset
				take := min(remaining, int(length-offset))
				offset = 0
				before := take
				if err = t.rangeKeys(scope, group, &start, &take, &result.Keys); err != nil {
					return result, err
				}
				remaining -= before - take
				if remaining == 0 {
					break
				}
			}
		}
	}
	if e != nil {
		return result, e
	}
	result.More = p.Offset+uint64(len(result.Keys)) < result.Total
	return result, nil
}
func selectFinite(t *tree, p driver.ReadyOrderPlan, scope string, root *driver.ReadyOrderNode) (driver.ReadyOrderResult, error) {
	result := driver.ReadyOrderResult{}
	var raw, authorized []driver.ReadyOrderLeaf
	var rawSet map[string]bool
	if p.RawRefs != nil {
		set, err := refSet(p.RawRefs)
		if err != nil {
			return result, err
		}
		rawSet = set
		raw, err = t.leaves(set, p.Scope)
		if err != nil {
			return result, err
		}
	}
	if p.UniformAuthorization {
		authorized = append(authorized, raw...)
	} else {
		set, err := refSet(p.AuthorizedRefs)
		if err != nil {
			return result, err
		}
		if rawSet != nil {
			for id := range set {
				if !rawSet[id] {
					delete(set, id)
				}
			}
		}
		authorized, err = t.leaves(set, p.Scope)
		if err != nil {
			return result, err
		}
	}
	nodes, err := driver.BuildReadyOrderNodes(authorized)
	if err != nil {
		return result, err
	}
	for _, node := range nodes {
		t.work.DigestBytes += node.DigestBytes()
	}
	result.Total = uint64(len(authorized))
	result.Commitment = nodes[""].Commitment
	if p.Offset > result.Total {
		return result, unavailable("offset outside finite membership")
	}
	if p.Seed != "" {
		pivots := map[int64]string{}
		for _, leaf := range authorized {
			priority := leaf.Priority
			if _, exists := pivots[priority]; exists {
				continue
			}
			if rawSet != nil {
				var group []driver.ReadyOrderLeaf
				for _, l := range raw {
					if l.Priority == priority {
						group = append(group, l)
					}
				}
				if len(group) == 0 {
					return result, unavailable("authorized item absent from raw domain")
				}
				pivots[priority] = driver.ReadyOrderKey(priority, group[driver.ReadyOrderPivot(p.Seed, priority, uint64(len(group)))].Key.Ref.ID)
			} else {
				group, e := t.atPrefix(scope, root, driver.ReadyOrderKey(priority, ""))
				if e != nil {
					return result, e
				}
				if group == nil {
					return result, unavailable("raw priority group missing")
				}
				pivot, e := t.rank(scope, group, driver.ReadyOrderPivot(p.Seed, priority, group.Commitment.Count))
				if e != nil {
					return result, e
				}
				pivots[priority] = driver.ReadyOrderKey(priority, pivot.Ref.ID)
			}
		}
		sort.Slice(authorized, func(i, j int) bool {
			a, b := authorized[i], authorized[j]
			if a.Priority != b.Priority {
				return a.Priority < b.Priority
			}
			ka, kb := driver.ReadyOrderKey(a.Priority, a.Key.Ref.ID), driver.ReadyOrderKey(b.Priority, b.Key.Ref.ID)
			pivot := pivots[a.Priority]
			if (ka < pivot) != (kb < pivot) {
				return ka >= pivot
			}
			return ka < kb
		})
	}
	end := min(result.Total, p.Offset+uint64(p.Limit))
	for _, leaf := range authorized[p.Offset:end] {
		result.Keys = append(result.Keys, leaf.Key)
	}
	result.More = end < result.Total
	return result, nil
}

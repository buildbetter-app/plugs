package readyindex

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
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

// Public ClaimNext rotates only an equal bucket/rank/creation-time group.
// Counted radix subtrees let the selector skip every untouched group without
// streaming its leaves. The first leaf supplies the existing priority seed.
func (t *tree) seededPublic(scope string, root *driver.ReadyOrderNode, seed string, offset *uint64, remaining *int, out *[]driver.SelectionKey) error {
	for base := uint64(0); base < root.Commitment.Count && *remaining > 0; {
		first, err := t.rank(scope, root, base)
		if err != nil {
			return err
		}
		var descriptor source
		present, err := t.read(sourceKey(first.Ref.ID), &descriptor)
		if err != nil {
			return err
		}
		if !present || descriptor.Leaf == nil || descriptor.Leaf.Sort == nil {
			return unavailable("missing public sort tuple")
		}
		prefix, err := driver.ReadyOrderGroupPrefix(*descriptor.Leaf)
		if err != nil {
			return err
		}
		group, err := t.atPrefix(scope, root, prefix)
		if err != nil {
			return err
		}
		if group == nil || group.Commitment.Count == 0 {
			return unavailable("public sort group missing")
		}
		count := group.Commitment.Count
		base += count
		if *offset >= count {
			*offset -= count
			continue
		}
		pivot := driver.ReadyOrderPivot(seed, descriptor.Leaf.Priority, count)
		for _, span := range [][2]uint64{{pivot, count}, {0, pivot}} {
			length := span[1] - span[0]
			if *offset >= length {
				*offset -= length
				continue
			}
			start := span[0] + *offset
			take := min(*remaining, int(length-*offset))
			*offset = 0
			before := take
			if err = t.rangeKeys(scope, group, &start, &take, out); err != nil {
				return err
			}
			*remaining -= before - take
			if *remaining == 0 {
				break
			}
		}
	}
	return nil
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
		prefix, err := driver.ReadyOrderTreeKey(*s.Leaf)
		if err != nil {
			return nil, err
		}
		n := driver.ReadyOrderNode{Prefix: prefix, Leaf: s.Leaf}
		if err = driver.SealReadyOrderNode(&n); err != nil {
			return nil, err
		}
		t.work.DigestBytes += n.DigestBytes()
		out = append(out, *s.Leaf)
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := driver.ReadyOrderTreeKey(out[i])
		b, _ := driver.ReadyOrderTreeKey(out[j])
		return a < b
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
	if !present || coverage.Version != 2 || coverage.Incarnation != v.Incarnation || coverage.Through.String() != v.At.String() || coverage.Unsafe != 0 {
		return result, unavailable("source, lease, heartbeat or nonlocal coverage")
	}
	if coverage.OrderMode == "public" {
		now := time.Now().UTC()
		if now.Before(coverage.OrderAt) || !coverage.ValidUntil.IsZero() && !now.Before(coverage.ValidUntil) {
			return result, unavailable("public ordering age")
		}
	} else if coverage.OrderMode != "" {
		return result, unavailable("unknown Ready ordering")
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
		return selectFinite(t, p, scope, root, coverage.OrderMode)
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
	} else if coverage.OrderMode == "public" {
		e = t.seededPublic(scope, root, p.Seed, &offset, &remaining, &result.Keys)
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
func selectFinite(t *tree, p driver.ReadyOrderPlan, scope string, root *driver.ReadyOrderNode, mode string) (driver.ReadyOrderResult, error) {
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
		if mode == "public" {
			var err error
			authorized, err = t.seedFinitePublic(scope, root, p.Seed, raw, authorized)
			if err != nil {
				return result, err
			}
		} else {
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
	}
	end := min(result.Total, p.Offset+uint64(p.Limit))
	for _, leaf := range authorized[p.Offset:end] {
		result.Keys = append(result.Keys, leaf.Key)
	}
	result.More = end < result.Total
	return result, nil
}

func (t *tree) seedFinitePublic(scope string, root *driver.ReadyOrderNode, seed string, raw, authorized []driver.ReadyOrderLeaf) ([]driver.ReadyOrderLeaf, error) {
	pivots := map[string]string{}
	for _, leaf := range authorized {
		if leaf.Sort == nil {
			return nil, unavailable("missing public sort tuple")
		}
		prefix, err := driver.ReadyOrderGroupPrefix(leaf)
		if err != nil {
			return nil, err
		}
		if _, known := pivots[prefix]; known {
			continue
		}
		if raw != nil {
			group := []driver.ReadyOrderLeaf{}
			for _, candidate := range raw {
				candidatePrefix, e := driver.ReadyOrderGroupPrefix(candidate)
				if e != nil {
					return nil, e
				}
				if candidatePrefix == prefix {
					group = append(group, candidate)
				}
			}
			if len(group) == 0 {
				return nil, unavailable("authorized item absent from raw group")
			}
			pivot := group[driver.ReadyOrderPivot(seed, group[0].Priority, uint64(len(group)))]
			pivots[prefix], err = driver.ReadyOrderTreeKey(pivot)
		} else {
			group, e := t.atPrefix(scope, root, prefix)
			if e != nil {
				return nil, e
			}
			if group == nil {
				return nil, unavailable("public sort group missing")
			}
			first, e := t.rank(scope, group, 0)
			if e != nil {
				return nil, e
			}
			var firstSource source
			present, e := t.read(sourceKey(first.Ref.ID), &firstSource)
			if e != nil {
				return nil, e
			}
			if !present || firstSource.Leaf == nil || firstSource.Leaf.Sort == nil {
				return nil, unavailable("missing public sort source")
			}
			pivotKey, e := t.rank(scope, group, driver.ReadyOrderPivot(seed, firstSource.Leaf.Priority, group.Commitment.Count))
			if e != nil {
				return nil, e
			}
			var pivotSource source
			present, e = t.read(sourceKey(pivotKey.Ref.ID), &pivotSource)
			if e != nil {
				return nil, e
			}
			if !present || pivotSource.Leaf == nil {
				return nil, unavailable("missing public sort pivot")
			}
			pivots[prefix], err = driver.ReadyOrderTreeKey(*pivotSource.Leaf)
		}
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(authorized, func(i, j int) bool {
		a, _ := driver.ReadyOrderTreeKey(authorized[i])
		b, _ := driver.ReadyOrderTreeKey(authorized[j])
		ga, gb := a[:34], b[:34]
		if ga != gb {
			return ga < gb
		}
		pivot := pivots[ga]
		if (a < pivot) != (b < pivot) {
			return a >= pivot
		}
		return a < b
	})
	return authorized, nil
}

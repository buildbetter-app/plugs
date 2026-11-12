package memory

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// A persistent AVL identity index shares untouched nodes between snapshots.
// Publication updates one search path per changed identity, never a parent's
// child vector. Historical membership is tested from version headers only.
type childIDNode struct {
	left, right *childIDNode
	order, key  string
	height      int
}

func childHeight(n *childIDNode) int {
	if n == nil {
		return 0
	}
	return n.height
}
func childNode(n *childIDNode, left, right *childIDNode) *childIDNode {
	return &childIDNode{left: left, right: right, order: n.order, key: n.key, height: 1 + max(childHeight(left), childHeight(right))}
}
func childRotateLeft(n *childIDNode) *childIDNode {
	r := n.right
	return childNode(r, childNode(n, n.left, r.left), r.right)
}
func childRotateRight(n *childIDNode) *childIDNode {
	l := n.left
	return childNode(l, l.left, childNode(n, l.right, n.right))
}
func childBalance(n *childIDNode) *childIDNode {
	if childHeight(n.left)-childHeight(n.right) > 1 {
		if childHeight(n.left.left) < childHeight(n.left.right) {
			n = childNode(n, childRotateLeft(n.left), n.right)
		}
		return childRotateRight(n)
	}
	if childHeight(n.right)-childHeight(n.left) > 1 {
		if childHeight(n.right.right) < childHeight(n.right.left) {
			n = childNode(n, n.left, childRotateRight(n.right))
		}
		return childRotateLeft(n)
	}
	return n
}
func childInsert(n *childIDNode, order, key string) *childIDNode {
	if n == nil {
		return &childIDNode{order: order, key: key, height: 1}
	}
	if order == n.order {
		return n
	}
	if order < n.order {
		return childBalance(childNode(n, childInsert(n.left, order, key), n.right))
	}
	return childBalance(childNode(n, n.left, childInsert(n.right, order, key)))
}

func childRemove(n *childIDNode, order string) *childIDNode {
	if n == nil {
		return nil
	}
	if order < n.order {
		return childBalance(childNode(n, childRemove(n.left, order), n.right))
	}
	if order > n.order {
		return childBalance(childNode(n, n.left, childRemove(n.right, order)))
	}
	if n.left == nil {
		return n.right
	}
	if n.right == nil {
		return n.left
	}
	successor := n.right
	for successor.left != nil {
		successor = successor.left
	}
	return childBalance(childNode(successor, n.left, childRemove(n.right, successor.order)))
}

func childMaximum(ctx context.Context, root *childIDNode, parent string, eligible func(string) bool) (uint64, error) {
	lower, upper := parent+"\x00", parent+"\x01"
	var seek func(*childIDNode) (string, bool, error)
	seek = func(n *childIDNode) (string, bool, error) {
		if n == nil {
			return "", false, nil
		}
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		driver.ReportReadWork(ctx, driver.ReadChildIDCandidates, 1)
		if n.order < lower {
			return seek(n.right)
		}
		if n.order >= upper {
			return seek(n.left)
		}
		if rank, found, err := seek(n.right); found || err != nil {
			return rank, found, err
		}
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		if eligible(n.key) {
			return strings.SplitN(n.order[len(lower):], "\x00", 2)[0], true, nil
		}
		return seek(n.left)
	}
	rank, found, err := seek(root)
	if err != nil || !found {
		return 0, err
	}
	if rank == "~" {
		return 0, fmt.Errorf("%w: child counter overflow", driver.ErrUnsupported)
	}
	return strconv.ParseUint(rank, 10, 64)
}

func (r *reader) MaxChildNumber(ctx context.Context, parent string, excluded []model.Ref) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return 0, err
	}
	exclude := map[string]bool{}
	for _, ref := range excluded {
		key, err := recordKey(ref)
		if err != nil {
			return 0, err
		}
		exclude[key] = true
	}
	root := r.state.childIDs
	head, _ := r.state.meta.Head.Uint64()
	if r.at != head {
		root = r.state.childIDHistory
	}
	return childMaximum(ctx, root, parent, func(key string) bool {
		if exclude[key] {
			return false
		}
		if r.at == head {
			_, live := r.state.records[key]
			return live
		}
		versions := r.state.versions[key]
		// Header-only historical visibility; no body reconstruction.
		for i := len(versions) - 1; i >= 0; i-- {
			at, _ := versions[i].Revision.Uint64()
			if at <= r.at {
				return !versions[i].Deleted
			}
		}
		return false
	})
}

func (t *transaction) MaxChildNumber(ctx context.Context, parent string, excluded []model.Ref) (uint64, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return 0, err
	}
	exclude := map[string]bool{}
	for _, ref := range excluded {
		key, err := recordKey(ref)
		if err != nil {
			return 0, err
		}
		exclude[key] = true
	}
	maximum, err := childMaximum(ctx, t.base.childIDs, parent, func(key string) bool {
		if exclude[key] {
			return false
		}
		if _, replaced := t.records[key]; replaced {
			return false
		}
		_, live := t.base.records[key]
		return live
	})
	if err != nil {
		return 0, err
	}
	for key, record := range t.records {
		driver.ReportReadWork(ctx, driver.ReadChildIDCandidates, 1)
		if record == nil || record.Kind != "bead" || exclude[key] {
			continue
		}
		p, rank := driver.ChildIDOrder(key)
		if p != parent {
			continue
		}
		driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		if rank == "~" {
			return 0, fmt.Errorf("%w: child counter overflow", driver.ErrUnsupported)
		}
		n, err := strconv.ParseUint(rank, 10, 64)
		if err != nil {
			return 0, err
		}
		maximum = max(maximum, n)
	}
	return maximum, nil
}

var _ driver.NumericChildReader = (*reader)(nil)
var _ driver.NumericChildReader = (*transaction)(nil)

package memory

import (
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This persistent AVL census changes only paths to affected rows. Snapshot
// publication must not copy or reprove an entire selection family per write.
// The row, source and reverse-target indexes contain driver envelope metadata;
// no logical key or value is decoded here.
type selectionCensusNode struct {
	key         string
	owner       selectionRowOwner
	left, right *selectionCensusNode
	height      int
}

func censusHeight(n *selectionCensusNode) int {
	if n == nil {
		return 0
	}
	return n.height
}

func censusNode(key string, owner selectionRowOwner, left, right *selectionCensusNode) *selectionCensusNode {
	return &selectionCensusNode{key: key, owner: owner, left: left, right: right, height: 1 + max(censusHeight(left), censusHeight(right))}
}

func censusBalance(key string, owner selectionRowOwner, left, right *selectionCensusNode) *selectionCensusNode {
	if censusHeight(left) > censusHeight(right)+1 {
		if censusHeight(left.right) > censusHeight(left.left) {
			pivot := left.right
			left = censusNode(pivot.key, pivot.owner, censusNode(left.key, left.owner, left.left, pivot.left), pivot.right)
		}
		return censusNode(left.key, left.owner, left.left, censusNode(key, owner, left.right, right))
	}
	if censusHeight(right) > censusHeight(left)+1 {
		if censusHeight(right.left) > censusHeight(right.right) {
			pivot := right.left
			right = censusNode(pivot.key, pivot.owner, pivot.left, censusNode(right.key, right.owner, pivot.right, right.right))
		}
		return censusNode(right.key, right.owner, censusNode(key, owner, left, right.left), right.right)
	}
	return censusNode(key, owner, left, right)
}

func (n *selectionCensusNode) get(key string) (selectionRowOwner, bool) {
	for n != nil {
		switch {
		case key < n.key:
			n = n.left
		case key > n.key:
			n = n.right
		default:
			return n.owner, true
		}
	}
	return selectionRowOwner{}, false
}

func (n *selectionCensusNode) put(key string, owner selectionRowOwner) *selectionCensusNode {
	if n == nil {
		return censusNode(key, owner, nil, nil)
	}
	switch {
	case key < n.key:
		return censusBalance(n.key, n.owner, n.left.put(key, owner), n.right)
	case key > n.key:
		return censusBalance(n.key, n.owner, n.left, n.right.put(key, owner))
	default:
		return censusNode(key, owner, n.left, n.right)
	}
}

func (n *selectionCensusNode) remove(key string) *selectionCensusNode {
	if n == nil {
		return nil
	}
	switch {
	case key < n.key:
		return censusBalance(n.key, n.owner, n.left.remove(key), n.right)
	case key > n.key:
		return censusBalance(n.key, n.owner, n.left, n.right.remove(key))
	case n.left == nil:
		return n.right
	case n.right == nil:
		return n.left
	default:
		first := n.right
		for first.left != nil {
			first = first.left
		}
		return censusBalance(first.key, first.owner, n.left, n.right.remove(first.key))
	}
}

func (n *selectionCensusNode) each(fn func(string, selectionRowOwner)) {
	if n != nil {
		n.left.each(fn)
		fn(n.key, n.owner)
		n.right.each(fn)
	}
}

func (n *selectionCensusNode) prefix(prefix string, fn func(string, selectionRowOwner)) {
	if n == nil {
		return
	}
	if n.key >= prefix {
		n.left.prefix(prefix, fn)
	}
	if strings.HasPrefix(n.key, prefix) {
		fn(n.key[len(prefix):], n.owner)
	}
	if n.key < prefix || strings.HasPrefix(n.key, prefix) {
		n.right.prefix(prefix, fn)
	}
}

func (p *selectionCoverageState) sourceRows(source string, fn func(string, selectionRowOwner)) {
	p.sources.prefix(source+"\x00", fn)
}

func selectionRowTargets(state driver.LockState, fn func(string)) {
	if state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedAncestorClosure || state.Derived == nil || state.Derived.Index == nil || state.Derived.Index.Facts == nil {
		return
	}
	for _, fact := range state.Derived.Index.Facts.Rows {
		if fact.Target != nil {
			fn(keyID(driver.LockKey{Kind: driver.LockRecord, Ref: fact.Target}))
		}
	}
}

func (p *selectionCoverageState) removeOwner(id string, old driver.LockState) {
	owner, ok := p.owners.get(id)
	if !ok {
		return
	}
	p.owners = p.owners.remove(id)
	p.sources = p.sources.remove(owner.source + "\x00" + id)
	selectionRowTargets(old, func(target string) { p.targets = p.targets.remove(target + "\x00" + id) })
}

func (p *selectionCoverageState) putOwner(id string, owner selectionRowOwner, row driver.LockState) {
	p.owners = p.owners.put(id, owner)
	p.sources = p.sources.put(owner.source+"\x00"+id, owner)
	selectionRowTargets(row, func(target string) { p.targets = p.targets.put(target+"\x00"+id, owner) })
}

package readyindex

import (
	"context"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ExistenceKeys declares every committed point row needed for a boolean
// observation. Engines load these and the current Head in one SQL statement.
func ExistenceKeys(p driver.ReadyOrderPlan) ([]driver.LockKey, error) {
	scope := scopeID(p.Scope)
	keys := []driver.LockKey{lock(markerKey), lock(headerKey(scope)), lock(nodeKey(scope, ""))}
	if p.UniformAuthorization && p.RawRefs == nil {
		return keys, nil
	}
	refs := p.AuthorizedRefs
	if p.UniformAuthorization {
		refs = p.RawRefs
	}
	set, err := refSet(refs)
	if err != nil {
		return nil, err
	}
	if !p.UniformAuthorization && p.RawRefs != nil {
		raw, err := refSet(p.RawRefs)
		if err != nil {
			return nil, err
		}
		for id := range set {
			if !raw[id] {
				delete(set, id)
			}
		}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		keys = append(keys, lock(sourceKey(id)))
	}
	return keys, nil
}

type committedReadyObservation struct {
	view   driver.View
	states map[string]driver.LockState
}

func (r committedReadyObservation) Snapshot() driver.View { return r.view }
func (r committedReadyObservation) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	states := make([]driver.LockState, len(keys))
	for i, key := range keys {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return nil, err
		}
		var found bool
		states[i], found = r.states[encoded]
		if !found {
			return nil, unavailable("incomplete fresh committed observation")
		}
	}
	return states, nil
}

// ExistsCommitted accepts the one-statement committed observation collected
// by a post-Head SQL adapter. It cannot fetch additional rows or inspect any
// staged transaction overlay while validating completeness and eligibility.
func ExistsCommitted(ctx context.Context, view driver.View, p driver.ReadyOrderPlan, states []driver.LockState) (bool, error) {
	keys, err := ExistenceKeys(p)
	if err != nil {
		return false, err
	}
	if len(keys) != len(states) {
		return false, unavailable("incomplete fresh committed observation")
	}
	r := committedReadyObservation{view: view, states: make(map[string]driver.LockState, len(states))}
	for i, key := range keys {
		expected, err := driver.EncodeLockKey(key)
		if err != nil {
			return false, err
		}
		actual, err := driver.EncodeLockKey(states[i].Key)
		if err != nil || expected != actual {
			return false, unavailable("fresh committed identity mismatch")
		}
		r.states[actual] = states[i]
	}
	return Exists(ctx, r, p)
}

// Exists evaluates the complete maintained Ready domain through a fresh
// committed observer. Only an engine's post-Head adapter may provide that
// observer; an ordinary snapshot Reader is not a freshness certificate.
// Coverage gaps refuse rather than becoming an empty queue.
func Exists(ctx context.Context, r Observer, p driver.ReadyOrderPlan) (bool, error) {
	v := r.Snapshot()
	if v.At == nil || !v.Branch.IsZero() || p.Incarnation != v.Incarnation || p.ContentRevision.Validate() != nil || p.AuthorityRevision.Validate() != nil || p.ContentRevision.String() != v.At.String() || p.AuthorityRevision.String() != v.At.String() || p.Eligibility == "" || p.Offset != 0 {
		return false, unavailable("fresh plan binding")
	}
	if p.UniformAuthorization && p.AuthorizedRefs != nil || !p.UniformAuthorization && p.AuthorizedRefs == nil {
		return false, unavailable("incomplete fresh authorization boundary")
	}
	t := newTree(ctx, r)
	var coverage marker
	present, err := t.read(markerKey, &coverage)
	if err != nil {
		return false, err
	}
	if !present || coverage.Version != 2 || coverage.Incarnation != v.Incarnation || coverage.Through.String() != v.At.String() || coverage.Unsafe != 0 {
		return false, unavailable("fresh source, lease or nonlocal coverage")
	}
	scope := scopeID(p.Scope)
	var header scopeHeader
	if _, err = t.read(headerKey(scope), &header); err != nil {
		return false, err
	}
	for binding, n := range header.Bindings {
		if n == 0 || binding != p.Eligibility {
			return false, unavailable("fresh effective ontology changed")
		}
	}
	root, err := t.node(scope, "")
	if err != nil {
		return false, err
	}
	if p.UniformAuthorization && p.RawRefs == nil {
		return root.Commitment.Count != 0, ctx.Err()
	}
	refs := p.AuthorizedRefs
	if p.UniformAuthorization {
		refs = p.RawRefs
	}
	set, err := refSet(refs)
	if err != nil {
		return false, err
	}
	if !p.UniformAuthorization && p.RawRefs != nil {
		raw, err := refSet(p.RawRefs)
		if err != nil {
			return false, err
		}
		for id := range set {
			if !raw[id] {
				delete(set, id)
			}
		}
	}
	leaves, err := t.leaves(set, p.Scope)
	return len(leaves) != 0, err
}

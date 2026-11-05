// Package readyindex maintains exact Ready order through ordinary opaque
// derived rows. Every read is a point lookup; no source bodies, COUNT, OFFSET,
// full leaf streams, engine introspection or asynchronous maintenance is used.
package readyindex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const prefix = "plug.ready.v2/"
const markerKey = prefix + "coverage"

// This valid-shaped hash exists only in the private node cache. materialize
// replaces it by sealing every surviving dirty node, children before parents.
const unpublishedNodeHash = "0000000000000000000000000000000000000000000000000000000000000000"

type Observer interface {
	Snapshot() driver.View
	Observe(context.Context, []driver.LockKey) ([]driver.LockState, error)
}
type marker struct {
	Version     int
	Incarnation string
	Through     model.Revision
	Unsafe      uint64
	OrderMode   string
	OrderAt     time.Time
	ValidUntil  time.Time
}
type scopeHeader struct{ Bindings map[string]uint64 }
type source struct {
	Project, Type, Binding string
	Local                  bool
	Leaf                   *driver.ReadyOrderLeaf
}

// Mutation is a trusted, final core source image. A nil Item denotes deletion;
// an ineligible source still needs a descriptor to account for coverage.
type Mutation = driver.ReadyOrderMutation
type tree struct {
	ctx    context.Context
	r      Observer
	rows   map[string]*driver.DerivedRow
	before map[string]driver.LockState
	nodes  map[string]*driver.ReadyOrderNode
	dirty  map[string]bool
	work   driver.ReadyOrderWork
}

func newTree(ctx context.Context, r Observer) *tree {
	return &tree{ctx: ctx, r: r, rows: map[string]*driver.DerivedRow{}, before: map[string]driver.LockState{}, nodes: map[string]*driver.ReadyOrderNode{}, dirty: map[string]bool{}}
}
func lock(key string) driver.LockKey {
	return driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: key}}
}
func scopeID(s driver.ReadyOrderScope) string {
	b, _ := json.Marshal(s)
	return base64.RawURLEncoding.EncodeToString(b)
}
func nodeKey(scope, p string) string { return prefix + "node/" + scope + "/" + p }
func headerKey(scope string) string  { return prefix + "scope/" + scope }
func sourceKey(id string) string {
	return prefix + "source/" + base64.RawURLEncoding.EncodeToString([]byte(id))
}
func scopes(s source) []string {
	p, t := s.Project, s.Type
	return []string{scopeID(driver.ReadyOrderScope{}), scopeID(driver.ReadyOrderScope{Project: &p}), scopeID(driver.ReadyOrderScope{Type: &t}), scopeID(driver.ReadyOrderScope{Project: &p, Type: &t})}
}
func unavailable(reason string) error {
	return fmt.Errorf("%w: Ready order %s", driver.ErrUnsupported, reason)
}
func (t *tree) read(key string, out any) (bool, error) {
	row, known := t.rows[key]
	if !known {
		states, err := t.r.Observe(t.ctx, []driver.LockKey{lock(key)})
		if err != nil {
			return false, err
		}
		if len(states) != 1 {
			return false, unavailable("incomplete point observation")
		}
		original := states[0]
		if original.Derived != nil {
			copy := original.Derived.Clone()
			original.Derived = &copy
		}
		t.before[key] = original
		if states[0].Present {
			if states[0].Derived == nil || states[0].Derived.Key != key {
				return false, unavailable("missing node bytes")
			}
			v := *states[0].Derived
			row = &v
		}
		t.rows[key] = row
		t.work.Nodes++
	}
	if row == nil {
		return false, nil
	}
	if err := json.Unmarshal(row.Value, out); err != nil {
		return false, unavailable("malformed metadata")
	}
	return true, nil
}
func (t *tree) write(key string, value any) error {
	if _, known := t.rows[key]; !known {
		var ignored json.RawMessage
		if _, err := t.read(key, &ignored); err != nil {
			return err
		}
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	t.rows[key] = &driver.DerivedRow{Key: key, Value: b}
	t.dirty[key] = true
	return nil
}
func (t *tree) remove(key string) error {
	if _, known := t.rows[key]; !known {
		var ignored json.RawMessage
		if _, err := t.read(key, &ignored); err != nil {
			return err
		}
	}
	t.rows[key] = nil
	t.dirty[key] = true
	delete(t.nodes, key)
	return nil
}
func (t *tree) node(scope, p string) (*driver.ReadyOrderNode, error) {
	key := nodeKey(scope, p)
	if n, ok := t.nodes[key]; ok {
		return n, nil
	}
	var n driver.ReadyOrderNode
	present, err := t.read(key, &n)
	if err != nil {
		return nil, err
	}
	if !present {
		if p != "" {
			return nil, unavailable("child absent")
		}
		n.Prefix = ""
		if err := driver.SealReadyOrderNode(&n); err != nil {
			return nil, err
		}
	} else {
		original := n.Commitment
		if n.Prefix != p || driver.SealReadyOrderNode(&n) != nil || n.Commitment != original {
			return nil, unavailable("invalid counted commitment")
		}
	}
	t.nodes[key] = &n
	t.work.DigestBytes += n.DigestBytes()
	if n.Leaf != nil {
		t.work.Leaves++
	}
	return &n, nil
}
func (t *tree) save(scope string, n *driver.ReadyOrderNode) error {
	sort.Slice(n.Children, func(i, j int) bool { return n.Children[i].Prefix < n.Children[j].Prefix })
	count, err := driver.ValidateReadyOrderNode(n)
	if err != nil {
		return err
	}
	n.Commitment = driver.ReadyCommitment{Count: count, Hash: unpublishedNodeHash}
	key := nodeKey(scope, n.Prefix)
	t.nodes[key] = n
	// Every intermediate image is validated and every before-image observed.
	// Only materialize seals and serializes dirty nodes for publication, after
	// exact source revisions and all final child commitments are installed.
	if _, known := t.rows[key]; !known {
		var ignored json.RawMessage
		if _, err := t.read(key, &ignored); err != nil {
			return err
		}
	}
	t.rows[key] = &driver.DerivedRow{Key: key}
	t.dirty[key] = true
	return nil
}
func edge(n *driver.ReadyOrderNode) driver.ReadyOrderChild {
	return driver.ReadyOrderChild{Prefix: n.Prefix, Commitment: n.Commitment}
}

// replace descends only the changed radix path. All new split-node identities
// depend on source sort keys, so their locks are known before Head allocates
// the source revision. Delete compresses unary nodes to the canonical shape.
func (t *tree) replace(scope, p, key string, leaf *driver.ReadyOrderLeaf) (*driver.ReadyOrderNode, error) {
	n, err := t.node(scope, p)
	if err != nil {
		return nil, err
	}
	if key == p {
		n.Leaf = leaf
	} else {
		i := sort.Search(len(n.Children), func(i int) bool { return n.Children[i].Prefix[:len(p)+2] >= key[:len(p)+2] })
		if i == len(n.Children) || n.Children[i].Prefix[:len(p)+2] != key[:len(p)+2] {
			if leaf == nil {
				return n, nil
			}
			child := &driver.ReadyOrderNode{Prefix: key, Leaf: leaf}
			if err = t.save(scope, child); err != nil {
				return nil, err
			}
			n.Children = append(n.Children, edge(child))
		} else {
			old := n.Children[i]
			common := driver.CommonReadyPrefix(old.Prefix, key)
			var child *driver.ReadyOrderNode
			if common == old.Prefix {
				child, err = t.replace(scope, old.Prefix, key, leaf)
			} else if leaf == nil {
				return n, nil
			} else {
				child = &driver.ReadyOrderNode{Prefix: common, Children: []driver.ReadyOrderChild{old}}
				if key == common {
					child.Leaf = leaf
				} else {
					l := &driver.ReadyOrderNode{Prefix: key, Leaf: leaf}
					err = t.save(scope, l)
					child.Children = append(child.Children, edge(l))
				}
				if err == nil {
					err = t.save(scope, child)
				}
			}
			if err != nil {
				return nil, err
			}
			if child == nil {
				n.Children = append(n.Children[:i], n.Children[i+1:]...)
			} else {
				n.Children[i] = edge(child)
			}
		}
	}
	if p != "" && n.Leaf == nil && len(n.Children) <= 1 {
		if err = t.remove(nodeKey(scope, p)); err != nil {
			return nil, err
		}
		if len(n.Children) == 0 {
			return nil, nil
		}
		return t.node(scope, n.Children[0].Prefix)
	}
	if err = t.save(scope, n); err != nil {
		return nil, err
	}
	return n, nil
}

type Delta struct {
	t            *tree
	coverage     marker
	mutations    map[string]Mutation
	sentinels    map[driver.Sentinel]bool
	headers      map[string]*scopeHeader
	valid        bool
	published    bool
	registration driver.ReadyOrderPublication
}

type readyOrderPublicationFinalizer interface {
	FinalizeReadyOrderPublication(context.Context, driver.ReadyOrderPublication) error
}

// Prepare starts only at the proven empty initial Head, or continues an exact
// previously maintained Head. A skipped/raw publication is a coverage gap;
// present final images never retroactively certify it. Historical rows are
// retained by the ordinary driver derived-row/version machinery.
func Prepare(ctx context.Context, r Observer, mutations []Mutation) (*Delta, error) {
	t := newTree(ctx, r)
	d := &Delta{t: t, mutations: map[string]Mutation{}, sentinels: map[driver.Sentinel]bool{}, headers: map[string]*scopeHeader{}}
	v := r.Snapshot()
	if v.At == nil || v.Incarnation == "" || !v.Branch.IsZero() {
		return nil, unavailable("snapshot")
	}
	present, err := t.read(markerKey, &d.coverage)
	if err != nil {
		return nil, err
	}
	if !present {
		if !v.At.IsZero() {
			return d, nil
		}
		d.coverage = marker{Version: 2, Incarnation: v.Incarnation, Through: model.Natural64(0)}
	}
	if d.coverage.Version != 2 || d.coverage.Incarnation != v.Incarnation || d.coverage.Through.String() != v.At.String() {
		return d, nil
	}
	d.valid = true
	// The literal global scope guards coverage/control metadata. It is a
	// canonical predicate key, not a wildcard-matching rule in the driver.
	global, err := driver.NewReadySentinel("*", "*")
	if err != nil {
		return nil, err
	}
	d.sentinels[global] = true
	// Control publications carry the same semantic root and eligibility
	// census. Observe these paths before Head even when no bead is changing.
	globalScope := scopeID(driver.ReadyOrderScope{})
	if _, err = t.node(globalScope, ""); err != nil {
		return nil, err
	}
	var globalHeader scopeHeader
	if _, err = t.read(headerKey(globalScope), &globalHeader); err != nil {
		return nil, err
	}
	for _, m := range mutations {
		if driver.ValidateHierarchyRef(m.Ref) != nil || m.Priority < 0 || m.Priority > 4 {
			return nil, unavailable("mutation identity or priority")
		}
		if _, duplicate := d.mutations[m.Ref.ID]; duplicate {
			return nil, unavailable("duplicate mutation")
		}
		d.mutations[m.Ref.ID] = m
		var old source
		found, e := t.read(sourceKey(m.Ref.ID), &old)
		if e != nil {
			return nil, e
		}
		if found {
			if err = d.adjust(old, -1); err != nil {
				return nil, err
			}
		}
		if m.Item == nil {
			if err = t.remove(sourceKey(m.Ref.ID)); err != nil {
				return nil, err
			}
			continue
		}
		if m.Binding == "" || m.Item.ID != m.Ref.ID || m.Item.Project != m.Project || m.Item.Type != m.Type {
			return nil, unavailable("incomplete mutation")
		}
		if m.Eligible {
			if m.Sort != nil && d.coverage.OrderMode == "" {
				if t.nodes[nodeKey(globalScope, "")].Commitment.Count != 0 {
					return nil, unavailable("mixed public ordering and legacy tree")
				}
				d.coverage.OrderMode = "public"
			}
			if (m.Sort != nil) != (d.coverage.OrderMode == "public") {
				return nil, unavailable("mixed Ready ordering")
			}
		}
		s := source{Project: m.Project, Type: m.Type, Binding: m.Binding, Local: m.Local}
		if m.Eligible {
			item := *m.Item
			item.Revision = model.Natural64(0)
			l, e := driver.ReadyOrderLeafFor(item, m.Priority)
			if e != nil {
				return nil, e
			}
			if m.Sort != nil {
				value := *m.Sort
				l.Sort = &value
				if _, e = driver.ReadyOrderTreeKey(l); e != nil {
					return nil, e
				}
				if value.RankedAt.After(d.coverage.OrderAt) {
					d.coverage.OrderAt = value.RankedAt
				}
				if value.Bucket == 0 {
					deadline := value.CreatedAt.Add(48 * time.Hour)
					if d.coverage.ValidUntil.IsZero() || deadline.Before(d.coverage.ValidUntil) {
						d.coverage.ValidUntil = deadline
					}
				}
			}
			s.Leaf = &l
		}
		if err = d.adjust(s, 1); err != nil {
			return nil, err
		}
		if err = t.write(sourceKey(m.Ref.ID), s); err != nil {
			return nil, err
		}
	}
	// Many mutations share the same four scope headers. Their point
	// observations happen at first use, while the final header image needs
	// serialization only once after every binding count is settled.
	for scope, header := range d.headers {
		if err = t.write(headerKey(scope), header); err != nil {
			return nil, err
		}
	}
	if err = t.write(markerKey, d.coverage); err != nil {
		return nil, err
	}
	return d, nil
}
func (d *Delta) adjust(s source, direction int) error {
	project := s.Project
	if project == "" {
		project = "*"
	}
	sentinel, err := driver.NewReadySentinel(project, s.Type)
	if err != nil {
		return unavailable("predicate scope")
	}
	d.sentinels[sentinel] = true
	if !s.Local {
		if direction < 0 {
			if d.coverage.Unsafe == 0 {
				return unavailable("unsafe count underflow")
			}
			d.coverage.Unsafe--
		} else {
			d.coverage.Unsafe++
		}
	}
	for _, scope := range scopes(s) {
		header := d.headers[scope]
		if header == nil {
			header = &scopeHeader{}
			if _, err := d.t.read(headerKey(scope), header); err != nil {
				return err
			}
			if header.Bindings == nil {
				header.Bindings = map[string]uint64{}
			}
			d.headers[scope] = header
		}
		if direction < 0 {
			if header.Bindings[s.Binding] == 0 {
				return unavailable("binding count underflow")
			}
			header.Bindings[s.Binding]--
			if header.Bindings[s.Binding] == 0 {
				delete(header.Bindings, s.Binding)
			}
		} else {
			header.Bindings[s.Binding]++
		}
		if s.Leaf != nil {
			var l *driver.ReadyOrderLeaf
			if direction > 0 {
				l = s.Leaf
			}
			key, err := driver.ReadyOrderTreeKey(*s.Leaf)
			if err != nil {
				return err
			}
			if _, err := d.t.replace(scope, "", key, l); err != nil {
				return err
			}
		}
	}
	return nil
}
func (d *Delta) Available() bool { return d != nil && d.valid }

// BaseRevision is the exact source Head observed during preparation.
func (d *Delta) BaseRevision() model.Revision { return d.coverage.Through }
func (d *Delta) Locks() []driver.LockKey {
	if !d.Available() {
		return nil
	}
	type encodedLock struct {
		key driver.LockKey
		id  string
	}
	ordered := make([]encodedLock, 0, len(d.t.rows)+len(d.sentinels))
	appendKey := func(key driver.LockKey) {
		id, _ := driver.EncodeLockKey(key)
		ordered = append(ordered, encodedLock{key: key, id: id})
	}
	for key := range d.t.rows {
		appendKey(lock(key))
	}
	for sentinel := range d.sentinels {
		appendKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].id < ordered[j].id })
	keys := make([]driver.LockKey, len(ordered))
	for i, entry := range ordered {
		keys[i] = entry.key
	}
	return keys
}

// Publish allocates no locks and reads no new nodes. It stamps exact H+1 source
// revisions, reseals the already declared ancestor paths, then stages all rows
// in the source transaction. Rollback therefore removes the entire change.
func (d *Delta) materialize(revision model.Revision) ([]driver.DerivedRow, []driver.DerivedRow, error) {
	if !d.Available() {
		return nil, nil, nil
	}
	if d.published {
		return nil, nil, fmt.Errorf("Ready projection already published")
	}
	d.published = true
	next, e := d.coverage.Through.Uint64()
	actual, e2 := revision.Uint64()
	if e != nil || e2 != nil || next == ^uint64(0) || actual != next+1 {
		return nil, nil, unavailable("publication revision")
	}
	for id, m := range d.mutations {
		if m.Item == nil || !m.Eligible {
			continue
		}
		item := *m.Item
		item.Revision = revision
		leaf, err := driver.ReadyOrderLeafFor(item, m.Priority)
		if err != nil {
			return nil, nil, err
		}
		if m.Sort != nil {
			value := *m.Sort
			leaf.Sort = &value
		}
		var s source
		if _, err = d.t.read(sourceKey(id), &s); err != nil {
			return nil, nil, err
		}
		s.Leaf = &leaf
		if err = d.t.write(sourceKey(id), s); err != nil {
			return nil, nil, err
		}
		for _, scope := range scopes(s) {
			prefix, err := driver.ReadyOrderTreeKey(leaf)
			if err != nil {
				return nil, nil, err
			}
			key := nodeKey(scope, prefix)
			n := d.t.nodes[key]
			if n == nil || n.Leaf == nil {
				return nil, nil, unavailable("undeclared revision leaf")
			}
			n.Leaf = &leaf
		}
	}
	keys := make([]string, 0, len(d.t.dirty))
	for k := range d.t.dirty {
		if strings.HasPrefix(k, prefix+"node/") && d.t.rows[k] != nil {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })
	for _, key := range keys {
		n := d.t.nodes[key]
		base := key[:len(key)-len(n.Prefix)]
		for i, c := range n.Children {
			if child := d.t.nodes[base+c.Prefix]; child != nil {
				n.Children[i] = edge(child)
			}
		}
		if err := driver.SealReadyOrderNode(n); err != nil {
			return nil, nil, err
		}
		d.t.work.DigestBytes += n.DigestBytes()
		b, err := json.Marshal(n)
		if err != nil {
			return nil, nil, err
		}
		d.t.rows[key] = &driver.DerivedRow{Key: key, Value: b}
	}
	d.coverage.Through = revision
	if err := d.t.write(markerKey, d.coverage); err != nil {
		return nil, nil, err
	}
	keys = keys[:0]
	for k := range d.t.dirty {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	removed, added := []driver.DerivedRow{}, []driver.DerivedRow{}
	for _, key := range keys {
		if row := d.t.rows[key]; row != nil {
			added = append(added, *row)
			d.t.work.WrittenBytes += uint64(len(row.Value))
		} else {
			removed = append(removed, driver.DerivedRow{Key: key})
		}
		d.t.work.WrittenNodes++
	}
	return removed, added, nil
}

func (d *Delta) Publish(ctx context.Context, tx driver.WriteTx, revision model.Revision) (driver.ReadyOrderWork, error) {
	if !d.Available() {
		return driver.ReadyOrderWork{}, nil
	}
	removed, added, err := d.materialize(revision)
	if err != nil {
		return d.t.work, err
	}
	publication, proofErr := d.publication(revision, removed, added)
	if proofErr != nil && !errors.Is(proofErr, driver.ErrUnsupported) {
		return d.t.work, proofErr
	}
	if proofErr == nil {
		d.registration = publication
	}
	if err = tx.PutDerived(ctx, driver.DerivedReadyState, removed, added); err != nil {
		return d.t.work, err
	}
	if err = d.BumpPredicates(ctx, tx); err != nil {
		return d.t.work, err
	}
	if writer, ok := tx.(driver.ReadyOrderPublicationWriter); ok && proofErr == nil {
		if err = writer.StageReadyOrderPublication(ctx, publication); err != nil && !errors.Is(err, driver.ErrUnsupported) {
			return d.t.work, err
		}
	}
	return d.t.work, nil
}

// FinalizePublication registers the same immutable transition after the core
// has finished ALL source, authority and predicate staging. Any accepted later
// mutation still invalidates the engine's registered overlay signature.
func (d *Delta) FinalizePublication(ctx context.Context, tx driver.WriteTx) error {
	if d == nil || !d.published {
		return nil
	}
	if d.registration.IsZero() {
		return nil
	}
	if writer, ok := tx.(driver.ReadyOrderPublicationWriter); ok {
		if err := writer.StageReadyOrderPublication(ctx, d.registration); err != nil && !errors.Is(err, driver.ErrUnsupported) {
			return err
		}
		if finalizer, ok := tx.(readyOrderPublicationFinalizer); ok {
			if err := finalizer.FinalizeReadyOrderPublication(ctx, d.registration); err != nil && !errors.Is(err, driver.ErrUnsupported) {
				return err
			}
		}
	}
	return nil
}

// BumpPredicates completes the declared update locks even when an intervening
// raw publication makes optional maintenance unavailable. It does not advance
// coverage or write any nodes, so the next selection takes exact fallback.
func (d *Delta) BumpPredicates(ctx context.Context, tx driver.WriteTx) error {
	sentinels := make([]driver.Sentinel, 0, len(d.sentinels))
	for sentinel := range d.sentinels {
		sentinels = append(sentinels, sentinel)
	}
	sort.Slice(sentinels, func(i, j int) bool { return sentinels[i] < sentinels[j] })
	return tx.BumpSentinels(ctx, sentinels)
}

package memory

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// selectionView keeps historical content out of the transaction's same-token
// overlay. The reference evaluator scans memory; SQL execution is a later lane.
type selectionView struct {
	state   *state
	at      uint64
	records map[string]*driver.Record
	derived map[string]driver.LockState
	bumps   map[string]driver.LockKey
}

func (v selectionView) observeID(id string, at uint64) driver.LockState {
	h := v.state.keyHistory[id]
	i := sort.Search(len(h), func(i int) bool { n, _ := h[i].Revision.Uint64(); return n > at }) - 1
	s := driver.LockState{Revision: model.Natural64(0)}
	if i >= 0 {
		s = h[i]
	}
	if at != v.at {
		return s
	}
	if r, ok := v.records[id]; ok {
		s.Record = nil
		s.Derived = nil
		s.Present = r != nil
		if r != nil {
			c := *r
			c.Revision = s.Revision
			s.Record = &c
			s.Key = driver.LockKey{Kind: driver.LockRecord, Ref: &c.Ref}
		}
	}
	if d, ok := v.derived[id]; ok {
		d.Revision = s.Revision
		s = d
	}
	if k, ok := v.bumps[id]; ok {
		s.Key = k
		s.Present = true
	}
	return s
}
func (v selectionView) source(index *driver.SelectionIndex, at uint64) (driver.LockState, error) {
	s := v.observeID(keyID(index.Source.Key), at)
	if s.Present != index.Source.Present || s.Revision.String() != index.Source.Revision.String() {
		return driver.LockState{}, fmt.Errorf("%w: selection source observation changed", driver.ErrUnsupported)
	}
	digest := ""
	if s.Present {
		switch index.Source.Key.Kind {
		case driver.LockRecord:
			if s.Record == nil {
				return driver.LockState{}, fmt.Errorf("%w: selection source image missing", driver.ErrUnsupported)
			}
			digest = driver.SelectionDigest(s.Record.Body)
		case driver.LockDerived:
			if s.Derived == nil {
				return driver.LockState{}, fmt.Errorf("%w: selection source row missing", driver.ErrUnsupported)
			}
			digest = driver.SelectionDigest(s.Derived.Value)
		}
	}
	if digest != index.Source.Digest {
		return driver.LockState{}, fmt.Errorf("%w: selection source digest changed", driver.ErrUnsupported)
	}
	return s, nil
}
func (v selectionView) index(row driver.DerivedRow, table driver.DerivedTable, at uint64) (driver.LockState, error) {
	if row.Index == nil {
		return driver.LockState{}, fmt.Errorf("%w: selection projection unavailable", driver.ErrUnsupported)
	}
	if err := row.Index.Validate(table, row); err != nil {
		return driver.LockState{}, err
	}
	if row.Index.Incarnation != v.state.meta.Incarnation || !row.Index.Branch.IsZero() {
		return driver.LockState{}, fmt.Errorf("%w: selection index snapshot mismatch", driver.ErrUnsupported)
	}
	return v.source(row.Index, at)
}
func (v selectionView) derivedIDs(table driver.DerivedTable) []string {
	set := map[string]bool{}
	for _, key := range v.state.derivedKeys[table] {
		set[keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: key}})] = true
	}
	for id, s := range v.derived {
		if s.Key.Derived != nil && s.Key.Derived.Table == table {
			set[id] = true
		}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

type selectionBead struct {
	row      driver.SelectionBead
	revision model.Revision
}

func (v selectionView) beads(ctx context.Context, at uint64) (map[string]selectionBead, error) {
	out := map[string]selectionBead{}
	for _, id := range v.derivedIDs(driver.DerivedSortKey) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s := v.observeID(id, at)
		if !s.Present {
			continue
		}
		if s.Derived == nil {
			return nil, fmt.Errorf("%w: selection row missing", driver.ErrUnsupported)
		}
		if s.Derived.Index == nil {
			continue
		}
		source, err := v.index(*s.Derived, driver.DerivedSortKey, at)
		if err != nil {
			return nil, err
		}
		if s.Derived.Index.Bead == nil || source.Record == nil || source.Record.Kind != "bead" {
			return nil, fmt.Errorf("%w: selection bead layout/source mismatch", driver.ErrUnsupported)
		}
		b := *s.Derived.Index.Bead
		k, err := recordKey(b.Ref)
		if err != nil {
			return nil, err
		}
		if _, ok := out[k]; ok {
			return nil, fmt.Errorf("%w: duplicate selection bead projection", driver.ErrUnsupported)
		}
		out[k] = selectionBead{row: b, revision: source.Revision}
	}
	// Coverage is checked against actual live sources, not a decorative marker.
	// Memory's reference path may enumerate; no SQL fallback is implied.
	ids := map[string]bool{}
	for id := range v.state.kinds {
		ids[id] = true
	}
	for id := range v.records {
		ids[id] = true
	}
	for id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s := v.observeID(id, at)
		if s.Present && s.Record != nil && s.Record.Kind == "bead" {
			if _, ok := out[id]; !ok {
				return nil, fmt.Errorf("%w: live bead lacks selection projection", driver.ErrUnsupported)
			}
		}
	}
	return out, nil
}

type selectionFacts map[string]driver.SelectionFacts

func (v selectionView) facts(ctx context.Context, e driver.SelectionExpr, at uint64, out selectionFacts) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.Exists != nil {
		x := e.Exists
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &x.Row}
		id := keyID(key)
		if _, ok := out[id]; !ok {
			s := v.observeID(id, at)
			if !s.Present || s.Derived == nil {
				return fmt.Errorf("%w: selection fact coverage unavailable", driver.ErrUnsupported)
			}
			if _, err := v.index(*s.Derived, x.Row.Table, at); err != nil {
				return err
			}
			if s.Derived.Index.Facts == nil {
				return fmt.Errorf("%w: selection expected complete facts", driver.ErrUnsupported)
			}
			out[id] = *s.Derived.Index.Facts
		}
		if out[id].Kind != x.Kind {
			return fmt.Errorf("%w: selection fact domain uncovered", driver.ErrUnsupported)
		}
	}
	for _, child := range e.Args {
		if err := v.facts(ctx, child, at, out); err != nil {
			return err
		}
	}
	return nil
}

func beadValue(b *selectionBead, identity model.Ref, c driver.SelectionColumn) *driver.SelectionValue {
	var value driver.SelectionValue
	if c == driver.SelectionID {
		value = driver.SelectionString(identity.ID)
		return &value
	}
	if b == nil {
		return nil
	}
	switch c {
	case driver.SelectionType:
		value = driver.SelectionString(b.row.Type)
	case driver.SelectionProject:
		if b.row.Project == nil {
			return nil
		}
		value = driver.SelectionString(*b.row.Project)
	case driver.SelectionStatus:
		value = driver.SelectionString(b.row.Status)
	case driver.SelectionTitle:
		value = driver.SelectionString(b.row.Title)
	case driver.SelectionRevision:
		value = driver.SelectionString(b.revision.String())
	case driver.SelectionTemplate:
		value = driver.SelectionString(b.row.Template)
	case driver.SelectionParent:
		if b.row.Parent == nil {
			return nil
		}
		value = driver.SelectionRef(*b.row.Parent)
	case driver.SelectionPriority:
		value = driver.SelectionInt(b.row.Priority)
	}
	return &value
}
func factValue(f driver.SelectionFact, c driver.SelectionFactColumn) driver.SelectionValue {
	switch c {
	case driver.SelectionFactRef:
		return driver.SelectionRef(*f.Ref)
	case driver.SelectionFactTarget:
		if f.Link != nil {
			return driver.SelectionRef(f.Link.To)
		}
		return driver.SelectionRef(*f.Target)
	case driver.SelectionFactLinkKind:
		return driver.SelectionString(f.Link.Kind)
	case driver.SelectionFactSubject:
		return driver.SelectionString(f.Subject)
	case driver.SelectionFactScope:
		return driver.SelectionString(f.Scope)
	case driver.SelectionFactRole:
		return driver.SelectionString(f.Role)
	case driver.SelectionFactName:
		return driver.SelectionString(f.Name)
	case driver.SelectionFactValue:
		return driver.SelectionString(f.Value)
	case driver.SelectionFactActive:
		return driver.SelectionBool(f.Active)
	case driver.SelectionFactDepth:
		return driver.SelectionInt(int64(f.Depth))
	}
	return driver.SelectionValue{}
}
func evalSelection(ctx context.Context, e driver.SelectionExpr, b *selectionBead, identity model.Ref, facts selectionFacts) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	switch e.Op {
	case driver.SelectionTrue:
		return true, nil
	case driver.SelectionFalse:
		return false, nil
	case driver.SelectionResourceExists:
		return b != nil, nil
	case driver.SelectionAll:
		for _, a := range e.Args {
			match, err := evalSelection(ctx, a, b, identity, facts)
			if err != nil || !match {
				return false, err
			}
		}
		return true, nil
	case driver.SelectionAny:
		for _, a := range e.Args {
			match, err := evalSelection(ctx, a, b, identity, facts)
			if err != nil || match {
				return match, err
			}
		}
		return false, nil
	case driver.SelectionNot:
		match, err := evalSelection(ctx, e.Args[0], b, identity, facts)
		return !match, err
	case driver.SelectionRefs:
		for _, r := range e.Refs {
			if driver.SelectionSameRef(r, identity) {
				return true, nil
			}
		}
		return false, nil
	case driver.SelectionCompareOp:
		c := e.Compare
		v := beadValue(b, identity, c.Column)
		if c.Op == "absent" {
			return v == nil, nil
		}
		if v == nil {
			return false, nil
		}
		if c.Op == "contains" {
			return strings.Contains(*v.String, *c.Value.String), nil
		}
		return sameSelectionValue(*v, *c.Value), nil
	case driver.SelectionExistsOp:
		x := e.Exists
		id := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &x.Row})
		for _, f := range facts[id].Rows {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if f.Kind != x.Kind {
				continue
			}
			match := true
			for _, t := range x.Tests {
				want := t.Value
				actual := factValue(f, t.Column)
				if want == nil {
					if t.Candidate == driver.SelectionID && actual.Ref != nil {
						r := driver.SelectionRef(identity)
						want = &r
					} else {
						want = beadValue(b, identity, t.Candidate)
					}
				}
				if want == nil || !sameSelectionValue(actual, *want) {
					match = false
					break
				}
			}
			if match {
				return true, nil
			}
		}
	}
	return false, nil // only reachable for already-rejected plans
}
func sameSelectionValue(a, b driver.SelectionValue) bool {
	if a.Ref != nil && b.Ref != nil {
		return driver.SelectionSameRef(*a.Ref, *b.Ref)
	}
	return reflect.DeepEqual(a, b)
}
func sameSelectionKey(a, b driver.SelectionKey) bool {
	if !driver.SelectionSameRef(a.Ref, b.Ref) || a.Revision.String() != b.Revision.String() || a.MembershipLeaf != b.MembershipLeaf || len(a.Order) != len(b.Order) {
		return false
	}
	for i := range a.Order {
		if !sameSelectionValue(a.Order[i], b.Order[i]) {
			return false
		}
	}
	return true
}
func compareValue(a, b driver.SelectionValue) int {
	if a.String != nil {
		return strings.Compare(*a.String, *b.String)
	}
	if a.Int != nil {
		if *a.Int < *b.Int {
			return -1
		}
		if *a.Int > *b.Int {
			return 1
		}
		return 0
	}
	return 0
}
func orderKey(b selectionBead, order []driver.SelectionOrder) driver.SelectionKey {
	k := driver.SelectionKey{Ref: b.row.Ref, Revision: b.revision, MembershipLeaf: b.row.MembershipLeaf, Order: make([]driver.SelectionValue, 0, len(order))}
	for _, o := range order {
		v := beadValue(&b, b.row.Ref, o.Column)
		if v == nil {
			x := driver.SelectionString("")
			v = &x
		}
		k.Order = append(k.Order, *v)
	}
	return k
}

func (v selectionView) selectPlan(ctx context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
	zero := driver.SelectionResult{}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := p.Validate(); err != nil {
		return zero, err
	}
	head, _ := v.state.meta.Head.Uint64()
	a, _ := p.AuthorityRevision.Uint64()
	c, _ := p.ContentRevision.Uint64()
	if p.Incarnation != v.state.meta.Incarnation {
		return zero, driver.ErrIncarnationChanged{From: p.Incarnation, To: v.state.meta.Incarnation}
	}
	if a != v.at || v.at != head {
		return zero, fmt.Errorf("%w: selection requires bound current authority", driver.ErrUnsupported)
	}
	floor, _ := v.state.meta.HistoryFloor.Uint64()
	if c < floor {
		return zero, driver.ErrTruncated{Floor: v.state.meta.HistoryFloor}
	}
	content, err := v.beads(ctx, c)
	if err != nil {
		return zero, err
	}
	authority := content
	if c != a {
		authority, err = v.beads(ctx, a)
		if err != nil {
			return zero, err
		}
	}
	cf, af := selectionFacts{}, selectionFacts{}
	if err = v.facts(ctx, p.Membership, c, cf); err != nil {
		return zero, err
	}
	if err = v.facts(ctx, p.Authorization, a, af); err != nil {
		return zero, err
	}
	items := make([]selectionBead, 0)
	for id, b := range content {
		if err = ctx.Err(); err != nil {
			return zero, err
		}
		var current *selectionBead
		if x, ok := authority[id]; ok {
			current = &x
		}
		member, err := evalSelection(ctx, p.Membership, &b, b.row.Ref, cf)
		if err != nil {
			return zero, err
		}
		if !member {
			continue
		}
		allowed, err := evalSelection(ctx, p.Authorization, current, b.row.Ref, af)
		if err != nil {
			return zero, err
		}
		if allowed {
			items = append(items, b)
			if len(items) > p.EffectiveCap() {
				return zero, driver.ErrSelectionCap{Limit: p.EffectiveCap()}
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		for _, o := range p.Order {
			a, b := beadValue(&items[i], items[i].row.Ref, o.Column), beadValue(&items[j], items[j].row.Ref, o.Column)
			if a == nil {
				x := driver.SelectionString("")
				a = &x
			}
			if b == nil {
				x := driver.SelectionString("")
				b = &x
			}
			cmp := compareValue(*a, *b)
			if cmp != 0 {
				if o.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return items[i].row.Ref.ID < items[j].row.Ref.ID
	})
	start, end := 0, 0
	if p.Page != nil {
		if p.Page.Offset > uint64(len(items)) {
			return zero, fmt.Errorf("%w: selection offset beyond membership", driver.ErrUnsupported)
		}
		start = int(p.Page.Offset)
		if p.Page.After != nil {
			found := false
			for i, b := range items {
				if sameSelectionKey(orderKey(b, p.Order), p.Page.After.Key) {
					start = i + 1
					found = true
					break
				}
			}
			if !found {
				return zero, fmt.Errorf("%w: keyset anchor outside selection", driver.ErrUnsupported)
			}
		}
		end = start + p.Page.Limit
		if end > len(items) {
			end = len(items)
		}
	}
	result := driver.SelectionResult{Total: uint64(len(items))}
	if p.GroupBy == driver.SelectionGroupType {
		groups := map[string]driver.SelectionTypeGroup{}
		for i, b := range items {
			g := groups[b.row.Type]
			g.Type = b.row.Type
			g.Count++
			if p.Page != nil && i >= end {
				g.Elided++
			}
			groups[b.row.Type] = g
		}
		if len(groups) > driver.SelectionGroupCap {
			return zero, driver.ErrSelectionCap{Limit: driver.SelectionGroupCap, Groups: true}
		}
		for _, g := range groups {
			result.Groups = append(result.Groups, g)
		}
		sort.Slice(result.Groups, func(i, j int) bool { return result.Groups[i].Type < result.Groups[j].Type })
	}
	if p.Page != nil {
		result.Keys = make([]driver.SelectionKey, 0, end-start)
		for _, b := range items[start:end] {
			result.Keys = append(result.Keys, orderKey(b, p.Order))
		}
		result.More = end < len(items)
		if result.More && end > start {
			binding, err := p.Binding()
			if err != nil {
				return zero, err
			}
			result.Next = &driver.SelectionKeyset{Binding: binding, Key: orderKey(items[end-1], p.Order)}
		}
	}
	if err = ctx.Err(); err != nil {
		return zero, err
	}
	return result, nil
}

func (r *reader) Select(ctx context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(ctx); err != nil {
		return driver.SelectionResult{}, err
	}
	return (selectionView{state: r.state, at: r.at}).selectPlan(ctx, p)
}
func (t *transaction) selectionView() selectionView {
	at, _ := t.snapshotAt.Uint64()
	return selectionView{state: t.base, at: at, records: t.records, derived: t.derived, bumps: t.bumps}
}
func (t *transaction) Select(ctx context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.SelectionResult{}, err
	}
	return t.selectionView().selectPlan(ctx, p)
}

var _ driver.SelectionReader = (*reader)(nil)
var _ driver.SelectionReader = (*transaction)(nil)

func (t *transaction) validateSelectionWrites(proposed map[string]driver.LockState) error {
	v := t.selectionView()
	v.derived = proposed
	for _, s := range proposed {
		if !s.Present || s.Derived == nil || s.Derived.Index == nil {
			continue
		}
		index := s.Derived.Index
		id := keyID(index.Source.Key)
		i := sort.Search(len(t.acquired), func(i int) bool { return keyID(t.acquired[i].Key) >= id })
		if i == len(t.acquired) || keyID(t.acquired[i].Key) != id {
			return fmt.Errorf("%w: selection source absent from lock census", driver.ErrUnsupported)
		}
		source, err := v.index(*s.Derived, s.Key.Derived.Table, v.at)
		if err != nil {
			return err
		}
		if index.Bead != nil && (source.Record == nil || source.Record.Kind != "bead") {
			return fmt.Errorf("%w: selection bead source kind", driver.ErrUnsupported)
		}
	}
	return nil
}
func (t *transaction) selectionSourceChanged(key driver.LockKey) bool {
	id := keyID(key)
	_, record := t.records[id]
	_, derived := t.derived[id]
	_, bump := t.bumps[id]
	return record || derived || bump
}

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
	state            *state
	at               uint64
	records          map[string]*driver.Record
	derived          map[string]driver.LockState
	bumps            map[string]driver.LockKey
	declarations     map[string]sourceDeclaration
	writeIncarnation string
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
	return v.observeSource(index.Source, at)
}
func (v selectionView) observeSource(source driver.SelectionSource, at uint64) (driver.LockState, error) {
	s := v.observeID(keyID(source.Key), at)
	if s.Present != source.Present || s.Revision.String() != source.Revision.String() {
		return driver.LockState{}, fmt.Errorf("%w: source observation changed", driver.ErrSelectionUnavailable)
	}
	digest := ""
	if s.Present {
		switch source.Key.Kind {
		case driver.LockRecord:
			if s.Record == nil {
				return driver.LockState{}, fmt.Errorf("%w: source image missing", driver.ErrSelectionUnavailable)
			}
			digest = driver.SelectionDigest(s.Record.Body)
		case driver.LockDerived:
			if s.Derived == nil {
				return driver.LockState{}, fmt.Errorf("%w: source row missing", driver.ErrSelectionUnavailable)
			}
			digest = driver.SelectionDigest(s.Derived.Value)
		}
	}
	if digest != source.Digest {
		return driver.LockState{}, fmt.Errorf("%w: source digest changed", driver.ErrSelectionUnavailable)
	}
	return s, nil
}
func (v selectionView) index(row driver.DerivedRow, table driver.DerivedTable, at uint64) (driver.LockState, error) {
	if row.Index == nil {
		return driver.LockState{}, fmt.Errorf("%w: projection unavailable", driver.ErrSelectionUnavailable)
	}
	if err := row.Index.Validate(table, row); err != nil {
		return driver.LockState{}, fmt.Errorf("%w: %v", driver.ErrSelectionUnavailable, err)
	}
	incarnation := v.state.meta.Incarnation
	if v.writeIncarnation != "" {
		incarnation = v.writeIncarnation
	}
	if row.Index.Incarnation != incarnation || !row.Index.Branch.IsZero() {
		return driver.LockState{}, fmt.Errorf("%w: index snapshot mismatch", driver.ErrSelectionUnavailable)
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
			return nil, fmt.Errorf("%w: selection row missing", driver.ErrSelectionUnavailable)
		}
		if s.Derived.Index == nil {
			continue
		}
		source, err := v.index(*s.Derived, driver.DerivedSortKey, at)
		if err != nil {
			return nil, err
		}
		if s.Derived.Index.Bead == nil || source.Record == nil || source.Record.Kind != "bead" {
			return nil, fmt.Errorf("%w: selection bead layout/source mismatch", driver.ErrSelectionUnavailable)
		}
		b := *s.Derived.Index.Bead
		k, err := recordKey(b.Ref)
		if err != nil {
			return nil, err
		}
		if _, ok := out[k]; ok {
			return nil, fmt.Errorf("%w: duplicate selection bead projection", driver.ErrSelectionUnavailable)
		}
		out[k] = selectionBead{row: b, revision: source.Revision}
	}
	// The temporal range was admitted before enumeration. This independent
	// census still requires one exact projection per live source at that revision.
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
				return nil, fmt.Errorf("%w: live bead lacks selection projection", driver.ErrSelectionUnavailable)
			}
		}
	}
	return out, nil
}

type selectionFacts struct {
	rows    map[string]driver.SelectionFacts
	sources map[*driver.SelectionSources]map[string]bool
	// Ref sets belong to expression nodes in this request, never to a store or
	// snapshot cache. Different vectors can share a backing array or prefix.
	refs map[*driver.SelectionExpr]map[string]bool
}

func newSelectionFacts() selectionFacts {
	return selectionFacts{rows: map[string]driver.SelectionFacts{}, sources: map[*driver.SelectionSources]map[string]bool{}, refs: map[*driver.SelectionExpr]map[string]bool{}}
}

func (v selectionView) facts(ctx context.Context, e *driver.SelectionExpr, at uint64, out selectionFacts) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.Op == driver.SelectionRefs {
		members := make(map[string]bool, len(e.Refs))
		for _, ref := range e.Refs {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Plan validation already checked each Ref. Keep the same opaque
			// canonical identity as SelectionSameRef, including branch aliases.
			members[keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})] = true
		}
		out.refs[e] = members
	}
	if e.Exists != nil {
		x := e.Exists
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &x.Row}
		id := keyID(key)
		if _, ok := out.rows[id]; !ok {
			s := v.observeID(id, at)
			if !s.Present || s.Derived == nil {
				return fmt.Errorf("%w: selection fact coverage unavailable", driver.ErrSelectionUnavailable)
			}
			if index := s.Derived.Index; index != nil && index.Source.Key.Kind == driver.LockRecord {
				family := driver.SelectionProjectionFamily{Table: x.Row.Table, Kind: x.Kind, Version: driver.SelectionIndexVersion}
				switch {
				case x.Row.Table == driver.DerivedAttribute && x.Kind == driver.SelectionLabelFact,
					x.Row.Table == driver.DerivedReferenceIndex && x.Kind == driver.SelectionLinkFact,
					x.Row.Table == driver.DerivedAncestorClosure && x.Kind == driver.SelectionAncestorFact:
					if err := v.requireSelectionCoverage(family, at); err != nil {
						return err
					}
				}
			}
			if _, err := v.index(*s.Derived, x.Row.Table, at); err != nil {
				return err
			}
			if s.Derived.Index.Facts == nil {
				return fmt.Errorf("%w: selection expected complete facts", driver.ErrSelectionUnavailable)
			}
			out.rows[id] = *s.Derived.Index.Facts
		}
		if out.rows[id].Kind != x.Kind {
			return fmt.Errorf("%w: selection fact domain uncovered", driver.ErrSelectionUnavailable)
		}
	}
	if e.Sources != nil {
		members, err := v.sourceMembers(ctx, *e, at)
		if err != nil {
			return err
		}
		out.sources[e.Sources] = members
	}
	for i := range e.Args {
		if err := v.facts(ctx, &e.Args[i], at, out); err != nil {
			return err
		}
	}
	return nil
}

// Membership comes from the already-maintained source indexes, never the
// SelectionFact link/closure rows or a second source declaration database.
// This oracle may enumerate keys; it never obtains candidate bodies here.
func (v selectionView) sourceMembers(ctx context.Context, e driver.SelectionExpr, at uint64) (map[string]bool, error) {
	out := map[string]bool{}
	refs := canonicalSourceKeys(e.Sources.Refs)
	kinds := map[string]bool{}
	for _, kind := range e.Sources.Kinds {
		kinds[string(kind)] = true
	}
	if e.Op == driver.SelectionChildrenOp {
		root, err := v.state.hierarchy.rootAt(v.state.meta.Incarnation, 0, at)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", driver.ErrSelectionUnavailable, err)
		}
		for parent := range refs {
			for _, source := range root.children[parent] {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				id, err := recordKey(source.record.Ref)
				if err != nil {
					return nil, err
				}
				out[id] = true
			}
		}
	} else {
		root, err := v.state.sourceLinks.rootAt(v.state.meta.Incarnation, 0, at)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", driver.ErrSelectionUnavailable, err)
		}
		for target := range refs {
			for kind := range kinds {
				for _, id := range root.incoming[sourceLinkBucket{kind: kind, targetKey: target}] {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					out[id] = true
				}
			}
		}
	}
	if at != v.at {
		return out, nil
	}
	for id, record := range v.records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if v.state.kinds[id] != "bead" && (record == nil || record.Kind != "bead") {
			continue
		}
		delete(out, id) // a staged image shadows its entire committed membership
		if record == nil {
			continue
		}
		declaration, ok := v.declarations[id]
		if !ok {
			return nil, fmt.Errorf("%w: staged source declaration unavailable", driver.ErrSelectionUnavailable)
		}
		if e.Op == driver.SelectionChildrenOp {
			if refs[declaration.parentKey] {
				out[id] = true
			}
		} else {
			// Parent-only publication is a complete, independent domain. It
			// can satisfy Children, but Incoming requires the separate
			// complete outgoing-links declaration.
			if !declaration.graph {
				return nil, fmt.Errorf("%w: staged outgoing link declaration unavailable", driver.ErrSelectionUnavailable)
			}
			for _, link := range declaration.links {
				target, err := recordKey(link.target)
				if err != nil {
					return nil, err
				}
				if refs[target] && kinds[link.kind] {
					out[id] = true
				}
			}
		}
	}
	return out, nil
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
	for _, absent := range b.row.Absent {
		if absent == c {
			return nil
		}
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
		value = driver.SelectionBool(b.row.Template)
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
	case driver.SelectionFactScopeKind:
		return driver.SelectionString(string(f.ScopeKind))
	case driver.SelectionFactProjectName:
		if f.ProjectName != nil {
			return driver.SelectionString(*f.ProjectName)
		}
		return driver.SelectionValue{}
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
func evalSelection(ctx context.Context, e *driver.SelectionExpr, b *selectionBead, identity model.Ref, identityKey string, facts selectionFacts) (bool, error) {
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
		for i := range e.Args {
			match, err := evalSelection(ctx, &e.Args[i], b, identity, identityKey, facts)
			if err != nil || !match {
				return false, err
			}
		}
		return true, nil
	case driver.SelectionAny:
		for i := range e.Args {
			match, err := evalSelection(ctx, &e.Args[i], b, identity, identityKey, facts)
			if err != nil || match {
				return match, err
			}
		}
		return false, nil
	case driver.SelectionNot:
		match, err := evalSelection(ctx, &e.Args[0], b, identity, identityKey, facts)
		return !match, err
	case driver.SelectionRefs:
		return facts.refs[e][identityKey], nil
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
		for _, f := range facts.rows[id].Rows {
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
	case driver.SelectionChildrenOp, driver.SelectionIncomingOp:
		id, err := recordKey(identity)
		if err != nil {
			return false, err
		}
		return facts.sources[e.Sources][id], nil
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
func orderValue(b *selectionBead, c driver.SelectionColumn) driver.SelectionValue {
	if value := beadValue(b, b.row.Ref, c); value != nil {
		return *value
	}
	// Existing Ready priority defaults to numeric zero. Ordinary view fields
	// sort by their existing empty string default, including absent project.
	if c == driver.SelectionPriority {
		return driver.SelectionInt(0)
	}
	return driver.SelectionString("")
}

func orderKey(b selectionBead, order []driver.SelectionOrder) driver.SelectionKey {
	k := driver.SelectionKey{Ref: b.row.Ref, Revision: b.revision, MembershipLeaf: b.row.MembershipLeaf, Order: make([]driver.SelectionValue, 0, len(order))}
	for _, o := range order {
		k.Order = append(k.Order, orderValue(&b, o.Column))
	}
	return k
}

func (v selectionView) selectPlan(ctx context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
	return v.evaluate(ctx, p, false)
}
func (v selectionView) evaluate(ctx context.Context, p driver.SelectionPlan, existence bool) (driver.SelectionResult, error) {
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
	// A complete final image cannot prove an unbuilt or interrupted revision
	// range. Both planes require their own certificate, even with no candidates.
	family := driver.SelectionProjectionFamily{Table: driver.DerivedSortKey, Version: driver.SelectionIndexVersion}
	if err := v.requireSelectionCoverage(family, c); err != nil {
		return zero, err
	}
	if err := v.requireSelectionCoverage(family, a); err != nil {
		return zero, err
	}
	boundary, err := p.Boundary.Description()
	if err != nil {
		return zero, err
	}
	for _, source := range boundary.Sources {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if _, err := v.observeSource(source, a); err != nil {
			return zero, err
		}
	}
	// Relation coverage is checked by facts for every declared source arm,
	// including arms hidden below false/NOT, empty vectors and probes. Parent
	// and outgoing-link roots are independent domains: a Children-only plan
	// needs the former, while Incoming needs the latter. Requiring both here
	// would reject valid Parent-only stores and would turn an unrelated
	// ordinary plan into a link-coverage refusal.
	cv := v
	if p.ContentCommitted || c != a {
		cv.records, cv.derived, cv.bumps, cv.declarations = nil, nil, nil, nil
	}
	// Preflight every declared arm at its own plane before even reading bead
	// candidates. False/NOT, exclusions, empty vectors and probes cannot hide
	// unavailable facts or independent source domains.
	cf, af := newSelectionFacts(), newSelectionFacts()
	if err = cv.facts(ctx, &p.Membership, c, cf); err != nil {
		return zero, err
	}
	if err = v.facts(ctx, &p.Authorization, a, af); err != nil {
		return zero, err
	}
	if boundary.HostExpression != nil {
		if err = v.facts(ctx, boundary.HostExpression, a, af); err != nil {
			return zero, err
		}
	}
	content, err := cv.beads(ctx, c)
	if err != nil {
		return zero, err
	}
	authority := content
	if c != a || p.ContentCommitted {
		authority, err = v.beads(ctx, a)
		if err != nil {
			return zero, err
		}
	}
	excluded := canonicalSourceKeys(p.Excluded)
	items := make([]selectionBead, 0)
	for id, b := range content {
		if excluded[id] {
			continue
		}
		if err = ctx.Err(); err != nil {
			return zero, err
		}
		var current *selectionBead
		if x, ok := authority[id]; ok {
			current = &x
		}
		member, err := evalSelection(ctx, &p.Membership, &b, b.row.Ref, id, cf)
		if err != nil {
			return zero, err
		}
		if !member {
			continue
		}
		allowed, err := evalSelection(ctx, &p.Authorization, current, b.row.Ref, id, af)
		if err != nil {
			return zero, err
		}
		if allowed && boundary.HostExpression != nil {
			allowed, err = evalSelection(ctx, boundary.HostExpression, current, b.row.Ref, id, af)
			if err != nil {
				return zero, err
			}
		}
		if allowed {
			if existence {
				if err := ctx.Err(); err != nil {
					return zero, err
				}
				return driver.SelectionResult{Total: 1}, nil
			}
			items = append(items, b)
			if len(items) > p.EffectiveCap() {
				return zero, driver.ErrSelectionCap{Limit: p.EffectiveCap()}
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		for _, o := range p.Order {
			cmp := compareValue(orderValue(&items[i], o.Column), orderValue(&items[j], o.Column))
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
	return selectionView{state: t.base, at: at, records: t.records, derived: t.derived, bumps: t.bumps, declarations: t.sourceDeclarations}
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

func (t *transaction) PutSelectionDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	return t.PutDerived(ctx, table, del, add)
}

var _ driver.SelectionIndexWriter = (*transaction)(nil)

func (t *transaction) FreshSelectionExists(ctx context.Context, p driver.SelectionPlan) (bool, error) {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return false, err
	}
	if s.head != t {
		return false, fmt.Errorf("%w: fresh existence requires Head", driver.ErrSelectionInvalid)
	}
	if p.Mode != driver.SelectionCountMode || p.Cap != 0 || p.Page != nil || p.GroupBy != "" || len(p.Excluded) != 0 || p.ContentRevision.String() != p.AuthorityRevision.String() {
		return false, fmt.Errorf("%w: fresh existence requires uncapped current count intent", driver.ErrSelectionInvalid)
	}
	at, _ := s.state.meta.Head.Uint64()
	result, err := (selectionView{state: s.state, at: at}).evaluate(ctx, p, true)
	if err != nil {
		return false, err
	}
	return result.Total != 0, nil
}

var _ driver.FreshSelectionExistsReader = (*transaction)(nil)

func (t *transaction) validateSelectionWrites(proposed map[string]driver.LockState) error {
	v := t.selectionView()
	if t.restore != nil {
		v.writeIncarnation = t.restore.Incarnation
	}
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

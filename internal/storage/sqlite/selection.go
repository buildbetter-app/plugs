package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// validateSelectionWrites checks the complete proposed overlay before any
// annotated row is staged. The trusted projection must bind a source already
// in the transaction's complete lock census and its exact observed image.
func (t *transaction) validateSelectionWrites(ctx context.Context, proposed map[string]driver.LockState) error {
	previous := t.derived
	t.derived = proposed
	defer func() { t.derived = previous }()
	return t.validateSelectionRows(ctx, proposed)
}

// rows may be only the changed annotations and their affected dependents;
// observations always use the complete proposed t.derived overlay.
func (t *transaction) validateSelectionRows(ctx context.Context, rows map[string]driver.LockState) error {
	// The mutex, pinned SQL snapshot and proposed overlay are stable for this
	// call. Many ancestor rows share one source; materialize it once here, but
	// never retain an observation across staging calls or into Commit.
	sources := make(map[string]driver.LockState)
	for _, state := range rows {
		if !state.Present || state.Derived == nil || state.Derived.Index == nil {
			continue
		}
		index := state.Derived.Index
		incarnation := t.meta.Incarnation
		if t.restore != nil {
			incarnation = t.restore.Incarnation
		}
		if index.Incarnation != incarnation {
			return fmt.Errorf("%w: SQLite selection incarnation mismatch", driver.ErrUnsupported)
		}
		id := keyID(index.Source.Key)
		i := sort.Search(len(t.acquired), func(i int) bool { return keyID(t.acquired[i].Key) >= id })
		if i == len(t.acquired) || keyID(t.acquired[i].Key) != id {
			return fmt.Errorf("%w: SQLite selection source absent from lock census", driver.ErrUnsupported)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		source, observed := sources[id]
		if !observed {
			var err error
			source, err = t.observe(ctx, index.Source.Key)
			if err != nil {
				return err
			}
			sources[id] = source
		}
		if source.Present != index.Source.Present || source.Revision.String() != index.Source.Revision.String() {
			return fmt.Errorf("%w: SQLite selection source observation changed", driver.ErrUnsupported)
		}
		digest := ""
		if source.Present {
			switch index.Source.Key.Kind {
			case driver.LockRecord:
				if source.Record == nil {
					return fmt.Errorf("%w: SQLite selection source image missing", driver.ErrUnsupported)
				}
				digest = driver.SelectionDigest(source.Record.Body)
			case driver.LockDerived:
				if source.Derived == nil {
					return fmt.Errorf("%w: SQLite selection source row missing", driver.ErrUnsupported)
				}
				digest = driver.SelectionDigest(source.Derived.Value)
			}
		}
		if digest != index.Source.Digest {
			return fmt.Errorf("%w: SQLite selection source digest changed", driver.ErrUnsupported)
		}
		if index.Bead != nil && (source.Record == nil || source.Record.Kind != "bead") {
			return fmt.Errorf("%w: SQLite selection bead source kind", driver.ErrUnsupported)
		}
	}
	return nil
}

func (t *transaction) selectionSourceStaged(source string) {
	for row := range t.selectionDependents[source] {
		if t.selectionRecheck == nil {
			t.selectionRecheck = make(map[string]bool)
		}
		t.selectionRecheck[row] = true
	}
}

// Apply only this call's final delta temporarily, retaining an undo entry for
// every touched identity. Validation sees the full overlay, including earlier
// staged derived sources, but does not copy or recheck unrelated accepted rows.
// A failed call restores every arm and leaves dependency bookkeeping unchanged.
func (t *transaction) stageSelectionDelta(ctx context.Context, delta map[string]driver.LockState, annotated bool) error {
	type priorState struct {
		state driver.LockState
		found bool
	}
	prior := make(map[string]priorState, len(delta))
	accepted := false
	defer func() {
		if !accepted {
			for id, old := range prior {
				if old.found {
					t.derived[id] = old.state
				} else {
					delete(t.derived, id)
				}
			}
		}
	}()
	for id, state := range delta {
		old, found := t.derived[id]
		prior[id] = priorState{old, found}
		t.derived[id] = state
	}
	if annotated {
		check := make(map[string]driver.LockState, len(delta)+len(t.selectionRecheck))
		for id := range t.selectionRecheck {
			check[id] = t.derived[id]
		}
		for id, state := range delta {
			check[id] = state
			for dependent := range t.selectionDependents[id] {
				check[dependent] = t.derived[dependent]
			}
		}
		if err := t.validateSelectionRows(ctx, check); err != nil {
			return err
		}
	}
	for id, state := range delta {
		old := prior[id].state
		if old.Present && old.Derived != nil && old.Derived.Index != nil {
			source := keyID(old.Derived.Index.Source.Key)
			delete(t.selectionDependents[source], id)
			if len(t.selectionDependents[source]) == 0 {
				delete(t.selectionDependents, source)
			}
		}
		if state.Present && state.Derived != nil && state.Derived.Index != nil {
			source := keyID(state.Derived.Index.Source.Key)
			if t.selectionDependents == nil {
				t.selectionDependents = make(map[string]map[string]bool)
			}
			if t.selectionDependents[source] == nil {
				t.selectionDependents[source] = make(map[string]bool)
			}
			t.selectionDependents[source][id] = true
		}
	}
	t.selectionRecheck = nil
	accepted = true
	return nil
}

func (t *transaction) selectionSourceChanged(key driver.LockKey) bool {
	id := keyID(key)
	_, record := t.records[id]
	_, derived := t.derived[id]
	_, bump := t.bumps[id]
	return record || derived || bump
}

func selectionRelationNeeds(e driver.SelectionExpr) (parent, links bool) {
	switch e.Op {
	case driver.SelectionChildrenOp:
		parent = true
	case driver.SelectionIncomingOp:
		links = true
	}
	for _, child := range e.Args {
		p, l := selectionRelationNeeds(child)
		parent, links = parent || p, links || l
	}
	return parent, links
}

// Parent and outgoing/incoming-link coverage are independent source domains.
// Check every required domain before false, NOT, empty and zero-limit paths.
func (r *reader) validateSelectionRelations(ctx context.Context, p driver.SelectionPlan, at uint64) error {
	a, _ := p.AuthorityRevision.Uint64()
	boundary, err := p.Boundary.Description()
	if err != nil {
		return err
	}
	checks := []struct {
		parent, links bool
		at            uint64
	}{}
	parent, links := selectionRelationNeeds(p.Membership)
	checks = append(checks, struct {
		parent, links bool
		at            uint64
	}{parent, links, at})
	parent, links = selectionRelationNeeds(p.Authorization)
	checks = append(checks, struct {
		parent, links bool
		at            uint64
	}{parent, links, a})
	if boundary.HostExpression != nil {
		parent, links = selectionRelationNeeds(*boundary.HostExpression)
		checks = append(checks, struct {
			parent, links bool
			at            uint64
		}{parent, links, a})
	}
	for _, check := range checks {
		for _, domain := range []struct {
			needed bool
			table  string
		}{{check.parent, "hierarchy_index_coverage"}, {check.links, "graph_link_index_coverage"}} {
			if err := r.requireSelectionRelation(ctx, domain.table, domain.needed, check.at); err != nil {
				return err
			}
		}
	}
	if err := r.validateSelectionDeclarations(p.Membership, at, !p.ContentCommitted); err != nil {
		return err
	}
	if err := r.validateSelectionDeclarations(p.Authorization, a, true); err != nil {
		return err
	}
	if boundary.HostExpression != nil {
		if err := r.validateSelectionDeclarations(*boundary.HostExpression, a, true); err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) validateSelectionDeclarations(e driver.SelectionExpr, at uint64, overlay bool) error {
	if e.Sources != nil && overlay && r.owner != nil && at == r.at {
		for id, record := range r.owner.records {
			if record == nil || record.Kind != "bead" {
				continue
			}
			declaration, ok := r.owner.sourceOverlay.Declarations[id]
			if !ok || e.Op == driver.SelectionIncomingOp && !declaration.Graph {
				return fmt.Errorf("%w: SQLite staged selection source declaration unavailable", driver.ErrSelectionUnavailable)
			}
		}
	}
	for _, child := range e.Args {
		if err := r.validateSelectionDeclarations(child, at, overlay); err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) requireSelectionRelation(ctx context.Context, table string, needed bool, at uint64) error {
	if !needed {
		return nil
	}
	var format int
	var first, through, state string
	err := r.tx.QueryRowContext(ctx, `SELECT format,built_from,certified_through,state FROM `+table+` WHERE incarnation=? AND branch=?`, r.meta.Incarnation, revKey(0)).Scan(&format, &first, &through, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: SQLite selection relation coverage unavailable", driver.ErrSelectionUnavailable)
	}
	if err != nil {
		return err
	}
	if format != 1 || state != "ready" || first > revKey(at) || through < revKey(at) {
		return fmt.Errorf("%w: SQLite selection relation coverage incomplete", driver.ErrSelectionUnavailable)
	}
	return nil
}

func (r *reader) validateSelectionBoundary(ctx context.Context, p driver.SelectionPlan, at uint64) (driver.SelectionBoundarySpec, error) {
	boundary, err := p.Boundary.Description()
	if err != nil {
		return boundary, err
	}
	for _, want := range boundary.Sources {
		if err := ctx.Err(); err != nil {
			return boundary, err
		}
		id := keyID(want.Key)
		actual, digest, err := r.selectionSourceState(ctx, id, at, true)
		if err != nil {
			return boundary, err
		}
		if actual.Revision.String() != want.Revision.String() || actual.Present != want.Present || digest != want.Digest {
			return boundary, fmt.Errorf("%w: SQLite sealed authority source changed", driver.ErrSelectionUnavailable)
		}
	}
	return boundary, nil
}

// Staged annotations were fully validated when accepted. A later staged
// source write can still make one stale, so a read rechecks its thin source
// observation and final in-memory bytes without reconstructing a record or
// decoding a whole derived value from key_history.
func (r *reader) validateSelectionOverlay(ctx context.Context) error {
	if r.owner == nil || !r.owner.selectionAnnotated {
		return nil
	}
	for _, state := range r.owner.derived {
		if !state.Present || state.Derived == nil || state.Derived.Index == nil {
			continue
		}
		want := state.Derived.Index.Source
		actual, digest, err := r.selectionSourceState(ctx, keyID(want.Key), r.at, true)
		if err != nil {
			return err
		}
		if actual.Revision.String() != want.Revision.String() || actual.Present != want.Present || digest != want.Digest {
			return fmt.Errorf("%w: SQLite staged selection source changed", driver.ErrSelectionUnavailable)
		}
	}
	return nil
}

// Select evaluates both closed expressions in SQLite before its authorized
// cap, aggregate and page. It returns thin keys; source bodies are never read
// by the selection query, including the cap probe and page lookahead.
func (r *reader) Select(ctx context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(ctx); err != nil {
		return driver.SelectionResult{}, err
	}
	return r.selectPlan(ctx, p)
}

func (r *reader) selectPlan(ctx context.Context, p driver.SelectionPlan) (driver.SelectionResult, error) {
	zero := driver.SelectionResult{}
	facts, err := r.preflightSelection(ctx, p)
	if err != nil {
		return zero, err
	}
	if p.Page != nil && (p.Page.Offset > math.MaxInt64 || p.Page.Offset > math.MaxInt64-uint64(p.Page.Limit)) {
		return zero, fmt.Errorf("%w: SQLite selection offset outside integer range", driver.ErrUnsupported)
	}
	b := sqliteSelectionSQL{facts: facts, owner: r.owner, meta: r.meta}
	query, err := b.query(p)
	if err != nil {
		return zero, err
	}
	if len(b.args) > 32000 {
		return zero, fmt.Errorf("%w: SQLite selection binding budget exceeded", driver.ErrUnsupported)
	}
	rows, err := r.tx.QueryContext(ctx, query, b.args...)
	if err != nil {
		return zero, err
	}
	defer rows.Close()
	var total uint64
	var start int64
	var haveMeta bool
	result := driver.SelectionResult{}
	for rows.Next() {
		var tag int
		var n, groupCount, elided, priority, template, ordinal sql.NullInt64
		var groupType, recordKey, id, kind, project, status, title, parent, leaf, revision sql.NullString
		if err = rows.Scan(&tag, &n, &groupType, &groupCount, &elided, &recordKey, &id, &kind, &project, &status, &title, &template, &parent, &priority, &leaf, &revision, &ordinal); err != nil {
			return zero, err
		}
		switch tag {
		case 0:
			if !n.Valid || n.Int64 < 0 || haveMeta {
				return zero, fmt.Errorf("%w: SQLite selection malformed aggregate", driver.ErrUnsupported)
			}
			haveMeta = true
			total = uint64(n.Int64)
			if p.Page != nil {
				if !ordinal.Valid || ordinal.Int64 < 0 {
					return zero, fmt.Errorf("%w: SQLite selection keyset anchor unavailable", driver.ErrUnsupported)
				}
				start = ordinal.Int64
			}
		case 1:
			if !groupType.Valid || !groupCount.Valid || !elided.Valid || groupCount.Int64 < 0 || elided.Int64 < 0 {
				return zero, fmt.Errorf("%w: SQLite selection malformed group", driver.ErrUnsupported)
			}
			result.Groups = append(result.Groups, driver.SelectionTypeGroup{Type: groupType.String, Count: uint64(groupCount.Int64), Elided: uint64(elided.Int64)})
			driver.ReportReadWork(ctx, driver.ReadAggregateGroups, 1)
		case 2:
			if !recordKey.Valid || !id.Valid || !kind.Valid || !leaf.Valid || !revision.Valid {
				return zero, fmt.Errorf("%w: SQLite selection malformed key", driver.ErrUnsupported)
			}
			ref := model.RefByID(id.String, model.Natural64(0))
			if keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}) != recordKey.String {
				return zero, fmt.Errorf("%w: SQLite selection key identity mismatch", driver.ErrUnsupported)
			}
			rev, err := parseRev(revision.String)
			if err != nil {
				return zero, err
			}
			key := driver.SelectionKey{Ref: ref, Revision: rev, MembershipLeaf: leaf.String}
			for _, order := range p.Order {
				switch order.Column {
				case driver.SelectionID:
					key.Order = append(key.Order, driver.SelectionString(id.String))
				case driver.SelectionType:
					key.Order = append(key.Order, driver.SelectionString(kind.String))
				case driver.SelectionProject:
					key.Order = append(key.Order, driver.SelectionString(project.String))
				case driver.SelectionStatus:
					key.Order = append(key.Order, driver.SelectionString(status.String))
				case driver.SelectionTitle:
					key.Order = append(key.Order, driver.SelectionString(title.String))
				case driver.SelectionRevision:
					key.Order = append(key.Order, driver.SelectionString(revision.String))
				case driver.SelectionPriority:
					key.Order = append(key.Order, driver.SelectionInt(priority.Int64))
				}
			}
			result.Keys = append(result.Keys, key)
			// Only thin keys actually decoded from SQLite are counted here;
			// this is never the authorized SQL input or VM-step count.
			driver.ReportReadWork(ctx, driver.ReadSelectorKeys, 1)
		default:
			return zero, fmt.Errorf("%w: SQLite selection unknown result row", driver.ErrUnsupported)
		}
	}
	if err = rows.Err(); err != nil {
		return zero, err
	}
	if !haveMeta || total > uint64(p.EffectiveCap()) {
		if total > uint64(p.EffectiveCap()) {
			return zero, driver.ErrSelectionCap{Limit: p.EffectiveCap()}
		}
		return zero, fmt.Errorf("%w: SQLite selection missing aggregate", driver.ErrUnsupported)
	}
	if len(result.Groups) > driver.SelectionGroupCap {
		return zero, driver.ErrSelectionCap{Limit: driver.SelectionGroupCap, Groups: true}
	}
	if p.Page != nil {
		if uint64(start) > total {
			return zero, fmt.Errorf("%w: SQLite selection offset beyond membership", driver.ErrUnsupported)
		}
		result.More = uint64(start)+uint64(p.Page.Limit) < total
		if result.More && len(result.Keys) > 0 {
			binding, err := p.Binding()
			if err != nil {
				return zero, err
			}
			result.Next = &driver.SelectionKeyset{Binding: binding, Key: result.Keys[len(result.Keys)-1]}
		}
	}
	result.Total = total
	if err = ctx.Err(); err != nil {
		return zero, err
	}
	return result, nil
}

func (r *reader) preflightSelection(ctx context.Context, p driver.SelectionPlan) (map[selectionFactID]selectionFactBinding, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if p.Incarnation != r.meta.Incarnation {
		return nil, driver.ErrIncarnationChanged{From: p.Incarnation, To: r.meta.Incarnation}
	}
	c, _ := p.ContentRevision.Uint64()
	a, _ := p.AuthorityRevision.Uint64()
	if a != r.at || r.at != mustUint(r.meta.Head) {
		return nil, fmt.Errorf("%w: SQLite selection requires bound current authority", driver.ErrUnsupported)
	}
	floor := mustUint(r.meta.HistoryFloor)
	if c < floor {
		return nil, driver.ErrTruncated{Floor: r.meta.HistoryFloor}
	}
	boundary, err := r.validateSelectionBoundary(ctx, p, a)
	if err != nil {
		return nil, err
	}
	// The counting bead family is required at both planes, even for false,
	// empty and zero-limit plans. Record-sourced fact families are checked
	// alongside their exact envelopes before any boolean branch is evaluated.
	sortFamily := sqliteSelectionFamilies()[0]
	if err = r.requireSelectionFamily(ctx, sortFamily, c); err != nil {
		return nil, err
	}
	if err = r.requireSelectionFamily(ctx, sortFamily, a); err != nil {
		return nil, err
	}
	if err = r.validateSelectionRelations(ctx, p, c); err != nil {
		return nil, err
	}
	if r.owner != nil {
		if err = r.validateSelectionOverlay(ctx); err != nil {
			return nil, err
		}
		covered, completeErr := r.owner.selectionFamilyCompleteForCommit(ctx, r.tx, r.meta.Incarnation, sortFamily, r.owner.derived)
		if completeErr != nil {
			return nil, completeErr
		}
		if !covered {
			return nil, fmt.Errorf("%w: SQLite staged counting family incomplete", driver.ErrSelectionUnavailable)
		}
	}
	facts := make(map[selectionFactID]selectionFactBinding)
	if err = r.selectionFacts(ctx, p.Membership, c, p.ContentCommitted, facts); err != nil {
		return nil, err
	}
	if err = r.selectionFacts(ctx, p.Authorization, a, false, facts); err != nil {
		return nil, err
	}
	if boundary.HostExpression != nil {
		if err = r.selectionFacts(ctx, *boundary.HostExpression, a, false, facts); err != nil {
			return nil, err
		}
	}
	return facts, nil
}

var _ driver.SelectionReader = (*reader)(nil)
var _ driver.SelectionReader = (*transaction)(nil)

func (t *transaction) FreshSelectionExists(ctx context.Context, p driver.SelectionPlan) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return false, err
	}
	if !t.hasHead {
		return false, fmt.Errorf("%w: fresh existence requires Head", driver.ErrSelectionInvalid)
	}
	if p.Mode != driver.SelectionCountMode || p.Cap != 0 || p.Page != nil || p.GroupBy != "" || len(p.Excluded) != 0 || p.ContentRevision.String() != p.AuthorityRevision.String() {
		return false, fmt.Errorf("%w: fresh existence requires uncapped current count intent", driver.ErrSelectionInvalid)
	}
	// The reserved SQLite writer has a pinned committed snapshot. Omitting
	// owner excludes every staged record, typed row and source declaration.
	p.ContentCommitted = true
	committed := reader{store: t.store, tx: t.tx, meta: t.meta, at: t.at}
	facts, err := committed.preflightSelection(ctx, p)
	if err != nil {
		return false, err
	}
	b := sqliteSelectionSQL{facts: facts, meta: t.meta}
	query, err := b.existsQuery(p)
	if err != nil {
		return false, err
	}
	if len(b.args) > 32000 {
		return false, fmt.Errorf("%w: SQLite selection binding budget exceeded", driver.ErrUnsupported)
	}
	var yes int
	if err := t.tx.QueryRowContext(ctx, query, b.args...).Scan(&yes); err != nil {
		return false, err
	}
	return yes != 0, nil
}

var _ driver.FreshSelectionExistsReader = (*transaction)(nil)

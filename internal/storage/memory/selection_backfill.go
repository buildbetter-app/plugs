package memory

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Coverage and its source-to-row census are immutable with their state. A gap
// retains earlier truthful intervals; later complete writes cannot fill it.
type selectionCoverageState struct {
	ranges  map[driver.SelectionProjectionFamily][]driver.SelectionCoverage
	owners  *selectionCensusNode
	sources *selectionCensusNode
	targets *selectionCensusNode
	virgin  bool // proven new source lifetime; never inferred from current emptiness
}

type selectionRowOwner struct {
	family driver.SelectionProjectionFamily
	source string
}

// Only a successful same-call typed publication may explain removal of a cold
// row. The build identity binds its incarnation and declared family layout;
// neither a later call nor another build may inherit this provenance.
type selectionBuildDeletion struct {
	owner selectionRowOwner
	build *selectionBuild
}

func (t *transaction) selectionBuildDeletedOwner(id string) (selectionRowOwner, bool) {
	deleted, ok := t.selectionBuildDeletes[id]
	b := t.store.selectionBuild
	return deleted.owner, ok && b != nil && deleted.build == b && b.incarnation == t.incarnation && deleted.owner.family.Validate() == nil
}

// Called under Store.mu only after PutDerived has validated its entire call,
// before replacing the previous transaction delta. Inputs declares opaque
// ownership; storage never decodes a closure key or source body.
func (t *transaction) recordSelectionBuildDeletes(table driver.DerivedTable, del, add []driver.DerivedRow) {
	b := t.store.selectionBuild
	if b == nil {
		return
	}
	coreTable := false
	for _, family := range b.project.Families {
		coreTable = coreTable || family.Table == table
	}
	if !coreTable {
		return
	}
	rowID := func(row driver.DerivedRow) string {
		return keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}})
	}
	// A subsequent raw overwrite cannot borrow an earlier typed declaration.
	for _, rows := range [][]driver.DerivedRow{del, add} {
		for _, row := range rows {
			delete(t.selectionBuildDeletes, rowID(row))
		}
	}
	if len(del) == 0 {
		return
	}
	type candidate struct {
		owner  selectionRowOwner
		ranges []driver.DerivedRowRange
	}
	candidates := map[selectionRowOwner]candidate{}
	declare := func(source string, record driver.Record, family driver.SelectionProjectionFamily) {
		if record.Kind != "bead" {
			return
		}
		inputs, err := b.project.Inputs(copyRecord(record))
		if err != nil {
			return // Ordinary writes remain supported; the build fails closed.
		}
		owner := selectionRowOwner{source: source, family: family}
		for _, input := range inputs {
			if input.Table != table || input.Max <= 0 || input.Max > b.project.MaxPageRows {
				continue
			}
			entry := candidates[owner]
			entry.owner = owner
			input.Prefix = bytes.Clone(input.Prefix)
			entry.ranges = append(entry.ranges, input)
			candidates[owner] = entry
		}
	}
	for _, row := range add {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		family, typed := rowFamily(driver.LockState{Key: key, Present: true, Derived: &row})
		if !typed || row.Index.Incarnation != b.incarnation {
			continue
		}
		declared := false
		for _, requested := range b.project.Families {
			declared = declared || family == requested
		}
		if !declared {
			continue
		}
		source := keyID(row.Index.Source.Key)
		record := t.base.records[source]
		if staged, ok := t.records[source]; ok {
			if staged == nil {
				continue
			}
			record = *staged
		}
		owner := selectionRowOwner{source: source, family: family}
		if _, exists := candidates[owner]; !exists {
			declare(source, record, family)
		}
	}
	// A real source deletion has no add rows. Its staged deletion supplies the
	// owner; the observer below still requires every declared input to be gone.
	for source, record := range t.records {
		if record != nil {
			continue
		}
		for _, family := range b.project.Families {
			if family.Table == table {
				declare(source, t.base.records[source], family)
			}
		}
	}
	// Match the declared opaque ranges once by byte prefix. A large moved
	// subtree must not compare every removal against every affected source.
	type rangeNode struct {
		children map[byte]*rangeNode
		owners   []selectionRowOwner
	}
	root := &rangeNode{}
	for _, candidate := range candidates {
		for _, input := range candidate.ranges {
			node := root
			for _, value := range input.Prefix {
				if node.children == nil {
					node.children = map[byte]*rangeNode{}
				}
				if node.children[value] == nil {
					node.children[value] = &rangeNode{}
				}
				node = node.children[value]
			}
			node.owners = append(node.owners, candidate.owner)
		}
	}
	for _, row := range del {
		id := rowID(row)
		old := t.base.keys[id]
		if old.Derived != nil && old.Derived.Index != nil {
			continue // Typed/independent domains keep their existing exact checks.
		}
		_, previouslyStaged := t.derived[id]
		if !old.Present && !previouslyStaged {
			continue // Removing an already absent row changes no logical input.
		}
		matches := 0
		var owner selectionRowOwner
		if !previouslyStaged && old.Present && old.Derived != nil && row.Index == nil && old.Derived.Key == row.Key && bytes.Equal(old.Derived.Value, row.Value) {
			for node, offset := root, 0; node != nil; offset++ {
				for _, declared := range node.owners {
					matches++
					owner = declared
				}
				if offset == len(row.Key) {
					break
				}
				node = node.children[row.Key[offset]]
			}
		}
		if matches != 1 {
			// Sticky within this transaction: raw delete then later typed add
			// cannot launder the earlier call. Rollback publishes no verdict.
			t.selectionBuildRawDelete = true
			continue
		}
		if t.selectionBuildDeletes == nil {
			t.selectionBuildDeletes = map[string]selectionBuildDeletion{}
		}
		t.selectionBuildDeletes[id] = selectionBuildDeletion{owner: owner, build: b}
	}
}

func selectionFamilies() []driver.SelectionProjectionFamily {
	return []driver.SelectionProjectionFamily{
		{Table: driver.DerivedSortKey, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedAttribute, Kind: driver.SelectionLabelFact, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedReferenceIndex, Kind: driver.SelectionLinkFact, Version: driver.SelectionIndexVersion},
		{Table: driver.DerivedAncestorClosure, Kind: driver.SelectionAncestorFact, Version: driver.SelectionIndexVersion},
	}
}

func newSelectionCoverage() *selectionCoverageState {
	// New is unbuilt. Only an explicit complete build or a complete first
	// typed publication can establish a range, at that operation's own Head.
	return &selectionCoverageState{ranges: map[driver.SelectionProjectionFamily][]driver.SelectionCoverage{}, virgin: true}
}

func (p *selectionCoverageState) clone() *selectionCoverageState {
	n := &selectionCoverageState{ranges: map[driver.SelectionProjectionFamily][]driver.SelectionCoverage{}}
	if p != nil {
		n.virgin = p.virgin
		for family, ranges := range p.ranges {
			n.ranges[family] = append([]driver.SelectionCoverage(nil), ranges...)
		}
		n.owners, n.sources, n.targets = p.owners, p.sources, p.targets
	}
	return n
}

// requireSelectionCoverage is consumed by the owning evaluator before any
// candidates, booleans, exclusions or probe. It adds no graph-domain coupling.
// The three fact-family ranges apply only to record-sourced envelopes.
// Independently complete derived-source envelopes, record-sourced
// Grant/Project/Team and CutClosure Ancestor retain exact envelope admission;
// this helper does not invent a global family for them.
func (v selectionView) requireSelectionCoverage(family driver.SelectionProjectionFamily, at uint64) error {
	if v.state.selection != nil {
		for _, coverage := range v.state.selection.ranges[family] {
			if coverage.Require(family, v.state.meta.Incarnation, model.Natural64(0), model.Natural64(at)) == nil {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: unbuilt selection family/revision", driver.ErrSelectionUnavailable)
}

func rowFamily(s driver.LockState) (driver.SelectionProjectionFamily, bool) {
	if !s.Present || s.Derived == nil || s.Key.Derived == nil || s.Derived.Index == nil {
		return driver.SelectionProjectionFamily{}, false
	}
	// Table/kind/version alone do not establish startup ownership. Complete
	// derived-source envelopes sharing these tables keep their own admission,
	// census and history; neither catch-up nor orphan reconciliation owns them.
	if s.Derived.Index.Source.Key.Kind != driver.LockRecord || s.Derived.Index.Source.Key.Ref == nil {
		return driver.SelectionProjectionFamily{}, false
	}
	for _, family := range selectionFamilies() {
		if family.Matches(driver.DerivedSelectionRow{Table: s.Key.Derived.Table, Row: *s.Derived}) {
			return family, true
		}
	}
	return driver.SelectionProjectionFamily{}, false
}

// prepareSelectionCoverage never rejects an otherwise accepted source write.
// It extends ONLY an existing certified interval when the entire affected
// source/family delta is complete. Untyped or incomplete work leaves a gap.
func (t *transaction) prepareSelectionCoverage(next *state) map[driver.SelectionProjectionFamily]bool {
	before := t.store.state
	p := before.selection.clone()
	if t.restore != nil {
		p = (*selectionCoverageState)(nil).clone()
	}
	valid := map[driver.SelectionProjectionFamily]bool{}
	deltaValid := map[driver.SelectionProjectionFamily]bool{}
	for _, family := range selectionFamilies() {
		valid[family] = true
		deltaValid[family] = true
	}
	affected := map[string]bool{}
	required := map[string]map[driver.SelectionProjectionFamily]bool{}
	require := func(source string, family driver.SelectionProjectionFamily) {
		affected[source] = true
		if required[source] == nil {
			required[source] = map[driver.SelectionProjectionFamily]bool{}
		}
		required[source][family] = true
	}
	invalidate := func(source string, family driver.SelectionProjectionFamily) {
		valid[family] = false
		if required[source][family] {
			deltaValid[family] = false
		}
	}
	for id := range t.records {
		if before.kinds[id] == "bead" || next.kinds[id] == "bead" {
			for _, family := range selectionFamilies() {
				require(id, family)
			}
		}
	}
	for id := range t.bumps {
		if before.kinds[id] == "bead" {
			for _, family := range selectionFamilies() {
				require(id, family)
			}
		}
	}
	v := selectionView{state: next}
	v.at, _ = next.meta.Head.Uint64()
	for id, changed := range t.derived {
		if old, ok := p.owners.get(id); ok {
			require(old.source, old.family)
			p.removeOwner(id, before.keys[id])
		}
		if old, ok := t.selectionBuildDeletedOwner(id); ok {
			require(old.source, old.family)
		}
		if !changed.Present {
			continue
		}
		actual := next.keys[id]
		family, typed := rowFamily(actual)
		if !typed {
			if actual.Derived != nil && actual.Derived.Index != nil {
				// A different admitted complete domain sharing this table does
				// not change the source-built family.
				continue
			}
			for _, family := range selectionFamilies() {
				if changed.Key.Derived != nil && changed.Key.Derived.Table == family.Table {
					valid[family] = false
					deltaValid[family] = false
				}
			}
			continue
		}
		index := actual.Derived.Index
		if index.Source.Key.Kind != driver.LockRecord || index.Source.Key.Ref == nil {
			// Other admitted complete fact sets keep their per-envelope checks.
			continue
		}
		source := keyID(index.Source.Key)
		if next.kinds[source] != "bead" {
			continue
		}
		require(source, family)
		p.putOwner(id, selectionRowOwner{family: family, source: source}, actual)
		if _, err := v.index(*actual.Derived, family.Table, v.at); err != nil {
			invalidate(source, family)
			continue
		}
	}
	// An affected source's FULL remaining typed delta matters, including old
	// rows that were not replaced. One new self row cannot cover stale ancestors.
	for source := range affected {
		counts := map[driver.SelectionProjectionFamily]int{}
		p.sourceRows(source, func(id string, owner selectionRowOwner) {
			actual := next.keys[id]
			family, typed := rowFamily(actual)
			if !typed || family != owner.family {
				invalidate(source, owner.family)
				return
			}
			if _, err := v.index(*actual.Derived, owner.family.Table, v.at); err != nil {
				invalidate(source, owner.family)
			}
			counts[owner.family]++
		})
		for _, family := range selectionFamilies() {
			if record, live := next.records[source]; live && record.Kind == "bead" {
				if counts[family] == 0 || family.Kind == "" && counts[family] != 1 {
					invalidate(source, family)
				}
			} else if counts[family] != 0 {
				invalidate(source, family)
			}
		}
	}
	// Changed source bytes require all families; a closure-only update to an
	// unchanged descendant requires its complete closure delta. Untouched cold
	// families stay unavailable and private until the final whole-source proof.
	// Global published-parent readiness is likewise a coverage-only constraint.
	if !selectionAffectedParentChains(before, next, p, affected, v) {
		valid[driver.SelectionProjectionFamily{Table: driver.DerivedAncestorClosure, Kind: driver.SelectionAncestorFact, Version: driver.SelectionIndexVersion}] = false
	}
	neverPopulated := t.restore == nil && before.selection != nil && before.selection.virgin && before.meta.Incarnation == next.meta.Incarnation && before.meta.HistoryFloor.IsZero()
	for _, family := range selectionFamilies() {
		if !valid[family] {
			continue
		}
		ranges := p.ranges[family]
		if t.restore != nil {
			// A current-image restore has no transferable old temporal proof.
			p.ranges[family] = []driver.SelectionCoverage{{Family: family, Incarnation: next.meta.Incarnation, Branch: model.Natural64(0), First: next.meta.Head, Through: next.meta.Head}}
		} else if len(ranges) > 0 && ranges[len(ranges)-1].Require(family, before.meta.Incarnation, model.Natural64(0), before.meta.Head) == nil {
			ranges[len(ranges)-1].Through = next.meta.Head
		} else if len(ranges) == 0 && neverPopulated && len(affected) > 0 {
			p.ranges[family] = []driver.SelectionCoverage{{Family: family, Incarnation: next.meta.Incarnation, Branch: model.Natural64(0), First: next.meta.Head, Through: next.meta.Head}}
		}
	}
	if len(affected) > 0 {
		p.virgin = false
	}
	next.selection = p
	return deltaValid
}

// A build watches only actual successful publications. No uncommitted delta
// can dirty, invalidate or advance this observer.
type selectionBuild struct {
	incarnation string
	through     model.Revision
	dirty       map[string]bool
	invalid     bool
	project     driver.GraphSourceProjection
}

func (t *transaction) publishSelectionBuildChanges(next *state, valid map[driver.SelectionProjectionFamily]bool) {
	b := t.store.selectionBuild
	if b == nil {
		return
	}
	if next.meta.Incarnation != b.incarnation || t.restore != nil {
		b.invalid = true
	}
	if t.selectionBuildRawDelete {
		b.invalid = true
	}
	for _, family := range b.project.Families {
		if !valid[family] {
			b.invalid = true
		}
	}
	// Validate only this publication. Earlier successful dirty sources remain
	// queued for startup catch-up and need not be scanned again on every save.
	dirty := map[string]bool{}
	mark := func(source string) { dirty[source], b.dirty[source] = true, true }
	published := map[selectionRowOwner]bool{}
	for id := range t.derived {
		if owner, ok := next.selection.owners.get(id); ok {
			published[owner] = true
		}
	}
	for id, record := range t.records {
		if t.store.state.kinds[id] == "bead" || record != nil && record.Kind == "bead" {
			mark(id)
			if record != nil {
				for _, family := range selectionFamilies() {
					if !published[selectionRowOwner{source: id, family: family}] {
						b.invalid = true
					}
				}
			}
		}
	}
	for id, changed := range t.derived {
		if !changed.Present {
			if !t.store.state.keys[id].Present {
				continue // Ordinary cold deletion may include absent family rows.
			}
			if owner, ok := t.selectionBuildDeletedOwner(id); ok {
				mark(owner.source)
				continue
			}
		}
		for _, family := range selectionFamilies() {
			if changed.Key.Derived == nil || changed.Key.Derived.Table != family.Table {
				continue
			}
			state := next.keys[id]
			if !state.Present {
				state = t.store.state.keys[id]
			}
			if _, typed := rowFamily(state); !typed {
				if state.Derived != nil && state.Derived.Index != nil {
					continue
				}
				b.invalid = true
				continue
			}
			if state.Derived.Index.Source.Key.Kind != driver.LockRecord || state.Derived.Index.Source.Key.Ref == nil {
				continue
			}
			mark(keyID(state.Derived.Index.Source.Key))
		}
	}
	for id := range t.bumps {
		if t.store.state.kinds[id] == "bead" {
			b.invalid = true
		}
	}
	// Every remaining declared closure input after an inter-page publication
	// must ALREADY have its exact final source binding. Catch-up must not repair
	// incomplete/raw deltas from final values and turn them into certification.
	v := selectionView{state: next, at: mustSelectionHead(next)}
	for source := range dirty {
		record, live := next.records[source]
		if !live {
			record = t.store.state.records[source]
		}
		if record.Kind != "bead" {
			continue
		}
		inputs, err := b.project.Inputs(copyRecord(record))
		if err != nil {
			b.invalid = true
			continue
		}
		total := 0
		for _, input := range inputs {
			count := 0
			catalog := next.derivedKeys[input.Table]
			for j := sort.SearchStrings(catalog, string(input.Prefix)); j < len(catalog) && strings.HasPrefix(catalog[j], string(input.Prefix)); j++ {
				id := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: input.Table, Key: catalog[j]}})
				actual := next.keys[id]
				if !actual.Present {
					continue
				}
				count++
				total++
				if !live || count > input.Max || total > b.project.MaxPageRows || actual.Derived == nil || actual.Derived.Index == nil || keyID(actual.Derived.Index.Source.Key) != source {
					b.invalid = true
					continue
				}
				if _, err := v.index(*actual.Derived, input.Table, v.at); err != nil {
					b.invalid = true
				}
			}
			if live && count == 0 {
				b.invalid = true
			}
		}
	}
	b.through = next.meta.Head
}

func (s *Store) BuildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder, projections ...driver.GraphSourceProjection) error {
	if err := s.buildGraphSourceIndexes(ctx, lease, decode); err != nil {
		return err
	}
	for _, projection := range projections {
		if err := s.buildSelectionSourceIndexes(ctx, lease, decode, projection); err != nil {
			return driver.NewSelectionBackfillRefused(err)
		}
	}
	return nil
}

func (s *Store) buildSelectionSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder, project driver.GraphSourceProjection) error {
	if err := project.Validate(); err != nil {
		return err
	}
	value, err := s.Write(ctx, lease)
	if err != nil {
		return err
	}
	defer value.Close()
	t := value.(*transaction)
	s.mu.Lock()
	if err := t.check(ctx); err != nil {
		s.mu.Unlock()
		return err
	}
	initial := s.state
	at, _ := initial.meta.Head.Uint64()
	ready := true
	for _, family := range project.Families {
		ready = ready && (selectionView{state: initial}).requireSelectionCoverage(family, at) == nil
	}
	if ready {
		s.mu.Unlock()
		return nil
	}
	if s.selectionBuild != nil {
		s.mu.Unlock()
		return fmt.Errorf("%w: selection build already active", driver.ErrSelectionUnavailable)
	}
	project.Families = append([]driver.SelectionProjectionFamily(nil), project.Families...)
	watch := &selectionBuild{incarnation: initial.meta.Incarnation, through: initial.meta.Head, dirty: map[string]bool{}, project: project}
	s.selectionBuild = watch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.selectionBuild == watch {
			s.selectionBuild = nil
		}
		s.mu.Unlock()
	}()
	staged := map[string][]driver.DerivedSelectionRow{}
	ids := []string{}
	for id, record := range initial.records {
		if record.Kind == "bead" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if err := selectionBuildPages(ctx, initial, ids, decode, project, staged); err != nil {
		return err
	}
	// Catch up bounded pages of complete ordinary typed publications. A short
	// final graph/Head fence checks that no publication escaped the observer.
	for attempt := 0; attempt < driver.SelectionBackfillPageSize; attempt++ {
		s.mu.Lock()
		if err := t.check(ctx); err != nil {
			s.mu.Unlock()
			return err
		}
		current := s.state
		if watch.invalid || watch.incarnation != current.meta.Incarnation || watch.through != current.meta.Head {
			s.mu.Unlock()
			return fmt.Errorf("%w: untyped or gapped inter-page publication", driver.ErrSelectionUnavailable)
		}
		dirty := make([]string, 0, len(watch.dirty))
		for id := range watch.dirty {
			dirty = append(dirty, id)
		}
		watch.dirty = map[string]bool{}
		s.mu.Unlock()
		sort.Strings(dirty)
		if err := selectionBuildPages(ctx, current, dirty, decode, project, staged); err != nil {
			return err
		}
		next, err := prepareSelectionBuildState(ctx, current, staged, project)
		if err != nil {
			return err
		}
		final, err := s.Write(ctx, lease)
		if err != nil {
			return err
		}
		if _, err = final.Lock(ctx, driver.LockSet{{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate}}); err == nil {
			_, err = final.Head(ctx)
		}
		if err != nil {
			final.Close()
			return err
		}
		s.mu.Lock()
		fence := final.(*transaction)
		if err = fence.check(ctx); err == nil && (watch.invalid || watch.through != s.state.meta.Head || watch.incarnation != s.state.meta.Incarnation) {
			err = fmt.Errorf("%w: selection final source fence changed", driver.ErrSelectionUnavailable)
		}
		if err != nil {
			s.mu.Unlock()
			final.Close()
			return err
		}
		if len(watch.dirty) > 0 {
			s.mu.Unlock()
			final.Close()
			continue
		}
		// All census/proof/copy work happened on the immutable current
		// snapshot outside the final fence. A changed state (even a response
		// finalization at the same Head) must never be overwritten.
		if s.state != current {
			s.mu.Unlock()
			final.Close()
			continue
		}
		if err = fence.check(ctx); err == nil {
			s.state = next
		}
		s.mu.Unlock()
		final.Close()
		return err
	}
	return fmt.Errorf("%w: selection build could not reach a stable final step", driver.ErrSelectionUnavailable)
}

// prepareSelectionBuildState verifies the complete typed census, current source
// bindings and exact logical values before taking the short final fence. Private
// staging has no readable intervals and cannot escape a failed build.
func prepareSelectionBuildState(ctx context.Context, current *state, staged map[string][]driver.DerivedSelectionRow, project driver.GraphSourceProjection) (*state, error) {
	v := selectionView{state: current}
	v.at = mustSelectionHead(current)
	building := map[driver.SelectionProjectionFamily]bool{}
	for _, family := range project.Families {
		building[family] = v.requireSelectionCoverage(family, v.at) != nil
	}
	expected := map[string]driver.DerivedSelectionRow{}
	var chains []driver.DerivedSelectionRow
	for source, rows := range staged {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record, live := current.records[source]
		if !live || record.Kind != "bead" {
			return nil, fmt.Errorf("%w: orphan staged selection source", driver.ErrSelectionUnavailable)
		}
		for _, row := range rows {
			if _, err := v.index(row.Row, row.Table, v.at); err != nil {
				return nil, err
			}
			if keyID(row.Row.Index.Source.Key) != source {
				return nil, fmt.Errorf("%w: inconsistent staged selection owner", driver.ErrSelectionUnavailable)
			}
			key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: row.Table, Key: row.Row.Key}}
			id := keyID(key)
			if _, duplicate := expected[id]; duplicate {
				return nil, fmt.Errorf("%w: duplicate cross-page selection identity", driver.ErrSelectionUnavailable)
			}
			expected[id] = row
			actual := current.keys[id]
			if actual.Present && (actual.Derived == nil || actual.Derived.Key != row.Row.Key || !bytes.Equal(actual.Derived.Value, row.Row.Value)) || row.Table == driver.DerivedAncestorClosure && !actual.Present {
				return nil, fmt.Errorf("%w: startup cannot rewrite logical selection values", driver.ErrSelectionUnavailable)
			}
			chains = append(chains, row)
		}
	}
	for source, record := range current.records {
		if record.Kind == "bead" && len(staged[source]) == 0 {
			return nil, fmt.Errorf("%w: incomplete live selection source census", driver.ErrSelectionUnavailable)
		}
	}
	for _, family := range project.Families {
		for _, key := range current.derivedKeys[family.Table] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			id := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: family.Table, Key: key}})
			actual := current.keys[id]
			if typed, ok := rowFamily(actual); ok && typed == family {
				if kind := current.kinds[keyID(actual.Derived.Index.Source.Key)]; kind != "" && kind != "bead" {
					continue // Independently complete nonbead record-source domain.
				}
				if _, covered := expected[id]; !covered {
					return nil, fmt.Errorf("%w: orphan current selection row", driver.ErrSelectionUnavailable)
				}
			}
		}
		if family.Kind == driver.SelectionAncestorFact {
			if err := driver.ValidateSelectionParentChains(chains); err != nil {
				return nil, err
			}
		}
	}
	next := current.fork()
	coverage := next.selection.clone()
	coverage.owners.each(func(id string, owner selectionRowOwner) {
		if building[owner.family] {
			coverage.removeOwner(id, current.keys[id])
		}
	})
	changes := map[string]driver.LockState{}
	for id, row := range expected {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: row.Table, Key: row.Row.Key}}
		owned := row.Row.Clone()
		state := driver.LockState{Key: key, Revision: current.meta.Head, Present: true, Derived: &owned}
		family, _ := rowFamily(state)
		if !building[family] {
			continue // Already certified family rows/history remain exact.
		}
		changes[id] = state
		coverage.putOwner(id, selectionRowOwner{family: family, source: keyID(owned.Index.Source.Key)}, state)
		next.publishKey(id, state)
	}
	next.publishDerivedKeys(changes)
	for _, family := range project.Families {
		if building[family] {
			coverage.ranges[family] = append(coverage.ranges[family], driver.SelectionCoverage{Family: family, Incarnation: current.meta.Incarnation, Branch: model.Natural64(0), First: current.meta.Head, Through: current.meta.Head})
		}
	}
	next.selection = coverage
	return next, ctx.Err()
}

func mustSelectionHead(s *state) uint64 {
	n, _ := s.meta.Head.Uint64()
	return n
}

func selectionBuildPages(ctx context.Context, current *state, ids []string, decode driver.GraphSourceDecoder, project driver.GraphSourceProjection, staged map[string][]driver.DerivedSelectionRow) error {
	for offset := 0; offset < len(ids); offset += driver.SelectionBackfillPageSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		records := []driver.Record{}
		metadata := []driver.GraphSourceMetadata{}
		for _, id := range ids[offset:min(offset+driver.SelectionBackfillPageSize, len(ids))] {
			delete(staged, id)
			record, present := current.records[id]
			if !present || record.Kind != "bead" {
				continue
			}
			image := copyRecord(record)
			decoded, err := decode(image)
			if err != nil {
				return err
			}
			records, metadata = append(records, image), append(metadata, decoded)
		}
		page, err := driver.NewSelectionProjectionPage(records, metadata, project)
		if err != nil {
			return err
		}
		inputs := make([][]driver.DerivedRow, len(page.Ranges))
		total := 0
		for slot, input := range page.Ranges {
			catalog := current.derivedKeys[input.Table]
			for j := sort.SearchStrings(catalog, string(input.Prefix)); j < len(catalog) && strings.HasPrefix(catalog[j], string(input.Prefix)); j++ {
				key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: input.Table, Key: catalog[j]}}
				state := current.keys[keyID(key)]
				if !state.Present {
					continue
				}
				total++
				if state.Derived == nil || len(inputs[slot]) >= input.Max || total > project.MaxPageRows {
					return fmt.Errorf("%w: selection closure range/page unavailable", driver.ErrSelectionUnavailable)
				}
				inputs[slot] = append(inputs[slot], state.Derived.Clone())
			}
		}
		rows, err := page.Decode(inputs, current.meta.Incarnation)
		if err != nil {
			return err
		}
		for i, record := range records {
			id, _ := recordKey(record.Ref)
			staged[id] = rows[i]
		}
	}
	return nil
}

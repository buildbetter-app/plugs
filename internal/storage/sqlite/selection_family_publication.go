package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// A first typed publication may establish a new interval only when every
// bead known to this store is introduced in this very commit. Objects from a
// deleted or imported lifetime prevent a later final-image recertification.
func (t *transaction) selectionNeverSourcedBefore(ctx context.Context, c queryer, current driver.Meta) (bool, error) {
	// An imported source starts a new incarnation, not a virgin authored
	// lifetime. Its complete final image must go through explicit backfill;
	// the restore commit cannot certify source history it did not publish.
	if t.restore != nil {
		return false, nil
	}
	// Open initializes this marker from legacy objects once. A missing marker
	// is conservatively non-virgin so an unusual pre-marker incarnation can
	// never gain a first-publication certificate by inference.
	var virgin int
	err := c.QueryRowContext(ctx, `SELECT virgin FROM selection_virgin_state WHERE incarnation=?`, current.Incarnation).Scan(&virgin)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if virgin != 1 {
		return false, nil
	}
	for _, record := range t.records {
		if record != nil && record.Kind == "bead" {
			return true, nil
		}
	}
	return false, nil
}

// Complete typed rows must accompany every changed live bead. Existing fact
// identities may be replaced, but an untyped/deleted row for a still-live
// source or a changed source with no complete replacement invalidates only
// that family's coverage. The final Ancestor chain is checked separately.
func (t *transaction) selectionFamilyCompleteForCommit(ctx context.Context, c queryer, incarnation string, family driver.SelectionProjectionFamily, derived map[string]driver.LockState) (bool, error) {
	if family.Kind == "" {
		return t.selectionCompleteForCommit(ctx, c, incarnation, derived)
	}
	// Ancestor closures are represented as one physical derived row per
	// source/depth. The other typed families have one row per source, but an
	// ancestor source legitimately owns several rows. Keep both the complete
	// staged replacement set and explicit removals by source. Hierarchy delta
	// publication can remove stale rows for an unchanged descendant before
	// adding the complete replacement set; that is valid only when every old
	// row is accounted for and at least one complete replacement remains.
	bySource := map[string]map[string]struct{}{}
	removedBySource := map[string]map[string]struct{}{}
	touchedSources := map[string]struct{}{}
	for id, state := range derived {
		if state.Key.Derived == nil {
			continue
		}
		// The encoded derived key carries its table. Do not issue a typed family
		// lookup for rows owned by another family; the old implementation made
		// every family scan the full staged overlay on every commit.
		if state.Key.Derived.Table != family.Table {
			continue
		}
		var oldSource, oldKind string
		err := c.QueryRowContext(ctx, `SELECT f.source_key,f.kind FROM selection_fact_sets f
			JOIN objects o ON o.key=f.source_key AND o.kind='bead'
			WHERE f.incarnation=? AND f.branch=? AND f.row_key=? AND f.row_table=? AND f.end_revision IS NULL
			AND f.source_key>='0:' AND f.source_key<'0;'`,
			incarnation, revKey(0), id, string(family.Table)).Scan(&oldSource, &oldKind)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		if oldKind == string(family.Kind) && oldSource != "" {
			touchedSources[oldSource] = struct{}{}
		}
		newFamily := state.Present && state.Derived != nil && state.Derived.Index != nil &&
			state.Derived.Index.Source.Key.Kind == driver.LockRecord &&
			family.Matches(driver.DerivedSelectionRow{Table: state.Key.Derived.Table, Row: *state.Derived})
		if newFamily {
			var known bool
			newFamily, known, err = t.selectionRecordBuiltSource(ctx, c, keyID(state.Derived.Index.Source.Key))
			if err != nil {
				return false, err
			}
			if !known {
				return false, nil // an unknown or deleted record owner cannot certify
			}
		}
		if oldKind == string(family.Kind) && state.Present && !newFamily {
			// A present untyped or wrong-family replacement is an ordinary/raw
			// write to an existing typed row. It cannot be reinterpreted as the
			// explicit remove arm of a complete hierarchy delta.
			return false, nil
		}
		if oldKind == string(family.Kind) && !newFamily {
			rows := removedBySource[oldSource]
			if rows == nil {
				rows = make(map[string]struct{})
				removedBySource[oldSource] = rows
			}
			rows[id] = struct{}{}
		}
		if !newFamily {
			continue
		}
		source := state.Derived.Index.Source
		if source.Key.Kind != driver.LockRecord || source.Key.Ref == nil {
			return false, nil
		}
		sourceID := keyID(source.Key)
		touchedSources[sourceID] = struct{}{}
		if record, changed := t.records[sourceID]; changed && (record == nil || record.Kind != "bead") {
			return false, nil
		}
		rows := bySource[sourceID]
		if rows == nil {
			rows = make(map[string]struct{})
			bySource[sourceID] = rows
		}
		if _, duplicate := rows[id]; duplicate {
			return false, nil
		}
		if family.Kind != driver.SelectionAncestorFact && len(rows) != 0 {
			return false, nil
		}
		rows[id] = struct{}{}
		if oldKind == string(family.Kind) && oldSource != sourceID {
			removed, changed := t.records[oldSource]
			if !changed || removed != nil {
				return false, nil
			}
		}
	}
	for id, record := range t.records {
		if record != nil && record.Kind == "bead" {
			if _, complete := bySource[id]; !complete {
				return false, nil
			}
		}
		// Delete records have no typed overlay row of their own, but their
		// source identity still closes every active fact envelope below. Keep
		// deleted bead sources in the touched set so a raw Delete cannot carry
		// ready coverage without an explicit tombstone for each physical row.
		if record == nil {
			if identity, known := t.sourceIdentities[id]; known && identity.Kind == "bead" {
				touchedSources[id] = struct{}{}
			}
		}
	}
	for source := range touchedSources {
		staged := bySource[source]
		if record, changed := t.records[source]; changed {
			if record != nil && record.Kind == "bead" && len(staged) == 0 {
				return false, nil
			}
			if record != nil {
				continue // source publication closes its previous fact sets
			}
			// A raw bead deletion is complete only when every active typed row
			// for this source has an explicit Present=false derived state. A
			// pipeline delete may remove all rows and is accepted below; an
			// unannotated or partial delete remains unavailable.
			if len(staged) != 0 {
				return false, nil
			}
		}
		rows, err := querySelectionSourceFactRows(ctx, c, incarnation, family, source)
		if err != nil {
			return false, err
		}
		if record, deleted := t.records[source]; deleted && record == nil {
			for _, previous := range rows {
				if _, removed := removedBySource[source][previous]; !removed {
					return false, nil
				}
			}
			continue
		}
		if len(rows) != 0 && len(staged) == 0 {
			// A live unchanged source cannot lose its only typed envelope. This
			// also rejects a partial remove without the complete replacement delta.
			return false, nil
		}
		for _, previous := range rows {
			if _, replaced := staged[previous]; replaced {
				continue
			}
			if _, removed := removedBySource[source][previous]; removed {
				continue
			}
			return false, nil
		}
	}
	// Independently complete derived, read-fact and sentinel sourced rows do
	// not participate in this record-built family. Their exact envelopes still
	// follow the ordinary source-observation admission path.
	return true, nil
}

// Every touched source still needs its own final chain proved. Only a change
// to canonical Parent or source presence can invalidate unchanged descendants.
type selectionParentProofScope struct {
	chains   []string
	subtrees []string
}

// Capture the old canonical Parent before sourcePlan.Publish replaces it. The
// complete, final declaration overlay is authoritative even if a source was
// staged several times. Missing prior coverage/rows, raw writes, restores and
// lifetime changes conservatively retain the full descendant proof.
func (t *transaction) prepareSelectionParentProof(ctx context.Context, c queryer, current driver.Meta, parentReady bool) (selectionParentProofScope, error) {
	scope := selectionParentProofScope{chains: t.selectionParentProofSeeds(t.derived)}
	subtrees := make(map[string]bool)
	var comparable []string
	for id, record := range t.records {
		identity, known := t.sourceIdentities[id]
		if !(known && identity.Kind == "bead") && !(record != nil && record.Kind == "bead") {
			continue
		}
		subtrees[id] = true
		if t.restore != nil || !parentReady || record == nil || record.Kind != "bead" {
			continue
		}
		if _, typed := t.sourceOverlay.Declarations[id]; typed {
			comparable = append(comparable, id)
		}
	}
	sort.Strings(comparable)
	for start := 0; start < len(comparable); start += selectionSourceHistoryBatch {
		end := min(start+selectionSourceHistoryBatch, len(comparable))
		args := make([]any, 0, 2+end-start)
		args = append(args, current.Incarnation, revKey(0))
		for _, id := range comparable[start:end] {
			args = append(args, id)
		}
		rows, err := c.QueryContext(ctx, `SELECT child_key,parent_key FROM hierarchy_source_current
			WHERE incarnation=? AND branch=? AND kind='bead' AND child_key IN (`+strings.TrimSuffix(strings.Repeat("?,", end-start), ",")+`)`, args...)
		if err != nil {
			return selectionParentProofScope{}, err
		}
		for rows.Next() {
			var id string
			var parent sql.NullString
			if err := rows.Scan(&id, &parent); err != nil {
				_ = rows.Close()
				return selectionParentProofScope{}, err
			}
			if parent.String == t.sourceOverlay.Declarations[id].ParentKey {
				delete(subtrees, id)
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return selectionParentProofScope{}, err
		}
	}
	for id := range subtrees {
		scope.subtrees = append(scope.subtrees, id)
	}
	sort.Strings(scope.subtrees)
	return scope, nil
}

// Include changed bead sources and typed record-backed sources even when
// their Parent is unchanged; their own projection/closure may have changed.
func (t *transaction) selectionParentProofSeeds(derived map[string]driver.LockState) []string {
	seen := make(map[string]struct{}, len(t.records)+len(derived))
	for id, record := range t.records {
		if identity, ok := t.sourceIdentities[id]; ok && identity.Kind == "bead" {
			seen[id] = struct{}{}
			continue
		}
		if record != nil && record.Kind == "bead" {
			seen[id] = struct{}{}
		}
	}
	for _, state := range derived {
		if state.Derived == nil || state.Derived.Index == nil || state.Derived.Index.Source.Key.Kind != driver.LockRecord {
			continue
		}
		seen[keyID(state.Derived.Index.Source.Key)] = struct{}{}
	}
	seeds := make([]string, 0, len(seen))
	for id := range seen {
		seeds = append(seeds, id)
	}
	sort.Strings(seeds)
	return seeds
}

// A core fact family is rebuilt from bead records only. A live, known
// nonbead record can still own an independently complete fact envelope, so
// it must not change this family's coverage or be discarded by startup.
func (t *transaction) selectionRecordBuiltSource(ctx context.Context, c queryer, id string) (built, known bool, err error) {
	if record, staged := t.records[id]; staged {
		if record == nil {
			return false, false, nil
		}
		return record.Kind == "bead", true, nil
	}
	var kind string
	err = c.QueryRowContext(ctx, `SELECT o.kind FROM objects o JOIN records r ON r.key=o.key WHERE o.key=?`, id).Scan(&kind)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return kind == "bead", true, nil
}

// The caller compares row identities as a set. An ORDER BY row_key can make
// SQLite choose the row-key primary index and walk every source in the branch
// before applying source_key. Force the current-source index for this bounded
// touched-source lookup; canonical ancestor sources may return multiple rows.
const selectionSourceFactRowsSQL = `SELECT row_key FROM selection_fact_sets INDEXED BY selection_fact_sets_source
	WHERE incarnation=? AND branch=? AND row_table=? AND kind=? AND source_key=? AND end_revision IS NULL`

func querySelectionSourceFactRows(ctx context.Context, c queryer, incarnation string, family driver.SelectionProjectionFamily, source string) ([]string, error) {
	rows, err := c.QueryContext(ctx, selectionSourceFactRowsSQL, incarnation, revKey(0), string(family.Table), string(family.Kind), source)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type selectionFamilyDecision struct {
	family     driver.SelectionProjectionFamily
	interval   sqliteFamilyInterval
	priorReady bool
	complete   bool
	firstTyped bool
}

func (t *transaction) prepareSelectionFamilyCoverage(ctx context.Context, c *sql.Conn, current driver.Meta, incarnation string, derived map[string]driver.LockState) ([]selectionFamilyDecision, error) {
	previous := revKey(mustUint(current.Head))
	firstTyped, err := t.selectionNeverSourcedBefore(ctx, c, current)
	if err != nil {
		return nil, err
	}
	var decisions []selectionFamilyDecision
	for _, family := range sqliteSelectionFamilies() {
		interval, found, err := selectionFamilyInterval(ctx, c, incarnation, family, previous)
		if err != nil {
			return nil, err
		}
		priorReady := t.restore == nil && found && interval.state == "ready" && interval.through >= previous
		if !priorReady && !firstTyped {
			continue
		}
		complete, err := t.selectionFamilyCompleteForCommit(ctx, c, incarnation, family, derived)
		if err != nil {
			return nil, err
		}
		decisions = append(decisions, selectionFamilyDecision{family: family, interval: interval, priorReady: priorReady, complete: complete, firstTyped: firstTyped})
	}
	return decisions, nil
}

func publishSelectionFamilyCoverage(ctx context.Context, c *sql.Conn, incarnation, previous, next string, decisions []selectionFamilyDecision, parentScope selectionParentProofScope) error {
	for _, decision := range decisions {
		family, interval, priorReady, complete, firstTyped := decision.family, decision.interval, decision.priorReady, decision.complete, decision.firstTyped
		if complete && family.Kind == driver.SelectionAncestorFact {
			valid, err := verifySQLiteSelectionParentChainsScoped(ctx, c, incarnation, next, parentScope)
			if err != nil {
				return err
			}
			complete = valid
		}
		if priorReady && complete {
			result, err := c.ExecContext(ctx, `UPDATE selection_family_coverage SET certified_through=?
				WHERE incarnation=? AND branch=? AND family_table=? AND family_kind=?
				AND built_from=? AND certified_through=? AND state='ready'`,
				next, incarnation, revKey(0), string(family.Table), string(family.Kind), interval.first, previous)
			if err != nil {
				return err
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				return fmt.Errorf("%w: SQLite selection coverage carry changed", driver.ErrSelectionUnavailable)
			}
			continue
		}
		state := "unbuilt"
		if complete && firstTyped {
			state = "ready"
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO selection_family_coverage
			(incarnation,branch,family_table,family_kind,format,built_from,certified_through,state)
			VALUES(?,?,?,?,?,?,?,?)`, incarnation, revKey(0), string(family.Table), string(family.Kind), family.Version, next, next, state); err != nil {
			return err
		}
	}
	return nil
}

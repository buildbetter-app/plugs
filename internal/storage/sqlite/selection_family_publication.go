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
func (t *transaction) selectionNeverSourcedBefore(ctx context.Context, c *sql.Conn, current driver.Meta) (bool, error) {
	// An imported source starts a new incarnation, not a virgin authored
	// lifetime. Its complete final image must go through explicit backfill;
	// the restore commit cannot certify source history it did not publish.
	if t.restore != nil {
		return false, nil
	}
	// The old census materialized every bead key before discovering that an
	// ordinary write had left an older bead untouched.  That made each commit
	// proportional to the whole store.  First ask the indexed anti-join for an
	// unchanged bead and stop at the first witness; only the exceptional case
	// where every existing bead is part of this transaction needs the complete
	// historical check below.
	changed := make([]string, 0, len(t.records))
	for id := range t.records {
		changed = append(changed, id)
	}
	if len(changed) == 0 {
		return false, nil
	}
	// Keep the fast path below within SQLite's portable host-parameter budget.
	// A transaction this large is already affected-sized work; retain the
	// original exact census rather than constructing an unbounded IN list.
	if len(changed) > 800 {
		return t.selectionNeverSourcedBeforeFull(ctx, c, current)
	}
	placeholders := make([]string, len(changed))
	args := make([]any, len(changed))
	for i, id := range changed {
		placeholders[i], args[i] = "?", id
	}
	var untouched int
	if err := c.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM objects WHERE kind='bead' AND key NOT IN (`+strings.Join(placeholders, ",")+`))`, args...).Scan(&untouched); err != nil {
		return false, err
	}
	if untouched != 0 {
		return false, nil
	}
	rows, err := c.QueryContext(ctx, `SELECT key FROM objects WHERE kind='bead' AND key IN (`+strings.Join(placeholders, ",")+`)`, args...)
	if err != nil {
		return false, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return false, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	seen := false
	for _, id := range ids {
		record, changed := t.records[id]
		if !changed || record == nil || record.Kind != "bead" {
			return false, nil
		}
		seen = true
		var old int
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM versions WHERE key=? AND rev<=?)`, id, revKey(mustUint(current.Head))).Scan(&old); err != nil {
			return false, err
		}
		if old != 0 {
			return false, nil
		}
	}
	return seen, nil
}

func (t *transaction) selectionNeverSourcedBeforeFull(ctx context.Context, c *sql.Conn, current driver.Meta) (bool, error) {
	rows, err := c.QueryContext(ctx, `SELECT key FROM objects WHERE kind='bead'`)
	if err != nil {
		return false, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return false, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	seen := false
	for _, id := range ids {
		record, changed := t.records[id]
		if !changed || record == nil || record.Kind != "bead" {
			return false, nil
		}
		seen = true
		var old int
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM versions WHERE key=? AND rev<=?)`, id, revKey(mustUint(current.Head))).Scan(&old); err != nil {
			return false, err
		}
		if old != 0 {
			return false, nil
		}
	}
	return seen, nil
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
	// source/depth.  The other typed families have one row per source, but an
	// ancestor source legitimately owns several rows.  Keep the complete set
	// of staged row keys so the source census and replacement checks do not
	// collapse a valid multi-depth closure into a duplicate-source refusal.
	bySource := map[string]map[string]struct{}{}
	for id, state := range derived {
		if state.Key.Derived == nil {
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
		if oldKind == string(family.Kind) && !newFamily {
			removed, changed := t.records[oldSource]
			if !changed || removed != nil {
				return false, nil
			}
		}
		if !newFamily {
			continue
		}
		source := state.Derived.Index.Source
		if source.Key.Kind != driver.LockRecord || source.Key.Ref == nil {
			return false, nil
		}
		sourceID := keyID(source.Key)
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
	}
	for source, staged := range bySource {
		if _, changed := t.records[source]; changed {
			continue // source publication closes its previous fact sets
		}
		rows, err := querySelectionSourceFactRows(ctx, c, incarnation, family, source)
		if err != nil {
			return false, err
		}
		for _, previous := range rows {
			replaced := false
			for row := range staged {
				if previous == row {
					replaced = true
					break
				}
			}
			if !replaced {
				if _, replaced = derived[previous]; !replaced {
					return false, nil
				}
			}
		}
	}
	// Independently complete derived, read-fact and sentinel sourced rows do
	// not participate in this record-built family. Their exact envelopes still
	// follow the ordinary source-observation admission path.
	return true, nil
}

// selectionParentProofSeeds returns the source identities whose committed
// parent/ancestor component can change in this transaction.  The recursive
// SQL proof expands each seed through the current Parent index, so an ancestor
// move also checks every existing descendant whose chain depends on it.  A
// source-only update remains local to its own chain.
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

func querySelectionSourceFactRows(ctx context.Context, c queryer, incarnation string, family driver.SelectionProjectionFamily, source string) ([]string, error) {
	// queryer has QueryRowContext only. The bounded count/identity query is
	// sufficient because a complete family has at most one row per source.
	var count int
	var row sql.NullString
	err := c.QueryRowContext(ctx, `SELECT COUNT(*),MIN(row_key) FROM selection_fact_sets
		WHERE incarnation=? AND branch=? AND row_table=? AND kind=? AND source_key=? AND end_revision IS NULL`,
		incarnation, revKey(0), string(family.Table), string(family.Kind), source).Scan(&count, &row)
	if err != nil {
		return nil, err
	}
	if count > 1 {
		return []string{"", ""}, nil
	}
	if count == 0 {
		return nil, nil
	}
	return []string{row.String}, nil
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

func publishSelectionFamilyCoverage(ctx context.Context, c *sql.Conn, incarnation, previous, next string, decisions []selectionFamilyDecision, parentSeeds []string) error {
	for _, decision := range decisions {
		family, interval, priorReady, complete, firstTyped := decision.family, decision.interval, decision.priorReady, decision.complete, decision.firstTyped
		if complete && family.Kind == driver.SelectionAncestorFact {
			valid, err := verifySQLiteSelectionParentChainsAffected(ctx, c, incarnation, next, parentSeeds)
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

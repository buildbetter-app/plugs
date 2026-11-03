package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// reboundSelectionSources returns the commit-local annotation overlay.  It is
// deliberately detached from t.derived: the SQL transaction may fail after
// this point, and an aborted publication must not leave a future H+1 source
// token in the still-open transaction's in-memory annotations.
func (t *transaction) reboundSelectionSources(revision model.Revision) map[string]driver.LockState {
	derived := make(map[string]driver.LockState, len(t.derived))
	for id, state := range t.derived {
		if state.Derived == nil || state.Derived.Index == nil || !t.selectionSourceChanged(state.Derived.Index.Source.Key) {
			derived[id] = state
			continue
		}
		copy := state.Derived.Clone()
		copy.Index.Source.Revision = revision
		state.Derived = &copy
		derived[id] = state
	}
	return derived
}

// selectionCompleteForCommit is the inductive counting-family check. A new
// store has no certified interval; an explicit complete empty startup or a
// virgin first typed publication establishes one. Every later bead mutation
// must carry a typed sort-key delta in the same transaction. An untyped write
// invalidates coverage rather than certifying a partial index.
func (t *transaction) selectionCompleteForCommit(ctx context.Context, c queryer, incarnation string, derived map[string]driver.LockState) (bool, error) {
	if t.restore != nil && incarnation != t.restore.Incarnation {
		return false, nil
	}
	bySource := make(map[string]driver.LockState)
	for _, state := range derived {
		if state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedSortKey || !state.Present || state.Derived == nil || state.Derived.Index == nil || state.Derived.Index.Bead == nil {
			continue
		}
		key := keyID(state.Derived.Index.Source.Key)
		if _, duplicate := bySource[key]; duplicate {
			return false, fmt.Errorf("%w: duplicate SQLite selection source", driver.ErrUnsupported)
		}
		// A staged new sort-key row must not coexist with another committed
		// projection for the same bead. That old row is removed from the
		// same-token overlay only when its own logical key is also staged.
		var previous string
		err := c.QueryRowContext(ctx, `SELECT derived_key FROM selection_bead_history
		 WHERE incarnation=? AND branch=? AND record_key=? AND end_revision IS NULL`, incarnation, revKey(0), key).Scan(&previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		if err == nil && previous != keyID(state.Key) {
			if _, replaced := derived[previous]; !replaced {
				return false, nil
			}
		}
		bySource[key] = state
	}
	for id, record := range t.records {
		kind := ""
		if record != nil {
			kind = record.Kind
		} else {
			err := c.QueryRowContext(ctx, `SELECT kind FROM objects WHERE key=?`, id).Scan(&kind)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				return false, err
			}
		}
		if kind != "bead" {
			continue
		}
		if record != nil {
			state, ok := bySource[id]
			if !ok || !driver.SelectionSameRef(state.Derived.Index.Bead.Ref, record.Ref) {
				return false, nil
			}
			continue
		}
		var derivedKey string
		err := c.QueryRowContext(ctx, `SELECT derived_key FROM selection_bead_history
		 WHERE incarnation=? AND branch=? AND record_key=? AND end_revision IS NULL`, incarnation, revKey(0), id).Scan(&derivedKey)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		state, ok := derived[derivedKey]
		if !ok || state.Present {
			return false, nil
		}
	}
	for id, state := range derived {
		if state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedSortKey {
			continue
		}
		var previous string
		err := c.QueryRowContext(ctx, `SELECT record_key FROM selection_bead_history
		 WHERE incarnation=? AND branch=? AND derived_key=? AND end_revision IS NULL`, incarnation, revKey(0), id).Scan(&previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		if err == nil {
			if state.Present && state.Derived != nil && state.Derived.Index != nil && state.Derived.Index.Bead != nil && keyID(state.Derived.Index.Source.Key) == previous {
				continue
			}
			if record, removed := t.records[previous]; !removed || record != nil {
				return false, nil
			}
		} else if state.Present && (state.Derived == nil || state.Derived.Index == nil) {
			// An untyped sort-key replacement can shadow a typed logical row.
			return false, nil
		}
	}
	return true, nil
}

func (t *transaction) publishSelection(ctx context.Context, c *sql.Conn, current driver.Meta, revision model.Revision, derived map[string]driver.LockState) error {
	next := revKey(mustUint(revision))
	incarnation := current.Incarnation
	if t.restore != nil {
		incarnation = t.restore.Incarnation
	}
	var err error
	familyDecisions, err := t.prepareSelectionFamilyCoverage(ctx, c, current, incarnation, derived)
	if err != nil {
		return err
	}
	changedSources := make(map[string]bool, len(t.records)+len(t.derived)+len(t.bumps))
	for id := range t.records {
		changedSources[id] = true
	}
	for id := range derived {
		changedSources[id] = true
	}
	for id := range t.bumps {
		changedSources[id] = true
	}
	for id := range changedSources {
		// An annotated fact set sourced from a changed record/row/guard is
		// invalid until its complete replacement is published in this commit.
		if _, err = c.ExecContext(ctx, `UPDATE selection_fact_sets SET end_revision=?
		 WHERE incarnation=? AND branch=? AND source_key=? AND end_revision IS NULL`, next, incarnation, revKey(0), id); err != nil {
			return err
		}
	}
	// Persist the source observation used by every typed fact envelope. This is
	// deliberately a thin digest/revision row: selectors must not reconstruct
	// source bodies merely to prove that a fact envelope is still bound.
	for id, record := range t.records {
		if _, err = c.ExecContext(ctx, `UPDATE selection_source_history SET end_revision=?
			WHERE incarnation=? AND branch=? AND source_key=? AND end_revision IS NULL`, next, incarnation, revKey(0), id); err != nil {
			return err
		}
		digest := ""
		present := 0
		if record != nil {
			present = 1
			digest = driver.SelectionDigest(record.Body)
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO selection_source_history(
			incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest)
			VALUES(?,?,?,?,?,?,?)`, incarnation, revKey(0), id, next, next, present, digest); err != nil {
			return err
		}
	}
	for id, state := range t.derived {
		if state.Key.Derived == nil {
			continue
		}
		if _, err = c.ExecContext(ctx, `UPDATE selection_source_history SET end_revision=?
			WHERE incarnation=? AND branch=? AND source_key=? AND end_revision IS NULL`, next, incarnation, revKey(0), id); err != nil {
			return err
		}
		digest := ""
		present := 0
		if state.Present && state.Derived != nil {
			present = 1
			digest = driver.SelectionDigest(state.Derived.Value)
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO selection_source_history(
			incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest)
			VALUES(?,?,?,?,?,?,?)`, incarnation, revKey(0), id, next, next, present, digest); err != nil {
			return err
		}
	}
	// Close every changed logical row before inserting any new physical image.
	// This handles swaps and prevents a stale typed row surviving an untyped
	// replacement or delete. The key_history and these intervals share Commit.
	for id := range t.derived {
		if _, err = c.ExecContext(ctx, `UPDATE selection_bead_history SET end_revision=?
		 WHERE incarnation=? AND branch=? AND derived_key=? AND end_revision IS NULL`, next, incarnation, revKey(0), id); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `UPDATE selection_fact_sets SET end_revision=?
		 WHERE incarnation=? AND branch=? AND row_key=? AND end_revision IS NULL`, next, incarnation, revKey(0), id); err != nil {
			return err
		}
	}
	for id, state := range derived {
		if !state.Present || state.Derived == nil || state.Derived.Index == nil {
			continue
		}
		index := state.Derived.Index
		if index.Bead != nil {
			b := index.Bead
			var project, parent any
			var status any = b.Status
			var title any = b.Title
			var template any = b.Template
			var priority any = b.Priority
			for _, absent := range b.Absent {
				switch absent {
				case driver.SelectionStatus:
					status = nil
				case driver.SelectionTitle:
					title = nil
				case driver.SelectionTemplate:
					template = nil
				case driver.SelectionPriority:
					priority = nil
				}
			}
			if b.Project != nil {
				project = *b.Project
			}
			if b.Parent != nil {
				parent = keyID(driver.LockKey{Kind: driver.LockRecord, Ref: b.Parent})
			}
			_, err = c.ExecContext(ctx, `INSERT INTO selection_bead_history(
			 incarnation,branch,derived_key,record_key,start_revision,bead_id,bead_type,project,status,title,template,parent_key,priority,membership_leaf,revision_text,source_revision,source_digest)
				 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, incarnation, revKey(0), id, keyID(index.Source.Key), next, b.Ref.ID, b.Type, project, status, title, template, parent, priority, b.MembershipLeaf, index.Source.Revision.String(), revKey(mustUint(index.Source.Revision)), index.Source.Digest)
			if err != nil {
				return err
			}
			continue
		}
		f := index.Facts
		present := 0
		if index.Source.Present {
			present = 1
		}
		_, err = c.ExecContext(ctx, `INSERT INTO selection_fact_sets(
		 incarnation,branch,row_key,row_table,kind,start_revision,source_key,source_revision,source_present,source_digest)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`, incarnation, revKey(0), id, string(index.Row.Table), string(f.Kind), next, keyID(index.Source.Key), revKey(mustUint(index.Source.Revision)), present, index.Source.Digest)
		if err != nil {
			return err
		}
		for ordinal, fact := range f.Rows {
			var ref, target, link any
			if fact.Ref != nil {
				ref = keyID(driver.LockKey{Kind: driver.LockRecord, Ref: fact.Ref})
			}
			if fact.Target != nil {
				target = keyID(driver.LockKey{Kind: driver.LockRecord, Ref: fact.Target})
			}
			if fact.Link != nil {
				target = keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &fact.Link.To})
				link = fact.Link.Kind
			}
			active := 0
			if fact.Active {
				active = 1
			}
			var scopeKind, projectName any
			if fact.Kind == driver.SelectionScopedGrantFact {
				scopeKind = string(fact.ScopeKind)
				if fact.ProjectName != nil {
					projectName = *fact.ProjectName
				}
			}
			_, err = c.ExecContext(ctx, `INSERT INTO selection_facts(
			 incarnation,branch,row_key,start_revision,ordinal,kind,ref_key,target_key,link_kind,subject,scope,role,scope_kind,project_name,name,value,active,depth)
			 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, incarnation, revKey(0), id, next, ordinal, string(fact.Kind), ref, target, link, fact.Subject, fact.Scope, fact.Role, scopeKind, projectName, fact.Name, fact.Value, active, int64(fact.Depth))
			if err != nil {
				return err
			}
		}
	}
	// Coverage is published after all rows in this same fenced SQL transaction.
	// Each family retains its prior historical interval if this commit cannot
	// prove a complete delta, and new typed-first readiness starts only here.
	return publishSelectionFamilyCoverage(ctx, c, incarnation, revKey(mustUint(current.Head)), next, familyDecisions)
}

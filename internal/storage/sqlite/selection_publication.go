package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

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

// A ready sort row has its own untyped keyspace. It can coexist with typed
// selection rows only after the physical lookup proves this exact key has no
// typed predecessor. The key uses the same ID boundary as the Writer codec.
func disjointReadySortRow(state driver.LockState) bool {
	if state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedSortKey ||
		state.Derived == nil || state.Derived.Index != nil {
		return false
	}
	id, ready := strings.CutPrefix(state.Key.Derived.Key, "ready/")
	if !ready {
		return false
	}
	_, err := driver.NewBeadSentinel(id)
	return err == nil
}

// selectionCompleteForCommit is the inductive counting-family check. A new
// store has no certified interval; an explicit complete empty startup or a
// virgin first typed publication establishes one. Every later bead mutation
// must carry a typed sort-key delta in the same transaction. An untyped write
// invalidates coverage rather than certifying a partial index.
func (t *transaction) selectionCompleteForCommit(ctx context.Context, c queryer, incarnation string, derived map[string]driver.LockState) (complete bool, err error) {
	if t.restore != nil && incarnation != t.restore.Incarnation {
		return false, nil
	}
	bySource := make(map[string]driver.LockState)
	// A first typed publication has no current sort rows. Probe those row
	// identities in bounded batches, then retain the scalar lookup whenever a
	// batch contains a hit. This mirrors the fact-family miss proof below while
	// preserving the old per-row lookup and error order for existing images.
	keys := make([]string, 0, len(derived))
	for id, state := range derived {
		if state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedSortKey || !state.Present || state.Derived == nil || state.Derived.Index == nil || state.Derived.Index.Bead == nil {
			continue
		}
		keys = append(keys, id)
	}
	conn, _ := c.(*sql.Conn)
	var recordStatements [2]*sql.Stmt
	defer func() {
		for _, statement := range recordStatements {
			if statement != nil {
				if closeErr := statement.Close(); closeErr != nil && err == nil {
					complete, err = false, closeErr
				}
			}
		}
	}()
	absent := false
	for position, id := range keys {
		state := derived[id]
		if conn != nil && position%selectionFamilyOldRowBatch == 0 {
			absent = false
			end := min(position+selectionFamilyOldRowBatch, len(keys))
			count := end - position
			if count > 1 {
				arm := 0
				if count != selectionFamilyOldRowBatch {
					arm = 1
				}
				if recordStatements[arm] == nil {
					recordStatements[arm], err = conn.PrepareContext(ctx, selectionBeadOldRowPresentSQL(count, "record_key"))
					if err != nil {
						return false, err
					}
				}
				args := make([]any, 0, 2+count)
				args = append(args, incarnation, revKey(0))
				for _, key := range keys[position:end] {
					state := derived[key]
					args = append(args, keyID(state.Derived.Index.Source.Key))
				}
				var present int
				lookupErr := recordStatements[arm].QueryRowContext(ctx, args...).Scan(&present)
				if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
					return false, lookupErr
				}
				absent = errors.Is(lookupErr, sql.ErrNoRows)
			}
		}
		key := keyID(state.Derived.Index.Source.Key)
		if _, duplicate := bySource[key]; duplicate {
			return false, fmt.Errorf("%w: duplicate SQLite selection source", driver.ErrUnsupported)
		}
		// A staged new sort-key row must not coexist with another committed
		// projection for the same bead. That old row is removed from the
		// same-token overlay only when its own logical key is also staged.
		var previous string
		var lookupErr error
		if absent {
			if lookupErr = ctx.Err(); lookupErr != nil {
				return false, lookupErr
			}
		} else {
			lookupErr = c.QueryRowContext(ctx, `SELECT derived_key FROM selection_bead_history
				 WHERE incarnation=? AND branch=? AND record_key=? AND end_revision IS NULL`, incarnation, revKey(0), key).Scan(&previous)
			if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
				return false, lookupErr
			}
		}
		if !absent && lookupErr == nil && previous != keyID(state.Key) {
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
	derivedKeys := make([]string, 0, len(derived))
	for id, state := range derived {
		if state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedSortKey {
			continue
		}
		derivedKeys = append(derivedKeys, id)
	}
	var derivedStatements [2]*sql.Stmt
	defer func() {
		for _, statement := range derivedStatements {
			if statement != nil {
				if closeErr := statement.Close(); closeErr != nil && err == nil {
					complete, err = false, closeErr
				}
			}
		}
	}()
	absent = false
	for position, id := range derivedKeys {
		state := derived[id]
		if conn != nil && position%selectionFamilyOldRowBatch == 0 {
			absent = false
			end := min(position+selectionFamilyOldRowBatch, len(derivedKeys))
			count := end - position
			if count > 1 {
				arm := 0
				if count != selectionFamilyOldRowBatch {
					arm = 1
				}
				if derivedStatements[arm] == nil {
					derivedStatements[arm], err = conn.PrepareContext(ctx, selectionBeadOldRowPresentSQL(count, "derived_key"))
					if err != nil {
						return false, err
					}
				}
				args := make([]any, 0, 2+count)
				args = append(args, incarnation, revKey(0))
				for _, key := range derivedKeys[position:end] {
					args = append(args, key)
				}
				var present int
				lookupErr := derivedStatements[arm].QueryRowContext(ctx, args...).Scan(&present)
				if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
					return false, lookupErr
				}
				absent = errors.Is(lookupErr, sql.ErrNoRows)
			}
		}
		var previous string
		var lookupErr error
		if absent {
			if lookupErr = ctx.Err(); lookupErr != nil {
				return false, lookupErr
			}
		} else {
			lookupErr = c.QueryRowContext(ctx, `SELECT record_key FROM selection_bead_history
				 WHERE incarnation=? AND branch=? AND derived_key=? AND end_revision IS NULL`, incarnation, revKey(0), id).Scan(&previous)
			if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
				return false, lookupErr
			}
		}
		if absent {
			if state.Present && (state.Derived == nil || state.Derived.Index == nil) {
				if !disjointReadySortRow(state) {
					return false, nil
				}
			}
			continue
		}
		if lookupErr == nil {
			if state.Present && (state.Derived == nil || state.Derived.Index == nil) {
				// An untyped replacement cannot inherit this exact typed row's
				// coverage, even when its source is deleted in this token.
				return false, nil
			}
			if state.Present && state.Derived != nil && state.Derived.Index != nil && state.Derived.Index.Bead != nil && keyID(state.Derived.Index.Source.Key) == previous {
				continue
			}
			if record, removed := t.records[previous]; !removed || record != nil {
				return false, nil
			}
		} else if state.Present && (state.Derived == nil || state.Derived.Index == nil) {
			if !disjointReadySortRow(state) {
				return false, nil
			}
		}
	}
	return true, nil
}

func (t *transaction) publishSelection(ctx context.Context, c *sql.Conn, current driver.Meta, revision model.Revision, derived map[string]driver.LockState, readyProof *readyOrderCommitProof) error {
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
	beadLifetime := t.restore != nil
	if !beadLifetime {
		for id, record := range t.records {
			if record != nil && record.Kind == "bead" {
				beadLifetime = true
				break
			}
			if identity, ok := t.sourceIdentities[id]; ok && identity.Kind == "bead" {
				beadLifetime = true
				break
			}
		}
	}
	if beadLifetime {
		if _, err = c.ExecContext(ctx, `INSERT INTO selection_virgin_state(incarnation,virgin) VALUES(?,0)
			ON CONFLICT(incarnation) DO UPDATE SET virgin=0`, incarnation); err != nil {
			return err
		}
	}
	// The first authored publication may have no prior projection rows to
	// close. Prove both physical arms empty on this same fenced connection;
	// an imported or legacy image must retain the ordinary close sequence.
	emptyPriorProjections := false
	if current.Head.IsZero() && t.restore == nil {
		if err = c.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM selection_bead_history)
			AND NOT EXISTS(SELECT 1 FROM selection_fact_sets)`).Scan(&emptyPriorProjections); err != nil {
			return err
		}
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
	if !emptyPriorProjections {
		if err = invalidateSelectionFactSets(ctx, c, incarnation, next, changedSources); err != nil {
			return err
		}
	}
	emptySourceHistory := readyProof != nil && readyProof.sourceHistoryEmpty
	if emptySourceHistory {
		// Earlier publication arms may run triggers on this connection. Carry
		// the virgin proof only while the thin history is still entirely empty.
		if err = c.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM selection_source_history)`).Scan(&emptySourceHistory); err != nil {
			return err
		}
	}
	// Publish the same thin record/derived observations in bounded batches.
	if err = t.publishSelectionSourceHistoryWithProof(ctx, c, incarnation, next, emptySourceHistory); err != nil {
		return err
	}
	// Close both physical arms before inserting any replacement image.
	if !emptyPriorProjections {
		if err = closeSelectionDerived(ctx, c, incarnation, next, t.derived); err != nil {
			return err
		}
	}
	if err = publishSelectionRows(ctx, c, incarnation, next, derived); err != nil {
		return err
	}
	// Coverage is published after all rows in this same fenced SQL transaction.
	// Each family retains its prior historical interval if this commit cannot
	// prove a complete delta, and new typed-first readiness starts only here.
	return publishSelectionFamilyCoverage(ctx, c, incarnation, revKey(mustUint(current.Head)), next, familyDecisions, t.selectionParentProofSeeds(derived))
}

func publishSelectionRows(ctx context.Context, c *sql.Conn, incarnation, next string, derived map[string]driver.LockState) (err error) {
	rows := newSelectionRowInsertBatch(c)
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	// Preserve every scalar statement boundary. Buffered multi-row execution
	// changes statement-sensitive triggers and immediate-FK error precedence.
	// Flushing at the original execution positions still reuses three handles.
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
			err = rows.AddBead(ctx, incarnation, revKey(0), id, keyID(index.Source.Key), next, b.Ref.ID, b.Type, project, status, title, template, parent, priority, b.MembershipLeaf, index.Source.Revision.String(), revKey(mustUint(index.Source.Revision)), index.Source.Digest)
			if err == nil {
				err = rows.Flush(ctx)
			}
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
		err = rows.AddFactSet(ctx, incarnation, revKey(0), id, string(index.Row.Table), string(f.Kind), next, keyID(index.Source.Key), revKey(mustUint(index.Source.Revision)), present, index.Source.Digest)
		if err == nil {
			err = rows.Flush(ctx)
		}
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
			err = rows.AddFact(ctx, incarnation, revKey(0), id, next, ordinal, string(fact.Kind), ref, target, link, fact.Subject, fact.Scope, fact.Role, scopeKind, projectName, fact.Name, fact.Value, active, int64(fact.Depth))
			if err == nil {
				err = rows.Flush(ctx)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

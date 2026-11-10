package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Keep each invalidation below SQLite's older 999-variable limit. All source
// closures share one fenced SQL transaction, so a batch boundary cannot expose
// a partial fact-set image.
const selectionFactInvalidationBatch = 128

// Each source-history insert binds seven values. A batch of 128 stays below
// SQLite's older 999-variable limit, and both statements remain in Commit's
// fenced transaction.
const selectionSourceHistoryBatch = 128

// Close both physical projection arms before inserting any replacement. The
// bounded list stays below SQLite's older variable limit; current-only indexes
// keep a touched logical row independent of its retained historical versions.
const selectionDerivedCloseBatch = 128

type selectionCloseExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func selectionBeadCloseSQL(count int) string {
	return `UPDATE selection_bead_history INDEXED BY selection_bead_current_derived SET end_revision=?
		WHERE incarnation=? AND branch=? AND end_revision IS NULL AND derived_key IN (` +
		strings.TrimSuffix(strings.Repeat("?,", count), ",") + `)`
}

func selectionFactCloseSQL(count int) string {
	return `UPDATE selection_fact_sets INDEXED BY selection_fact_sets_current_row SET end_revision=?
		WHERE incarnation=? AND branch=? AND end_revision IS NULL AND row_key IN (` +
		strings.TrimSuffix(strings.Repeat("?,", count), ",") + `)`
}

func closeSelectionDerived(ctx context.Context, c selectionCloseExecutor, incarnation, next string, derived map[string]driver.LockState) error {
	keys := make([]string, 0, len(derived))
	for id := range derived {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for start := 0; start < len(keys); start += selectionDerivedCloseBatch {
		end := min(start+selectionDerivedCloseBatch, len(keys))
		args := make([]any, 0, 3+end-start)
		args = append(args, next, incarnation, revKey(0))
		for _, id := range keys[start:end] {
			args = append(args, id)
		}
		if _, err := c.ExecContext(ctx, selectionBeadCloseSQL(end-start), args...); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, selectionFactCloseSQL(end-start), args...); err != nil {
			return err
		}
	}
	return nil
}

func selectionFactInvalidationSQL(count int) string {
	return `UPDATE selection_fact_sets INDEXED BY selection_fact_sets_source SET end_revision=?
		WHERE incarnation=? AND branch=? AND end_revision IS NULL AND source_key IN (` +
		strings.TrimSuffix(strings.Repeat("?,", count), ",") + `)`
}

func invalidateSelectionFactSets(ctx context.Context, c *sql.Conn, incarnation, next string, changedSources map[string]bool) error {
	if len(changedSources) == 0 {
		return nil
	}
	sources := make([]string, 0, len(changedSources))
	for source := range changedSources {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	for start := 0; start < len(sources); start += selectionFactInvalidationBatch {
		end := min(start+selectionFactInvalidationBatch, len(sources))
		args := make([]any, 0, 3+end-start)
		args = append(args, next, incarnation, revKey(0))
		for _, source := range sources[start:end] {
			args = append(args, source)
		}
		if _, err := c.ExecContext(ctx, selectionFactInvalidationSQL(end-start), args...); err != nil {
			return err
		}
	}
	return nil
}

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
			if state.Present && (state.Derived == nil || state.Derived.Index == nil) {
				// Even a ready/<id> row is a raw replacement when this logical
				// key already owns a typed bead projection.
				return false, nil
			}
			if state.Present && state.Derived != nil && state.Derived.Index != nil && state.Derived.Index.Bead != nil && keyID(state.Derived.Index.Source.Key) == previous {
				continue
			}
			if record, removed := t.records[previous]; !removed || record != nil {
				return false, nil
			}
		} else if state.Present && (state.Derived == nil || state.Derived.Index == nil) {
			// Main's ready-order row is a separate, unannotated sort-key
			// namespace. It cannot change bead selection coverage unless it
			// replaces an existing typed row, checked above. Keep every other
			// unannotated sort-key write unavailable.
			id, ready := strings.CutPrefix(state.Key.Derived.Key, "ready/")
			if !ready || id == "" {
				return false, nil
			}
			if _, err := driver.NewBeadSentinel(id); err != nil {
				return false, nil
			}
		}
	}
	return true, nil
}

func (t *transaction) publishSelection(ctx context.Context, c *sql.Conn, current driver.Meta, revision model.Revision, derived map[string]driver.LockState, parentScope selectionParentProofScope) (err error) {
	phase := commitPhaseTracker{trace: commitTraceFromContext(ctx)}
	phase.next(CommitPhaseSelectionPrepare, len(derived))
	defer func() { phase.finish(err) }()
	next := revKey(mustUint(revision))
	incarnation := current.Incarnation
	if t.restore != nil {
		incarnation = t.restore.Incarnation
	}
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
	phase.next(CommitPhaseSelectionSources, len(changedSources))
	if err = t.publishSelectionSources(ctx, c, incarnation, next, changedSources); err != nil {
		return err
	}
	// Close every changed logical row before inserting any new physical image.
	// This handles swaps and prevents a stale typed row surviving an untyped
	// replacement or delete. The key_history and these intervals share Commit.
	phase.next(CommitPhaseSelectionClose, len(t.derived))
	if err = closeSelectionDerived(ctx, c, incarnation, next, t.derived); err != nil {
		return err
	}
	phase.next(CommitPhaseSelectionInsert, len(derived))
	if err = publishSelectionRows(ctx, c, incarnation, next, derived); err != nil {
		return err
	}
	// Coverage is published after all rows in this same fenced SQL transaction.
	// Each family retains its prior historical interval if this commit cannot
	// prove a complete delta, and new typed-first readiness starts only here.
	phase.next(CommitPhaseSelectionCoverage, len(familyDecisions))
	return publishSelectionFamilyCoverage(ctx, c, incarnation, revKey(mustUint(current.Head)), next, familyDecisions, parentScope)
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

func (t *transaction) publishSelectionSources(ctx context.Context, c *sql.Conn, incarnation, next string, changedSources map[string]bool) (err error) {
	phase := commitPhaseTracker{trace: commitTraceFromContext(ctx)}
	phase.next(CommitPhaseSelectionInvalidate, len(changedSources))
	defer func() { phase.finish(err) }()
	// An annotated fact set sourced from a changed record/row/guard is invalid
	// until its complete replacement is published in this fenced transaction.
	if err = invalidateSelectionFactSets(ctx, c, incarnation, next, changedSources); err != nil {
		return err
	}
	phase.next(CommitPhaseSelectionHistory, len(t.records)+len(t.derived))
	// Persist thin source observations for typed fact envelopes without forcing
	// selectors to reconstruct record bodies or complete derived rows.
	return t.publishSelectionSourceHistory(ctx, c, incarnation, next)
}

type selectionSourceObservation struct {
	key     string
	present int
	digest  string
}

func selectionSourceHistoryCloseSQL(count int) string {
	return `UPDATE selection_source_history INDEXED BY selection_source_history_current SET end_revision=?
		WHERE incarnation=? AND branch=? AND end_revision IS NULL AND source_key IN (` +
		strings.TrimSuffix(strings.Repeat("?,", count), ",") + `)`
}

func selectionSourceHistoryInsertSQL(count int) string {
	return `INSERT INTO selection_source_history(
		incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest)
		VALUES ` + strings.TrimSuffix(strings.Repeat("(?,?,?,?,?,?,?),", count), ",")
}

func (t *transaction) publishSelectionSourceHistory(ctx context.Context, c *sql.Conn, incarnation, next string) error {
	observations := make([]selectionSourceObservation, 0, len(t.records)+len(t.derived))
	for id, record := range t.records {
		observation := selectionSourceObservation{key: id}
		if record != nil {
			observation.present = 1
			observation.digest = driver.SelectionDigest(record.Body)
		}
		observations = append(observations, observation)
	}
	for id, state := range t.derived {
		if state.Key.Derived == nil {
			continue
		}
		observation := selectionSourceObservation{key: id}
		if state.Present && state.Derived != nil {
			observation.present = 1
			observation.digest = driver.SelectionDigest(state.Derived.Value)
		}
		observations = append(observations, observation)
	}
	for start := 0; start < len(observations); start += selectionSourceHistoryBatch {
		end := min(start+selectionSourceHistoryBatch, len(observations))
		batch := observations[start:end]
		closeArgs := make([]any, 0, 3+len(batch))
		closeArgs = append(closeArgs, next, incarnation, revKey(0))
		insertArgs := make([]any, 0, 7*len(batch))
		for _, observation := range batch {
			closeArgs = append(closeArgs, observation.key)
			insertArgs = append(insertArgs, incarnation, revKey(0), observation.key,
				next, next, observation.present, observation.digest)
		}
		if _, err := c.ExecContext(ctx, selectionSourceHistoryCloseSQL(len(batch)), closeArgs...); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, selectionSourceHistoryInsertSQL(len(batch)), insertArgs...); err != nil {
			return err
		}
	}
	return nil
}

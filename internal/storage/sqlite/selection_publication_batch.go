package sqlite

import (
	"context"
	"database/sql"
	"sort"
	"strings"

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

type selectionPublicationExecutor interface {
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

func closeSelectionDerived(ctx context.Context, c selectionPublicationExecutor, incarnation, next string, derived map[string]driver.LockState) error {
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

func invalidateSelectionFactSets(ctx context.Context, c selectionPublicationExecutor, incarnation, next string, changedSources map[string]bool) error {
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

func (t *transaction) publishSelectionSourceHistory(ctx context.Context, c selectionPublicationExecutor, incarnation, next string) error {
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

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

type selectionSourceHistoryProber interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

const selectionDerivedSourceHistoryEmptySQL = `SELECT NOT EXISTS(
	SELECT 1 FROM selection_source_history INDEXED BY selection_source_history_current
	WHERE incarnation=? AND branch=? AND end_revision IS NULL
	AND source_key >= '3:' AND source_key < '4:')`

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

func (t *transaction) publishSelectionSourceHistory(ctx context.Context, c selectionPublicationExecutor, incarnation, next string) (err error) {
	return t.publishSelectionSourceHistoryWithProof(ctx, c, incarnation, next, false)
}

func (t *transaction) publishSelectionSourceHistoryWithProof(ctx context.Context, c selectionPublicationExecutor, incarnation, next string, sourceHistoryEmpty bool) (err error) {
	observations := make([]selectionSourceObservation, 0, len(t.records)+len(t.derived))
	for id, record := range t.records {
		observation := selectionSourceObservation{key: id}
		if record != nil {
			observation.present = 1
			observation.digest = driver.SelectionDigest(record.Body)
		}
		observations = append(observations, observation)
	}
	recordCount := len(observations)
	// Derived observations are already eager in key_history. Close only the
	// legacy thin intervals so retained pins keep their exact old witnesses.
	for id, state := range t.derived {
		if state.Key.Derived == nil {
			continue
		}
		observations = append(observations, selectionSourceObservation{key: id})
	}
	// Prepare at each arm's first execution on the held connection. Full
	// batches reuse their handles; exact-arity tails retain separate cleanup.
	var closeStatement, insertStatement keyPublicationStatement
	derivedProbeChecked := false
	for start := 0; start < len(observations); start += selectionSourceHistoryBatch {
		if sourceHistoryEmpty && start >= recordCount {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !derivedProbeChecked {
				// Record INSERT triggers ran after the initial virgin proof.
				// A native derived interval they created must retain the normal
				// close and its error precedence. Probe the current partial index
				// once, on the same executor connection, before omitting work.
				if probe, ok := c.(selectionSourceHistoryProber); ok {
					err = probe.QueryRowContext(ctx, selectionDerivedSourceHistoryEmptySQL, incarnation, revKey(0)).Scan(&sourceHistoryEmpty)
					if err != nil {
						return err
					}
				} else {
					sourceHistoryEmpty = false
				}
				derivedProbeChecked = true
			}
			if sourceHistoryEmpty {
				continue
			}
		}
		end := min(start+selectionSourceHistoryBatch, len(observations))
		batch := observations[start:end]
		closeArgs := make([]any, 0, 3+len(batch))
		closeArgs = append(closeArgs, next, incarnation, revKey(0))
		insertArgs := make([]any, 0, 7*len(batch))
		for offset, observation := range batch {
			closeArgs = append(closeArgs, observation.key)
			if start+offset < recordCount {
				insertArgs = append(insertArgs, incarnation, revKey(0), observation.key,
					next, next, observation.present, observation.digest)
			}
		}
		if closeStatement == nil || len(batch) != selectionSourceHistoryBatch {
			closeStatement, err = prepareKeyPublication(ctx, c, selectionSourceHistoryCloseSQL(len(batch)))
			if err != nil {
				return err
			}
			defer closeKeyPublicationStatement(closeStatement, &err)
		}
		if _, err := closeStatement.ExecContext(ctx, closeArgs...); err != nil {
			return err
		}
		insertCount := len(insertArgs) / 7
		if insertCount == 0 {
			continue
		}
		if insertStatement == nil || insertCount != selectionSourceHistoryBatch {
			insertStatement, err = prepareKeyPublication(ctx, c, selectionSourceHistoryInsertSQL(insertCount))
			if err != nil {
				return err
			}
			defer closeKeyPublicationStatement(insertStatement, &err)
		}
		if _, err := insertStatement.ExecContext(ctx, insertArgs...); err != nil {
			return err
		}
	}
	return nil
}

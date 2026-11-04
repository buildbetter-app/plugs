package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSelectionSourceInvalidationBatchesAndRollsBack(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	branch, next := revKey(0), revKey(2)
	changed := make(map[string]bool, selectionFactInvalidationBatch+1)
	sources := make([]string, 0, selectionFactInvalidationBatch+1)
	insert := func(row, source string, end any) {
		t.Helper()
		_, err := s.db.ExecContext(ctx, `INSERT INTO selection_fact_sets(
			incarnation,branch,row_key,row_table,kind,start_revision,end_revision,
			source_key,source_revision,source_present,source_digest)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`, meta.Incarnation, branch, row,
			string(driver.DerivedAncestorClosure), string(driver.SelectionAncestorFact),
			revKey(0), end, source, revKey(0), 1, "test-digest")
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i <= selectionFactInvalidationBatch; i++ {
		source := fmt.Sprintf("0:invalidation-source-%03d", i)
		sources = append(sources, source)
		changed[source] = true
		insert(fmt.Sprintf("3:invalidation-row-%03d", i), source, nil)
	}
	insert("3:invalidation-extra", sources[0], nil)
	insert("3:invalidation-untouched", "0:untouched-source", nil)
	insert("3:invalidation-old", sources[0], revKey(1))

	// Check the exact UPDATE the publication helper executes, including its
	// partial-index predicate and the largest bound parameter list.
	args := []any{next, meta.Incarnation, branch}
	for _, source := range sources[:selectionFactInvalidationBatch] {
		args = append(args, source)
	}
	plan, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+selectionFactInvalidationSQL(selectionFactInvalidationBatch), args...)
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for plan.Next() {
		var node, parent, unused int
		var detail string
		if err := plan.Scan(&node, &parent, &unused, &detail); err != nil {
			_ = plan.Close()
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := plan.Err(); err != nil {
		_ = plan.Close()
		t.Fatal(err)
	}
	_ = plan.Close()
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH selection_fact_sets USING INDEX selection_fact_sets_source") ||
		!strings.Contains(joined, "source_key=?") || strings.Contains(joined, "SCAN selection_fact_sets") {
		t.Fatalf("source invalidation lost source-key lookup: %s", joined)
	}

	check := func(wantActive, wantClosed int) {
		t.Helper()
		var active, closed, old int
		err := s.db.QueryRowContext(ctx, `SELECT SUM(end_revision IS NULL),
			SUM(end_revision=?),SUM(end_revision=?) FROM selection_fact_sets
			WHERE incarnation=? AND branch=?`, next, revKey(1), meta.Incarnation, branch).Scan(&active, &closed, &old)
		if err != nil || active != wantActive || closed != wantClosed || old != 1 {
			t.Fatalf("fact intervals active=%d closed=%d old=%d err=%v, want %d/%d/1", active, closed, old, err, wantActive, wantClosed)
		}
	}
	check(selectionFactInvalidationBatch+3, 0)
	abort := errors.New("abort after both invalidation batches")
	var work selectionCloseWork
	err = s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = selectionCloseWork{conn: c}
		if err := invalidateSelectionFactSets(ctx, &work, meta.Incarnation, next, changed); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) || work.statements != 2 || work.rows != 130 {
		t.Fatalf("invalidation abort: actual_exec=%d rows_affected=%d err=%v", work.statements, work.rows, err)
	}
	check(selectionFactInvalidationBatch+3, 0)
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	err = s.immediate(cancelled, nil, func(c *sql.Conn) error {
		work = selectionCloseWork{conn: c, cancel: cancel}
		return invalidateSelectionFactSets(cancelled, &work, meta.Incarnation, next, changed)
	})
	if !errors.Is(err, context.Canceled) || work.statements != 1 || work.rows != 129 {
		t.Fatalf("invalidation cancellation: actual_exec=%d rows_affected=%d err=%v", work.statements, work.rows, err)
	}
	check(selectionFactInvalidationBatch+3, 0)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = selectionCloseWork{conn: c}
		return invalidateSelectionFactSets(ctx, &work, meta.Incarnation, next, changed)
	}); err != nil {
		t.Fatal(err)
	}
	if work.statements != 2 || work.rows != 130 {
		t.Fatalf("invalidation commit: actual_exec=%d rows_affected=%d", work.statements, work.rows)
	}
	check(1, selectionFactInvalidationBatch+2)
	t.Logf("source invalidation actual_exec=%d rows_affected=%d executor_row_visits=UNMEASURED", work.statements, work.rows)
}

// The thin source observation is a temporal envelope, including tombstones
// and derived values. Both sides of a 128-row batch boundary must publish
// together, and an error after the second batch must restore every interval.
func TestSelectionSourceHistoryBatchesAndRollsBack(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	branch, previous, next := revKey(0), revKey(100), revKey(101)
	tx := &transaction{records: make(map[string]*driver.Record), derived: make(map[string]driver.LockState)}
	want := make(map[string]selectionSourceObservation)
	for i := 0; i <= selectionSourceHistoryBatch; i++ {
		id := fmt.Sprintf("0:history-source-%03d", i)
		observation := selectionSourceObservation{key: id}
		if i%7 != 0 {
			body := []byte(fmt.Sprintf(`{"source":%d}`, i))
			tx.records[id] = &driver.Record{Body: body}
			observation.present = 1
			observation.digest = driver.SelectionDigest(body)
		} else {
			tx.records[id] = nil
		}
		want[id] = observation
	}
	derivedKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "history-batch-derived"}
	derivedLock := driver.LockKey{Kind: driver.LockDerived, Derived: &derivedKey}
	derivedID := keyID(derivedLock)
	derivedValue := []byte(`{"derived":true}`)
	tx.derived[derivedID] = driver.LockState{Key: derivedLock, Present: true,
		Derived: &driver.DerivedRow{Value: derivedValue}}
	want[derivedID] = selectionSourceObservation{key: derivedID, present: 1,
		digest: driver.SelectionDigest(derivedValue)}
	deletedDerivedKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "history-batch-deleted"}
	deletedDerivedLock := driver.LockKey{Kind: driver.LockDerived, Derived: &deletedDerivedKey}
	deletedDerivedID := keyID(deletedDerivedLock)
	tx.derived[deletedDerivedID] = driver.LockState{Key: deletedDerivedLock}
	want[deletedDerivedID] = selectionSourceObservation{key: deletedDerivedID}
	untouched := "0:history-untouched"
	insertOld := func(id string) {
		t.Helper()
		_, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(
			incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest)
			VALUES(?,?,?,?,?,?,?)`, meta.Incarnation, branch, id, previous, previous, 1, "old-digest")
		if err != nil {
			t.Fatal(err)
		}
	}
	for id := range want {
		insertOld(id)
	}
	insertOld(untouched)
	// A deep closed history for one touched source must stay historical. The
	// current-row index lets the close statement ignore these older versions.
	historicalID := "0:history-source-000"
	for revision := uint64(0); revision < 100; revision++ {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(
			incarnation,branch,source_key,start_revision,end_revision,source_revision,source_present,source_digest)
			VALUES(?,?,?,?,?,?,?,?)`, meta.Incarnation, branch, historicalID,
			revKey(revision), revKey(revision+1), revKey(revision), 1, "historical-digest"); err != nil {
			t.Fatal(err)
		}
	}
	// This is the production UPDATE, with its largest batch of source keys.
	args := []any{next, meta.Incarnation, branch}
	for id := range want {
		if len(args) == 3+selectionSourceHistoryBatch {
			break
		}
		args = append(args, id)
	}
	plan, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+selectionSourceHistoryCloseSQL(selectionSourceHistoryBatch), args...)
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for plan.Next() {
		var node, parent, unused int
		var detail string
		if err := plan.Scan(&node, &parent, &unused, &detail); err != nil {
			_ = plan.Close()
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := plan.Err(); err != nil {
		_ = plan.Close()
		t.Fatal(err)
	}
	_ = plan.Close()
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH selection_source_history USING INDEX selection_source_history_current") ||
		!strings.Contains(joined, "source_key=?") || strings.Contains(joined, "SCAN selection_source_history") {
		t.Fatalf("source history close lost source-key lookup: %s", joined)
	}
	check := func(published bool) {
		t.Helper()
		for id, observation := range want {
			var end sql.NullString
			if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_source_history
				WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=?`,
				meta.Incarnation, branch, id, previous).Scan(&end); err != nil {
				t.Fatal(err)
			}
			if published != end.Valid || published && end.String != next {
				t.Fatalf("source %q old interval end=%+v published=%t", id, end, published)
			}
			var present int
			var digest, revision string
			err := s.db.QueryRowContext(ctx, `SELECT source_revision,source_present,source_digest
				FROM selection_source_history WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=?`,
				meta.Incarnation, branch, id, next).Scan(&revision, &present, &digest)
			if !published {
				if !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("source %q survived rollback: %v", id, err)
				}
				continue
			}
			if err != nil || revision != next || present != observation.present || digest != observation.digest {
				t.Fatalf("source %q new observation revision=%q present=%d digest=%q err=%v, want %+v",
					id, revision, present, digest, err, observation)
			}
		}
		var end sql.NullString
		if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_source_history
			WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=?`,
			meta.Incarnation, branch, untouched, previous).Scan(&end); err != nil || end.Valid {
			t.Fatalf("untouched source interval end=%+v err=%v", end, err)
		}
		var historicalCount int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_source_history
			WHERE incarnation=? AND branch=? AND source_key=? AND start_revision<?
			AND end_revision IS NOT NULL AND source_digest=?`,
			meta.Incarnation, branch, historicalID, previous, "historical-digest").Scan(&historicalCount); err != nil ||
			historicalCount != 100 {
			t.Fatalf("historical source interval count=%d err=%v", historicalCount, err)
		}
		for revision := uint64(0); revision < 100; revision++ {
			var historicalEnd string
			if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_source_history
				WHERE incarnation=? AND branch=? AND source_key=? AND start_revision=?`,
				meta.Incarnation, branch, historicalID, revKey(revision)).Scan(&historicalEnd); err != nil ||
				historicalEnd != revKey(revision+1) {
				t.Fatalf("historical source revision %d end=%q err=%v", revision, historicalEnd, err)
			}
		}
	}
	check(false)
	abort := errors.New("abort after source history batches")
	var work selectionCloseWork
	err = s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = selectionCloseWork{conn: c}
		if err := tx.publishSelectionSourceHistory(ctx, &work, meta.Incarnation, next); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) || work.statements != 4 || work.rows != 262 {
		t.Fatalf("source history abort: actual_exec=%d rows_affected=%d err=%v", work.statements, work.rows, err)
	}
	check(false)
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	err = s.immediate(cancelled, nil, func(c *sql.Conn) error {
		work = selectionCloseWork{conn: c, cancel: cancel}
		return tx.publishSelectionSourceHistory(cancelled, &work, meta.Incarnation, next)
	})
	if !errors.Is(err, context.Canceled) || work.statements != 1 || work.rows != 128 {
		t.Fatalf("source history cancellation: actual_exec=%d rows_affected=%d err=%v", work.statements, work.rows, err)
	}
	check(false)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = selectionCloseWork{conn: c}
		return tx.publishSelectionSourceHistory(ctx, &work, meta.Incarnation, next)
	}); err != nil {
		t.Fatal(err)
	}
	if work.statements != 4 || work.rows != 262 {
		t.Fatalf("source history commit: actual_exec=%d rows_affected=%d", work.statements, work.rows)
	}
	check(true)
	t.Logf("source history actual_exec=%d rows_affected=%d executor_row_visits=UNMEASURED", work.statements, work.rows)
}

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
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
	var work keyPublicationWork
	err = s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
		if err := tx.publishSelectionSourceHistory(ctx, &work, meta.Incarnation, next); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) || work.statements != 4 || work.rows != 262 {
		t.Fatalf("source history abort: actual_exec=%d rows_affected=%d err=%v", work.statements, work.rows, err)
	}
	checkKeyPublicationPrepared(t, work, 4)
	check(false)
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	err = s.immediate(cancelled, nil, func(c *sql.Conn) error {
		work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c, cancel: cancel}}
		return tx.publishSelectionSourceHistory(cancelled, &work, meta.Incarnation, next)
	})
	if !errors.Is(err, context.Canceled) || work.statements != 1 || work.rows != 128 {
		t.Fatalf("source history cancellation: actual_exec=%d rows_affected=%d err=%v", work.statements, work.rows, err)
	}
	checkKeyPublicationPrepared(t, work, 1)
	check(false)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
		return tx.publishSelectionSourceHistory(ctx, &work, meta.Incarnation, next)
	}); err != nil {
		t.Fatal(err)
	}
	if work.statements != 4 || work.rows != 262 {
		t.Fatalf("source history commit: actual_exec=%d rows_affected=%d", work.statements, work.rows)
	}
	checkKeyPublicationPrepared(t, work, 4)
	check(true)
	t.Logf("source history actual_exec=%d rows_affected=%d executor_row_visits=UNMEASURED", work.statements, work.rows)
}

type selectionSourceHistoryImage struct {
	incarnation, branch, key, start, revision, digest string
	end                                               sql.NullString
	present                                           int
}

func selectionSourceHistoryImages(t *testing.T, s *Store) map[[2]string]selectionSourceHistoryImage {
	t.Helper()
	rows, err := s.db.QueryContext(context.Background(), `SELECT incarnation,branch,source_key,
		start_revision,end_revision,source_revision,source_present,source_digest FROM selection_source_history`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	images := make(map[[2]string]selectionSourceHistoryImage)
	for rows.Next() {
		var image selectionSourceHistoryImage
		if err := rows.Scan(&image.incarnation, &image.branch, &image.key, &image.start,
			&image.end, &image.revision, &image.present, &image.digest); err != nil {
			t.Fatal(err)
		}
		images[[2]string{image.key, image.start}] = image
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return images
}

func TestSelectionSourceHistoryPreparedReuseAndErrors(t *testing.T) {
	prepareErr := errors.New("source history prepare failed")
	firstClose := errors.New("first error after actual native Close")
	laterClose := errors.New("later error after actual native Close")
	abort := errors.New("abort after all prepared source history writes")
	for _, test := range []struct {
		name                         string
		count, prepared, executions  int
		rows                         int64
		failPrepareAt                int
		closeErrors                  []error
		cancel, duplicate, lateAbort bool
		wantErr                      error
	}{
		{name: "empty"},
		{name: "tail_only", count: 127, prepared: 2, executions: 2, rows: 254},
		{name: "full", count: 128, prepared: 2, executions: 2, rows: 256},
		{name: "full_tail", count: 129, prepared: 4, executions: 4, rows: 258},
		{name: "reused_full", count: 256, prepared: 2, executions: 4, rows: 512},
		{name: "reused_full_tail", count: 257, prepared: 4, executions: 6, rows: 514},
		{name: "full_cleanup", count: 256, prepared: 2, executions: 4, rows: 512,
			closeErrors: []error{firstClose, laterClose}, wantErr: firstClose},
		{name: "tail_cleanup", count: 257, prepared: 4, executions: 6, rows: 514,
			closeErrors: []error{nil, firstClose, laterClose, laterClose}, wantErr: firstClose},
		{name: "prepare_first_close", count: 257, failPrepareAt: 1, wantErr: prepareErr},
		{name: "prepare_first_insert", count: 257, prepared: 1, executions: 1, rows: 128,
			failPrepareAt: 2, closeErrors: []error{firstClose}, wantErr: prepareErr},
		{name: "prepare_tail_close", count: 257, prepared: 2, executions: 4, rows: 512,
			failPrepareAt: 3, closeErrors: []error{firstClose, laterClose}, wantErr: prepareErr},
		{name: "prepare_tail_insert", count: 257, prepared: 3, executions: 5, rows: 513,
			failPrepareAt: 4, closeErrors: []error{firstClose, laterClose, laterClose}, wantErr: prepareErr},
		{name: "cancellation", count: 257, prepared: 1, executions: 1, rows: 128,
			cancel: true, closeErrors: []error{firstClose}, wantErr: context.Canceled},
		{name: "unique_tail", count: 257, prepared: 4, executions: 5, rows: 513,
			duplicate: true, closeErrors: []error{firstClose, laterClose, laterClose, laterClose}},
		{name: "late_abort", count: 257, prepared: 4, executions: 6, rows: 514,
			lateAbort: true, wantErr: abort},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			meta, err := s.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			branch, previous, next := revKey(0), revKey(1), revKey(2)
			tx := &transaction{records: make(map[string]*driver.Record), derived: make(map[string]driver.LockState)}
			observations := make(map[string]selectionSourceObservation)
			derivedID := ""
			for i := 0; i < test.count; i++ {
				id := fmt.Sprintf("0:prepared-history-%03d", i)
				body := []byte(fmt.Sprintf(`{"source":%d}`, i))
				observation := selectionSourceObservation{key: id}
				if i == test.count-1 {
					// Record observations precede this sole derived observation,
					// so the UNIQUE case fails in the tail after two full batches.
					row := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "prepared-history-derived"}
					key := driver.LockKey{Kind: driver.LockDerived, Derived: &row}
					id = keyID(key)
					derivedID = id
					tx.derived[id] = driver.LockState{Key: key, Present: true, Derived: &driver.DerivedRow{Value: body}}
					observation = selectionSourceObservation{key: id, present: 1, digest: driver.SelectionDigest(body)}
				} else if i%7 == 0 {
					tx.records[id] = nil
				} else {
					tx.records[id] = &driver.Record{Body: body}
					observation.present, observation.digest = 1, driver.SelectionDigest(body)
				}
				observations[id] = observation
			}
			seed := func(id, start string, end any) {
				t.Helper()
				if _, err := s.db.ExecContext(ctx, `INSERT INTO selection_source_history(
					incarnation,branch,source_key,start_revision,end_revision,source_revision,source_present,source_digest)
					VALUES(?,?,?,?,?,?,?,?)`, meta.Incarnation, branch, id, start, end, start, 1, "old-digest"); err != nil {
					t.Fatal(err)
				}
			}
			for id := range observations {
				seed(id, previous, nil)
			}
			seed("0:prepared-history-untouched", previous, nil)
			if test.duplicate {
				seed(derivedID, next, revKey(3))
			}
			before := selectionSourceHistoryImages(t, s)
			after := make(map[[2]string]selectionSourceHistoryImage, len(before)+test.count)
			for key, image := range before {
				if _, touched := observations[image.key]; touched && image.start == previous {
					image.end = sql.NullString{String: next, Valid: true}
				}
				after[key] = image
			}
			for id, observation := range observations {
				after[[2]string{id, next}] = selectionSourceHistoryImage{incarnation: meta.Incarnation,
					branch: branch, key: id, start: next, revision: next, present: observation.present, digest: observation.digest}
			}
			keyPublicationTriggerMarker(t, s)
			check := func(published bool) {
				t.Helper()
				want := before
				if published {
					want = after
				}
				if !reflect.DeepEqual(selectionSourceHistoryImages(t, s), want) {
					t.Fatal("prepared source history changed an exact interval/presence/digest image")
				}
				var dirty, ever int
				var generation string
				if err := s.db.QueryRowContext(ctx, `SELECT dirty,ever_source,generation FROM ready_order_fence
					WHERE singleton=1`).Scan(&dirty, &ever, &generation); err != nil {
					t.Fatal(err)
				}
				changed := published && test.count != 0
				if (dirty == 1) != changed || ever != 1 || (generation != "key-batch-before") != changed || len(keyPublicationJournal(t, s)) != 0 {
					t.Fatalf("source-history trigger dirty=%d ever_source=%d generation=%q changed=%t", dirty, ever, generation, changed)
				}
			}
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			var work keyPublicationWork
			err = s.immediate(runCtx, nil, func(c *sql.Conn) error {
				work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c},
					failPrepareAt: test.failPrepareAt, prepareErr: prepareErr, closeErrors: test.closeErrors}
				if test.cancel {
					work.cancel = cancel
				}
				if err := tx.publishSelectionSourceHistory(runCtx, &work, meta.Incarnation, next); err != nil {
					return err
				}
				if test.lateAbort {
					return abort
				}
				return nil
			})
			if test.duplicate {
				if err == nil || !strings.Contains(err.Error(), "UNIQUE constraint failed") || errors.Is(err, firstClose) || errors.Is(err, laterClose) {
					t.Fatalf("native UNIQUE error lost to cleanup: %v", err)
				}
			} else if !errors.Is(err, test.wantErr) {
				t.Fatalf("publication error=%v, want %v", err, test.wantErr)
			}
			checkKeyPublicationPrepared(t, work, test.prepared)
			if work.statements != test.executions || work.rows != test.rows || work.maxArgs > 7*selectionSourceHistoryBatch ||
				test.failPrepareAt != 0 && work.prepareAttempts != test.failPrepareAt {
				t.Fatalf("prepares=%d attempts=%d actual_exec=%d rows_affected=%d max_bindings=%d", work.prepared, work.prepareAttempts, work.statements, work.rows, work.maxArgs)
			}
			check(err == nil)
			if err != nil {
				if test.duplicate {
					if _, err := s.db.ExecContext(ctx, `DELETE FROM selection_source_history
						WHERE source_key=? AND start_revision=?`, derivedID, next); err != nil {
						t.Fatal(err)
					}
				}
				keyPublicationTriggerMarker(t, s)
				// Recover through the actual production *sql.Conn preparer.
				if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
					return tx.publishSelectionSourceHistory(ctx, c, meta.Incarnation, next)
				}); err != nil {
					t.Fatalf("native prepared recovery: %v", err)
				}
				check(true)
			}
		})
	}
}

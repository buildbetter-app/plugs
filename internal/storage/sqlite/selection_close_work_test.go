package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	modernsqlite "modernc.org/sqlite"
)

// Observe successful engine executions and their affected rows, rather than
// counting the input map. These counts do not represent executor row visits.
type selectionCloseWork struct {
	conn       *sql.Conn
	statements int
	rows       int64
	cancel     context.CancelFunc
}

func (w *selectionCloseWork) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := w.conn.ExecContext(ctx, query, args...)
	if err != nil {
		return result, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	w.statements++
	w.rows += n
	if w.cancel != nil {
		w.cancel()
	}
	return result, nil
}

func insertSelectionCloseImage(ctx context.Context, q selectionCloseExecutor, incarnation, row, record, start string, end any) error {
	_, err := q.ExecContext(ctx, `INSERT INTO selection_bead_history(
		incarnation,branch,derived_key,record_key,start_revision,end_revision,
		bead_id,bead_type,membership_leaf,revision_text,source_revision,source_digest)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, incarnation, revKey(0), row, record, start, end,
		record, "task", "close-work-leaf", start, start, "close-work-digest")
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO selection_fact_sets(
		incarnation,branch,row_key,row_table,kind,start_revision,end_revision,
		source_key,source_revision,source_present,source_digest)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`, incarnation, revKey(0), row,
		string(driver.DerivedAncestorClosure), string(driver.SelectionAncestorFact),
		start, end, record, start, 1, "close-work-digest")
	return err
}

func TestSelectionDerivedCloseBatchesRollbackAndCancellation(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta := getMeta(t, ctx, s)
	const count = selectionDerivedCloseBatch + 1
	derived := make(map[string]driver.LockState, count)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("3:close-row-%03d", i)
			derived[id] = driver.LockState{}
			if err := insertSelectionCloseImage(ctx, c, meta.Incarnation, id,
				fmt.Sprintf("0:close-source-%03d", i), revKey(1), nil); err != nil {
				return err
			}
		}
		return insertSelectionCloseImage(ctx, c, meta.Incarnation, "3:untouched", "0:untouched", revKey(1), nil)
	}); err != nil {
		t.Fatal(err)
	}
	check := func(closed int) {
		t.Helper()
		for _, table := range []string{"selection_bead_history", "selection_fact_sets"} {
			var active, changed int
			if err := s.db.QueryRowContext(ctx, `SELECT SUM(end_revision IS NULL),COALESCE(SUM(end_revision=?),0) FROM `+table,
				revKey(2)).Scan(&active, &changed); err != nil || changed != closed || active != count+1-closed {
				t.Fatalf("%s active=%d closed=%d err=%v", table, active, changed, err)
			}
		}
	}
	check(0)
	abort := errors.New("rollback after both close batches")
	var work selectionCloseWork
	err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = selectionCloseWork{conn: c}
		if err := closeSelectionDerived(ctx, &work, meta.Incarnation, revKey(2), derived); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) || work.statements != 4 || work.rows != 2*count {
		t.Fatalf("executed close work statements=%d rows=%d err=%v", work.statements, work.rows, err)
	}
	check(0)
	// Cancel after the first successful physical UPDATE. The enclosing real
	// transaction must roll that arm back as well as refuse the remaining arms.
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	err = s.immediate(cancelled, nil, func(c *sql.Conn) error {
		work = selectionCloseWork{conn: c, cancel: cancel}
		return closeSelectionDerived(cancelled, &work, meta.Incarnation, revKey(2), derived)
	})
	if err == nil || work.statements != 1 || work.rows != selectionDerivedCloseBatch {
		t.Fatalf("cancelled close work statements=%d rows=%d err=%v", work.statements, work.rows, err)
	}
	check(0)
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		work = selectionCloseWork{conn: c}
		return closeSelectionDerived(ctx, &work, meta.Incarnation, revKey(2), derived)
	}); err != nil {
		t.Fatal(err)
	}
	if work.statements != 4 || work.rows != 2*count {
		t.Fatalf("committed close work statements=%d rows=%d", work.statements, work.rows)
	}
	check(count)
	t.Logf("SQLite derived close actual_exec=%d actual_rows_affected=%d staged_keys=%d executor_row_visits=UNMEASURED", work.statements, work.rows, count)
}

func selectionCloseCacheHits(c *sql.Conn, reset bool) (int, error) {
	var hits int
	err := c.Raw(func(raw any) error {
		status, ok := raw.(modernsqlite.DBStatus)
		if !ok {
			return fmt.Errorf("SQLite connection does not expose cache status")
		}
		var err error
		hits, _, err = status.Status(modernsqlite.DBStatusCacheHit, reset)
		return err
	})
	return hits, err
}

func TestSelectionDerivedCloseExecutedPageWorkWithDeepHistory(t *testing.T) {
	for _, history := range []int{1000, 2000} {
		t.Run(fmt.Sprintf("history_%d", history), func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			meta := getMeta(t, ctx, s)
			const row, record = "3:deep-close", "0:deep-close"
			current, next := revKey(uint64(history)), revKey(uint64(history+1))
			if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				for i := 0; i < history; i++ {
					if err := insertSelectionCloseImage(ctx, c, meta.Incarnation, row, record,
						revKey(uint64(i)), revKey(uint64(i+1))); err != nil {
						return err
					}
				}
				return insertSelectionCloseImage(ctx, c, meta.Incarnation, row, record, current, nil)
			}); err != nil {
				t.Fatal(err)
			}
			args := []any{next, meta.Incarnation, revKey(0), row}
			for _, query := range []string{selectionBeadCloseSQL(1), selectionFactCloseSQL(1)} {
				plan, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
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
				err = plan.Err()
				_ = plan.Close()
				text := strings.Join(details, " | ")
				if err != nil || strings.Contains(text, "SCAN ") ||
					!strings.Contains(text, "SEARCH ") ||
					!(strings.Contains(text, "selection_bead_current_derived") || strings.Contains(text, "selection_fact_sets_current_row")) {
					t.Fatalf("production close plan %q err=%v", text, err)
				}
				t.Logf("history=%d production_close_plan=%q", history, text)
			}
			// Execute the prior key/history lookup and the production current-only
			// close on the same populated engine, rolling each back. Cache hits
			// measure actual page accesses, not row visits or output cardinality.
			oldSQL := `UPDATE selection_bead_history INDEXED BY sqlite_autoindex_selection_bead_history_1 SET end_revision=?
				WHERE incarnation=? AND branch=? AND derived_key=? AND end_revision IS NULL`
			measure := func(query string) int {
				t.Helper()
				var hits int
				abort := errors.New("rollback measured close")
				err := s.immediate(ctx, nil, func(c *sql.Conn) error {
					if _, err := selectionCloseCacheHits(c, true); err != nil {
						return err
					}
					work := selectionCloseWork{conn: c}
					if _, err := work.ExecContext(ctx, query, args...); err != nil {
						return err
					}
					var err error
					hits, err = selectionCloseCacheHits(c, false)
					if err != nil {
						return err
					}
					if work.statements != 1 || work.rows != 1 {
						t.Errorf("measured execution statements=%d rows=%d", work.statements, work.rows)
					}
					return abort
				})
				if !errors.Is(err, abort) {
					t.Fatal(err)
				}
				return hits
			}
			prior, bounded := measure(oldSQL), measure(selectionBeadCloseSQL(1))
			if bounded <= 0 || bounded > 256 || bounded >= prior {
				t.Fatalf("history=%d actual cache hits prior=%d current=%d; want current<=256 and smaller than prior", history, prior, bounded)
			}
			var historical, active int
			if err := s.db.QueryRowContext(ctx, `SELECT SUM(end_revision IS NOT NULL),SUM(end_revision IS NULL)
				FROM selection_bead_history WHERE derived_key=?`, row).Scan(&historical, &active); err != nil || historical != history || active != 1 {
				t.Fatalf("measured rollback lost intervals: historical=%d active=%d err=%v", historical, active, err)
			}
			t.Logf("history=%d prior_actual_page_cache_hits=%d current_actual_page_cache_hits=%d executor_row_visits=UNMEASURED", history, prior, bounded)
		})
	}
}

// This is a driver-cost control with real typed writes and full sorted locks,
// not the 100k service workload or a successful end-to-end latency claim. The
// complete Commit timer includes its fenced SQL COMMIT and driver cleanup;
// nested phase times are reported separately and are never added to it.
func TestSelectionCommitTotalCostAtSourcePopulations(t *testing.T) {
	for _, population := range []int{1000, 2000} {
		t.Run(fmt.Sprintf("sources_%d", population), func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			renewCtx, stop := context.WithCancel(ctx)
			t.Cleanup(stop)
			go func() {
				ticker := time.NewTicker(driver.LeaseRenewEvery)
				defer ticker.Stop()
				for {
					select {
					case <-renewCtx.Done():
						return
					case <-ticker.C:
						if err := lease.Renew(renewCtx); err != nil {
							return
						}
					}
				}
			}()
			refs := make([]model.Ref, population)
			projections := make([]driver.DerivedRowKey, population)
			extra := make([]driver.LockKey, population)
			for i := range refs {
				refs[i] = testRef(fmt.Sprintf("commit-cost-%04d", i))
				projections[i] = driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: refs[i].ID}
				extra[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &projections[i]}
			}
			seed, header := beginSourceTestWithLocks(t, s, lease, extra, true, refs...)
			projection := func(i int, revision model.Revision, body []byte, title string) driver.DerivedRow {
				value := []byte(`{"projection":true}`)
				return driver.DerivedRow{Key: projections[i].Key, Value: value, Index: &driver.SelectionIndex{
					Version: driver.SelectionIndexVersion, Row: projections[i], Incarnation: header.Incarnation,
					Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
					Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &refs[i]},
						Revision: revision, Present: true, Digest: driver.SelectionDigest(body)},
					Bead: &driver.SelectionBead{Ref: refs[i], Type: "task", Title: title},
				}}
			}
			rows := make([]driver.DerivedRow, population)
			for i, ref := range refs {
				body := []byte(fmt.Sprintf(`{"id":%q,"type":"task","title":"original"}`, ref.ID))
				if err := seed.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
					t.Fatal(err)
				}
				rows[i] = projection(i, model.Natural64(0), body, "original")
				header.Changes = append(header.Changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
			}
			if err := seed.PutSelectionDerived(ctx, driver.DerivedSortKey, nil, rows); err != nil {
				t.Fatal(err)
			}
			trace := &CommitTrace{}
			started := time.Now()
			entry, err := seed.Commit(WithCommitTrace(ctx, trace), header)
			seedElapsed := time.Since(started)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("population=%d seed_total_commit_ns=%d actual_revision=%s", population, seedElapsed.Nanoseconds(), entry.Revision)
			for sample := 0; sample < 3; sample++ {
				tx, h := beginSourceTestWithLocks(t, s, lease, extra[:1], true, refs[0])
				title := fmt.Sprintf("changed-%d", sample)
				body := []byte(fmt.Sprintf(`{"id":%q,"type":"task","title":%q}`, refs[0].ID, title))
				if err := tx.PutGraphRecord(ctx, driver.Record{Ref: refs[0], Kind: "bead", Body: body}, nil, nil); err != nil {
					t.Fatal(err)
				}
				row := projection(0, entry.Revision, body, title)
				if err := tx.PutSelectionDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{row}); err != nil {
					t.Fatal(err)
				}
				h.Changes = []model.ChangeRecord{{Ref: refs[0], Op: model.OpPatch, FromRevision: entry.Revision}}
				trace = &CommitTrace{}
				started = time.Now()
				entry, err = tx.Commit(WithCommitTrace(ctx, trace), h)
				elapsed := time.Since(started)
				if err != nil {
					t.Fatal(err)
				}
				phases := map[CommitPhase]int64{}
				for _, phase := range trace.Snapshot() {
					if phase.Failed {
						t.Fatalf("successful Commit has failed phase: %+v", phase)
					}
					phases[phase.Phase] = phase.Duration.Nanoseconds()
				}
				for _, required := range []CommitPhase{CommitPhaseRecords, CommitPhaseSelectionClose, CommitPhaseSQLiteCommit, CommitPhaseFinish} {
					if _, ok := phases[required]; !ok {
						t.Fatalf("actual Commit missing phase %s: %+v", required, phases)
					}
				}
				var durable int
				if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_bead_history WHERE end_revision IS NULL`).Scan(&durable); err != nil || durable != population {
					t.Fatalf("actual current selection rows=%d want=%d err=%v", durable, population, err)
				}
				t.Logf("population=%d sample=%d affected_sources=1 affected_derived=1 total_commit_ns=%d actual_revision=%s phases_ns=%v executor_row_visits=UNMEASURED ancestor_family=UNBUILT allocator_population=0", population, sample, elapsed.Nanoseconds(), entry.Revision, phases)
			}
		})
	}
}

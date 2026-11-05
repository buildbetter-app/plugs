package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func selectionRowPublicationContractInputs(facts bool) map[string]driver.LockState {
	rows := make(map[string]driver.LockState)
	add := func(key driver.DerivedRowKey, index *driver.SelectionIndex) {
		lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
		index.Row = key
		rows[keyID(lock)] = driver.LockState{Key: lock, Present: true,
			Derived: &driver.DerivedRow{Key: key.Key, Index: index}}
	}
	if facts {
		source := testKey("row-boundary-source")
		ref := *source.Ref
		add(driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: "row-boundary-facts"},
			&driver.SelectionIndex{Source: driver.SelectionSource{Key: source,
				Revision: model.Natural64(0), Present: true, Digest: "row-boundary-digest"},
				Facts: &driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{
					{Kind: driver.SelectionAncestorFact, Ref: &ref, Depth: 0},
					{Kind: driver.SelectionAncestorFact, Ref: &ref, Depth: 1},
				}}})
		return rows
	}
	for i := 0; i < 2; i++ {
		source := testKey(fmt.Sprintf("row-boundary-bead-%d", i))
		add(driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: fmt.Sprintf("row-boundary-sort-%d", i)},
			&driver.SelectionIndex{Source: driver.SelectionSource{Key: source,
				Revision: model.Natural64(0), Present: true, Digest: "row-boundary-digest"},
				Bead: &driver.SelectionBead{Ref: *source.Ref, Type: "task", MembershipLeaf: "row-boundary-leaf"}})
	}
	return rows
}

// The actual product caller must keep the original per-row statement boundary:
// a second row observes changes()==1 after the first scalar INSERT. A buffered
// multi-row INSERT instead exposes the prestatement value to both triggers.
func TestSelectionRowPublicationScalarBoundaryTriggers(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	for _, query := range []string{
		`CREATE TABLE selection_row_boundary_journal(row_key TEXT)`,
		`CREATE TRIGGER selection_row_boundary_refusal BEFORE INSERT ON selection_bead_history
		 WHEN (SELECT COUNT(*) FROM selection_row_boundary_journal)>0 AND changes()<>1
		 BEGIN SELECT RAISE(ABORT,'selection scalar statement boundary changed'); END`,
		`CREATE TRIGGER selection_row_boundary_journal_insert AFTER INSERT ON selection_bead_history
		 BEGIN INSERT INTO selection_row_boundary_journal(row_key) VALUES(NEW.derived_key); END`,
	} {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		if _, err := c.ExecContext(ctx, `DELETE FROM selection_row_boundary_journal WHERE 0`); err != nil {
			return err
		}
		return publishSelectionRows(ctx, c, "row-boundary-incarnation", revKey(1), selectionRowPublicationContractInputs(false))
	}); err != nil {
		t.Fatalf("scalar-boundary publication must retain trigger acceptance: %v", err)
	}
	for _, table := range []string{"selection_bead_history", "selection_row_boundary_journal"} {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil || count != 2 {
			t.Fatalf("%s count=%d, want two accepted scalar rows: %v", table, count, err)
		}
	}
}

// A missing immediate parent must fail on ordinal zero, before a later fact's
// trigger can produce another error. Outer rollback alone does not prove this.
func TestSelectionRowPublicationScalarForeignKeyErrorOrder(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	before, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`CREATE TRIGGER selection_row_boundary_remove_parent AFTER INSERT ON selection_fact_sets
		 BEGIN DELETE FROM selection_fact_sets WHERE incarnation=NEW.incarnation AND branch=NEW.branch
		 AND row_key=NEW.row_key AND start_revision=NEW.start_revision; END`,
		`CREATE TRIGGER selection_row_boundary_later_refusal BEFORE INSERT ON selection_facts WHEN NEW.ordinal=1
		 BEGIN SELECT RAISE(ABORT,'later selection fact refusal'); END`,
	} {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	err = s.immediate(ctx, nil, func(c *sql.Conn) error {
		var foreignKeys int
		if err := c.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
			return err
		}
		if foreignKeys != 1 {
			return fmt.Errorf("native foreign keys=%d, want enabled", foreignKeys)
		}
		return publishSelectionRows(ctx, c, "row-boundary-incarnation", revKey(1), selectionRowPublicationContractInputs(true))
	})
	if err == nil || !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") || strings.Contains(err.Error(), "later selection fact refusal") {
		t.Fatalf("earlier native FK refusal lost to a later ordinal: %v", err)
	}
	for _, table := range []string{"selection_bead_history", "selection_fact_sets", "selection_facts"} {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s survived native rollback: count=%d err=%v", table, count, err)
		}
	}
	after, err := s.Meta(ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("failed row publication changed metadata: before=%+v after=%+v err=%v", before, after, err)
	}
	for _, name := range []string{"selection_row_boundary_remove_parent", "selection_row_boundary_later_refusal"} {
		if _, err := s.db.ExecContext(ctx, `DROP TRIGGER `+name); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
		return publishSelectionRows(ctx, c, "row-boundary-incarnation", revKey(1), selectionRowPublicationContractInputs(true))
	}); err != nil {
		t.Fatalf("publication did not recover after native FK rollback: %v", err)
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_facts`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("recovered fact count=%d want two: %v", count, err)
	}
}

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
			if _, derived := tx.derived[id]; derived {
				if !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("derived source %q received a duplicate thin interval: %v", id, err)
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
	if !errors.Is(err, abort) || work.statements != 4 || work.rows != 260 {
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
	if work.statements != 4 || work.rows != 260 {
		t.Fatalf("source history commit: actual_exec=%d rows_affected=%d", work.statements, work.rows)
	}
	checkKeyPublicationPrepared(t, work, 4)
	check(true)
	t.Logf("source history actual_exec=%d rows_affected=%d executor_row_visits=UNMEASURED", work.statements, work.rows)
}

func TestSelectionSourceHistoryVirginProofKeepsRecordCloses(t *testing.T) {
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx := &transaction{records: make(map[string]*driver.Record), derived: make(map[string]driver.LockState)}
	for i := 0; i < selectionSourceHistoryBatch+1; i++ {
		id := fmt.Sprintf("0:virgin-record-%03d", i)
		tx.records[id] = &driver.Record{Body: []byte(fmt.Sprintf(`{"record":%d}`, i))}
	}
	for i := 0; i < 2*selectionSourceHistoryBatch+1; i++ {
		key := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: fmt.Sprintf("virgin-derived-%03d", i)}
		lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
		tx.derived[keyID(lock)] = driver.LockState{Key: lock}
	}
	var work keyPublicationWork
	err = s.immediate(ctx, nil, func(c *sql.Conn) error {
		var empty bool
		if err := c.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM selection_source_history)`).Scan(&empty); err != nil {
			return err
		}
		if !empty {
			return errors.New("fixture source history is not empty")
		}
		work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
		return tx.publishSelectionSourceHistoryWithProof(ctx, &work, meta.Incarnation, revKey(1), empty)
	})
	if err != nil {
		t.Fatal(err)
	}
	// The second batch still contains one record and must execute its close.
	// Only the later derived-only batches may omit zero-row native work.
	checkKeyPublicationPrepared(t, work, 3)
	if work.statements != 4 || work.rows != int64(len(tx.records)) || work.maxArgs > 7*selectionSourceHistoryBatch {
		t.Fatalf("virgin source history exec=%d rows=%d max_args=%d", work.statements, work.rows, work.maxArgs)
	}
	var records, derived int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE source_key LIKE '3:%') FROM selection_source_history`).Scan(&records, &derived); err != nil {
		t.Fatal(err)
	}
	if records != len(tx.records) || derived != 0 {
		t.Fatalf("virgin source history records=%d derived=%d", records, derived)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := (&transaction{derived: tx.derived}).publishSelectionSourceHistoryWithProof(cancelled, &keyPublicationWork{}, meta.Incarnation, revKey(2), true); !errors.Is(err, context.Canceled) {
		t.Fatalf("skipped derived close hid cancellation: %v", err)
	}
}

func TestSelectionSourceHistoryRecordTriggerInvalidatesVirginCloseProof(t *testing.T) {
	for _, test := range []struct {
		name, start string
		refuse      bool
	}{
		{name: "closes_triggered_derived_interval", start: revKey(0)},
		{name: "retains_native_check_refusal", start: revKey(1), refuse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			meta, err := s.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			tx := &transaction{records: make(map[string]*driver.Record), derived: make(map[string]driver.LockState)}
			for i := 0; i < selectionSourceHistoryBatch; i++ {
				id := fmt.Sprintf("0:trigger-record-%03d", i)
				tx.records[id] = &driver.Record{Body: []byte(`{"record":true}`)}
			}
			key := driver.DerivedRowKey{Table: driver.DerivedReadyState, Key: "trigger-derived"}
			lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
			derivedID := keyID(lock)
			tx.derived[derivedID] = driver.LockState{Key: lock}
			// The initial empty proof is true. One actual record INSERT then
			// creates the touched derived interval before the next batch closes.
			trigger := fmt.Sprintf(`CREATE TRIGGER selection_source_history_record_insert
				AFTER INSERT ON selection_source_history WHEN NEW.source_key='0:trigger-record-000'
				BEGIN INSERT INTO selection_source_history(
				incarnation,branch,source_key,start_revision,source_revision,source_present,source_digest)
				VALUES(NEW.incarnation,NEW.branch,'%s','%s','%s',1,'trigger'); END`,
				derivedID, test.start, test.start)
			if _, err := s.db.ExecContext(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			plan, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+selectionDerivedSourceHistoryEmptySQL, meta.Incarnation, revKey(0))
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
			if !strings.Contains(joined, "SEARCH selection_source_history USING") ||
				!strings.Contains(joined, "selection_source_history_current") || strings.Contains(joined, "SCAN selection_source_history") {
				t.Fatalf("derived close proof lost bounded index lookup: %s", joined)
			}
			var work keyPublicationWork
			err = s.immediate(ctx, nil, func(c *sql.Conn) error {
				var empty bool
				if err := c.QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM selection_source_history)`).Scan(&empty); err != nil {
					return err
				}
				if !empty {
					return errors.New("fixture source history is not empty")
				}
				work = keyPublicationWork{selectionCloseWork: selectionCloseWork{conn: c}}
				return tx.publishSelectionSourceHistoryWithProof(ctx, &work, meta.Incarnation, revKey(1), empty)
			})
			if test.refuse {
				if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
					t.Fatalf("native derived close refusal hidden: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			checkKeyPublicationPrepared(t, work, 3)
			var total int
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_source_history`).Scan(&total); err != nil {
				t.Fatal(err)
			}
			if test.refuse {
				if total != 0 || work.statements != 2 {
					t.Fatalf("native refusal rollback: rows=%d executed=%d", total, work.statements)
				}
				return
			}
			var end sql.NullString
			if err := s.db.QueryRowContext(ctx, `SELECT end_revision FROM selection_source_history WHERE source_key=?`, derivedID).Scan(&end); err != nil {
				t.Fatal(err)
			}
			if total != selectionSourceHistoryBatch+1 || !end.Valid || end.String != revKey(1) || work.statements != 3 {
				t.Fatalf("triggered derived interval not closed: rows=%d end=%+v executed=%d", total, end, work.statements)
			}
		})
	}
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

// Make the native UNIQUE failure deterministic without relying on Go map
// iteration order. The production helper still receives 257 record
// observations and the engine raises its UNIQUE error on the final record
// INSERT, after the two full batches have succeeded.
func selectionSourceHistoryUniqueTailFault(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	for _, query := range []string{
		`CREATE TABLE selection_source_history_unique_tail_guard(id INTEGER PRIMARY KEY)`,
		`INSERT INTO selection_source_history_unique_tail_guard(id) VALUES(1)`,
		`CREATE TABLE selection_source_history_unique_tail_count(n INTEGER NOT NULL)`,
		`INSERT INTO selection_source_history_unique_tail_count(n) VALUES(0)`,
		`CREATE TRIGGER selection_source_history_unique_tail_before
			BEFORE INSERT ON selection_source_history
			WHEN (SELECT n FROM selection_source_history_unique_tail_count)=256
			BEGIN INSERT INTO selection_source_history_unique_tail_guard(id) VALUES(1); END`,
		`CREATE TRIGGER selection_source_history_unique_tail_after
			AFTER INSERT ON selection_source_history
			BEGIN UPDATE selection_source_history_unique_tail_count SET n=n+1; END`,
	} {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
}

func dropSelectionSourceHistoryUniqueTailFault(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	for _, name := range []string{
		"selection_source_history_unique_tail_before",
		"selection_source_history_unique_tail_after",
	} {
		if _, err := s.db.ExecContext(ctx, `DROP TRIGGER `+name); err != nil {
			t.Fatal(err)
		}
	}
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
			for i := 0; i < test.count; i++ {
				id := fmt.Sprintf("0:prepared-history-%03d", i)
				body := []byte(fmt.Sprintf(`{"source":%d}`, i))
				observation := selectionSourceObservation{key: id}
				if i == test.count-1 {
					// Keep a valid eagerly published record sentinel in the final
					// observation. The engine fault below, rather than this key's
					// map position, determines the failing tail statement.
					ref := testRef("prepared-history-record")
					id = keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
					tx.records[id] = &driver.Record{Ref: ref, Kind: "bead", Body: body}
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
			if test.duplicate {
				selectionSourceHistoryUniqueTailFault(t, s)
			}
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
					dropSelectionSourceHistoryUniqueTailFault(t, s)
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

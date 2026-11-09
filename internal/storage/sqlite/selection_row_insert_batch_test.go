package sqlite

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This observer forwards to the actual SQLite connection and native prepared
// statements. Only cleanup errors are injected, after a real native Close.
// Counts describe dispatch/RowsAffected, not trigger writes or executor visits.
type selectionRowInsertNative struct {
	driver sqldriver.Driver
	dsn    string
	mu     sync.Mutex
	active bool
	connID int
	work   selectionRowInsertNativeWork

	cancelBefore, cancelAfter context.CancelFunc
	cancelAt                  int
	closeErrors               []error
}

type selectionRowInsertNativeWork struct {
	prepares, closes []string
	execs            []selectionRowInsertNativeExec
	queries, direct  int
}

type selectionRowInsertNativeExec struct {
	query    string
	bindings int
	connID   int
	rows     int64
	err      error
}

func (n *selectionRowInsertNative) Driver() sqldriver.Driver { return n.driver }
func (n *selectionRowInsertNative) Connect(context.Context) (sqldriver.Conn, error) {
	conn, err := n.driver.Open(n.dsn)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	n.connID++
	id := n.connID
	n.mu.Unlock()
	return &selectionRowInsertNativeConn{Conn: conn, native: n, id: id}, nil
}

type selectionRowInsertNativeConn struct {
	sqldriver.Conn
	native *selectionRowInsertNative
	id     int
}

func selectionRowInsertNativeSQL(query string) bool {
	return strings.HasPrefix(query, "INSERT INTO selection_bead_history(") ||
		strings.HasPrefix(query, "INSERT INTO selection_fact_sets(") ||
		strings.HasPrefix(query, "INSERT INTO selection_facts(")
}

func (c *selectionRowInsertNativeConn) BeginTx(ctx context.Context, options sqldriver.TxOptions) (sqldriver.Tx, error) {
	return c.Conn.(sqldriver.ConnBeginTx).BeginTx(ctx, options)
}

func (c *selectionRowInsertNativeConn) Prepare(query string) (sqldriver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *selectionRowInsertNativeConn) PrepareContext(ctx context.Context, query string) (sqldriver.Stmt, error) {
	statement, err := c.Conn.(sqldriver.ConnPrepareContext).PrepareContext(ctx, query)
	if !selectionRowInsertNativeSQL(query) {
		return statement, err
	}
	c.native.mu.Lock()
	if c.native.active {
		c.native.work.prepares = append(c.native.work.prepares, query)
	}
	c.native.mu.Unlock()
	if err != nil {
		return statement, err
	}
	return &selectionRowInsertNativeStmt{Stmt: statement, conn: c, query: query}, nil
}

func (c *selectionRowInsertNativeConn) ExecContext(ctx context.Context, query string, values []sqldriver.NamedValue) (sqldriver.Result, error) {
	c.native.mu.Lock()
	if c.native.active && selectionRowInsertNativeSQL(query) {
		c.native.work.direct++
	}
	c.native.mu.Unlock()
	return c.Conn.(sqldriver.ExecerContext).ExecContext(ctx, query, values)
}

func (c *selectionRowInsertNativeConn) QueryContext(ctx context.Context, query string, values []sqldriver.NamedValue) (sqldriver.Rows, error) {
	c.native.mu.Lock()
	if c.native.active {
		c.native.work.queries++
	}
	c.native.mu.Unlock()
	return c.Conn.(sqldriver.QueryerContext).QueryContext(ctx, query, values)
}

type selectionRowInsertNativeStmt struct {
	sqldriver.Stmt
	conn  *selectionRowInsertNativeConn
	query string
}

func (s *selectionRowInsertNativeStmt) ExecContext(ctx context.Context, values []sqldriver.NamedValue) (sqldriver.Result, error) {
	n := s.conn.native
	n.mu.Lock()
	active := n.active
	position := len(n.work.execs)
	var before, after context.CancelFunc
	if active {
		n.work.execs = append(n.work.execs, selectionRowInsertNativeExec{query: s.query, bindings: len(values), connID: s.conn.id})
		if position+1 == n.cancelAt {
			before, after = n.cancelBefore, n.cancelAfter
			n.cancelBefore, n.cancelAfter = nil, nil
		}
	}
	n.mu.Unlock()
	if before != nil {
		before() // cancel at the real native Exec, without fabricating its result
	}
	result, err := s.Stmt.(sqldriver.StmtExecContext).ExecContext(ctx, values)
	var rows int64
	if err == nil {
		rows, err = result.RowsAffected()
	}
	if active {
		n.mu.Lock()
		n.work.execs[position].rows, n.work.execs[position].err = rows, err
		n.mu.Unlock()
	}
	if after != nil && err == nil {
		after()
	}
	return result, err
}

func (s *selectionRowInsertNativeStmt) QueryContext(ctx context.Context, values []sqldriver.NamedValue) (sqldriver.Rows, error) {
	s.conn.native.mu.Lock()
	if s.conn.native.active {
		s.conn.native.work.queries++
	}
	s.conn.native.mu.Unlock()
	return s.Stmt.(sqldriver.StmtQueryContext).QueryContext(ctx, values)
}

func (s *selectionRowInsertNativeStmt) Close() error {
	err := s.Stmt.Close()
	n := s.conn.native
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.active {
		n.work.closes = append(n.work.closes, s.query)
		if position := len(n.work.closes) - 1; err == nil && position < len(n.closeErrors) {
			err = n.closeErrors[position]
		}
	}
	return err
}

func (n *selectionRowInsertNative) start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.work = selectionRowInsertNativeWork{}
	n.active = true
}

func (n *selectionRowInsertNative) stop() selectionRowInsertNativeWork {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.active = false
	return n.work
}

// This recipient has no Ready certificate fence. Retain the donor's trigger
// invalidation/rollback oracle with an explicitly test-owned marker on the
// actual SQLite tables, without importing an unrelated production certificate.
// Both the literal scalar reference and prepared writer use this same fixture.
func selectionRowInsertFixtureStore(t *testing.T, ctx context.Context, path string) *Store {
	t.Helper()
	s := openStore(t, ctx, path)
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE selection_row_insert_fence (
	 singleton INTEGER PRIMARY KEY, dirty INTEGER NOT NULL, ever_source INTEGER NOT NULL, generation TEXT NOT NULL);
	 INSERT INTO selection_row_insert_fence VALUES(1,0,0,'initial')`); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"selection_bead_history", "selection_fact_sets", "selection_facts"} {
		if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER selection_row_insert_fence_`+table+` AFTER INSERT ON `+table+`
		 BEGIN UPDATE selection_row_insert_fence SET dirty=1,ever_source=1,generation=lower(hex(randomblob(16))) WHERE singleton=1; END`); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func selectionRowInsertNativeStore(t *testing.T) (*Store, *selectionRowInsertNative) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "row-inserts.db")
	s := selectionRowInsertFixtureStore(t, context.Background(), path)
	u := url.URL{Scheme: "file", Path: path}
	n := &selectionRowInsertNative{driver: s.db.Driver(), dsn: u.String() + "?_pragma=busy_timeout(0)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s.db = sql.OpenDB(n)
	return s, n
}

type selectionRowInsertInput struct {
	kind   selectionRowInsertKind
	values []any
}

func (input selectionRowInsertInput) add(ctx context.Context, batch *selectionRowInsertBatch) error {
	switch input.kind {
	case selectionRowInsertBead:
		return batch.AddBead(ctx, input.values...)
	case selectionRowInsertFactSet:
		return batch.AddFactSet(ctx, input.values...)
	default:
		return batch.AddFact(ctx, input.values...)
	}
}

// Independent copies of the three original scalar INSERT arms, with no use of
// the helper's query builder, batching constants, or parameter normalization.
func (input selectionRowInsertInput) scalar(ctx context.Context, conn *sql.Conn) error {
	var query string
	switch input.kind {
	case selectionRowInsertBead:
		query = `INSERT INTO selection_bead_history(
		 incarnation,branch,derived_key,record_key,start_revision,bead_id,bead_type,project,status,title,template,parent_key,priority,membership_leaf,revision_text,source_revision,source_digest)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	case selectionRowInsertFactSet:
		query = `INSERT INTO selection_fact_sets(
		 incarnation,branch,row_key,row_table,kind,start_revision,source_key,source_revision,source_present,source_digest)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`
	default:
		query = `INSERT INTO selection_facts(
		 incarnation,branch,row_key,start_revision,ordinal,kind,ref_key,target_key,link_kind,subject,scope,role,scope_kind,project_name,name,value,active,depth)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	}
	_, err := conn.ExecContext(ctx, query, input.values...)
	return err
}

func selectionRowInsertBeadInput(i int) selectionRowInsertInput {
	var project, status, title, template, parent, priority any
	if i%3 == 1 {
		project, status, title, template, priority = "", "", "", false, int64(0)
	} else if i%3 == 2 {
		project, status, title, template, parent, priority = "project 雪", "todo", "quoted \" <>&\x00", true, "0:parent", int64(-3)
	}
	return selectionRowInsertInput{selectionRowInsertBead, []any{
		"row-insert-incarnation", revKey(0), fmt.Sprintf("3:bead-%04d", i), fmt.Sprintf("0:bead-%04d", i), revKey(42),
		fmt.Sprintf("bead-%04d", i), "task", project, status, title, template, parent, priority, "leaf 雪",
		"18446744073709551614", revKey(^uint64(0) - 1), "digest <>&",
	}}
}

func selectionRowInsertSetInput(i int) selectionRowInsertInput {
	return selectionRowInsertInput{selectionRowInsertFactSet, []any{
		"row-insert-incarnation", revKey(0), fmt.Sprintf("3:set-%04d", i), "ancestor_closure", "ancestor_v1", revKey(42),
		fmt.Sprintf("0:source-%04d", i), revKey(41), i % 2, "source digest 雪",
	}}
}

func selectionRowInsertFactInput(set, ordinal int) selectionRowInsertInput {
	var ref, target, link, scopeKind, project any
	if ordinal%3 == 1 {
		ref, target, link, scopeKind, project = "0:ref", "0:target", "parent", "project", ""
	} else if ordinal%3 == 2 {
		scopeKind, project = "project", "project 雪"
	}
	return selectionRowInsertInput{selectionRowInsertFact, []any{
		"row-insert-incarnation", revKey(0), fmt.Sprintf("3:set-%04d", set), revKey(42), ordinal, "ancestor_v1",
		ref, target, link, "subject 雪", "scope", "role", scopeKind, project, "label", "value\x00<&>", ordinal % 2, int64(ordinal),
	}}
}

func selectionRowInsertInputs(kind selectionRowInsertKind, count int) []selectionRowInsertInput {
	inputs := make([]selectionRowInsertInput, count)
	for i := range inputs {
		switch kind {
		case selectionRowInsertBead:
			inputs[i] = selectionRowInsertBeadInput(i)
		case selectionRowInsertFactSet:
			inputs[i] = selectionRowInsertSetInput(i)
		default:
			inputs[i] = selectionRowInsertFactInput(i/3, i%3)
		}
	}
	return inputs
}

func selectionRowInsertSeedParents(t *testing.T, s *Store, inputs []selectionRowInsertInput) {
	t.Helper()
	parents := make(map[int]bool)
	for _, input := range inputs {
		if input.kind == selectionRowInsertFact {
			var id int
			if _, err := fmt.Sscanf(input.values[2].(string), "3:set-%04d", &id); err != nil {
				t.Fatal(err)
			}
			parents[id] = true
		}
	}
	if err := s.immediate(context.Background(), nil, func(conn *sql.Conn) error {
		for id := range parents {
			if err := selectionRowInsertSetInput(id).scalar(context.Background(), conn); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func selectionRowInsertClose(batch *selectionRowInsertBatch, prior *error) {
	if err := batch.Close(); err != nil && *prior == nil {
		*prior = err
	}
}

func selectionRowInsertPublish(ctx context.Context, conn *sql.Conn, inputs []selectionRowInsertInput) (err error) {
	batch := newSelectionRowInsertBatch(conn)
	defer selectionRowInsertClose(batch, &err)
	for _, input := range inputs {
		if err := input.add(ctx, batch); err != nil {
			return err
		}
	}
	return batch.Flush(ctx)
}

func selectionRowInsertImages(t *testing.T, q queryer) map[string][][]any {
	t.Helper()
	images := make(map[string][][]any)
	for _, table := range []string{"selection_bead_history", "selection_fact_sets", "selection_facts"} {
		rows, err := q.QueryContext(context.Background(), `SELECT * FROM `+table+` ORDER BY 1,2,3,4,5`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values, targets := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			for i, value := range values {
				if bytes, ok := value.([]byte); ok {
					copied := make([]byte, len(bytes))
					copy(copied, bytes)
					values[i] = copied
				}
			}
			images[table] = append(images[table], values)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return images
}

// This test-only marker observes real SQL trigger effects, not Ready certification.
func selectionRowInsertFenceMarker(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(), `UPDATE selection_row_insert_fence
		SET dirty=0,ever_source=0,generation='row-insert-before' WHERE singleton=1`); err != nil {
		t.Fatal(err)
	}
}

func selectionRowInsertCheckFence(t *testing.T, s *Store, changed bool) {
	t.Helper()
	var dirty, ever int
	var generation string
	if err := s.db.QueryRowContext(context.Background(), `SELECT dirty,ever_source,generation FROM selection_row_insert_fence WHERE singleton=1`).Scan(&dirty, &ever, &generation); err != nil {
		t.Fatal(err)
	}
	want := 0
	if changed {
		want = 1
	}
	if dirty != want || ever != want || (generation != "row-insert-before") != changed {
		t.Fatalf("test-trigger invalidation dirty=%d ever_source=%d generation=%q changed=%t", dirty, ever, generation, changed)
	}
}

func selectionRowInsertCheckNative(t *testing.T, work selectionRowInsertNativeWork, prepares, executions, width, cap, count int) {
	t.Helper()
	if work.queries != 0 || work.direct != 0 || len(work.prepares) != prepares || len(work.execs) != executions || !reflect.DeepEqual(work.prepares, work.closes) {
		t.Fatalf("actual queries=%d direct_exec=%d prepares=%d prepared_exec=%d closes=%d want=%d/%d; cleanup order=%t", work.queries, work.direct, len(work.prepares), len(work.execs), len(work.closes), prepares, executions, reflect.DeepEqual(work.prepares, work.closes))
	}
	var total int64
	var id, maxBindings int
	for _, execution := range work.execs {
		if execution.err != nil || execution.bindings > 999 || execution.bindings/width > 128 || execution.bindings/width > cap || execution.rows != int64(execution.bindings/width) {
			t.Fatalf("invalid native batch: %#v width=%d cap=%d", execution, width, cap)
		}
		if id != 0 && execution.connID != id {
			t.Fatal("prepared writes left their held connection")
		}
		id, maxBindings = execution.connID, max(maxBindings, execution.bindings)
		total += execution.rows
	}
	if total != int64(count) || maxBindings != width*min(count, cap) {
		t.Fatalf("actual affected=%d max_bindings=%d want=%d/%d", total, maxBindings, count, width*min(count, cap))
	}
}

func TestSelectionRowInsertBatchesNativeCapsReuseAndScalarImages(t *testing.T) {
	for _, arm := range []struct {
		name             string
		kind             selectionRowInsertKind
		columns, maxRows int
	}{
		{"bead", selectionRowInsertBead, 17, 58},
		{"fact_set", selectionRowInsertFactSet, 10, 99},
		{"fact", selectionRowInsertFact, 18, 55},
	} {
		for _, count := range []int{0, 1, arm.maxRows - 1, arm.maxRows, arm.maxRows + 1, 2 * arm.maxRows, 2*arm.maxRows + 1, 1000, 2000} {
			t.Run(fmt.Sprintf("%s/%d", arm.name, count), func(t *testing.T) {
				ctx := context.Background()
				s, native := selectionRowInsertNativeStore(t)
				reference := selectionRowInsertFixtureStore(t, ctx, filepath.Join(t.TempDir(), "scalar.db"))
				inputs := selectionRowInsertInputs(arm.kind, count)
				if arm.kind == selectionRowInsertFact {
					selectionRowInsertSeedParents(t, s, inputs)
					selectionRowInsertSeedParents(t, reference, inputs)
				}
				selectionRowInsertFenceMarker(t, s)
				selectionRowInsertFenceMarker(t, reference)
				var work selectionRowInsertNativeWork
				if err := s.immediate(ctx, nil, func(conn *sql.Conn) error {
					native.start()
					err := selectionRowInsertPublish(ctx, conn, inputs)
					work = native.stop()
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if err := reference.immediate(ctx, nil, func(conn *sql.Conn) error {
					for _, input := range inputs {
						if err := input.scalar(ctx, conn); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(selectionRowInsertImages(t, s.db), selectionRowInsertImages(t, reference.db)) {
					t.Fatal("exact source tokens, temporal rows, NULLs, ordinals or scalar values differ")
				}
				prepares := 0
				if count >= arm.maxRows {
					prepares++
				}
				if count%arm.maxRows != 0 {
					prepares++
				}
				selectionRowInsertCheckNative(t, work, prepares, (count+arm.maxRows-1)/arm.maxRows, arm.columns, arm.maxRows, count)
				selectionRowInsertCheckFence(t, s, count != 0)
				selectionRowInsertCheckFence(t, reference, count != 0)
			})
		}
	}
}

func selectionRowInsertInstallTrace(t *testing.T, s *Store) {
	t.Helper()
	for _, query := range []string{
		`CREATE TABLE selection_row_trace(seq INTEGER PRIMARY KEY,event TEXT NOT NULL)`,
		`CREATE TRIGGER selection_row_bead_trace AFTER INSERT ON selection_bead_history BEGIN
		 INSERT INTO selection_row_trace(event) VALUES('bead:'||NEW.derived_key); END`,
		`CREATE TRIGGER selection_row_set_trace AFTER INSERT ON selection_fact_sets BEGIN
		 INSERT INTO selection_row_trace(event) VALUES('set:'||NEW.row_key); END`,
		`CREATE TRIGGER selection_row_fact_parent BEFORE INSERT ON selection_facts BEGIN
		 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM selection_fact_sets
		 WHERE incarnation=NEW.incarnation AND branch=NEW.branch AND row_key=NEW.row_key
		 AND start_revision=NEW.start_revision) THEN RAISE(ABORT,'fact before set') END; END`,
		`CREATE TRIGGER selection_row_fact_trace AFTER INSERT ON selection_facts BEGIN
		 INSERT INTO selection_row_trace(event) VALUES('fact:'||NEW.row_key||':'||NEW.ordinal); END`,
	} {
		if _, err := s.db.ExecContext(context.Background(), query); err != nil {
			t.Fatal(err)
		}
	}
}

func selectionRowInsertTrace(t *testing.T, q queryer) []string {
	t.Helper()
	rows, err := q.QueryContext(context.Background(), `SELECT event FROM selection_row_trace ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var trace []string
	for rows.Next() {
		var event string
		if err := rows.Scan(&event); err != nil {
			t.Fatal(err)
		}
		trace = append(trace, event)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return trace
}

func TestSelectionRowInsertBatchesKeepContiguousTriggerAndParentOrder(t *testing.T) {
	ctx := context.Background()
	s, native := selectionRowInsertNativeStore(t)
	reference := selectionRowInsertFixtureStore(t, ctx, filepath.Join(t.TempDir(), "scalar.db"))
	selectionRowInsertInstallTrace(t, s)
	selectionRowInsertInstallTrace(t, reference)
	// Empty complete sets remain writes, and facts retain their supplied
	// ordinal/source order rather than moving behind another source's set.
	inputs := []selectionRowInsertInput{
		selectionRowInsertBeadInput(0), selectionRowInsertBeadInput(1), selectionRowInsertBeadInput(2),
		selectionRowInsertSetInput(0), selectionRowInsertFactInput(0, 0), selectionRowInsertFactInput(0, 1),
		selectionRowInsertSetInput(1), selectionRowInsertFactInput(1, 0),
		selectionRowInsertSetInput(2), selectionRowInsertBeadInput(3),
	}
	var work selectionRowInsertNativeWork
	if err := s.immediate(ctx, nil, func(conn *sql.Conn) error {
		native.start()
		err := selectionRowInsertPublish(ctx, conn, inputs)
		work = native.stop()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := reference.immediate(ctx, nil, func(conn *sql.Conn) error {
		for _, input := range inputs {
			if err := input.scalar(ctx, conn); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selectionRowInsertImages(t, s.db), selectionRowInsertImages(t, reference.db)) ||
		!reflect.DeepEqual(selectionRowInsertTrace(t, s.db), selectionRowInsertTrace(t, reference.db)) {
		t.Fatal("same-SQL coalescing reordered rows, parent visibility or row-trigger events")
	}
	// B3,S1,F2,S1,F1,S1,B1: S1 is reused across three separate runs.
	if work.queries != 0 || work.direct != 0 || len(work.execs) != 7 || len(work.prepares) != 5 || !reflect.DeepEqual(work.prepares, work.closes) {
		t.Fatalf("contiguous runs/prepared first-use order changed: %#v", work)
	}
}

// Capture the actual still-open transaction after failure, before the owning
// immediate callback rolls it back. This deliberately distinguishes statement
// prefix semantics from the separate durable rollback oracle.
func selectionRowInsertFailedAttempt(t *testing.T, s *Store, native *selectionRowInsertNative, inputs []selectionRowInsertInput) (map[string][][]any, selectionRowInsertNativeWork, error) {
	t.Helper()
	ctx := context.Background()
	var prefix map[string][][]any
	var work selectionRowInsertNativeWork
	err := s.immediate(ctx, nil, func(conn *sql.Conn) (err error) {
		if native == nil {
			for _, input := range inputs {
				if err = input.scalar(ctx, conn); err != nil {
					break
				}
			}
		} else {
			native.start()
			batch := newSelectionRowInsertBatch(conn)
			for _, input := range inputs {
				if err = input.add(ctx, batch); err != nil {
					break
				}
			}
			if err == nil {
				err = batch.Flush(ctx)
			}
			if err != nil {
				if later := batch.AddBead(ctx, selectionRowInsertBeadInput(999).values...); later != err || batch.Flush(ctx) != err {
					t.Fatal("later Add/Flush replaced the first native error")
				}
			}
			selectionRowInsertClose(batch, &err)
			work = native.stop()
		}
		if err == nil {
			return errors.New("failure fixture unexpectedly succeeded")
		}
		prefix = selectionRowInsertImages(t, conn)
		return err
	})
	return prefix, work, err
}

func TestSelectionRowInsertBatchesExposeAbortPrefixesAndRollBack(t *testing.T) {
	for _, test := range []struct {
		name, table, errorText string
		kind                   selectionRowInsertKind
		trigger, foreign       bool
	}{
		{"bead_unique", "selection_bead_history", "UNIQUE", selectionRowInsertBead, false, false},
		{"set_unique", "selection_fact_sets", "UNIQUE", selectionRowInsertFactSet, false, false},
		{"fact_unique", "selection_facts", "UNIQUE", selectionRowInsertFact, false, false},
		{"fact_foreign_key", "selection_facts", "FOREIGN KEY", selectionRowInsertFact, false, true},
		{"trigger_abort", "selection_bead_history", "row trigger abort", selectionRowInsertBead, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			s, native := selectionRowInsertNativeStore(t)
			reference := selectionRowInsertFixtureStore(t, ctx, filepath.Join(t.TempDir(), "scalar.db"))
			inputs := selectionRowInsertInputs(test.kind, 3)
			if test.kind == selectionRowInsertFact {
				selectionRowInsertSeedParents(t, s, inputs)
				selectionRowInsertSeedParents(t, reference, inputs)
			}
			if test.foreign {
				inputs[2] = selectionRowInsertFactInput(99, 0) // earlier valid facts exist at the failure boundary
			} else if !test.trigger {
				inputs[2] = inputs[0]
			}
			if test.trigger {
				for _, store := range []*Store{s, reference} {
					if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER selection_row_reject BEFORE INSERT ON selection_bead_history
					 WHEN NEW.bead_id='bead-0002' BEGIN SELECT RAISE(ABORT,'row trigger abort'); END`); err != nil {
						t.Fatal(err)
					}
				}
			}
			selectionRowInsertFenceMarker(t, s)
			selectionRowInsertFenceMarker(t, reference)
			before := selectionRowInsertImages(t, s.db)
			native.closeErrors = []error{io.ErrClosedPipe}
			batchedPrefix, work, batchErr := selectionRowInsertFailedAttempt(t, s, native, inputs)
			scalarPrefix, _, scalarErr := selectionRowInsertFailedAttempt(t, reference, nil, inputs)
			if batchErr == nil || scalarErr == nil || !strings.Contains(batchErr.Error(), test.errorText) || !strings.Contains(scalarErr.Error(), test.errorText) || errors.Is(batchErr, io.ErrClosedPipe) {
				t.Fatalf("native errors or cleanup precedence: batch=%v scalar=%v", batchErr, scalarErr)
			}
			if len(batchedPrefix[test.table]) != len(before[test.table]) || len(scalarPrefix[test.table]) != len(before[test.table])+2 {
				t.Fatalf("statement-wide ABORT boundary changed: batch_prefix=%d scalar_prefix=%d baseline=%d", len(batchedPrefix[test.table]), len(scalarPrefix[test.table]), len(before[test.table]))
			}
			if work.direct != 0 || work.queries != 0 || len(work.execs) != 1 || work.execs[0].err == nil || !reflect.DeepEqual(work.prepares, work.closes) {
				t.Fatalf("failed batch was retried, queried or leaked: %#v", work)
			}
			if !reflect.DeepEqual(selectionRowInsertImages(t, s.db), before) || !reflect.DeepEqual(selectionRowInsertImages(t, reference.db), before) {
				t.Fatal("outer rollback retained failed statement data")
			}
			selectionRowInsertCheckFence(t, s, false)
			selectionRowInsertCheckFence(t, reference, false)
			if test.trigger {
				for _, store := range []*Store{s, reference} {
					if _, err := store.db.ExecContext(ctx, `DROP TRIGGER selection_row_reject`); err != nil {
						t.Fatal(err)
					}
				}
			}
			native.closeErrors = nil
			recovery := selectionRowInsertInputs(test.kind, 3)
			if err := s.immediate(ctx, nil, func(conn *sql.Conn) error { return selectionRowInsertPublish(ctx, conn, recovery) }); err != nil {
				t.Fatalf("failed native batch damaged recovery: %v", err)
			}
			if err := reference.immediate(ctx, nil, func(conn *sql.Conn) error {
				for _, input := range recovery {
					if err := input.scalar(ctx, conn); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(selectionRowInsertImages(t, s.db), selectionRowInsertImages(t, reference.db)) {
				t.Fatal("recovered native images differ from scalar reference")
			}
		})
	}
}

func TestSelectionRowInsertBatchesExposeEarlierFKLaterUniqueFirstError(t *testing.T) {
	ctx := context.Background()
	s, native := selectionRowInsertNativeStore(t)
	reference := selectionRowInsertFixtureStore(t, ctx, filepath.Join(t.TempDir(), "scalar.db"))
	for _, store := range []*Store{s, reference} {
		if err := store.immediate(ctx, nil, func(conn *sql.Conn) error {
			if err := selectionRowInsertSetInput(0).scalar(ctx, conn); err != nil {
				return err
			}
			return selectionRowInsertFactInput(0, 1).scalar(ctx, conn)
		}); err != nil {
			t.Fatal(err)
		}
	}
	before := selectionRowInsertImages(t, s.db)
	inputs := []selectionRowInsertInput{selectionRowInsertFactInput(99, 0), selectionRowInsertFactInput(0, 1)}
	_, work, batchErr := selectionRowInsertFailedAttempt(t, s, native, inputs)
	_, _, scalarErr := selectionRowInsertFailedAttempt(t, reference, nil, inputs)
	// Immediate FK validation occurs at the end of a native statement. A later
	// duplicate in that same statement can therefore win before the earlier
	// missing parent is reported. This is explicitly UNSUPPORTED equivalence.
	if batchErr == nil || scalarErr == nil || !strings.Contains(batchErr.Error(), "UNIQUE") || !strings.Contains(scalarErr.Error(), "FOREIGN KEY") {
		t.Fatalf("concrete first-error boundary: multi-row=%v scalar=%v", batchErr, scalarErr)
	}
	if len(work.execs) != 1 || work.execs[0].err == nil || !reflect.DeepEqual(selectionRowInsertImages(t, s.db), before) || !reflect.DeepEqual(selectionRowInsertImages(t, reference.db), before) {
		t.Fatal("first-error control lost native failure or durable rollback")
	}
}

func TestSelectionRowInsertBatchesExposeChangesSensitiveTriggerRefusal(t *testing.T) {
	ctx := context.Background()
	s, native := selectionRowInsertNativeStore(t)
	reference := selectionRowInsertFixtureStore(t, ctx, filepath.Join(t.TempDir(), "scalar.db"))
	for _, store := range []*Store{s, reference} {
		if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER selection_row_changes BEFORE INSERT ON selection_bead_history
		 WHEN NEW.bead_id='bead-0001' AND changes()=1
		 BEGIN SELECT RAISE(ABORT,'prior scalar changes'); END`); err != nil {
			t.Fatal(err)
		}
	}
	inputs := selectionRowInsertInputs(selectionRowInsertBead, 2)
	if err := s.immediate(ctx, nil, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `UPDATE selection_row_insert_fence SET dirty=dirty WHERE singleton=99`); err != nil {
			return err
		}
		native.start()
		err := selectionRowInsertPublish(ctx, conn, inputs)
		native.stop()
		return err
	}); err != nil {
		t.Fatalf("multi-row statement unexpectedly inherited scalar changes(): %v", err)
	}
	scalarErr := reference.immediate(ctx, nil, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `UPDATE selection_row_insert_fence SET dirty=dirty WHERE singleton=99`); err != nil {
			return err
		}
		for _, input := range inputs {
			if err := input.scalar(ctx, conn); err != nil {
				return err
			}
		}
		return nil
	})
	if scalarErr == nil || !strings.Contains(scalarErr.Error(), "prior scalar changes") {
		t.Fatalf("scalar reference did not expose trigger refusal: %v", scalarErr)
	}
	if len(selectionRowInsertImages(t, s.db)["selection_bead_history"]) != 2 || len(selectionRowInsertImages(t, reference.db)["selection_bead_history"]) != 0 {
		t.Fatal("statement-boundary control lost the durable success/refusal difference")
	}
}

func TestSelectionRowInsertBatchesLazyCloseAndArgumentOwnership(t *testing.T) {
	ctx := context.Background()
	s, native := selectionRowInsertNativeStore(t)
	var work selectionRowInsertNativeWork
	if err := s.immediate(ctx, nil, func(conn *sql.Conn) error {
		native.start()
		batch := newSelectionRowInsertBatch(conn)
		if err := batch.Flush(ctx); err != nil {
			return err
		}
		first := selectionRowInsertBeadInput(0)
		bytes := []byte("accepted")
		first.values[9] = bytes
		if err := first.add(ctx, batch); err != nil {
			return err
		}
		bytes[0] = 'X'
		second := selectionRowInsertBeadInput(1)
		second.values[9] = []byte{} // nonnil empty blob must not become SQL NULL
		if err := second.add(ctx, batch); err != nil {
			return err
		}
		if err := batch.Flush(ctx); err != nil {
			return err
		}
		if err := batch.Close(); err != nil || batch.Close() != nil {
			return fmt.Errorf("idempotent close: %v", err)
		}
		if first.add(ctx, batch) == nil || batch.Flush(ctx) == nil {
			return errors.New("closed helper accepted later work")
		}
		work = native.stop()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	selectionRowInsertCheckNative(t, work, 1, 1, 17, 58, 2)
	var title, kind string
	var length int
	if err := s.db.QueryRowContext(ctx, `SELECT title FROM selection_bead_history WHERE bead_id='bead-0000'`).Scan(&title); err != nil || title != "accepted" {
		t.Fatalf("accepted arguments changed before Flush: title=%q err=%v", title, err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT typeof(title),length(title) FROM selection_bead_history WHERE bead_id='bead-0001'`).Scan(&kind, &length); err != nil || kind != "blob" || length != 0 {
		t.Fatalf("empty blob became NULL: kind=%q length=%d err=%v", kind, length, err)
	}
	before := selectionRowInsertImages(t, s.db)
	if err := s.immediate(ctx, nil, func(conn *sql.Conn) error {
		native.start()
		batch := newSelectionRowInsertBatch(conn)
		if err := batch.Close(); err != nil || batch.Close() != nil {
			return fmt.Errorf("unused Close: %v", err)
		}
		pending := newSelectionRowInsertBatch(conn)
		if err := selectionRowInsertBeadInput(99).add(ctx, pending); err != nil {
			return err
		}
		if err := pending.Close(); err != nil {
			return err
		}
		work = native.stop()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(work.prepares) != 0 || len(work.execs) != 0 || len(work.closes) != 0 || work.queries != 0 || work.direct != 0 || !reflect.DeepEqual(selectionRowInsertImages(t, s.db), before) {
		t.Fatal("constructor/empty Flush/Close eagerly published pending data")
	}
}

func TestSelectionRowInsertBatchesCloseAllHandlesAndPreserveFirstCleanupError(t *testing.T) {
	ctx := context.Background()
	s, native := selectionRowInsertNativeStore(t)
	first, later := errors.New("first after native Close"), errors.New("later after native Close")
	native.closeErrors = []error{first, later}
	inputs := selectionRowInsertInputs(selectionRowInsertBead, 117)
	var work selectionRowInsertNativeWork
	err := s.immediate(ctx, nil, func(conn *sql.Conn) (err error) {
		native.start()
		batch := newSelectionRowInsertBatch(conn)
		for _, input := range inputs {
			if err := input.add(ctx, batch); err != nil {
				return err
			}
		}
		if err := batch.Flush(ctx); err != nil {
			return err
		}
		err = batch.Close()
		if err != first || batch.Close() != first {
			t.Fatal("Close did not retain the first native cleanup result")
		}
		work = native.stop()
		return err
	})
	if !errors.Is(err, first) || len(work.execs) != 3 || len(work.prepares) != 2 || !reflect.DeepEqual(work.prepares, work.closes) {
		t.Fatalf("cleanup order/rollback: err=%v work=%#v", err, work)
	}
	if len(selectionRowInsertImages(t, s.db)["selection_bead_history"]) != 0 {
		t.Fatal("cleanup error committed rows before coverage")
	}
	native.closeErrors = nil
	if err := s.immediate(ctx, nil, func(conn *sql.Conn) error { return selectionRowInsertPublish(ctx, conn, inputs) }); err != nil {
		t.Fatalf("native cleanup damaged later publication: %v", err)
	}
}

func TestSelectionRowInsertBatchesNativeCancellationRollbackAndRecovery(t *testing.T) {
	for _, boundary := range []string{"before_first_native_exec", "after_first_native_exec"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			s, native := selectionRowInsertNativeStore(t)
			selectionRowInsertFenceMarker(t, s)
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			native.cancelAt = 1
			if boundary == "before_first_native_exec" {
				native.cancelBefore = cancel
			} else {
				native.cancelAfter = cancel
			}
			native.closeErrors = []error{io.ErrClosedPipe}
			inputs := selectionRowInsertInputs(selectionRowInsertBead, 117)
			var work selectionRowInsertNativeWork
			err := s.immediate(call, nil, func(conn *sql.Conn) error {
				native.start()
				err := selectionRowInsertPublish(call, conn, inputs)
				work = native.stop()
				return err
			})
			if !errors.Is(err, context.Canceled) || errors.Is(err, io.ErrClosedPipe) || len(work.execs) != 1 || len(work.prepares) != 1 || !reflect.DeepEqual(work.prepares, work.closes) || work.queries != 0 || work.direct != 0 {
				t.Fatalf("native cancellation/cleanup boundary: err=%v work=%#v", err, work)
			}
			if boundary == "before_first_native_exec" && (!errors.Is(work.execs[0].err, context.Canceled) || work.execs[0].rows != 0) {
				t.Fatal("pre-execution cancellation was not supplied by the actual native statement")
			}
			if boundary == "after_first_native_exec" && (work.execs[0].err != nil || work.execs[0].rows != 58) {
				t.Fatal("post-execution cancellation did not follow a real full native batch")
			}
			if len(selectionRowInsertImages(t, s.db)["selection_bead_history"]) != 0 {
				t.Fatal("canceled write retained an accepted native prefix")
			}
			selectionRowInsertCheckFence(t, s, false)
			native.closeErrors, native.cancelBefore, native.cancelAfter = nil, nil, nil
			native.cancelAt = 0
			if err := s.immediate(ctx, nil, func(conn *sql.Conn) error { return selectionRowInsertPublish(ctx, conn, inputs) }); err != nil {
				t.Fatalf("canceled native connection failed recovery: %v", err)
			}
			if len(selectionRowInsertImages(t, s.db)["selection_bead_history"]) != 117 {
				t.Fatal("recovered write lost accepted rows")
			}
			selectionRowInsertCheckFence(t, s, true)
		})
	}
}

func TestSelectionRowInsertBatchesLateAbortAndNativeSQLFailure(t *testing.T) {
	ctx := context.Background()
	s, native := selectionRowInsertNativeStore(t)
	selectionRowInsertFenceMarker(t, s)
	before := selectionRowInsertImages(t, s.db)
	inputs := selectionRowInsertInputs(selectionRowInsertBead, 117)
	abort := errors.New("late error before coverage/commit")
	var work selectionRowInsertNativeWork
	err := s.immediate(ctx, nil, func(conn *sql.Conn) error {
		native.start()
		err := selectionRowInsertPublish(ctx, conn, inputs)
		work = native.stop()
		if err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) || len(work.execs) != 3 || !reflect.DeepEqual(work.prepares, work.closes) || !reflect.DeepEqual(selectionRowInsertImages(t, s.db), before) {
		t.Fatalf("late abort did not roll back completed native batches: err=%v work=%#v", err, work)
	}
	selectionRowInsertCheckFence(t, s, false)
	// A real SQL preparation/execution failure, with no fabricated driver
	// response and no scalar retry. SQLite may defer its prepare until Exec.
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE selection_bead_history RENAME COLUMN source_digest TO unavailable_source_digest`); err != nil {
		t.Fatal(err)
	}
	err = s.immediate(ctx, nil, func(conn *sql.Conn) error {
		native.start()
		batch := newSelectionRowInsertBatch(conn)
		err := selectionRowInsertBeadInput(0).add(ctx, batch)
		if err == nil {
			err = batch.Flush(ctx)
		}
		if err == nil || !strings.Contains(err.Error(), "source_digest") || batch.Flush(ctx) != err || batch.AddFactSet(ctx, selectionRowInsertSetInput(0).values...) != err {
			t.Fatalf("native SQL error became a miss/retry or changed: %v", err)
		}
		selectionRowInsertClose(batch, &err)
		work = native.stop()
		return err
	})
	if err == nil || len(work.prepares) != 1 || len(work.execs) > 1 || work.queries != 0 || work.direct != 0 {
		t.Fatalf("native SQL failure dispatch: err=%v work=%#v", err, work)
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE selection_bead_history RENAME COLUMN unavailable_source_digest TO source_digest`); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selectionRowInsertImages(t, s.db), before) {
		t.Fatal("native failed statement published data")
	}
	if err := s.immediate(ctx, nil, func(conn *sql.Conn) error { return selectionRowInsertPublish(ctx, conn, inputs) }); err != nil {
		t.Fatalf("late abort/native SQL failure did not recover: %v", err)
	}
}

// Exercise the recipient's actual publication caller too. A failure on the
// second fact must leave the first fact in the still-open transaction, then
// outer rollback must erase the set, fact and trigger effects together.
func TestSelectionPublicationPreparedRowsKeepScalarBoundaries(t *testing.T) {
	for _, failSecond := range []bool{false, true} {
		t.Run(fmt.Sprintf("second_fact_failure_%t", failSecond), func(t *testing.T) {
			ctx := context.Background()
			s, native := selectionRowInsertNativeStore(t)
			selectionRowInsertFenceMarker(t, s)
			if failSecond {
				if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER selection_row_second_fact BEFORE INSERT ON selection_facts
				 WHEN NEW.ordinal=1 BEGIN SELECT RAISE(ABORT,'second scalar fact'); END`); err != nil {
					t.Fatal(err)
				}
			}
			ref := testRef("pl-scalar-source")
			key := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: "scalar-publication"}
			row := driver.DerivedRow{Key: key.Key, Value: []byte(`{}`), Index: &driver.SelectionIndex{
				Version: driver.SelectionIndexVersion, Incarnation: "scalar-publication", Branch: model.Natural64(0), Row: key, ValueDigest: driver.SelectionDigest([]byte(`{}`)),
				Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: model.Natural64(1), Present: true, Digest: driver.SelectionDigest([]byte(`{}`))},
				Facts: &driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{
					{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &ref},
					{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &ref, Depth: 1},
				}},
			}}
			lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
			derived := map[string]driver.LockState{keyID(lock): {Key: lock, Present: true, Derived: &row}}
			var prefix map[string][][]any
			var work selectionRowInsertNativeWork
			err := s.immediate(ctx, nil, func(conn *sql.Conn) error {
				native.start()
				err := publishSelectionRows(ctx, conn, "scalar-publication", revKey(2), derived)
				work = native.stop()
				prefix = selectionRowInsertImages(t, conn)
				return err
			})
			wantFacts := 2
			if failSecond {
				wantFacts = 1
				if err == nil || !strings.Contains(err.Error(), "second scalar fact") {
					t.Fatalf("actual caller lost native first error: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if len(prefix["selection_fact_sets"]) != 1 || len(prefix["selection_facts"]) != wantFacts {
				t.Fatalf("actual caller changed statement prefix: %#v", prefix)
			}
			if len(work.prepares) != 2 || len(work.execs) != 3 || !reflect.DeepEqual(work.prepares, work.closes) || work.queries != 0 || work.direct != 0 {
				t.Fatalf("actual caller lost scalar reuse/cleanup: %#v", work)
			}
			for i, width := range []int{10, 18, 18} {
				if work.execs[i].bindings != width || (work.execs[i].err != nil) != (failSecond && i == 2) {
					t.Fatalf("actual caller changed scalar execution %d: %#v", i, work.execs[i])
				}
			}
			durable := selectionRowInsertImages(t, s.db)
			if failSecond && len(durable) != 0 || !failSecond && !reflect.DeepEqual(durable, prefix) {
				t.Fatalf("actual caller commit/rollback changed durable image: %#v", durable)
			}
			selectionRowInsertCheckFence(t, s, !failSecond)
		})
	}
}

func TestSelectionRowInsertPreparedScalarPreservesNativeBoundaries(t *testing.T) {
	for _, name := range []string{"mixed_rows_and_triggers", "changes_trigger_refusal", "earlier_fk_before_later_unique", "trigger_abort_prefix"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, native := selectionRowInsertNativeStore(t)
			reference := selectionRowInsertFixtureStore(t, ctx, filepath.Join(t.TempDir(), "scalar.db"))
			inputs := []selectionRowInsertInput{
				selectionRowInsertBeadInput(0), selectionRowInsertBeadInput(1),
				selectionRowInsertSetInput(0), selectionRowInsertFactInput(0, 0), selectionRowInsertFactInput(0, 1),
				selectionRowInsertSetInput(1), selectionRowInsertSetInput(2), selectionRowInsertFactInput(2, 0),
				selectionRowInsertBeadInput(2),
			}
			for _, store := range []*Store{s, reference} {
				if name == "earlier_fk_before_later_unique" {
					if err := store.immediate(ctx, nil, func(conn *sql.Conn) error {
						if err := selectionRowInsertSetInput(0).scalar(ctx, conn); err != nil {
							return err
						}
						return selectionRowInsertFactInput(0, 1).scalar(ctx, conn)
					}); err != nil {
						t.Fatal(err)
					}
				}
				selectionRowInsertInstallTrace(t, store)
				if name == "earlier_fk_before_later_unique" {
					// Let the actual FK constraint supply the first error;
					// the separate ordering trace must not replace it.
					if _, err := store.db.ExecContext(ctx, `DROP TRIGGER selection_row_fact_parent`); err != nil {
						t.Fatal(err)
					}
				}
				if name == "changes_trigger_refusal" {
					if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER selection_row_changes BEFORE INSERT ON selection_bead_history
					 WHEN NEW.bead_id='bead-0001' AND changes()=1
					 BEGIN SELECT RAISE(ABORT,'prior scalar changes'); END`); err != nil {
						t.Fatal(err)
					}
				}
				if name == "trigger_abort_prefix" {
					if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER selection_row_reject BEFORE INSERT ON selection_bead_history
					 WHEN NEW.bead_id='bead-0002' BEGIN SELECT RAISE(ABORT,'row trigger abort'); END`); err != nil {
						t.Fatal(err)
					}
				}
			}
			wantError, wantExec, wantPrepares := "", len(inputs), 3
			switch name {
			case "changes_trigger_refusal":
				inputs = selectionRowInsertInputs(selectionRowInsertBead, 2)
				wantError, wantExec, wantPrepares = "prior scalar changes", 2, 1
			case "earlier_fk_before_later_unique":
				inputs = []selectionRowInsertInput{selectionRowInsertFactInput(99, 0), selectionRowInsertFactInput(0, 1)}
				wantError, wantExec, wantPrepares = "FOREIGN KEY", 1, 1
			case "trigger_abort_prefix":
				inputs = selectionRowInsertInputs(selectionRowInsertBead, 3)
				wantError, wantExec, wantPrepares = "row trigger abort", 3, 1
			}
			var work selectionRowInsertNativeWork
			var prefix, scalarPrefix map[string][][]any
			var trace, scalarTrace []string
			err := s.immediate(ctx, nil, func(conn *sql.Conn) (err error) {
				if _, err := conn.ExecContext(ctx, `UPDATE selection_row_insert_fence SET dirty=dirty WHERE singleton=99`); err != nil {
					return err
				}
				native.start()
				batch := newSelectionRowInsertBatch(conn)
				for _, input := range inputs {
					if err = input.add(ctx, batch); err != nil {
						break
					}
					// This is the integrator's supported contract: Flush exactly
					// where the original scalar statement returned to Go.
					if err = batch.Flush(ctx); err != nil {
						break
					}
				}
				selectionRowInsertClose(batch, &err)
				work = native.stop()
				prefix, trace = selectionRowInsertImages(t, conn), selectionRowInsertTrace(t, conn)
				return err
			})
			scalarErr := reference.immediate(ctx, nil, func(conn *sql.Conn) (err error) {
				if _, err := conn.ExecContext(ctx, `UPDATE selection_row_insert_fence SET dirty=dirty WHERE singleton=99`); err != nil {
					return err
				}
				for _, input := range inputs {
					if err = input.scalar(ctx, conn); err != nil {
						break
					}
				}
				scalarPrefix, scalarTrace = selectionRowInsertImages(t, conn), selectionRowInsertTrace(t, conn)
				return err
			})
			if wantError == "" {
				if err != nil || scalarErr != nil {
					t.Fatalf("prepared scalar success: prepared=%v scalar=%v", err, scalarErr)
				}
			} else if err == nil || scalarErr == nil || !strings.Contains(err.Error(), wantError) || err.Error() != scalarErr.Error() {
				t.Fatalf("first native error changed: prepared=%v scalar=%v", err, scalarErr)
			}
			if !reflect.DeepEqual(prefix, scalarPrefix) || !reflect.DeepEqual(trace, scalarTrace) ||
				!reflect.DeepEqual(selectionRowInsertImages(t, s.db), selectionRowInsertImages(t, reference.db)) {
				t.Fatal("single-row Flush changed trigger-visible prefix or durable images")
			}
			if len(work.execs) != wantExec || len(work.prepares) != wantPrepares || !reflect.DeepEqual(work.prepares, work.closes) || work.queries != 0 || work.direct != 0 {
				t.Fatalf("prepared scalar lifecycle: %#v", work)
			}
			for _, execution := range work.execs {
				if execution.bindings != 17 && execution.bindings != 10 && execution.bindings != 18 {
					t.Fatal("supported scalar path coalesced a statement")
				}
			}
		})
	}
}

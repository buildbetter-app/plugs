package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Count actual successful native source-observation query executions, including
// misses. This measures SQL dispatch, not executor row visits or cache hits.
type selectionStageConnector struct {
	driver         sqldriver.Driver
	dsn            string
	queries        atomic.Int64
	cancelTriggers atomic.Int64
	nativeAttempts atomic.Int64
	canceledNative atomic.Int64
	mu             sync.Mutex
	cancel         context.CancelFunc
	batchPrepares  atomic.Int64
	batchAttempts  atomic.Int64
	batchQueries   atomic.Int64
	batchRows      atomic.Int64
	batchCloses    atomic.Int64
	batchMaxKeys   atomic.Int64
	batchCanceled  atomic.Int64
	batchCancel    context.CancelFunc
	batchCloseErr  error
	family         selectionFamilyNativeWork
}

type selectionFamilyNativeWork struct {
	prepares, attempts, queries, scalar, rows, closes, maxKeys, canceled atomic.Int64
	cancel                                                               context.CancelFunc // connector.mu
	closeErr                                                             error              // connector.mu
}

func (c *selectionStageConnector) Driver() sqldriver.Driver { return c.driver }
func (c *selectionStageConnector) Connect(context.Context) (sqldriver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &selectionStageConn{Conn: conn, counts: c}, nil
}

type selectionStageConn struct {
	sqldriver.Conn
	counts *selectionStageConnector
}

func (c *selectionStageConn) BeginTx(ctx context.Context, options sqldriver.TxOptions) (sqldriver.Tx, error) {
	return c.Conn.(sqldriver.ConnBeginTx).BeginTx(ctx, options)
}

func (c *selectionStageConn) PrepareContext(ctx context.Context, query string) (sqldriver.Stmt, error) {
	var statement sqldriver.Stmt
	var err error
	if conn, ok := c.Conn.(sqldriver.ConnPrepareContext); ok {
		statement, err = conn.PrepareContext(ctx, query)
	} else {
		statement, err = c.Conn.Prepare(query)
	}
	if err == nil && strings.HasPrefix(query, "SELECT 1 FROM selection_fact_sets f INDEXED BY selection_fact_sets_current_row") {
		c.counts.family.prepares.Add(1)
		return &selectionFamilyOldRowStatement{Stmt: statement, counts: c.counts}, nil
	}
	if err != nil || !strings.HasPrefix(query, "WITH requested(ordinal,key) AS (VALUES ") {
		return statement, err
	}
	c.counts.batchPrepares.Add(1)
	return &selectionStageBatchStatement{Stmt: statement, counts: c.counts}, nil
}

type selectionFamilyOldRowStatement struct {
	sqldriver.Stmt
	counts *selectionStageConnector
}

func (s *selectionFamilyOldRowStatement) QueryContext(ctx context.Context, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	w := &s.counts.family
	w.attempts.Add(1)
	keys := int64(len(args) - 3)
	for old := w.maxKeys.Load(); old < keys; old = w.maxKeys.Load() {
		if w.maxKeys.CompareAndSwap(old, keys) {
			break
		}
	}
	s.counts.mu.Lock()
	cancel := w.cancel
	w.cancel = nil
	s.counts.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	rows, err := s.Stmt.(sqldriver.StmtQueryContext).QueryContext(ctx, args)
	if errors.Is(err, context.Canceled) {
		w.canceled.Add(1)
	}
	if err != nil {
		return rows, err
	}
	w.queries.Add(1)
	return &selectionFamilyOldRowRows{Rows: rows, work: w}, nil
}

func (s *selectionFamilyOldRowStatement) Close() error {
	err := s.Stmt.Close()
	s.counts.family.closes.Add(1)
	s.counts.mu.Lock()
	closeErr := s.counts.family.closeErr
	s.counts.mu.Unlock()
	if err == nil {
		err = closeErr
	}
	return err
}

type selectionFamilyOldRowRows struct {
	sqldriver.Rows
	work *selectionFamilyNativeWork
}

func (r *selectionFamilyOldRowRows) Next(values []sqldriver.Value) error {
	err := r.Rows.Next(values)
	if err == nil {
		r.work.rows.Add(1)
	}
	return err
}

type selectionStageBatchStatement struct {
	sqldriver.Stmt
	counts *selectionStageConnector
}

func (s *selectionStageBatchStatement) QueryContext(ctx context.Context, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	s.counts.batchAttempts.Add(1)
	keys := int64(len(args) - 1)
	for old := s.counts.batchMaxKeys.Load(); old < keys; old = s.counts.batchMaxKeys.Load() {
		if s.counts.batchMaxKeys.CompareAndSwap(old, keys) {
			break
		}
	}
	s.counts.mu.Lock()
	cancel := s.counts.batchCancel
	s.counts.batchCancel = nil
	s.counts.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	rows, err := s.Stmt.(sqldriver.StmtQueryContext).QueryContext(ctx, args)
	if errors.Is(err, context.Canceled) {
		s.counts.batchCanceled.Add(1)
	}
	if err != nil {
		return rows, err
	}
	s.counts.batchQueries.Add(1)
	return &selectionStageBatchRows{Rows: rows, counts: s.counts}, nil
}

func (s *selectionStageBatchStatement) Close() error {
	err := s.Stmt.Close()
	s.counts.batchCloses.Add(1)
	s.counts.mu.Lock()
	closeErr := s.counts.batchCloseErr
	s.counts.mu.Unlock()
	if err == nil {
		err = closeErr
	}
	return err
}

type selectionStageBatchRows struct {
	sqldriver.Rows
	counts *selectionStageConnector
}

func (r *selectionStageBatchRows) Next(values []sqldriver.Value) error {
	err := r.Rows.Next(values)
	if err == nil {
		r.counts.batchRows.Add(1)
	}
	return err
}

func (c *selectionStageConn) ExecContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	return c.Conn.(sqldriver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *selectionStageConn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	sourceQuery := strings.Contains(query, "FROM key_history WHERE key=? AND rev<=?")
	var cancel context.CancelFunc
	if sourceQuery {
		c.counts.mu.Lock()
		cancel = c.counts.cancel
		c.counts.cancel = nil
		c.counts.mu.Unlock()
	}
	// Cancel only after validation reaches the native query boundary. A
	// cancellation after QueryContext returned could race a completed miss.
	// The actual driver still executes its context handling and supplies the
	// error; this observer neither fabricates nor replaces that error.
	if cancel != nil {
		c.counts.cancelTriggers.Add(1)
		cancel()
		c.counts.nativeAttempts.Add(1)
	}
	rows, err := c.Conn.(sqldriver.QueryerContext).QueryContext(ctx, query, args)
	if err == nil && sourceQuery {
		c.counts.queries.Add(1)
	}
	if err == nil && strings.HasPrefix(query, "SELECT f.source_key,f.kind FROM selection_fact_sets f") {
		c.counts.family.scalar.Add(1)
	}
	if cancel != nil && errors.Is(err, context.Canceled) {
		c.counts.canceledNative.Add(1)
	}
	return rows, err
}

func selectionStageStore(t *testing.T, ctx context.Context) (*Store, *selectionStageConnector) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stage-work.db")
	s := openStore(t, ctx, path)
	u := url.URL{Scheme: "file", Path: path}
	counts := &selectionStageConnector{driver: s.db.Driver(),
		dsn: u.String() + "?_pragma=busy_timeout(0)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)"}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s.db = sql.OpenDB(counts)
	return s, counts
}

func selectionStageBead(incarnation string, ref model.Ref, body []byte) driver.DerivedRow {
	key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: ref.ID}
	value := []byte(`{"projection":true}`)
	return driver.DerivedRow{Key: key.Key, Value: value, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: key, Incarnation: incarnation,
		Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
		Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref},
			Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)},
		Bead: &driver.SelectionBead{Ref: ref, Type: "task"},
	}}
}

func TestSelectionStageValidationExecutedWork(t *testing.T) {
	for _, population := range []int{1000, 2000} {
		t.Run(fmt.Sprintf("sources_%d", population), func(t *testing.T) {
			ctx := testContext(t)
			s, counts := selectionStageStore(t, ctx)
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			stop := renewNativeDetachLease(t, ctx, lease)
			defer stop()
			refs := make([]model.Ref, population)
			plans := make([]*pipeline.SourceProjectionPlan, population)
			extra := make([]driver.LockKey, 0, 4*population)
			sentinels := make([]driver.Sentinel, 0, 3*population)
			for i := range refs {
				refs[i] = testRef(fmt.Sprintf("stage-work-%04d", i))
				body := []byte(fmt.Sprintf(`{"id":%q,"type":"task","fields":{"status":"todo"}}`, refs[i].ID))
				plan, err := pipeline.PrepareSourceRecord(driver.Record{Ref: refs[i], Kind: "bead", Body: body})
				if err != nil {
					t.Fatal(err)
				}
				plans[i] = plan
				for _, key := range plan.LockKeys() {
					if key.Kind == driver.LockDerived {
						extra = append(extra, key)
					}
				}
				closure, err := driver.NewClosureSentinel(refs[i].ID)
				if err != nil {
					t.Fatal(err)
				}
				extra = append(extra, driver.LockKey{Kind: driver.LockSentinel, Sentinel: closure})
				sentinels = append(sentinels, plan.Sentinels()...)
			}
			tx, header := beginSourceTestWithLocks(t, s, lease, extra, true, refs...)
			for _, ref := range refs {
				header.Changes = append(header.Changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
			}
			counts.queries.Store(0)
			started := time.Now()
			for _, plan := range plans {
				if err := plan.Publish(ctx, tx); err != nil {
					t.Fatal(err)
				}
			}
			elapsed, queries := time.Since(started), counts.queries.Load()
			// The actual producer observes then stages each source, followed by
			// three typed publications. Independent earlier sources add no work.
			if queries != int64(5*population) {
				t.Fatalf("interleaved product publication executed %d native source queries for %d sources; want five per source", queries, population)
			}
			if err := tx.BumpSentinels(ctx, sentinels); err != nil {
				t.Fatal(err)
			}
			counts.queries.Store(0)
			rows, err := plans[0].SelectionRows(header.Incarnation, model.Natural64(0))
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.PutSelectionDerived(ctx, driver.DerivedSortKey, nil, rows[:1]); err != nil {
				t.Fatal(err)
			}
			if got := counts.queries.Load(); got != 1 {
				t.Fatalf("one replacement repeated unrelated source queries: %d", got)
			}
			if _, err := tx.Commit(ctx, header); err != nil {
				t.Fatal(err)
			}
			wantBatches := int64((population + 127) / 128)
			if counts.batchQueries.Load() != wantBatches || counts.batchRows.Load() != int64(population) ||
				counts.batchPrepares.Load() != 2 || counts.batchCloses.Load() != 2 || counts.batchMaxKeys.Load() != 128 {
				t.Fatalf("Commit native history queries=%d rows=%d prepares=%d closes=%d max_keys=%d; want %d/%d/2/2/128",
					counts.batchQueries.Load(), counts.batchRows.Load(), counts.batchPrepares.Load(), counts.batchCloses.Load(), counts.batchMaxKeys.Load(), wantBatches, population)
			}
			// Source publication supplies attribute and reference fact families.
			// This fixture does not publish the canonical I1 ancestor family.
			if counts.family.queries.Load() != 2*wantBatches || counts.family.scalar.Load() != 0 || counts.family.rows.Load() != 0 ||
				counts.family.prepares.Load() != 4 || counts.family.closes.Load() != 4 || counts.family.maxKeys.Load() != 128 {
				t.Fatalf("Commit family all-miss work: queries=%d scalar=%d rows=%d prepares=%d closes=%d max=%d; want %d/0/0/4/4/128",
					counts.family.queries.Load(), counts.family.scalar.Load(), counts.family.rows.Load(), counts.family.prepares.Load(), counts.family.closes.Load(), counts.family.maxKeys.Load(), 2*wantBatches)
			}
			var durable int
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_bead_history WHERE end_revision IS NULL`).Scan(&durable); err != nil || durable != population {
				t.Fatalf("durable typed population=%d want=%d: %v", durable, population, err)
			}
			t.Logf("population=%d interleaved_product_sources=%d successful_native_source_queries=%d stage_elapsed_ns=%d one_replacement_queries=1 durable_current_rows=%d executor_row_visits=UNMEASURED", population, population, queries, elapsed.Nanoseconds(), durable)
		})
	}
}

// These fixtures seed real temporal rows, then compare the new observer with
// the existing scalar reader on the same pinned SQLite transaction. They do
// not use acquired-lock images as observation answers.
func selectionValidationReader(t *testing.T, ctx context.Context, s *Store, at uint64) *transaction {
	t.Helper()
	meta := getMeta(t, ctx, s)
	readTx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = readTx.Rollback() })
	// Pin the real read view before any observations or caller-side mutations.
	var head string
	if err := readTx.QueryRowContext(ctx, `SELECT head FROM meta WHERE singleton=1`).Scan(&head); err != nil {
		t.Fatal(err)
	}
	tx := &transaction{reader: &reader{store: s, tx: readTx, meta: meta, at: at},
		records: make(map[string]*driver.Record), derived: make(map[string]driver.LockState), bumps: make(map[string]driver.LockKey)}
	tx.reader.owner = tx
	return tx
}

func selectionValidationExec(t *testing.T, ctx context.Context, s *Store, query string, args ...any) {
	t.Helper()
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func selectionValidationJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func selectionValidationRequests(keys []driver.LockKey) map[string]driver.LockState {
	rows := make(map[string]driver.LockState, len(keys))
	for i, key := range keys {
		rows[fmt.Sprint(i)] = driver.LockState{Present: true, Derived: &driver.DerivedRow{
			Index: &driver.SelectionIndex{Source: driver.SelectionSource{Key: key}}}}
	}
	return rows
}

func TestSelectionValidationScalarParityAndPinnedHistory(t *testing.T) {
	for _, at := range []uint64{5, 10} {
		t.Run(fmt.Sprintf("at_%d", at), func(t *testing.T) {
			ctx := testContext(t)
			s, counts := selectionStageStore(t, ctx)
			selectionValidationExec(t, ctx, s, `UPDATE meta SET head=? WHERE singleton=1`, revKey(10))
			keys := make([]driver.LockKey, 260)
			for i := range keys {
				ref := testRef(fmt.Sprintf("validation-%03d", i))
				key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
				if i%6 >= 3 {
					row := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: fmt.Sprintf("opaque/%03d;:", i)}
					if i == 3 {
						row.Key += string([]byte{0xff, 0xfe})
					}
					key = driver.LockKey{Kind: driver.LockDerived, Derived: &row}
				}
				if i%6 == 5 {
					sentinel, err := driver.NewClosureSentinel(fmt.Sprintf("validation-sentinel-%03d", i))
					if err != nil {
						t.Fatal(err)
					}
					key = driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
				}
				if _, err := driver.EncodeLockKey(key); err != nil {
					t.Fatal(err)
				}
				keys[i] = key
				if i%6 == 2 { // no history or record: absence remains revision zero
					continue
				}
				old := driver.LockState{Key: key, Revision: model.Natural64(2), Present: i%6 != 4}
				if key.Kind == driver.LockRecord {
					body := []byte(fmt.Sprintf(`{"version":2,"id":%q}`, ref.ID))
					old.Record = &driver.Record{Ref: ref, Kind: "bead", Body: body, Revision: old.Revision}
					selectionValidationExec(t, ctx, s, `INSERT INTO objects(key,kind) VALUES(?,?)`, keyID(key), "bead")
					v := driver.Version{Ref: ref, Revision: old.Revision, Base: true, Body: body, Principal: model.PrincipalRef{Kind: "system", ID: "validation-history"}}
					selectionValidationExec(t, ctx, s, `INSERT INTO versions(key,rev,base,data) VALUES(?,?,1,?)`, keyID(key), revKey(2), selectionValidationJSON(t, v))
				} else if old.Present && key.Kind == driver.LockDerived {
					old.Derived = &driver.DerivedRow{Key: key.Derived.Key, Value: []byte(`{"version":2}`)}
				}
				selectionValidationExec(t, ctx, s, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(key), revKey(2), selectionValidationJSON(t, old))
				newer := old
				newer.Revision = model.Natural64(7)
				if key.Kind == driver.LockRecord {
					v := driver.Version{Ref: ref, Revision: newer.Revision, Base: true, Body: []byte(`{"version":7}`), Principal: model.PrincipalRef{Kind: "system", ID: "validation-history"}}
					if i%6 == 1 {
						v.Deleted, newer.Present, newer.Record = true, false, nil
					} else {
						newer.Record = &driver.Record{Ref: ref, Kind: "bead", Body: v.Body, Revision: newer.Revision}
						selectionValidationExec(t, ctx, s, `INSERT INTO records(key,data) VALUES(?,?)`, keyID(key), selectionValidationJSON(t, newer.Record))
					}
					selectionValidationExec(t, ctx, s, `INSERT INTO versions(key,rev,base,data) VALUES(?,?,1,?)`, keyID(key), revKey(7), selectionValidationJSON(t, v))
				}
				selectionValidationExec(t, ctx, s, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(key), revKey(7), selectionValidationJSON(t, newer))
			}
			tx := selectionValidationReader(t, ctx, s, at)
			for i, key := range keys {
				id := keyID(key)
				switch i % 12 {
				case 0:
					tx.records[id] = &driver.Record{Ref: *key.Ref, Kind: "bead", Body: []byte(`{"staged":true}`)}
				case 1:
					tx.records[id] = nil
				case 3:
					row := selectionStageBead("clone-incarnation", testRef("clone-source"), []byte(`{}`))
					row.Key = key.Derived.Key
					tx.derived[id] = driver.LockState{Key: key, Present: true, Derived: &row}
				case 4:
					tx.derived[id] = driver.LockState{Key: key}
				case 5:
					tx.bumps[id] = key // scalar observe intentionally retains history revision
				}
			}
			// A later durable history mutation must not replace this pinned view,
			// even if a source has not yet been requested by the observer.
			changed := driver.LockState{Key: keys[8], Revision: model.Natural64(4), Present: true}
			selectionValidationExec(t, ctx, s, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(keys[8]), revKey(4), selectionValidationJSON(t, changed))
			history := newSelectionValidationHistory(tx, selectionValidationRequests(keys))
			for _, key := range keys {
				want, err := tx.observe(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				got, err := history.observe(ctx, key)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("scalar parity for %q at %d: got=%#v want=%#v err=%v", keyID(key), at, got, want, err)
				}
				if got.Derived != nil && got.Derived.Index != nil {
					got.Derived.Value[0] = '!'
					got.Derived.Index.Bead.Type = "mutated"
					got.Derived.Index.Source.Key.Ref.ID = "mutated"
					again, err := history.observe(ctx, key)
					if err != nil || !reflect.DeepEqual(again, want) {
						t.Fatalf("cached observation aliases derived overlay: %v", err)
					}
				}
			}
			var err error
			history.close(&err)
			if err != nil || counts.batchQueries.Load() != 3 || counts.batchRows.Load() != 260 || counts.batchPrepares.Load() != 2 || counts.batchCloses.Load() != 2 || counts.batchMaxKeys.Load() != 128 {
				t.Fatalf("native parity work: queries=%d rows=%d prepares=%d closes=%d max=%d err=%v", counts.batchQueries.Load(), counts.batchRows.Load(), counts.batchPrepares.Load(), counts.batchCloses.Load(), counts.batchMaxKeys.Load(), err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := history.observe(cancelled, keys[0]); !errors.Is(err, context.Canceled) || counts.batchQueries.Load() != 3 {
				t.Fatalf("cached history bypassed cancellation or queried again: %v", err)
			}
		})
	}
}

func TestSelectionValidationMissingAndMalformedHistory(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, []byte(`{"broken":`)} {
		name := fmt.Sprintf("bytes_%d", len(payload))
		if payload == nil {
			name = "missing"
		}
		t.Run(name, func(t *testing.T) {
			ctx := testContext(t)
			s, counts := selectionStageStore(t, ctx)
			goodRow := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "good-first"}
			badRow := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "bad-later"}
			good := driver.LockKey{Kind: driver.LockDerived, Derived: &goodRow}
			bad := driver.LockKey{Kind: driver.LockDerived, Derived: &badRow}
			if payload != nil {
				selectionValidationExec(t, ctx, s, `INSERT INTO key_history(key,rev,data) VALUES(?,?,?)`, keyID(bad), revKey(0), payload)
			}
			tx := selectionValidationReader(t, ctx, s, 0)
			history := newSelectionValidationHistory(tx, selectionValidationRequests([]driver.LockKey{good, bad}))
			if state, err := history.observe(ctx, good); err != nil || state.Present || !state.Revision.IsZero() {
				t.Fatalf("later malformed history evaluated before requested source: %#v %v", state, err)
			}
			want, scalarErr := tx.observe(ctx, bad)
			got, err := history.observe(ctx, bad)
			if !reflect.DeepEqual(got, want) || reflect.TypeOf(err) != reflect.TypeOf(scalarErr) || fmt.Sprint(err) != fmt.Sprint(scalarErr) {
				t.Fatalf("scalar error parity: got=%#v err=%v want=%#v err=%v", got, err, want, scalarErr)
			}
			if payload != nil && err == nil {
				t.Fatal("stored empty/malformed history was treated as absence")
			}
			prior := err
			counts.mu.Lock()
			counts.batchCloseErr = io.ErrClosedPipe
			counts.mu.Unlock()
			history.close(&err)
			if prior != nil && err != prior || prior == nil && !errors.Is(err, io.ErrClosedPipe) || counts.batchQueries.Load() != 1 || counts.batchCloses.Load() != 1 {
				t.Fatalf("history/cleanup precedence: prior=%v final=%v queries=%d closes=%d", prior, err, counts.batchQueries.Load(), counts.batchCloses.Load())
			}
		})
	}
}

func TestSelectionValidationGuardsAndScalarStage(t *testing.T) {
	ctx := testContext(t)
	s, counts := selectionStageStore(t, ctx)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	ref := testRef("validation-guards")
	key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: ref.ID}
	lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
	tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{lock}, true, ref)
	body := []byte(`{"id":"validation-guards","type":"task"}`)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: body}, nil, nil); err != nil {
		t.Fatal(err)
	}
	row := selectionStageBead(header.Incarnation, ref, body)
	state := driver.LockState{Key: lock, Present: true, Derived: &row}
	rows := map[string]driver.LockState{keyID(lock): state}
	counts.queries.Store(0)
	if err := tx.validateSelectionRows(ctx, rows); err != nil || counts.queries.Load() != 1 || counts.batchPrepares.Load() != 0 {
		t.Fatalf("staging stopped using scalar observation: %v scalar_queries=%d prepares=%d", err, counts.queries.Load(), counts.batchPrepares.Load())
	}
	previous := tx.derived
	for _, name := range []string{"valid", "incarnation", "lock_census", "revision", "present", "digest", "bead_kind"} {
		t.Run(name, func(t *testing.T) {
			candidate := row.Clone()
			before := counts.batchQueries.Load()
			want := ""
			switch name {
			case "incarnation":
				candidate.Index.Incarnation, want = "other-incarnation", "incarnation mismatch"
			case "lock_census":
				other := testRef("not-acquired")
				candidate.Index.Source.Key.Ref, want = &other, "absent from lock census"
			case "revision":
				candidate.Index.Source.Revision, want = model.Natural64(1), "observation changed"
			case "present":
				candidate.Index.Source.Present, want = false, "observation changed"
			case "digest":
				candidate.Index.Source.Digest, want = "stale-digest", "digest changed"
			case "bead_kind":
				tx.records[keyID(candidate.Index.Source.Key)].Kind, want = "memory", "bead source kind"
				defer func() { tx.records[keyID(candidate.Index.Source.Key)].Kind = "bead" }()
			}
			proposed := map[string]driver.LockState{keyID(lock): {Key: lock, Present: true, Derived: &candidate}}
			err := tx.validateSelectionWrites(ctx, proposed)
			if want == "" && err != nil || want != "" && (!errors.Is(err, driver.ErrUnsupported) || !strings.Contains(err.Error(), want)) {
				t.Fatalf("%s guard: got %v want %q", name, err, want)
			}
			if name == "incarnation" || name == "lock_census" {
				if counts.batchQueries.Load() != before {
					t.Fatal("source query ran before the original guard")
				}
			} else if counts.batchQueries.Load() != before+1 {
				t.Fatal("final validation bypassed the pinned history query")
			}
			if !reflect.DeepEqual(tx.derived, previous) {
				t.Fatal("validation replaced the original overlay")
			}
		})
	}
	before := counts.batchPrepares.Load()
	if err := tx.validateSelectionWrites(ctx, nil); err != nil || counts.batchPrepares.Load() != before {
		t.Fatalf("empty overlay prepared or failed: %v", err)
	}
}

func TestSelectionValidationNativeCancellationAndErrors(t *testing.T) {
	t.Run("cancel_at_prepared_query_then_recover", func(t *testing.T) {
		ctx := testContext(t)
		s, counts := selectionStageStore(t, ctx)
		key := driver.GraphBlockingSentinelLock
		tx := selectionValidationReader(t, ctx, s, 0)
		history := newSelectionValidationHistory(tx, selectionValidationRequests([]driver.LockKey{key}))
		call, cancel := context.WithCancel(ctx)
		defer cancel()
		counts.mu.Lock()
		counts.batchCancel, counts.batchCloseErr = cancel, io.ErrClosedPipe
		counts.mu.Unlock()
		_, err := history.observe(call, key)
		if !errors.Is(err, context.Canceled) || counts.batchAttempts.Load() != 1 || counts.batchQueries.Load() != 0 || counts.batchCanceled.Load() != 1 {
			t.Fatalf("native prepared-query cancellation attempts=%d successes=%d canceled=%d err=%v", counts.batchAttempts.Load(), counts.batchQueries.Load(), counts.batchCanceled.Load(), err)
		}
		history.close(&err)
		if !errors.Is(err, context.Canceled) || counts.batchCloses.Load() != 1 {
			t.Fatalf("cleanup masked native cancellation: %v", err)
		}
		counts.mu.Lock()
		counts.batchCloseErr = nil
		counts.mu.Unlock()
		recovered := newSelectionValidationHistory(tx, selectionValidationRequests([]driver.LockKey{key}))
		if _, err := recovered.observe(ctx, key); err != nil {
			t.Fatalf("canceled observation damaged the pinned reader: %v", err)
		}
		var cleanup error
		recovered.close(&cleanup)
		if cleanup != nil || counts.batchPrepares.Load() != 2 || counts.batchCloses.Load() != 2 || counts.batchQueries.Load() != 1 {
			t.Fatalf("recovery lifecycle: prepares=%d closes=%d queries=%d err=%v", counts.batchPrepares.Load(), counts.batchCloses.Load(), counts.batchQueries.Load(), cleanup)
		}
	})
	t.Run("native_missing_table_error_without_scalar_retry", func(t *testing.T) {
		ctx := testContext(t)
		s, counts := selectionStageStore(t, ctx)
		selectionValidationExec(t, ctx, s, `DROP TABLE key_history`)
		key := driver.GraphBlockingSentinelLock
		tx := selectionValidationReader(t, ctx, s, 0)
		history := newSelectionValidationHistory(tx, selectionValidationRequests([]driver.LockKey{key}))
		counts.mu.Lock()
		counts.batchCloseErr = io.ErrClosedPipe
		counts.mu.Unlock()
		_, err := history.observe(ctx, key)
		if err == nil || !strings.Contains(err.Error(), "no such table") {
			t.Fatalf("native SQL error became a miss: %v", err)
		}
		prior := err
		history.close(&err)
		if err != prior || counts.queries.Load() != 0 || counts.batchQueries.Load() != 0 || counts.batchCloses.Load() != counts.batchPrepares.Load() {
			t.Fatalf("native error was replaced/retried or leaked a handle: %v", err)
		}
	})
}

func TestSelectionValidationCommitCleanupRollsBackAndRecovers(t *testing.T) {
	ctx := testContext(t)
	s, counts := selectionStageStore(t, ctx)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	ref := testRef("validation-cleanup")
	body := []byte(`{"id":"validation-cleanup","type":"task","fields":{"status":"todo"}}`)
	plan, err := pipeline.PrepareSourceRecord(driver.Record{Ref: ref, Kind: "bead", Body: body})
	if err != nil {
		t.Fatal(err)
	}
	var extra []driver.LockKey
	for _, key := range plan.LockKeys() {
		if key.Kind == driver.LockDerived {
			extra = append(extra, key)
		}
	}
	closure, err := driver.NewClosureSentinel(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	extra = append(extra, driver.LockKey{Kind: driver.LockSentinel, Sentinel: closure})
	tx, header := beginSourceTestWithLocks(t, s, lease, extra, true, ref)
	header.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)}}
	if err := plan.Publish(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, plan.Sentinels()); err != nil {
		t.Fatal(err)
	}
	counts.mu.Lock()
	counts.batchCloseErr = io.ErrClosedPipe
	counts.mu.Unlock()
	if _, err := tx.Commit(ctx, header); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("final validation ignored prepared-statement close error: %v", err)
	}
	if !getMeta(t, ctx, s).Head.IsZero() {
		t.Fatal("failed validation advanced Head")
	}
	for _, table := range []string{"records", "versions", "key_history", "selection_bead_history", "selection_fact_sets", "changesets"} {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("failed validation published %s rows=%d: %v", table, count, err)
		}
	}
	counts.mu.Lock()
	counts.batchCloseErr = nil
	counts.mu.Unlock()
	if _, err := tx.Commit(ctx, header); err != nil {
		t.Fatalf("rolled-back validation did not recover: %v", err)
	}
	read, err := s.Read(ctx, driver.View{Incarnation: header.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	got, err := read.Get(ctx, []model.Ref{ref})
	if err != nil || len(got) != 1 || !bytes.Equal(got[0].Body, body) || getMeta(t, ctx, s).Head.String() != "1" {
		t.Fatalf("recovery failed durable image parity: %#v %v", got, err)
	}
	if counts.batchPrepares.Load() != 2 || counts.batchCloses.Load() != 2 || counts.batchQueries.Load() != 2 {
		t.Fatalf("Commit cleanup lifecycle: prepares=%d closes=%d queries=%d", counts.batchPrepares.Load(), counts.batchCloses.Load(), counts.batchQueries.Load())
	}
}

func selectionFamilyOldRowRequests(count int) map[string]driver.LockState {
	rows := make(map[string]driver.LockState, count)
	for i := 0; i < count; i++ {
		key := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: fmt.Sprintf("family-old-row/%04d", i)}
		if i == 0 {
			key.Key += string([]byte{0xff})
		}
		lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
		rows[keyID(lock)] = driver.LockState{Key: lock, Present: true, Derived: &driver.DerivedRow{Key: key.Key, Value: []byte(`{}`)}}
	}
	return rows
}

func TestSelectionFamilyOldRowMissBatches(t *testing.T) {
	for _, population := range []int{0, 1, 128, 129, 258, 1000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			ctx := testContext(t)
			s, counts := selectionStageStore(t, ctx)
			meta := getMeta(t, ctx, s)
			family := sqliteSelectionFamilies()[1]
			owner := &transaction{records: map[string]*driver.Record{}}
			rows := selectionFamilyOldRowRequests(population)
			err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				// The wrapper deliberately retains the scalar path on the same
				// connection/image; it supplies no fabricated observation result.
				scalar := &selectionCountingQueryer{conn: c}
				want, err := owner.selectionFamilyCompleteForCommit(ctx, scalar, meta.Incarnation, family, rows)
				if err != nil || !want || scalar.rowQueries != population {
					return fmt.Errorf("scalar fixture: complete=%t queries=%d err=%v", want, scalar.rowQueries, err)
				}
				counts.family.scalar.Store(0)
				got, err := owner.selectionFamilyCompleteForCommit(ctx, c, meta.Incarnation, family, rows)
				if err != nil || got != want {
					return fmt.Errorf("native batch parity: complete=%t want=%t err=%v", got, want, err)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			queries, scalar, prepares := int64((population+127)/128), int64(0), int64(0)
			if population >= 128 {
				prepares++
			}
			if population%128 > 1 {
				prepares++
			}
			if population%128 == 1 {
				scalar = 1 // the final single key retains its original scalar call
			}
			if counts.family.queries.Load()+counts.family.scalar.Load() != queries || counts.family.scalar.Load() != scalar ||
				counts.family.prepares.Load() != prepares || counts.family.closes.Load() != prepares || counts.family.rows.Load() != 0 || counts.family.maxKeys.Load() > 128 {
				t.Fatalf("native miss work queries=%d scalar=%d prepares=%d closes=%d rows=%d max=%d; want total=%d scalar=%d prepares=%d",
					counts.family.queries.Load(), counts.family.scalar.Load(), counts.family.prepares.Load(), counts.family.closes.Load(), counts.family.rows.Load(), counts.family.maxKeys.Load(), queries, scalar, prepares)
			}
		})
	}
	t.Run("pinned_sql_tx_keeps_scalar_path", func(t *testing.T) {
		ctx := testContext(t)
		s, counts := selectionStageStore(t, ctx)
		meta := getMeta(t, ctx, s)
		readTx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer readTx.Rollback()
		owner := &transaction{records: map[string]*driver.Record{}}
		complete, err := owner.selectionFamilyCompleteForCommit(ctx, readTx, meta.Incarnation, sqliteSelectionFamilies()[1], selectionFamilyOldRowRequests(129))
		if err != nil || !complete || counts.family.scalar.Load() != 129 || counts.family.prepares.Load() != 0 {
			t.Fatalf("staged-reader path changed: complete=%t scalar=%d prepares=%d err=%v", complete, counts.family.scalar.Load(), counts.family.prepares.Load(), err)
		}
	})
}

func TestSelectionFamilyOldRowPredicateAndPositiveHitParity(t *testing.T) {
	for _, fixture := range []string{"incarnation", "branch", "row_table", "row_key", "closed", "nonbead", "missing_object", "source_range", "wrong_kind", "duplicate_active"} {
		t.Run(fixture, func(t *testing.T) {
			ctx := testContext(t)
			s, counts := selectionStageStore(t, ctx)
			meta := getMeta(t, ctx, s)
			family := sqliteSelectionFamilies()[1]
			rows := selectionFamilyOldRowRequests(2)
			var rowKey string
			for id := range rows {
				rowKey = id
				break
			}
			incarnation, branch, table := meta.Incarnation, revKey(0), string(family.Table)
			kind, objectKind, source := string(family.Kind), "bead", "0:family-source-a"
			var end any
			switch fixture {
			case "incarnation":
				incarnation = "other-incarnation"
			case "branch":
				branch = revKey(1)
			case "row_table":
				table = string(driver.DerivedReferenceIndex)
			case "row_key":
				rowKey = "3:attribute:not-requested"
			case "closed":
				end = revKey(2)
			case "nonbead":
				objectKind = "memory"
			case "source_range":
				source = "1:outside-record-domain"
			case "wrong_kind":
				kind = string(driver.SelectionLinkFact)
			}
			if fixture != "missing_object" {
				selectionValidationExec(t, ctx, s, `INSERT INTO objects(key,kind) VALUES(?,?)`, source, objectKind)
			}
			insert := `INSERT INTO selection_fact_sets(incarnation,branch,row_key,row_table,kind,start_revision,end_revision,source_key,source_revision,source_present,source_digest)
				VALUES(?,?,?,?,?,?,?,?,?,?,?)`
			selectionValidationExec(t, ctx, s, insert, incarnation, branch, rowKey, table, kind, revKey(1), end, source, revKey(1), 1, "source-digest")
			if fixture == "duplicate_active" {
				selectionValidationExec(t, ctx, s, `INSERT INTO objects(key,kind) VALUES(?,?)`, "0:family-source-b", "bead")
				selectionValidationExec(t, ctx, s, insert, incarnation, branch, rowKey, table, string(driver.SelectionLinkFact), revKey(2), nil, "0:family-source-b", revKey(2), 1, "other-digest")
			}
			conn, err := s.db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			owner := &transaction{records: map[string]*driver.Record{}}
			scalar := &selectionCountingQueryer{conn: conn}
			want, wantErr := owner.selectionFamilyCompleteForCommit(ctx, scalar, meta.Incarnation, family, rows)
			counts.family.scalar.Store(0)
			got, err := owner.selectionFamilyCompleteForCommit(ctx, conn, meta.Incarnation, family, rows)
			if err != nil || wantErr != nil || got != want {
				t.Fatalf("positive/scalar first-match parity: got=%t err=%v want=%t err=%v", got, err, want, wantErr)
			}
			hit := fixture == "wrong_kind" || fixture == "duplicate_active"
			if hit && (counts.family.rows.Load() != 1 || counts.family.scalar.Load() == 0) ||
				!hit && (counts.family.rows.Load() != 0 || counts.family.scalar.Load() != 0 || !got) ||
				counts.family.queries.Load() != 1 || counts.family.prepares.Load() != 1 || counts.family.closes.Load() != 1 || counts.family.maxKeys.Load() != 2 {
				t.Fatalf("predicate/hit dispatch: fixture=%s hit=%t rows=%d scalar=%d queries=%d prepares=%d closes=%d max=%d", fixture, hit, counts.family.rows.Load(), counts.family.scalar.Load(), counts.family.queries.Load(), counts.family.prepares.Load(), counts.family.closes.Load(), counts.family.maxKeys.Load())
			}
		})
	}
}

func TestSelectionFamilyOldRowNativeFailureRollback(t *testing.T) {
	for _, failure := range []string{"query_cancel", "missing_index", "cleanup"} {
		t.Run(failure, func(t *testing.T) {
			ctx := testContext(t)
			s, counts := selectionStageStore(t, ctx)
			meta := getMeta(t, ctx, s)
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			counts.mu.Lock()
			counts.family.closeErr = io.ErrClosedPipe
			if failure == "query_cancel" {
				counts.family.cancel = cancel
			}
			counts.mu.Unlock()
			owner := &transaction{records: map[string]*driver.Record{}}
			rows := selectionFamilyOldRowRequests(258)
			err := s.immediate(call, nil, func(c *sql.Conn) error {
				if _, err := c.ExecContext(call, `INSERT INTO records(key,data) VALUES('family-rollback-marker',X'7b7d')`); err != nil {
					return err
				}
				if failure == "missing_index" {
					if _, err := c.ExecContext(call, `DROP INDEX selection_fact_sets_current_row`); err != nil {
						return err
					}
				}
				complete, err := owner.selectionFamilyCompleteForCommit(call, c, meta.Incarnation, sqliteSelectionFamilies()[1], rows)
				if complete {
					return fmt.Errorf("failed family observation returned complete")
				}
				return err
			})
			if failure == "query_cancel" && (!errors.Is(err, context.Canceled) || counts.family.canceled.Load() != 1) ||
				failure == "missing_index" && (err == nil || !strings.Contains(err.Error(), "no such index")) ||
				failure == "cleanup" && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("original failure precedence: mode=%s err=%v native_canceled=%d", failure, err, counts.family.canceled.Load())
			}
			if counts.family.scalar.Load() != 0 || counts.family.closes.Load() != counts.family.prepares.Load() {
				t.Fatal("native error retried scalar SQL or leaked a prepared handle")
			}
			var durable int
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM records`).Scan(&durable); err != nil || durable != 0 || !getMeta(t, ctx, s).Head.IsZero() {
				t.Fatalf("failed family observation published partial state: rows=%d err=%v", durable, err)
			}
			counts.mu.Lock()
			counts.family.closeErr = nil
			counts.mu.Unlock()
			if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				complete, err := owner.selectionFamilyCompleteForCommit(ctx, c, meta.Incarnation, sqliteSelectionFamilies()[1], rows)
				if err == nil && !complete {
					return fmt.Errorf("family observation did not recover")
				}
				return err
			}); err != nil {
				t.Fatalf("native rollback/schema/connection recovery: %v", err)
			}
		})
	}
}

func TestSelectionStageValidationDirtyRecordAndFinalCommit(t *testing.T) {
	for _, mutation := range []string{"put", "typed_put", "delete"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := testContext(t)
			s, _ := selectionStageStore(t, ctx)
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			a, b := testRef("dirty-a"), testRef("dirty-b")
			ka := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: a.ID}
			kb := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: b.ID}
			tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &ka}, {Kind: driver.LockDerived, Derived: &kb}}, true, a, b)
			original, changed := []byte(`{"type":"task","title":"a"}`), []byte(`{"type":"task","title":"changed"}`)
			for _, ref := range []model.Ref{a, b} {
				if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: original}, nil, nil); err != nil {
					t.Fatal(err)
				}
				header.Changes = append(header.Changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
			}
			row := selectionStageBead(header.Incarnation, a, original)
			if err := tx.PutSelectionDerived(ctx, ka.Table, nil, []driver.DerivedRow{row}); err != nil {
				t.Fatal(err)
			}
			var err error
			switch mutation {
			case "put":
				err = tx.Put(ctx, driver.Record{Ref: a, Kind: "bead", Body: changed})
			case "typed_put":
				err = tx.PutGraphRecord(ctx, driver.Record{Ref: a, Kind: "bead", Body: changed}, nil, nil)
			case "delete":
				err = tx.Delete(ctx, a)
				header.Changes[0].Op = model.OpDelete
			}
			if err != nil {
				t.Fatal(err)
			}
			other := selectionStageBead(header.Incarnation, b, original)
			if err := tx.PutSelectionDerived(ctx, kb.Table, nil, []driver.DerivedRow{other}); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("unrelated batch accepted stale prior source: %v", err)
			}
			if len(tx.derived) != 1 || !reflect.DeepEqual(tx.derived[keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &ka})].Derived, &row) {
				t.Fatal("rejected batch changed prior staged rows")
			}
			if err := tx.PutSelectionDerived(ctx, kb.Table, nil, nil); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("failed batch discarded dirty source recheck: %v", err)
			}
			if _, err := tx.Commit(ctx, header); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("final Commit accepted stale annotation: %v", err)
			}
			if !getMeta(t, ctx, s).Head.IsZero() {
				t.Fatal("failed final validation published a revision")
			}
			if err := tx.PutGraphRecord(ctx, driver.Record{Ref: a, Kind: "bead", Body: original}, nil, nil); err != nil {
				t.Fatal(err)
			}
			if err := tx.PutSelectionDerived(ctx, kb.Table, nil, []driver.DerivedRow{other}); err != nil {
				t.Fatalf("corrected source could not recover rejected batch: %v", err)
			}
		})
	}
}

func TestSelectionStageValidationDerivedDependenciesAndCancellation(t *testing.T) {
	ctx := testContext(t)
	s, counts := selectionStageStore(t, ctx)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	source := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "opaque-source"}
	dependent := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "typed-dependent"}
	extra := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "typed-extra"}
	tx, header := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{{Kind: driver.LockDerived, Derived: &source}, {Kind: driver.LockDerived, Derived: &dependent}, {Kind: driver.LockDerived, Derived: &extra}}, false)
	a, b := []byte(`{"opaque":"a"}`), []byte(`{"opaque":"b"}`)
	fact := func(key driver.DerivedRowKey, body []byte) driver.DerivedRow {
		value := []byte(`{"facts":[]}`)
		return driver.DerivedRow{Key: key.Key, Value: value, Index: &driver.SelectionIndex{
			Version: driver.SelectionIndexVersion, Row: key, Incarnation: header.Incarnation,
			Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
			Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &source},
				Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)},
			Facts: &driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{}},
		}}
	}
	if err := tx.PutDerived(ctx, source.Table, nil, []driver.DerivedRow{{Key: source.Key, Value: a}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutSelectionDerived(ctx, dependent.Table, nil, []driver.DerivedRow{fact(dependent, a)}); err != nil {
		t.Fatal(err)
	}
	sourceID := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &source})
	if err := tx.PutDerived(ctx, source.Table, nil, []driver.DerivedRow{{Key: source.Key, Value: b}}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("raw source change bypassed prior dependent: %v", err)
	}
	if got := string(tx.derived[sourceID].Derived.Value); got != string(a) {
		t.Fatalf("failed source replacement retained %s", got)
	}
	// Both arms must be checked against the complete proposed batch. The new
	// dependent binds the new opaque bytes even if its map entry is visited first.
	if err := tx.PutSelectionDerived(ctx, source.Table, []driver.DerivedRow{{Key: source.Key}}, []driver.DerivedRow{fact(dependent, b)}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("deleted source accepted a present-source dependent: %v", err)
	}
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	counts.mu.Lock()
	counts.cancel = cancel
	counts.mu.Unlock()
	if err := tx.PutSelectionDerived(call, extra.Table, nil, []driver.DerivedRow{fact(extra, a)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("native observation cancellation not propagated: %v", err)
	}
	if triggers, attempts, canceled := counts.cancelTriggers.Load(), counts.nativeAttempts.Load(), counts.canceledNative.Load(); triggers != 1 || attempts != 1 || canceled != 1 {
		t.Fatalf("native query entry cancellation triggers=%d native_attempts=%d context_canceled=%d; want exactly one of each", triggers, attempts, canceled)
	}
	if len(tx.derived) != 2 || string(tx.derived[sourceID].Derived.Value) != string(a) {
		t.Fatal("canceled proposed batch leaked staged state")
	}
	if err := tx.PutSelectionDerived(ctx, extra.Table, nil, []driver.DerivedRow{fact(extra, a)}); err != nil {
		t.Fatalf("canceled batch damaged source/dependency recovery: %v", err)
	}
	if err := tx.PutDerived(ctx, source.Table, []driver.DerivedRow{{Key: source.Key}}, nil); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("deletion bypassed previously accepted dependents: %v", err)
	}
	if err := tx.PutDerived(ctx, source.Table, []driver.DerivedRow{{Key: dependent.Key}, {Key: extra.Key}}, []driver.DerivedRow{{Key: source.Key, Value: b}}); err != nil {
		t.Fatalf("same-batch dependent removals and source replacement failed: %v", err)
	}
	if err := tx.PutSelectionDerived(ctx, dependent.Table, nil, []driver.DerivedRow{fact(dependent, b)}); err != nil {
		t.Fatalf("replacement source could not bind a new dependent: %v", err)
	}
	if err := tx.PutDerived(ctx, source.Table, []driver.DerivedRow{{Key: source.Key}, {Key: dependent.Key}, {Key: extra.Key}}, nil); err != nil {
		t.Fatalf("complete dependent/source deletion failed: %v", err)
	}
	t.Logf("native_query_entry_cancel_triggers=%d native_query_attempts=%d native_context_canceled=%d rollback_and_dependency_recovery=PASS", counts.cancelTriggers.Load(), counts.nativeAttempts.Load(), counts.canceledNative.Load())
}

func TestSelectionStageValidationNonDataSourceBumps(t *testing.T) {
	ctx := testContext(t)
	s, counts := selectionStageStore(t, ctx)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	// Keep the donor's exact read-fact identity on the retained W4 base,
	// which predates the exported semantic graph-certificate constant.
	fact, err := driver.NewReadFact("core/graph-validity/v1")
	if err != nil {
		t.Fatal(err)
	}
	sources := []driver.LockKey{driver.GraphBlockingSentinelLock, {Kind: driver.LockReadFact, Fact: fact}}
	keys := []driver.DerivedRowKey{{Table: driver.DerivedAttribute, Key: "sentinel-source"}, {Table: driver.DerivedAttribute, Key: "read-fact-source"}}
	extra := []driver.LockKey{sources[1]}
	for _, key := range keys {
		key := key
		extra = append(extra, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
	}
	tx, header := beginSourceTestWithLocks(t, s, lease, extra, false)
	for i, key := range keys {
		value := []byte(`{"facts":[]}`)
		row := driver.DerivedRow{Key: key.Key, Value: value, Index: &driver.SelectionIndex{
			Version: driver.SelectionIndexVersion, Row: key, Incarnation: header.Incarnation,
			Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value),
			Source: driver.SelectionSource{Key: sources[i], Revision: model.Natural64(0)},
			Facts:  &driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{}},
		}}
		if err := tx.PutSelectionDerived(ctx, key.Table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpReadFacts(ctx, []driver.ReadFact{fact}); err != nil {
		t.Fatal(err)
	}
	counts.queries.Store(0)
	if err := tx.PutSelectionDerived(ctx, driver.DerivedAttribute, nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := counts.queries.Load(); got != 2 {
		t.Fatalf("sentinel/read-fact dependents were not rechecked after bumps: %d native queries", got)
	}
}

// This correctness fixture holds the production lease across archive setup
// and restore. Supervise normal heartbeats and join them before closing the
// physical store; neither a missed renewal nor its error is ignored.
func renewNativeDetachLease(t *testing.T, ctx context.Context, lease driver.ActiveLease) func() {
	t.Helper()
	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil {
					if errors.Is(err, context.Canceled) && renewCtx.Err() != nil {
						done <- nil
					} else {
						done <- err
					}
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("native detach lease renewal: %v", err)
		}
	}
}

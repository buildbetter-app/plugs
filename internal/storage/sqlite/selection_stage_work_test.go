package sqlite

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
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
			var durable int
			if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM selection_bead_history WHERE end_revision IS NULL`).Scan(&durable); err != nil || durable != population {
				t.Fatalf("durable typed population=%d want=%d: %v", durable, population, err)
			}
			t.Logf("population=%d interleaved_product_sources=%d successful_native_source_queries=%d stage_elapsed_ns=%d one_replacement_queries=1 durable_current_rows=%d executor_row_visits=UNMEASURED", population, population, queries, elapsed.Nanoseconds(), durable)
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
	fact := pipeline.GraphValidityReadFact
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

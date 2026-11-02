package postgres

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This transparent database/sql probe counts actual driver calls and rows. It
// uses the real PostgreSQL driver and the same disposable database/DSN as Store.
// It supplies no query result, policy decision, or elapsed-time substitute.
type queryProbe struct{ calls, rows atomic.Int64 }
type queryProbeConnector struct {
	base  sqldriver.Driver
	dsn   string
	probe *queryProbe
}

func (c queryProbeConnector) Driver() sqldriver.Driver { return c.base }
func (c queryProbeConnector) Connect(context.Context) (sqldriver.Conn, error) {
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &queryProbeConn{Conn: conn, probe: c.probe}, nil
}

type queryProbeConn struct {
	sqldriver.Conn
	probe *queryProbe
}

func (c *queryProbeConn) CheckNamedValue(value *sqldriver.NamedValue) error {
	if checker, ok := c.Conn.(sqldriver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return sqldriver.ErrSkip
}

func (c *queryProbeConn) BeginTx(ctx context.Context, opts sqldriver.TxOptions) (sqldriver.Tx, error) {
	if tx, ok := c.Conn.(sqldriver.ConnBeginTx); ok {
		return tx.BeginTx(ctx, opts)
	}
	return c.Conn.Begin()
}
func (c *queryProbeConn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	c.probe.calls.Add(1)
	rows, err := c.Conn.(sqldriver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &queryProbeRows{Rows: rows, probe: c.probe}, nil
}

type queryProbeRows struct {
	sqldriver.Rows
	probe *queryProbe
}

func (r *queryProbeRows) Next(values []sqldriver.Value) error {
	err := r.Rows.Next(values)
	if err == nil {
		r.probe.rows.Add(1)
	}
	return err
}

func TestQueryHeadPageBoundsRealSQLWork(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	s := openTest(t, dsn)
	lease := testLease(t, s)
	changes := make([]conformance.FixtureChange, 2001)
	for i := range changes {
		changes[i] = conformance.FixtureChange{Ref: testRef(fmt.Sprintf("page-%04d", i)), Kind: "test", Op: model.OpCreate, Body: []byte(`{"project":"p","type":"issue"}`)}
	}
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	probe := &queryProbe{}
	db := sql.OpenDB(queryProbeConnector{base: s.db.Driver(), dsn: dsn, probe: probe})
	observed := &Store{db: db, resources: make(map[interface{ Close() error }]struct{}), leases: make(map[*activeLease]struct{})}
	t.Cleanup(func() {
		if err := observed.Close(); err != nil {
			t.Error(err)
		}
	})
	r := testRead(t, observed, nil)
	for _, filter := range []map[string]any{nil, {"kind": "test"}} {
		t.Run(fmt.Sprint(filter), func(t *testing.T) {
			var cursor *driver.Cursor
			seen := 0
			for {
				probe.calls.Store(0)
				probe.rows.Store(0)
				started := time.Now()
				page, err := r.Query(ctx, driver.Query{Filter: filter, Limit: 1000, Cursor: cursor})
				t.Logf("QUERY_SQL_WORK offset=%d calls=%d rows=%d returned=%d elapsed=%s", seen, probe.calls.Load(), probe.rows.Load(), len(page.Items), time.Since(started))
				if err != nil {
					t.Fatal(err)
				}
				if probe.calls.Load() > 2 || probe.rows.Load() > 1001 {
					t.Errorf("one page visited the whole corpus: calls=%d rows=%d", probe.calls.Load(), probe.rows.Load())
				}
				for _, record := range page.Items {
					if record.Ref.ID != changes[seen].Ref.ID || string(record.Body) != string(changes[seen].Body) {
						t.Fatal("page changed membership/body/order")
					}
					seen++
				}
				if page.NextCursor == nil {
					break
				}
				cursor = page.NextCursor
			}
			if seen != len(changes) {
				t.Fatalf("lost tail: %d", seen)
			}
		})
	}
}

func queryIDs(t *testing.T, ctx context.Context, r driver.Reader, filter map[string]any) []string {
	t.Helper()
	ids := []string{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Filter: filter, Limit: 1, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range page.Items {
			ids = append(ids, record.Ref.ID)
		}
		if page.NextCursor == nil {
			return ids
		}
		if len(page.Items) != 1 {
			t.Fatal("non-advancing page")
		}
		cursor = page.NextCursor
	}
}

func TestQueryPagingPreservesSnapshotHistoryOverlayAndBindings(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	seed := func(changes ...conformance.FixtureChange) model.Changeset {
		t.Helper()
		entry, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes)
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	change := func(id string, op model.OpKind) conformance.FixtureChange {
		return conformance.FixtureChange{Ref: testRef(id), Kind: "test", Op: op, Body: []byte(`{"project":"p","type":"issue"}`)}
	}
	first := seed(change("page-a", model.OpCreate), change("page-b", model.OpCreate), change("page-d", model.OpCreate))
	pinned := testRead(t, s, nil)
	page, err := pinned.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Limit: 1})
	if err != nil || page.NextCursor == nil || page.Items[0].Ref.ID != "page-a" {
		t.Fatalf("first page: %+v %v", page, err)
	}
	keys := []driver.LockKey{testLock(testRef("page-a")), testLock(testRef("page-b")), testLock(testRef("page-d"))}
	before, err := pinned.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	deleted := seed(change("page-b", model.OpDelete), change("page-c", model.OpCreate))
	seed(change("page-b", model.OpCreate))
	if ids := queryIDs(t, ctx, pinned, map[string]any{"kind": "test"}); !reflect.DeepEqual(ids, []string{"page-a", "page-b", "page-d"}) {
		t.Fatalf("pinned snapshot changed: %v", ids)
	}
	after, err := pinned.Observe(ctx, keys)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("query changed observations or borrowed current state")
	}
	old := testRead(t, s, &first.Revision)
	if ids := queryIDs(t, ctx, old, map[string]any{"kind": "test"}); !reflect.DeepEqual(ids, []string{"page-a", "page-b", "page-d"}) {
		t.Fatalf("historical replay changed: %v", ids)
	}
	tombstone := testRead(t, s, &deleted.Revision)
	if ids := queryIDs(t, ctx, tombstone, nil); !reflect.DeepEqual(ids, []string{"page-a", "page-c", "page-d"}) {
		t.Fatalf("historical tombstone changed: %v", ids)
	}
	current := testRead(t, s, nil)
	want := []string{"page-a", "page-b", "page-c", "page-d"}
	for _, filter := range []map[string]any{nil, {"kind": "test"}, {"project": "p", "type": "issue"}} {
		if ids := queryIDs(t, ctx, current, filter); !reflect.DeepEqual(ids, want) {
			t.Fatalf("current/fallback differs: %v", ids)
		}
	}
	for _, mutate := range []func(*driver.Cursor){
		func(c *driver.Cursor) { c.Index = model.Natural64(100) },
		func(c *driver.Cursor) { c.Revision = model.Natural64(0) },
		func(c *driver.Cursor) { c.Method = "versions" },
		func(c *driver.Cursor) { c.FilterDigest = "wrong" },
		func(c *driver.Cursor) { c.PrincipalDigest = "wrong" },
		func(c *driver.Cursor) { c.OntologyRevision = model.Natural64(1) },
		func(c *driver.Cursor) { c.Incarnation = "wrong" },
	} {
		bad := *page.NextCursor
		mutate(&bad)
		if _, err := pinned.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Cursor: &bad}); err == nil {
			t.Fatal("invalid cursor accepted")
		}
	}
	end := *page.NextCursor
	end.Index = model.Natural64(3)
	if empty, err := pinned.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Cursor: &end}); err != nil || len(empty.Items) != 0 || empty.NextCursor != nil {
		t.Fatalf("exact end rejected: %+v %v", empty, err)
	}
	zero := model.Natural64(0)
	for _, q := range []driver.Query{{Filter: map[string]any{"unknown": "p"}}, {Filter: map[string]any{"kind": 1}}, {AsOf: &zero}, {Limit: -1}, {Limit: 10001}} {
		if _, err := current.Query(ctx, q); err == nil {
			t.Fatal("invalid query accepted")
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := current.Query(canceled, driver.Query{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation changed: %v", err)
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	requests := []driver.LockRequest{{Key: testLock(testRef("page-a")), Mode: driver.LockUpdate}, {Key: testLock(testRef("page-e")), Mode: driver.LockUpdate}}
	if _, err = tx.Lock(ctx, requests); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Delete(ctx, testRef("page-a")); err != nil {
		t.Fatal(err)
	}
	if err = tx.Put(ctx, driver.Record{Ref: testRef("page-e"), Kind: "test", Body: []byte(`{"project":"p","type":"issue"}`)}); err != nil {
		t.Fatal(err)
	}
	if ids := queryIDs(t, ctx, tx, map[string]any{"kind": "test"}); !reflect.DeepEqual(ids, []string{"page-b", "page-c", "page-d", "page-e"}) {
		t.Fatalf("staged overlay changed: %v", ids)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if ids := queryIDs(t, ctx, current, nil); !reflect.DeepEqual(ids, want) {
		t.Fatal("rolled back overlay leaked")
	}
}

package sqlite

import (
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This transparent database/sql probe counts actual driver calls and rows. It
// uses the real SQLite driver and the same file/connection settings as Store.
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
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "query-work.db")
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	changes := make([]conformance.FixtureChange, 2001)
	for i := range changes {
		changes[i] = conformance.FixtureChange{Ref: testRef(fmt.Sprintf("page-%04d", i)), Kind: "test", Op: model.OpCreate, Body: []byte(`{"project":"p","type":"issue"}`)}
	}
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	probe := &queryProbe{}
	u := url.URL{Scheme: "file", Path: path}
	db := sql.OpenDB(queryProbeConnector{base: s.db.Driver(), dsn: u.String() + "?_pragma=busy_timeout(0)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)", probe: probe})
	observed := &Store{db: db, lockDir: s.lockDir, children: make(map[io.Closer]struct{}), leases: make(map[*activeLease]struct{})}
	t.Cleanup(func() {
		if err := observed.Close(); err != nil {
			t.Error(err)
		}
	})
	meta := getMeta(t, ctx, s)
	r := getReader(t, ctx, observed, meta, nil)
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
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "query-semantics.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
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
	meta := getMeta(t, ctx, s)
	pinned := getReader(t, ctx, s, meta, nil)
	page, err := pinned.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Limit: 1})
	if err != nil || page.NextCursor == nil || page.Items[0].Ref.ID != "page-a" {
		t.Fatalf("first page: %+v %v", page, err)
	}
	keys := []driver.LockKey{testKey("page-a"), testKey("page-b"), testKey("page-d")}
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
	old := getReader(t, ctx, s, meta, &first.Revision)
	if ids := queryIDs(t, ctx, old, map[string]any{"kind": "test"}); !reflect.DeepEqual(ids, []string{"page-a", "page-b", "page-d"}) {
		t.Fatalf("historical replay changed: %v", ids)
	}
	tombstone := getReader(t, ctx, s, meta, &deleted.Revision)
	if ids := queryIDs(t, ctx, tombstone, nil); !reflect.DeepEqual(ids, []string{"page-a", "page-c", "page-d"}) {
		t.Fatalf("historical tombstone changed: %v", ids)
	}
	current := getReader(t, ctx, s, meta, nil)
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
	requests := []driver.LockRequest{{Key: testKey("page-a"), Mode: driver.LockUpdate}, {Key: testKey("page-e"), Mode: driver.LockUpdate}}
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

func TestQueryHistoricalPageBoundsRealSQLWork(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "historical-query-work.db")
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	changes := make([]conformance.FixtureChange, 2002)
	for i := range changes {
		kind := "test"
		if i%7 == 0 {
			kind = "other"
		}
		changes[i] = conformance.FixtureChange{Ref: testRef(fmt.Sprintf("history-%04d", i)), Kind: kind, Op: model.OpCreate, Body: []byte(`{"project":"p","type":"issue"}`)}
	}
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	deleted, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: changes[1].Ref, Kind: changes[1].Kind, Op: model.OpDelete}})
	if err != nil {
		t.Fatal(err)
	}
	later := []conformance.FixtureChange{
		{Ref: changes[0].Ref, Kind: changes[0].Kind, Op: model.OpPatch, Body: []byte(`{"project":"newer","type":"issue"}`)},
		changes[1],
		{Ref: testRef("history-future"), Kind: "test", Op: model.OpCreate, Body: changes[0].Body},
	}
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, later); err != nil {
		t.Fatal(err)
	}
	probe := &queryProbe{}
	u := url.URL{Scheme: "file", Path: path}
	db := sql.OpenDB(queryProbeConnector{base: s.db.Driver(), dsn: u.String() + "?_pragma=busy_timeout(0)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)", probe: probe})
	observed := &Store{db: db, lockDir: s.lockDir, children: make(map[io.Closer]struct{}), leases: make(map[*activeLease]struct{})}
	t.Cleanup(func() {
		if err := observed.Close(); err != nil {
			t.Error(err)
		}
	})
	meta := getMeta(t, ctx, s)
	r := getReader(t, ctx, observed, meta, &deleted.Revision)
	for _, filter := range []map[string]any{nil, {"kind": "test"}, {"kind": "other"}, {"kind": "absent"}} {
		t.Run(fmt.Sprint(filter), func(t *testing.T) {
			want := []conformance.FixtureChange{}
			for i, change := range changes {
				if i != 1 && (len(filter) == 0 || filter["kind"] == change.Kind) {
					want = append(want, change)
				}
			}
			var cursor, bound *driver.Cursor
			seen := 0
			for {
				probe.calls.Store(0)
				probe.rows.Store(0)
				started := time.Now()
				page, err := r.Query(ctx, driver.Query{Filter: filter, Limit: 1000, Cursor: cursor})
				t.Logf("HISTORICAL_QUERY_SQL_WORK offset=%d calls=%d rows=%d returned=%d elapsed=%s asof=%s head=%s", seen, probe.calls.Load(), probe.rows.Load(), len(page.Items), time.Since(started), deleted.Revision, meta.Head)
				if err != nil {
					t.Fatal(err)
				}
				// Only history-0000 needs reconstruction; all other visible
				// post-images are unchanged at this historical revision.
				if probe.calls.Load() > 10 || probe.rows.Load() > 1010 {
					t.Errorf("historical page reconstructed the complete corpus: calls=%d rows=%d", probe.calls.Load(), probe.rows.Load())
				}
				for _, record := range page.Items {
					if seen >= len(want) || record.Ref.ID != want[seen].Ref.ID || record.Kind != want[seen].Kind || string(record.Body) != string(want[seen].Body) {
						t.Fatal("historical page changed membership/body/order or reused a newer image")
					}
					seen++
				}
				if page.NextCursor == nil {
					break
				}
				bound, cursor = page.NextCursor, page.NextCursor
			}
			if seen != len(want) {
				t.Fatalf("historical tail lost: %d want %d", seen, len(want))
			}
			if bound != nil {
				end := *bound
				end.Index = model.Natural64(uint64(len(want)))
				if page, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: &end}); err != nil || len(page.Items) != 0 || page.NextCursor != nil {
					t.Fatalf("historical exact end: %+v %v", page, err)
				}
				end.Index = model.Natural64(uint64(len(want) + 1))
				if _, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: &end}); err == nil {
					t.Fatal("historical cursor beyond end accepted")
				}
			}
		})
	}
	zero := model.Natural64(0)
	beforeCreation := getReader(t, ctx, observed, meta, &zero)
	if page, err := beforeCreation.Query(ctx, driver.Query{}); err != nil || len(page.Items) != 0 || page.NextCursor != nil {
		t.Fatalf("pre-creation historical query: %+v %v", page, err)
	}
}

func TestQueryHistoricalPreservesExactReplayBodyBytes(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "historical-body-bytes.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	a, b := testRef("raw-a"), testRef("raw-b")
	base := []byte(" { \"text\": \"<base>&\", \"n\": 1 }\n")
	patched := []byte(" { \"text\": \"<patch>&\", \"n\": 2 }\n")
	seed := func(ref driver.Ref, op model.OpKind, body []byte) model.Revision {
		t.Helper()
		entry, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "test", Op: op, Body: body}})
		if err != nil {
			t.Fatal(err)
		}
		return entry.Revision
	}
	baseAt := seed(a, model.OpCreate, base)
	patchAt := seed(a, model.OpPatch, patched)
	otherAt := seed(b, model.OpCreate, base)
	meta := getMeta(t, ctx, s)
	canonicalPatch, err := json.Marshal(json.RawMessage(patched))
	if err != nil {
		t.Fatal(err)
	}
	check := func(name string, r driver.Reader, refs []driver.Ref, bodies [][]byte) string {
		t.Helper()
		point, err := r.Get(ctx, refs)
		if err != nil || len(point) != len(bodies) {
			t.Fatalf("%s Get: %+v %v", name, point, err)
		}
		for i := range point {
			if string(point[i].Body) != string(bodies[i]) {
				t.Fatalf("%s frozen replay control changed: got=%q want=%q", name, point[i].Body, bodies[i])
			}
		}
		page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Limit: 1000})
		if err != nil || page.NextCursor != nil {
			t.Fatalf("%s Query: %+v %v", name, page, err)
		}
		if !reflect.DeepEqual(page.Items, point) {
			t.Errorf("%s Query differs byte-for-byte from historical Get/recordAt: Query=%+v Get=%+v", name, page.Items, point)
		}
		encoded, err := json.Marshal(page.Items)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("HISTORICAL_BODY_BYTES phase=%s asof=%s records=%d replay_body=%q query_record_stream=%s", name, page.AsOf, len(point), point[0].Body, encoded)
		return string(encoded)
	}
	// Head Get retains Put's valid raw bytes. History retains the precise
	// stored base or forward-patch Value bytes, including HTML escaping.
	check("current_raw", getReader(t, ctx, s, meta, nil), []driver.Ref{a, b}, [][]byte{patched, base})
	check("historical_base", getReader(t, ctx, s, meta, &baseAt), []driver.Ref{a}, [][]byte{base})
	pinned := getReader(t, ctx, s, meta, &patchAt)
	before := check("historical_patch", pinned, []driver.Ref{a}, [][]byte{canonicalPatch})
	seed(a, model.OpPatch, []byte(`{"text":"later","n":4}`))
	check("pinned_after_write", pinned, []driver.Ref{a}, [][]byte{canonicalPatch})
	fresh := getReader(t, ctx, s, meta, &patchAt)
	if after := check("fresh_same_asof", fresh, []driver.Ref{a}, [][]byte{canonicalPatch}); before != after {
		t.Error("same-asOf encoded record stream changed after a later current update")
	}
	check("historical_raw_base_and_patch", getReader(t, ctx, s, meta, &otherAt), []driver.Ref{a, b}, [][]byte{canonicalPatch, base})
}

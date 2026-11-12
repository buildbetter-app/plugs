package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Pin the existing current-main contract while independently checking the
// cursor regression found in the candidate current-query optimization.
func TestCurrentQueryCursorRejectsOutOfRangeAndPreservesExactEnd(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "query-cursor.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	for _, id := range []string{"cursor-a", "cursor-b", "cursor-c"} {
		seedRecord(t, ctx, s, lease, id, `{}`)
	}
	meta := getMeta(t, ctx, s)
	r := getReader(t, ctx, s, meta, nil)
	filter := map[string]any{"kind": "test"}
	first, err := r.Query(ctx, driver.Query{Filter: filter, Limit: 1})
	if err != nil || len(first.Items) != 1 || first.Items[0].Ref.ID != "cursor-a" || first.NextCursor == nil {
		t.Fatalf("first current page: %+v, %v", first, err)
	}
	for _, tc := range []struct {
		name  string
		index model.Natural
	}{
		{"past_end", model.Natural64(100)},
		{"uint64_max", model.Natural64(math.MaxUint64)},
		{"beyond_uint64", model.Natural("1e1000000000")},
		{"fractional", model.Natural("1.5")},
		{"negative", model.Natural("-1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := *first.NextCursor
			bad.Index = tc.index
			page, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: &bad, Limit: 1})
			if err == nil || len(page.Items) != 0 || page.NextCursor != nil {
				t.Fatalf("invalid current cursor returned a page: %+v, %v", page, err)
			}
		})
	}
	// A nonzero offset cursor without the new opaque key marker is an old
	// token. Refuse it rather than preserving the removed OFFSET compatibility
	// path, including its old exact-end behavior.
	legacyOffset := *first.NextCursor
	legacyOffset.After = ""
	legacyOffset.Index = model.Natural64(3)
	if page, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: &legacyOffset, Limit: 1}); err == nil || len(page.Items) != 0 {
		t.Fatalf("legacy offset cursor was accepted: %+v, %v", page, err)
	}
	// Changing the key while retaining the original rank is a tampered cursor;
	// the indexed rank marker must agree with the opaque key.
	cursorCKey, err := recordKey(testRef("cursor-c"))
	if err != nil {
		t.Fatal(err)
	}
	tampered := *first.NextCursor
	tampered.After = cursorCKey
	if page, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: &tampered, Limit: 1}); err == nil || len(page.Items) != 0 {
		t.Fatalf("tampered key cursor was accepted: %+v, %v", page, err)
	}
	exactEnd := *first.NextCursor
	exactEnd.After = cursorCKey
	exactEnd.Index = model.Natural64(3)
	page, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: &exactEnd, Limit: 1})
	if err != nil || len(page.Items) != 0 || page.NextCursor != nil || page.AsOf.String() != first.AsOf.String() {
		t.Fatalf("exact-end cursor lost its empty pinned page: %+v, %v", page, err)
	}
	second, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: first.NextCursor, Limit: 1})
	if err != nil || len(second.Items) != 1 || second.Items[0].Ref.ID != "cursor-b" || second.NextCursor == nil {
		t.Fatalf("invalid cursors changed valid continuation: %+v, %v", second, err)
	}
	last, err := r.Query(ctx, driver.Query{Filter: filter, Cursor: second.NextCursor, Limit: 1})
	if err != nil || len(last.Items) != 1 || last.Items[0].Ref.ID != "cursor-c" || last.NextCursor != nil {
		t.Fatalf("valid final page changed: %+v, %v", last, err)
	}
	if after := getMeta(t, ctx, s); after.Head.String() != meta.Head.String() || after.Incarnation != meta.Incarnation {
		t.Fatal("read/refused cursors changed store state")
	}
}

func TestCurrentQueryCursorSurvivesStoreReopen(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "query-cursor-reopen.db")
	opened, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	s := opened.(*Store)
	lease, err := s.Lease(ctx, "query-reopen", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"reopen-a", "reopen-b", "reopen-c"} {
		seedRecord(t, ctx, s, lease, id, `{}`)
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	first, err := r.Query(ctx, driver.Query{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == nil {
		t.Fatalf("first unfiltered page: %+v %v", first, err)
	}
	digest := sha256.Sum256([]byte("null"))
	if first.NextCursor.FilterDigest != hex.EncodeToString(digest[:]) {
		t.Fatalf("public filter digest changed: %q", first.NextCursor.FilterDigest)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedValue, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	reopened := reopenedValue.(*Store)
	defer reopened.Close()
	reopenedMeta, err := reopened.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	continued, err := reopened.Read(ctx, driver.View{Incarnation: reopenedMeta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer continued.Close()
	second, err := continued.Query(ctx, driver.Query{Limit: 1, Cursor: first.NextCursor})
	if err != nil || len(second.Items) != 1 || second.Items[0].Ref.ID != "reopen-b" {
		t.Fatalf("reopened continuation: %+v %v", second, err)
	}
}

func TestSchemaThreeRepairsQueryKeysetColumnsAndIndex(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "schema-three.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.ExecContext(ctx, `
CREATE TABLE meta(singleton INTEGER PRIMARY KEY, store_id TEXT NOT NULL, incarnation TEXT NOT NULL, schema_version INTEGER NOT NULL, head TEXT NOT NULL, history_floor TEXT NOT NULL);
INSERT INTO meta VALUES(1,'store','incarnation',3,'00000000000000000000','00000000000000000000');
CREATE TABLE objects(key TEXT PRIMARY KEY, kind TEXT NOT NULL);
CREATE TABLE records(key TEXT PRIMARY KEY, data BLOB NOT NULL);`)
	if err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	value, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	defer s.Close()
	var columns map[string]bool
	columns = map[string]bool{}
	rows, err := s.db.QueryContext(ctx, `SELECT name FROM pragma_table_info('objects')`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !columns["query_order"] || !columns["query_kind_order"] {
		t.Fatalf("schema-three columns were not repaired: %v", columns)
	}
	var index int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='objects_kind_key'`).Scan(&index); err != nil || index != 1 {
		t.Fatalf("kind keyset index missing: count=%d err=%v", index, err)
	}
}

func TestSchemaTwoMigrationPreservesRowsRanksAndReadWork(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "schema-two.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	refA, refB := testRef("schema-two-a"), testRef("schema-two-b")
	keyA, err := recordKey(refA)
	if err != nil {
		raw.Close()
		t.Fatal(err)
	}
	keyB, err := recordKey(refB)
	if err != nil {
		raw.Close()
		t.Fatal(err)
	}
	dataA, err := json.Marshal(driver.Record{Ref: refA, Kind: "test", Body: []byte(`{"value":"a"}`)})
	if err != nil {
		raw.Close()
		t.Fatal(err)
	}
	dataB, err := json.Marshal(driver.Record{Ref: refB, Kind: "test", Body: []byte(`{"value":"b"}`)})
	if err != nil {
		raw.Close()
		t.Fatal(err)
	}
	_, err = raw.ExecContext(ctx, `
CREATE TABLE meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1), store_id TEXT NOT NULL, incarnation TEXT NOT NULL, schema_version INTEGER NOT NULL, head TEXT NOT NULL, history_floor TEXT NOT NULL);
INSERT INTO meta VALUES(1,'store','incarnation',2,'00000000000000000002','00000000000000000000');
CREATE TABLE replays(scope TEXT PRIMARY KEY, created INTEGER NOT NULL, rev TEXT NOT NULL, request_hash TEXT NOT NULL, response BLOB NOT NULL, changeset BLOB, finalized INTEGER NOT NULL DEFAULT 0, post BLOB);
CREATE TABLE objects(key TEXT PRIMARY KEY, kind TEXT NOT NULL);
CREATE TABLE records(key TEXT PRIMARY KEY, data BLOB NOT NULL);`)
	if err == nil {
		_, err = raw.ExecContext(ctx, `INSERT INTO objects(key,kind) VALUES(?,?),(?,?)`, keyA, "test", keyB, "test")
	}
	if err == nil {
		_, err = raw.ExecContext(ctx, `INSERT INTO records(key,data) VALUES(?,?),(?,?)`, keyA, dataA, keyB, dataB)
	}
	if err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	value, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	s := value.(*Store)
	defer s.Close()
	meta := getMeta(t, ctx, s)
	if meta.SchemaVersion != schemaVersion || meta.Head.String() != "2" {
		t.Fatalf("schema-two migration changed metadata: %+v", meta)
	}
	r := getReader(t, ctx, s, meta, nil)
	var work queryReadWork
	first, err := r.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == nil || first.Items[0].Ref.ID != "schema-two-a" {
		t.Fatalf("schema-two first page: %+v %v", first, err)
	}
	if work.selectors.Load() != 2 || work.payloads.Load() != 2 || work.payloadBytes.Load() != int64(len(`{"value":"a"}`)+len(`{"value":"b"}`)) {
		t.Fatalf("schema-two read work changed: selectors=%d payloads=%d bytes=%d", work.selectors.Load(), work.payloads.Load(), work.payloadBytes.Load())
	}
	second, err := r.Query(ctx, driver.Query{Limit: 1, Cursor: first.NextCursor})
	if err != nil || len(second.Items) != 1 || second.Items[0].Ref.ID != "schema-two-b" || second.NextCursor != nil {
		t.Fatalf("schema-two continuation: %+v %v", second, err)
	}
}

func TestCurrentQueryReadWorkIncludesFilteredCandidates(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "query-accounting.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	changes := []conformance.FixtureChange{
		{Ref: testRef("accounting-test"), Kind: "test", Op: model.OpCreate, Body: []byte(`{"value":"test"}`)},
		{Ref: testRef("accounting-other"), Kind: "other", Op: model.OpCreate, Body: []byte(`{"value":"other"}`)},
		{Ref: testRef("accounting-deleted"), Kind: "other", Op: model.OpCreate, Body: []byte(`{"value":"deleted"}`)},
	}
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: testRef("accounting-deleted"), Kind: "other", Op: model.OpDelete}}); err != nil {
		t.Fatal(err)
	}
	// A legacy/current image may retain a JSON null Body. Seed that physical
	// row directly so its nil-body bytes are zero while the
	// payload record still counts as materialized.
	nilRef := testRef("accounting-nil")
	nilKey, err := recordKey(nilRef)
	if err != nil {
		t.Fatal(err)
	}
	nilData, err := json.Marshal(driver.Record{Ref: nilRef, Kind: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO objects(key,kind) VALUES(?,?)`, nilKey, "other"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO records(key,data) VALUES(?,?)`, nilKey, nilData); err != nil {
		t.Fatal(err)
	}
	if err := rebuildQueryOrder(ctx, s.db); err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, s)
	r := getReader(t, ctx, s, meta, nil)
	var work queryReadWork
	page, err := r.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Filter: map[string]any{"kind": "test"}, Limit: 10})
	if err != nil || len(page.Items) != 1 || page.Items[0].Ref.ID != "accounting-test" {
		t.Fatalf("filtered current page: %+v, %v", page, err)
	}
	wantBytes := int64(len(changes[0].Body))
	if work.selectors.Load() != 1 || work.payloads.Load() != 1 || work.payloadBytes.Load() != wantBytes {
		t.Fatalf("head materialization accounting changed: selectors=%d payloads=%d bytes=%d want_bytes=%d", work.selectors.Load(), work.payloads.Load(), work.payloadBytes.Load(), wantBytes)
	}
	// An unfiltered head page materializes every live row in its bounded
	// lookahead window, including a nil body, while the tombstone has no record
	// row and therefore is not materialized.
	work.reset()
	unfiltered, err := r.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Limit: 10})
	if err != nil || len(unfiltered.Items) != 3 || unfiltered.NextCursor != nil {
		t.Fatalf("unfiltered current page: %+v, %v", unfiltered, err)
	}
	if work.selectors.Load() != 3 || work.payloads.Load() != 3 || work.payloadBytes.Load() != wantBytes+int64(len(changes[1].Body)) {
		t.Fatalf("live-row materialization accounting changed: selectors=%d payloads=%d bytes=%d", work.selectors.Load(), work.payloads.Load(), work.payloadBytes.Load())
	}
}

// queryProbe is a transparent wrapper around the real SQLite driver. It
// counts driver-level query calls and rows returned, including queries issued
// through a transaction, and retains the exact SQL text for the bounded proof.
type queryProbe struct {
	calls, rows atomic.Int64
	mu          sync.Mutex
	statements  []string
}

func (p *queryProbe) observeStatement(query string) {
	p.calls.Add(1)
	p.mu.Lock()
	p.statements = append(p.statements, query)
	p.mu.Unlock()
}

func (p *queryProbe) reset() {
	p.calls.Store(0)
	p.rows.Store(0)
	p.mu.Lock()
	p.statements = nil
	p.mu.Unlock()
}

func (p *queryProbe) statementSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.statements...)
}

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
	var (
		tx  sqldriver.Tx
		err error
	)
	if begin, ok := c.Conn.(sqldriver.ConnBeginTx); ok {
		tx, err = begin.BeginTx(ctx, opts)
	} else {
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, err
	}
	return &queryProbeTx{Tx: tx, probe: c.probe}, nil
}

func (c *queryProbeConn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	c.probe.observeStatement(query)
	rows, err := c.Conn.(sqldriver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &queryProbeRows{Rows: rows, probe: c.probe}, nil
}

type queryProbeTx struct {
	sqldriver.Tx
	probe *queryProbe
}

func (t *queryProbeTx) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	t.probe.observeStatement(query)
	queryer, ok := t.Tx.(sqldriver.QueryerContext)
	if !ok {
		return nil, sqldriver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &queryProbeRows{Rows: rows, probe: t.probe}, nil
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

type queryReadWork struct {
	queries, returned, selectors, payloads, payloadBytes atomic.Int64
}

func (w *queryReadWork) ObserveReadWork(kind driver.ReadWorkKind, amount int64) {
	switch kind {
	case driver.ReadQueryCalls:
		w.queries.Add(amount)
	case driver.ReadQueryReturned:
		w.returned.Add(amount)
	case driver.ReadSelectorKeys:
		w.selectors.Add(amount)
	case driver.ReadPayloadRecords:
		w.payloads.Add(amount)
	case driver.ReadPayloadBytes:
		w.payloadBytes.Add(amount)
	}
}

func (w *queryReadWork) reset() {
	w.queries.Store(0)
	w.returned.Store(0)
	w.selectors.Store(0)
	w.payloads.Store(0)
	w.payloadBytes.Store(0)
}

func renewQueryLease(t *testing.T, lease driver.ActiveLease) {
	t.Helper()
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = lease.Renew(context.Background())
			case <-stop:
				return
			}
		}
	}()
	t.Cleanup(func() { close(stop) })
}

func TestQueryHeadPagingStatementBoundAcrossCorpusSizes(t *testing.T) {
	for _, size := range []int{1000, 5000, 25000} {
		t.Run(fmt.Sprintf("records_%d", size), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			t.Cleanup(cancel)
			path := filepath.Join(t.TempDir(), "query-page.db")
			s := openStore(t, ctx, path)
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			renewQueryLease(t, lease)

			changes := make([]conformance.FixtureChange, size)
			for i := range changes {
				changes[i] = conformance.FixtureChange{
					Ref:  testRef(fmt.Sprintf("query-%06d", i)),
					Kind: "test",
					Op:   model.OpCreate,
					Body: []byte(`{"project":"query","type":"issue"}`),
				}
			}
			for start := 0; start < len(changes); start += 500 {
				end := min(start+500, len(changes))
				if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes[start:end]); err != nil {
					t.Fatalf("seed %d..%d: %v", start, end, err)
				}
			}

			probe := &queryProbe{}
			u := url.URL{Scheme: "file", Path: path}
			db := sql.OpenDB(queryProbeConnector{
				base:  s.db.Driver(),
				dsn:   u.String() + "?_pragma=busy_timeout(0)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)",
				probe: probe,
			})
			observed := &Store{db: db, lockDir: s.lockDir, children: make(map[io.Closer]struct{}), leases: make(map[*activeLease]struct{})}
			t.Cleanup(func() {
				if err := observed.Close(); err != nil {
					t.Error(err)
				}
			})

			meta := getMeta(t, ctx, s)
			for _, filter := range []map[string]any{nil, {"kind": "test"}} {
				t.Run(fmt.Sprintf("filter_%v", filter != nil), func(t *testing.T) {
					r := getReader(t, ctx, observed, meta, nil)
					var cursor *driver.Cursor
					var work queryReadWork
					seen := 0
					const limit = 997
					for pageNumber := 0; ; pageNumber++ {
						if pageNumber > 0 {
							if err := r.Close(); err != nil {
								t.Fatalf("close page reader %d: %v", pageNumber-1, err)
							}
							r = getReader(t, ctx, observed, meta, nil)
						}
						probe.reset()
						work.reset()
						page, err := r.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Filter: filter, Limit: limit, Cursor: cursor})
						if err != nil {
							t.Fatalf("page %d: %v", pageNumber, err)
						}
						calls, rows := probe.calls.Load(), probe.rows.Load()
						t.Logf("QUERY_SQL_STATEMENTS corpus=%d page=%d calls=%d rows=%d returned=%d", size, pageNumber, calls, rows, len(page.Items))
						expectedCalls := int64(1)
						if cursor != nil {
							expectedCalls = 2 // anchor validation plus the ordered keyset page
						}
						if calls != expectedCalls || rows < int64(len(page.Items)) || rows > int64(limit+2) {
							t.Fatalf("page work grew with corpus: calls=%d want_calls=%d rows=%d limit=%d", calls, expectedCalls, rows, limit)
						}
						statements := probe.statementSnapshot()
						if len(statements) != int(calls) {
							t.Fatalf("probe lost SQL statements: calls=%d statements=%v", calls, statements)
						}
						if cursor != nil {
							if calls != 2 {
								t.Fatalf("canonical continuation SQL count changed: calls=%d statements=%v", calls, statements)
							}
							keyset := false
							for _, statement := range statements {
								if strings.Contains(statement, "o.key>?") && strings.Contains(statement, "ORDER BY o.key LIMIT ?") {
									keyset = true
								}
								if strings.Contains(statement, "OFFSET") {
									t.Fatalf("canonical continuation used OFFSET: %s", statement)
								}
							}
							if !keyset {
								t.Fatalf("canonical continuation lacked ordered keyset SQL: %v", statements)
							}
						}
						wantPageBytes := rows * int64(len(changes[0].Body))
						if work.queries.Load() != 1 || work.selectors.Load() != rows || work.payloads.Load() != rows || work.payloadBytes.Load() != wantPageBytes || work.returned.Load() != int64(len(page.Items)) {
							t.Fatalf("read-work accounting changed: queries=%d selectors=%d payloads=%d payload_bytes=%d want_bytes=%d returned=%d sql_rows=%d page_items=%d", work.queries.Load(), work.selectors.Load(), work.payloads.Load(), work.payloadBytes.Load(), wantPageBytes, work.returned.Load(), rows, len(page.Items))
						}
						for _, record := range page.Items {
							want := changes[seen]
							if record.Ref.ID != want.Ref.ID || record.Kind != want.Kind || string(record.Body) != string(want.Body) {
								t.Fatalf("page %d changed order/body at %d: got=%s want=%s", pageNumber, seen, record.Ref.ID, want.Ref.ID)
							}
							seen++
						}
						if page.NextCursor == nil {
							break
						}
						if len(page.Items) != limit {
							t.Fatalf("nonterminal page returned %d rows", len(page.Items))
						}
						cursor = page.NextCursor
					}
					if seen != size {
						t.Fatalf("lost records: got=%d want=%d", seen, size)
					}
				})
			}
		})
	}
}

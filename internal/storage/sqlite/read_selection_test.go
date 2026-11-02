package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This probe delegates the actual engine statement and counts its emitted
// rows. Faults are injected only after real SQL executes, at the row transport
// boundary; it never supplies a permission decision or a successful result.
type selectionSQLProbe struct {
	mu          sync.Mutex
	statement   string
	args        []sqldriver.NamedValue
	rows, calls int
	fault       string
	cancel      context.CancelFunc
}
type selectionSQLConnector struct {
	base  sqldriver.Driver
	dsn   string
	probe *selectionSQLProbe
}

func (c selectionSQLConnector) Driver() sqldriver.Driver { return c.base }
func (c selectionSQLConnector) Connect(context.Context) (sqldriver.Conn, error) {
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &selectionSQLConn{Conn: conn, probe: c.probe}, nil
}

type selectionSQLConn struct {
	sqldriver.Conn
	probe *selectionSQLProbe
}

func (c *selectionSQLConn) BeginTx(ctx context.Context, o sqldriver.TxOptions) (sqldriver.Tx, error) {
	if v, ok := c.Conn.(sqldriver.ConnBeginTx); ok {
		return v.BeginTx(ctx, o)
	}
	return c.Conn.Begin()
}
func (c *selectionSQLConn) CheckNamedValue(v *sqldriver.NamedValue) error {
	if n, ok := c.Conn.(sqldriver.NamedValueChecker); ok {
		return n.CheckNamedValue(v)
	}
	return sqldriver.ErrSkip
}

var errSelectionTransport = errors.New("selection transport fault")

func (c *selectionSQLConn) QueryContext(ctx context.Context, q string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	rows, err := c.Conn.(sqldriver.QueryerContext).QueryContext(ctx, q, args)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(q, "WITH candidates AS (") {
		return rows, nil
	}
	c.probe.mu.Lock()
	c.probe.statement = q
	c.probe.args = append([]sqldriver.NamedValue(nil), args...)
	c.probe.calls++
	fault, cancel := c.probe.fault, c.probe.cancel
	c.probe.mu.Unlock()
	if fault == "query" {
		_ = rows.Close()
		return nil, errSelectionTransport
	}
	return &selectionSQLRows{Rows: rows, probe: c.probe, fault: fault, cancel: cancel}, nil
}

type selectionSQLRows struct {
	sqldriver.Rows
	probe  *selectionSQLProbe
	fault  string
	cancel context.CancelFunc
	seen   int
}

func (r *selectionSQLRows) Next(v []sqldriver.Value) error {
	if r.fault == "iteration" && r.seen == 1 {
		return errSelectionTransport
	}
	if r.fault == "short" && r.seen == 1 {
		return io.EOF
	}
	err := r.Rows.Next(v)
	if err != nil {
		return err
	}
	r.seen++
	r.probe.mu.Lock()
	r.probe.rows++
	r.probe.mu.Unlock()
	if r.seen == 2 {
		switch r.fault {
		case "scan":
			v[1] = "invalid ordinal"
		case "duplicate":
			v[1] = int64(0)
		case "total":
			v[0] = int64(1)
		case "mixed_null":
			v[1] = nil
		}
	}
	if r.fault == "cancel" && r.seen == 1 {
		r.cancel()
	}
	return nil
}
func (r *selectionSQLRows) Close() error {
	err := r.Rows.Close()
	if r.fault == "close" {
		return errSelectionTransport
	}
	return err
}

func TestReadSelectionRealSQLOraclesAndAtomicErrors(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "selection.db")
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	ids := []string{"z", "Red", "red", "猫", "café", "cafe\u0301", "q' OR 1=1 --", "a", "tail"}
	changes := make([]conformance.FixtureChange, len(ids))
	for i, id := range ids {
		changes[i] = conformance.FixtureChange{Ref: testRef(id), Kind: "test", Op: model.OpCreate, Body: []byte(`{"exact":"body"}`)}
	}
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	probe := &selectionSQLProbe{}
	u := url.URL{Scheme: "file", Path: path}
	db := sql.OpenDB(selectionSQLConnector{base: s.db.Driver(), dsn: u.String() + "?_pragma=busy_timeout(0)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)", probe: probe})
	observed := &Store{db: db, lockDir: s.lockDir, children: make(map[io.Closer]struct{}), leases: make(map[*activeLease]struct{})}
	t.Cleanup(func() {
		if err := observed.Close(); err != nil {
			t.Error(err)
		}
	})
	r := getReader(t, ctx, observed, getMeta(t, ctx, s), nil)
	view := r.Snapshot()
	candidates := make([]driver.ReadCandidate, len(ids))
	for i, id := range ids {
		candidates[i] = driver.ReadCandidate{Ordinal: i, Ref: testRef(id), Prepared: i != 2}
	}
	all := driver.SelectionExpr{Op: driver.SelectionTrue}
	cases := []struct {
		name               string
		expr               driver.SelectionExpr
		principal, sponsor string
		want               []int
	}{
		{"all", all, "user", "", []int{0, 1, 3, 4, 5, 6, 7, 8}},
		{"deny", driver.SelectionExpr{Op: driver.SelectionFalse}, "user", "", nil},
		{"exact_case_utf8_literal", driver.SelectionExpr{Op: driver.SelectionIDs, IDs: []string{"red", "猫", "café", "q' OR 1=1 --", "猫"}}, "user", "", []int{3, 4, 6}},
		{"unicode_not_normalized", driver.SelectionExpr{Op: driver.SelectionIDs, IDs: []string{"cafe\u0301"}}, "user", "", []int{5}},
		{"missing_ids", driver.SelectionExpr{Op: driver.SelectionIDs}, "user", "", nil},
		{"empty_and", driver.SelectionExpr{Op: driver.SelectionAll}, "user", "", []int{0, 1, 3, 4, 5, 6, 7, 8}},
		{"empty_or", driver.SelectionExpr{Op: driver.SelectionAny}, "user", "", nil},
		{"sponsor_intersection", driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionPrincipalKind, Kind: "agent"}, {Op: driver.SelectionSponsorKind, Kind: "user"}, {Op: driver.SelectionIDs, IDs: []string{"z", "tail"}}}}, "agent", "user", []int{0, 8}},
		{"sponsor_missing", driver.SelectionExpr{Op: driver.SelectionSponsorKind, Kind: "user"}, "agent", "", nil},
		{"forbid_wins", driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionAny, Args: []driver.SelectionExpr{{Op: driver.SelectionIDs, IDs: []string{"z", "Red", "tail"}}, {Op: driver.SelectionFalse}}}, {Op: driver.SelectionNot, Args: []driver.SelectionExpr{{Op: driver.SelectionIDs, IDs: []string{"Red"}}}}}}, "user", "", []int{0, 8}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for offset := 0; offset <= len(tc.want); offset += min(3, max(1, len(tc.want)-offset)) {
				probe.mu.Lock()
				probe.rows, probe.calls = 0, 0
				probe.mu.Unlock()
				request := &driver.ReadSelection{View: view, Candidates: candidates, Predicate: tc.expr, PrincipalKind: tc.principal, SponsorKind: tc.sponsor, Offset: offset, Limit: 3}
				page, err := r.Query(ctx, driver.Query{AsOf: view.At, ReadSelection: request})
				if err != nil {
					t.Fatal(err)
				}
				if page.ReadSelection == nil || len(page.Items) != 0 || page.NextCursor != nil || page.AsOf.String() != view.At.String() {
					t.Fatalf("selection leaked ordinary result: %+v", page)
				}
				end := min(offset+3, len(tc.want))
				want := tc.want[offset:end]
				if page.ReadSelection.Total != len(tc.want) || !reflect.DeepEqual(append([]int{}, page.ReadSelection.Ordinals...), append([]int{}, want...)) {
					t.Fatalf("independent order/total oracle: got=%+v want=%v total=%d", page.ReadSelection, want, len(tc.want))
				}
				probe.mu.Lock()
				statement := probe.statement
				args := append([]sqldriver.NamedValue(nil), probe.args...)
				rows, calls := probe.rows, probe.calls
				probe.mu.Unlock()
				raw, _ := json.Marshal(args)
				t.Logf("READY_SELECTION_SQL engine=sqlite case=%s input=%d offset=%d T=%d ordinals=%v calls=%d emitted_rows=%d sql_sha256=%x parameters_sha256=%x", tc.name, len(candidates), offset, page.ReadSelection.Total, page.ReadSelection.Ordinals, calls, rows, sha256.Sum256([]byte(statement)), sha256.Sum256(raw))
				if calls != 1 || rows != max(1, end-offset) {
					t.Fatalf("actual SQL did not return only T/page rows: calls=%d rows=%d", calls, rows)
				}
				if offset == len(tc.want) {
					break
				}
			}
		})
	}
	// Offset T is an empty terminal page; T+1 must refuse, including deny-all.
	for _, total := range []int{0, 8} {
		expr := all
		if total == 0 {
			expr = driver.SelectionExpr{Op: driver.SelectionFalse}
		}
		page, err := r.Query(ctx, driver.Query{ReadSelection: &driver.ReadSelection{View: view, Candidates: candidates, Predicate: expr, Limit: 3, Offset: total + 1}})
		if !errors.Is(err, driver.ErrReadSelectionOffset) || page.ReadSelection != nil || len(page.Items) != 0 {
			t.Fatalf("beyond-end refusal: %+v %v", page, err)
		}
	}
	if p, e := r.Query(ctx, driver.Query{ReadSelection: &driver.ReadSelection{View: view, Predicate: all, Limit: 3}}); e != nil || p.ReadSelection == nil || p.ReadSelection.Total != 0 {
		t.Fatalf("empty corpus: %+v %v", p, e)
	}
	t.Run("descriptor_refusals", func(t *testing.T) {
		changes := []struct {
			name   string
			mutate func(*driver.Query)
		}{
			{"mixed_filter", func(q *driver.Query) { q.Filter = map[string]any{} }},
			{"mixed_cursor", func(q *driver.Query) { q.Cursor = &driver.Cursor{} }},
			{"mixed_limit", func(q *driver.Query) { q.Limit = 1 }},
			{"wrong_asof", func(q *driver.Query) { at := model.Natural64(999); q.AsOf = &at }},
			{"wrong_incarnation", func(q *driver.Query) { q.ReadSelection.View.Incarnation = "other" }},
			{"wrong_view", func(q *driver.Query) { at := model.Natural64(999); q.ReadSelection.View.At = &at }},
			{"branch", func(q *driver.Query) { q.ReadSelection.View.Branch = model.Natural64(1) }},
			{"negative_offset", func(q *driver.Query) { q.ReadSelection.Offset = -1 }},
			{"oversize_page", func(q *driver.Query) { q.ReadSelection.Limit = 1001 }},
			{"duplicate", func(q *driver.Query) { q.ReadSelection.Candidates[1].Ref = q.ReadSelection.Candidates[0].Ref }},
			{"ordinal", func(q *driver.Query) { q.ReadSelection.Candidates[1].Ordinal = 9 }},
			{"candidate_branch", func(q *driver.Query) { q.ReadSelection.Candidates[1].Ref.Branch = model.Natural64(1) }},
			{"unknown_node", func(q *driver.Query) { q.ReadSelection.Predicate.Op = "grant_exists" }},
			{"mixed_node", func(q *driver.Query) { q.ReadSelection.Predicate.IDs = []string{"z"} }},
			{"bad_not", func(q *driver.Query) { q.ReadSelection.Predicate = driver.SelectionExpr{Op: driver.SelectionNot} }},
		}
		for _, tc := range changes {
			t.Run(tc.name, func(t *testing.T) {
				q := driver.Query{ReadSelection: &driver.ReadSelection{View: view, Candidates: append([]driver.ReadCandidate(nil), candidates...), Predicate: all, Limit: 3}}
				tc.mutate(&q)
				probe.mu.Lock()
				probe.calls = 0
				probe.mu.Unlock()
				p, e := r.Query(ctx, q)
				if e == nil || p.ReadSelection != nil || len(p.Items) != 0 || p.NextCursor != nil {
					t.Fatalf("malformed mode did not atomically refuse: %+v %v", p, e)
				}
				probe.mu.Lock()
				calls := probe.calls
				probe.mu.Unlock()
				if calls != 0 {
					t.Fatal("invalid descriptor reached SQL")
				}
			})
		}
	})
	t.Run("ordinary_nil_slot", func(t *testing.T) {
		var cursor *driver.Cursor
		var got []driver.Record
		for {
			p, e := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Limit: 2, Cursor: cursor})
			if e != nil || p.ReadSelection != nil {
				t.Fatalf("ordinary Query changed: %+v %v", p, e)
			}
			got = append(got, p.Items...)
			cursor = p.NextCursor
			if cursor == nil {
				break
			}
		}
		refs := make([]driver.Ref, len(got))
		for i, row := range got {
			refs[i] = row.Ref
		}
		point, e := r.Get(ctx, refs)
		if e != nil || len(got) != len(ids) || !reflect.DeepEqual(point, got) {
			t.Fatalf("ordinary pages/Get differ: got=%d %v", len(got), e)
		}
	})
	t.Run("transport_failure_discards_partial_page", func(t *testing.T) {
		for _, fault := range []string{"query", "iteration", "scan", "close", "short", "duplicate", "total", "mixed_null", "cancel"} {
			t.Run(fault, func(t *testing.T) {
				faultReader := getReader(t, ctx, observed, getMeta(t, ctx, s), nil)
				defer faultReader.Close()
				call, cancel := context.WithCancel(ctx)
				defer cancel()
				probe.mu.Lock()
				probe.fault, probe.cancel = fault, cancel
				probe.mu.Unlock()
				p, e := faultReader.Query(call, driver.Query{ReadSelection: &driver.ReadSelection{View: view, Candidates: candidates, Predicate: all, Limit: 3}})
				probe.mu.Lock()
				probe.fault, probe.cancel = "", nil
				probe.mu.Unlock()
				if e == nil || p.ReadSelection != nil || len(p.Items) != 0 || p.NextCursor != nil {
					t.Fatalf("%s exposed a partial result: %+v %v", fault, p, e)
				}
				t.Logf("READY_SELECTION_ATOMIC_ERROR engine=sqlite fault=%s error=%v", fault, e)
			})
		}
	})
	// Snapshot was current when pinned. Later commits must not change it.
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: testRef("later"), Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	p, e := r.Query(ctx, driver.Query{ReadSelection: &driver.ReadSelection{View: view, Candidates: candidates, Predicate: all, Limit: 3}})
	if e != nil || p.ReadSelection == nil || p.ReadSelection.Total != 8 {
		t.Fatalf("pinned authority changed after later commit: %+v %v", p, e)
	}
	historical := getReader(t, ctx, observed, getMeta(t, ctx, s), view.At)
	p, e = historical.Query(ctx, driver.Query{ReadSelection: &driver.ReadSelection{View: historical.Snapshot(), Candidates: candidates, Predicate: all, Limit: 3}})
	if e == nil || p.ReadSelection != nil {
		t.Fatalf("historical selection accepted: %+v %v", p, e)
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	ref := testRef("staged-selection")
	if _, err = tx.Lock(ctx, []driver.LockRequest{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	p, e = tx.Query(ctx, driver.Query{ReadSelection: &driver.ReadSelection{View: tx.Snapshot(), Candidates: candidates, Predicate: all, Limit: 3}})
	if e == nil || p.ReadSelection != nil {
		t.Fatalf("writer/staged selection accepted: %+v %v", p, e)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	p, e = r.Query(canceled, driver.Query{ReadSelection: &driver.ReadSelection{View: view, Candidates: candidates, Predicate: all, Limit: 3}})
	if !errors.Is(e, context.Canceled) || p.ReadSelection != nil {
		t.Fatalf("cancellation not atomic: %+v %v", p, e)
	}
}

package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Model the interval after the socket's deadline but before the context timer
// callback. Err and Done still belong to the parent; no scheduling race is
// needed to exercise the deadline/Err distinction deterministically.
type queryPendingDeadline struct {
	context.Context
	at time.Time
}

func (c queryPendingDeadline) Deadline() (time.Time, bool) { return c.at, true }

type querySQLStateFailure struct{ error }

func (querySQLStateFailure) SQLState() string { return "40001" }
func (e querySQLStateFailure) Unwrap() error  { return e.error }

func TestQueryDeadlineErrorPreservesIndependentFailures(t *testing.T) {
	ctx := context.Background()
	expired := queryPendingDeadline{Context: ctx, at: time.Now().Add(-time.Second)}
	future := queryPendingDeadline{Context: ctx, at: time.Now().Add(time.Hour)}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	canceledDeadline := queryPendingDeadline{Context: canceled, at: expired.at}
	timeout := &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded}
	wrapped := fmt.Errorf("write failed: %w", timeout)
	sqlState := querySQLStateFailure{wrapped}
	var sqlStateTimeout net.Error
	if !errors.As(sqlState, &sqlStateTimeout) || !sqlStateTimeout.Timeout() {
		t.Fatal("SQLSTATE control must expose its underlying network timeout")
	}
	connectionError := &net.OpError{Op: "write", Net: "tcp", Err: errors.New("connection reset")}
	joined := errors.Join(timeout, driver.ErrConflict)
	if expired.Err() != nil || expired.Done() != nil {
		t.Fatal("control must exercise an elapsed deadline before context cancellation")
	}
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		err      error
		deadline bool
	}{
		{"socket_deadline_before_context_callback", expired, timeout, true},
		{"wrapped_socket_deadline", expired, wrapped, true},
		{"no_operation_deadline", ctx, wrapped, false},
		{"operation_deadline_not_reached", future, wrapped, false},
		{"independent_cancellation", canceledDeadline, wrapped, false},
		{"non_timeout_network_failure", expired, connectionError, false},
		{"timeout_text_is_not_a_timeout", expired, errors.New("write failed: i/o timeout"), false},
		{"sqlstate_with_timeout_cause", expired, sqlState, false},
		{"joined_independent_failure", expired, joined, false},
		{"wrapped_joined_independent_failure", expired, fmt.Errorf("query failed: %w", joined), false},
		{"ordinary_storage_failure", expired, driver.ErrConflict, false},
		{"existing_context_deadline", expired, context.DeadlineExceeded, false},
		{"success_after_deadline", expired, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := queryDeadlineError(tc.ctx, tc.err)
			if !tc.deadline {
				if got != tc.err {
					t.Fatalf("independent error changed: got=%v want=%v", got, tc.err)
				}
				return
			}
			if !errors.Is(got, context.DeadlineExceeded) || !errors.Is(got, tc.err) {
				t.Fatalf("deadline or original cause lost: %v", got)
			}
			var network net.Error
			if !errors.As(got, &network) || !network.Timeout() {
				t.Fatalf("network timeout no longer inspectable: %v", got)
			}
		})
	}
}

// Exercise the public Query boundary with a real pgx call blocked by a real
// table lock. The control above covers the narrower socket/context timer race;
// this test also requires no partial page and a working fresh snapshot after
// the canceled connection is discarded.
func TestQueryDeadlineWhilePostgresReadIsBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	ref := testRef("query-deadline")
	tx, h := beginTest(t, s, lease, testLock(ref))
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
		t.Fatal(err)
	}
	h.Changes = []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}
	if _, err := tx.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	r := testRead(t, s, nil)
	blocker, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.ExecContext(ctx, `LOCK TABLE plug_postgres.records IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	readCtx, readCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer readCancel()
	page, err := r.Query(readCtx, driver.Query{Limit: 1})
	if !errors.Is(err, context.DeadlineExceeded) || len(page.Items) != 0 || page.NextCursor != nil {
		t.Fatalf("blocked Query returned page=%+v err=%v", page, err)
	}
	// A timed-out pgx connection may already be closed. Finish that reader
	// before checking a fresh one; repeated cleanup remains idempotent.
	_ = r.Close()
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	page, err = testRead(t, s, nil).Query(ctx, driver.Query{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Items[0].Ref.ID != ref.ID || string(page.Items[0].Body) != `{"value":1}` {
		t.Fatalf("fresh Query after canceled read returned page=%+v err=%v", page, err)
	}
}

// Keep real PostgreSQL metadata/snapshot acquisition, then supply the actual
// normalizer's error at the public Reader.Query boundary. The deterministic
// socket/context-timer race is covered without depending on timer scheduling
// or exporting the driver's private error type to viewops.
type queryDeadlineViewStore struct {
	*Store
	cause                    error
	queryCalls, readerCloses int
}

func (s *queryDeadlineViewStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &queryDeadlineViewReader{Reader: r, store: s}, nil
}

type queryDeadlineViewReader struct {
	driver.Reader
	store *queryDeadlineViewStore
}

func (r *queryDeadlineViewReader) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	r.store.queryCalls++
	return driver.Page[driver.Record]{}, r.store.cause
}

func (r *queryDeadlineViewReader) Close() error {
	r.store.readerCloses++
	return r.Reader.Close()
}

func TestQueryDeadlineNormalizerComposesWithPublicView(t *testing.T) {
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	expired := queryPendingDeadline{Context: context.Background(), at: time.Now().Add(-time.Second)}
	future := queryPendingDeadline{Context: context.Background(), at: time.Now().Add(time.Hour)}
	network := &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded}
	normalized := queryDeadlineError(expired, network)
	if !errors.Is(normalized, context.DeadlineExceeded) || !errors.Is(normalized, network) || errors.Is(network, context.DeadlineExceeded) {
		t.Fatal("control must use the production normalizer's custom deadline over a non-context network cause")
	}
	independent := driver.ErrConflict
	for _, tc := range []struct {
		name  string
		cause error
		busy  bool
	}{
		{"normalized_timeout", normalized, true},
		{"fmt_wrapped_normalized_timeout", fmt.Errorf("query: %w", normalized), true},
		{"homogeneous_context_join", errors.Join(normalized, context.DeadlineExceeded), true},
		{"independent_join", errors.Join(normalized, independent), false},
		{"nested_independent_join", errors.Join(fmt.Errorf("storage: %w", independent), errors.Join(normalized, context.DeadlineExceeded)), false},
		{"fmt_wrapped_independent_join", fmt.Errorf("query: %w", errors.Join(normalized, independent)), false},
		// The production normalizer refuses joined inputs. An outer custom-Is
		// wrapper must not allow a mapper to bypass branch validation either.
		{"custom_deadline_over_independent_join", queryDeadlineFailure{cause: errors.Join(network, independent)}, false},
		{"mixed_context_join", errors.Join(normalized, context.Canceled), false},
		{"fmt_wrapped_mixed_context_join", fmt.Errorf("query: %w", errors.Join(normalized, context.Canceled)), false},
		{"operation_deadline_not_reached", queryDeadlineError(future, network), false},
		{"sqlstate_with_timeout_cause", queryDeadlineError(expired, querySQLStateFailure{network}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &queryDeadlineViewStore{Store: s, cause: tc.cause}
			svc, err := viewops.New(store, lease, viewops.Config{})
			if err != nil {
				t.Fatal(err)
			}
			result, err := svc.Run(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, viewops.RunRequest{
				CountOnly: true, Definition: &viewops.Definition{Filter: &viewops.Filter{}},
			})
			if store.queryCalls != 1 || store.readerCloses != 1 {
				t.Fatalf("public view did not consume and close the Query reader: calls=%d closes=%d err=%v", store.queryCalls, store.readerCloses, err)
			}
			if !reflect.DeepEqual(result, viewops.Result{}) {
				t.Fatalf("failed public view returned a partial result: %+v", result)
			}
			if !tc.busy {
				if err != tc.cause {
					t.Fatalf("public view changed an independent cause: got=%v want=%v", err, tc.cause)
				}
				return
			}
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "busy" || !typed.Data.Retryable || typed.Data.Hint != "view read was canceled or exceeded its deadline" {
				t.Fatalf("production-normalized deadline lost public busy classification: %v", err)
			}
		})
	}
}

func TestStagedGetReportsOnlyMaterializedBodies(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	ref := testRef("staged-payload-accounting")
	body := []byte(`{"staged":"payload"}`)
	tx, _ := beginTest(t, s, lease, testLock(ref))
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: body}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		refs []model.Ref
	}{
		{name: "single", refs: []model.Ref{ref}},
		{name: "duplicate", refs: []model.Ref{ref, ref}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Observe only the real staged Get, excluding Put and verification
			// reads. Each returned body must be an independent materialization.
			work := &selectionSQLWork{}
			rows, err := tx.Get(driver.WithReadWorkObserver(ctx, work), tc.refs)
			if err != nil || len(rows) != len(tc.refs) {
				t.Fatalf("staged Get returned %d rows, want %d: %v", len(rows), len(tc.refs), err)
			}
			for i, row := range rows {
				if row.Ref.ID != ref.ID || !bytes.Equal(row.Body, body) {
					t.Fatalf("staged result %d changed: %+v", i, row)
				}
			}
			want := map[driver.ReadWorkKind]int64{
				driver.ReadGetCalls:       1,
				driver.ReadPayloadRecords: int64(len(tc.refs)),
				driver.ReadPayloadBytes:   int64(len(tc.refs) * len(body)),
			}
			if !reflect.DeepEqual(work.counts, want) {
				t.Fatalf("staged Get work=%v, want actual body copies=%v", work.counts, want)
			}
			t.Logf("staged Get: calls=1 materialized_bodies=%d body_bytes=%d", len(tc.refs), len(tc.refs)*len(body))
			rows[0].Body[0] = '!'
			for _, row := range rows[1:] {
				if !bytes.Equal(row.Body, body) {
					t.Fatal("duplicate result bodies alias")
				}
			}
			again, err := tx.Get(ctx, []model.Ref{ref})
			if err != nil || len(again) != 1 || !bytes.Equal(again[0].Body, body) {
				t.Fatalf("returned body aliases staged bytes: rows=%+v err=%v", again, err)
			}
		})
	}
}

func TestMissingRecordReadsDoNotProbeVersionHistory(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	first := seedTest(t, s, lease, "present", `{"value":1}`)
	r := testRead(t, s, nil).(*reader)
	historyScans := func(r *reader) int64 {
		t.Helper()
		var scans int64
		if err := r.sql.QueryRowContext(ctx, `SELECT seq_scan+coalesce(idx_scan,0) FROM pg_stat_xact_user_tables WHERE relid='plug_postgres.versions'::regclass`).Scan(&scans); err != nil {
			t.Fatal(err)
		}
		return scans
	}
	refs := []model.Ref{testRef("present")}
	keys := []driver.LockKey{testLock(refs[0])}
	for i := 0; i < 512; i++ {
		ref := testRef(fmt.Sprintf("absent-%04d", i))
		refs = append(refs, ref)
		keys = append(keys, testLock(ref))
	}
	refs = append(refs, refs[0])
	sort.Slice(keys, func(i, j int) bool { a, _ := keyID(keys[i]); b, _ := keyID(keys[j]); return a < b })
	before := historyScans(r)
	rows, err := r.Get(ctx, refs)
	if err != nil || len(rows) != 2 || rows[0].Ref.ID != "present" || rows[1].Ref.ID != "present" {
		t.Fatalf("current batch lost hits, misses or duplicate order: %+v %v", rows, err)
	}
	states, err := r.Observe(ctx, keys)
	if err != nil || len(states) != len(keys) {
		t.Fatalf("current batch observations: %+v %v", states, err)
	}
	for _, state := range states {
		if state.Key.Ref.ID == "present" {
			if !state.Present || state.Record == nil || state.Revision != first.Revision {
				t.Fatalf("current hit lost its revision: %+v", state)
			}
		} else if state.Present || state.Record != nil || !state.Revision.IsZero() {
			t.Fatalf("current absence acquired a record or revision: %+v", state)
		}
	}
	missingKey, err := recordKey(testRef("single-absent"))
	if err != nil {
		t.Fatal(err)
	}
	if _, exists, err := r.recordAt(ctx, missingKey); err != nil || exists {
		t.Fatalf("single physical absence: exists=%t error=%v", exists, err)
	}
	if got := historyScans(r) - before; got != 0 {
		t.Fatalf("physical absences performed %d version-history scans, want none", got)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	seedTest(t, s, lease, "future", `{"value":2}`)
	past := testRead(t, s, &first.Revision).(*reader)
	before = historyScans(past)
	rows, err = past.Get(ctx, []model.Ref{testRef("future"), testRef("present")})
	if err != nil || len(rows) != 1 || rows[0].Ref.ID != "present" || rows[0].Revision != first.Revision {
		t.Fatalf("historical read disclosed a future row or lost its hit: %+v %v", rows, err)
	}
	if got := historyScans(past) - before; got == 0 {
		t.Fatal("a newer physical row did not use historical reconstruction")
	}
}

func TestBatchedReaderPreservesHistoryMissesAndStagedOverlays(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	a, b, missing := testRef("batch-a"), testRef("batch-b"), testRef("batch-missing")
	fact := driver.LockKey{Kind: driver.LockReadFact, Fact: "reader/batch"}
	derived := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "batch"}}
	keys := []driver.LockKey{testLock(a), testLock(b), testLock(missing), fact, driver.GraphBlockingSentinelLock, derived}
	sort.Slice(keys, func(i, j int) bool { left, _ := keyID(keys[i]); right, _ := keyID(keys[j]); return left < right })
	assertQuery := func(reader driver.Reader, wantIDs []string, wantBody string) {
		t.Helper()
		var cursor *driver.Cursor
		var ids []string
		for pageNumber := 0; ; pageNumber++ {
			if pageNumber > 3 {
				t.Fatal("query paging did not terminate")
			}
			page, err := reader.Query(ctx, driver.Query{Filter: map[string]any{"kind": "test"}, Limit: 1, Cursor: cursor})
			if err != nil || page.AsOf.String() != reader.Snapshot().At.String() {
				t.Fatalf("query lost its snapshot: page=%+v err=%v", page, err)
			}
			for _, row := range page.Items {
				ids = append(ids, row.Ref.ID)
				if string(row.Body) != wantBody {
					t.Fatalf("query reconstructed wrong content: %+v", row)
				}
			}
			cursor = page.NextCursor
			if cursor == nil {
				break
			}
		}
		if !reflect.DeepEqual(ids, wantIDs) {
			t.Fatalf("query membership/order=%v want=%v", ids, wantIDs)
		}
	}
	initial := driver.DerivedRow{Key: "batch", Value: []byte("old")}
	tx, header := beginTest(t, s, lease, keys...)
	for _, ref := range []model.Ref{a, b} {
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"value":1}`)}); err != nil {
			t.Fatal(err)
		}
	}
	assertQuery(tx, []string{a.ID, b.ID}, `{"value":1}`)
	// A revision-zero backing snapshot is empty, but the transaction must
	// retain its own staged records and observations before its first commit.
	if rows, err := tx.Get(ctx, []model.Ref{a, missing, b}); err != nil || len(rows) != 2 || rows[0].Ref.ID != a.ID || rows[1].Ref.ID != b.ID {
		t.Fatalf("revision-zero staged records: %+v %v", rows, err)
	}
	if states, err := tx.Observe(ctx, keys); err != nil {
		t.Fatal(err)
	} else {
		for _, state := range states {
			if state.Key.Kind == driver.LockRecord && (state.Key.Ref.ID == a.ID || state.Key.Ref.ID == b.ID) && (!state.Present || state.Record == nil || string(state.Record.Body) != `{"value":1}`) {
				t.Fatalf("revision-zero staged observation lost: %+v", state)
			}
		}
	}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{initial}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpReadFacts(ctx, []driver.ReadFact{fact.Fact}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	header.Changes = []model.ChangeRecord{{Ref: a, Op: model.OpCreate}, {Ref: b, Op: model.OpCreate}}
	first, err := tx.Commit(ctx, header)
	if err != nil {
		t.Fatal(err)
	}
	tx, header = beginTest(t, s, lease, keys...)
	if err := tx.Put(ctx, driver.Record{Ref: a, Kind: "test", Body: []byte(`{"value":2}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, []driver.DerivedRow{initial}, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpReadFacts(ctx, []driver.ReadFact{fact.Fact}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	header.Changes = []model.ChangeRecord{{Ref: a, Op: model.OpPatch, FromRevision: first.Revision}, {Ref: b, Op: model.OpDelete, FromRevision: first.Revision}}
	second, err := tx.Commit(ctx, header)
	if err != nil {
		t.Fatal(err)
	}
	zero := model.Natural64(0)
	empty := testRead(t, s, &zero)
	assertQuery(empty, nil, "")
	if rows, err := empty.Get(ctx, []model.Ref{a, b, missing}); err != nil || len(rows) != 0 {
		t.Fatalf("revision zero disclosed a later record: %+v %v", rows, err)
	}
	if states, err := empty.Observe(ctx, keys); err != nil {
		t.Fatal(err)
	} else {
		for _, state := range states {
			if state.Present || !state.Revision.IsZero() || state.Record != nil || state.Derived != nil {
				t.Fatalf("revision zero disclosed a later key state: %+v", state)
			}
		}
	}
	for _, historical := range []bool{true, false} {
		var at *model.Revision
		if historical {
			at = &first.Revision
		}
		r := testRead(t, s, at)
		rows, err := r.Get(ctx, []model.Ref{b, missing, a, a})
		if err != nil {
			t.Fatal(err)
		}
		wantCount, wantBody := 2, `{"value":2}`
		if historical {
			wantCount, wantBody = 3, `{"value":1}`
		}
		if len(rows) != wantCount || string(rows[len(rows)-1].Body) != wantBody || rows[len(rows)-1].Ref.ID != a.ID {
			t.Fatalf("history=%t rows=%+v", historical, rows)
		}
		wantIDs := []string{a.ID}
		if historical {
			wantIDs = append(wantIDs, b.ID)
		}
		assertQuery(r, wantIDs, wantBody)
		rows[len(rows)-1].Body[0] = '!'
		if rows[len(rows)-2].Body[0] != '{' {
			t.Fatal("duplicate result bodies alias")
		}
		states, err := r.Observe(ctx, keys)
		if err != nil || len(states) != len(keys) {
			t.Fatalf("history=%t observations=%+v err=%v", historical, states, err)
		}
		for i, state := range states {
			if !reflect.DeepEqual(state.Key, keys[i]) {
				t.Fatal("batch observation order changed")
			}
			if keys[i].Kind == driver.LockRecord && keys[i].Ref.ID == missing.ID {
				if state.Present || !state.Revision.IsZero() || state.Record != nil {
					t.Fatalf("missing record was not retained: %+v", state)
				}
				continue
			}
			wantRevision := second.Revision
			if historical {
				wantRevision = first.Revision
			}
			deleted := !historical && (keys[i].Kind == driver.LockDerived || keys[i].Kind == driver.LockRecord && keys[i].Ref.ID == b.ID)
			if state.Revision.String() != wantRevision.String() || state.Present == deleted || deleted && (state.Record != nil || state.Derived != nil) {
				t.Fatalf("history=%t hit/tombstone revision changed: %+v", historical, state)
			}
		}
	}
	// Transaction reads must still see only their own staged replacements and
	// removals. The batched reader must not substitute any retained read cache.
	tx, _ = beginTest(t, s, lease, keys...)
	if err := tx.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []model.Ref{b, missing} {
		if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"staged":true}`)}); err != nil {
			t.Fatal(err)
		}
	}
	staged := driver.DerivedRow{Key: "batch", Value: []byte("staged")}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{staged}); err != nil {
		t.Fatal(err)
	}
	assertQuery(tx, []string{b.ID, missing.ID}, `{"staged":true}`)
	rows, err := tx.Get(ctx, []model.Ref{b, a, missing, b})
	if err != nil || len(rows) != 3 || rows[0].Ref.ID != b.ID || rows[1].Ref.ID != missing.ID || rows[2].Ref.ID != b.ID {
		t.Fatalf("staged Get=%+v err=%v", rows, err)
	}
	states, err := tx.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if state.Key.Kind == driver.LockRecord {
			if state.Key.Ref.ID == a.ID {
				if state.Present || state.Record != nil {
					t.Fatal("staged deletion was reused")
				}
			} else if !state.Present || state.Record == nil || string(state.Record.Body) != `{"staged":true}` {
				t.Fatalf("staged record missing: %+v", state)
			}
		} else if state.Key.Kind == driver.LockDerived && (!state.Present || state.Derived == nil || !reflect.DeepEqual(*state.Derived, staged)) {
			t.Fatalf("staged derived row missing: %+v", state)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := tx.Query(canceled, driver.Query{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Query: %v", err)
	}
	if _, err := tx.Get(canceled, []model.Ref{a, b}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Get: %v", err)
	}
	if _, err := tx.Observe(canceled, keys); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Observe: %v", err)
	}
}

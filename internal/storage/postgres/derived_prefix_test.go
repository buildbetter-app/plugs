package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/jackc/pgx/v5"
)

// Capture the statement and binds from the real optional reader method. The
// subsequent EXPLAIN runs on that reader's pinned transaction, without changing
// planner settings or replacing the method with a separately maintained query.
type prefixQueryCaptureKey struct{}
type prefixQueryEventKey struct{}

type prefixQueryEvent struct {
	sql      string
	args     []any
	started  time.Time
	elapsed  time.Duration
	returned bool
	err      error
	rows     int64
}

type prefixQueryCapture struct {
	mu     sync.Mutex
	events []*prefixQueryEvent
}

type prefixQueryTracer struct{}

func (prefixQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	capture, _ := ctx.Value(prefixQueryCaptureKey{}).(*prefixQueryCapture)
	if capture == nil || !strings.HasPrefix(strings.TrimSpace(data.SQL), "SELECT ") || !strings.Contains(data.SQL, "plug_postgres.states") {
		return ctx
	}
	args := data.Args
	if len(args) > 0 {
		if _, ok := args[0].(pgx.QueryResultFormatsByOID); ok {
			args = args[1:]
		}
	}
	event := &prefixQueryEvent{sql: data.SQL, started: time.Now()}
	for _, arg := range args {
		if values, ok := arg.([][]byte); ok {
			owned := make([][]byte, len(values))
			for i, value := range values {
				owned[i] = append([]byte(nil), value...)
			}
			arg = owned
		}
		event.args = append(event.args, arg)
	}
	capture.mu.Lock()
	capture.events = append(capture.events, event)
	capture.mu.Unlock()
	return context.WithValue(ctx, prefixQueryEventKey{}, event)
}

func (prefixQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	event, _ := ctx.Value(prefixQueryEventKey{}).(*prefixQueryEvent)
	if event == nil {
		return
	}
	capture := ctx.Value(prefixQueryCaptureKey{}).(*prefixQueryCapture)
	capture.mu.Lock()
	event.elapsed, event.returned, event.err, event.rows = time.Since(event.started), true, data.Err, data.CommandTag.RowsAffected()
	capture.mu.Unlock()
}

func pgPrefixWrite(t *testing.T, s *Store, lease driver.ActiveLease, del, add []driver.DerivedRow) (driver.WriteTx, driver.ChangesetHeader) {
	t.Helper()
	byID := map[string]driver.LockKey{}
	anchor := testLock(testRef("prefix-control-anchor"))
	id, _ := keyID(anchor)
	byID[id] = anchor
	sentinel, err := driver.NewClosureSentinel("prefix-control-anchor")
	if err != nil {
		t.Fatal(err)
	}
	predicate := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
	id, _ = keyID(predicate)
	byID[id] = predicate
	for _, rows := range [][]driver.DerivedRow{del, add} {
		for _, row := range rows {
			key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: row.Key}}
			id, err := keyID(key)
			if err != nil {
				t.Fatal(err)
			}
			byID[id] = key
		}
	}
	keys := make([]driver.LockKey, 0, len(byID))
	for _, key := range byID {
		keys = append(keys, key)
	}
	tx, h := beginTest(t, s, lease, keys...)
	if err := tx.PutDerived(context.Background(), driver.DerivedAttribute, del, add); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(context.Background(), []driver.Sentinel{sentinel}); err != nil {
		t.Fatal(err)
	}
	return tx, h
}

func pgPrefixCommit(t *testing.T, s *Store, lease driver.ActiveLease, del, add []driver.DerivedRow) model.Revision {
	t.Helper()
	ctx := context.Background()
	tx, h := pgPrefixWrite(t, s, lease, del, add)
	ref := testRef("prefix-control-anchor")
	states, err := tx.Observe(ctx, []driver.LockKey{testLock(ref)})
	if err != nil || len(states) != 1 {
		t.Fatalf("anchor observation=%+v: %v", states, err)
	}
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"control":true}`)}); err != nil {
		t.Fatal(err)
	}
	op := model.OpCreate
	if states[0].Present {
		op = model.OpPatch
	}
	h.Changes = []model.ChangeRecord{{Ref: ref, Op: op, FromRevision: states[0].Revision}}
	entry, err := tx.Commit(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	return entry.Revision
}

// Two real physical populations with an unchanged 100-prefix empty result.
// The regression bounds executor work rather than noisy wall time: neither an
// index traversal nor a sequential scan may inspect unrelated states. Every
// states access must have both parameterized binary key-range index bounds.
func TestDerivedPrefixPinnedRangeProbeBound(t *testing.T) {
	s, lease := openCommitQueryTest(t, prefixQueryTracer{})
	for offset := 0; offset < 20000; offset += 1000 {
		add := make([]driver.DerivedRow, 1000)
		for i := range add {
			add[i] = driver.DerivedRow{Key: fmt.Sprintf("outside/%06d", offset+i), Value: []byte("unrelated")}
		}
		pgPrefixCommit(t, s, lease, nil, add)
		population := offset + len(add)
		if population != 2000 && population != 20000 {
			continue
		}
		for _, analyzed := range []bool{false, true} {
			if analyzed {
				if _, err := s.db.ExecContext(context.Background(), `ANALYZE plug_postgres.states`); err != nil {
					t.Fatal(err)
				}
			}
			t.Run(fmt.Sprintf("population_%d/analyzed_%t", population, analyzed), func(t *testing.T) {
				r := testRead(t, s, nil).(*reader)
				defer r.Close()
				var physical int
				// Each real publication also writes one required closure-predicate
				// history row. Count those actual rows rather than hiding them.
				wantPhysical := population + population/1000
				if err := r.sql.QueryRowContext(context.Background(), `SELECT count(*) FROM plug_postgres.states`).Scan(&physical); err != nil || physical != wantPhysical {
					t.Fatalf("actual physical states=%d want=%d: %v", physical, wantPhysical, err)
				}
				prefixes := make([]string, 100)
				for i := range prefixes {
					prefixes[i] = fmt.Sprintf("absent/%03d/", i)
				}
				capture := &prefixQueryCapture{}
				ctx := context.WithValue(context.Background(), prefixQueryCaptureKey{}, capture)
				rows, err := r.DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, prefixes)
				if err != nil || len(rows) != 0 {
					t.Fatalf("actual prefix read rows=%d: %v", len(rows), err)
				}
				capture.mu.Lock()
				events := append([]*prefixQueryEvent(nil), capture.events...)
				capture.mu.Unlock()
				if len(events) != 1 {
					t.Fatalf("actual prefix statements=%d want=1", len(events))
				}
				event := events[0]
				if !event.returned || event.err != nil || event.rows != 0 || len(event.args) != 3 {
					t.Fatalf("unreconciled real query event: %+v", event)
				}
				for _, arg := range event.args[:2] {
					values, ok := arg.([][]byte)
					if !ok || len(values) != 100 {
						t.Fatalf("actual prefix bind=%T count=%d want=100", arg, len(values))
					}
				}
				if event.args[2] != decimal(r.at) {
					t.Fatalf("query revision=%v want pinned=%d", event.args[2], r.at)
				}
				var raw []byte
				if err := r.sql.QueryRowContext(context.Background(), "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+event.sql, event.args...).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				t.Logf("W9_PREFIX_PROBE_PLAN physical_states=%d analyzed=%t prefixes=100 real_rows=0 real_sql_ms=%.6f plan=%s", physical, analyzed, float64(event.elapsed)/float64(time.Millisecond), raw)
				var plan any
				if err := json.Unmarshal(raw, &plan); err != nil {
					t.Fatal(err)
				}
				var stateNodes, unbounded int
				var stateRows float64
				keyRange := func(condition string) bool {
					return strings.Contains(condition, "key >= ") && strings.Contains(condition, "key < ")
				}
				var walk func(any)
				walk = func(value any) {
					switch node := value.(type) {
					case map[string]any:
						if node["Relation Name"] == "states" {
							stateNodes++
							loops, _ := node["Actual Loops"].(float64)
							for _, key := range []string{"Actual Rows", "Rows Removed by Filter", "Rows Removed by Index Recheck"} {
								count, _ := node[key].(float64)
								stateRows += loops * count
							}
							condition, _ := node["Index Cond"].(string)
							bounded := keyRange(condition)
							if node["Node Type"] == "Bitmap Heap Scan" {
								bounded = false
								recheck, _ := node["Recheck Cond"].(string)
								children, _ := node["Plans"].([]any)
								if len(children) == 1 {
									child, _ := children[0].(map[string]any)
									indexCond, _ := child["Index Cond"].(string)
									bounded = keyRange(recheck) && keyRange(indexCond) && child["Node Type"] == "Bitmap Index Scan" && child["Index Name"] == "states_pkey" && child["Actual Loops"] == loops
								}
							}
							if !bounded || loops != 100 {
								unbounded++
							}
						}
						for _, child := range node {
							walk(child)
						}
					case []any:
						for _, child := range node {
							walk(child)
						}
					}
				}
				walk(plan)
				t.Logf("W9_PREFIX_PROBE_WORK physical_states=%d analyzed=%t states_nodes=%d unbounded_nodes=%d examined_state_rows=%.0f", physical, analyzed, stateNodes, unbounded, stateRows)
				if stateNodes == 0 || unbounded != 0 || stateRows != 0 {
					t.Errorf("empty prefix read inspected unrelated states: nodes=%d unbounded=%d examined=%.0f", stateNodes, unbounded, stateRows)
				}
			})
		}
	}
}

func pgRequirePrefixRows(t *testing.T, r driver.Reader, prefixes []string, want []driver.DerivedRow) {
	t.Helper()
	before := append([]string(nil), prefixes...)
	rows, err := r.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(context.Background(), driver.DerivedAttribute, prefixes)
	if err != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("prefix rows=%+v want=%+v: %v", rows, want, err)
	}
	if !reflect.DeepEqual(prefixes, before) {
		t.Fatal("mutated caller prefixes")
	}
	if len(rows) > 0 {
		rows[0].Value[0] = '!'
		again, err := r.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(context.Background(), driver.DerivedAttribute, prefixes)
		if err != nil || !reflect.DeepEqual(again, want) {
			t.Fatalf("caller mutated retained row bytes: %+v: %v", again, err)
		}
	}
}

func TestDerivedPrefixUnionSnapshotHistoryAndStaging(t *testing.T) {
	s, lease := openCommitQueryTest(t)
	a := driver.DerivedRow{Key: "a/one", Value: []byte("one")}
	b := driver.DerivedRow{Key: "a/two", Value: []byte("two")}
	c := driver.DerivedRow{Key: "b/three", Value: []byte("three")}
	initial := []driver.DerivedRow{a, b, c, {Key: "ab/outside", Value: []byte("outside")}}
	prefixes := []string{"b/", "a/two", "a/", "a/", "missing/"}
	want := []driver.DerivedRow{a, b, c}
	// More than one write batch and more than 100 disjoint selected ranges.
	for i := 0; i < 260; i++ {
		row := driver.DerivedRow{Key: fmt.Sprintf("bucket-%03d/value", i), Value: []byte("bucket")}
		initial = append(initial, row)
		if i%2 == 0 || i >= 250 {
			prefixes = append(prefixes, fmt.Sprintf("bucket-%03d/", i))
			want = append(want, row)
		}
	}
	first := pgPrefixCommit(t, s, lease, nil, initial)
	pinned := testRead(t, s, nil)
	updated := driver.DerivedRow{Key: b.Key, Value: []byte("updated")}
	added := driver.DerivedRow{Key: "a/four", Value: []byte("four")}
	pgPrefixCommit(t, s, lease, []driver.DerivedRow{a}, []driver.DerivedRow{updated, added})
	pgRequirePrefixRows(t, pinned, prefixes, want)
	pgRequirePrefixRows(t, testRead(t, s, &first), prefixes, want)
	current := testRead(t, s, nil)
	now := append([]driver.DerivedRow{added, updated, c}, want[3:]...)
	pgRequirePrefixRows(t, current, prefixes, now)
	pgRequirePrefixRows(t, current, nil, []driver.DerivedRow{})
	all, err := current.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(context.Background(), driver.DerivedAttribute, []string{"a/", "", "bucket-000/"})
	if err != nil || len(all) != len(initial) {
		t.Fatalf("whole-table union rows=%d want=%d: %v", len(all), len(initial), err)
	}
	other, err := current.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(context.Background(), driver.DerivedAncestorClosure, []string{""})
	if err != nil || len(other) != 0 {
		t.Fatalf("table isolation rows=%+v: %v", other, err)
	}
	staged := driver.DerivedRow{Key: "a/staged", Value: []byte("staged")}
	replaced := driver.DerivedRow{Key: b.Key, Value: []byte("staged-two")}
	tx, _ := pgPrefixWrite(t, s, lease, []driver.DerivedRow{c}, []driver.DerivedRow{staged, replaced})
	pgRequirePrefixRows(t, tx, []string{"b/", "a/", "a/staged"}, []driver.DerivedRow{added, staged, replaced})
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	pgRequirePrefixRows(t, current, prefixes, now)
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := current.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(context.Background(), driver.DerivedAttribute, prefixes); !errors.Is(err, driver.ErrTxClosed) {
		t.Fatalf("closed reader: %v", err)
	}
}

func TestDerivedPrefixReadRefusalsAndMalformedHistory(t *testing.T) {
	s, lease := openCommitQueryTest(t)
	row := driver.DerivedRow{Key: "a/one", Value: []byte("one")}
	pgPrefixCommit(t, s, lease, nil, []driver.DerivedRow{row})
	r := testRead(t, s, nil).(driver.DerivedPrefixReader)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.DerivedRowsForPrefixes(canceled, driver.DerivedAttribute, []string{"a/"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err := r.DerivedRowsForPrefixes(context.Background(), "invalid", nil); err == nil {
		t.Fatal("accepted unknown table for empty input")
	}
	if _, err := r.DerivedRowsForPrefixes(context.Background(), driver.DerivedAttribute, []string{"", "bad\x00prefix"}); err == nil {
		t.Fatal("ignored invalid prefix hidden by whole-table prefix")
	}
	id, err := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: row.Key}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(context.Background(), `UPDATE plug_postgres.states SET value=$1 WHERE key=$2`, []byte("{"), []byte(id)); err != nil {
		t.Fatal(err)
	}
	fresh := testRead(t, s, nil).(driver.DerivedPrefixReader)
	if rows, err := fresh.DerivedRowsForPrefixes(context.Background(), driver.DerivedAttribute, []string{"a/"}); err == nil {
		t.Fatalf("malformed native history became successful rows: %+v", rows)
	}
}

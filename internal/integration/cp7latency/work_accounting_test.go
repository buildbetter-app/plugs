package cp7latency_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestExecutedReadWorkOnRealSQLReadersAndTransactions(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 11)
			r := f.reader(t, nil)
			defer r.Close()
			q := driver.Query{Filter: map[string]any{"kind": "bead", "id": beadRef(11).ID}, Limit: 1}
			plain, err := r.Query(context.Background(), q)
			if err != nil || len(plain.Items) != 1 || plain.Items[0].Ref.ID != beadRef(11).ID {
				t.Fatalf("unobserved control: %v", err)
			}
			nilObserved, err := r.Query(driver.WithReadWorkObserver(context.Background(), nil), q)
			if err != nil || !reflect.DeepEqual(plain, nilObserved) {
				t.Fatalf("nil observer changed query: %v", err)
			}
			var counts counters
			ctx := driver.WithReadWorkObserver(context.Background(), &counts)
			observed, err := r.Query(ctx, q)
			if err != nil || !reflect.DeepEqual(plain, observed) {
				t.Fatalf("observer changed query: %v", err)
			}
			invalid := driver.Query{Filter: map[string]any{"unsupported": "value"}}
			_, plainErr := r.Query(context.Background(), invalid)
			_, observedErr := r.Query(ctx, invalid)
			if plainErr == nil || observedErr == nil || plainErr.Error() != observedErr.Error() {
				t.Fatal("observer changed query refusal")
			}
			work, api := counts.work(), counts.value()
			var corpusBytes int64
			for i := 1; i <= f.n; i++ {
				corpusBytes += int64(len(body(t, i)))
			}
			if api.Queries != 2 || api.Returned != 1 || work.SelectorKeys < int64(f.n) || work.Payloads < int64(f.n) || work.PayloadBytes < corpusBytes || work.Payloads <= api.Returned {
				t.Fatalf("discarded candidates' executed work missing: api=%+v work=%+v", api, work)
			}
			// No query/driver wrapper is introduced by accounting. In particular,
			// the SQL reader's prefix capability is unchanged by observer use.
			if engine == "sqlite" {
				if _, ok := r.(driver.DerivedPrefixReader); !ok {
					t.Fatal("SQLite prefix capability hidden")
				}
			}
			counts.reset()
			rows, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
			if err != nil || len(rows) != 66 || counts.work().PrefixStates != 66 || counts.work().PrefixBytes == 0 {
				t.Fatalf("real prefix decode work: rows=%d work=%+v err=%v", len(rows), counts.work(), err)
			}
			// A real WriteTx.Reader sees the same observer. Old Read-only wrappers
			// could not count this work and hid optional transaction capabilities.
			tx, err := f.store.Store.Write(ctx, f.lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			if _, ok := tx.(driver.AllocatorContinuation); !ok {
				t.Fatal("allocator transaction capability hidden")
			}
			if _, ok := tx.(driver.HashAllocatorContinuation); !ok {
				t.Fatal("hash allocator transaction capability hidden")
			}
			if _, ok := tx.(driver.CheckpointGuardTx); !ok {
				t.Fatal("checkpoint transaction capability hidden")
			}
			counts.reset()
			page, err := tx.Query(ctx, q)
			if err != nil || len(page.Items) != 1 || counts.value().Queries != 1 || counts.work().Payloads < int64(f.n) {
				t.Fatalf("transaction query work escaped accounting: %+v %v", counts.work(), err)
			}
			counts.reset()
			values, err := tx.Get(ctx, []driver.Ref{beadRef(11)})
			if err != nil || len(values) != 1 || counts.value().Gets != 1 || counts.work().Payloads != 1 || counts.work().PayloadBytes != int64(len(values[0].Body)) {
				t.Fatalf("transaction Get work escaped accounting: %+v %v", counts.work(), err)
			}
			t.Logf("CP7_EXECUTED_WORK query_output=1 materialized_at_least=%d transaction_query_and_get=true prefix_states=66 sql_executor_work=UNMEASURED", f.n)
		})
	}
}

func TestReadWorkConcurrentRequestsStayIsolated(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 11)
			var observed, untouched counters
			ctx := driver.WithReadWorkObserver(context.Background(), &observed)
			other := driver.WithReadWorkObserver(context.Background(), &untouched)
			var wg sync.WaitGroup
			errs := make(chan error, 4)
			for i := 0; i < 4; i++ {
				r := f.reader(t, nil)
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer r.Close()
					_, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1})
					errs <- err
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			if observed.value().Queries != 4 || observed.value().Returned != 4 || observed.work().Payloads < 44 || untouched.value().Queries != 0 || untouched.work() != (executedReadWork{}) {
				t.Fatal("concurrent observer lost work or leaked across requests")
			}
			r := f.reader(t, nil)
			defer r.Close()
			if _, err := r.Query(other, driver.Query{Limit: 1}); err != nil {
				t.Fatal(err)
			}
			if observed.value().Queries != 4 || untouched.value().Queries != 1 {
				t.Fatal("request-local observer changed another request")
			}
		})
	}
}

func TestAggregateWorkIncludesNonEmittedSummaryProjections(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 11)
			var counts counters
			ctx := driver.WithReadWorkObserver(context.Background(), &counts)
			got, err := f.views.Run(ctx, f.user(1), groupedRequest())
			if err != nil {
				t.Fatal(err)
			}
			f.checkGrouped(t, got, 1)
			ordinary := counts.work()
			if ordinary.AggregateInputs != 11 || ordinary.AggregateGroups != 1 || ordinary.Projections != 22 {
				t.Fatalf("ordinary executed projection/aggregate work=%+v", ordinary)
			}
			counts.reset()
			req := groupedRequest()
			req.Definition.Fields = []string{}
			req.Definition.Sort = nil
			got, err = f.views.Run(ctx, f.user(1), req)
			if err != nil || len(got.Groups) != 1 || got.Groups[0].Count != 11 || len(got.Groups[0].Items) != 0 {
				t.Fatalf("summary: %+v %v", got, err)
			}
			summary := counts.work()
			if summary.AggregateInputs != 11 || summary.AggregateGroups != 1 || summary.Projections != 11 {
				t.Fatalf("empty output hid executed summary work=%+v", summary)
			}
			t.Logf("CP7_AGGREGATE_WORK ordinary=%+v fields_empty_summary=%+v emitted_summary_payload_rows=0", ordinary, summary)
		})
	}
}

// Main batches historical misses without repeating the current-row query.
// Count actual materializations, including the discarded current body, on
// every real call; duplicate output rows do not create another Scan.
func TestPostgresHistoricalReadCountsDiscardedCurrentBodies(t *testing.T) {
	f := newFixture(t, "postgres", 2)
	originalOne, originalTwo := body(t, 1), body(t, 2)
	currentOne := append(append([]byte(nil), originalOne...), ' ')
	at := f.seededAt.Revision
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: beadRef(1), Kind: "bead", Op: model.OpPatch, Body: currentOne, ChangedPaths: []string{"/fields/title"}},
		{Ref: beadRef(2), Kind: "bead", Op: model.OpDelete},
	}); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"Get", "Query", "Observe"} {
		t.Run(method, func(t *testing.T) {
			r := f.reader(t, &at)
			defer r.Close()
			read := func(ctx context.Context) ([]driver.Record, error) {
				switch method {
				case "Get":
					return r.Get(ctx, []model.Ref{beadRef(1), beadRef(2), beadRef(99), beadRef(1)})
				case "Query":
					page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"id": beadRef(1).ID}, Limit: 1})
					if err == nil && (page.AsOf != at || page.NextCursor != nil) {
						t.Fatal("historical query changed snapshot or invented a tail")
					}
					return page.Items, err
				default:
					keys := []driver.LockKey{{Kind: driver.LockRecord, Ref: refPointer(beadRef(1))}, {Kind: driver.LockRecord, Ref: refPointer(beadRef(2))}, {Kind: driver.LockRecord, Ref: refPointer(beadRef(99))}}
					states, err := r.Observe(ctx, keys)
					if err != nil {
						return nil, err
					}
					if len(states) != 3 || !states[0].Present || !states[1].Present || states[2].Present || states[2].Record != nil || !states[2].Revision.IsZero() {
						t.Fatalf("historical observations changed hit/deleted/absence semantics: %+v", states)
					}
					return []driver.Record{*states[0].Record, *states[1].Record}, nil
				}
			}
			plain, err := read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := [][]byte{originalOne, originalTwo}
			if method == "Get" {
				want = append(want, originalOne)
			}
			if method == "Query" {
				want = want[:1]
			}
			if len(plain) != len(want) {
				t.Fatalf("historical output rows=%d want=%d", len(plain), len(want))
			}
			for i, row := range plain {
				if !reflect.DeepEqual(row.Body, want[i]) || row.Revision != at {
					t.Fatalf("historical body/snapshot changed: %+v", row)
				}
			}
			nilObserved, err := read(driver.WithReadWorkObserver(context.Background(), nil))
			if err != nil || !reflect.DeepEqual(plain, nilObserved) {
				t.Fatalf("nil observer changed historical result: %v", err)
			}
			var counts counters
			for repeat := int64(1); repeat <= 2; repeat++ {
				got, err := read(driver.WithReadWorkObserver(context.Background(), &counts))
				if err != nil || !reflect.DeepEqual(plain, got) {
					t.Fatalf("observer changed historical result: %v", err)
				}
				work := counts.work()
				wantBytes := repeat * int64(len(currentOne)+len(originalOne)+len(originalTwo))
				if work.Payloads != repeat*3 || work.PayloadBytes != wantBytes {
					t.Fatalf("discarded/repeated historical work=%+v want_payloads=%d want_bytes=%d", work, repeat*3, wantBytes)
				}
				t.Logf("CP7_HISTORICAL_BODY_WORK method=%s repeat=%d output_rows=%d current_body_scans_per_call=1 historical_bodies_per_call=2 absent=1 deleted_current_body=0 payload_records=%d payload_bytes=%d", method, repeat, len(got), work.Payloads, work.PayloadBytes)
			}
		})
	}
}

func refPointer(ref model.Ref) *model.Ref { return &ref }

func TestPostgresReadWorkPreservesStagedAndErrorPrecedence(t *testing.T) {
	f := newFixture(t, "postgres", 2)
	tx, err := f.store.Write(context.Background(), f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	ref := beadRef(1)
	if _, err := tx.Lock(context.Background(), []driver.LockRequest{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(context.Background()); err != nil {
		t.Fatal(err)
	}
	staged := append(append([]byte(nil), body(t, 1)...), ' ')
	if err := tx.Put(context.Background(), driver.Record{Ref: ref, Kind: "bead", Body: staged}); err != nil {
		t.Fatal(err)
	}
	refs := []model.Ref{ref, beadRef(99), ref}
	plain, err := tx.Get(context.Background(), refs)
	if err != nil || len(plain) != 2 || !reflect.DeepEqual(plain[0].Body, staged) {
		t.Fatalf("staged control: %+v %v", plain, err)
	}
	var counts counters
	got, err := tx.Get(driver.WithReadWorkObserver(context.Background(), &counts), refs)
	if err != nil || !reflect.DeepEqual(plain, got) || counts.work().Payloads != 1 || counts.work().PayloadBytes != int64(len(staged)) {
		t.Fatalf("staged duplicate accounting/parity: %+v %v", counts.work(), err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	counts.reset()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, plainErr := tx.Get(cancelled, refs)
	_, observedErr := tx.Get(driver.WithReadWorkObserver(cancelled, &counts), refs)
	if !errors.Is(plainErr, context.Canceled) || !errors.Is(observedErr, context.Canceled) || counts.work().Payloads != 0 {
		t.Fatalf("cancellation must precede closed transaction: %v %v %+v", plainErr, observedErr, counts.work())
	}
	_, plainErr = tx.Get(context.Background(), refs)
	_, observedErr = tx.Get(driver.WithReadWorkObserver(context.Background(), &counts), refs)
	if !errors.Is(plainErr, driver.ErrTxClosed) || !errors.Is(observedErr, driver.ErrTxClosed) || counts.work().Payloads != 0 {
		t.Fatalf("closed transaction parity: %v %v", plainErr, observedErr)
	}
	if err := f.lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, plainErr = tx.Get(context.Background(), refs)
	_, observedErr = tx.Get(driver.WithReadWorkObserver(context.Background(), &counts), refs)
	var plainFenced, observedFenced driver.ErrFenced
	if !errors.As(plainErr, &plainFenced) || !errors.As(observedErr, &observedFenced) || !reflect.DeepEqual(plainFenced, observedFenced) || counts.work().Payloads != 0 {
		t.Fatalf("lease loss must precede closed transaction: %v %v", plainErr, observedErr)
	}
	t.Log("CP7_BODY_WORK_PARITY staged_duplicates=true cancellation_before_closed=true closed_before_read=true lease_loss_before_closed=true payload_work_on_refusal=0")
}

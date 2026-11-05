package cp7latency_test

import (
	"context"
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

func TestPostgresHistoricalReadCountsDiscardedCurrentBodies(t *testing.T) {
	f := newFixture(t, "postgres", 2)
	originalOne := body(t, 1)
	originalTwo := body(t, 2)
	currentOne := append(append([]byte(nil), originalOne...), ' ')
	at := f.seededAt.Revision
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: beadRef(1), Kind: "bead", Op: model.OpPatch, Body: currentOne, ChangedPaths: []string{"/fields/title"}},
		{Ref: beadRef(2), Kind: "bead", Op: model.OpDelete},
	}); err != nil {
		t.Fatal(err)
	}

	refs := []model.Ref{beadRef(1), beadRef(2), beadRef(99)}
	plainReader := f.reader(t, &at)
	plain, err := plainReader.Get(context.Background(), refs)
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) != 2 || !reflect.DeepEqual(plain[0].Body, originalOne) || !reflect.DeepEqual(plain[1].Body, originalTwo) {
		t.Fatalf("historical control lost body/snapshot/absence semantics: %+v", plain)
	}
	if err := plainReader.Close(); err != nil {
		t.Fatal(err)
	}

	observedReader := f.reader(t, &at)
	defer observedReader.Close()
	var counts counters
	observed, err := observedReader.Get(driver.WithReadWorkObserver(context.Background(), &counts), refs)
	if err != nil || !reflect.DeepEqual(plain, observed) {
		t.Fatalf("observer changed historical result: %v", err)
	}
	work := counts.work()
	wantBytes := int64(2*len(currentOne) + len(originalOne) + len(originalTwo))
	if work.Payloads != 4 || work.PayloadBytes != wantBytes {
		t.Fatalf("historical body work missed discarded/repeated materialization: work=%+v want_payloads=4 want_bytes=%d", work, wantBytes)
	}
	t.Logf("CP7_HISTORICAL_BODY_WORK output_rows=2 current_body_scans=2 historical_bodies=2 absent=1 deleted_current_body=0 payload_records=%d payload_bytes=%d", work.Payloads, work.PayloadBytes)
}

package batchsql_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// diagnosticCounts observes only the public storage boundary. It deliberately
// does not change a request, lock, timeout, or returned value.
type diagnosticCounts struct {
	mu sync.Mutex

	readerGets, readerGetRefs         int
	readerObserves, readerObserveKeys int
	readerQueries, readerQueryItems   int
	readerDerived, readerDerivedRows  int
	readerIdempotency                 int

	writeLocks, writeLockKeys, lockStates int
	writeHeads                            int
	writePuts, writePutDerivedRows        int
	writeBumps, writeBumpKeys             int
	writeCommits                          int
	lockIDs                               []string
}

func (c *diagnosticCounts) snapshot() diagnosticCounts {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := *c
	v.lockIDs = append([]string(nil), c.lockIDs...)
	return v
}

type diagnosticStore struct {
	driver.Store
	counts *diagnosticCounts
}

func (s *diagnosticStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &diagnosticReader{Reader: r, counts: s.counts}, nil
}

func (s *diagnosticStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &diagnosticWriteTx{WriteTx: tx, counts: s.counts}, nil
}

type diagnosticReader struct {
	driver.Reader
	counts *diagnosticCounts
}

func (r *diagnosticReader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	r.counts.mu.Lock()
	r.counts.readerGets++
	r.counts.readerGetRefs += len(refs)
	r.counts.mu.Unlock()
	return r.Reader.Get(ctx, refs)
}

func (r *diagnosticReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	r.counts.mu.Lock()
	r.counts.readerObserves++
	r.counts.readerObserveKeys += len(keys)
	r.counts.mu.Unlock()
	return r.Reader.Observe(ctx, keys)
}

func (r *diagnosticReader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	rows, err := r.Reader.DerivedRows(ctx, table, prefix)
	r.counts.mu.Lock()
	r.counts.readerDerived++
	r.counts.readerDerivedRows += len(rows)
	r.counts.mu.Unlock()
	return rows, err
}

func (r *diagnosticReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	page, err := r.Reader.Query(ctx, query)
	r.counts.mu.Lock()
	r.counts.readerQueries++
	r.counts.readerQueryItems += len(page.Items)
	r.counts.mu.Unlock()
	return page, err
}

func (r *diagnosticReader) IdempotencyByKey(ctx context.Context, principal, label, method, key string) (*driver.IdempotencyRecord, error) {
	r.counts.mu.Lock()
	r.counts.readerIdempotency++
	r.counts.mu.Unlock()
	return r.Reader.IdempotencyByKey(ctx, principal, label, method, key)
}

func (r *diagnosticReader) LoadAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	reader, ok := r.Reader.(driver.AllocatorReader)
	if !ok {
		return driver.OfflineAllocator{}, driver.ErrUnsupported
	}
	return reader.LoadAllocator(ctx, prefix)
}

type diagnosticWriteTx struct {
	driver.WriteTx
	counts *diagnosticCounts
}

func (t *diagnosticWriteTx) Lock(ctx context.Context, requests []driver.LockRequest) ([]driver.LockState, error) {
	ids := make([]string, 0, len(requests))
	for _, request := range requests {
		id, err := driver.EncodeLockKey(request.Key)
		if err == nil {
			ids = append(ids, id)
		}
	}
	t.counts.mu.Lock()
	t.counts.writeLocks++
	t.counts.writeLockKeys += len(requests)
	t.counts.lockIDs = append(t.counts.lockIDs, ids...)
	t.counts.mu.Unlock()
	states, err := t.WriteTx.Lock(ctx, requests)
	t.counts.mu.Lock()
	t.counts.lockStates += len(states)
	t.counts.mu.Unlock()
	return states, err
}

func (t *diagnosticWriteTx) Head(ctx context.Context) (model.Revision, error) {
	t.counts.mu.Lock()
	t.counts.writeHeads++
	t.counts.mu.Unlock()
	return t.WriteTx.Head(ctx)
}

func (t *diagnosticWriteTx) Put(ctx context.Context, record driver.Record) error {
	t.counts.mu.Lock()
	t.counts.writePuts++
	t.counts.mu.Unlock()
	return t.WriteTx.Put(ctx, record)
}

func (t *diagnosticWriteTx) PutDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	t.counts.mu.Lock()
	t.counts.writePutDerivedRows += len(del) + len(add)
	t.counts.mu.Unlock()
	return t.WriteTx.PutDerived(ctx, table, del, add)
}

func (t *diagnosticWriteTx) BumpSentinels(ctx context.Context, sentinels []driver.Sentinel) error {
	t.counts.mu.Lock()
	t.counts.writeBumps++
	t.counts.writeBumpKeys += len(sentinels)
	t.counts.mu.Unlock()
	return t.WriteTx.BumpSentinels(ctx, sentinels)
}

func (t *diagnosticWriteTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	t.counts.mu.Lock()
	t.counts.writeCommits++
	t.counts.mu.Unlock()
	return t.WriteTx.Commit(ctx, header)
}

func (t *diagnosticWriteTx) LoadAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	reader, ok := t.WriteTx.(driver.AllocatorReader)
	if !ok {
		return driver.OfflineAllocator{}, driver.ErrUnsupported
	}
	return reader.LoadAllocator(ctx, prefix)
}

func (t *diagnosticWriteTx) RestoreAllocators(ctx context.Context, values []driver.OfflineAllocator) error {
	continuation, ok := t.WriteTx.(driver.AllocatorContinuation)
	if !ok {
		return driver.ErrUnsupported
	}
	return continuation.RestoreAllocators(ctx, values)
}

func (t *diagnosticWriteTx) AllocateTopLevel(ctx context.Context, prefix, expected string) error {
	continuation, ok := t.WriteTx.(driver.AllocatorContinuation)
	if !ok {
		return driver.ErrUnsupported
	}
	return continuation.AllocateTopLevel(ctx, prefix, expected)
}

func (t *diagnosticWriteTx) AllocateChild(ctx context.Context, prefix, expected string) error {
	continuation, ok := t.WriteTx.(driver.AllocatorContinuation)
	if !ok {
		return driver.ErrUnsupported
	}
	return continuation.AllocateChild(ctx, prefix, expected)
}

// linearRecordRefComparisons reproduces the exact source-level work in the
// frozen SQLite candidate's transaction.recordRef: it scans the globally
// sorted acquired set until each record key. validatePredicates and Commit
// each call recordRef once for every staged record, so the returned count is
// the deterministic comparison work for those two passes.
func linearRecordRefComparisons(lockIDs []string, engine string, records int) (comparisons, matched int) {
	if engine != "sqlite" {
		return 0, 0
	}
	for i := 1; i <= records; i++ {
		ref := refForDiagnostic(engine, i)
		want, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: ref})
		if err != nil {
			continue
		}
		for _, got := range lockIDs {
			comparisons++
			if got == want {
				matched++
				break
			}
		}
	}
	return comparisons, matched
}

func refForDiagnostic(engine string, n int) *model.Ref {
	return &model.Ref{ID: fmt.Sprintf("pl-batch-%s-%03d", engine, n)}
}

func TestHierarchyThousandSQLiteSourceDiagnostic(t *testing.T) {
	f := newFixture(t, "sqlite")
	cs := model.ChangeSet{Ops: make([]model.Op, 1000)}
	for i := range cs.Ops {
		cs.Ops[i] = hierarchyCreate(fmt.Sprintf("row-%d", i), nil, "work")
	}

	// Preserve the existing 1001 atomic refusal and no-authorizer setup before
	// the instrumented successful attempt. This is a diagnostic, not a retry.
	m := f.meta("1000-op-diagnostic")
	before := f.state(t)
	oversized := model.ChangeSet{Ops: append(append([]model.Op{}, cs.Ops...), hierarchyCreate("row-1000", nil, "work"))}
	if _, err := f.writer.Apply(f.ctx, batchPrincipal, oversized, m); err == nil {
		t.Fatal("1001-operation refusal unexpectedly succeeded")
	} else {
		requireCode(t, err, "invalid_params")
	}
	requireSameState(t, before, f.state(t))
	if f.receipt(t, m) != nil {
		t.Fatal("1001 refusal published a receipt")
	}

	counts := &diagnosticCounts{}
	f.store = &diagnosticStore{Store: f.store, counts: counts}
	writer, metrics := pipeline.NewWithTelemetry(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID))
	f.writer = writer
	commitMeta := f.meta("1000-op-diagnostic-commit")
	started := time.Now()
	out, err := f.writer.ApplyWithOutcome(f.ctx, batchPrincipal, cs, commitMeta)
	elapsed := time.Since(started)
	phase := map[string]float64{}
	for lane, results := range metrics.Snapshot().LaneMillis {
		for result, millis := range results {
			if millis != 0 {
				phase[string(lane)+"/"+string(result)] += millis
			}
		}
	}
	snapshot := counts.snapshot()
	comparisons, matched := linearRecordRefComparisons(snapshot.lockIDs, "sqlite", len(cs.Ops))
	t.Logf("diagnostic result: err=%v elapsed_ms=%.3f phases_ms=%v", err, float64(elapsed)/float64(time.Millisecond), phase)
	t.Logf("storage boundary: gets=%d refs=%d observes=%d keys=%d derived_reads=%d rows=%d queries=%d items=%d idempotency=%d locks=%d lock_keys=%d lock_states=%d heads=%d puts=%d derived_rows=%d bump_calls=%d bump_keys=%d commits=%d", snapshot.readerGets, snapshot.readerGetRefs, snapshot.readerObserves, snapshot.readerObserveKeys, snapshot.readerDerived, snapshot.readerDerivedRows, snapshot.readerQueries, snapshot.readerQueryItems, snapshot.readerIdempotency, snapshot.writeLocks, snapshot.writeLockKeys, snapshot.lockStates, snapshot.writeHeads, snapshot.writePuts, snapshot.writePutDerivedRows, snapshot.writeBumps, snapshot.writeBumpKeys, snapshot.writeCommits)
	t.Logf("sqlite recordRef source work: acquired_keys=%d record_keys=%d matched=%d linear_comparisons_per_pass=%d linear_comparisons_validatePredicates_plus_Commit=%d", len(snapshot.lockIDs), len(cs.Ops), matched, comparisons, 2*comparisons)
	if len(snapshot.lockIDs) == 0 || matched != len(cs.Ops) {
		t.Fatalf("diagnostic did not observe the complete acquired record lock set: keys=%d matched=%d records=%d", len(snapshot.lockIDs), matched, len(cs.Ops))
	}
	if err != nil {
		var typed *model.Error
		if !errors.As(err, &typed) || typed.Data.Code != "busy" {
			t.Fatalf("1000-operation diagnostic returned unexpected error: %T %v", err, err)
		}
		after := f.state(t)
		requireSameState(t, before, after)
		if receipt := f.receipt(t, commitMeta); receipt != nil {
			t.Fatal("failed 1000-operation diagnostic published a receipt")
		}
		return
	}
	if out.Replayed || out.Commit.Revision.String() != "1" || len(out.Commit.Results) != len(cs.Ops) {
		t.Fatalf("successful diagnostic changed the operation contract: %+v", out)
	}
	committed := f.state(t)
	if len(committed.Records) != len(cs.Ops) || len(committed.Versions) != len(cs.Ops) || len(committed.Log) != 1 || len(committed.Log[0].Changes) != len(cs.Ops) {
		t.Fatalf("successful diagnostic did not publish one complete 1000-operation changeset: %+v", committed)
	}
	if receipt := f.receipt(t, commitMeta); receipt == nil || receipt.ResponsePending || receipt.Commit.Revision.String() != "1" {
		t.Fatal("successful diagnostic receipt is incomplete")
	}
	if len(snapshot.lockIDs) <= len(cs.Ops) || snapshot.writePutDerivedRows == 0 || snapshot.writeBumpKeys == 0 {
		t.Fatalf("diagnostic missed hierarchy/closure staging: %+v", snapshot)
	}
}

var _ driver.Store = (*diagnosticStore)(nil)
var _ driver.Reader = (*diagnosticReader)(nil)
var _ driver.WriteTx = (*diagnosticWriteTx)(nil)
var _ driver.AllocatorReader = (*diagnosticReader)(nil)
var _ driver.AllocatorContinuation = (*diagnosticWriteTx)(nil)

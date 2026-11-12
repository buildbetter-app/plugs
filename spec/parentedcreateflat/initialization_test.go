package parentedcreateflat

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type queryPageWork struct {
	Limit, Returned         int
	Cursor, NextCursor      bool
	MaterializationsExposed bool
	ReadWork                map[driver.ReadWorkKind]int64
}

type queryCensus struct {
	mu    sync.Mutex
	pages []queryPageWork
}

func (q *queryCensus) snapshot() []queryPageWork {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]queryPageWork(nil), q.pages...)
}

// Retain real engine capabilities while observing each actual Query call.
// Fixture readbacks use the original store and are outside this wrapper.
type censusStore struct {
	driver.Store
	driver.ResponseFinalizer
	driver.CheckpointStore
	census *queryCensus
}

type censusReader struct {
	driver.Reader
	driver.AllocatorReader
	driver.ParentCounterReader
	driver.NumericChildReader
	driver.HierarchyChildrenExcludingReader
	driver.InverseSourceLinksReader
	census *queryCensus
}

type prefixCensusReader struct {
	*censusReader
	driver.DerivedPrefixReader
}

func (s censusStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	wrapped := &censusReader{Reader: r, AllocatorReader: r.(driver.AllocatorReader), ParentCounterReader: r.(driver.ParentCounterReader), NumericChildReader: r.(driver.NumericChildReader), HierarchyChildrenExcludingReader: r.(driver.HierarchyChildrenExcludingReader), InverseSourceLinksReader: r.(driver.InverseSourceLinksReader), census: s.census}
	// PostgreSQL retains the ordinary DerivedRows fallback. Advertising an
	// absent optional capability would either panic here or change that path.
	if prefix, ok := r.(driver.DerivedPrefixReader); ok {
		return &prefixCensusReader{censusReader: wrapped, DerivedPrefixReader: prefix}, nil
	}
	return wrapped, nil
}

type forwardingReadWork struct {
	parent context.Context
	local  *readWork
}

func (w forwardingReadWork) ObserveReadWork(kind driver.ReadWorkKind, n int64) {
	w.local.ObserveReadWork(kind, n)
	driver.ReportReadWork(w.parent, kind, n)
}

func (r *censusReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	work := &readWork{counts: map[driver.ReadWorkKind]int64{}}
	page, err := r.Reader.Query(driver.WithReadWorkObserver(ctx, forwardingReadWork{parent: ctx, local: work}), query)
	counts := work.snapshot()
	_, exposed := counts[driver.ReadPayloadRecords]
	r.census.mu.Lock()
	r.census.pages = append(r.census.pages, queryPageWork{Limit: query.Limit, Returned: len(page.Items), Cursor: query.Cursor != nil, NextCursor: page.NextCursor != nil, MaterializationsExposed: exposed, ReadWork: counts})
	r.census.mu.Unlock()
	return page, err
}

func installQueryCensus(o *ordinaryB04, census *queryCensus) {
	o.f.t.Helper()
	o.server.Close()
	s := censusStore{Store: o.f.store, ResponseFinalizer: o.f.store.(driver.ResponseFinalizer), CheckpointStore: o.f.store.(driver.CheckpointStore), census: census}
	o.f.writer = pipeline.New(s, o.f.lease, pipeline.WithClock(o.f.clock.Now), pipeline.WithIDGenerator(o.f.nextID), pipeline.WithOntology(o.f.ontology))
	o.f.svc = service.New(s, o.f.lease, o.f.writer, service.WithClock(o.f.clock.Now), service.WithOntology(o.f.ontology))
	o.install()
}

func assertGraphCertificate(t *testing.T, o *ordinaryB04, want bool) {
	t.Helper()
	r, err := o.f.store.Read(o.f.ctx, driver.View{Incarnation: o.f.metaSnapshot().Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	states, err := r.Observe(o.f.ctx, []driver.LockKey{pipeline.GraphValidityLockKey(), driver.GraphBlockingSentinelLock})
	if err != nil || len(states) != 2 {
		t.Fatalf("certificate states=%+v err=%v", states, err)
	}
	current := states[0].Present && states[1].Present && states[0].Revision == states[1].Revision
	if current != want {
		t.Fatalf("certificate current=%t want=%t states=%+v", current, want, states)
	}
}

func assertOneCensus(t *testing.T, backend string, pages []queryPageWork, rows int) {
	t.Helper()
	wantCalls := (rows + 999) / 1000
	if len(pages) != wantCalls {
		t.Fatalf("initialization census actual Query calls=%d want=%d pages=%+v", len(pages), wantCalls, pages)
	}
	for i, page := range pages {
		if page.Limit != 1000 || page.Returned != min(1000, rows-i*1000) || page.Cursor != (i > 0) || page.NextCursor != (i+1 < wantCalls) {
			t.Fatalf("initialization census pagination[%d]=%+v", i, page)
		}
		if backend != "memory" && (!page.MaterializationsExposed || page.ReadWork[driver.ReadPayloadRecords] < int64(page.Returned) || page.ReadWork[driver.ReadQueryCalls] != 1) {
			t.Fatalf("actual SQL query observer is not live: %+v", page)
		}
	}
}

func TestParentedCreateInitializationCensus(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var h *postgresHarness
			if backend == "postgres" {
				h = newPostgresHarness(t)
			}
			for _, unrelated := range []int{0, 2000} {
				t.Run(fmt.Sprintf("unrelated-%d", unrelated), func(t *testing.T) {
					dsn := ""
					if h != nil {
						dsn = h.dsn(t)
					}
					ids := make([]string, 0, unrelated+1)
					known := map[string]expectedCreate{}
					var flatOps []model.Op
					for i := 0; i < unrelated; i++ {
						id := fmt.Sprintf("initialization-unrelated-%04d", i)
						ids = append(ids, id)
						known[id] = expectedCreate{ID: id, Title: id, Priority: 4}
						flatOps = append(flatOps, ordinaryCreateOp(id, project, map[string]any{"title": id, "priority": 4}, ""))
					}
					ids = append(ids, "initialization-anchor")
					work := &readWork{counts: map[driver.ReadWorkKind]int64{}}
					o := newOrdinaryB04Observed(t, backend, dsn, ids, work.wrap)
					for start := 0; start < unrelated; start += 50 {
						o.seed(fmt.Sprintf("unrelated-%04d", start), flatOps[start:start+50]...)
					}
					anchor := expectedCreate{ID: "initialization-anchor", Title: "initialization anchor", Priority: 4}
					known[anchor.ID] = anchor
					o.seed("untimed-anchor", ordinaryCreateOp("anchor", project, map[string]any{"title": anchor.Title, "priority": 4}, ""))
					assertCommit(t, o.receipts["untimed-anchor"].Commit, expectedCommit(t, unrelated/50+1, []expectedCreate{anchor}, o.f.clock.Now()))
					assertGraphCertificate(t, o, false)
					census := &queryCensus{}
					installQueryCensus(o, census)
					phases := []string{"first", "later"}
					if backend != "memory" {
						phases = append(phases, "reopened")
					}
					for batch, phase := range phases {
						if phase == "reopened" {
							o.reopen("initialization-sql-reopen")
							installQueryCensus(o, census)
						}
						var ops []model.Op
						var expected []expectedCreate
						for i := 0; i < 50; i++ {
							title := fmt.Sprintf("initialization-child-%04d", batch*50+i)
							e := expectedCreate{ID: fmt.Sprintf("%s.%d", anchor.ID, batch*50+i+1), Title: title, Parent: anchor.ID, Priority: 4}
							expected = append(expected, e)
							known[e.ID] = e
							ops = append(ops, ordinaryCreateOp(title, project, map[string]any{"title": title, "priority": 4}, anchor.ID))
						}
						before, queryStart := work.snapshot(), len(census.snapshot())
						raw := o.callSeedWithRetry(phase, "tx.apply", owner, map[string]any{"changeSet": model.ChangeSet{Ops: ops}})
						delta := map[driver.ReadWorkKind]int64{}
						for kind, n := range work.snapshot() {
							delta[kind] = n - before[kind]
						}
						pages := census.snapshot()[queryStart:]
						if backend != "memory" {
							var queryPayloads int64
							for _, page := range pages {
								queryPayloads += page.ReadWork[driver.ReadPayloadRecords]
							}
							if delta[driver.ReadQueryCalls] != int64(len(pages)) || delta[driver.ReadPayloadRecords] < queryPayloads || delta[driver.ReadPayloadRecords] <= 0 {
								t.Fatalf("query work did not reach enclosing HTTP observer: delta=%v pages=%+v", delta, pages)
							}
						}
						// Root 09:51/clarification: the large-store first batch is
						// explicitly exempt from the steady 1000-payload cap.
						exempt := phase == "first" && unrelated == 2000
						o.retain("initialization-"+phase, map[string]any{"engine": backend, "unrelated": unrelated, "actualQueryCalls": len(pages), "queries": pages, "readWork": delta, "elapsedMS": o.lastSeedElapsedMS, "attempts": o.lastSeedAttempts, "retries": o.lastSeedRetries, "firstLargeStorePayloadCapExemption": exempt, "recurredAfterSQLReopen": phase == "reopened" && len(pages) > 0, "memoryMaterializations": "NOT_EXPOSED"})
						t.Logf("INITIALIZATION engine=%s unrelated=%d phase=%s actualQueryCalls=%d queries=%+v readWork=%v elapsedMS=%.6f firstLargeStorePayloadCapExemption=%t", backend, unrelated, phase, len(pages), pages, delta, o.lastSeedElapsedMS, exempt)
						assertCommit(t, o.commitResponse(phase, "tx.apply", raw), expectedCommit(t, unrelated/50+batch+2, expected, o.f.clock.Now()))
						if o.lastSeedAttempts != 1 || o.lastSeedRetries != 0 {
							t.Fatal("initialization control required a retry")
						}
						if phase == "first" {
							assertOneCensus(t, backend, pages, unrelated+1)
						} else if phase == "later" && len(pages) != 0 {
							t.Fatalf("initialization census recurred before reopen: %+v", pages)
						} else if phase == "reopened" && len(pages) > 0 {
							assertOneCensus(t, backend, pages, unrelated+1+batch*50)
						}
						candidateCap := int64(1275)
						if backend == "memory" {
							candidateCap = 3200
						}
						if delta[driver.ReadChildIDCandidates] <= 0 || delta[driver.ReadChildIDCandidates] > candidateCap || !exempt && !(phase == "reopened" && len(pages) > 0) && delta[driver.ReadPayloadRecords] > 1000 {
							t.Fatalf("initialization steady work cap failed: %v", delta)
						}
						assertGraphCertificate(t, o, true)
					}
					got := snapshot(t, o, true, known)
					if got.Records != len(known) || got.Versions == nil || *got.Versions != len(known) || got.Changesets != unrelated/50+1+len(phases) {
						t.Fatalf("initialization durable conservation failed: %+v", got)
					}
				})
			}
		})
	}
}

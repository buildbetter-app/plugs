package s1b05b08

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The barrier pauses only the first two real SQL transactions immediately
// before forwarding their complete, unchanged lock sets. Neither transaction
// has acquired the graph sentinel yet. Both Writer calls have prepared
// against the old graph; the actual driver must serialize publication and the
// losing Writer must reconsider its stale graph. No storage behavior is faked.
type cycleAdmissionBarrier struct {
	driver.Store
	mu       sync.Mutex
	ready    chan struct{}
	arrivals []cycleAdmissionArrival
}

type cycleAdmissionArrival struct {
	head         model.Revision
	graphUpdate  bool
	lockSetValid bool
}

func (s *cycleAdmissionBarrier) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &cycleAdmissionTx{WriteTx: tx, barrier: s}, nil
}

type cycleAdmissionTx struct {
	driver.WriteTx
	barrier *cycleAdmissionBarrier
}

// Preserve the optional atomic graph writer of the real SQL transaction while
// the barrier observes the lock boundary. Hiding it would turn a certified
// source update into an untyped Put and mask the concurrency behavior.
func (tx *cycleAdmissionTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	writer, ok := tx.WriteTx.(driver.GraphSourceWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	return writer.PutGraphRecord(ctx, record, parent, links)
}

func (tx *cycleAdmissionTx) Lock(ctx context.Context, requests []driver.LockRequest) ([]driver.LockState, error) {
	arrival := cycleAdmissionArrival{lockSetValid: driver.ValidateLockSet(requests) == nil}
	if at := tx.WriteTx.Snapshot().At; at != nil {
		arrival.head = *at
	}
	for _, request := range requests {
		if request.Key.Kind == driver.LockSentinel && request.Key.Sentinel == driver.SentinelGraphBlocking && request.Mode == driver.LockUpdate {
			arrival.graphUpdate = true
		}
	}
	s := tx.barrier
	s.mu.Lock()
	s.arrivals = append(s.arrivals, arrival)
	n := len(s.arrivals)
	if n == 2 {
		close(s.ready)
	}
	s.mu.Unlock()
	if n <= 2 {
		select {
		case <-s.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return tx.WriteTx.Lock(ctx, requests)
}

func TestB06SQLConcurrentOppositeSchedulingEdgesHaveOneDurableWinner(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			left, right := f.create(t, "opposing-left"), f.create(t, "opposing-right")
			before := []durableState{f.state(t, left), f.state(t, right)}
			initial, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			base, err := initial.Head.Uint64()
			if err != nil {
				t.Fatal(err)
			}
			barrier := &cycleAdmissionBarrier{Store: f.store, ready: make(chan struct{})}
			writer := pipeline.New(barrier, f.lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithClock(func() time.Time { return f.now }))
			type attempt struct {
				from, to model.Ref
				kind     string
				meta     model.WriteMeta
				change   model.ChangeSet
				commit   model.Commit
				err      error
			}
			results := make(chan attempt, 2)
			for i, edge := range []attempt{{from: left, to: right, kind: "blocks"}, {from: right, to: left, kind: "conditional-blocks"}} {
				edge.meta = model.WriteMeta{Project: project, Agent: actor, AgentLabel: actor, Method: "tx.apply", IdempotencyKey: key("sql-opposing-" + model.Natural64(uint64(i)).String())}
				edge.change = model.ChangeSet{Ops: []model.Op{{Kind: model.OpLink, Link: &model.LinkOp{From: edge.from, To: edge.to, LinkKind: edge.kind}}}}
				go func(value attempt) {
					value.commit, value.err = writer.Apply(f.ctx, anonymous, value.change, value.meta)
					results <- value
				}(edge)
			}
			var winner, loser attempt
			wins, refusals := 0, 0
			for range 2 {
				value := <-results
				if value.err == nil {
					wins++
					winner = value
				} else {
					refusals++
					loser = value
					requireErrorCode(t, value.err, "ontology_guard_failed")
				}
			}
			barrier.mu.Lock()
			arrivals := append([]cycleAdmissionArrival(nil), barrier.arrivals...)
			barrier.mu.Unlock()
			if len(arrivals) < 2 {
				t.Fatalf("both real transactions must reach the lock boundary: %+v", arrivals)
			}
			for _, arrival := range arrivals[:2] {
				if arrival.head != initial.Head || !arrival.graphUpdate || !arrival.lockSetValid {
					t.Fatalf("opposing admissions did not share the old head and a sorted graph update lock: %+v", arrivals)
				}
			}
			if wins != 1 || refusals != 1 || winner.commit.Revision != model.Natural64(base+1) {
				t.Fatalf("opposing SQL admissions: wins=%d refusals=%d winner=%+v", wins, refusals, winner)
			}
			after := []durableState{f.state(t, left), f.state(t, right)}
			for i, ref := range []model.Ref{left, right} {
				if after[i].Head != winner.commit.Revision.String() || after[i].Changes != before[i].Changes+1 || after[i].GraphSentinel != winner.commit.Revision.String() {
					t.Fatalf("race published extra history or lost graph-sentinel publication: before=%+v after=%+v", before, after)
				}
				if ref.ID == winner.from.ID {
					if after[i].Versions != before[i].Versions+1 || !reflect.DeepEqual(f.document(t, ref).Links, []pipeline.Link{{To: winner.to, Kind: winner.kind}}) {
						t.Fatalf("winner edge/version not retained exactly once: %+v", after[i])
					}
				} else if after[i].Versions != before[i].Versions || !reflect.DeepEqual(after[i].Record, before[i].Record) {
					t.Fatalf("refused edge changed its source or history: before=%+v after=%+v", before[i], after[i])
				}
			}
			assertNoReceipt(t, f, anonymous, actor, "tx.apply", loser.meta.IdempotencyKey)
			r, err := f.store.Read(f.ctx, driver.View{Incarnation: initial.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			changes, changesErr := r.Changesets(f.ctx, nil, 1000)
			principal, principalErr := driver.PrincipalKey(anonymous)
			receipt, receiptErr := r.IdempotencyByKey(f.ctx, principal, actor, "tx.apply", winner.meta.IdempotencyKey)
			closeErr := r.Close()
			if changesErr != nil || principalErr != nil || receiptErr != nil || closeErr != nil {
				t.Fatalf("history/receipt read failed: %v %v %v %v", changesErr, principalErr, receiptErr, closeErr)
			}
			if len(changes) != before[0].Changes+1 || len(changes[len(changes)-1].Changes) != 1 || changes[len(changes)-1].Changes[0].Ref.ID != winner.from.ID ||
				receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, winner.commit) {
				t.Fatalf("winner history/final receipt is incomplete or includes the loser: changes=%+v receipt=%+v", changes, receipt)
			}

			// Close and open the actual same-DSN store. Check errors here rather
			// than relying on the historical fixture's best-effort cleanup.
			if err := f.lease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.lease = nil
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			f.store = nil
			f.store, err = f.storage.Open(f.ctx, driver.Config{DSN: f.dsn})
			if err != nil {
				t.Fatal(err)
			}
			f.open(t)
			reopened, err := f.store.Meta(f.ctx)
			if err != nil || reopened.StoreID != initial.StoreID || reopened.Incarnation != initial.Incarnation || reopened.Head != winner.commit.Revision {
				t.Fatalf("same-DSN reopen changed store identity or head: meta=%+v err=%v", reopened, err)
			}
			if got := []durableState{f.state(t, left), f.state(t, right)}; !reflect.DeepEqual(got, after) {
				t.Fatalf("race graph/history/sentinel changed after actual SQL reopen: before=%+v after=%+v", after, got)
			}
			report := f.call(t, f.ctx, "bead.cycles", nil, "", "")
			var cycles service.CyclesResult
			if report.Error != nil || json.Unmarshal(report.Result, &cycles) != nil || len(cycles.Items) != 0 || cycles.CycleCount.String() != "0" || cycles.Truncated {
				t.Fatalf("reopened graph contains a scheduling cycle or report refusal: %+v %s", report.Error, report.Result)
			}
			replay, err := f.writer.ApplyWithOutcome(f.ctx, anonymous, winner.change, winner.meta)
			if err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Commit, winner.commit) {
				t.Fatalf("durable winner receipt did not replay after SQL reopen: %+v err=%v", replay, err)
			}
			_, err = f.writer.Apply(f.ctx, anonymous, loser.change, loser.meta)
			requireErrorCode(t, err, "ontology_guard_failed")
			assertNoReceipt(t, f, anonymous, actor, "tx.apply", loser.meta.IdempotencyKey)
			if got := []durableState{f.state(t, left), f.state(t, right)}; !reflect.DeepEqual(got, after) {
				t.Fatalf("receipt replay or repeated refusal changed durable state: before=%+v after=%+v", after, got)
			}
			t.Logf("actual %s opposing transactions: one winner, one typed refusal; same-DSN Close/Open, zero-cycle report, final receipt replay and refusal conservation completed", engine)
		})
	}
}

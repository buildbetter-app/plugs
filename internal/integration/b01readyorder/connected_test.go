package b01readyorder_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func publicWrite(t *testing.T, f *fixture, method, key string, params map[string]any) model.Commit {
	t.Helper()
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": fixtureKey(key)}
	response := f.call(t, method, params)
	if response.Error != nil {
		t.Fatalf("public %s refused: %+v", method, response.Error)
	}
	var commit model.Commit
	if err := json.Unmarshal(response.Result, &commit); err != nil || len(commit.Results) != 1 || commit.Results[0].Ref.ID == "" {
		t.Fatalf("public %s lost its committed result: %s %v", method, response.Result, err)
	}
	return commit
}

// The input here is always an ordinary Writer result with the actual
// *service.Ontology classifier. No fixture rank or classifier is installed.
func connectedFact(t *testing.T, f *fixture, ref model.Ref, revision model.Revision, created, ranked time.Time, bucket, rank int) driver.DerivedRow {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	key, err := pipeline.ReadySortKeyRowKey(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.DerivedRows(f.ctx, driver.DerivedSortKey, key)
	if err != nil || len(rows) != 1 || rows[0].Key != key {
		t.Fatalf("ordinary Writer omitted its persisted sort row: %+v %v", rows, err)
	}
	id, fact, err := pipeline.DecodeReadySortKey(rows[0])
	if err != nil || id != ref.ID || fact.Revision.String() != revision.String() || fact.Bucket != bucket || fact.Rank != rank || !fact.CreatedAt.Equal(created) || !fact.RankedAt.Equal(ranked) {
		t.Fatalf("canonical persisted sort fact differs: id=%s fact=%+v err=%v", id, fact, err)
	}
	beads, err := r.Get(f.ctx, []model.Ref{ref})
	if err != nil || len(beads) != 1 || beads[0].Revision.String() != fact.Revision.String() {
		t.Fatalf("bead/rank revision publication differs: %+v %v", beads, err)
	}
	doc, err := pipeline.DecodeDocument(beads[0].Body)
	actualCreated, known := doc.CreationTime()
	if err != nil || !known || !actualCreated.Equal(fact.CreatedAt) {
		t.Fatalf("rank invented or replaced creation time: %+v %v", doc, err)
	}
	lock := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: key}}
	states, err := r.Observe(f.ctx, []driver.LockKey{lock})
	if err != nil || len(states) != 1 || !states[0].Present || states[0].Revision.String() != revision.String() || states[0].Derived == nil || !reflect.DeepEqual(*states[0].Derived, rows[0]) {
		t.Fatalf("derived row did not share its commit revision: %+v %v", states, err)
	}
	return rows[0]
}

func connectedReceipt(t *testing.T, f *fixture, method, agent, key string) *driver.IdempotencyRecord {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	principal, err := driver.PrincipalKey(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(f.ctx, principal, agent, method, fixtureKey(key))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestB01PublicCreateProducesTheStoredRankThatReadyActuallyConsumes(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		createdAt := f.clock.Now()
		first := publicWrite(t, f, "bead.create", "connected-first", map[string]any{"type": "issue", "fields": map[string]any{"title": "aging first", "priority": 0}})
		firstRef := first.Results[0].Ref
		connectedFact(t, f, firstRef, first.Revision, createdAt, createdAt, 0, 0)
		if receipt := connectedReceipt(t, f, "bead.create", "", "connected-first"); receipt == nil || receipt.ResponsePending || receipt.Commit.Revision.String() != first.Revision.String() {
			t.Fatalf("public creation lost its final producer receipt: %+v", receipt)
		}

		f.advance(t, 48*time.Hour)
		rankedAt := f.clock.Now()
		second := publicWrite(t, f, "bead.create", "connected-second", map[string]any{"type": "issue", "fields": map[string]any{"title": "recent second", "priority": 4}})
		secondRef := second.Results[0].Ref
		connectedFact(t, f, secondRef, second.Revision, rankedAt, rankedAt, 0, 4)
		// This is an ordinary, observable body mutation at the inclusive cutoff.
		// It invokes the real classifier and atomic producer, without a refresh
		// hook or a literal derived-row write from the test.
		updated := publicWrite(t, f, "bead.update", "connected-at-cutoff", map[string]any{"id": firstRef, "fields": map[string]any{"title": "edited at the cutoff"}})
		stored := connectedFact(t, f, firstRef, updated.Revision, createdAt, rankedAt, 0, 0)
		receipts := []*driver.IdempotencyRecord{
			connectedReceipt(t, f, "bead.create", "", "connected-first"),
			connectedReceipt(t, f, "bead.create", "", "connected-second"),
			connectedReceipt(t, f, "bead.update", "", "connected-at-cutoff"),
		}
		if receipts[1] == nil || receipts[2] == nil {
			t.Fatal("connected writes omitted their final receipts")
		}
		assertRead := func(want ...string) {
			f.unchanged(t, func() { f.pages(t, nil, want...) })
			got := []*driver.IdempotencyRecord{
				connectedReceipt(t, f, "bead.create", "", "connected-first"),
				connectedReceipt(t, f, "bead.create", "", "connected-second"),
				connectedReceipt(t, f, "bead.update", "", "connected-at-cutoff"),
			}
			if !reflect.DeepEqual(got, receipts) {
				t.Fatal("ready consumption changed producer receipts")
			}
			if row := connectedFact(t, f, firstRef, updated.Revision, createdAt, rankedAt, 0, 0); !reflect.DeepEqual(row, stored) {
				t.Fatal("ready consumption rewrote producer bytes")
			}
		}

		// A live recomputation would put the recent p4 row before this now-old
		// p0 row. The opposite literal answer proves stored-rank consumption.
		f.advance(t, time.Nanosecond)
		assertRead(firstRef.ID, secondRef.ID)
		f.advance(t, time.Minute-time.Nanosecond)
		assertRead(firstRef.ID, secondRef.ID)
		f.advance(t, time.Nanosecond)
		assertRead(secondRef.ID, firstRef.ID)
	})
}

type stagedRankFailureStore struct {
	driver.Store
	driver.ResponseFinalizer
	driver.CheckpointStore
	staged *atomic.Bool
}

func (s stagedRankFailureStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return stagedRankFailureTx{WriteTx: tx, staged: s.staged}, nil
}

type stagedRankFailureTx struct {
	driver.WriteTx
	staged *atomic.Bool
}

// Preserve typed source publication so the injected refusal still follows
// the real driver's sort-row staging on a certified store.
func (tx stagedRankFailureTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	writer, ok := tx.WriteTx.(driver.GraphSourceWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	return writer.PutGraphRecord(ctx, record, parent, links)
}

func (tx stagedRankFailureTx) PutDerived(ctx context.Context, table driver.DerivedTable, remove, add []driver.DerivedRow) error {
	if err := tx.WriteTx.PutDerived(ctx, table, remove, add); err != nil {
		return err
	}
	if table == driver.DerivedSortKey {
		tx.staged.Store(true)
		return errors.New("injected refusal after actual sort-row staging")
	}
	return nil
}

func TestB01CanonicalProducerMutationNoopReplayAndStagedFailure(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		at := f.clock.Now()
		created := publicWrite(t, f, "bead.create", "connected-create", map[string]any{"type": "issue", "fields": map[string]any{"title": "canonical producer", "priority": 2}})
		ref := created.Results[0].Ref
		connectedFact(t, f, ref, created.Revision, at, at, 0, 2)
		mutated := publicWrite(t, f, "bead.update", "connected-mutate", map[string]any{"id": ref, "fields": map[string]any{"priority": 1}})
		stable := connectedFact(t, f, ref, mutated.Revision, at, at, 0, 1)
		f.advance(t, time.Second)
		f.unchanged(t, func() {
			noop := publicWrite(t, f, "bead.update", "connected-noop", map[string]any{"id": ref, "fields": map[string]any{"priority": 1}})
			if noop.Revision.String() != mutated.Revision.String() {
				t.Fatal("same-body public update advanced the graph")
			}
		})
		if receipt := connectedReceipt(t, f, "bead.update", "", "connected-noop"); receipt == nil || receipt.ResponsePending || receipt.Changeset.Kind != "" {
			t.Fatalf("same-body producer no-op lacks a final no-op receipt: %+v", receipt)
		}
		f.unchanged(t, func() {
			replay := publicWrite(t, f, "bead.update", "connected-mutate", map[string]any{"id": ref, "fields": map[string]any{"priority": 1}})
			if !reflect.DeepEqual(replay, mutated) {
				t.Fatal("public replay changed its original commit")
			}
		})
		if row := connectedFact(t, f, ref, mutated.Revision, at, at, 0, 1); !reflect.DeepEqual(row, stable) {
			t.Fatal("same-body no-op/replay refreshed the persisted rank")
		}

		// This injected error occurs after the real driver's PutDerived staged
		// the canonical producer output. The inherited claimsql target retains
		// its separate real SQL-trigger commit failure and store reopen tests.
		var staged atomic.Bool
		faultStore := stagedRankFailureStore{Store: f.store, ResponseFinalizer: f.store.(driver.ResponseFinalizer), CheckpointStore: f.store.(driver.CheckpointStore), staged: &staged}
		writer := pipeline.New(faultStore, f.lease, pipeline.WithOntology(f.onto), pipeline.WithClock(f.clock.Now), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b01-publication", Subscriber: publicationProbe{&f.posts}}))
		faultService := service.New(faultStore, f.lease, writer, service.WithOntology(f.onto), service.WithClock(f.clock.Now))
		meta := model.WriteMeta{Project: project, Agent: "author", AgentLabel: "author", IdempotencyKey: fixtureKey("connected-failed")}
		f.unchanged(t, func() {
			if _, err := faultService.Update(f.ctx, anonymous, service.UpdateRequest{ID: ref, Fields: map[string]any{"priority": 4}}, meta); err == nil {
				t.Fatal("staged producer refusal returned success")
			}
		})
		if !staged.Load() {
			t.Fatal("producer refusal did not reach the actual driver sort-row staging boundary")
		}
		if receipt := connectedReceipt(t, f, "bead.update", "author", "connected-failed"); receipt != nil {
			t.Fatalf("failed producer published a receipt: %+v", receipt)
		}
		if row := connectedFact(t, f, ref, mutated.Revision, at, at, 0, 1); !reflect.DeepEqual(row, stable) {
			t.Fatal("failed producer published staged rank bytes")
		}
		retry, err := f.svc.Update(f.ctx, anonymous, service.UpdateRequest{ID: ref, Fields: map[string]any{"priority": 4}}, meta)
		if err != nil {
			t.Fatal(err)
		}
		connectedFact(t, f, ref, retry.Revision, at, f.clock.Now(), 0, 4)
	})
}

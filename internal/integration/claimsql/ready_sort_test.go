package claimsql_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// A fixture classifier isolates the Writer/storage boundary. This test is not
// evidence for B01's canonical comparator or a periodic rank-refresh worker.
type sqlSortOntology struct {
	*service.Ontology
	rank, calls int
}

func (o *sqlSortOntology) ReadySortKey(doc pipeline.Document, _ time.Time) (int, int, bool, error) {
	o.calls++
	_, known := doc.CreationTime()
	return 0, o.rank, known, nil
}

func sqlSortRows(t *testing.T, f *fixture) []driver.DerivedRow {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	rows, err := r.DerivedRows(f.ctx, driver.DerivedSortKey, "ready/")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func requireSQLSortFact(t *testing.T, f *fixture, ref model.Ref, revision model.Revision, createdAt, rankedAt time.Time, rank int) driver.DerivedRow {
	t.Helper()
	rows := sqlSortRows(t, f)
	if len(rows) != 1 {
		t.Fatalf("sort rows=%+v", rows)
	}
	id, fact, err := pipeline.DecodeReadySortKey(rows[0])
	if err != nil || id != ref.ID || fact.Revision != revision || fact.Bucket != 0 || fact.Rank != rank || !fact.CreatedAt.Equal(createdAt) || !fact.RankedAt.Equal(rankedAt) {
		t.Fatalf("persisted sort fact id=%s fact=%+v err=%v", id, fact, err)
	}
	r := f.reader(t)
	defer r.Close()
	key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: rows[0].Key}}
	states, err := r.Observe(f.ctx, []driver.LockKey{key})
	if err != nil || len(states) != 1 || !states[0].Present || states[0].Revision != revision || states[0].Derived == nil || !reflect.DeepEqual(*states[0].Derived, rows[0]) {
		t.Fatalf("sort lock state did not receive the allocated bead revision: %+v err=%v", states, err)
	}
	if f.record(t, ref).Revision != revision {
		t.Fatal("bead and derived row published different revisions")
	}
	return rows[0]
}

func TestSQLReadySortRowsShareAtomicBodyHistoryAndReceiptPublication(t *testing.T) {
	eachEngine(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		o := &sqlSortOntology{Ontology: service.NewOntology(nil), rank: 2}
		writer := func() *pipeline.Writer {
			return pipeline.New(f.store, f.lease, pipeline.WithOntology(o), pipeline.WithClock(f.now))
		}
		meta := func(n int, method string) model.WriteMeta {
			return model.WriteMeta{Method: method, IdempotencyKey: writeKey(n), Project: claimProject, AgentLabel: "alice"}
		}
		createdAt := f.now()
		created, err := writer().Apply(f.ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "atomic ready rank"}}}}}, meta(9301, "bead.create"))
		if err != nil {
			t.Fatal(err)
		}
		ref := created.Results[0].Ref
		createFact := requireSQLSortFact(t, f, ref, created.Revision, createdAt, createdAt, 2)
		f.advance(time.Second)
		claimedAt := f.now()
		claimSet := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{}}}}}
		claim, err := writer().Apply(f.ctx, anonymous, claimSet, meta(9302, "bead.claim"))
		if err != nil {
			t.Fatal(err)
		}
		claimFact := requireSQLSortFact(t, f, ref, claim.Revision, createdAt, claimedAt, 2)
		f.advance(time.Second)
		before := f.state(t, "")
		calls := o.calls
		if _, err := writer().Apply(f.ctx, anonymous, claimSet, meta(9303, "bead.claim")); err != nil {
			t.Fatal(err)
		}
		requireSameState(t, before, f.state(t, ""))
		if o.calls != calls || !reflect.DeepEqual(sqlSortRows(t, f), []driver.DerivedRow{claimFact}) {
			t.Fatal("same-holder no-op classified or refreshed rank")
		}

		f.advance(time.Second)
		o.rank = 1
		patch := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{"priority": 1}}}}}
		patchMeta := meta(9304, "bead.update")
		before = f.state(t, "")
		f.engine.recoveryFault(t, true)
		_, failed := writer().Apply(f.ctx, anonymous, patch, patchMeta)
		f.engine.recoveryFault(t, false)
		if failed == nil {
			t.Fatal("database changeset trigger did not refuse publication")
		}
		requireSameState(t, before, f.state(t, ""))
		if !reflect.DeepEqual(sqlSortRows(t, f), []driver.DerivedRow{claimFact}) {
			t.Fatal("SQL failure published a new derived sort row")
		}
		r := f.reader(t)
		principal, _ := driver.PrincipalKey(anonymous)
		receipt, receiptErr := r.IdempotencyByKey(f.ctx, principal, "alice", patchMeta.Method, patchMeta.IdempotencyKey)
		_ = r.Close()
		if receiptErr != nil || receipt != nil {
			t.Fatalf("SQL failure persisted a successful rank receipt=%+v err=%v", receipt, receiptErr)
		}
		patched, err := writer().Apply(f.ctx, anonymous, patch, patchMeta)
		if err != nil {
			t.Fatal(err)
		}
		patchFact := requireSQLSortFact(t, f, ref, patched.Revision, createdAt, f.now(), 1)
		requireLifecycleTimes(t, sqlLifecycleBody(t, f, ref), &createdAt, &claimedAt, nil)
		before = f.state(t, "")
		if replay, err := writer().Apply(f.ctx, anonymous, patch, patchMeta); err != nil || !reflect.DeepEqual(replay, patched) {
			t.Fatalf("rank mutation receipt replay=%+v err=%v", replay, err)
		}
		requireSameState(t, before, f.state(t, ""))
		f.reopen(t)
		if !reflect.DeepEqual(sqlSortRows(t, f), []driver.DerivedRow{patchFact}) {
			t.Fatal("store close/reopen changed persisted rank bytes")
		}
		for _, historic := range []struct {
			revision model.Revision
			row      driver.DerivedRow
		}{{created.Revision, createFact}, {claim.Revision, claimFact}} {
			r, err := f.store.Read(f.ctx, driver.View{Incarnation: f.inc, At: &historic.revision})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := r.DerivedRows(f.ctx, driver.DerivedSortKey, "ready/")
			_ = r.Close()
			if err != nil || !reflect.DeepEqual(rows, []driver.DerivedRow{historic.row}) {
				t.Fatalf("pinned rank bytes changed at %s: %+v err=%v", historic.revision, rows, err)
			}
		}
		deleted, err := writer().Apply(f.ctx, anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ref}}}}, meta(9305, "tx.apply"))
		if err != nil {
			t.Fatal(err)
		}
		if len(sqlSortRows(t, f)) != 0 {
			t.Fatal("deletion left a live rank row")
		}
		r = f.reader(t)
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: patchFact.Key}}
		states, err := r.Observe(f.ctx, []driver.LockKey{key})
		_ = r.Close()
		if err != nil || len(states) != 1 || states[0].Present || states[0].Revision != deleted.Revision {
			t.Fatalf("rank tombstone not atomic with deletion: %+v err=%v", states, err)
		}
		t.Logf("%s: actual Writer/typed derived locks/after-Head revision, body/history/receipt rollback, same-holder no-op and reopen/delete persistence; fixture classifier only", e.name)
	})
}

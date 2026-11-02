package memory

import (
	"context"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestReadSelectionMemoryHeadAndStagedRefusal(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "selection", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	ref := model.RefByID("source", model.Natural64(0))
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	candidates := []driver.ReadCandidate{{Ordinal: 0, Ref: model.RefByID("b", model.Natural64(0)), Prepared: true}, {Ordinal: 1, Ref: model.RefByID("a", model.Natural64(0)), Prepared: true}}
	q := driver.Query{ReadSelection: &driver.ReadSelection{View: r.Snapshot(), Candidates: candidates, Predicate: driver.SelectionExpr{Op: driver.SelectionIDs, IDs: []string{"a"}}, Limit: 1}}
	p, err := r.Query(ctx, q)
	if err != nil || p.ReadSelection == nil || p.ReadSelection.Total != 1 || !reflect.DeepEqual(p.ReadSelection.Ordinals, []int{1}) {
		t.Fatalf("memory ordinal semantics: %+v %v", p, err)
	}
	ordinary, err := r.Query(ctx, driver.Query{Limit: 1})
	if err != nil || ordinary.ReadSelection != nil || len(ordinary.Items) != 1 || ordinary.Items[0].Ref.ID != "source" {
		t.Fatalf("ordinary nil-slot changed: %+v %v", ordinary, err)
	}
	q.Filter = map[string]any{}
	p, err = r.Query(ctx, q)
	if err == nil || p.ReadSelection != nil {
		t.Fatal("mixed selection mode accepted")
	}
	q.Filter = nil
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, []driver.LockRequest{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"staged":true}`)}); err != nil {
		t.Fatal(err)
	}
	q.ReadSelection.View = tx.Snapshot()
	p, err = tx.Query(ctx, q)
	if err == nil || p.ReadSelection != nil {
		t.Fatalf("memory staged selection accepted: %+v %v", p, err)
	}
	ordinary, err = tx.Query(ctx, driver.Query{})
	if err != nil || len(ordinary.Items) != 1 || string(ordinary.Items[0].Body) != `{"staged":true}` {
		t.Fatalf("staged ordinary fallback changed: %+v %v", ordinary, err)
	}
}

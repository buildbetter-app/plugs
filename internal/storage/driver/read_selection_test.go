package driver

import (
	"context"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestReadSelectionFreezesEveryMutableInput(t *testing.T) {
	at := model.Natural64(7)
	view := View{At: &at, Incarnation: "incarnation"}
	q := Query{AsOf: &at, ReadSelection: &ReadSelection{View: view, Candidates: []ReadCandidate{{Ordinal: 0, Ref: model.RefByID("a", model.Natural64(0)), Prepared: true}, {Ordinal: 1, Ref: model.RefByID("b", model.Natural64(0)), Prepared: true}}, Predicate: SelectionExpr{Op: SelectionAll, Args: []SelectionExpr{{Op: SelectionIDs, IDs: []string{"a"}}}}, Limit: 1, PrincipalKind: "user"}}
	frozen, err := FreezeReadSelection(q, view, true)
	if err != nil {
		t.Fatal(err)
	}
	q.ReadSelection.Candidates[0].Ref.ID = "mutated"
	q.ReadSelection.Predicate.Args[0].IDs[0] = "b"
	q.ReadSelection.Predicate.Args[0].Op = SelectionFalse
	*q.ReadSelection.View.At = model.Natural64(99)
	if frozen.View.At.String() != "7" || frozen.Candidates[0].Ref.ID != "a" || frozen.Predicate.Args[0].IDs[0] != "a" || frozen.Predicate.Args[0].Op != SelectionIDs {
		t.Fatal("selection retains caller-owned mutable storage")
	}
	page, err := SelectReadMemory(context.Background(), frozen)
	if err != nil || page.Total != 1 || !reflect.DeepEqual(page.Ordinals, []int{0}) {
		t.Fatalf("frozen descriptor changed after mutation: %+v %v", page, err)
	}
}

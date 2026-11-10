package pipeline

import (
	"context"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAllocatorReservationsDetachAndSurviveSourceDispatchReplacement(t *testing.T) {
	h := setup(t)
	seedAllocator(t, h.store, h.lease, driver.NewHashAllocator("demo", 3, 8, 0.25))
	creator, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	input := driver.HashInput{Title: "initial", Creator: creator, Timestamp: h.now}
	first, err := driver.HashCandidate("demo", input, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := driver.HashCandidate("demo", input, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	third, err := driver.HashCandidate("demo", input, 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{first}
	ctx := WithAllocatorReservations(context.Background(), ids)
	ids[0] = second // the caller cannot mutate an installed reservation
	ctx = WithSourceOperations(ctx, nil, nil)
	request := meta(620)
	request.Project = "demo"
	cs := model.ChangeSet{Ops: []model.Op{create("first"), create("second")}}
	w := h.writer()
	commit, err := w.Apply(ctx, by, cs, request)
	if err != nil || len(commit.Results) != 2 || commit.Results[0].Ref.ID != second || commit.Results[1].Ref.ID != third {
		t.Fatalf("immutable source and staged reservations: %+v %v", commit, err)
	}
	replay, err := w.ApplyWithOutcome(ctx, by, cs, request)
	if err != nil || !replay.Replayed || !reflect.DeepEqual(commit, replay.Commit) {
		t.Fatalf("reserved hash replay: %+v %v", replay, err)
	}
}

func TestParentOnlyAllocatorRetriesReservedSourceChild(t *testing.T) {
	h := setup(t)
	w := h.writer()
	parent := pipelineCreate(t, w, "parent", 621).Results[0].Ref
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Parent: &parent, Fields: map[string]any{"title": "reserved fallback child"}}}}}
	ctx := WithAllocatorReservations(context.Background(), []string{parent.ID + ".1"})
	commit, err := w.Apply(ctx, by, cs, meta(622))
	if err != nil || len(commit.Results) != 1 || commit.Results[0].Ref.ID != parent.ID+".2" {
		t.Fatalf("parent-only reservation retry: %+v %v", commit, err)
	}
	r := h.read(t)
	value, err := r.(driver.ParentCounterReader).LoadParentCounter(ctx, parent.ID)
	if err != nil || value.LastChild != 2 {
		t.Fatalf("reserved child counter: %+v %v", value, err)
	}
	replay, err := w.ApplyWithOutcome(ctx, by, cs, meta(622))
	if err != nil || !replay.Replayed || !reflect.DeepEqual(commit, replay.Commit) {
		t.Fatalf("parent-only replay: %+v %v", replay, err)
	}
}

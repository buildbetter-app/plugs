package s1b05b08

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestB06MixedSchedulingCycleRefusalRollsBackEarlierBatchWrites(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			for _, kinds := range [][2]string{{"blocks", "conditional-blocks"}, {"parent-child", "blocks"}, {"conditional-blocks", "parent-child"}} {
				t.Run(kinds[0]+"-"+kinds[1], func(t *testing.T) {
					f := newFixture(t, engine)
					left, right, extra := f.create(t, "mixed-left"), f.create(t, "mixed-right"), f.create(t, "mixed-extra")
					seed := f.call(t, f.ctx, "bead.link", map[string]any{"from": left, "to": right, "kind": kinds[0]}, key("mixed-seed"), actor)
					if seed.Error != nil {
						t.Fatalf("scheduling seed refused: %+v", seed)
					}
					before := []durableState{f.state(t, left), f.state(t, right), f.state(t, extra)}
					cs := model.ChangeSet{Ops: []model.Op{
						{Kind: model.OpPatch, Patch: &model.PatchOp{Target: extra, Fields: map[string]any{"title": "must roll back"}}},
						{Kind: model.OpLink, Link: &model.LinkOp{From: right, To: left, LinkKind: kinds[1]}},
					}}
					refused := f.call(t, f.ctx, "tx.apply", map[string]any{"changeSet": cs}, key("mixed-cycle"), actor)
					if refused.Error == nil || refused.Error.Data.Code != "ontology_guard_failed" {
						t.Fatalf("mixed scheduling cycle was not refused: %+v", refused)
					}
					if after := []durableState{f.state(t, left), f.state(t, right), f.state(t, extra)}; !reflect.DeepEqual(before, after) {
						t.Fatalf("cycle refusal changed graph/history/sentinel: before=%+v after=%+v", before, after)
					}
					assertNoReceipt(t, f, anonymous, actor, "tx.apply", key("mixed-cycle"))
				})
			}
		})
	}
}

func TestB06PureTracksLoopRemainsOutsideSchedulingAndCloseGuards(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			left, right := f.create(t, "tracks-left"), f.create(t, "tracks-right")
			for i, pair := range [][2]model.Ref{{left, right}, {right, left}} {
				got := f.call(t, f.ctx, "bead.link", map[string]any{"from": pair[0], "to": pair[1], "kind": "tracks"}, key("tracks-"+model.Natural64(uint64(i)).String()), actor)
				if got.Error != nil {
					t.Fatalf("pure tracks loop became a scheduling cycle: %+v", got)
				}
			}
			requireOpaqueReadyIDs(t, f, left.ID, right.ID)
			if got := f.call(t, f.ctx, "bead.close", map[string]any{"id": left}, key("tracks-close"), actor); got.Error != nil {
				t.Fatalf("tracks annotation became a close blocker: %+v", got)
			}
			f.reopen(t)
			requireOpaqueEdge(t, f, left, right, "tracks")
			requireOpaqueEdge(t, f, right, left, "tracks")
			requireOpaqueReadyIDs(t, f, right.ID)
		})
	}
}

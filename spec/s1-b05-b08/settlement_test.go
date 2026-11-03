package s1b05b08

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB08MultiTargetCloseRefusesAtomicallyThenSettlesInOrder(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			parent := f.create(t, "batch-close-parent")
			child := f.createWithParent(t, "batch-close-child", parent)
			before := []durableState{f.state(t, parent), f.state(t, child)}
			closeOp := func(ref model.Ref) model.Op {
				return model.Op{Kind: model.OpClose, Close: &model.CloseOp{Target: ref}}
			}
			// An earlier title write and a close of the still-open parent
			// cannot escape a later typed refusal in this atomic request.
			refused := model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: child, Fields: map[string]any{"title": "must roll back"}}},
				closeOp(parent), closeOp(child),
			}}
			got := f.call(t, f.ctx, "tx.apply", map[string]any{"changeSet": refused}, key("close-parent-first"), actor)
			if got.Error == nil || got.Error.Data.Code != "ontology_guard_failed" {
				t.Fatalf("parent-first close bypassed the ordered guard: %+v", got)
			}
			if after := []durableState{f.state(t, parent), f.state(t, child)}; !reflect.DeepEqual(before, after) {
				t.Fatalf("refused close changed graph, history or sentinel: before=%+v after=%+v", before, after)
			}
			assertNoReceipt(t, f, anonymous, actor, "tx.apply", key("close-parent-first"))
			ordered := model.ChangeSet{Ops: []model.Op{closeOp(child), closeOp(parent)}}
			got = f.call(t, f.ctx, "tx.apply", map[string]any{"changeSet": ordered}, key("close-child-first"), actor)
			var commit model.Commit
			if got.Error != nil || json.Unmarshal(got.Result, &commit) != nil || len(commit.Results) != 2 {
				t.Fatalf("child-first close failed: %+v", got)
			}
			after := []durableState{f.state(t, parent), f.state(t, child)}
			for i, ref := range []model.Ref{parent, child} {
				if f.document(t, ref).Fields["status"] != "done" || after[i].Head != commit.Revision.String() ||
					after[i].Changes != before[i].Changes+1 || after[i].Versions != before[i].Versions+1 {
					t.Fatalf("atomic close did not settle once: ref=%s before=%+v after=%+v", ref.ID, before[i], after[i])
				}
			}
			f.reopen(t)
			replay := f.call(t, f.ctx, "tx.apply", map[string]any{"changeSet": ordered}, key("close-child-first"), actor)
			if replay.Error != nil || replay.Meta["replayed"] != true || string(replay.Result) != string(got.Result) {
				t.Fatalf("ordered close receipt did not replay: %+v", replay)
			}
			if again := []durableState{f.state(t, parent), f.state(t, child)}; !reflect.DeepEqual(after, again) {
				t.Fatalf("close replay changed durable state: before=%+v after=%+v", after, again)
			}
		})
	}
}

func TestB08StaleCloseCASRefusesWithoutReceiptOrHistory(t *testing.T) {
	for _, engine := range testEngines {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			id := f.create(t, "stale-close")
			meta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := reader.Get(f.ctx, []model.Ref{id})
			_ = reader.Close()
			if err != nil || len(rows) != 1 {
				t.Fatalf("read close revision: %v %v", rows, err)
			}
			stale := rows[0].Revision
			update := f.call(t, f.ctx, "bead.update", map[string]any{"id": id, "fields": map[string]any{"title": "new revision"}}, key("stale-close-update"), actor)
			if update.Error != nil {
				t.Fatalf("seed update failed: %+v", update)
			}
			before := f.state(t, id)
			close := f.call(t, f.ctx, "bead.close", map[string]any{
				"id": id, "expect": []model.Precondition{{Ref: id, Revision: &stale}},
			}, key("stale-close-cas"), actor)
			if close.Error == nil || close.Error.Data.Code != "precondition_failed" {
				t.Fatalf("stale close did not preserve CAS: %+v", close)
			}
			if after := f.state(t, id); !reflect.DeepEqual(before, after) {
				t.Fatalf("stale close changed durable state: before=%+v after=%+v", before, after)
			}
			assertNoReceipt(t, f, anonymous, actor, "bead.close", key("stale-close-cas"))
		})
	}
}

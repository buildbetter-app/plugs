package b13memory_test

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// These public controls are separate from the closed f638 imported-envelope
// refusal unit. Every write goes through the existing Writer/service/byte RPC.
func TestB13UnicodeCoreReplacementForgetPreservesExactNeighbor(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seed := f.seedExistingRows()
			want := map[string]memoryValue{"b13-empty": {ID: "b13-empty", Key: "empty legacy ✓", Content: "", Revision: seed}}
			const literalKey = "  東京/Ω e\u0301  "
			const literal = " \tFirst\n東京 e\u0301 \x00 "
			first, _ := f.accept("memory.save", map[string]any{"key": literalKey, "content": literal}, key("new-source-core-explicit"), model.OpCreate)
			id := first.Results[0].Ref.ID
			want[id] = memoryValue{ID: id, Key: literalKey, Content: literal, Revision: first.Revision}
			neighbor, _ := f.accept("memory.save", map[string]any{"key": literalKey + "neighbor", "content": "exact neighbor"}, key("new-source-core-neighbor"), model.OpCreate)
			neighborID := neighbor.Results[0].Ref.ID
			want[neighborID] = memoryValue{ID: neighborID, Key: literalKey + "neighbor", Content: "exact neighbor", Revision: neighbor.Revision}
			f.verifyConnected(want, "")
			f.refuse("memory.save", map[string]any{"content": "東京 !!!"}, key("new-source-core-underivable"), "invalid_params")
			f.refuse("memory.save", map[string]any{"key": literalKey, "content": " \t\n"}, key("new-source-core-blank"), "invalid_params")
			const replacement = "\nREPLACED Ω\t東京  "
			changed, _ := f.accept("memory.save", map[string]any{"key": literalKey, "content": replacement}, key("new-source-core-replace"), model.OpPatch)
			if changed.Results[0].Ref.ID != id {
				t.Fatal("replacement changed the explicit Unicode key identity")
			}
			want[id] = memoryValue{ID: id, Key: literalKey, Content: replacement, Revision: changed.Revision}
			f.verifyConnected(want, "")
			f.reopen()
			f.verifyConnected(want, "")
			f.accept("memory.delete", map[string]any{"id": id}, key("new-source-core-forget"), model.OpDelete)
			delete(want, id)
			f.refuse("memory.get", map[string]any{"id": id}, "", "not_found")
			f.refuse("memory.delete", map[string]any{"id": id}, key("new-source-core-forget-miss"), "not_found")
			f.verifyConnected(want, "")
			f.reopen()
			f.verifyConnected(want, "")
		})
	}
}

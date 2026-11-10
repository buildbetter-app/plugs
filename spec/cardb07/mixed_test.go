package cardb20

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB07PublicMixedPlaneIdentity(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b07MixedPublic(t, engine) })
	}
}
func b07MixedPublic(t *testing.T, engine string) {
	h := b07NewHost(t, engine)
	a, b := h.create("mixed-durable-a", nil), h.create("mixed-durable-b", nil)
	before := h.snapshot()
	reply := h.call("x.workflow.invoke", "mixed-invoke", map[string]any{"template": "b07", "project": "b07", "mode": "ephemeral", "checklist": []any{map[string]any{"label": "first", "title": "mixed first"}, map[string]any{"label": "second", "title": "mixed second"}}})
	if engine == "memory" {
		if reply.Error == nil || reply.Error.Data.Code != "plugin_durable_state_unavailable" {
			t.Fatalf("memory capability differs: %+v", reply)
		}
		h.same(before)
		b07JSON(t, filepath.Join(h.root, "mixed-capability.json"), map[string]any{"outcome": "CAPABILITY_REFUSAL", "full_B07": "INCOMPLETE", "path": "internal/server/composition/config.go", "reason": "normal memory composition requires persistent workflow storage"})
		return
	}
	if reply.Error != nil {
		t.Fatal(reply.Error)
	}
	var invoked struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if e := json.Unmarshal(reply.Result, &invoked); e != nil || len(invoked.Items) != 2 {
		t.Fatal("actual workflow invocation missing items")
	}
	x, y := model.RefByID(invoked.Items[0].ID, model.Natural64(0)), model.RefByID(invoked.Items[1].ID, model.Natural64(0))
	for _, test := range []struct {
		name     string
		from, to model.Ref
	}{{"durable_to_active", a, x}, {"active_to_durable", x, b}, {"active_to_active", x, y}} {
		t.Run(test.name, func(t *testing.T) {
			old := h.t
			h.t = t
			defer func() { h.t = old }()
			p := map[string]any{"from": test.from, "to": test.to, "kind": "custom kind", "metadata": map[string]any{"nativeExtensions": b07OpaqueInputs()}}
			first := h.call("bead.link", test.name+"-first", p)
			b07Success(t, first)
			before := h.snapshot()
			duplicate := h.call("bead.link", test.name+"-duplicate", p)
			b07Success(t, duplicate)
			h.quietWorkflow(before)
			h.reopen()
			before = h.snapshot()
			replay := h.call("bead.link", test.name+"-first", p)
			if replay.Error != nil || !bytes.Equal(first.Result, replay.Result) {
				t.Fatal("mixed edge replay differs")
			}
			h.same(before)
			p["kind"] = "blocks"
			refusal := h.call("bead.link", test.name+"-retype", p)
			if refusal.Error == nil || refusal.Error.Data.Code != "ontology_guard_failed" {
				t.Fatal("mixed retype not refused")
			}
			h.same(before)
		})
	}
	before = h.snapshot()
	r := h.call("tx.apply", "mixed-malformed-batch", map[string]any{"changeSet": map[string]any{"ops": []any{map[string]any{"kind": "patch", "target": a, "fields": map[string]any{"title": "must-not-publish"}}, map[string]any{"kind": "link", "from": x, "to": b, "linkKind": "custom kind", "metadata": map[string]any{"nativeExtensions": map[string]any{"beads.future_invalid": "null true"}}}}}})
	if r.Error == nil || r.Error.Data.Code != "invalid_params" {
		t.Fatal("mixed malformed batch not refused")
	}
	h.same(before)
	h.reopen()
	h.same(before)
	b07JSON(t, filepath.Join(h.root, "mixed-outcomes.json"), map[string]any{"outcome": "public SQL mixed endpoint identity, no-op, replay, retype and malformed preparation conservation", "successful_mixed_tx_apply": "NOT_ESTABLISHED", "native_history_credit": false})
}

func (h *b07Host) quietWorkflow(want b07State) {
	h.t.Helper()
	got := h.snapshot()
	if len(got.Checkpoints) != len(want.Checkpoints) {
		h.t.Fatal("mixed no-op changed namespace set")
	}
	changed := 0
	for key, before := range want.Checkpoints {
		after := got.Checkpoints[key]
		if reflect.DeepEqual(before, after) {
			continue
		}
		changed++
		if after.Generation != before.Generation+1 {
			h.t.Fatal("mixed no-op namespace generation differs")
		}
		var old, now map[string]json.RawMessage
		if json.Unmarshal(before.Payload, &old) != nil || json.Unmarshal(after.Payload, &now) != nil {
			h.t.Fatal("checkpoint payload invalid")
		}
		var oldReceipts, newReceipts map[string]json.RawMessage
		if json.Unmarshal(old["receipts"], &oldReceipts) != nil || json.Unmarshal(now["receipts"], &newReceipts) != nil || len(newReceipts) != len(oldReceipts)+1 {
			h.t.Fatal("mixed no-op must add one receipt")
		}
		for k, v := range oldReceipts {
			if !bytes.Equal(v, newReceipts[k]) {
				h.t.Fatal("mixed no-op rewrote prior receipt")
			}
		}
		delete(old, "receipts")
		delete(now, "receipts")
		if !reflect.DeepEqual(old, now) {
			h.t.Fatal("mixed no-op changed item/edge/audit state")
		}
	}
	if changed != 1 {
		h.t.Fatal("mixed no-op did not publish exactly one namespace receipt")
	}
	got.Checkpoints = map[string]driver.Checkpoint{}
	want.Checkpoints = map[string]driver.Checkpoint{}
	if !reflect.DeepEqual(want, got) {
		h.t.Fatal("mixed no-op changed core records/history/derived/receipts")
	}
}

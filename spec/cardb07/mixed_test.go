package cardb20

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func TestB07PublicMixedPlaneIdentity(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b07MixedPublic(t, engine) })
	}
}
func b07MixedPublic(t *testing.T, engine string) {
	h := b07NewHost(t, engine)
	// Preserve this separately attributed capability-negative contract. The
	// default ordinary provider's positive routes have independent fixtures.
	h.workflowCapabilityUnavailable = true
	h.installUnavailableWorkflowCapability()
	a, b := h.create("mixed-durable-a", nil), h.create("mixed-durable-b", nil)
	before := h.snapshot()
	missing := model.RefByID("workflow:item/00000000000000000000000000000000", model.Natural64(0))
	unknown := h.call("tx.apply", "mixed-missing-item", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: a, Fields: map[string]any{"title": "discard durable draft"}}},
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: missing, Fields: map[string]any{"title": "absent active item"}}},
	}}})
	if unknown.Error == nil || unknown.Error.Data.Code != "not_found" {
		t.Fatal("unknown workflow item must remain not_found", unknown.Error)
	}
	h.same(before)
	reply := h.call("x.workflow.invoke", "mixed-invoke", map[string]any{"template": "b07", "project": "b07", "mode": "ephemeral", "checklist": []any{map[string]any{"label": "first", "title": "mixed first"}, map[string]any{"label": "second", "title": "mixed second"}}})
	if engine == "memory" {
		if reply.Error == nil || reply.Error.Data.Code != "plugin_durable_state_unavailable" {
			t.Fatalf("memory capability differs: %+v", reply)
		}
		h.same(before)
		b07JSON(t, filepath.Join(h.root, "mixed-capability.json"), map[string]any{"outcome": "EXPLICIT_UNAVAILABLE_CAPABILITY_REFUSAL", "positive_credit": false, "capability": "restricted restart-durable facade on process-lifetime memory"})
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
	ready := h.ready()
	before = h.snapshot()
	mixed := map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: a, Fields: map[string]any{"title": "mixed committed durable"}}},
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: x, Fields: map[string]any{"title": "mixed committed active"}}},
	}}}
	visible := h.call("bead.get", "", map[string]any{"id": x})
	if visible.Error != nil {
		t.Fatal("existing workflow item is not publicly visible", visible.Error)
	}
	project := h.config.Project
	h.config.Project = "b07-outside-scope"
	scoped := h.call("tx.apply", "ordinary-mixed-wrong-project", mixed)
	h.config.Project = project
	if scoped.Error == nil || scoped.Error.Data.Code != "forbidden" {
		t.Fatal("unsupported route bypassed workflow project visibility", scoped.Error)
	}
	h.same(before)
	// The explicitly restricted legacy provider has no atomic participant.
	// Preserve its typed refusal and complete state through physical reopen.
	for _, label := range []string{"ordinary-mixed-route", "ordinary-mixed-route-after-reopen"} {
		refused := h.call("tx.apply", label, mixed)
		if refused.Error == nil || refused.Error.Data.Code != "unsupported" {
			t.Fatal("ordinary mixed route refusal differs", refused.Error)
		}
		h.same(before)
		h.requireReady(ready)
		h.reopen()
		h.same(before)
	}
	late := h.call("tx.apply", "mixed-late-route-refusal", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: a, Fields: map[string]any{"title": "discard durable draft"}}},
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: x, Fields: map[string]any{"title": "discard active draft"}}},
		{Kind: model.OpLink, Link: &model.LinkOp{From: x, To: b, LinkKind: "blocks"}},
	}}})
	if late.Error == nil || late.Error.Data.Code != "unsupported" {
		t.Fatal("ordinary mixed batch routing refusal differs", late.Error)
	}
	h.same(before)
	before = h.snapshot()
	r := h.call("tx.apply", "mixed-malformed-batch", map[string]any{"changeSet": map[string]any{"ops": []any{map[string]any{"kind": "patch", "target": a, "fields": map[string]any{"title": "must-not-publish"}}, map[string]any{"kind": "patch", "target": x, "fields": map[string]any{"title": "active-must-not-publish"}}, map[string]any{"kind": "link", "from": x, "to": b, "linkKind": "custom kind", "metadata": map[string]any{"nativeExtensions": map[string]any{"beads.future_invalid": "null true"}}}}}})
	if r.Error == nil || r.Error.Data.Code != "invalid_params" {
		t.Fatal("mixed malformed batch not refused")
	}
	h.same(before)
	h.reopen()
	h.same(before)
	b07JSON(t, filepath.Join(h.root, "mixed-outcomes.json"), map[string]any{"outcome": "public SQL mixed endpoint identity, no-op, replay, retype and conserved route/malformed refusals", "capability": "explicit durable-only facade without optional workflow checkpoint capability", "generic_mixed_tx_apply": "unsupported; zero positive credit", "positive_mixed_tx_apply": "separate cardb07positive default-composition target", "native_history_credit": false})
}

func (h *b07Host) installUnavailableWorkflowCapability() {
	h.t.Helper()
	capability, err := driver.RequireWorkflowCheckpointStore(h.runtime.Store, h.runtime.Lease)
	if err != nil {
		h.t.Fatal(err)
	}
	state, err := pluginhost.NewWorkflowRuntimeState(capability, h.runtime.Lease)
	if err != nil {
		h.t.Fatal(err)
	}
	provider, err := workflowplugin.New(b07RestrictedWorkflowState{state})
	if err != nil {
		h.t.Fatal(err)
	}
	meta, err := h.runtime.Store.Meta(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	service.WithWorkflow(provider.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation))(h.runtime.Service)
}

// Keep actual reads and truthful restart persistence while deliberately
// withholding the optional workflow lifetime and atomic snapshot capabilities.
// Missing-item reads and permanent tools must retain their usual contracts.
type b07RestrictedWorkflowState struct{ pluginapi.DurableState }

func (s b07RestrictedWorkflowState) Persistent() bool {
	marker, ok := s.DurableState.(interface{ Persistent() bool })
	return ok && marker.Persistent()
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
		old, oldRest := h.workflowPayload(before.Payload, before.Generation)
		now, newRest := h.workflowPayload(after.Payload, after.Generation)
		if !reflect.DeepEqual(oldRest, newRest) {
			h.t.Fatal("mixed no-op changed another namespace value or envelope field")
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

func (h *b07Host) workflowPayload(raw []byte, generation uint64) (map[string]json.RawMessage, []map[string]json.RawMessage) {
	h.t.Helper()
	var envelope, values, state, value map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil || json.Unmarshal(envelope["values"], &values) != nil || json.Unmarshal(values["workflow/state"], &state) != nil || json.Unmarshal(state["value"], &value) != nil || value == nil {
		h.t.Fatal("checkpoint workflow value missing")
	}
	var revision uint64
	if json.Unmarshal(state["revision"], &revision) != nil || revision != generation {
		h.t.Fatal("workflow value revision differs from namespace generation")
	}
	delete(envelope, "values")
	delete(values, "workflow/state")
	delete(state, "revision")
	delete(state, "value")
	return value, []map[string]json.RawMessage{envelope, values, state}
}

package cardb07positive_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	publicproof "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func TestB07OrdinaryMemoryMixedProviderSuccess(t *testing.T) {
	ordinaryPositive(t, "memory")
}

func TestB07OrdinarySQLGenericMixedBatchSuccess(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { ordinaryPositive(t, engine) })
	}
}

func ordinaryPositive(t *testing.T, engine string) {
	h := publicproof.NewHost(t, engine)
	durable := h.Create("durable")
	unrelated := h.Create("unrelated")
	initial := h.Capture()
	invocation := h.Call("x.workflow.invoke", "invoke", map[string]any{"template": "b07-positive", "project": "b07positive", "mode": "ephemeral", "checklist": []any{map[string]any{"label": "active", "title": "active original"}}}, "")
	var invoked workflowplugin.InvokeResult
	if err := json.Unmarshal(invocation.Result, &invoked); err != nil || len(invoked.Items) != 1 || invoked.Plane != "ephemeral" {
		t.Fatal("public invocation did not return one actual active item", err)
	}
	item := invoked.Items[0]
	if !strings.HasPrefix(item.ID, workflowplugin.ItemPrefix) || item.Kind != workflowplugin.ItemKind || item.Project != "b07positive" || item.RunID == "" || item.Plane != "ephemeral" || item.Plugin != workflowplugin.Alias || invoked.Run == nil || invoked.Run.ID != item.RunID {
		t.Fatalf("provider identity missing: %+v", invoked)
	}
	proof.Save(t, h.Root, "actual-provider-identity.json", invoked)
	active := model.RefByID(item.ID, model.Natural64(0))
	before := h.Capture()
	if !reflect.DeepEqual(initial.Rows, before.Rows) || !reflect.DeepEqual(initial.Versions, before.Versions) || !reflect.DeepEqual(initial.Changes, before.Changes) || !reflect.DeepEqual(initial.Derived, before.Derived) || !reflect.DeepEqual(initial.Receipts, before.Receipts) || initial.Meta != before.Meta || before.Checkpoints[proof.Namespace].Generation != initial.Checkpoints[proof.Namespace].Generation+1 {
		t.Fatal("ordinary invocation changed core or missed exactly one checkpoint publication")
	}
	if len(before.Checkpoints) != len(initial.Checkpoints) {
		t.Fatal("invocation changed the checkpoint inventory")
	}
	for key, checkpoint := range initial.Checkpoints {
		if key != proof.Namespace && !reflect.DeepEqual(checkpoint, before.Checkpoints[key]) {
			t.Fatal("invocation changed unrelated namespace", key)
		}
	}
	proof.WorkflowNamespaceEffect(t, initial, before)
	if engine == "memory" {
		if persistent, ok := h.Runtime.Store.(driver.PersistentCheckpointStore); ok && persistent.Persistent() {
			t.Fatal("memory incorrectly advertises restart persistence")
		}
	}
	metadata := &model.LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": `" {\"wide\":9007199254740993} "`, "beads.thread_id": `""`, "beads.future_object": `{ "n":1.00e+02, "null":null }`}}
	change := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: active, Fields: map[string]any{"title": "active committed"}}},
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: durable, Fields: map[string]any{"title": "durable committed"}}},
		{Kind: model.OpLink, Link: &model.LinkOp{From: durable, To: active, LinkKind: "relates-to", Metadata: metadata}},
		{Kind: model.OpLink, Link: &model.LinkOp{From: active, To: unrelated, LinkKind: "relates-to", Metadata: metadata}},
	}}
	reply := h.Call("tx.apply", "mixed-success", map[string]any{"changeSet": change}, "")
	var committed model.Commit
	if err := json.Unmarshal(reply.Result, &committed); err != nil || len(committed.Results) != 4 {
		t.Fatal("generic mixed batch lost ordered results", err)
	}
	after := h.Capture()
	if reply.Meta.Replayed || after.Meta.Head.String() != committed.Revision.String() || len(after.Changes) != len(before.Changes)+1 || after.Checkpoints[proof.Namespace].Generation != before.Checkpoints[proof.Namespace].Generation+1 {
		t.Fatal("mixed batch did not publish one joined history/checkpoint effect")
	}
	if committed.Results[0].Ref != active || committed.Results[1].Ref != durable || committed.Results[2].Ref != active || committed.Results[3].Ref != active {
		t.Fatalf("mixed operation order/endpoint identity: %+v", committed.Results)
	}
	if len(after.Receipts) != len(before.Receipts)+1 || after.Receipts["mixed-success"].Pending || !reflect.DeepEqual(after.Receipts["mixed-success"].Commit, committed) {
		t.Fatal("missing exact nonpending generic receipt")
	}
	if !reflect.DeepEqual(after.Receipts["mixed-success"].Changeset, after.Changes[len(before.Changes)]) {
		t.Fatal("generic receipt lost its exact joined history entry")
	}
	for label, receipt := range before.Receipts {
		if !reflect.DeepEqual(receipt, after.Receipts[label]) {
			t.Fatal("existing core receipt changed", label)
		}
	}
	if !reflect.DeepEqual(before.Row(t, unrelated.ID), after.Row(t, unrelated.ID)) || !reflect.DeepEqual(before.Versions[unrelated.ID], after.Versions[unrelated.ID]) || !reflect.DeepEqual(before.Checkpoints["b07positive/unrelated"], after.Checkpoints["b07positive/unrelated"]) {
		t.Fatal("mixed success changed unrelated state")
	}
	var doc pipeline.Document
	if err := json.Unmarshal(after.Row(t, durable.ID).Body, &doc); err != nil || doc.Fields["title"] != "durable committed" {
		t.Fatal("exact durable patch missing", err)
	}
	get := h.Call("bead.get", "read-active", map[string]any{"id": active}, "")
	assertPublicItem(t, get.Result, active.ID, "b07positive", "active committed", workflowplugin.ItemKind, item.RunID)
	durableRead := h.Call("bead.get", "read-durable", map[string]any{"id": durable}, "")
	assertPublicItem(t, durableRead.Result, durable.ID, "b07positive", "durable committed", "", "")
	assertWorkflow(t, before, after, item.ID, durable.ID, unrelated.ID, metadata)
	assertOrdinaryCoreEffect(t, before, after, durable.ID, "durable committed")
	proof.Same(t, after, h.Capture())
	h.Run("ReplayAndReopen", func(t *testing.T) {
		replay := h.Call("tx.apply", "mixed-success", map[string]any{"changeSet": change}, "")
		if !replay.Meta.Replayed || !bytes.Equal(reply.Result, replay.Result) {
			t.Fatal("mixed replay lost exact result or replay flag")
		}
		proof.Same(t, after, h.Capture())
		h.Reopen()
		proof.Same(t, after, h.Capture())
		replay = h.Call("tx.apply", "mixed-success", map[string]any{"changeSet": change}, "")
		if !replay.Meta.Replayed || !bytes.Equal(reply.Result, replay.Result) {
			t.Fatal("reopened mixed replay changed result")
		}
		proof.Same(t, after, h.Capture())
	})
	h.Run("ChangedRequestKeyReuseConservesBothPlanes", func(t *testing.T) {
		changed := model.ChangeSet{Ops: append([]model.Op(nil), change.Ops...)}
		changed.Ops[0] = model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: active, Fields: map[string]any{"title": "same key different request"}}}
		h.Call("tx.apply", "mixed-success", map[string]any{"changeSet": changed}, "idempotency_key_reused")
		proof.Same(t, after, h.Capture())
	})
	h.Run("LateRetypeRefusalRollsBackBothPlanes", func(t *testing.T) {
		bad := model.ChangeSet{Ops: []model.Op{
			{Kind: model.OpPatch, Patch: &model.PatchOp{Target: active, Fields: map[string]any{"title": "discard active retype draft"}}},
			{Kind: model.OpPatch, Patch: &model.PatchOp{Target: durable, Fields: map[string]any{"title": "discard durable retype draft"}}},
			{Kind: model.OpUnlink, Link: &model.LinkOp{From: active, To: unrelated, LinkKind: "relates-to"}},
			{Kind: model.OpLink, Link: &model.LinkOp{From: durable, To: active, LinkKind: "blocks"}},
		}}
		h.Call("tx.apply", "late-retype", map[string]any{"changeSet": bad}, "ontology_guard_failed")
		proof.Same(t, after, h.Capture())
	})
	h.Run("LateRefusalConservesBothPlanes", func(t *testing.T) {
		bad := model.ChangeSet{Ops: append(append([]model.Op{}, change.Ops...), model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("genuinely-missing", model.Natural64(0)), Fields: map[string]any{"title": "never"}}})}
		bad.Ops[0] = model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: active, Fields: map[string]any{"title": "unpublished active draft"}}}
		bad.Ops[1] = model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: durable, Fields: map[string]any{"title": "unpublished durable draft"}}}
		h.Call("tx.apply", "late-missing", map[string]any{"changeSet": bad}, "not_found")
		proof.Same(t, after, h.Capture())
		absent := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(workflowplugin.ItemPrefix+strings.Repeat("f", 32), model.Natural64(0)), Fields: map[string]any{"title": "never"}}}}}
		h.Call("tx.apply", "absent-active", map[string]any{"changeSet": absent}, "not_found")
		proof.Same(t, after, h.Capture())
	})
	h.Run("TypedUnsupportedConservesBothPlanes", func(t *testing.T) {
		external := model.ChangeSet{Ops: []model.Op{change.Ops[0], change.Ops[1], {Kind: model.OpLink, Link: &model.LinkOp{From: active, To: model.RefByID("external-target", model.Natural64(0)), TargetKind: model.LinkTargetExternal, LinkKind: "relates-to"}}}}
		h.Call("tx.apply", "unsupported-external", map[string]any{"changeSet": external}, "unsupported")
		proof.Same(t, after, h.Capture())
	})
	h.Run("WrongProjectAndMalformedMetadataConserveBothPlanes", func(t *testing.T) {
		h.Call("tx.apply", "generic-wrong-project", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{change.Ops[0], change.Ops[1]}}, "_meta": map[string]any{"project": "another-project"}}, "forbidden")
		proof.Same(t, after, h.Capture())
		h.Call("bead.link", "wrong-project", map[string]any{"from": active, "to": unrelated, "kind": "relates-to", "_meta": map[string]any{"project": "another-project"}}, "forbidden")
		proof.Same(t, after, h.Capture())
		// Keep the opaque token malformed on the actual public wire. The typed
		// ChangeSet marshaler correctly refuses it before any RPC can execute.
		bad := map[string]any{"ops": []any{change.Ops[0], change.Ops[1], map[string]any{
			"kind": model.OpLink, "from": active, "to": unrelated, "linkKind": "relates-to",
			"metadata": map[string]any{"nativeExtensions": map[string]string{"beads.future_invalid": "null true"}},
		}}}
		h.Call("tx.apply", "malformed-token", map[string]any{"changeSet": bad}, "invalid_params")
		proof.Same(t, after, h.Capture())
	})
	h.Run("SameTypeFreshKeyHasOneNamespaceReceiptEffect", func(t *testing.T) {
		duplicate := h.Call("bead.link", "fresh-duplicate", map[string]any{"from": active, "to": unrelated, "kind": "relates-to", "metadata": metadata}, "")
		next := h.Capture()
		assertNamespaceReceiptOnly(t, after, next)
		replay := h.Call("bead.link", "fresh-duplicate", map[string]any{"from": active, "to": unrelated, "kind": "relates-to", "metadata": metadata}, "")
		if !bytes.Equal(duplicate.Result, replay.Result) {
			t.Fatal("direct duplicate replay changed original result")
		}
		proof.Same(t, next, h.Capture())
	})
}

func assertPublicItem(t *testing.T, raw json.RawMessage, id, project, title, kind, run string) {
	t.Helper()
	var got struct {
		Bead struct {
			ID, Project, Kind, Plane, Plugin string
			WorkflowRunID                    string `json:"workflowRunId"`
			Fields                           map[string]any
		} `json:"bead"`
	}
	if err := json.Unmarshal(raw, &got); err != nil || got.Bead.ID != id || got.Bead.Project != project || got.Bead.Fields["title"] != title || got.Bead.Kind != kind || got.Bead.WorkflowRunID != run {
		t.Fatalf("public exact identity/value readback differs: %s (%v)", raw, err)
	}
	if kind != "" && (got.Bead.Plane != "ephemeral" || got.Bead.Plugin != workflowplugin.Alias) {
		t.Fatal("public readback lost actual provider/plane")
	}
}

func assertOrdinaryCoreEffect(t *testing.T, before, after proof.State, changed, title string) {
	t.Helper()
	proof.DerivedTitleEffect(t, before, after, changed)
	expectedMeta := before.Meta
	head, err := before.Meta.Head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	expectedMeta.Head = model.Natural64(head + 1)
	if after.Meta != expectedMeta || len(before.Rows) != len(after.Rows) || len(before.Checkpoints) != len(after.Checkpoints) || !reflect.DeepEqual(before.Changes, after.Changes[:len(before.Changes)]) {
		t.Fatal("mixed success changed core identity/inventory/old log")
	}
	for _, row := range before.Rows {
		next := after.Row(t, row.Ref.ID)
		if row.Ref.ID != changed {
			if !reflect.DeepEqual(row, next) || !reflect.DeepEqual(before.Versions[row.Ref.ID], after.Versions[row.Ref.ID]) {
				t.Fatal("mixed success changed unrelated record/version", row.Ref.ID)
			}
			continue
		}
		oldDoc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			t.Fatal(err)
		}
		nextDoc, err := pipeline.DecodeDocument(next.Body)
		if err != nil {
			t.Fatal(err)
		}
		oldDoc.Fields["title"] = title
		if !reflect.DeepEqual(oldDoc, nextDoc) {
			t.Fatalf("durable patch changed data beyond its exact requested title: want %+v, got %+v", oldDoc, nextDoc)
		}
		// Memory retains the create's inferred ID arm and the patch's explicit
		// ID arm internally. Compare their complete public reference encoding.
		oldRef, err := json.Marshal(row.Ref)
		if err != nil {
			t.Fatal(err)
		}
		nextRef, err := json.Marshal(next.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(oldRef, nextRef) || next.Kind != row.Kind || next.Revision != after.Meta.Head || len(after.Versions[changed]) != len(before.Versions[changed])+1 {
			t.Fatal("durable patch changed public identity/kind or missed the exact revision/version effect")
		}
		if !reflect.DeepEqual(before.Versions[changed], after.Versions[changed][:len(before.Versions[changed])]) {
			t.Fatal("mixed patch changed retained durable versions")
		}
	}
	for key, checkpoint := range before.Checkpoints {
		if key != proof.Namespace && !reflect.DeepEqual(checkpoint, after.Checkpoints[key]) {
			t.Fatal("mixed success changed unrelated namespace", key)
		}
	}
}

func assertNamespaceReceiptOnly(t *testing.T, before, after proof.State) {
	t.Helper()
	proof.WorkflowNamespaceEffect(t, before, after)
	if after.Checkpoints[proof.Namespace].Generation != before.Checkpoints[proof.Namespace].Generation+1 {
		t.Fatal("fresh duplicate did not publish one namespace generation")
	}
	oldLogical, newLogical := before.Workflow(t), after.Workflow(t)
	var oldReceipts, newReceipts map[string]json.RawMessage
	if json.Unmarshal(oldLogical["receipts"], &oldReceipts) != nil || json.Unmarshal(newLogical["receipts"], &newReceipts) != nil || len(newReceipts) != len(oldReceipts)+1 {
		t.Fatal("fresh duplicate did not add exactly one provider receipt")
	}
	for key, receipt := range oldReceipts {
		if !bytes.Equal(receipt, newReceipts[key]) {
			t.Fatal("fresh duplicate changed an old provider receipt")
		}
	}
	delete(oldLogical, "receipts")
	delete(newLogical, "receipts")
	if !reflect.DeepEqual(oldLogical, newLogical) {
		t.Fatal("fresh duplicate changed item/edge/run/generation/audit state")
	}
	changed := after.Checkpoints[proof.Namespace]
	after.Checkpoints = copyCheckpoints(after.Checkpoints)
	after.Checkpoints[proof.Namespace] = before.Checkpoints[proof.Namespace]
	proof.Same(t, before, after)
	if len(changed.Payload) == 0 {
		t.Fatal("fresh duplicate lost namespace payload")
	}
}

func copyCheckpoints(values map[string]driver.Checkpoint) map[string]driver.Checkpoint {
	result := make(map[string]driver.Checkpoint, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func assertWorkflow(t *testing.T, before, after proof.State, active, from, to string, metadata *model.LinkMetadata) {
	t.Helper()
	proof.WorkflowEffect(t, before, after, active, "active committed", "bead.update", "bead.link", "bead.link")
	var value struct {
		Items map[string]struct {
			Title          string         `json:"title"`
			Fields         map[string]any `json:"fields"`
			Links, Inbound []workflowplugin.RelationView
		} `json:"items"`
		Receipts map[string]json.RawMessage `json:"receipts"`
	}
	logical := after.Workflow(t)
	data, _ := json.Marshal(logical)
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	item := value.Items[active]
	if item.Title != "active committed" || len(item.Links) != 1 || len(item.Inbound) != 1 || item.Links[0].From != active || item.Links[0].To.ID != to || item.Inbound[0].From != from || item.Inbound[0].To.ID != active {
		t.Fatalf("exact committed active mixed edges missing: %+v", item)
	}
	if item.Links[0].Kind != "relates-to" || item.Inbound[0].Kind != "relates-to" || !reflect.DeepEqual(item.Links[0].Metadata, metadata) || !reflect.DeepEqual(item.Inbound[0].Metadata, metadata) {
		t.Fatal("mixed edge kind or exact opaque metadata differs")
	}
	var oldReceipts map[string]json.RawMessage
	if err := json.Unmarshal(before.Workflow(t)["receipts"], &oldReceipts); err != nil {
		t.Fatal(err)
	}
	if len(value.Receipts) != len(oldReceipts)+3 {
		t.Fatal("mixed operations did not retain three intended provider receipts")
	}
	for key, receipt := range oldReceipts {
		if !bytes.Equal(receipt, value.Receipts[key]) {
			t.Fatal("existing provider receipt changed")
		}
	}
	oldLogical := before.Workflow(t)
	if _, fabricated := logical["source"]; fabricated {
		t.Fatal("ordinary workflow fabricated admitted source attribution")
	}
	for key, raw := range oldLogical {
		if key != "items" && key != "receipts" && key != "audit" && !bytes.Equal(raw, logical[key]) {
			t.Fatal("mixed operations changed unrelated provider state", key)
		}
	}
	var oldAudit, newAudit []json.RawMessage
	if json.Unmarshal(oldLogical["audit"], &oldAudit) != nil || json.Unmarshal(logical["audit"], &newAudit) != nil || len(newAudit) != len(oldAudit)+3 || !reflect.DeepEqual(oldAudit, newAudit[:len(oldAudit)]) {
		t.Fatal("mixed operations changed retained audit or lost intended audit effects")
	}
}

// The explicit legacy durable-only constructor remains fail-closed on memory;
// process-lifetime success cannot silently become a restart-durability claim.
func TestB07OrdinaryMemoryDurableCapabilityRefusalConservation(t *testing.T) {
	h := publicproof.NewHost(t, "memory")
	before := h.Capture()
	state := pluginhost.NewDurableState(h.Runtime.Store.(driver.CheckpointStore), h.Runtime.Lease, "workflow", "default")
	provider, err := workflowplugin.New(state)
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Invoke(context.Background(), publicproof.Anonymous, workflowplugin.InvokeRequest{Template: "unavailable", Project: "b07positive", Mode: "ephemeral", Checklist: []workflowplugin.ChecklistItem{{Label: "one", Title: "never"}}}, model.WriteMeta{Project: "b07positive", IdempotencyKey: strings.Repeat("r", 32)})
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "plugin_durable_state_unavailable" {
		t.Fatalf("legacy capability refusal changed: %v", err)
	}
	proof.Same(t, before, h.Capture())
	proof.Save(t, h.Root, "capability-refusal.json", map[string]any{"code": typed.Data.Code, "route": "explicit restart-durable provider on process-lifetime memory", "positiveCredit": false})
}

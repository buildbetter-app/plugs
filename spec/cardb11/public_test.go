package cardb11_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	public "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflow "github.com/buildbetter-app/plugs/plugins/workflow"
)

// Adopted PR467 and binding17:14 admit truthful process-only memory state.
func checkpointLifetime(t *testing.T, h *public.Host, engine string) {
	t.Helper()
	capability, err := driver.RequireWorkflowCheckpointStore(h.Runtime.Store, h.Runtime.Lease)
	if err != nil {
		t.Fatal(err)
	}
	persistent := false
	marker, present := capability.(driver.PersistentCheckpointStore)
	if present {
		persistent = marker.Persistent()
	}
	want := driver.CheckpointLifetimeRestart
	if engine == "memory" {
		want = driver.CheckpointLifetimeProcess
	}
	if capability.WorkflowCheckpointLifetime() != want || persistent != (engine != "memory") {
		t.Fatal("selected engine misrepresented checkpoint lifetime or persistence")
	}
	proof.Save(t, h.Root, "b11-lifetime.json", map[string]any{"lifetime": capability.WorkflowCheckpointLifetime(), "persistence_marker_present": present, "persistent": persistent})
}

func metadata(text string) *model.LinkMetadata {
	token, err := json.Marshal(text)
	if err != nil {
		panic(err)
	}
	return &model.LinkMetadata{NativeExtensions: map[string]string{
		"beads.metadata":  string(token),
		"beads.thread_id": `"  exact thread  "`,
		"beads.future":    `{ "wide":9007199254740993, "value":null }`,
	}}
}

func document(t *testing.T, state proof.State, ref model.Ref) pipeline.Document {
	t.Helper()
	doc, err := pipeline.DecodeDocument(state.Row(t, ref.ID).Body)
	if err != nil || doc.ID != ref.ID {
		t.Fatal("invalid committed endpoint", err)
	}
	return doc
}

func onlyNoopReceipt(t *testing.T, h *public.Host, before proof.State, label string, reply public.Reply) {
	t.Helper()
	var commit model.Commit
	if err := json.Unmarshal(reply.Result, &commit); err != nil || commit.Revision != before.Meta.Head {
		t.Fatal("metadata-equal re-add advanced the public revision", err)
	}
	after := h.Capture()
	receipt, ok := after.Receipts[label]
	if !ok || receipt.Pending || !reflect.DeepEqual(receipt.Changeset, model.Changeset{}) || !reflect.DeepEqual(receipt.Commit, commit) {
		t.Fatal("missing completed no-op receipt")
	}
	delete(after.Receipts, label)
	proof.Same(t, before, after)
}

func publicEdge(t *testing.T, h *public.Host, from, to model.Ref, kind string, want *model.LinkMetadata) {
	t.Helper()
	reply := h.Call("bead.get", "read-"+from.ID, map[string]any{"id": from}, "")
	var got struct {
		Bead struct {
			ID    string
			Links []workflow.RelationView
		}
	}
	if err := json.Unmarshal(reply.Result, &got); err != nil || got.Bead.ID != from.ID || len(got.Bead.Links) != 1 {
		t.Fatal("public endpoint/edge inventory differs", err)
	}
	edge := got.Bead.Links[0]
	if edge.From != from.ID || edge.To != to || edge.Kind != kind || !reflect.DeepEqual(edge.Metadata, want) {
		t.Fatalf("public edge tuple/value differs: %+v", edge)
	}
	doc := document(t, h.Capture(), from)
	if len(doc.Links) != 1 || doc.Links[0].To != to || doc.Links[0].Kind != kind || !reflect.DeepEqual(doc.Links[0].Metadata, want) {
		t.Fatal("public edge differs from exact committed body")
	}
}

func TestB11PublicMetadataIdentityAndConservation(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			h := public.NewHost(t, engine)
			from, to, sibling := h.Create("source"), h.Create("target"), h.Create("unchanged sibling")
			original := metadata(` {"a":1.0,"b":[true,null,"x"],"wide":9007199254740993} `)
			params := func(value *model.LinkMetadata) map[string]any {
				return map[string]any{"from": from, "to": to, "kind": "related", "metadata": value}
			}
			first := h.Call("bead.link", "initial-link", params(original), "")
			publicEdge(t, h, from, to, "related", original)
			initial := h.Capture()
			for i, equivalent := range []string{
				`{"wide":9007199254740993.0,"b":[true,null,"x"],"a":1e0}`,
				`{"a":1,"wide":9007199254740993,"b":[true,null,"\u0078"]}`,
				`{"a":1,"wide":9007199254740992,"b":[true,null,"x"]}`,
			} {
				label := []string{"equal-number-form", "equal-order-and-escape", "native-wide-rounding"}[i]
				before := h.Capture()
				reply := h.Call("bead.link", label, params(metadata(equivalent)), "")
				onlyNoopReceipt(t, h, before, label, reply)
				publicEdge(t, h, from, to, "related", original)
			}
			h.Reopen()
			publicEdge(t, h, from, to, "related", original)
			before := h.Capture()
			replay := h.Call("bead.link", "initial-link", params(original), "")
			if !replay.Meta.Replayed || !bytes.Equal(replay.Result, first.Result) {
				t.Fatal("original public link response did not replay after SQL reopen")
			}
			proof.Same(t, before, h.Capture())
			// A7/#11 adopts immutable keys: display updates keep the same endpoints.
			h.Call("bead.update", "display-update", map[string]any{"id": to, "fields": map[string]any{"title": "renamed display"}}, "")
			publicEdge(t, h, from, to, "related", original)
			afterDisplay := h.Capture()
			if !reflect.DeepEqual(initial.Row(t, from.ID), afterDisplay.Row(t, from.ID)) || !reflect.DeepEqual(initial.Row(t, sibling.ID), afterDisplay.Row(t, sibling.ID)) || document(t, afterDisplay, to).Fields["title"] != "renamed display" {
				t.Fatal("display update changed endpoint/link identity or sibling")
			}
			before = h.Capture()
			h.Call("tx.apply", "late-type-conflict", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: sibling, Fields: map[string]any{"title": "must roll back"}}},
				{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: "blocks"}},
			}}}, "ontology_guard_failed")
			proof.Same(t, before, h.Capture())
			// A distinct native JCS value writes exactly one source version.
			changed := metadata(`{"a":1,"b":[true,null,"x"],"wide":9007199254740994}`)
			h.Call("bead.link", "wide-value-changed", params(changed), "")
			after := h.Capture()
			if len(after.Changes) != len(before.Changes)+1 || len(after.Versions[from.ID]) != len(before.Versions[from.ID])+1 || !reflect.DeepEqual(before.Row(t, to.ID), after.Row(t, to.ID)) || !reflect.DeepEqual(before.Row(t, sibling.ID), after.Row(t, sibling.ID)) {
				t.Fatal("metadata value change did not conserve endpoints/history")
			}
			publicEdge(t, h, from, to, "related", changed)
			// Retarget/retype is the installed ordered unlink/link transaction.
			before = h.Capture()
			h.Call("tx.apply", "replace-endpoint-and-type", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpUnlink, Link: &model.LinkOp{From: from, To: to, LinkKind: "related"}},
				{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: sibling, LinkKind: "relates-to", Metadata: changed}},
			}}}, "")
			after = h.Capture()
			if len(after.Rows) != len(before.Rows) || len(after.Changes) != len(before.Changes)+1 || !reflect.DeepEqual(before.Row(t, to.ID), after.Row(t, to.ID)) || !reflect.DeepEqual(before.Row(t, sibling.ID), after.Row(t, sibling.ID)) {
				t.Fatal("ordered edge replacement duplicated/lost an endpoint")
			}
			publicEdge(t, h, from, sibling, "relates-to", changed)
			before = h.Capture()
			h.Reopen()
			proof.Same(t, before, h.Capture())
			publicEdge(t, h, from, sibling, "relates-to", changed)
		})
	}
}

// Plug's opaque metadata-text tokens can represent values that native's JSON
// column cannot store. These controls qualify the copied comparator's byte
// fallback and exact Plug token retention, separately from native admission.
func TestB11PublicOpaqueMetadataByteFallback(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			h := public.NewHost(t, engine)
			for _, test := range []struct {
				name, initial, incoming string
				equal                   bool
			}{
				{"overflow-different", `{"n":1e999999999}`, `{"n":10e999999998}`, false},
				{"overflow-identical", `{"n":1e999999999}`, `{"n":1e999999999}`, true},
				{"duplicate-different", `{"x":1,"x":2}`, `{"x":2}`, false},
				{"duplicate-identical", `{"x":1,"x":2}`, `{"x":1,"x":2}`, true},
			} {
				h.Run(test.name, func(t *testing.T) {
					from, to := h.Create(test.name+" source"), h.Create(test.name+" target")
					original, incoming := metadata(test.initial), metadata(test.incoming)
					params := map[string]any{"from": from, "to": to, "kind": "related", "metadata": original}
					h.Call("bead.link", test.name+"-initial", params, "")
					before := h.Capture()
					params["metadata"] = incoming
					label := test.name + "-readd"
					reply := h.Call("bead.link", label, params, "")
					want := incoming
					if test.equal {
						onlyNoopReceipt(t, h, before, label, reply)
						want = original
					} else {
						after := h.Capture()
						if len(after.Changes) != len(before.Changes)+1 || len(after.Versions[from.ID]) != len(before.Versions[from.ID])+1 || !reflect.DeepEqual(before.Row(t, to.ID), after.Row(t, to.ID)) {
							t.Fatal("native byte-fallback difference did not make one conserved source write")
						}
					}
					publicEdge(t, h, from, to, "related", want)
					before = h.Capture()
					h.Reopen()
					proof.Same(t, before, h.Capture())
					publicEdge(t, h, from, to, "related", want)
					replay := h.Call("bead.link", label, params, "")
					if !replay.Meta.Replayed || !bytes.Equal(replay.Result, reply.Result) {
						t.Fatal("native comparison fallback did not replay the exact committed reply")
					}
					proof.Same(t, before, h.Capture())
				})
			}
		})
	}
}

func TestB11PublicUnresolvedExternalIdentity(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			h := public.NewHost(t, engine)
			from := h.Create("source with unresolved external")
			to := model.RefByID("external:tracker/ticket-1", model.Natural64(0))
			params := map[string]any{"from": from, "to": to, "targetKind": "external", "kind": "related"}
			first := h.Call("bead.link", "external-first", params, "")
			publicEdge(t, h, from, to, "related", nil)
			before := h.Capture()
			doc := document(t, before, from)
			if len(before.Rows) != 1 || doc.Links[0].TargetKind != model.LinkTargetExternal {
				t.Fatal("external unresolved identity materialized a local endpoint")
			}
			params["kind"] = "blocks"
			h.Call("bead.link", "external-retype", params, "ontology_guard_failed")
			proof.Same(t, before, h.Capture())
			params["kind"] = "related"
			h.Reopen()
			proof.Same(t, before, h.Capture())
			replay := h.Call("bead.link", "external-first", params, "")
			if !replay.Meta.Replayed || !bytes.Equal(first.Result, replay.Result) {
				t.Fatal("unresolved external identity failed exact replay")
			}
			proof.Same(t, before, h.Capture())
			h.Call("bead.unlink", "external-unlink", params, "")
			after := h.Capture()
			if len(after.Rows) != 1 || len(document(t, after, from).Links) != 0 {
				t.Fatal("external unlink changed endpoint inventory")
			}
			h.Reopen()
			proof.Same(t, after, h.Capture())
		})
	}
}

func TestB11PublicCrossPlaneEndpointIdentity(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			h := public.NewHost(t, engine)
			durable, sibling := h.Create("durable endpoint"), h.Create("untouched endpoint")
			invoke := map[string]any{"template": "b11-endpoints", "project": "b07positive", "mode": "ephemeral", "checklist": []any{
				map[string]any{"label": "source", "title": "active source"},
				map[string]any{"label": "target", "title": "active target"},
			}}
			checkpointLifetime(t, h, engine)
			reply := h.Call("x.workflow.invoke", "active-endpoints", invoke, "")
			var invoked workflow.InvokeResult
			if err := json.Unmarshal(reply.Result, &invoked); err != nil || len(invoked.Items) != 2 {
				t.Fatal("public invocation did not produce two actual endpoints", err)
			}
			active := model.RefByID(invoked.Items[0].ID, model.Natural64(0))
			other := model.RefByID(invoked.Items[1].ID, model.Natural64(0))
			for _, pair := range []struct {
				name     string
				from, to model.Ref
			}{{"durable-to-active", durable, active}, {"active-to-durable", active, sibling}, {"active-to-active", active, other}} {
				h.Run(pair.name, func(t *testing.T) {
					value := metadata(` {"n":1.0,"wide":9007199254740993} `)
					params := map[string]any{"from": pair.from, "to": pair.to, "kind": "related", "metadata": value}
					before := h.Capture()
					first := h.Call("bead.link", pair.name+"-link", params, "")
					after := h.Capture()
					if !reflect.DeepEqual(before.Rows, after.Rows) || !reflect.DeepEqual(before.Versions, after.Versions) || !reflect.DeepEqual(before.Derived, after.Derived) || !reflect.DeepEqual(before.Changes, after.Changes) {
						t.Fatal("cross-plane edge duplicated ordinary rows/history")
					}
					read := h.Call("bead.get", pair.name+"-read", map[string]any{"id": pair.from}, "")
					var projection struct {
						Bead struct {
							ID    string
							Links []workflow.RelationView
						}
					}
					if err := json.Unmarshal(read.Result, &projection); err != nil || projection.Bead.ID != pair.from.ID {
						t.Fatal("public mixed endpoint is absent", err)
					}
					count := 0
					for _, edge := range projection.Bead.Links {
						if edge.To == pair.to {
							count++
							if edge.From != pair.from.ID || edge.Kind != "related" || !reflect.DeepEqual(edge.Metadata, value) {
								t.Fatal("cross-plane endpoint tuple/token changed")
							}
						}
					}
					if count != 1 {
						t.Fatal("public cross-plane pair was duplicated or dropped")
					}
					before = h.Capture()
					params["kind"] = "blocks"
					h.Call("bead.link", pair.name+"-retype", params, "ontology_guard_failed")
					proof.Same(t, before, h.Capture())
					params["kind"] = "related"
					h.Reopen()
					proof.Same(t, before, h.Capture())
					replay := h.Call("bead.link", pair.name+"-link", params, "")
					if !bytes.Equal(first.Result, replay.Result) {
						t.Fatal("cross-plane response changed after physical reopen")
					}
					proof.Same(t, before, h.Capture())
				})
			}
			before := h.Capture()
			missing := model.RefByID(workflow.ItemPrefix+strings.Repeat("f", 32), model.Natural64(0))
			h.Call("bead.link", "unknown-active-target", map[string]any{"from": durable, "to": missing, "kind": "related"}, "not_found")
			proof.Same(t, before, h.Capture())
			h.Reopen()
			proof.Same(t, before, h.Capture())
		})
	}
}

// Nested metadata is compared by the native JCS value while the original
// token remains the committed value.  A same-endpoint retype is a typed
// conflict until the caller explicitly unlinks the existing relation.
func TestB11PublicNestedMetadataAndTypedEndpointConflict(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			h := public.NewHost(t, engine)
			from, to, sibling := h.Create("nested source"), h.Create("nested target"), h.Create("nested sibling")
			original := metadata(` {"outer":{"alpha":{"deep":[{"x":1.0,"y":"v"}],"nil":null},"beta":[true,{"z":2e+0}]},"array":[{"q":[1,2,3]}],"n":9007199254740993} `)
			params := func(kind string, value *model.LinkMetadata) map[string]any {
				params := map[string]any{"from": from, "to": to, "kind": kind}
				if value != nil {
					params["metadata"] = value
				}
				return params
			}
			first := h.Call("bead.link", "nested-initial", params("related", original), "")
			publicEdge(t, h, from, to, "related", original)

			before := h.Capture()
			conflict := h.Call("bead.link", "nested-retype", params("blocks", nil), "ontology_guard_failed")
			if conflict.Error == nil || conflict.Error.Data.Code != "ontology_guard_failed" {
				t.Fatal("same-endpoint different-type link was not typed-refused")
			}
			proof.Same(t, before, h.Capture())

			equivalent := metadata(` { "n":9007199254740992.0, "array":[{"q":[1.0,2e0,3.00]}], "outer":{"beta":[true,{"z":2}],"alpha":{"nil":null,"deep":[{"y":"v","x":1}]}} } `)
			before = h.Capture()
			equal := h.Call("bead.link", "nested-semantic-equal", params("related", equivalent), "")
			onlyNoopReceipt(t, h, before, "nested-semantic-equal", equal)
			publicEdge(t, h, from, to, "related", original)

			changed := metadata(`{"n":9007199254740994,"array":[{"q":[1,2,3]}],"outer":{"alpha":{"deep":[{"x":1.0,"y":"v"}],"nil":null},"beta":[true,{"z":3}]}}`)
			before = h.Capture()
			h.Call("bead.link", "nested-value-change", params("related", changed), "")
			after := h.Capture()
			if len(after.Changes) != len(before.Changes)+1 || len(after.Versions[from.ID]) != len(before.Versions[from.ID])+1 || !reflect.DeepEqual(before.Row(t, sibling.ID), after.Row(t, sibling.ID)) {
				t.Fatal("nested metadata value change did not conserve unrelated endpoint state")
			}
			publicEdge(t, h, from, to, "related", changed)

			before = h.Capture()
			h.Reopen()
			proof.Same(t, before, h.Capture())
			publicEdge(t, h, from, to, "related", changed)
			replay := h.Call("bead.link", "nested-initial", params("related", original), "")
			if !replay.Meta.Replayed || !bytes.Equal(replay.Result, first.Result) {
				t.Fatal("nested endpoint identity did not replay the original response")
			}
			proof.Same(t, before, h.Capture())
		})
	}
}

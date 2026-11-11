package cardb11_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	public "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	workflow "github.com/buildbetter-app/plugs/plugins/workflow"
)

func movedEdge(t *testing.T, h *public.Host, from, to model.Ref, value *model.LinkMetadata) {
	t.Helper()
	reply := h.Call("bead.get", "move-read-"+from.ID, map[string]any{"id": from}, "")
	var got struct {
		Bead struct {
			ID    string
			Links []workflow.RelationView
		}
	}
	if err := json.Unmarshal(reply.Result, &got); err != nil || got.Bead.ID != from.ID || len(got.Bead.Links) != 1 {
		t.Fatal("plane move duplicated or lost the source/edge", err)
	}
	want := workflow.RelationView{From: from.ID, To: to, Kind: "related", Metadata: value}
	if !reflect.DeepEqual(got.Bead.Links[0], want) {
		t.Fatalf("plane move changed the canonical endpoint tuple or lexical metadata: got=%+v want=%+v", got.Bead.Links[0], want)
	}
}

func TestB11PublicPlaneMoveConservesEdges(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			h := public.NewHost(t, engine)
			source, outgoing, incoming, target, sibling := h.Create("move source"), h.Create("move outgoing"), h.Create("move incoming"), h.Create("promotion target"), h.Create("untouched sibling")
			value := metadata(` {"wide":9007199254740993,"n":1.0} `)
			for _, pair := range [][2]model.Ref{{source, outgoing}, {incoming, source}} {
				h.Call("bead.link", "move-link-"+pair[0].ID, map[string]any{"from": pair[0], "to": pair[1], "kind": "related", "metadata": value}, "")
			}
			before := h.Capture()
			params := map[string]any{"source": source, "template": "b11-move", "label": "moved"}
			// Frozen workflow-contract.md57-65 forbids a restart-durable move
			// into the process-local memory provider. The refusal is the proof.
			if engine == "memory" {
				h.Call("x.workflow.run.demote", "demote", params, "plugin_durable_state_unavailable")
				proof.Same(t, before, h.Capture())
				return
			}
			first := h.Call("x.workflow.run.demote", "demote", params, "")
			var demoted workflow.DemoteResult
			if err := json.Unmarshal(first.Result, &demoted); err != nil || demoted.Item.ID == "" {
				t.Fatal("demotion returned no new workflow endpoint", err)
			}
			active := model.RefByID(demoted.Item.ID, model.Natural64(0))
			movedEdge(t, h, active, outgoing, value)
			movedEdge(t, h, incoming, active, value)
			h.Call("bead.get", "old-durable-absent", map[string]any{"id": source}, "not_found")
			after := h.Capture()
			if !reflect.DeepEqual(before.Row(t, outgoing.ID), after.Row(t, outgoing.ID)) || !reflect.DeepEqual(before.Row(t, target.ID), after.Row(t, target.ID)) || !reflect.DeepEqual(before.Row(t, sibling.ID), after.Row(t, sibling.ID)) || len(after.Versions[active.ID]) != 0 {
				t.Fatal("demotion changed unrelated endpoint rows or created a core workflow version")
			}
			h.Reopen()
			proof.Same(t, after, h.Capture())
			movedEdge(t, h, active, outgoing, value)
			movedEdge(t, h, incoming, active, value)
			replay := h.Call("x.workflow.run.demote", "demote", params, "")
			if !bytes.Equal(first.Result, replay.Result) {
				t.Fatal("demotion replay changed the committed response")
			}
			proof.Same(t, after, h.Capture())
			promote := map[string]any{"item": active, "target": target}
			promoted := h.Call("x.workflow.run.promote", "promote", promote, "")
			movedEdge(t, h, target, outgoing, value)
			movedEdge(t, h, incoming, target, value)
			h.Call("bead.get", "old-active-absent", map[string]any{"id": active}, "not_found")
			afterPromotion := h.Capture()
			if document(t, afterPromotion, target).Fields["title"] != "move source" || !reflect.DeepEqual(after.Row(t, outgoing.ID), afterPromotion.Row(t, outgoing.ID)) || !reflect.DeepEqual(after.Row(t, sibling.ID), afterPromotion.Row(t, sibling.ID)) || len(afterPromotion.Versions[active.ID]) != 0 {
				t.Fatal("promotion changed the body/endpoint inventory or created a core workflow version")
			}
			h.Reopen()
			proof.Same(t, afterPromotion, h.Capture())
			movedEdge(t, h, target, outgoing, value)
			movedEdge(t, h, incoming, target, value)
			replay = h.Call("x.workflow.run.promote", "promote", promote, "")
			if !bytes.Equal(promoted.Result, replay.Result) {
				t.Fatal("promotion replay changed the committed response")
			}
			proof.Same(t, afterPromotion, h.Capture())
		})
	}
}

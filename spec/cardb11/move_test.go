package cardb11_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	public "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	workflow "github.com/buildbetter-app/plugs/plugins/workflow"
)

func movedEdges(t *testing.T, h *public.Host, from model.Ref, want []workflow.RelationView) {
	t.Helper()
	reply := h.Call("bead.get", "move-read-"+from.ID, map[string]any{"id": from}, "")
	var got struct {
		Bead struct {
			ID    string
			Links []workflow.RelationView
		}
	}
	if err := json.Unmarshal(reply.Result, &got); err != nil || got.Bead.ID != from.ID || len(got.Bead.Links) != len(want) {
		t.Fatal("plane move duplicated or lost the source/edge", err)
	}
	less := func(edges []workflow.RelationView) {
		sort.Slice(edges, func(i, j int) bool {
			a, _ := json.Marshal(edges[i])
			b, _ := json.Marshal(edges[j])
			return string(a) < string(b)
		})
	}
	less(got.Bead.Links)
	less(want)
	if !reflect.DeepEqual(got.Bead.Links, want) {
		t.Fatalf("plane move changed the canonical incident tuples or lexical metadata: got=%+v want=%+v", got.Bead.Links, want)
	}
}

func TestB11PublicPlaneMoveConservesEdges(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			h := public.NewHost(t, engine)
			checkpointLifetime(t, h, engine)
			source, outgoing, incoming, target, sibling := h.Create("move source"), h.Create("move outgoing"), h.Create("move incoming"), h.Create("promotion target"), h.Create("untouched sibling")
			value := metadata(` {"wide":9007199254740993,"n":1.0} `)
			for _, pair := range [][2]model.Ref{{source, outgoing}, {incoming, source}} {
				h.Call("bead.link", "move-link-"+pair[0].ID, map[string]any{"from": pair[0], "to": pair[1], "kind": "related", "metadata": value}, "")
			}
			before := h.Capture()
			params := map[string]any{"source": source, "template": "b11-move", "label": "moved"}
			// The public demote contract promises a restart-durable checkpoint;
			// memory advertises process lifetime and refuses this move capability.
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
			activeEdges := []workflow.RelationView{{From: active.ID, To: outgoing, Kind: "related", Metadata: value}, {From: incoming.ID, To: active, Kind: "related", Metadata: value}}
			incomingEdges := []workflow.RelationView{{From: incoming.ID, To: active, Kind: "related", Metadata: value}}
			movedEdges(t, h, active, activeEdges)
			movedEdges(t, h, incoming, incomingEdges)
			h.Call("bead.get", "old-durable-absent", map[string]any{"id": source}, "not_found")
			after := h.Capture()
			if !reflect.DeepEqual(before.Row(t, outgoing.ID), after.Row(t, outgoing.ID)) || !reflect.DeepEqual(before.Row(t, target.ID), after.Row(t, target.ID)) || !reflect.DeepEqual(before.Row(t, sibling.ID), after.Row(t, sibling.ID)) || len(after.Versions[active.ID]) != 0 {
				t.Fatal("demotion changed unrelated endpoint rows or created a core workflow version")
			}
			h.Reopen()
			proof.Same(t, after, h.Capture())
			movedEdges(t, h, active, activeEdges)
			movedEdges(t, h, incoming, incomingEdges)
			replay := h.Call("x.workflow.run.demote", "demote", params, "")
			if !bytes.Equal(first.Result, replay.Result) {
				t.Fatal("demotion replay changed the committed response")
			}
			proof.Same(t, after, h.Capture())
			promote := map[string]any{"item": active, "target": target}
			promoted := h.Call("x.workflow.run.promote", "promote", promote, "")
			movedEdges(t, h, target, []workflow.RelationView{{From: target.ID, To: outgoing, Kind: "related", Metadata: value}})
			movedEdges(t, h, incoming, []workflow.RelationView{{From: incoming.ID, To: target, Kind: "related", Metadata: value}})
			h.Call("bead.get", "old-active-absent", map[string]any{"id": active}, "not_found")
			afterPromotion := h.Capture()
			if document(t, afterPromotion, target).Fields["title"] != "move source" || !reflect.DeepEqual(after.Row(t, outgoing.ID), afterPromotion.Row(t, outgoing.ID)) || !reflect.DeepEqual(after.Row(t, sibling.ID), afterPromotion.Row(t, sibling.ID)) || len(afterPromotion.Versions[active.ID]) != 0 {
				t.Fatal("promotion changed the body/endpoint inventory or created a core workflow version")
			}
			h.Reopen()
			proof.Same(t, afterPromotion, h.Capture())
			movedEdges(t, h, target, []workflow.RelationView{{From: target.ID, To: outgoing, Kind: "related", Metadata: value}})
			movedEdges(t, h, incoming, []workflow.RelationView{{From: incoming.ID, To: target, Kind: "related", Metadata: value}})
			replay = h.Call("x.workflow.run.promote", "promote", promote, "")
			var initialPromotion, replayedPromotion workflow.PromoteResult
			if json.Unmarshal(promoted.Result, &initialPromotion) != nil || json.Unmarshal(replay.Result, &replayedPromotion) != nil || replayedPromotion.Source != initialPromotion.Source || replayedPromotion.Target != initialPromotion.Target || replayedPromotion.Generation != initialPromotion.Generation || len(replayedPromotion.Relations) != 0 {
				t.Fatal("promotion replay lost terminal identity or retained relation payload")
			}
			proof.Same(t, afterPromotion, h.Capture())
		})
	}
}

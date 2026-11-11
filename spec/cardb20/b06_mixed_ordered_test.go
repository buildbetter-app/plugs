package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

type b06MixedBead struct {
	ID, Project, Plane, Kind string
	Revision                 model.Revision
	Fields                   map[string]any
	Links                    []workflowplugin.RelationView
}

func b06MixedGet(f *b06PlugFixture, id string) b06MixedBead {
	f.t.Helper()
	got := b06RequireSuccess[struct{ Bead b06MixedBead }](f.t, f.call("bead.get", map[string]any{"id": model.RefByID(id, model.Natural64(0))}, false))
	if got.Bead.ID != id || got.Bead.Project != "b06" {
		f.t.Fatalf("public readback wrong identity/project: %+v", got)
	}
	return got.Bead
}

func b06MixedCheckedLink(f *b06PlugFixture, edge b06Edge) {
	f.t.Helper()
	endpoint := edge.From
	if !strings.HasPrefix(endpoint, workflowplugin.ItemPrefix) {
		endpoint = edge.To
	}
	if !strings.HasPrefix(endpoint, workflowplugin.ItemPrefix) {
		b06RequireSuccess[model.Commit](f.t, f.link(edge))
		return
	}
	before := b06MixedGet(f, endpoint)
	core := f.image(fmt.Sprintf("direct-core-before-%d", f.requests))
	cp, err := f.store.(driver.CheckpointStore).LoadCheckpoint(context.Background(), b06WorkflowNamespace)
	if err != nil {
		f.t.Fatal(err)
	}
	commit := b06RequireSuccess[model.Commit](f.t, f.link(edge))
	generation, err := before.Revision.Uint64()
	if err != nil {
		f.t.Fatal(err)
	}
	want := model.Commit{Revision: model.Natural64(0), Results: []model.OpResult{{Ref: model.RefByID(endpoint, model.Natural64(0)), Revision: model.Natural64(generation + 1)}}, Warnings: []model.Warning{}}
	if !reflect.DeepEqual(commit, want) {
		f.t.Fatalf("direct active relation commit %+v want %+v", commit, want)
	}
	after := b06MixedGet(f, endpoint)
	wantBead := before
	wantBead.Revision = model.Natural64(generation + 1)
	wantBead.Links = nil
	for _, relation := range before.Links {
		if relation.From == endpoint {
			wantBead.Links = append(wantBead.Links, relation)
		}
	}
	newRelation := workflowplugin.RelationView{From: edge.From, To: model.RefByID(edge.To, model.Natural64(0)), Kind: edge.Kind}
	if edge.From == endpoint {
		wantBead.Links = append(wantBead.Links, newRelation)
	}
	for _, relation := range before.Links {
		if relation.From != endpoint {
			wantBead.Links = append(wantBead.Links, relation)
		}
	}
	if edge.From != endpoint {
		wantBead.Links = append(wantBead.Links, newRelation)
	}
	if !reflect.DeepEqual(after, wantBead) {
		f.t.Fatalf("direct active exact public readback %+v want %+v", after, wantBead)
	}
	stored, err := f.store.(driver.CheckpointStore).LoadCheckpoint(context.Background(), b06WorkflowNamespace)
	if err != nil || stored.Generation != cp.Generation+1 || bytes.Equal(cp.Payload, stored.Payload) {
		f.t.Fatalf("direct active checkpoint readback %+v err%v", stored, err)
	}
	if !bytes.Equal(core, f.image(fmt.Sprintf("direct-core-after-%d", f.requests))) {
		f.t.Fatal("direct active relation created core history/version/receipt")
	}
	b06Retain(f.t, fmt.Sprintf("direct-active-commit-%d.json", f.requests), map[string]any{"edge": edge, "commit": commit, "before": before, "after": after, "checkpoint": stored})
}

func b06MixedTxRefuse(f *b06PlugFixture, ops []model.Op, code string) {
	f.t.Helper()
	f.writes["tx.apply/"+fmt.Sprint(f.requests+1)] = "tx.apply"
	before := b06MixedImage(f, fmt.Sprintf("before-ordered-refusal-%d", f.requests+1))
	got := f.call("tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: ops}}, true)
	if got.Error == nil || string(got.Error.Data.Code) != code || len(got.Result) != 0 {
		f.t.Fatalf("ordered refusal %+v want %s", got, code)
	}
	if !bytes.Equal(before, b06MixedImage(f, fmt.Sprintf("after-ordered-refusal-%d", f.requests))) {
		f.t.Fatal("ordered refusal published preceding patch, edges, checkpoint, history or receipts")
	}
}

func b06MixedReplay(f *b06PlugFixture, method string, params map[string]any, key int) b06Envelope {
	f.t.Helper()
	f.requests++
	params["_meta"] = map[string]any{"project": "b06", "agent": "b06-recorder", "idempotencyKey": fmt.Sprintf("%032x", key)}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": f.requests, "method": method, "params": params})
	if err != nil {
		f.t.Fatal(err)
	}
	raw := f.server.Dispatch(rpc.WithPrincipal(context.Background(), f.by), request)
	b06Retain(f.t, fmt.Sprintf("rpc-%03d-replay.json", f.requests), map[string]any{"request": json.RawMessage(request), "response": json.RawMessage(raw), "original_key_index": key})
	var got b06Envelope
	if err := json.Unmarshal(raw, &got); err != nil {
		f.t.Fatal(err)
	}
	return got
}

func b06MixedOrdered(f *b06PlugFixture) {
	f.t.Helper()
	for _, id := range []string{"b06-tx-a", "b06-tx-b", "b06-tx-child"} {
		f.create(id)
	}
	ids := b06InvokeMixed(f, "tx-first", "tx-parent")
	patch := func(title string) model.Op {
		return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: model.RefByID("b06-tx-a", model.Natural64(0)), Fields: map[string]any{"title": title}}}
	}
	link := func(from, to, kind string) model.Op {
		return model.Op{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: model.RefByID(from, model.Natural64(0)), To: model.RefByID(to, model.Natural64(0)), LinkKind: kind}}
	}
	// These checks must observe each earlier operation's detached image. The
	// preceding durable patch is a visible rollback witness for either plane.
	b06MixedTxRefuse(f, []model.Op{patch("rollback-cycle"), link("b06-tx-a", ids[0], "blocks"), link(ids[0], "b06-tx-b", "blocks"), link("b06-tx-b", "b06-tx-a", "blocks")}, "ontology_guard_failed")
	b06MixedTxRefuse(f, []model.Op{patch("rollback-parent"), link("b06-tx-child", ids[1], "parent-child"), link(ids[1], "b06-tx-a", "parent-child"), link("b06-tx-child", "b06-tx-a", "blocks")}, "ontology_guard_failed")
	b06MixedTxRefuse(f, []model.Op{patch("rollback-self"), link(ids[0], ids[0], "tracks")}, "invalid_params")
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	head, err := meta.Head.Uint64()
	if err != nil {
		f.t.Fatal(err)
	}
	ops := []model.Op{patch("ordered-kept"), link("b06-tx-a", ids[0], "blocks"), link(ids[0], "b06-tx-b", "tracks")}
	params := func() map[string]any { return map[string]any{"changeSet": model.ChangeSet{Ops: ops}} }
	commit := b06RequireSuccess[model.Commit](f.t, f.call("tx.apply", params(), true))
	key := f.requests
	if commit.Revision != model.Natural64(head+1) || len(commit.Results) != 3 || len(commit.Warnings) != 0 {
		f.t.Fatalf("ordered transaction revision/result count %+v", commit)
	}
	for i, expected := range []struct {
		id       string
		revision model.Revision
	}{{"b06-tx-a", commit.Revision}, {ids[0], model.Natural64(1)}, {ids[0], model.Natural64(2)}} {
		if commit.Results[i].Ref != model.RefByID(expected.id, model.Natural64(0)) || commit.Results[i].Revision != expected.revision {
			f.t.Fatalf("ordered result[%d] %+v want %+v", i, commit.Results[i], expected)
		}
	}
	check := func() {
		ordinary := b06MixedGet(f, "b06-tx-a")
		if ordinary.Revision != commit.Revision || ordinary.Fields["title"] != "ordered-kept" || len(ordinary.Links) != 1 || ordinary.Links[0].From != "b06-tx-a" || ordinary.Links[0].To.ID != ids[0] || ordinary.Links[0].Kind != "blocks" {
			f.t.Fatalf("ordered durable readback %+v", ordinary)
		}
		active := b06MixedGet(f, ids[0])
		if active.Revision != model.Natural64(2) || active.Plane != "ephemeral" || active.Kind != workflowplugin.ItemKind || active.Fields["title"] != "tx-first" || len(active.Links) != 2 {
			f.t.Fatalf("ordered active readback %+v", active)
		}
		b06RequireEdges(f.t, []b06Edge{{active.Links[0].From, active.Links[0].To.ID, active.Links[0].Kind}, {active.Links[1].From, active.Links[1].To.ID, active.Links[1].Kind}}, []b06Edge{{"b06-tx-a", ids[0], "blocks"}, {ids[0], "b06-tx-b", "tracks"}})
		current, err := f.store.Meta(context.Background())
		if err != nil {
			f.t.Fatal(err)
		}
		if current.Head != commit.Revision {
			f.t.Fatal("ordered transaction produced extra core revisions")
		}
		before := b06MixedImage(f, fmt.Sprintf("before-tx-replay-%d", f.requests))
		replay := b06RequireSuccess[model.Commit](f.t, b06MixedReplay(f, "tx.apply", params(), key))
		if !reflect.DeepEqual(commit, replay) {
			f.t.Fatalf("ordered replay changed exact committed result %+v %+v", commit, replay)
		}
		if !bytes.Equal(before, b06MixedImage(f, fmt.Sprintf("after-tx-replay-%d", f.requests))) {
			f.t.Fatal("ordered replay changed either plane/history/receipts")
		}
		for _, id := range ids {
			got := f.call("history.get", map[string]any{"ref": model.RefByID(id, model.Natural64(0)), "asOf": map[string]any{"revision": commit.Revision, "incarnation": current.Incarnation}}, false)
			if got.Error == nil || got.Error.Data.Code != "not_found" || len(got.Result) != 0 {
				f.t.Fatalf("active item acquired permanent history %+v", got)
			}
		}
		version := b06RequireSuccess[service.HistoryVersion](f.t, f.call("history.get", map[string]any{"ref": model.RefByID("b06-tx-a", model.Natural64(0)), "asOf": map[string]any{"revision": commit.Revision, "incarnation": current.Incarnation}}, false))
		if version.Ref.ID != "b06-tx-a" || version.Revision != commit.Revision || version.Deleted || len(version.Patch) == 0 {
			f.t.Fatalf("ordinary ordered history missing %+v", version)
		}
	}
	check()
	before := b06MixedImage(f, "ordered-before-reopen")
	f.reopen()
	b06InstallMixed(f)
	if !bytes.Equal(before, b06MixedImage(f, "ordered-after-reopen")) {
		f.t.Fatal("ordered committed state changed after reopen")
	}
	check()
}

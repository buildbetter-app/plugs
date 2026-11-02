package s1relations_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func patch(target model.Ref, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: target, Fields: fields}}
}

func link(from, to model.Ref, kind string) model.Op {
	return model.Op{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: from, To: to, LinkKind: kind}}
}

func (f *fixture) batch(label string, ops ...model.Op) model.Commit {
	f.t.Helper()
	return f.write("tx.apply", label, map[string]any{"changeSet": model.ChangeSet{Ops: ops}})
}

func seedRelations(t *testing.T, f *fixture) (model.Ref, model.Commit, pipeline.Document) {
	t.Helper()
	parent := f.write("bead.create", "parent", map[string]any{"type": "issue", "fields": map[string]any{"title": "parent"}}).Results[0].Ref
	target := f.write("bead.create", "target", map[string]any{"type": "issue", "fields": map[string]any{"title": "target"}}).Results[0].Ref
	child := f.write("bead.create", "child", map[string]any{"type": "issue", "parent": parent,
		"fields": map[string]any{"title": "before", "priority": 2, "description": "before description", "labels": []string{"alpha", "beta"}}}).Results[0].Ref
	f.batch("relations", link(child, target, "relates-to"),
		model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Kind: model.OpComment, Target: child, Body: "retained ordinary comment"}},
		model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Kind: model.OpSetMeta, Target: child, Key: "plugin.note", Value: json.RawMessage(`{"nested":{"answer":42}}`)}},
	)
	closed := f.write("bead.close", "close", map[string]any{"id": child, "reason": "retained close provenance"})
	doc := f.document(child.ID)
	if doc.Fields["status"] != "done" || len(doc.Comments) != 2 || doc.Comments[1].Body != "retained close provenance" || doc.Comments[1].Principal.ID != actor.ID || doc.Parent == nil || doc.Parent.ID != parent.ID || len(doc.Links) != 1 || doc.Links[0].To.ID != target.ID {
		t.Fatalf("relation setup did not exercise the declared surfaces: %+v", doc)
	}
	return child, closed, doc
}

func sameRelations(t *testing.T, before, after pipeline.Document, labels any) {
	t.Helper()
	equalJSON(t, labels, after.Fields["labels"])
	equalJSON(t, before.Parent, after.Parent)
	equalJSON(t, before.Metadata, after.Metadata)
	equalJSON(t, before.Comments, after.Comments)
	equalJSON(t, before.Links, after.Links)
	equalJSON(t, before.Fields["status"], after.Fields["status"])
}

func (f *fixture) historical(target model.Ref, at model.Revision) pipeline.Document {
	f.t.Helper()
	got := f.call("history.get", "history-"+target.ID+"-"+at.String(), map[string]any{"ref": target, "asOf": map[string]any{"revision": at}})
	if got.Error != nil {
		f.t.Fatalf("public historical read refused: %s", got.Error.Data.Code)
	}
	var value struct {
		Body json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(got.Result, &value); err != nil {
		f.t.Fatal(err)
	}
	doc, err := pipeline.DecodeDocument(value.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	return doc
}

func (f *fixture) changedPaths(target model.Ref, from, to model.Revision) []string {
	f.t.Helper()
	got := f.call("history.diff", "diff-"+from.String()+"-"+to.String(), map[string]any{"ref": target, "fromRevision": from, "toRevision": to})
	if got.Error != nil {
		f.t.Fatalf("public history diff refused: %s", got.Error.Data.Code)
	}
	var value struct {
		ChangedPaths []string `json:"changedPaths"`
	}
	if err := json.Unmarshal(got.Result, &value); err != nil {
		f.t.Fatal(err)
	}
	return value.ChangedPaths
}

func TestB25ScalarPreservationAndExplicitLabelReplacementHaveDistinctPublicHistory(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		f := newFixture(t, kind, []string{"s1-parent", "s1-target", "s1-child"})
		child, closed, before := seedRelations(t, f)
		params := map[string]any{"id": child, "fields": map[string]any{"title": "after", "priority": 0, "description": "after description"}}
		updated := f.write("bead.update", "scalar", params)
		sameRelations(t, before, f.document(child.ID), []string{"alpha", "beta"})
		equalJSON(t, []string{"/fields/description", "/fields/priority", "/fields/title"}, f.changedPaths(child, closed.Revision, updated.Revision))
		sameRelations(t, before, f.historical(child, closed.Revision), []string{"alpha", "beta"})
		sameRelations(t, before, f.historical(child, updated.Revision), []string{"alpha", "beta"})
		replaced := f.write("bead.update", "replace-labels", map[string]any{"id": child, "fields": map[string]any{"labels": []string{"gamma"}}})
		sameRelations(t, before, f.document(child.ID), []string{"gamma"})
		equalJSON(t, []string{"/fields/labels"}, f.changedPaths(child, updated.Revision, replaced.Revision))
		for _, version := range []struct {
			at     model.Revision
			labels []string
		}{{closed.Revision, []string{"alpha", "beta"}}, {updated.Revision, []string{"alpha", "beta"}}, {replaced.Revision, []string{"gamma"}}} {
			sameRelations(t, before, f.historical(child, version.at), version.labels)
		}
		image := f.image()
		f.reopen()
		if !bytes.Equal(image, f.image()) {
			t.Fatal("reopen changed durable relation/version/event state")
		}
		// Replaying the earlier scalar request must return its stored commit
		// without restoring its older label image over the later replacement.
		replay := f.call("bead.update", "scalar", params)
		if replay.Error != nil || !replay.Meta.Replayed {
			t.Fatalf("old scalar request did not replay: %+v", replay)
		}
		equalJSON(t, json.RawMessage(jsonBytes(t, updated)), replay.Result)
		sameRelations(t, before, f.document(child.ID), []string{"gamma"})
		if !bytes.Equal(image, f.image()) {
			t.Fatal("old scalar replay changed the current relation/history image")
		}
		// Equal explicit replacement is a no-op with its own final receipt.
		noop := f.write("bead.update", "equal-labels", map[string]any{"id": child, "fields": map[string]any{"labels": []string{"gamma"}}})
		if noop.Revision != replaced.Revision || !bytes.Equal(image, f.image()) {
			t.Fatal("equal label replacement created a graph version or event")
		}
		receipt := f.receipt("bead.update", "equal-labels")
		if receipt == nil || receipt.ResponsePending || receipt.Changeset.Kind != "" {
			t.Fatalf("equal replacement lacked a final no-op receipt: %+v", receipt)
		}
		f.reopen()
		equalJSON(t, receipt, f.receipt("bead.update", "equal-labels"))
		sameRelations(t, before, f.historical(child, closed.Revision), []string{"alpha", "beta"})
		sameRelations(t, before, f.document(child.ID), []string{"gamma"})
	})
}

func TestB25AuxiliaryRefusalPreservesLabelsCloseProvenanceAndSameKeyRetry(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		f := newFixture(t, kind, []string{"s1-parent", "s1-target", "s1-child"})
		child, _, before := seedRelations(t, f)
		image := f.image()
		bad := model.ChangeSet{Ops: []model.Op{
			patch(child, map[string]any{"title": "must not land", "labels": []string{"must-not-land"}}),
			link(child, child, "blocks"),
		}}
		got := f.call("tx.apply", "auxiliary", map[string]any{"changeSet": bad})
		if got.Error == nil || got.Error.Data.Code != "ontology_guard_failed" || len(got.Result) != 0 {
			t.Fatalf("auxiliary refusal was not a typed result-free error: %+v", got)
		}
		if !bytes.Equal(image, f.image()) || f.receipt("tx.apply", "auxiliary") != nil {
			t.Fatal("auxiliary refusal changed data/version/event/head or published a receipt")
		}
		f.reopen()
		if !bytes.Equal(image, f.image()) {
			t.Fatal("auxiliary refusal became durable after reopen")
		}
		retryOps := []model.Op{patch(child, map[string]any{"title": "retry landed", "labels": []string{"gamma"}}),
			{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Kind: model.OpSetMeta, Target: child, Key: "plugin.retry", Value: json.RawMessage(`true`)}}}
		retried := f.batch("auxiliary", retryOps...)
		if retried.Revision.String() != "6" || len(retried.Results) != 2 {
			t.Fatalf("same-key retry did not publish one atomic revision: %+v", retried)
		}
		after := f.document(child.ID)
		equalJSON(t, before.Comments, after.Comments)
		equalJSON(t, before.Parent, after.Parent)
		equalJSON(t, before.Links, after.Links)
		equalJSON(t, []string{"gamma"}, after.Fields["labels"])
		if after.Fields["title"] != "retry landed" || string(after.Metadata["plugin.retry"]) != "true" || after.Fields["status"] != "done" {
			t.Fatalf("same-key retry final image is incomplete: %+v", after)
		}
		committed := f.image()
		f.reopen()
		replay := f.call("tx.apply", "auxiliary", map[string]any{"changeSet": model.ChangeSet{Ops: retryOps}})
		if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(committed, f.image()) {
			t.Fatal("same-key successful retry did not remain a callback-free stored replay")
		}
		equalJSON(t, json.RawMessage(jsonBytes(t, retried)), replay.Result)
	})
}

func TestB25DirectDeleteRemovesOwnedRelationsAndRetainsHistoryAcrossAllStores(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		f := newFixture(t, kind, []string{"s1-parent", "s1-target", "s1-child"})
		child, _, _ := seedRelations(t, f)
		parent, target := f.document("s1-parent"), f.document("s1-target")
		prefix, err := pipeline.AncestorPrefix(child.ID)
		if err != nil {
			t.Fatal(err)
		}
		r := f.reader()
		beforeClosure, err := r.DerivedRows(f.ctx, driver.DerivedAncestorClosure, prefix)
		_ = r.Close()
		if err != nil || len(beforeClosure) != 2 {
			t.Fatalf("delete setup did not include child self/parent references: %+v err=%v", beforeClosure, err)
		}
		// Delete with labels, metadata, comments, parent and an outbound
		// association still present. No preliminary unlink or field cleanup.
		ops := []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: child}}}
		deleted := f.batch("delete", ops...)
		if deleted.Revision.String() != "6" || len(deleted.Results) != 1 || len(deleted.Results[0].Record) != 0 {
			t.Fatalf("direct delete did not return its tombstone: %+v", deleted)
		}
		got := f.call("bead.get", "deleted-get", map[string]any{"id": child})
		if got.Error == nil || got.Error.Data.Code != "not_found" || len(got.Result) != 0 {
			t.Fatalf("deleted relation owner remained publicly readable: %+v", got)
		}
		r = f.reader()
		rows, err := r.Get(f.ctx, []model.Ref{child})
		if err != nil || len(rows) != 0 {
			t.Fatalf("delete retained current owner bytes: %+v err=%v", rows, err)
		}
		closure, err := r.DerivedRows(f.ctx, driver.DerivedAncestorClosure, prefix)
		if err != nil || len(closure) != 0 {
			t.Fatalf("delete retained owned reference index rows: %+v err=%v", closure, err)
		}
		versions, err := r.Versions(f.ctx, child, driver.PageReq{})
		_ = r.Close()
		if err != nil || versions.NextCursor != nil || len(versions.Items) != 4 {
			t.Fatalf("delete discarded prior relation history: %+v err=%v", versions, err)
		}
		var tombstones int
		for _, version := range versions.Items {
			if version.Deleted {
				tombstones++
				if version.Revision != deleted.Revision {
					t.Fatalf("wrong tombstone revision: %+v", version)
				}
			}
		}
		if tombstones != 1 {
			t.Fatalf("delete history has %d tombstones", tombstones)
		}
		equalJSON(t, parent, f.document("s1-parent"))
		equalJSON(t, target, f.document("s1-target"))
		image := f.image()
		f.reopen()
		replay := f.call("tx.apply", "delete", map[string]any{"changeSet": model.ChangeSet{Ops: ops}})
		if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(image, f.image()) {
			t.Fatal("delete replay after reopen recreated state or removed more history")
		}
		equalJSON(t, json.RawMessage(jsonBytes(t, deleted)), replay.Result)
		missing := f.call("tx.apply", "missing-delete", map[string]any{"changeSet": model.ChangeSet{Ops: ops}})
		if missing.Error == nil || missing.Error.Data.Code != "not_found" || len(missing.Result) != 0 || f.receipt("tx.apply", "missing-delete") != nil || !bytes.Equal(image, f.image()) {
			t.Fatalf("fresh delete of missing owner was not a state-preserving typed refusal: %+v", missing)
		}
	})
}

package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func link(from, to model.Ref, kind string) model.Op {
	return model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: kind}}
}

func TestConditionalLinksAreAtomicGuardedAndDoNotWriteEndpoints(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create(""), create("")}}, meta(1))
	dependent, blocker := seed.Results[0].Ref, seed.Results[1].Ref
	conditional := link(dependent, blocker, "conditional-blocks")
	linked := apply(t, w, model.ChangeSet{Ops: []model.Op{conditional}}, meta(2))
	doc, err := DecodeDocument(linked.Results[0].Record)
	if err != nil || len(doc.Links) != 1 || doc.Links[0].Kind != "conditional-blocks" || doc.Links[0].To.ID != blocker.ID {
		t.Fatalf("dependent links=%+v err=%v", doc.Links, err)
	}
	versions, err := h.read(t).Versions(context.Background(), blocker, driver.PageReq{})
	if err != nil || len(versions.Items) != 1 {
		t.Fatalf("read endpoint gained a version: %+v err=%v", versions, err)
	}
	for i, kind := range []string{"blocks", "conditional-blocks"} {
		_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(blocker, map[string]any{"title": "must roll back"}), link(blocker, dependent, kind)}}, meta(i+3))
		code(t, err, "ontology_guard_failed")
	}
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{link(dependent, blocker, "invented")}}, meta(5))
	code(t, err, "invalid_params")
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: blocker}}}}, meta(6))
	code(t, err, "ontology_guard_failed")
	// A duplicate edge and an absent unlink have the same guarded no-op semantics
	// as other unchanged images; they create no versions or changesets.
	duplicate := apply(t, w, model.ChangeSet{Ops: []model.Op{conditional}}, meta(7))
	if duplicate.Revision.String() != "2" {
		t.Fatalf("duplicate edge minted revision: %+v", duplicate)
	}
	conditional.Kind = model.OpUnlink
	unlinked := apply(t, w, model.ChangeSet{Ops: []model.Op{conditional}}, meta(8))
	if unlinked.Revision.String() != "3" {
		t.Fatalf("unlink=%+v", unlinked)
	}
	absent := apply(t, w, model.ChangeSet{Ops: []model.Op{conditional}}, meta(9))
	if absent.Revision.String() != "3" {
		t.Fatalf("absent unlink minted revision: %+v", absent)
	}
	records, err := h.read(t).Get(context.Background(), []model.Ref{blocker})
	if err != nil || len(records) != 1 {
		t.Fatal(records, err)
	}
	unchanged, err := DecodeDocument(records[0].Body)
	if err != nil || unchanged.Fields["title"] != "initial" {
		t.Fatalf("cycle refusal published partial field: %+v err=%v", unchanged, err)
	}
	deleted := apply(t, w, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: blocker}}}}, meta(10))
	if deleted.Revision.String() != "4" || len(deleted.Results[0].Record) != 0 {
		t.Fatalf("delete=%+v", deleted)
	}
	records, err = h.read(t).Get(context.Background(), []model.Ref{blocker})
	if err != nil || len(records) != 0 {
		t.Fatalf("delete did not remove record: %+v err=%v", records, err)
	}
	log, err := h.read(t).Changesets(context.Background(), nil, 10)
	if err != nil || len(log) != 4 || log[3].Changes[0].Op != model.OpDelete {
		t.Fatalf("log=%+v err=%v", log, err)
	}
}

func TestCommentsMetadataAndNullFieldsPreserveAtomicImages(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	ref := seed.Results[0].Ref
	body := "\n**exact comment body**\n"
	comment := model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Target: ref, Body: body}}
	metadata := model.Op{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: "plugin.literal.dot/~key", Value: json.RawMessage(`{"answer":42}`)}}
	cs := model.ChangeSet{Ops: []model.Op{comment, metadata, patch(ref, map[string]any{"title": nil})}}
	result := apply(t, w, cs, meta(2))
	doc, err := DecodeDocument(result.Results[0].Record)
	if err != nil || len(doc.Comments) != 1 || doc.Comments[0].Body != body || !reflect.DeepEqual(doc.Comments[0].Principal, by) || !doc.Comments[0].Time.Equal(h.now) {
		t.Fatalf("comment=%+v err=%v", doc.Comments, err)
	}
	if _, exists := doc.Fields["title"]; exists || doc.ID != ref.ID || doc.Type != "issue" || doc.Project != "test" || doc.Fields["status"] != "todo" {
		t.Fatalf("field clearing changed anchors: %+v", doc)
	}
	for _, opResult := range result.Results {
		if !reflect.DeepEqual(opResult.Record, result.Results[0].Record) {
			t.Fatal("batch results did not share final image")
		}
	}
	log, err := h.read(t).Changesets(context.Background(), nil, 10)
	want := []string{"/comments", "/fields/title", "/metadata/plugin.literal.dot~1~0key"}
	if err != nil || len(log) != 2 || len(log[1].Changes) != 1 || !reflect.DeepEqual(log[1].Changes[0].ChangedPaths, want) {
		t.Fatalf("JSON Pointer paths=%+v want=%v err=%v", log, want, err)
	}
	next := apply(t, w, model.ChangeSet{Ops: []model.Op{comment}}, meta(3))
	doc, err = DecodeDocument(next.Results[0].Record)
	if err != nil || !doc.Comments[1].Time.Equal(h.now.Add(time.Second)) {
		t.Fatalf("same-clock thread stamps=%+v err=%v", doc.Comments, err)
	}
	metadata.SetMeta.Value = json.RawMessage(`null`)
	cleared := apply(t, w, model.ChangeSet{Ops: []model.Op{metadata}}, meta(4))
	doc, err = DecodeDocument(cleared.Results[0].Record)
	if err != nil || len(doc.Metadata) != 0 {
		t.Fatalf("metadata clear=%+v err=%v", doc.Metadata, err)
	}
}

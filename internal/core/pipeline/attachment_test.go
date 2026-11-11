package pipeline

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestAttachmentPublishesEventWithoutOptionalOntologyAudit(t *testing.T) {
	h := setup(t)
	w := h.writer()
	created := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	ref := created.Results[0].Ref
	attachment := model.Attachment{Kind: "commit", URI: "git:0123456789abcdef", MediaType: "text/plain", Summary: "commit proof"}
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpAttach, Attach: &model.AttachOp{Target: ref, Attachment: attachment}}}}
	commit := apply(t, w, cs, meta(2))
	entries, err := h.read(t).Changesets(context.Background(), nil, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("attachment history=%+v err=%v", entries, err)
	}
	found := false
	for _, entry := range entries {
		if entry.Revision != commit.Revision {
			continue
		}
		found = true
		if len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadAttached || len(entry.Changes) != 1 || entry.Changes[0].Op != model.OpAttach {
			t.Fatalf("attachment lost its durable event without optional audit: %+v", entry)
		}
	}
	if !found {
		t.Fatal("attachment revision missing from history")
	}
	var doc Document
	if err := json.Unmarshal(commit.Results[0].Record, &doc); err != nil || !reflect.DeepEqual(doc.Attachments, []model.Attachment{attachment}) {
		t.Fatalf("attachment postimage=%+v err=%v", doc, err)
	}
}

func TestAttachmentResolvesEarlierCreateAliasWithoutMutatingRequest(t *testing.T) {
	h := setup(t)
	w := h.writer()
	attachment := model.Attachment{Kind: "link", URI: "https://example.invalid/proof", MediaType: "text/plain", Summary: "alias proof"}
	cs := model.ChangeSet{Ops: []model.Op{create("subject"), {Kind: model.OpAttach, Attach: &model.AttachOp{Target: model.RefByKey("subject"), Attachment: attachment}}}}
	before, err := json.Marshal(cs)
	if err != nil {
		t.Fatal(err)
	}
	commit := apply(t, w, cs, meta(1))
	after, err := json.Marshal(cs)
	if err != nil || string(before) != string(after) || len(commit.Results) != 2 || commit.Results[0].Ref != commit.Results[1].Ref {
		t.Fatalf("attachment changed caller alias or resolved a different target: %+v err=%v", commit, err)
	}
	replay := apply(t, w, cs, meta(1))
	if !reflect.DeepEqual(commit, replay) {
		t.Fatal("resolved alias cannot replay its original attachment receipt")
	}
	var doc Document
	if err := json.Unmarshal(commit.Results[1].Record, &doc); err != nil || !reflect.DeepEqual(doc.Attachments, []model.Attachment{attachment}) {
		t.Fatalf("alias attachment postimage=%+v err=%v", doc, err)
	}
}

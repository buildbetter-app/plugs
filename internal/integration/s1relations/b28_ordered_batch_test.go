package s1relations_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type b28BatchPost func(context.Context, port.CommittedChange, port.SnapshotReader) error

func (fn b28BatchPost) After(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
	return fn(ctx, change, reader)
}

type b28BatchDelivery struct {
	Change    port.CommittedChange
	At        string
	Rows      []driver.Record
	ReadError string
}

func b28BatchDocument(id, title, description string) pipeline.Document {
	return pipeline.Document{
		ID: id, Type: "issue", Project: "s1", CreatedAt: &fixedTime,
		// The frozen ontology schema supplies these fixed kernel defaults.
		Fields: map[string]any{
			"title": title, "description": description, "status": "todo",
			"priority": 2, "template": false,
		},
	}
}

// Ref permits an omitted branch on the ID arm; the typed zero value is branch 0.
// Assert the exact identity without requiring one optional zero-branch spelling.
func b28BatchIDRef(t *testing.T, wantID string, got model.Ref) {
	t.Helper()
	if err := got.Validate(); err != nil {
		t.Fatalf("invalid committed reference: %v", err)
	}
	if !got.IsID() || got.ID != wantID || got.Key != "" || got.Branch.String() != "0" {
		t.Fatalf("reference=%+v want ID arm id=%q key=empty branch=0", got, wantID)
	}
}

func b28BatchRows(t *testing.T, rows []driver.Record, want []pipeline.Document, revisions []string) {
	t.Helper()
	if len(rows) != len(want) || len(revisions) != len(want) {
		t.Fatalf("committed rows=%d want=%d revisions=%v", len(rows), len(want), revisions)
	}
	byID := make(map[string]driver.Record, len(rows))
	for _, row := range rows {
		if _, duplicate := byID[row.Ref.ID]; duplicate {
			t.Fatalf("duplicate committed row %s", row.Ref.ID)
		}
		byID[row.Ref.ID] = row
	}
	for i, doc := range want {
		row, ok := byID[doc.ID]
		if !ok || row.Revision.String() != revisions[i] {
			t.Fatalf("committed row %s: %+v want revision=%s", doc.ID, row, revisions[i])
		}
		b28BatchIDRef(t, doc.ID, row.Ref)
		equalJSON(t, doc, json.RawMessage(row.Body))
	}
}

// These literal vectors refine the reviewer's retained pre-source B28 whole-
// changeset expectations. Expected images are never learned from a callback,
// Commit result or current read. Distinct resources avoid imposing an unstated
// intermediate-image contract on repeated operations against the same bead.
func TestB28PublicHeterogeneousBatchPreservesWholeChangeOrderAndSnapshots(t *testing.T) {
	stores(t, func(t *testing.T, kind string) {
		before := []pipeline.Document{
			b28BatchDocument("b28-batch-z-patch", "patch before", "patch description α\nretained line"),
			b28BatchDocument("b28-batch-b-comment", "comment target", "comment description β"),
			b28BatchDocument("b28-batch-m-source", "link source", "link description γ"),
			b28BatchDocument("b28-batch-a-delete", "delete target", "delete description δ"),
			b28BatchDocument("b28-batch-n-target", "link target", "target description ε"),
		}
		ids, refs := make([]string, len(before)), make([]model.Ref, len(before))
		seedOps := make([]model.Op, len(before))
		for i, doc := range before {
			ids[i], refs[i] = doc.ID, ref(doc.ID)
			seedOps[i] = model.Op{Kind: model.OpCreate, Create: &model.CreateOp{
				// Keep the original sparse request; the expected image includes defaults.
				Kind: model.OpCreate, Type: "issue", Fields: map[string]any{
					"title": doc.Fields["title"], "description": doc.Fields["description"], "status": doc.Fields["status"],
				},
			}}
		}
		f := newFixture(t, kind, ids)
		seed := f.write("tx.apply", "b28-batch-seed", map[string]any{"changeSet": model.ChangeSet{Ops: seedOps}})
		if seed.Revision.String() != "1" || len(seed.Results) != 5 || len(seed.Warnings) != 0 {
			t.Fatalf("seed commit=%+v", seed)
		}
		for i, result := range seed.Results {
			b28BatchIDRef(t, refs[i].ID, result.Ref)
			if result.Revision.String() != "1" {
				t.Fatalf("seed row revision=%s", result.Revision.String())
			}
			equalJSON(t, before[i], result.Record)
		}

		patched := b28BatchDocument(ids[0], "patch after", "replacement description ζ\nexact second line")
		commented := before[1]
		commented.Comments = []pipeline.Comment{{Body: "whole-batch comment η\nexact second line", Principal: actor, Time: fixedTime}}
		linked := before[2]
		linked.Links = []pipeline.Link{{To: refs[4], Kind: "relates-to"}}
		after := []*pipeline.Document{&patched, &commented, &linked, nil}
		ops := []model.Op{
			{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: refs[0], Fields: map[string]any{
				"title": "patch after", "description": "replacement description ζ\nexact second line",
			}}},
			{Kind: model.OpComment, Comment: &model.CommentOp{Kind: model.OpComment, Target: refs[1], Body: "whole-batch comment η\nexact second line"}},
			{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: refs[2], To: refs[4], LinkKind: "relates-to"}},
			{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: refs[3]}},
		}
		params := map[string]any{"changeSet": model.ChangeSet{Ops: ops}}
		var mu sync.Mutex
		var deliveries []b28BatchDelivery
		observe := func() []b28BatchDelivery {
			mu.Lock()
			defer mu.Unlock()
			return append([]b28BatchDelivery(nil), deliveries...)
		}
		f.options = append(f.options, pipeline.WithPostSubscribers(pipeline.PostBinding{
			ID: "b28/whole-batch", Subscriber: b28BatchPost(func(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
				observation := b28BatchDelivery{Change: change, At: "missing"}
				if at := reader.Snapshot().At; at != nil {
					observation.At = at.String()
				}
				rows, err := reader.Get(ctx, refs)
				observation.Rows = rows
				if err != nil {
					observation.ReadError = err.Error()
				}
				mu.Lock()
				deliveries = append(deliveries, observation)
				mu.Unlock()
				return err
			}),
		}))
		f.bind()
		response := f.call("tx.apply", "b28-whole-batch", params)
		if response.Error != nil || response.Meta.Replayed {
			t.Fatalf("fresh whole-batch response=%s", f.frames[len(f.frames)-1].Response)
		}
		var commit model.Commit
		if err := json.Unmarshal(response.Result, &commit); err != nil {
			t.Fatal(err)
		}
		observed := observe()
		if commit.Revision.String() != "2" || len(commit.Results) != 4 || len(commit.Warnings) != 0 || len(observed) != 1 {
			t.Fatalf("whole-batch commit=%+v deliveries=%d", commit, len(observed))
		}
		batch := observed[0]
		if batch.At != "2" || batch.ReadError != "" || batch.Change.Revision.String() != "2" || len(batch.Change.Changes) != 4 {
			t.Fatalf("whole-batch delivery=%+v", batch)
		}
		equalJSON(t, actor, batch.Change.Principal)
		if batch.Change.Meta.Method != "tx.apply" || batch.Change.Meta.Project != "s1" || batch.Change.Meta.IdempotencyKey != requestKey("b28-whole-batch") {
			t.Fatalf("whole-batch delivery metadata=%+v", batch.Change.Meta)
		}
		for i, change := range batch.Change.Changes {
			if change.Resource.Kind != "bead" {
				t.Fatalf("operation %d resource kind=%q want bead", i, change.Resource.Kind)
			}
			b28BatchIDRef(t, refs[i].ID, change.Resource.Ref)
			equalJSON(t, ops[i], change.Op)
			equalJSON(t, before[i], change.Before)
			result := commit.Results[i]
			b28BatchIDRef(t, refs[i].ID, result.Ref)
			if result.Revision.String() != "2" {
				t.Fatalf("operation %d result revision=%s", i, result.Revision.String())
			}
			if after[i] == nil {
				if len(result.Record) != 0 || len(change.After) != 0 && string(change.After) != "null" {
					t.Fatalf("delete retained result/after image: %+v %+v", result, change)
				}
			} else {
				equalJSON(t, after[i], change.After)
				equalJSON(t, after[i], result.Record)
			}
		}
		live := []pipeline.Document{patched, commented, linked, before[4]}
		b28BatchRows(t, batch.Rows, live, []string{"2", "2", "2", "1"})
		receipt := f.receipt("tx.apply", "b28-whole-batch")
		if receipt == nil || receipt.ResponsePending {
			t.Fatal("whole-batch response was not finalized durably")
		}
		equalJSON(t, commit, receipt.Commit)

		assertCurrent := func(label string, wants []pipeline.Document, revisions []string, head string) {
			t.Helper()
			r := f.reader()
			rows, err := r.Get(f.ctx, refs)
			at := r.Snapshot().At
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if err != nil || at == nil || at.String() != head {
				t.Fatalf("current head=%v want=%s read error=%v", at, head, err)
			}
			b28BatchRows(t, rows, wants, revisions)
			for i, doc := range wants {
				got := f.call("bead.get", label+"-"+doc.ID, map[string]any{"id": ref(doc.ID)})
				if got.Error != nil {
					t.Fatalf("public current %s refused: %s", doc.ID, f.frames[len(f.frames)-1].Response)
				}
				var current struct {
					Bead struct {
						ID, Type, Project, Status string
						Revision                  model.Revision
						Fields                    map[string]any
						Links                     []struct {
							From string    `json:"from"`
							To   model.Ref `json:"to"`
							Kind string    `json:"kind"`
						}
						CreatedAt *time.Time
					} `json:"bead"`
					Comments    []pipeline.Comment `json:"comments"`
					Attachments []model.Attachment `json:"attachments"`
				}
				if err := json.Unmarshal(got.Result, &current); err != nil {
					t.Fatal(err)
				}
				if current.Bead.ID != doc.ID || current.Bead.Type != "issue" || current.Bead.Project != "s1" || current.Bead.Status != "todo" || current.Bead.Revision.String() != revisions[i] || current.Bead.CreatedAt == nil || !current.Bead.CreatedAt.Equal(fixedTime) || len(current.Attachments) != 0 || len(current.Comments) != len(doc.Comments) || len(current.Bead.Links) != len(doc.Links) {
					t.Fatalf("public current tuple=%s", got.Result)
				}
				equalJSON(t, doc.Fields, current.Bead.Fields)
				for j, comment := range doc.Comments {
					equalJSON(t, comment, current.Comments[j])
				}
				for j, link := range doc.Links {
					if current.Bead.Links[j].From != doc.ID || current.Bead.Links[j].Kind != "relates-to" {
						t.Fatalf("public current dependency=%s", got.Result)
					}
					b28BatchIDRef(t, link.To.ID, current.Bead.Links[j].To)
				}
			}
			deleted := f.call("bead.get", label+"-deleted", map[string]any{"id": refs[3]})
			if deleted.Error == nil || deleted.Error.Data.Code != "not_found" || len(deleted.Result) != 0 {
				t.Fatalf("public deleted read=%s", f.frames[len(f.frames)-1].Response)
			}
		}
		assertCurrent("batch-current", live, []string{"2", "2", "2", "1"}, "2")
		image := f.image()
		noop := f.write("tx.apply", "b28-batch-noop", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{ops[0]}}})
		if noop.Revision.String() != "2" || len(noop.Results) != 1 || noop.Results[0].Revision.String() != "2" || len(noop.Warnings) != 0 || len(observe()) != 1 || !bytes.Equal(image, f.image()) {
			t.Fatalf("whole-batch same-value patch published/redelivered: %+v", noop)
		}
		b28BatchIDRef(t, refs[0].ID, noop.Results[0].Ref)
		equalJSON(t, patched, noop.Results[0].Record)

		later := f.write("bead.update", "b28-batch-later", map[string]any{"id": refs[4], "fields": map[string]any{"title": "target later value"}})
		if later.Revision.String() != "3" || len(later.Results) != 1 || later.Results[0].Revision.String() != "3" || len(later.Warnings) != 0 {
			t.Fatalf("later independent commit=%+v", later)
		}
		live[3] = b28BatchDocument(ids[4], "target later value", "target description ε")
		b28BatchIDRef(t, refs[4].ID, later.Results[0].Ref)
		equalJSON(t, live[3], later.Results[0].Record)
		assertCurrent("later-current", live, []string{"2", "2", "2", "3"}, "3")
		beforeReplay := f.image()
		f.reopen()
		assertCurrent("reopened-current", live, []string{"2", "2", "2", "3"}, "3")
		replay := f.call("tx.apply", "b28-whole-batch", params)
		if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(beforeReplay, f.image()) {
			t.Fatalf("old whole-batch replay changed current state: %s", f.frames[len(f.frames)-1].Response)
		}
		equalJSON(t, json.RawMessage(response.Result), replay.Result)
		equalReceipt(t, receipt, f.receipt("tx.apply", "b28-whole-batch"))
		assertCurrent("replayed-current", live, []string{"2", "2", "2", "3"}, "3")
		final := observe()
		if len(final) != 2 || final[0].Change.Revision.String() != "2" || final[1].Change.Revision.String() != "3" || final[1].At != "3" || final[1].ReadError != "" {
			t.Fatalf("whole-batch/later delivery cardinality=%+v", final)
		}
		b28BatchRows(t, final[1].Rows, live, []string{"2", "2", "2", "3"})
		if kind == "memory" {
			t.Log("B28_ORDERED_BATCH process-local memory rebind reached; no restart-persistence claim")
		} else {
			t.Logf("B28_ORDERED_BATCH physical SQL close/open reached: %s", kind)
		}
		if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
			artifact := struct {
				Before              []pipeline.Document
				After               []*pipeline.Document
				Deliveries          []b28BatchDelivery
				Commit, Noop, Later model.Commit
				Current             json.RawMessage
			}{before, after, final, commit, noop, later, f.image()}
			if err := os.WriteFile(filepath.Join(dir, "b28-ordered-batch-"+kind+".json"), jsonBytes(t, artifact), 0600); err != nil {
				t.Fatal(err)
			}
		}
	})
}

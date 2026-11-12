package cardb20

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

// The final failure follows real changes on both installed public planes and
// an in-request create/update/close. Native BatchCloser survivors are a separate
// contract: this tx.apply must return no Commit and publish none of its prefix.
func TestB21PublicFinalRefusalAfterMixedBatchPrefix(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB21(t, engine)
			seed := b21Create("anchor", "original durable")
			seed.Create.Fields["description"] = "original description"
			anchor := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{seed}})).Results[0].Ref
			invoked := f.call("x.workflow.invoke", f.key(), map[string]any{
				"project": b21Project, "mode": "ephemeral", "template": "checklist",
				"checklist": []map[string]any{{"label": "subject", "title": "original ephemeral", "type": "issue"}},
			})
			if invoked.Error != nil {
				t.Fatalf("actual public invocation: %+v", invoked.Error.Data)
			}
			var invocation struct {
				Items []struct {
					ID string `json:"id"`
				} `json:"items"`
			}
			b21Decode(t, invoked.Result, &invocation)
			if len(invocation.Items) != 1 || invocation.Items[0].ID == "" {
				t.Fatalf("actual public item: %s", invoked.Result)
			}
			ephemeral := model.RefByID(invocation.Items[0].ID, model.Natural64(0))
			initialDurable, initialEphemeral := f.publicRow(anchor), f.publicRow(ephemeral)
			if initialEphemeral["plane"] != "ephemeral" {
				t.Fatalf("public plane: %+v", initialEphemeral)
			}
			f.publicFields(anchor, map[string]any{"title": "original durable", "description": "original description", "status": "todo"})
			f.publicFields(ephemeral, map[string]any{"title": "original ephemeral"})

			createdClose := b21Close(model.RefByKey("created"))
			createdClose.Close.Reason = "created first reason"
			prefix := []model.Op{
				b21Patch(anchor, map[string]any{"description": "prefix durable edit"}),
				b21Patch(ephemeral, map[string]any{"title": "prefix ephemeral edit"}),
				b21MixedCreate("created", "prefix created"),
				b21Patch(model.RefByKey("created"), map[string]any{"title": "prefix updated"}),
				createdClose,
			}
			missingClose := b21Close(model.RefByID("b21-absent-final", model.Natural64(0)))
			missingClose.Close.Reason = "anchor first reason"
			refusedOps := append(append([]model.Op(nil), prefix...), missingClose)
			key := f.key()
			before := f.state()
			posts := f.posts.count()
			refused := f.apply(key, model.ChangeSet{Ops: refusedOps})
			b21Code(t, refused, "not_found")
			b21Equal(t, before, f.state(), "final refusal conserves durable records, versions and history")
			b21Equal(t, initialDurable, f.publicRow(anchor), "final refusal exact durable public row")
			b21Equal(t, initialEphemeral, f.publicRow(ephemeral), "final refusal exact ephemeral public row")
			if f.receipt(key) != nil || f.posts.count() != posts {
				t.Fatal("final refusal published an accepted receipt or post")
			}
			if engine != "memory" {
				f.reopen()
				b21Equal(t, before, f.state(), "refused prefix remains absent after SQL reopen")
				b21Equal(t, initialDurable, f.publicRow(anchor), "refused durable public row after SQL reopen")
				b21Equal(t, initialEphemeral, f.publicRow(ephemeral), "refused ephemeral public row after SQL reopen")
				if f.receipt(key) != nil {
					t.Fatal("refused key became an accepted receipt after SQL reopen")
				}
			}

			// The refused key was never spent. Changing only the final target now
			// admits the same prefix through the same production public setup.
			finalClose := b21Close(anchor)
			finalClose.Close.Reason = "anchor first reason"
			accepted := model.ChangeSet{Ops: append(append([]model.Op(nil), prefix...), finalClose)}
			first := f.apply(key, accepted)
			commit := b21Commit(t, first)
			if first.Meta.Replayed || len(commit.Results) != 6 || commit.Revision != b21Next(t, before.Head) {
				t.Fatalf("accepted companion: %+v replay=%v", commit, first.Meta.Replayed)
			}
			created := commit.Results[2].Ref
			if !created.IsID() || created.ID == "" || created == anchor || created == ephemeral {
				t.Fatalf("new named binding: %+v", created)
			}
			for i, want := range []model.Ref{anchor, ephemeral, created, created, created, anchor} {
				b21Equal(t, want, commit.Results[i].Ref, "accepted declaration-order index")
			}
			state := f.state()
			if len(state.Records) != len(before.Records)+1 || len(state.Log) != len(before.Log)+1 || len(state.Log[len(state.Log)-1].Changes) != 2 || len(state.Versions[anchor.ID]) != len(before.Versions[anchor.ID])+1 || len(state.Versions[created.ID]) != 1 {
				t.Fatal("accepted prefix must publish two durable rows in one revision")
			}
			f.publicFields(anchor, map[string]any{"title": "original durable", "description": "prefix durable edit", "status": "done"})
			f.publicFields(created, map[string]any{"title": "prefix updated", "status": "done", "priority": json.Number("2"), "template": false})
			f.publicFields(ephemeral, map[string]any{"title": "prefix ephemeral edit"})
			b21Reason(t, f.doc(anchor), "anchor first reason", f.now, []string{"anchor first reason"})
			b21Reason(t, f.doc(created), "created first reason", f.now, []string{"created first reason"})
			if f.posts.count() != posts+1 {
				t.Fatal("accepted companion must deliver one committed post")
			}
			stored := f.receipt(key)
			if stored == nil || stored.ResponsePending {
				t.Fatal("accepted companion has no completed receipt")
			}
			b21Equal(t, commit, stored.Commit, "accepted companion stored Commit")
			checkReplay := func() {
				t.Helper()
				replayed := f.apply(key, accepted)
				b21Commit(t, replayed)
				if !replayed.Meta.Replayed || !bytes.Equal(first.Result, replayed.Result) || f.posts.count() != posts+1 {
					t.Fatal("accepted companion replay changed bytes or delivered post")
				}
				b21Equal(t, state, f.state(), "accepted companion replay durable conservation")
				f.publicFields(ephemeral, map[string]any{"title": "prefix ephemeral edit"})
			}
			checkReplay()
			if engine != "memory" {
				f.reopen()
				checkReplay()
				f.publicFields(anchor, map[string]any{"description": "prefix durable edit", "status": "done"})
				f.publicFields(created, map[string]any{"title": "prefix updated", "status": "done"})
			}
			b21Retain(t, engine+"-mixed-prefix", map[string]any{"refused_request": refusedOps, "refusal": refused, "before": before, "accepted_request": accepted, "commit": commit, "state": state, "durable": f.publicRow(anchor), "ephemeral": f.publicRow(ephemeral)})
		})
	}
}

type b21CloseIndexWant struct {
	ref      model.Ref
	revision model.Revision
	document pipeline.Document
}

func b21CloseIndexes(t *testing.T, commit model.Commit, want []b21CloseIndexWant) {
	t.Helper()
	if len(commit.Results) != len(want) || len(commit.Warnings) != 0 {
		t.Fatalf("indexed Commit shape: %+v", commit)
	}
	for i, expected := range want {
		got := commit.Results[i]
		b21Equal(t, expected.ref, got.Ref, "close result reference")
		b21Equal(t, expected.revision, got.Revision, "unchanged row keeps its own revision")
		document, err := pipeline.DecodeDocument(got.Record)
		if err != nil {
			t.Fatalf("indexed result %d has no full canonical record: %v", i, err)
		}
		b21Equal(t, expected.document, document, "indexed close exact stored document")
	}
}

// A no-op Commit has the current head, while every unchanged result retains
// that row's own revision. Replay must retain those saved records even after a
// later genuine settlement changes the current row and the store is reopened.
func TestB21PublicIndexedNoopCloseReceiptReplaysStoredRecords(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB21(t, engine)
			seed := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Create("a", "indexed A"), b21Create("b", "indexed B")}}))
			a, b := seed.Results[0].Ref, seed.Results[1].Ref
			f.now = f.now.Add(time.Hour)
			firstAAt := f.now
			closeA := b21Close(a)
			closeA.Close.Reason = "first indexed A"
			firstA := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{closeA}}))
			f.now = f.now.Add(time.Hour)
			firstBAt := f.now
			firstB := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Close(b)}}))
			if firstA.Revision == firstB.Revision {
				t.Fatal("fixture requires independently committed row revisions")
			}
			b21Reason(t, f.doc(a), "first indexed A", firstAAt, []string{"first indexed A"})
			b21Reason(t, f.doc(b), "", firstBAt, nil)
			f.publicFields(a, map[string]any{"title": "indexed A", "status": "done"})
			f.publicFields(b, map[string]any{"title": "indexed B", "status": "done"})
			before := f.state()
			publicA, publicB := f.publicRow(a), f.publicRow(b)
			want := []b21CloseIndexWant{{b, firstB.Revision, f.doc(b)}, {a, firstA.Revision, f.doc(a)}, {b, firstB.Revision, f.doc(b)}, {a, firstA.Revision, f.doc(a)}}
			explicit := "done"
			repeatA, repeatB := b21Close(a), b21Close(b)
			repeatA.Close.Reason, repeatA.Close.TargetStatus = "first indexed A", &explicit
			repeatB.Close.TargetStatus = &explicit
			request := model.ChangeSet{Ops: []model.Op{b21Close(b), repeatA, repeatB, closeA}}
			key := f.key()
			posts := f.posts.count()
			f.now = f.now.Add(time.Hour)
			first := f.apply(key, request)
			commit := b21Commit(t, first)
			if first.Meta.Replayed || commit.Revision != before.Head {
				t.Fatalf("all-no-op close Commit: %+v replay=%v", commit, first.Meta.Replayed)
			}
			b21CloseIndexes(t, commit, want)
			b21Equal(t, before, f.state(), "all-no-op indexed close durable conservation")
			b21Equal(t, publicA, f.publicRow(a), "all-no-op exact public A")
			b21Equal(t, publicB, f.publicRow(b), "all-no-op exact public B")
			stored := f.receipt(key)
			if stored == nil || stored.ResponsePending || f.posts.count() != posts {
				t.Fatal("no-op close requires completed receipt and no post")
			}
			b21Equal(t, commit, stored.Commit, "no-op stored indexed Commit")
			// The receipt stores a typed Commit, not a public JSON encoding.
			// Keep its full value equality above and compare the actual first
			// HTTP result bytes directly with every replay below.
			checkReplay := func(current b21State, currentA map[string]any) {
				t.Helper()
				replayed := f.apply(key, request)
				b21CloseIndexes(t, b21Commit(t, replayed), want)
				if !replayed.Meta.Replayed || !bytes.Equal(first.Result, replayed.Result) || f.posts.count() != posts {
					t.Fatal("indexed no-op replay changed stored bytes or called post")
				}
				b21Equal(t, current, f.state(), "indexed receipt replay current durable conservation")
				b21Equal(t, currentA, f.publicRow(a), "indexed receipt replay current A conservation")
				b21Equal(t, publicB, f.publicRow(b), "indexed receipt replay current B conservation")
			}
			checkReplay(before, publicA)
			if engine != "memory" {
				f.reopen()
				checkReplay(before, publicA)
			}

			// Binding17:19's named Plug deviation: a different repeat reason
			// appends one comment; native close drops that reason. This proves
			// saved no-op results cannot be rebuilt from the now-current row.
			f.now = f.now.Add(time.Hour)
			different := b21Close(a)
			different.Close.Reason = "later indexed settlement"
			changed := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{different}}))
			b21Equal(t, b21Next(t, before.Head), changed.Revision, "different repeat reason one revision")
			b21Reason(t, f.doc(a), "first indexed A", firstAAt, []string{"first indexed A", "later indexed settlement"})
			if f.posts.count() != posts+1 {
				t.Fatal("different repeat reason must deliver one post")
			}
			posts++
			current, currentA := f.state(), f.publicRow(a)
			if len(current.Log) != len(before.Log)+1 || len(current.Log[len(current.Log)-1].Changes) != 1 || len(current.Versions[a.ID]) != len(before.Versions[a.ID])+1 {
				t.Fatal("different repeat reason must change only one durable row/version")
			}
			checkReplay(current, currentA)
			if engine != "memory" {
				f.reopen()
				checkReplay(current, currentA)
				retained := f.receipt(key)
				if retained == nil || retained.ResponsePending {
					t.Fatal("SQL lost completed indexed no-op receipt")
				}
				b21Equal(t, commit, retained.Commit, "SQL retained original no-op indexed Commit")
			}
			b21Retain(t, engine+"-indexed-noop", map[string]any{"request": request, "saved_commit": commit, "saved_bytes": string(first.Result), "before": before, "current": current, "current_A": currentA, "current_B": publicB, "post_calls": posts})
		})
	}
}

package cardb20

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// All engines require a public item and exact body readback under the adopted
// PR467/17:14 contract. Memory has process lifetime; only SQL promises reopen.
func TestB21PublicEphemeralOnlyBatchHasNoDurableHistory(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB21(t, engine)
			before := f.state()
			reply := f.call("x.workflow.invoke", f.key(), map[string]any{"project": b21Project, "mode": "ephemeral", "template": "checklist", "checklist": []map[string]any{{"label": "first", "title": "ephemeral original", "type": "issue"}}})
			if reply.Error != nil {
				t.Fatalf("required actual public ephemeral is unavailable on %s: %+v", engine, reply.Error.Data)
			}
			var result struct {
				Items []struct {
					ID string `json:"id"`
				} `json:"items"`
			}
			b21Decode(t, reply.Result, &result)
			if len(result.Items) != 1 || result.Items[0].ID == "" {
				t.Fatalf("actual ephemeral invocation response %s", reply.Result)
			}
			ref := model.RefByID(result.Items[0].ID, model.Natural64(0))
			row := f.publicRow(ref)
			if row["id"] != ref.ID || row["plane"] != "ephemeral" {
				t.Fatalf("actual public ephemeral readback %+v", row)
			}
			c := b21Commit(t, f.apply(f.key(), model.ChangeSet{Ops: []model.Op{b21Patch(ref, map[string]any{"title": "ephemeral patched in batch"})}}))
			if len(c.Results) != 1 || c.Results[0].Ref != ref {
				t.Fatalf("ephemeral batch indexed result %+v", c)
			}
			row = f.publicRow(ref)
			fields, _ := row["fields"].(map[string]any)
			if fields["title"] != "ephemeral patched in batch" {
				t.Fatalf("ephemeral batch exact final image %+v", row)
			}
			b21Equal(t, before, f.state(), "ephemeral-only batch no durable records, history or versions")
			if engine != "memory" {
				f.reopen()
				b21Equal(t, before, f.state(), "ephemeral-only SQL durable state")
				row = f.publicRow(ref)
				fields, _ = row["fields"].(map[string]any)
				if fields["title"] != "ephemeral patched in batch" {
					t.Fatal("actual ephemeral value lost on SQL reopen")
				}
			}
			// Keep this durable+ephemeral batch separate from the ephemeral-only no-history
			// observation above. One core row delta yields one revision while
			// the same public batch changes the actual ephemeral item.
			mixed := model.ChangeSet{Ops: []model.Op{b21Create("durable", "mixed durable"), b21Patch(ref, map[string]any{"title": "mixed ephemeral"})}}
			mixedKey := f.key()
			mixedReply := f.apply(mixedKey, mixed)
			mixedCommit := b21Commit(t, mixedReply)
			if len(mixedCommit.Results) != 2 || mixedCommit.Revision != b21Next(t, before.Head) || mixedCommit.Results[1].Ref != ref {
				t.Fatalf("mixed-plane public commit %+v", mixedCommit)
			}
			durable := mixedCommit.Results[0].Ref
			state := f.state()
			if len(state.Records) != len(before.Records)+1 || len(state.Log) != len(before.Log)+1 || len(state.Versions[durable.ID]) != 1 {
				t.Fatal("mixed-plane durable delta is not exactly one row/revision")
			}
			f.publicFields(durable, map[string]any{"title": "mixed durable", "status": "todo"})
			f.publicFields(ref, map[string]any{"title": "mixed ephemeral"})
			posts := f.posts.count()
			checkReplay := func() {
				t.Helper()
				replayed := f.apply(mixedKey, mixed)
				if !replayed.Meta.Replayed || string(replayed.Result) != string(mixedReply.Result) || f.posts.count() != posts {
					t.Fatal("mixed-plane replay changed stored bytes or delivered post")
				}
				b21Equal(t, state, f.state(), "mixed replay durable conservation")
				f.publicFields(ref, map[string]any{"title": "mixed ephemeral"})
			}
			checkReplay()
			if engine != "memory" {
				f.reopen()
				checkReplay()
				f.publicFields(durable, map[string]any{"title": "mixed durable", "status": "todo"})
			}
		})
	}
}

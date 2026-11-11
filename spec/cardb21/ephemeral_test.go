package cardb20

import (
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// SQL qualification needs a public item and exact body readback. Memory follows
// engdocs/plug-runtime/workflow-contract.md:57-65: typed refusal before state.
func TestB21PublicEphemeralOnlyBatchHasNoDurableHistory(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB21(t, engine)
			before := f.state()
			checkpointStore := f.store.(driver.CheckpointStore)
			checkpoint, err := checkpointStore.LoadCheckpoint(f.ctx, "plugin/workflow/default/durable")
			if err != nil {
				t.Fatal(err)
			}
			posts := f.posts.count()
			reply := f.call("x.workflow.invoke", f.key(), map[string]any{"project": b21Project, "mode": "ephemeral", "template": "checklist", "checklist": []map[string]any{{"label": "first", "title": "ephemeral original", "type": "issue"}}})
			if engine == "memory" {
				b21Code(t, reply, "plugin_durable_state_unavailable")
				b21Equal(t, before, f.state(), "memory refused invocation conserves complete durable state")
				after, err := checkpointStore.LoadCheckpoint(f.ctx, "plugin/workflow/default/durable")
				if err != nil {
					t.Fatal(err)
				}
				b21Equal(t, checkpoint, after, "memory refusal publishes no workflow checkpoint")
				if f.posts.count() != posts {
					t.Fatal("memory refused invocation published post")
				}
				return
			}
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
		})
	}
}

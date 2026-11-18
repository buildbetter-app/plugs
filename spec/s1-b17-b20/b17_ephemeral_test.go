package s1b17b20

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func ephemeralPrecisionRuntime(t *testing.T, p *pairedStore) *runtime {
	t.Helper()
	meta, err := p.store.Meta(p.ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, err := driver.RequireWorkflowCheckpointStore(p.store, p.lease)
	if err != nil {
		t.Fatal(err)
	}
	state, err := pluginhost.NewWorkflowRuntimeState(checkpoints, p.lease)
	if err != nil {
		t.Fatal(err)
	}
	if p.engine == "memory" {
		persistent, ok := state.(interface{ Persistent() bool })
		if !ok || persistent.Persistent() || checkpoints.WorkflowCheckpointLifetime() != driver.CheckpointLifetimeProcess {
			t.Fatal("memory runtime must report process lifetime and effective Persistent false")
		}
	}
	provider, err := workflowplugin.New(state)
	if err != nil {
		t.Fatal(err)
	}
	provider.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)

	domain := service.New(p.store, p.lease, p.writer(), service.WithOntology(service.NewOntology(nil)), service.WithWorkflow(provider), service.WithClock(p.clock.Now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "cardb20", Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: []byte(b20CursorKey)})
	if err != nil {
		t.Fatal(err)
	}
	return &runtime{ctx: p.ctx, store: p.store, lease: p.lease, server: server, principal: b17Actor}
}

func ephemeralCoreState(t *testing.T, p *pairedStore, c *precisionClient, ref model.Ref, label string) {
	t.Helper()
	meta, err := p.store.Meta(p.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.store.Read(p.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(p.ctx, []model.Ref{ref})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := r.Versions(p.ctx, ref, driver.PageReq{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	log, err := r.Changesets(p.ctx, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(struct {
		Meta     driver.Meta
		Rows     []driver.Record
		Versions driver.Page[driver.Version]
		Log      []model.Changeset
	}{meta, rows, versions, log})
	if err != nil {
		t.Fatal(err)
	}
	c.save(t, label+".core.json", data)
	if !meta.Head.IsZero() || len(rows) != 0 || len(versions.Items) != 0 || versions.NextCursor != nil || len(log) != 0 {
		t.Fatalf("ephemeral metadata entered durable history: %s", data)
	}
}

func TestB17B18PublicEphemeralMetadataHasNoDurableVersion(t *testing.T) {
	const literalKey = "plugin.literal.dot/~key"
	const sibling = `{"integer":9007199254740993,"null":null,"spelling":1.0}`
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			c := newPrecisionClient(t, ephemeralPrecisionRuntime(t, p))
			input := map[string]any{"template": "b17-history", "project": "cardb20", "mode": "ephemeral", "checklist": []any{map[string]any{"label": "only", "title": "public ephemeral metadata", "type": "task", "metadata": map[string]any{"sibling": json.RawMessage(sibling)}}}}
			invoked := c.call(t, "x.workflow.invoke", input, true)
			// Binding 17:14Z accepts memory process-lifetime state, with no
			// restart promise; ephemeralPrecisionRuntime checks its actual markers.

			out := success[struct {
				Plane string `json:"plane"`
				Items []struct {
					ID string `json:"id"`
				} `json:"items"`
			}](t, "invoke", invoked)
			if out.Plane != "ephemeral" || len(out.Items) != 1 {
				t.Fatalf("invocation: %+v", out)
			}
			ref := model.RefByID(out.Items[0].ID, model.Natural64(0))
			get := func(t *testing.T) (model.Revision, map[string]json.RawMessage) {
				x := success[struct {
					Bead struct {
						Revision model.Revision             `json:"revision"`
						Metadata map[string]json.RawMessage `json:"metadata"`
					} `json:"bead"`
				}](t, "ephemeral get", c.call(t, "bead.get", map[string]any{"id": ref}, false))
				return x.Bead.Revision, x.Bead.Metadata
			}
			_, initial := get(t)
			if string(initial["sibling"]) != sibling {
				t.Fatalf("ephemeral invocation lost numeric tokens: %v", initial)
			}
			c.set(t, ref, literalKey, `1`)
			c.set(t, ref, literalKey, `1.0`)
			revision, metadata := get(t)
			if string(metadata[literalKey]) != `1.0` || string(metadata["sibling"]) != sibling {
				t.Fatalf("ephemeral swap lost literals/siblings: %v", metadata)
			}
			for i := 0; i < 2; i++ {
				c.set(t, ref, literalKey, `1.0`)
				after, afterMetadata := get(t)
				if after.String() != revision.String() || !reflect.DeepEqual(afterMetadata, metadata) {
					t.Fatal("ephemeral equal-target call changed item generation/metadata")
				}
			}
			c.set(t, ref, literalKey, `null`)
			revision, metadata = get(t)
			if metadata[literalKey] != nil || string(metadata["sibling"]) != sibling {
				t.Fatalf("ephemeral remove changed sibling: %v", metadata)
			}
			c.set(t, ref, literalKey, `null`)
			after, afterMetadata := get(t)
			if after.String() != revision.String() || !reflect.DeepEqual(afterMetadata, metadata) {
				t.Fatal("ephemeral absent-to-absent changed item")
			}
			ephemeralCoreState(t, p, c, ref, "after-mutations")
			check := func(t *testing.T) {
				got, values := get(t)
				if got.String() != revision.String() || !reflect.DeepEqual(values, metadata) {
					t.Fatal("ephemeral state did not survive actual SQL reopen")
				}
				for _, method := range []string{"history.get", "history.diff"} {
					params := map[string]any{"ref": ref, "asOf": map[string]any{"revision": 0}}
					if method == "history.diff" {
						params = map[string]any{"ref": ref, "fromRevision": 0, "toRevision": 0}
					}
					denied := c.call(t, method, params, false)
					if denied.Error == nil || denied.Error.Data.Code != "not_found" || len(denied.Result) != 0 {
						t.Fatalf("ephemeral permanent history response: %+v", denied)
					}
				}
				page := success[struct {
					Items []json.RawMessage `json:"items"`
				}](t, "durable history.query", c.call(t, "history.query", map[string]any{"asOf": map[string]any{"revision": 0}}, false))
				if len(page.Items) != 0 {
					t.Fatal("ephemeral item appeared in durable history.query")
				}
				ephemeralCoreState(t, p, c, ref, "read-invariance")
			}
			t.Run("before_reopen", check)
			if engine != "memory" {
				p.reopen(t)
				c.r = ephemeralPrecisionRuntime(t, p)
				t.Run("after_reopen", check)
			}
		})
	}
}

// A sequential public mixed write selects the same literal operation on both
// planes. Only the ordinary bead contributes a durable version and log entry.
func TestB17B18PublicMixedMetadataKeepsPlaneVersions(t *testing.T) {
	const value = `{"integer":9007199254740993,"null":null,"spelling":1.0}`
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			c := newPrecisionClient(t, ephemeralPrecisionRuntime(t, p))
			created := success[model.Commit](t, "create durable", c.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "mixed metadata durable"}}, true))
			durable := created.Results[0].Ref
			invoked := success[struct {
				Plane string `json:"plane"`
				Items []struct { ID string `json:"id"` } `json:"items"`
			}](t, "invoke active", c.call(t, "x.workflow.invoke", map[string]any{
				"template": "b17-mixed", "project": "cardb20", "mode": "ephemeral",
				"checklist": []any{map[string]any{"label": "active", "title": "mixed metadata active", "type": "task"}},
			}, true))
			if invoked.Plane != "ephemeral" || len(invoked.Items) != 1 { t.Fatalf("invocation: %+v", invoked) }
			active := model.RefByID(invoked.Items[0].ID, model.Natural64(0))
			before := precisionState(t, p, c, durable, "mixed-before")
			changed := success[model.Commit](t, "mixed metadata", c.call(t, "tx.apply", map[string]any{
				"changeSet": map[string]any{"ops": []any{
					map[string]any{"kind": "setMeta", "target": active, "key": "literal", "value": json.RawMessage(value)},
					map[string]any{"kind": "setMeta", "target": durable, "key": "literal", "value": json.RawMessage(value)},
				}},
			}, true))
			if len(changed.Results) != 2 || changed.Results[0].Ref.ID != active.ID || changed.Results[1].Ref.ID != durable.ID { t.Fatalf("mixed result identity/order: %+v", changed) }
			after := precisionOneVersion(t, p, c, durable, before, "mixed-after", changed)
			check := func(t *testing.T) {
				for _, ref := range []model.Ref{active, durable} {
					got := success[struct { Bead struct { Metadata map[string]json.RawMessage `json:"metadata"` } `json:"bead"` }](t, "mixed get", c.call(t, "bead.get", map[string]any{"id": ref}, false))
					if len(got.Bead.Metadata) != 1 || string(got.Bead.Metadata["literal"]) != value { t.Fatalf("mixed public literal changed: %+v", got) }
				}
				if got := precisionMetadata(t, c, durable, changed.Revision); len(got) != 1 || string(got["literal"]) != value { t.Fatal("mixed durable history lost metadata tokens") }
				meta, err := p.store.Meta(p.ctx)
				if err != nil { t.Fatal(err) }
				r, err := p.store.Read(p.ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil { t.Fatal(err) }
				rows, rowErr := r.Get(p.ctx, []model.Ref{active})
				versions, versionErr := r.Versions(p.ctx, active, driver.PageReq{Limit: 100})
				closeErr := r.Close()
				if rowErr != nil || versionErr != nil || closeErr != nil || len(rows) != 0 || len(versions.Items) != 0 || versions.NextCursor != nil { t.Fatal("mixed active metadata entered durable rows/history", rowErr, versionErr, closeErr) }
				if !reflect.DeepEqual(after, precisionState(t, p, c, durable, "mixed-read-invariance")) { t.Fatal("mixed public read changed source/history") }
			}
			t.Run("before_reopen", check)
			if engine != "memory" {
				p.reopen(t)
				c.r = ephemeralPrecisionRuntime(t, p)
				t.Run("after_reopen", check)
			}
		})
	}
}

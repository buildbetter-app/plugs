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
	if err != nil { t.Fatal(err) }
	var provider *workflowplugin.Provider
	if checkpoints, err := driver.RequireWorkflowCheckpointStore(p.store, p.lease); err == nil {
		provider, err = workflowplugin.New(pluginhost.NewDurableState(checkpoints, p.lease, workflowplugin.Alias, "default"))
		if err != nil { t.Fatal(err) }
		provider.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
	}
	domain := service.New(p.store, p.lease, p.writer(), service.WithOntology(service.NewOntology(nil)), service.WithWorkflow(provider), service.WithClock(p.clock.Now))
	registry, err := rpc.NewRegistry()
	if err != nil { t.Fatal(err) }
	if err := rpc.BindService(registry, domain); err != nil { t.Fatal(err) }
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "cardb20", Incarnation: meta.Incarnation, OntologyRevision: model.Natural64(1), CursorKey: []byte(b20CursorKey)})
	if err != nil { t.Fatal(err) }
	return &runtime{ctx:p.ctx, store:p.store, lease:p.lease, server:server, principal:b17Actor}
}

func ephemeralCoreState(t *testing.T, p *pairedStore, c *precisionClient, ref model.Ref, label string) {
	t.Helper()
	meta, err := p.store.Meta(p.ctx)
	if err != nil { t.Fatal(err) }
	r, err := p.store.Read(p.ctx, driver.View{Incarnation:meta.Incarnation})
	if err != nil { t.Fatal(err) }
	defer r.Close()
	rows, err := r.Get(p.ctx, []model.Ref{ref})
	if err != nil { t.Fatal(err) }
	versions, err := r.Versions(p.ctx, ref, driver.PageReq{Limit:100})
	if err != nil { t.Fatal(err) }
	log, err := r.Changesets(p.ctx, nil, 100)
	if err != nil { t.Fatal(err) }
	data, err := json.Marshal(struct { Meta driver.Meta; Rows []driver.Record; Versions driver.Page[driver.Version]; Log []model.Changeset }{meta, rows, versions, log})
	if err != nil { t.Fatal(err) }
	c.save(t,label+".core.json",data)
	if !meta.Head.IsZero() || len(rows)!=0 || len(versions.Items)!=0 || versions.NextCursor!=nil || len(log)!=0 {
		t.Fatalf("ephemeral metadata entered durable history: %s",data)
	}
}

func TestB17B18PublicEphemeralMetadataHasNoDurableVersion(t *testing.T) {
	const literalKey = "plugin.literal.dot/~key"
	const sibling = `{"integer":9007199254740993,"null":null,"spelling":1.0}`
	for _,engine := range []string{"memory","sqlite","postgres"} {
		t.Run(engine,func(t *testing.T) {
			p := newPairedStore(t,engine)
			c := newPrecisionClient(t,ephemeralPrecisionRuntime(t,p))
			input := map[string]any{"template":"b17-history", "project":"cardb20", "mode":"ephemeral", "checklist":[]any{map[string]any{"label":"only", "title":"public ephemeral metadata", "type":"task", "metadata":map[string]any{"sibling":json.RawMessage(sibling)}}}}
			invoked := c.call(t,"x.workflow.invoke",input,true)
			if engine=="memory" {
				// Frozen workflow-contract.md:57-65 forbids a positive process
				// memory invocation. The actual typed refusal is its required arm.
				if invoked.Error==nil || invoked.Error.Data.Code!="plugin_durable_state_unavailable" || len(invoked.Result)!=0 { t.Fatalf("memory contract refusal: %+v",invoked) }
				ephemeralCoreState(t,p,c,model.RefByID("workflow:item/unknown",model.Natural64(0)),"memory-refusal")
				return
			}
			out := success[struct { Plane string `json:"plane"`; Items []struct { ID string `json:"id"` } `json:"items"` }](t,"invoke",invoked)
			if out.Plane!="ephemeral" || len(out.Items)!=1 { t.Fatalf("invocation: %+v",out) }
			ref := model.RefByID(out.Items[0].ID,model.Natural64(0))
			get := func(t *testing.T) (model.Revision,map[string]json.RawMessage) {
				x := success[struct { Bead struct { Revision model.Revision `json:"revision"`; Metadata map[string]json.RawMessage `json:"metadata"` } `json:"bead"` }](t,"ephemeral get",c.call(t,"bead.get",map[string]any{"id":ref},false))
				return x.Bead.Revision,x.Bead.Metadata
			}
			_,initial := get(t)
			if string(initial["sibling"])!=sibling { t.Fatalf("ephemeral invocation lost numeric tokens: %v",initial) }
			c.set(t,ref,literalKey,`1`)
			c.set(t,ref,literalKey,`1.0`)
			revision,metadata := get(t)
			if string(metadata[literalKey])!=`1.0` || string(metadata["sibling"])!=sibling { t.Fatalf("ephemeral swap lost literals/siblings: %v",metadata) }
			for i:=0;i<2;i++ {
				c.set(t,ref,literalKey,`1.0`)
				after,afterMetadata := get(t)
				if after.String()!=revision.String() || !reflect.DeepEqual(afterMetadata,metadata) { t.Fatal("ephemeral equal-target call changed item generation/metadata") }
			}
			c.set(t,ref,literalKey,`null`)
			revision,metadata = get(t)
			if metadata[literalKey]!=nil || string(metadata["sibling"])!=sibling { t.Fatalf("ephemeral remove changed sibling: %v",metadata) }
			c.set(t,ref,literalKey,`null`)
			after,afterMetadata := get(t)
			if after.String()!=revision.String() || !reflect.DeepEqual(afterMetadata,metadata) { t.Fatal("ephemeral absent-to-absent changed item") }
			ephemeralCoreState(t,p,c,ref,"after-mutations")
			check := func(t *testing.T) {
				got,values := get(t)
				if got.String()!=revision.String() || !reflect.DeepEqual(values,metadata) { t.Fatal("ephemeral state did not survive actual SQL reopen") }
				for _,method := range []string{"history.get","history.diff"} {
					params := map[string]any{"ref":ref,"asOf":map[string]any{"revision":0}}
					if method=="history.diff" { params=map[string]any{"ref":ref,"fromRevision":0,"toRevision":0} }
					denied := c.call(t,method,params,false)
					if denied.Error==nil || denied.Error.Data.Code!="not_found" || len(denied.Result)!=0 { t.Fatalf("ephemeral permanent history response: %+v",denied) }
				}
				page := success[struct { Items []json.RawMessage `json:"items"` }](t,"durable history.query",c.call(t,"history.query",map[string]any{},false))
				if len(page.Items)!=0 { t.Fatal("ephemeral item appeared in durable history.query") }
				ephemeralCoreState(t,p,c,ref,"read-invariance")
			}
			t.Run("before_reopen",check)
			p.reopen(t)
			c.r=ephemeralPrecisionRuntime(t,p)
			t.Run("after_reopen",check)
		})
	}
}

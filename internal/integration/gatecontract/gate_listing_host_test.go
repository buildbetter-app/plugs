package gatecontract_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/ontologyrpc"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

var gateOntologyRef = model.RefByID("ontology/b14-gates", model.Natural64(0))

func gateCatalogDocument() ontology.Document {
	doc := ontology.DefaultDocument()
	doc.Types[ontology.GateType] = ontology.Type{Name: "Gate", Workflow: "issue", Fields: map[string]ontology.Field{
		"title": {Type: "string"}, "description": {Type: "string"},
		"labels": {Type: "strings"}, "blocked": {Type: "boolean"},
	}}
	return doc
}

func installGateCatalog(t *testing.T, f *fixture) {
	t.Helper()
	seedGateCatalog(t, f, ontology.DefaultDocument())
	gateApply(f, gatePlan(f, gateCatalogDocument(), []any{}, 1), 1)
}

// Only the initial, pre-installation catalog is setup. Installation, mapping,
// data mutations, listing and reopen readback all use the named public RPCs.
func seedGateCatalog(t *testing.T, f *fixture, doc ontology.Document) {
	t.Helper()
	raw, err := service.MarshalOntologySource(service.OntologySource{Base: doc})
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, Time: time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)}, []conformance.FixtureChange{{Ref: gateOntologyRef, Kind: "ontology", Op: model.OpCreate, Body: raw}})
	if err != nil {
		t.Fatal(err)
	}
	bindGateHost(t, f, f.store, true, false)
}

func storedGateOntology(t *testing.T, f *fixture) *service.Ontology {
	t.Helper()
	ctx := context.Background()
	r, err := f.store.Read(ctx, driver.View{Incarnation: f.meta().Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.Get(ctx, []model.Ref{gateOntologyRef})
	_ = r.Close()
	if err != nil || len(rows) != 1 {
		t.Fatalf("stored catalog: %v rows=%d", err, len(rows))
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	return service.NewOntology(compiled.Effective)
}

func bindGateHost(t *testing.T, f *fixture, store driver.Store, storedOntology, workflow bool, clock ...func() time.Time) {
	t.Helper()
	runtime := service.NewOntology(nil)
	if storedOntology {
		runtime = storedGateOntology(t, f)
	}
	writerOptions := []pipeline.Option{pipeline.WithOntology(runtime), pipeline.WithIDGenerator(func() (string, error) {
		f.nextID++
		return fmt.Sprintf("gc-%06d", f.nextID), nil
	})}
	if len(clock) > 0 {
		writerOptions = append(writerOptions, pipeline.WithClock(clock[0]))
	}
	writer := pipeline.New(store, f.lease, writerOptions...)
	options := []service.Option{service.WithOntology(runtime)}
	// A memory core cannot install the durable workflow provider. Keep that
	// actual capability boundary; the public invoke method returns its typed
	// refusal and ordinary gate reads continue without a provider plane.
	persistent, hasPersistence := f.store.(driver.PersistentCheckpointStore)
	if workflow && hasPersistence && persistent.Persistent() {
		checkpoints, ok := f.store.(driver.CheckpointStore)
		if !ok {
			t.Fatal("real engine lacks durable workflow checkpoints")
		}
		state := pluginhost.NewDurableState(checkpoints, f.lease, workflowplugin.Alias, "default")
		provider, err := workflowplugin.New(state)
		if err != nil {
			t.Fatal(err)
		}
		provider.WithIdentity(workflowplugin.Alias, "default", f.meta().Incarnation)
		if len(clock) > 0 {
			provider.WithClock(clock[0])
		}
		options = append(options, service.WithWorkflow(provider))
	}
	domain := service.New(store, f.lease, writer, options...)
	reg, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(reg, domain); err != nil {
		t.Fatal(err)
	}
	if storedOntology {
		ops, err := ontologyops.New(store, f.lease, ontologyops.Config{Source: service.StaticOntologySource(gateOntologyRef), ValidatingPre: ontologyops.NoValidatingPre(), ProtectedApplier: writer, OntologyRefresher: runtime})
		if err != nil {
			t.Fatal(err)
		}
		if err := ontologyrpc.Bind(reg, ops); err != nil {
			t.Fatal(err)
		}
	}
	f.server, err = rpc.New(reg, rpc.Config{DefaultProject: "local", Incarnation: f.meta().Incarnation})
	if err != nil {
		t.Fatal(err)
	}
}

func gateCreate(f *fixture, typ, await string, fields map[string]any) string {
	f.t.Helper()
	values := map[string]any{"title": "public gate", "await_type": await}
	for k, v := range fields {
		values[k] = v
	}
	return resultID(f.call("bead.create", map[string]any{"type": typ, "fields": values}, true, ""), 0)
}

func gateIDs(result map[string]any) []string {
	ids := []string{}
	for _, value := range result["items"].([]any) {
		ids = append(ids, value.(map[string]any)["id"].(string))
	}
	return ids
}

func typeMapping(from, to, id string) map[string]any {
	target := map[string]any{"project": "local", "type": from}
	if id != "" {
		target["id"] = id
	}
	return map[string]any{"id": "b14-map-type", "kind": "map_type", "target": target, "from": from, "to": to}
}

func gatePlan(f *fixture, doc ontology.Document, mappings []any, sourceRevision any) map[string]any {
	f.t.Helper()
	return f.call("ontology.plan", map[string]any{"project": "", "document": doc, "typeMappings": mappings, "expect": expect(gateOntologyRef.ID, sourceRevision)}, false, "")
}

func gateApply(f *fixture, plan map[string]any, sourceRevision any) {
	f.t.Helper()
	if !plan["changes"].(map[string]any)["ready"].(bool) || len(plan["violations"].([]any)) != 0 {
		f.t.Fatalf("map_type plan refused: %v", plan)
	}
	digest := plan["planDigest"]
	f.call("ontology.apply", map[string]any{"planDigest": digest, "expect": expect(gateOntologyRef.ID, sourceRevision), "humanPresence": map[string]any{"mode": "presence_check", "challenge": digest, "proof": digest}}, true, "")
}

func decodedJSON(t *testing.T, value any) any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var detached any
	if err := json.Unmarshal(raw, &detached); err != nil {
		t.Fatal(err)
	}
	return detached
}

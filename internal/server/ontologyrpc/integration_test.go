package ontologyrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type rpcFixture struct {
	transport *inprocess.Transport
	registry  *rpc.Registry
	core      *ontologyops.Service
	store     *memory.Store
	lease     driver.ActiveLease
	source    model.Ref
	base      ontology.Document
}

func fixture(t *testing.T) *rpcFixture {
	t.Helper()
	ctx := context.Background()
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "ontologyrpc-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	base := ontology.DefaultDocument()
	ref := model.Ref{ID: "ontology/rpc"}
	raw, err := service.MarshalOntologySource(service.OntologySource{Base: base})
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(ctx, store, lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, Time: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}, []conformance.FixtureChange{{Ref: ref, Kind: "ontology", Op: model.OpCreate, Body: raw}})
	if err != nil {
		t.Fatal(err)
	}
	core, err := ontologyops.New(store, lease, ontologyops.Config{Source: service.StaticOntologySource(ref), ValidatingPre: ontologyops.NoValidatingPre()})
	if err != nil {
		t.Fatal(err)
	}
	reg, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := Bind(reg, core); err != nil {
		t.Fatal(err)
	}
	compiled, problems := ontology.Compile(base, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	workflow := service.NewOntology(compiled.Effective)
	writer := pipeline.New(store, lease, pipeline.WithOntology(workflow))
	if err := rpc.BindService(reg, service.New(store, lease, writer, service.WithOntology(workflow))); err != nil {
		t.Fatal(err)
	}
	meta, _ := store.Meta(ctx)
	server, err := rpc.New(reg, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: "p", OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	return &rpcFixture{transport: inprocess.New(server), registry: reg, core: core, store: store, lease: lease, source: ref, base: base}
}

func (f *rpcFixture) call(t *testing.T, method string, params any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "ontology-test", "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.transport.RoundTrip(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Result json.RawMessage `json:"result"`
		Error  *wire.Error     `json:"error"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		t.Fatalf("%s: %s: %v", method, response, err)
	}
	if result.Error != nil || len(result.Result) == 0 {
		t.Fatalf("%s failed: %s", method, response)
	}
	desc, exists := f.registry.Catalog().Descriptor(method)
	if !exists || f.registry.Catalog().ValidateResult(desc, result.Result) != nil {
		t.Fatalf("%s returned noncanonical result: %s", method, response)
	}
	return result.Result
}

func TestByteRPCStoredSourceCompilerPlannerAndHistoricalRows(t *testing.T) {
	f := fixture(t)
	created := f.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "Actual stored row"}, "_meta": map[string]any{"idempotencyKey": "e33d4848-10c7-40cb-a3d4-49e34b0d02a1"}})
	var commit model.Commit
	if err := json.Unmarshal(created, &commit); err != nil || len(commit.Results) != 1 {
		t.Fatalf("actual create: %s %v", created, err)
	}
	got := f.call(t, "ontology.get", map[string]any{})
	var result ontologyops.Result
	if err := json.Unmarshal(got, &result); err != nil || result.Revision.String() != "1" || result.Document.Types["issue"].Name != f.base.Types["issue"].Name {
		t.Fatalf("get: %s %v", got, err)
	}
	f.call(t, "ontology.describe", map[string]any{"project": "p", "type": "issue"})
	schema := f.call(t, "ontology.schema", map[string]any{"project": "p", "type": "issue", "status": "todo"})
	if !strings.Contains(string(schema), `"const":"issue"`) || !strings.Contains(string(schema), `"enum":["todo"]`) {
		t.Fatalf("effective schema: %s", schema)
	}
	check := f.call(t, "ontology.check", map[string]any{})
	if !strings.Contains(string(check), `"ok":true`) {
		t.Fatalf("stored row integrity: %s", check)
	}
	doc := ontology.DefaultDocument()
	typ := doc.Types["issue"]
	typ.Fields["reviewed"] = ontology.Field{Type: "boolean", Default: false}
	doc.Types["issue"] = typ
	planned := f.call(t, "ontology.plan", map[string]any{"document": doc, "expect": []model.Precondition{model.PreconditionByRevision(f.source, model.Natural64(1))}})
	var plan ontologyops.PlanResult
	if err := json.Unmarshal(planned, &plan); err != nil || !plan.Changes.Ready || len(plan.Changes.Migration.Rows) != 1 || plan.BaseRevision.String() != "2" || len(plan.Violations) != 0 {
		t.Fatalf("actual planner: %s %v", planned, err)
	}
	if plan.Changes.Migration.Rows[0].Before.State.ID != commit.Results[0].Ref.ID || plan.Changes.Migration.Rows[0].After.State.Fields["reviewed"] != false {
		t.Fatalf("planner lost stored images: %s", planned)
	}
	meta, _ := f.store.Meta(context.Background())
	if meta.Head.String() != "2" {
		t.Fatal("read/plan calls wrote data")
	}
	if _, err := f.core.Planned(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, plan.PlanDigest); err != nil {
		t.Fatal(err)
	}
	raw, err := service.MarshalOntologySource(service.OntologySource{Base: doc})
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, Time: time.Date(2026, 10, 29, 12, 1, 0, 0, time.UTC)}, []conformance.FixtureChange{{Ref: f.source, Kind: "ontology", Op: model.OpPatch, Body: raw}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.Planned(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, plan.PlanDigest); err == nil {
		t.Fatal("source change did not invalidate retained plan")
	}
	history := f.call(t, "ontology.history", map[string]any{"limit": 1})
	if !strings.Contains(string(history), `"revision":1`) {
		t.Fatalf("real history: %s", history)
	}
	more := f.call(t, "ontology.history", map[string]any{"after": 1})
	if !strings.Contains(string(more), `"revision":3`) || strings.Contains(string(more), commit.Results[0].Ref.ID) {
		t.Fatalf("source history projection: %s", more)
	}
	diff := f.call(t, "ontology.diff", map[string]any{"fromRevision": 1, "toRevision": 3})
	if !strings.Contains(string(diff), `"op":"replace"`) || !strings.Contains(string(diff), `"reviewed"`) {
		t.Fatalf("real as-of diff: %s", diff)
	}
}

func TestRuntimeDiscoveryAndExplicitUnsupportedApply(t *testing.T) {
	f := fixture(t)
	discovery := f.call(t, "rpc.discover", map[string]any{})
	var doc struct {
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(discovery, &doc); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, method := range doc.Methods {
		found[method.Name] = true
	}
	for _, name := range names {
		if !found[name] {
			t.Fatalf("actual method absent from discovery: %s", name)
		}
	}
	if found["ontology.apply"] {
		t.Fatal("unimplemented apply was advertised")
	}
	response, _ := f.transport.RoundTrip(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"ontology.apply","params":{"planDigest":"anything"}}`))
	if !strings.Contains(string(response), `"code":"method_not_found"`) {
		t.Fatalf("apply must explicitly refuse: %s", response)
	}
	if err := Bind(f.registry, f.core); err == nil {
		t.Fatal("duplicate binding accepted")
	}
}

func TestCanonicalRPCParameterAndSourceRefusals(t *testing.T) {
	f := fixture(t)
	for _, tc := range []struct{ method, params, code string }{
		{"ontology.get", `{"unexpected":true}`, "invalid_params"},
		{"ontology.schema", `{"type":"issue"}`, "invalid_params"},
		{"ontology.schema", `{"project":"p","type":"missing"}`, "not_found"},
		{"ontology.plan", `{"document":{"statuses":{},"gate_kinds":{"broken":{"resolver":"builtin.manual","params":{"min_approvals":0}}}}}`, "invalid_params"},
		{"ontology.plan", `{"document":{},"directives":[]}`, "invalid_params"},
		{"ontology.history", `{"limit":1001}`, "invalid_params"},
		{"ontology.history", `{"after":99999999999999999999999999999}`, "cursor_invalid"},
		{"ontology.diff", `{"fromRevision":0,"toRevision":1}`, "not_found"},
		{"ontology.check", `{"project":"p","project":"q"}`, "invalid_request"},
	} {
		t.Run(tc.method+tc.params, func(t *testing.T) {
			request := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"method":%q,"params":%s}`, tc.method, tc.params))
			response, err := f.transport.RoundTrip(context.Background(), request)
			if err != nil || !strings.Contains(string(response), `"code":"`+tc.code+`"`) {
				t.Fatalf("got %s %v, want %s", response, err, tc.code)
			}
		})
	}
}

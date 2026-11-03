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

var anonymousCaller = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

func applyFixture(t *testing.T) *rpcFixture {
	t.Helper()
	return fixtureWithApply(t, true)
}

func fixture(t *testing.T) *rpcFixture {
	t.Helper()
	return fixtureWithApply(t, false)
}

func fixtureWithApply(t *testing.T, withApply bool) *rpcFixture {
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
	compiled, problems := ontology.Compile(base, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	workflow := service.NewOntology(compiled.Effective)
	writer := pipeline.New(store, lease, pipeline.WithOntology(workflow))
	protected := pipeline.ProtectedApplier(nil)
	if withApply {
		protected = writer
	}
	core, err := ontologyops.New(store, lease, ontologyops.Config{Source: service.StaticOntologySource(ref), ValidatingPre: ontologyops.NoValidatingPre(), ProtectedApplier: protected, OntologyRefresher: workflow})
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

func TestProtectedApplyPersistsCompleteImagesAndReplays(t *testing.T) {
	f := applyFixture(t)
	created := f.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "preserve", "status": "todo"}, "_meta": map[string]any{"idempotencyKey": "e33d4848-10c7-40cb-a3d4-49e34b0d02a1"}})
	var commit model.Commit
	if err := json.Unmarshal(created, &commit); err != nil || len(commit.Results) != 1 {
		t.Fatalf("create: %s %v", created, err)
	}
	ref := commit.Results[0].Ref
	full := pipeline.Document{ID: ref.ID, Project: "p", Type: "issue", Fields: map[string]any{"status": "todo", "title": "preserve"}, Metadata: map[string]json.RawMessage{"source": json.RawMessage(`{"keep":true}`)}, Comments: []pipeline.Comment{{Body: "keep me", Principal: anonymousCaller, Time: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}}, Links: []pipeline.Link{{To: model.Ref{ID: "other"}, Kind: "relates-to"}}, Attachments: []model.Attachment{{Kind: "link", MediaType: "text/plain", Summary: "keep"}}}
	body, _ := json.Marshal(full)
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Principal: anonymousCaller, Time: time.Date(2026, 10, 29, 12, 1, 0, 0, time.UTC)}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpPatch, Body: body, Graph: &conformance.FixtureGraphSource{Links: []driver.SourceLink{{Kind: full.Links[0].Kind, To: full.Links[0].To}}}}}); err != nil {
		t.Fatal(err)
	}
	doc := ontology.DefaultDocument()
	typ := doc.Types["issue"]
	typ.Fields["reviewed"] = ontology.Field{Type: "boolean", Default: false}
	doc.Types["issue"] = typ
	planned := f.call(t, "ontology.plan", map[string]any{"document": doc, "expect": []model.Precondition{model.PreconditionByRevision(f.source, model.Natural64(1))}})
	var plan ontologyops.PlanResult
	if err := json.Unmarshal(planned, &plan); err != nil || !plan.Changes.Ready || plan.PlanDigest == "" {
		t.Fatalf("plan: %s %v", planned, err)
	}
	request := map[string]any{"planDigest": plan.PlanDigest, "expect": []model.Precondition{model.PreconditionByRevision(f.source, model.Natural64(1))}, "humanPresence": map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest}, "_meta": map[string]any{"idempotencyKey": "f33d4848-10c7-40cb-a3d4-49e34b0d02a1"}}
	first := f.call(t, "ontology.apply", request)
	var applied model.Commit
	if err := json.Unmarshal(first, &applied); err != nil || applied.Revision.String() != "4" || len(applied.Results) != 2 {
		t.Fatalf("apply: %s %v", first, err)
	}
	reader, err := f.store.Read(context.Background(), driver.View{Incarnation: mustMeta(t, f.store).Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	records, err := reader.Get(context.Background(), []model.Ref{f.source, ref})
	_ = reader.Close()
	if err != nil || len(records) != 2 {
		t.Fatalf("read applied records: %v", err)
	}
	var got pipeline.Document
	for _, record := range records {
		if record.Ref.ID == ref.ID {
			got, err = pipeline.DecodeDocument(record.Body)
		}
	}
	if err != nil || got.Fields["reviewed"] != false || len(got.Comments) != 1 || len(got.Links) != 1 || len(got.Attachments) != 1 || len(got.Metadata) != 1 {
		t.Fatalf("complete row image was not preserved: %+v %v", got, err)
	}
	ordinary := f.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "after ontology apply"}, "_meta": map[string]any{"idempotencyKey": "c33d4848-10c7-40cb-a3d4-49e34b0d02a1"}})
	var ordinaryCommit model.Commit
	if err := json.Unmarshal(ordinary, &ordinaryCommit); err != nil || len(ordinaryCommit.Results) != 1 {
		t.Fatalf("ordinary create after apply: %s %v", ordinary, err)
	}
	ordinaryDoc, err := pipeline.DecodeDocument(ordinaryCommit.Results[0].Record)
	if err != nil || ordinaryDoc.Fields["reviewed"] != false {
		t.Fatalf("ordinary operation did not observe refreshed ontology: %+v %v", ordinaryDoc, err)
	}
	secondRaw, err := f.transport.RoundTrip(context.Background(), mustJSON(t, map[string]any{"jsonrpc": "2.0", "id": "replay", "method": "ontology.apply", "params": request}))
	if err != nil || !strings.Contains(string(secondRaw), `"replayed":true`) || !strings.Contains(string(secondRaw), `"revision":4`) {
		t.Fatalf("replay: %s %v", secondRaw, err)
	}
	if head := mustMeta(t, f.store).Head.String(); head != "5" {
		t.Fatalf("replay changed head after ordinary create: %s", head)
	}
}

func TestOlderProtectedApplyReplayKeepsCurrentOntologyForOrdinaryCreate(t *testing.T) {
	f := applyFixture(t)
	planAndRequest := func(doc ontology.Document, revision model.Revision, key string) map[string]any {
		t.Helper()
		expect := []model.Precondition{model.PreconditionByRevision(f.source, revision)}
		planned := f.call(t, "ontology.plan", map[string]any{"document": doc, "expect": expect})
		var plan ontologyops.PlanResult
		if err := json.Unmarshal(planned, &plan); err != nil || !plan.Changes.Ready || plan.PlanDigest == "" {
			t.Fatalf("plan: %s %v", planned, err)
		}
		return map[string]any{"planDigest": plan.PlanDigest, "expect": expect, "humanPresence": map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest}, "_meta": map[string]any{"idempotencyKey": key}}
	}
	docA := ontology.DefaultDocument()
	typA := docA.Types["issue"]
	typA.Fields["reviewed"] = ontology.Field{Type: "boolean", Default: false}
	docA.Types["issue"] = typA
	requestA := planAndRequest(docA, model.Natural64(1), "f33d4848-10c7-40cb-a3d4-49e34b0d02a1")
	first := f.call(t, "ontology.apply", requestA)
	var appliedA model.Commit
	if err := json.Unmarshal(first, &appliedA); err != nil || len(appliedA.Results) != 1 {
		t.Fatalf("apply A: %s %v", first, err)
	}

	docB := ontology.DefaultDocument()
	typB := docB.Types["issue"]
	typB.Fields["reviewed"] = ontology.Field{Type: "boolean", Default: false}
	typB.Fields["verified"] = ontology.Field{Type: "boolean", Default: true}
	docB.Types["issue"] = typB
	requestB := planAndRequest(docB, appliedA.Revision, "a33d4848-10c7-40cb-a3d4-49e34b0d02a1")
	f.call(t, "ontology.apply", requestB)
	storedB := f.call(t, "ontology.get", map[string]any{})
	var current ontologyops.Result
	if err := json.Unmarshal(storedB, &current); err != nil || current.Document == nil || current.Document.Types["issue"].Fields["verified"].Default != true {
		t.Fatalf("stored B: %s %v", storedB, err)
	}
	headB := mustMeta(t, f.store).Head

	replayed, err := f.transport.RoundTrip(context.Background(), mustJSON(t, map[string]any{"jsonrpc": "2.0", "id": "older-replay", "method": "ontology.apply", "params": requestA}))
	if err != nil {
		t.Fatal(err)
	}
	var replay struct {
		Result json.RawMessage `json:"result"`
		Error  *wire.Error     `json:"error"`
		Meta   struct {
			Replayed bool `json:"replayed"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(replayed, &replay); err != nil || replay.Error != nil || !replay.Meta.Replayed || string(replay.Result) != string(first) {
		t.Fatalf("replay lost A's original receipt: first=%s replay=%s err=%v", first, replayed, err)
	}
	if head := mustMeta(t, f.store).Head; head != headB {
		t.Fatalf("older replay advanced head: before=%s after=%s", headB, head)
	}
	if stored := f.call(t, "ontology.get", map[string]any{}); string(stored) != string(storedB) {
		t.Fatalf("older replay changed durable B: before=%s after=%s", storedB, stored)
	}

	ordinary := f.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "after older ontology replay"}, "_meta": map[string]any{"idempotencyKey": "c33d4848-10c7-40cb-a3d4-49e34b0d02a1"}})
	var created model.Commit
	if err := json.Unmarshal(ordinary, &created); err != nil || len(created.Results) != 1 {
		t.Fatalf("ordinary create after replay: %s %v", ordinary, err)
	}
	got, err := pipeline.DecodeDocument(created.Results[0].Record)
	if err != nil || got.Fields["reviewed"] != false || got.Fields["verified"] != true {
		t.Fatalf("older replay replaced runtime B with A: %+v %v", got, err)
	}
}

func TestProtectedApplyProofAndStaleRefusals(t *testing.T) {
	f := applyFixture(t)
	doc := ontology.DefaultDocument()
	typ := doc.Types["issue"]
	typ.Fields["reviewed"] = ontology.Field{Type: "boolean", Default: false}
	doc.Types["issue"] = typ
	planned := f.call(t, "ontology.plan", map[string]any{"document": doc})
	var plan ontologyops.PlanResult
	if err := json.Unmarshal(planned, &plan); err != nil {
		t.Fatal(err)
	}
	repeated := f.call(t, "ontology.plan", map[string]any{"document": doc})
	var repeatedPlan ontologyops.PlanResult
	if err := json.Unmarshal(repeated, &repeatedPlan); err != nil || repeatedPlan.PlanDigest != plan.PlanDigest {
		t.Fatalf("plan digest was not deterministic: %s %s", planned, repeated)
	}
	base := map[string]any{"planDigest": plan.PlanDigest, "expect": []model.Precondition{}, "_meta": map[string]any{"idempotencyKey": "a33d4848-10c7-40cb-a3d4-49e34b0d02a1"}}
	for _, tc := range []struct {
		name  string
		proof map[string]any
		by    model.PrincipalRef
		want  string
	}{
		{name: "missing", proof: map[string]any{"mode": "presence_check", "challenge": "wrong", "proof": "wrong"}, by: anonymousCaller, want: "human_presence_required"},
		{name: "service", proof: map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest}, by: model.PrincipalRef{Kind: "service", ID: "service:api"}, want: "human_presence_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]any{}
			for k, v := range base {
				params[k] = v
			}
			params["humanPresence"] = tc.proof
			raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": tc.name, "method": "ontology.apply", "params": params})
			response, err := f.transport.RoundTrip(rpc.WithPrincipal(context.Background(), tc.by), raw)
			if err != nil || !strings.Contains(string(response), `"code":"`+tc.want+`"`) {
				t.Fatalf("response=%s err=%v", response, err)
			}
		})
	}
	staleParams := map[string]any{}
	for k, v := range base {
		staleParams[k] = v
	}
	staleParams["humanPresence"] = map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest}
	updated := ontology.DefaultDocument()
	updated.Types["issue"] = doc.Types["issue"]
	updatedBody, _ := service.MarshalOntologySource(service.OntologySource{Base: updated})
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Principal: anonymousCaller, Time: time.Date(2026, 10, 29, 12, 2, 0, 0, time.UTC)}, []conformance.FixtureChange{{Ref: f.source, Kind: "ontology", Op: model.OpPatch, Body: updatedBody}}); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "stale", "method": "ontology.apply", "params": staleParams})
	response, err := f.transport.RoundTrip(context.Background(), raw)
	if err != nil || !strings.Contains(string(response), `"code":"ontology_plan_stale"`) {
		t.Fatalf("stale apply response=%s err=%v", response, err)
	}
}

func mustMeta(t *testing.T, store *memory.Store) driver.Meta {
	t.Helper()
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
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

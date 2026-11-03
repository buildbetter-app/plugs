package b03catalog_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func composedRuntimeCall(t *testing.T, transport *inprocess.Transport, method string, params map[string]any, project, key string) json.RawMessage {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": key}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": key, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(context.Background(), raw)
	if err != nil {
		t.Fatalf("%s transport: %v", method, err)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Data struct {
				Code string `json:"code"`
			} `json:"data"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("%s response=%s: %v", method, response, err)
	}
	if envelope.Error != nil {
		t.Fatalf("%s refused: %s (%s)", method, envelope.Error.Data.Code, envelope.Error.Message)
	}
	return envelope.Result
}

func composedRuntimeStore(t *testing.T, e engine, source model.Ref, body []byte) driver.Store {
	t.Helper()
	store, err := e.storage.Open(context.Background(), driver.Config{DSN: e.dsn})
	if err != nil {
		t.Fatalf("open %s: %v", e.name, err)
	}
	lease, err := store.Lease(context.Background(), "b03-composition-seed", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	_, err = conformance.Seed(context.Background(), store, lease, driver.ChangesetHeader{
		Principal: model.PrincipalRef{Kind: "system", ID: "b03-composition-seed"},
	}, []conformance.FixtureChange{{Ref: source, Kind: "ontology", Op: model.OpCreate, Body: body}})
	if err != nil {
		_ = lease.Release(context.Background())
		_ = store.Close()
		t.Fatalf("seed %s: %v", e.name, err)
	}
	if err := lease.Release(context.Background()); err != nil {
		_ = store.Close()
		t.Fatalf("release seed lease %s: %v", e.name, err)
	}
	return store
}

// This target intentionally uses composition.Open and its public byte RPC for
// every engine. The lower-level catalog tests retain their separate source
// reload and SQL fault coverage; this test proves the installed source is what
// the production runtime actually admits and shares with ordinary writes.
func TestConfiguredRuntimeLoadsCatalogAndAuditsOnEveryEngine(t *testing.T) {
	eachEngine(t, allEngines, func(t *testing.T, e engine) {
		sourceRef := model.Ref{ID: "ontology/b03-composition"}
		body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
		if err != nil {
			t.Fatal(err)
		}
		store := composedRuntimeStore(t, e, sourceRef, body)
		runtime, err := composition.Open(context.Background(), composition.Config{
			Driver:  e.name,
			DSN:     e.dsn,
			Project: "b03-composition",
			Address: composition.DefaultAddress,
			StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) {
				return store, nil
			},
			Ontology: &composition.OntologyConfig{Source: service.StaticOntologySource(sourceRef)},
		})
		if err != nil {
			_ = store.Close()
			t.Fatalf("configured %s runtime refused installed source: %v", e.name, err)
		}
		defer func() {
			if err := runtime.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}()

		transport := inprocess.New(runtime.RPC)
		got := composedRuntimeCall(t, transport, "ontology.get", nil, "b03-composition", "00000000000000000000000000000001")
		if !strings.Contains(string(got), `"revision":"1"`) || !strings.Contains(string(got), `"types"`) {
			t.Fatalf("%s configured ontology.get did not expose installed document: %s", e.name, got)
		}
		updated := ontology.DefaultDocument()
		issue := updated.Types["issue"]
		issue.Fields["reviewed"] = ontology.Field{Type: "boolean", Default: false}
		updated.Types["issue"] = issue
		updatedCompiled, problems := ontology.Compile(updated, nil, nil)
		if len(problems) != 0 {
			t.Fatalf("%s updated ontology compile: %+v", e.name, problems)
		}
		planned := composedRuntimeCall(t, transport, "ontology.plan", map[string]any{"document": updated}, "b03-composition", "00000000000000000000000000000002a")
		var plan ontologyops.PlanResult
		if err := json.Unmarshal(planned, &plan); err != nil || !plan.Changes.Ready || plan.PlanDigest == "" {
			t.Fatalf("%s configured ontology.plan=%s err=%v", e.name, planned, err)
		}
		composedRuntimeCall(t, transport, "ontology.apply", map[string]any{
			"planDigest": plan.PlanDigest,
			"expect":     plan.Changes.Expect,
			"humanPresence": map[string]any{
				"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest,
			},
		}, "b03-composition", "00000000000000000000000000000002b")
		refreshed := composedRuntimeCall(t, transport, "ontology.get", nil, "b03-composition", "00000000000000000000000000000002c")
		var refreshedResult ontologyops.Result
		if err := json.Unmarshal(refreshed, &refreshedResult); err != nil || refreshedResult.Revision != "2" || refreshedResult.Document == nil || refreshedResult.Document.Types["issue"].Fields["reviewed"].Default != false {
			t.Fatalf("%s ontology.apply did not refresh configured runtime: %s err=%v", e.name, refreshed, err)
		}
		created := composedRuntimeCall(t, transport, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{"title": "configured blocker"},
		}, "b03-composition", "00000000000000000000000000000003")
		var blockerCommit model.Commit
		if err := json.Unmarshal(created, &blockerCommit); err != nil || len(blockerCommit.Results) != 1 {
			t.Fatalf("%s blocker create=%s err=%v", e.name, created, err)
		}
		createdDoc, err := pipeline.DecodeDocument(blockerCommit.Results[0].Record)
		if err != nil || createdDoc.Fields["reviewed"] != false {
			t.Fatalf("%s ordinary create did not use refreshed ontology: %+v err=%v", e.name, createdDoc, err)
		}
		blocker := blockerCommit.Results[0].Ref
		dependentRaw := composedRuntimeCall(t, transport, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{"title": "configured dependent"},
		}, "b03-composition", "00000000000000000000000000000004")
		var dependentCommit model.Commit
		if err := json.Unmarshal(dependentRaw, &dependentCommit); err != nil || len(dependentCommit.Results) != 1 {
			t.Fatalf("%s dependent create=%s err=%v", e.name, dependentRaw, err)
		}
		dependent := dependentCommit.Results[0].Ref
		composedRuntimeCall(t, transport, "bead.link", map[string]any{"from": dependent, "to": blocker, "kind": "conditional-blocks"}, "b03-composition", "00000000000000000000000000000005")
		composedRuntimeCall(t, transport, "bead.close", map[string]any{"id": blocker, "reason": "configured audit"}, "b03-composition", "00000000000000000000000000000006")

		meta, err := runtime.Store.Meta(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		at := meta.Head
		reader, err := runtime.Store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &at})
		if err != nil {
			t.Fatal(err)
		}
		sets, err := reader.Changesets(context.Background(), nil, 100)
		_ = reader.Close()
		if err != nil || len(sets) == 0 {
			t.Fatalf("%s changesets=%v err=%v", e.name, sets, err)
		}
		last := sets[len(sets)-1]
		if len(last.Events) < 2 {
			t.Fatalf("%s configured close did not publish conditional audit event: %+v", e.name, last)
		}
		var pinned bool
		for _, event := range last.Events {
			if event.Details == nil || event.Details.Ontology.Ref != sourceRef {
				continue
			}
			if event.Details.Ontology.Revision == "2" && event.Details.Ontology.Digest == updatedCompiled.Digest() {
				pinned = true
			}
		}
		if !pinned {
			t.Fatalf("%s configured lifecycle lost ontology pin: %+v", e.name, last.Events)
		}
		events := composedRuntimeCall(t, transport, "event.list", map[string]any{"filter": map[string]any{"events": []string{"dependency.conditionUnmet"}}, "limit": 100}, "b03-composition", "00000000000000000000000000000007")
		if !strings.Contains(string(events), sourceRef.ID) || !strings.Contains(string(events), string(model.EventDependencyConditionUnmet)) {
			t.Fatalf("%s event.list omitted persisted installed ontology audit: %s", e.name, events)
		}
	})
}

package composition

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

var configuredOntologyRef = model.Ref{ID: "ontology/b03-runtime"}

func seedConfiguredStore(t *testing.T, kind string, body []byte) *memory.Store {
	t.Helper()
	store := memory.New()
	lease, err := store.Lease(context.Background(), "b03-runtime-seed", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "" {
		_, err = conformance.Seed(context.Background(), store, lease, driver.ChangesetHeader{
			Principal: model.PrincipalRef{Kind: "system", ID: "b03-runtime-seed"},
			Time:      time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC),
		}, []conformance.FixtureChange{{Ref: configuredOntologyRef, Kind: kind, Op: model.OpCreate, Body: body}})
		if err != nil {
			_ = lease.Release(context.Background())
			_ = store.Close()
			t.Fatal(err)
		}
	}
	if err := lease.Release(context.Background()); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	return store
}

func configuredSourceBody(t *testing.T) []byte {
	t.Helper()
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func uncompilableSourceBody(t *testing.T) []byte {
	t.Helper()
	doc := ontology.DefaultDocument()
	typ := doc.Types["issue"]
	typ.Fields["expression"] = ontology.Field{Type: "expression"}
	doc.Types["issue"] = typ
	body, err := service.MarshalOntologySource(service.OntologySource{Base: doc})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func runtimeJSONCall(t *testing.T, transport *inprocess.Transport, method string, params map[string]any, project, key string) json.RawMessage {
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
				Hint string `json:"hint"`
			} `json:"data"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("%s response: %s: %v", method, response, err)
	}
	if envelope.Error != nil {
		t.Fatalf("%s refused: %s (%s; hint=%s)", method, envelope.Error.Data.Code, envelope.Error.Message, envelope.Error.Data.Hint)
	}
	return envelope.Result
}

func TestConfiguredOntologyRuntimeLoadsCatalogAndAuditsLifecycle(t *testing.T) {
	body := configuredSourceBody(t)
	store := seedConfiguredStore(t, "ontology", body)
	config := Config{
		Driver:  "memory",
		Project: "b03-runtime",
		Address: DefaultAddress,
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) {
			return store, nil
		},
		Ontology: &OntologyConfig{Source: service.StaticOntologySource(configuredOntologyRef)},
	}
	runtime, err := Open(context.Background(), config)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()

	transport := inprocess.New(runtime.RPC)
	discovery := runtimeJSONCall(t, transport, "rpc.discover", nil, config.Project, "00000000000000000000000000000001")
	var catalog struct {
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(discovery, &catalog); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, method := range catalog.Methods {
		found[method.Name] = true
	}
	for _, method := range []string{"ontology.get", "ontology.plan", "ontology.apply", "event.list"} {
		if !found[method] {
			t.Fatalf("configured runtime discovery omitted %s", method)
		}
	}

	got := runtimeJSONCall(t, transport, "ontology.get", nil, config.Project, "00000000000000000000000000000002")
	var ontologyResult struct {
		Revision model.Revision     `json:"revision"`
		Document *ontology.Document `json:"document"`
	}
	if err := json.Unmarshal(got, &ontologyResult); err != nil || ontologyResult.Document == nil || ontologyResult.Revision != "1" {
		t.Fatalf("configured ontology.get=%s err=%v", got, err)
	}

	created := runtimeJSONCall(t, transport, "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "B03 configured blocker"},
	}, config.Project, "00000000000000000000000000000003")
	var blockerCommit model.Commit
	if err := json.Unmarshal(created, &blockerCommit); err != nil || len(blockerCommit.Results) != 1 {
		t.Fatalf("blocker create=%s err=%v", created, err)
	}
	blocker := blockerCommit.Results[0].Ref
	dependentRaw := runtimeJSONCall(t, transport, "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": "B03 configured dependent"},
	}, config.Project, "00000000000000000000000000000004")
	var dependentCommit model.Commit
	if err := json.Unmarshal(dependentRaw, &dependentCommit); err != nil || len(dependentCommit.Results) != 1 {
		t.Fatalf("dependent create=%s err=%v", dependentRaw, err)
	}
	dependent := dependentCommit.Results[0].Ref
	runtimeJSONCall(t, transport, "bead.link", map[string]any{"from": dependent, "to": blocker, "kind": "conditional-blocks"}, config.Project, "00000000000000000000000000000005")
	runtimeJSONCall(t, transport, "bead.close", map[string]any{"id": blocker, "reason": "configured audit"}, config.Project, "00000000000000000000000000000006")

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
		t.Fatalf("changesets=%v err=%v", sets, err)
	}
	last := sets[len(sets)-1]
	if len(last.Events) < 2 {
		t.Fatalf("configured lifecycle changeset has no complete audit event plan: %+v", last)
	}
	var pinned bool
	installed, err := service.DecodeOntologySource(body)
	if err != nil {
		t.Fatal(err)
	}
	compiled, problems := ontology.Compile(installed.Base, installed.Overlays, installed.Plugins)
	if len(problems) != 0 {
		t.Fatalf("runtime source did not compile in test: %+v", problems)
	}
	for _, event := range last.Events {
		if event.Type != model.EventDependencyConditionUnmet || event.Details == nil {
			continue
		}
		pin := event.Details.Ontology
		if pin.Ref == configuredOntologyRef && pin.Revision == "1" && pin.Digest == compiled.Digest() {
			pinned = true
		}
	}
	if !pinned {
		t.Fatalf("conditional lifecycle event lost installed ontology pin: %+v", last.Events)
	}

	events := runtimeJSONCall(t, transport, "event.list", map[string]any{"filter": map[string]any{"events": []string{"dependency.conditionUnmet"}}, "limit": 100}, config.Project, "00000000000000000000000000000007")
	if !strings.Contains(string(events), string(model.EventDependencyConditionUnmet)) || !strings.Contains(string(events), configuredOntologyRef.ID) {
		t.Fatalf("event.list omitted the persisted pinned audit event: %s", events)
	}
}

func TestConfiguredOntologyStartupRefusesInvalidSourceAndClosesStore(t *testing.T) {
	cases := []struct {
		name string
		kind string
		body []byte
	}{
		{name: "missing"},
		{name: "wrong-kind", kind: "config", body: configuredSourceBody(t)},
		{name: "malformed", kind: "ontology", body: []byte(`{"base":{},"unsupported":true}`)},
		{name: "valid-json-uncompilable", kind: "ontology", body: uncompilableSourceBody(t)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := seedConfiguredStore(t, tc.kind, tc.body)
			_, err := Open(context.Background(), Config{
				Driver: "memory", Project: "b03-runtime", Address: DefaultAddress,
				StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil },
				Ontology:     &OntologyConfig{Source: service.StaticOntologySource(configuredOntologyRef)},
			})
			if err == nil {
				_ = store.Close()
				t.Fatal("invalid configured ontology source admitted startup")
			}
			if _, metaErr := store.Meta(context.Background()); metaErr == nil {
				t.Fatalf("startup refusal did not close store for %s: %v", tc.name, err)
			}
		})
	}
}

func TestConfiguredOntologyRejectsUnprotectedLocator(t *testing.T) {
	store := seedConfiguredStore(t, "ontology", configuredSourceBody(t))
	_, err := Open(context.Background(), Config{
		Driver: "memory", Project: "b03-runtime", Address: DefaultAddress,
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil },
		Ontology:     &OntologyConfig{Source: service.StaticOntologySource(model.Ref{ID: "bead/not-ontology"})},
	})
	if err == nil || !strings.Contains(err.Error(), "protected branch-zero ontology") {
		_ = store.Close()
		t.Fatalf("unprotected locator error=%v", err)
	}
}

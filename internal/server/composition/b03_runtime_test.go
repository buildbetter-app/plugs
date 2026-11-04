package composition

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
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

func runtimeEventListContains(t *testing.T, transport *inprocess.Transport, project, eventType, resource string) bool {
	t.Helper()
	var cursor string
	for i := 0; i < 8; i++ {
		params := map[string]any{"filter": map[string]any{"events": []string{eventType}}, "limit": 100}
		if cursor != "" {
			params["after"] = cursor
		}
		raw := runtimeJSONCall(t, transport, "event.list", params, project, fmt.Sprintf("%032x", 0x70+i))
		var page struct {
			Events []json.RawMessage `json:"events"`
			Cursor string            `json:"cursor"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Events {
			if strings.Contains(string(event), eventType) && strings.Contains(string(event), resource) {
				return true
			}
		}
		if page.Cursor == "" || page.Cursor == cursor {
			return false
		}
		cursor = page.Cursor
	}
	return false
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
		if pin.Ref.ID == configuredOntologyRef.ID && pin.Ref.Branch.String() == configuredOntologyRef.Branch.String() && pin.Revision == "1" && pin.Digest == compiled.Digest() {
			pinned = true
		}
	}
	if !pinned {
		t.Fatalf("conditional lifecycle event lost installed ontology pin: %+v", last.Events)
	}

	if !runtimeEventListContains(t, transport, config.Project, string(model.EventDependencyConditionUnmet), configuredOntologyRef.ID) {
		t.Fatalf("event.list omitted the persisted pinned audit event")
	}
}

func TestConfiguredOntologyStartupRefusesInvalidSourceAndClosesStore(t *testing.T) {
	cases := []struct {
		name      string
		kind      string
		body      []byte
		wantError string
	}{
		{name: "missing", wantError: "source is absent"},
		{name: "wrong-kind", kind: "config", body: configuredSourceBody(t), wantError: "wrong kind"},
		{name: "malformed", kind: "ontology", body: []byte(`{"base":{},"unsupported":true}`), wantError: "decode installed ontology source"},
		{name: "valid-json-uncompilable", kind: "ontology", body: uncompilableSourceBody(t), wantError: "compile installed ontology source"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "valid-json-uncompilable" {
				source, err := service.DecodeOntologySource(tc.body)
				if err != nil {
					t.Fatalf("semantic refusal fixture failed decoding: %v", err)
				}
				if _, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins); len(problems) == 0 {
					t.Fatal("semantic refusal fixture unexpectedly compiled")
				}
			}
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
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("startup refused through the wrong boundary: %v; want %s", err, tc.wantError)
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

func openConfiguredRuntime(t *testing.T, configure func(*Config)) *Runtime {
	t.Helper()
	store := seedConfiguredStore(t, "ontology", configuredSourceBody(t))
	config := Config{
		Driver: "memory", Project: "b03-runtime", Address: DefaultAddress,
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil },
		Ontology:     &OntologyConfig{Source: service.StaticOntologySource(configuredOntologyRef)},
	}
	configure(&config)
	runtime, err := Open(context.Background(), config)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return runtime
}

func runtimeRefusal(t *testing.T, transport *inprocess.Transport, method string, params map[string]any, want string) {
	t.Helper()
	params["_meta"] = map[string]any{"project": "b03-runtime", "idempotencyKey": "000000000000000000000000000000ff"}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Error *struct {
			Data struct {
				Code string `json:"code"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil || envelope.Error == nil || envelope.Error.Data.Code != want {
		t.Fatalf("%s refusal=%s err=%v want=%s", method, response, err, want)
	}
}

func TestConfiguredEventReaderDoesNotAdvertiseMissingAuthorization(t *testing.T) {
	runtime := openConfiguredRuntime(t, func(config *Config) {
		config.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
	})
	runtimeRefusal(t, inprocess.New(runtime.RPC), "event.list", map[string]any{"filter": map[string]any{}, "limit": 100}, "method_not_found")
}

// This port is a trusted test policy, not a claim of native/AZ9 authorization.
type configuredEventPolicy struct {
	denyOrdinary    bool
	denyResource    string
	ordinaryReads   int
	deniedResources int
}

func (p *configuredEventPolicy) Read(_ context.Context, _ model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	if event.Type != model.EventDependencyConditionUnmet {
		p.ordinaryReads++
		if p.denyOrdinary {
			return coreevents.Visibility{}, nil
		}
	}
	return coreevents.Visibility{Readable: true, Before: event.Before, After: event.After}, nil
}

func (p *configuredEventPolicy) ReadResource(_ context.Context, _ model.PrincipalRef, resource model.AuditResource) (coreevents.ResourceVisibility, error) {
	if resource.Ref.ID == p.denyResource {
		p.deniedResources++
		return coreevents.ResourceVisibility{}, nil
	}
	return coreevents.ResourceVisibility{Readable: true}, nil
}

func TestConfiguredEventReaderHonorsOrdinaryAndConditionResourceDenial(t *testing.T) {
	policy := &configuredEventPolicy{}
	runtime := openConfiguredRuntime(t, func(config *Config) {
		config.EventAuthorization = policy
		config.Ontology.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error { return nil }
	})
	transport := inprocess.New(runtime.RPC)
	create := func(key, title string) model.Ref {
		raw := runtimeJSONCall(t, transport, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}}, "b03-runtime", key)
		var commit model.Commit
		if err := json.Unmarshal(raw, &commit); err != nil || len(commit.Results) != 1 {
			t.Fatalf("create=%s err=%v", raw, err)
		}
		return commit.Results[0].Ref
	}
	blocker := create("000000000000000000000000000000d1", "policy blocker")
	dependent := create("000000000000000000000000000000d2", "policy dependent")
	runtimeJSONCall(t, transport, "bead.link", map[string]any{"from": dependent, "to": blocker, "kind": "conditional-blocks"}, "b03-runtime", "000000000000000000000000000000d3")
	runtimeJSONCall(t, transport, "bead.close", map[string]any{"id": blocker}, "b03-runtime", "000000000000000000000000000000d4")

	policy.denyOrdinary = true
	if runtimeEventListContains(t, transport, "b03-runtime", string(model.EventBeadCreated), blocker.ID) || policy.ordinaryReads == 0 {
		t.Fatal("event.list leaked an ordinary event or bypassed its current read policy")
	}
	policy.denyOrdinary = false
	if !runtimeEventListContains(t, transport, "b03-runtime", string(model.EventBeadCreated), blocker.ID) {
		t.Fatal("current read permission did not reveal the existing ordinary event")
	}

	policy.denyResource = configuredOntologyRef.ID
	if runtimeEventListContains(t, transport, "b03-runtime", string(model.EventDependencyConditionUnmet), dependent.ID) || policy.deniedResources == 0 {
		t.Fatal("event.list leaked a condition notice or bypassed referenced-resource authorization")
	}
	policy.denyResource = ""
	if !runtimeEventListContains(t, transport, "b03-runtime", string(model.EventDependencyConditionUnmet), dependent.ID) {
		t.Fatal("current resource authorization did not reveal the pinned existing notice")
	}
}

type configuredAdmittedDispatcher struct {
	t       *testing.T
	runtime *Runtime
	token   string
}

func (d configuredAdmittedDispatcher) Dispatch(ctx context.Context, request []byte) []byte {
	identity, err := d.runtime.Authenticator.Admit(ctx, d.token)
	if err != nil {
		d.t.Fatalf("configured authenticator refused test credential: %v", err)
	}
	return d.runtime.RPC.Dispatch(rpc.WithPrincipal(ctx, identity.Principal()), request)
}

func configuredAuthenticatedTransport(t *testing.T, runtime *Runtime, keys *authnapikey.Provider) *inprocess.Transport {
	t.Helper()
	issued, err := keys.Issue(model.PrincipalRef{Kind: "user", ID: "user:b03-reader"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return inprocess.New(configuredAdmittedDispatcher{t: t, runtime: runtime, token: issued.Token})
}

func TestConfiguredOntologyInstalledAuthorityRequiresReadAdapterAndDisablesApply(t *testing.T) {
	t.Run("missing-current-read", func(t *testing.T) {
		store := seedConfiguredStore(t, "ontology", configuredSourceBody(t))
		_, err := Open(context.Background(), Config{
			Driver: "memory", Project: "b03-runtime", Address: DefaultAddress,
			StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil },
			Ontology:     &OntologyConfig{Source: service.StaticOntologySource(configuredOntologyRef)},
			Auth:         authn.Config{AuthorizationInstalled: func() bool { return true }},
		})
		if err == nil || !strings.Contains(err.Error(), "requires a current read authorizer") {
			_ = store.Close()
			t.Fatalf("missing installed authority error=%v", err)
		}
		if _, err := store.Meta(context.Background()); err == nil {
			t.Fatal("authority startup refusal did not close store")
		}
	})
	t.Run("installed-read-only", func(t *testing.T) {
		installed := func() bool { return true }
		keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: installed})
		runtime := openConfiguredRuntime(t, func(config *Config) {
			config.AuthMode = "authenticated"
			config.Auth = authn.Config{Provider: keys, AuthorizationInstalled: installed}
			config.Ontology.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error {
				return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
			}
		})
		transport := configuredAuthenticatedTransport(t, runtime, keys)
		runtimeRefusal(t, transport, "ontology.get", map[string]any{}, "forbidden")
		runtimeRefusal(t, transport, "ontology.apply", map[string]any{}, "method_not_found")
		runtimeRefusal(t, transport, "event.list", map[string]any{"filter": map[string]any{}, "limit": 100}, "method_not_found")
	})
	t.Run("later-installed-authority", func(t *testing.T) {
		installed := false
		current := func() bool { return installed }
		keys := authnapikey.New(authnapikey.Config{AuthorizationInstalled: current})
		runtime := openConfiguredRuntime(t, func(config *Config) {
			config.AuthMode = "authenticated"
			config.Auth = authn.Config{Provider: keys, AuthorizationInstalled: current}
		})
		transport := configuredAuthenticatedTransport(t, runtime, keys)
		runtimeJSONCall(t, transport, "ontology.get", nil, "b03-runtime", "000000000000000000000000000000e1")
		runtimeRefusal(t, transport, "ontology.apply", map[string]any{}, "method_not_found")
		// Commit an actual ordinary event before changing the installed marker.
		runtimeJSONCall(t, transport, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "before policy install"}}, "b03-runtime", "000000000000000000000000000000e2")
		installed = true
		runtimeRefusal(t, transport, "ontology.get", map[string]any{}, "forbidden")
		runtimeRefusal(t, transport, "event.list", map[string]any{"filter": map[string]any{}, "limit": 100}, "authz_postfilter_cap")
	})
}

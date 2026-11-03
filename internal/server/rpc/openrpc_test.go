package rpc_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

//go:embed openrpc.json
var generatedOpenRPC []byte

func TestGeneratedOpenRPCDeterminism(t *testing.T) {
	s := server(t, registry(t), rpc.Config{})
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	first, err := s.OpenRPC(context.Background(), by)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.OpenRPC(context.Background(), by)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("generator is nondeterministic")
	}
	if !bytes.Equal(first, generatedOpenRPC) {
		t.Fatal("OpenRPC artifact stale; run bazel run //internal/server/rpc/cmd/openrpcgen -- -out <checkout>/internal/server/rpc/openrpc.json")
	}
	var doc struct {
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err = json.Unmarshal(first, &doc); err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, m := range doc.Methods {
		names = append(names, m.Name)
	}
	if !reflect.DeepEqual(names, []string{"rpc.describe", "rpc.discover", "rpc.methods", "server.hello"}) {
		t.Fatal("artifact advertises unsupported handlers", names)
	}
}

// These are executable requests with independent domain expectations, rather
// than a copy of the binder's method-name list. Removing a binding must fail
// its request instead of silently reclassifying it as an intended refusal.
func catalogServiceRequests() map[string]struct{ params, domain string } {
	return map[string]struct{ params, domain string }{
		"bead.get":       {`{"id":{"id":"p-1"},"commentPage":{}}`, "get"},
		"bead.ready":     {`{}`, "ready"},
		"bead.create":    {`{"type":"task","fields":{}}`, "create"},
		"bead.update":    {`{"id":{"id":"p-1"},"fields":{}}`, "update"},
		"bead.claim":     {`{"id":{"id":"p-1"}}`, "claim"},
		"bead.claimNext": {`{}`, "claimNext"},
		"bead.comment":   {`{"id":{"id":"p-1"},"body":"conformance"}`, "comment"},
		"bead.close":     {`{"id":{"id":"p-1"}}`, "close"},
		"bead.reopen":    {`{"id":{"id":"p-1"}}`, "reopen"},
		"bead.defer":     {`{"id":{"id":"p-1"}}`, "defer"},
		"bead.cancel":    {`{"id":{"id":"p-1"}}`, "cancel"},
		"bead.fail":      {`{"id":{"id":"p-1"}}`, "fail"},
		"bead.link":      {`{"from":{"id":"p-1"},"to":{"id":"p-2"},"kind":"blocks"}`, "link"},
		"bead.unlink":    {`{"from":{"id":"p-1"},"to":{"id":"p-2"},"kind":"blocks"}`, "unlink"},
		"tx.apply":       {`{"changeSet":{"ops":[{"kind":"link","from":{"id":"p-1"},"to":{"id":"p-2"},"linkKind":"relates-to"}]}}`, "apply"},
		"history.query":  {`{"asOf":{"revision":1,"incarnation":"inc"}}`, "history"},
	}
}

func catalogBootstrap(t *testing.T) map[string]wire.MethodDesc {
	t.Helper()
	s := server(t, registry(t), rpc.Config{})
	raw := result(t, call(t, s, context.Background(), "rpc.describe", `{}`))["methods"]
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var descriptors []wire.MethodDesc
	if err := json.Unmarshal(encoded, &descriptors); err != nil {
		t.Fatal(err)
	}
	out := map[string]wire.MethodDesc{}
	for _, d := range descriptors {
		out[d.Name] = d
	}
	return out
}

func TestCatalogMethodsBoundOrTypedRefused(t *testing.T) {
	r := registry(t)
	probe := &serviceProbe{failure: &model.Error{Data: model.ErrorData{Code: "busy"}}}
	if err := rpc.BindService(r, probe); err != nil {
		t.Fatal(err)
	}
	fixtures := catalogServiceRequests()
	bound := r.Descriptors()
	if len(bound) != len(fixtures) {
		t.Fatalf("service binding/behavior fixture census differs: %d bindings, %d requests", len(bound), len(fixtures))
	}
	for _, d := range bound {
		if _, ok := fixtures[d.Name]; !ok {
			t.Fatalf("bound method needs an executable domain expectation: %s", d.Name)
		}
	}
	bootstrap := catalogBootstrap(t)
	s := server(t, r, rpc.Config{Incarnation: "inc"})
	ctx := rpc.WithPrincipal(context.Background(), model.PrincipalRef{Kind: "user", ID: "catalog-user"})
	for _, d := range r.Catalog().Descriptors() {
		t.Run(d.Name, func(t *testing.T) {
			params, code := `{}`, "method_not_found"
			fixture, domainBound := fixtures[d.Name]
			_, builtin := bootstrap[d.Name]
			if domainBound {
				var p map[string]any
				if err := json.Unmarshal([]byte(fixture.params), &p); err != nil {
					t.Fatal(err)
				}
				p["_meta"] = map[string]any{"idempotencyKey": "550e8400-e29b-41d4-a716-446655440000"}
				encoded, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				params, code = string(encoded), "busy"
				if err := r.Catalog().ValidateParams(d, encoded); err != nil {
					t.Fatalf("invalid conformance fixture: %v", err)
				}
			}
			probe.method = ""
			reply := call(t, s, ctx, d.Name, params)
			if builtin {
				result(t, reply)
			} else {
				requireCode(t, reply, code)
				if _, ok := reply["result"]; ok || reply["jsonrpc"] != "2.0" || reply["id"] != json.Number("1") {
					t.Fatalf("invalid refusal envelope: %v", reply)
				}
				encoded, err := json.Marshal(reply["error"])
				if err != nil {
					t.Fatal(err)
				}
				var got wire.Error
				if err := json.Unmarshal(encoded, &got); err != nil {
					t.Fatal(err)
				}
				want := r.Catalog().NewError(code, "", nil)
				if err := r.Catalog().ValidateError(&got); err != nil || got.Code != want.Code || got.Message != want.Message || got.Data.Retryable != want.Data.Retryable || got.Data.TraceID == "" {
					t.Fatalf("noncanonical typed refusal: %+v, validation=%v", got, err)
				}
			}
			if domainBound && probe.method != fixture.domain || !domainBound && probe.method != "" {
				t.Fatalf("accidental dispatch: got domain %q, want %q", probe.method, fixture.domain)
			}
			if d.Effect == "event" {
				if err := r.Register(d.Name, func(context.Context, rpc.Call) (any, error) { return nil, nil }); err == nil {
					t.Fatal("server notification accepted as a callable method")
				}
			}
		})
	}
}

func catalogJSONEqual(t *testing.T, label string, a, b any) {
	t.Helper()
	decode := func(value any) any {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := wire.DecodeJSON(encoded)
		if err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	if !reflect.DeepEqual(decode(a), decode(b)) {
		t.Fatalf("%s differs: got=%v want=%v", label, a, b)
	}
}

func sortedCatalogDescriptors(visible map[string]wire.MethodDesc) []wire.MethodDesc {
	names := make([]string, 0, len(visible))
	for name := range visible {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]wire.MethodDesc, 0, len(names))
	for _, name := range names {
		out = append(out, visible[name])
	}
	return out
}

func TestComposedOpenRPCMatchesBoundRegistry(t *testing.T) {
	r := registry(t)
	if err := rpc.BindService(r, &serviceProbe{}); err != nil {
		t.Fatal(err)
	}
	expected := catalogBootstrap(t)
	for _, d := range r.Descriptors() {
		expected[d.Name] = d
	}
	for _, readOnly := range []bool{false, true} {
		name := "all"
		if readOnly {
			name = "read-only"
		}
		t.Run(name, func(t *testing.T) {
			s := server(t, r, rpc.Config{Visible: func(_ context.Context, _ model.PrincipalRef, d wire.MethodDesc) (bool, error) {
				return !readOnly || d.Effect == "read", nil
			}})
			by := model.PrincipalRef{Kind: "user", ID: name}
			ctx := rpc.WithPrincipal(context.Background(), by)
			generated, err := s.OpenRPC(ctx, by)
			if err != nil {
				t.Fatal(err)
			}
			var doc struct {
				Methods []struct {
					Name   string       `json:"name"`
					Params []wire.Param `json:"params"`
					Result struct {
						Schema json.RawMessage `json:"schema"`
					} `json:"result"`
					Errors []struct {
						Ref string `json:"$ref"`
					} `json:"errors"`
					Effect      string `json:"x-effect"`
					Idempotency string `json:"x-idempotency"`
					Permission  string `json:"x-permission"`
					Stability   string `json:"x-stability"`
					AgentCore   bool   `json:"x-agent-core"`
				} `json:"methods"`
				Components json.RawMessage  `json:"components"`
				Errors     []wire.ErrorDesc `json:"x-error-catalog"`
			}
			if err := json.Unmarshal(generated, &doc); err != nil {
				t.Fatal(err)
			}
			visible := map[string]wire.MethodDesc{}
			for n, d := range expected {
				if !readOnly || d.Effect == "read" {
					visible[n] = d
				}
			}
			seen := map[string]bool{}
			for _, m := range doc.Methods {
				d, ok := visible[m.Name]
				if !ok || seen[m.Name] {
					t.Fatalf("unknown/hidden/duplicate generated method %s", m.Name)
				}
				seen[m.Name] = true
				refs := make([]string, 0, len(m.Errors))
				for _, e := range m.Errors {
					refs = append(refs, e.Ref)
				}
				got := wire.MethodDesc{Name: m.Name, Params: m.Params, Result: m.Result.Schema, Errors: refs, Effect: m.Effect, Idempotency: m.Idempotency, Permission: m.Permission, Stability: m.Stability, AgentCore: m.AgentCore}
				catalogJSONEqual(t, m.Name, got, d)
				params, err := json.Marshal(map[string]string{"method": m.Name})
				if err != nil {
					t.Fatal(err)
				}
				catalogJSONEqual(t, "rpc.describe "+m.Name, result(t, call(t, s, ctx, "rpc.describe", string(params)))["method"], d)
			}
			if len(seen) != len(visible) {
				t.Fatalf("generator dropped bound methods: got %d want %d", len(seen), len(visible))
			}
			catalogJSONEqual(t, "components", doc.Components, r.Catalog().ComponentsJSON())
			catalogJSONEqual(t, "error catalog", doc.Errors, r.Catalog().ErrorCatalog())
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(generated, &raw); err != nil {
				t.Fatal(err)
			}
			discovery := result(t, call(t, s, ctx, "rpc.discover", `{}`))
			catalogJSONEqual(t, "rpc.discover methods", discovery["methods"], raw["methods"])
			description := result(t, call(t, s, ctx, "rpc.describe", `{}`))
			descriptionMethods, ok := description["methods"].([]any)
			if !ok {
				t.Fatalf("rpc.describe methods has type %T", description["methods"])
			}
			if len(descriptionMethods) != len(visible) {
				t.Fatalf("rpc.describe method count: got %d want %d", len(descriptionMethods), len(visible))
			}
			described := map[string]bool{}
			for _, rawMethod := range descriptionMethods {
				method, ok := rawMethod.(map[string]any)
				name, nameOK := method["name"].(string)
				if !ok || !nameOK {
					t.Fatalf("rpc.describe returned malformed method: %v", rawMethod)
				}
				if described[name] {
					t.Fatalf("rpc.describe returned duplicate method %s", name)
				}
				if _, expected := visible[name]; !expected {
					t.Fatalf("rpc.describe returned unknown/hidden method %s", name)
				}
				described[name] = true
			}
			if len(described) != len(visible) {
				t.Fatalf("rpc.describe dropped methods: got %d want %d", len(described), len(visible))
			}
			catalogJSONEqual(t, "rpc.describe methods", descriptionMethods, sortedCatalogDescriptors(visible))
			catalogJSONEqual(t, "rpc.describe components", description["components"], doc.Components)
			catalogJSONEqual(t, "rpc.describe errors", description["errors"], doc.Errors)
		})
	}
}

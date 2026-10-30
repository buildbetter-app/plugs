package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestBoundedCatalogParity(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(c.Descriptors()); got != 81 {
		t.Fatalf("method inventory = %d", got)
	}
	if got := len(c.ErrorCatalog()); got != 61 {
		t.Fatalf("error inventory = %d", got)
	}
	if digest(catalogJSON) != "3d623784d20f916a3c7c56f12d1ab14eade0be9aca99a7712355b680dddc9453" {
		t.Fatal("bounded source snapshot changed; reconcile approved contract")
	}
	// These changes affect one of two independent descriptor representations.
	for _, change := range []struct{ before, after string }{
		{`"x-agent-core": true`, `"x-agent-core": false`},
		{`"methodCount": 81`, `"methodCount": 80`},
		{`"nameSha256": "925afc30`, `"nameSha256": "825afc30`},
		{`"stability": "v1"`, `"stability": ""`},
		{`"#/components/errors/RateLimited"`, `"#/components/errors/NotARealError"`},
	} {
		mutated := bytes.Replace(catalogJSON, []byte(change.before), []byte(change.after), 1)
		if bytes.Equal(mutated, catalogJSON) {
			t.Fatalf("mutation did not match %s", change.before)
		}
		if _, err = ParseCatalog(mutated); err == nil {
			t.Errorf("accepted catalog drift: %s", change.before)
		}
	}
	first, _ := c.Descriptor("bead.create")
	first.Params[0].Name = "corrupted"
	first.Result[0] = '!'
	second, _ := c.Descriptor("bead.create")
	if second.Params[0].Name != "type" || !json.Valid(second.Result) {
		t.Fatal("descriptor aliases catalog state")
	}
	generated, err := c.GenerateOpenRPC(c.Descriptors())
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Methods []map[string]json.RawMessage `json:"methods"`
	}
	if err = json.Unmarshal(generated, &document); err != nil {
		t.Fatal(err)
	}
	if len(document.Methods) != 81 {
		t.Fatal("generator dropped inventory methods")
	}
	for _, method := range document.Methods {
		for _, key := range []string{"params", "result", "errors", "x-effect", "x-idempotency", "x-permission", "x-stability", "x-agent-core"} {
			if _, exists := method[key]; !exists {
				t.Fatalf("generator missing %s", key)
			}
		}
		var name string
		_ = json.Unmarshal(method["name"], &name)
		d, ok := c.Descriptor(name)
		if !ok {
			t.Fatal("generated unknown method", name)
		}
		checks := map[string]any{"params": d.Params, "x-effect": d.Effect, "x-idempotency": d.Idempotency, "x-permission": d.Permission, "x-stability": d.Stability, "x-agent-core": d.AgentCore}
		for key, expected := range checks {
			encoded, _ := json.Marshal(expected)
			a, _ := DecodeJSON(encoded)
			b, _ := DecodeJSON(method[key])
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("generated descriptor drift %s.%s", name, key)
			}
		}
		var result struct {
			Schema json.RawMessage `json:"schema"`
		}
		_ = json.Unmarshal(method["result"], &result)
		a, _ := DecodeJSON(result.Schema)
		b, _ := DecodeJSON(d.Result)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("generated result schema drift: %s", name)
		}
		var errors []struct {
			Ref string `json:"$ref"`
		}
		_ = json.Unmarshal(method["errors"], &errors)
		refs := make([]string, 0, len(errors))
		for _, e := range errors {
			refs = append(refs, e.Ref)
		}
		if !reflect.DeepEqual(refs, d.Errors) {
			t.Fatalf("generated error attachment drift: %s", name)
		}
	}
	if _, err = c.GenerateOpenRPC([]MethodDesc{{Name: "incomplete"}}); err == nil {
		t.Fatal("generator accepted incomplete descriptor")
	}
}

func TestDecidedSessionMethodsCannotDisappearTogether(t *testing.T) {
	for _, missing := range []string{"session.open", "session.close", "presence.set"} {
		t.Run(missing, func(t *testing.T) {
			value, err := DecodeJSON(catalogJSON)
			if err != nil {
				t.Fatal(err)
			}
			doc := value.(map[string]any)
			registry := doc["x-method-registry"].(map[string]any)
			without := func(items []any) []any {
				var kept []any
				for _, item := range items {
					if item.(map[string]any)["name"] != missing {
						kept = append(kept, item)
					}
				}
				return kept
			}
			doc["methods"] = without(doc["methods"].([]any))
			entries := without(registry["methods"].([]any))
			registry["methods"] = entries
			registry["methodCount"] = len(entries)
			raw, err := json.Marshal(entries)
			if err != nil {
				t.Fatal(err)
			}
			registry["descriptorSha256"] = digest(raw)
			var names bytes.Buffer
			for _, item := range entries {
				names.WriteString(item.(map[string]any)["name"].(string) + "\n")
			}
			registry["nameSha256"] = digest(names.Bytes())
			raw, err = json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = ParseCatalog(raw); err == nil {
				t.Fatal("mutually omitted decided method was accepted after recomputing both inventory digests")
			}
		})
	}
}

func TestStrictParameters(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, params string
		valid          bool
	}{
		{"bead.create", `{"type":"issue","fields":{"score":9007199254740993}}`, true},
		{"bead.create", `{"type":"issue","fields":{},"parent":{"id":"","branch":1e1000000000}}`, true},
		{"bead.get", `{"id":{"key":""},"asOf":{"revision":1.0e1000000000}}`, true},
		{"bead.ready", `{"limit":1.0e3,"filter":{"types":["issue"]}}`, true},
		{"session.close", `{"sessionId":"session-1"}`, true},
		{"session.close", `{}`, false},
		{"session.close", `{"sessionId":"session-1","unexpected":true}`, false},
		{"history.redact", `{"ref":{"id":"p-1"},"paths":["description"],"humanPresence":{"mode":"presence_check","challenge":"test","proof":"test"}}`, true},
		{"history.redact", `{"ref":{"id":"p-1"},"paths":[],"humanPresence":{"mode":"presence_check","challenge":"test","proof":"test"}}`, false},
		{"bead.ready", `{"filter":{"unknown":true}}`, false},
		{"bead.ready", `{"limit":1001}`, false},
		{"bead.ready", `{"limit":0}`, false},
		{"bead.ready", `{"limit":1.1}`, false},
		{"bead.ready", `{"limit":1e1000000000}`, false},
		{"bead.create", `{"type":"issue"}`, false},
		{"bead.create", `{"type":null,"fields":{}}`, false},
		{"bead.get", `{"id":"pl-1"}`, false},
		{"bead.get", `{"id":{"id":"pl-1","key":"k"}}`, false},
		{"bead.get", `{"id":{"key":"k","branch":0}}`, false},
		{"bead.get", `{"id":{"id":"pl-1"},"asOf":{"revision":-1}}`, false},
		{"bead.get", `{"id":{"id":"pl-1"},"asOf":{"revision":null}}`, false},
		{"rpc.methods", `{"_meta":{"deadlineMs":1e1000000000,"agent":"cli"}}`, true},
		{"rpc.methods", `{"_meta":{"deadlineMs":0}}`, false},
		{"rpc.methods", `{"_meta":{"return":"brief"}}`, false},
		{"rpc.methods", `{"_meta":{"principal":{"kind":"user","id":"admin"}}}`, false},
		{"rpc.methods", `{"_meta":{"replayed":false}}`, false},
		{"bead.claim", `{"id":{"id":"p-1"},"_meta":{"sameClaim":true,"idempotencyKey":"550e8400-e29b-41d4-a716-446655440000"}}`, false},
		{"rpc.methods", `{"_meta":{"agent":"a","agent":"b"}}`, false},
		{"rpc.methods", `null`, false},
		{"rpc.methods", `[]`, false},
	} {
		t.Run(tc.method+"/"+tc.params, func(t *testing.T) {
			d, _ := c.Descriptor(tc.method)
			err := c.ValidateParams(d, []byte(tc.params))
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
		})
	}
}

func TestCanonicalErrorMapping(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range c.ErrorCatalog() {
		e := c.MapError(&model.Error{Message: "private implementation details", Data: model.ErrorData{Code: model.ErrorCode(d.Code), Retryable: !d.Retryable}})
		if d.Audience == "internal" || d.Audience == "plugin-host" || oneOf(d.Code, "subscriber_skipped", "post_subscriber_failed", "ontology_drifted", "visibility_lost") {
			if e.Data.Code != "internal_error" {
				t.Errorf("leaked %s", d.Code)
			}
			continue
		}
		if e.Code != d.JSONRPC || e.Data.Retryable != d.Retryable || strings.Contains(e.Message, "private") {
			t.Errorf("noncanonical %s: %+v", d.Code, e)
		}
	}
	for _, err := range []error{fmt.Errorf("database secret"), &model.Error{Data: model.ErrorData{Code: "made_up"}}} {
		e := c.MapError(err)
		if e.Data.Code != "internal_error" || strings.Contains(e.Error(), "secret") {
			t.Fatalf("leaked unknown failure: %v", e)
		}
	}
	e := c.NewError("lease_not_held", "", map[string]any{"expectedEpoch": model.Natural("1e1000000000"), "observedEpoch": model.Natural64(2)})
	if err = c.ValidateError(e); err != nil {
		t.Fatal(err)
	}
	e.Data.Details = map[string]any{"expectedEpoch": -1, "observedEpoch": 2}
	if err = c.ValidateError(e); err == nil {
		t.Fatal("accepted invalid typed details")
	}
}

func TestIdempotencyKeyEncodings(t *testing.T) {
	for _, key := range []string{"550e8400-e29b-41d4-a716-446655440000", "01JB7W3K9QX4T2M8V6RZ0YDC5H", "3fc5d89aba21452eafee4ab934e08632", "RBPn_NJiNEbovU2zokqvcA"} {
		if err := ValidateIdempotencyKey(key); err != nil {
			t.Errorf("%s: %v", key, err)
		}
	}
	for _, key := range []string{"", "w7-1", "sixteencharacters", "550e8400-e29b-11d4-a716-446655440000", "01JB7W3K9QX4T2M8V6RZ0YDC5I"} {
		if err := ValidateIdempotencyKey(key); err == nil {
			t.Errorf("accepted %q", key)
		}
	}
}

func TestJSONBoundary(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`{"a":1,"a":2}`), []byte(`{} {}`), []byte{34, 255, 34}, []byte(strings.Repeat("[", 130) + "0" + strings.Repeat("]", 130))} {
		if _, err := DecodeJSON(raw); err == nil {
			t.Fatalf("accepted invalid boundary: %q", raw)
		}
	}
	v, err := DecodeJSON([]byte(`{"n":9007199254740993}`))
	if err != nil || v.(map[string]any)["n"] != json.Number("9007199254740993") {
		t.Fatal("number precision lost")
	}
}

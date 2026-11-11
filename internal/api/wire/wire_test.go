package wire

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestBoundedCatalogParity(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(c.Descriptors()); got != 87 {
		t.Fatalf("method inventory = %d", got)
	}
	if _, ok := c.Descriptor("bead.comments"); ok {
		t.Fatal("comment listing must use the selected bead.get projection")
	}
	if got := len(c.ErrorCatalog()); got != 63 {
		t.Fatalf("error inventory = %d", got)
	}
	if digest(catalogJSON) != "9344b4e28e64cda7e25f7c7b5f92d540a05a7df1851d32b258b3510f86197387" {
		t.Fatal("bounded source snapshot changed; reconcile approved contract")
	}
	// These changes affect one of two independent descriptor representations.
	for _, change := range []struct{ before, after string }{
		{`"x-agent-core": true`, `"x-agent-core": false`},
		{`"methodCount": 87`, `"methodCount": 86`},
		{`"nameSha256": "50abe444`, `"nameSha256": "f4cc1165`},
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
	if len(document.Methods) != 87 {
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

func TestWorkflowTerminalClaimSchemaPreservesLiveLeaseRequirements(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	terminal := `{"revision":0,"results":[{"ref":{"id":"workflow:item/one","branch":0},"revision":3}],"warnings":[]}`
	leased := `{"revision":1,"results":[{"ref":{"id":"pl-one"},"revision":1}],"warnings":[],"leaseExpiresAt":"2026-11-03T01:00:00Z","leaseTtlMs":1800000}`
	workflowLeased := strings.Replace(strings.Replace(leased, "pl-one", "workflow:item/one", 1), `"revision":1`, `"revision":0`, 1)
	for _, name := range []string{"bead.claim", "bead.claimNext"} {
		method, ok := c.Descriptor(name)
		if !ok {
			t.Fatal("claim descriptor missing", name)
		}
		for _, tc := range []struct {
			name, result string
			valid        bool
		}{
			{"ordinary leased result", leased, true},
			{"live workflow leased result", workflowLeased, true},
			{"terminal workflow identity", terminal, true},
			{"ordinary claim still requires a lease", strings.Replace(terminal, "workflow:item/one", "pl-one", 1), false},
			{"bare prefix is not an item", strings.Replace(terminal, "workflow:item/one", "workflow:item/", 1), false},
			{"terminal branch must be zero", strings.Replace(terminal, `"branch":0`, `"branch":1`, 1), false},
			{"terminal has no core revision", strings.Replace(terminal, `"revision":0`, `"revision":1`, 1), false},
			{"lease TTL stays positive", strings.Replace(leased, `"leaseTtlMs":1800000`, `"leaseTtlMs":0`, 1), false},
			{"lease expiry stays required", strings.Replace(leased, `"leaseExpiresAt":"2026-11-03T01:00:00Z",`, "", 1), false},
			{"workflow lease TTL stays positive", strings.Replace(workflowLeased, `"leaseTtlMs":1800000`, `"leaseTtlMs":0`, 1), false},
			{"workflow lease expiry stays required with TTL", strings.Replace(workflowLeased, `"leaseExpiresAt":"2026-11-03T01:00:00Z",`, "", 1), false},
			{"workflow lease TTL stays required with expiry", strings.Replace(workflowLeased, `,"leaseTtlMs":1800000`, "", 1), false},
			{"terminal cannot retain record payload", strings.Replace(terminal, `"ref":`, `"record":{"title":"private"},"ref":`, 1), false},
			{"terminal has exactly one operation", `{"revision":0,"results":[],"warnings":[]}`, false},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				if err := c.ValidateResult(method, []byte(tc.result)); (err == nil) != tc.valid {
					t.Fatalf("valid=%v result=%s error=%v", tc.valid, tc.result, err)
				}
			})
		}
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
		{"bead.comment", `{"id":{"id":"p-1"},"body":"ok","principal":{"kind":"user","id":"attacker"}}`, false},
		{"bead.comment", `{"id":{"id":"p-1"},"body":"ok","origin":"import"}`, false},
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

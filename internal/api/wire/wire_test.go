package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp/syntax"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestBoundedCatalogParity(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(c.Descriptors()); got != 86 {
		t.Fatalf("method inventory = %d", got)
	}
	if _, ok := c.Descriptor("bead.comments"); ok {
		t.Fatal("comment listing must use the selected bead.get projection")
	}
	if got := len(c.ErrorCatalog()); got != 63 {
		t.Fatalf("error inventory = %d", got)
	}
	if digest(catalogJSON) != "f542d28b3622154798334b0c7d2908a9f83b54bccec88d47c5342527657a02e6" {
		t.Fatal("bounded source snapshot changed; reconcile approved contract")
	}
	// These changes affect one of two independent descriptor representations.
	for _, change := range []struct{ before, after string }{
		{`"x-agent-core": true`, `"x-agent-core": false`},
		{`"methodCount": 86`, `"methodCount": 85`},
		{`"nameSha256": "bedf3c90`, `"nameSha256": "f4cc1165`},
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
	if len(document.Methods) != 86 {
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

func TestB15CommentPageIsSelectedThroughBeadGet(t *testing.T) {
	canonicalPath := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "engdocs/design/plug/contracts/openrpc.json")
	canonical, err := os.ReadFile(canonicalPath)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"canonical": canonical, "embedded": catalogJSON} {
		t.Run(name, func(t *testing.T) {
			c, err := ParseCatalog(raw)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := c.Descriptor("bead.comments"); ok {
				t.Fatal("comment listing escaped the adopted bead.get surface")
			}
			get, ok := c.Descriptor("bead.get")
			if !ok {
				t.Fatal("bead.get descriptor missing")
			}
			for _, params := range []string{
				`{"id":{"id":"p-1"}}`,
				`{"id":{"id":"p-1"},"commentPage":{}}`,
				`{"id":{"id":"p-1"},"commentPage":{"limit":1}}`,
				`{"id":{"id":"p-1"},"commentPage":{"limit":500,"cursor":"opaque"}}`,
			} {
				if err := c.ValidateParams(get, []byte(params)); err != nil {
					t.Fatalf("valid comment selection refused: %s: %v", params, err)
				}
			}
			for _, params := range []string{
				`{"id":{"id":"p-1"},"commentPage":null}`,
				`{"id":{"id":"p-1"},"commentPage":{"limit":0}}`,
				`{"id":{"id":"p-1"},"commentPage":{"limit":501}}`,
				`{"id":{"id":"p-1"},"commentPage":{"cursor":1}}`,
				`{"id":{"id":"p-1"},"commentPage":{"unknown":true}}`,
				`{"id":{"id":"p-1"},"limit":1}`,
			} {
				if err := c.ValidateParams(get, []byte(params)); err == nil {
					t.Fatalf("invalid comment selection admitted: %s", params)
				}
			}
		})
	}
}

func TestWorkflowInvocationResultSchemasSeparatePermanentAndEphemeral(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	method, ok := c.Descriptor("x.workflow.invoke")
	if !ok {
		t.Fatal("workflow invocation descriptor is missing")
	}
	permanent := `{"id":"pl-ordinary","runId":"","label":"one","title":"ordinary","type":"task","project":"project","status":"open","plane":"permanent","kind":"bead","plugin":"","revision":1}`
	ephemeral := `{"id":"workflow:item/one","runId":"workflow:run/one","label":"one","title":"temporary","type":"task","project":"project","status":"open","plane":"ephemeral","kind":"workflow.item","plugin":"workflow","revision":0}`
	run := `{"id":"workflow:run/one","mode":"ephemeral","state":"active"}`
	for _, tc := range []struct {
		name, result string
		valid        bool
	}{
		{"ordinary permanent bead", `{"plane":"permanent","items":[` + permanent + `]}`, true},
		{"live ephemeral item", `{"plane":"ephemeral","run":` + run + `,"items":[` + ephemeral + `]}`, true},
		{"terminal identity only", `{"plane":"ephemeral","run":{"id":"workflow:run/one","mode":"ephemeral","state":"completed"},"items":[]}`, true},
		{"permanent cannot carry ephemeral items", `{"plane":"permanent","items":[` + ephemeral + `]}`, false},
		{"ephemeral cannot carry ordinary beads", `{"plane":"ephemeral","run":` + run + `,"items":[` + permanent + `]}`, false},
		{"ephemeral requires a run", `{"plane":"ephemeral","items":[` + ephemeral + `]}`, false},
		{"permanent has no plugin run", `{"plane":"permanent","run":` + run + `,"items":[` + permanent + `]}`, false},
		{"ephemeral item ID is typed", `{"plane":"ephemeral","run":` + run + `,"items":[` + strings.Replace(ephemeral, "workflow:item/one", "pl-ordinary", 1) + `]}`, false},
		{"ephemeral run ID is typed", `{"plane":"ephemeral","run":` + strings.Replace(run, "workflow:run/one", "pl-ordinary", 1) + `,"items":[]}`, false},
		{"ephemeral plugin is bound", `{"plane":"ephemeral","run":` + run + `,"items":[` + strings.Replace(ephemeral, `"plugin":"workflow"`, `"plugin":"other"`, 1) + `]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := c.ValidateResult(method, []byte(tc.result)); (err == nil) != tc.valid {
				t.Fatalf("valid=%v result=%s error=%v", tc.valid, tc.result, err)
			}
		})
	}
}

func TestWorkflowSchemasMatchCanonicalOpenRPC(t *testing.T) {
	path := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "engdocs/design/plug/contracts/openrpc.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	schemas := canonical.(map[string]any)["components"].(map[string]any)["schemas"].(map[string]any)
	embedded := c.root["components"].(map[string]any)["schemas"].(map[string]any)
	for _, name := range []string{"ClaimResult", "WorkflowClaimReceipt", "WorkflowItem", "WorkflowInvokeResult", "WorkflowRun"} {
		if schemas[name] == nil || !reflect.DeepEqual(schemas[name], embedded[name]) {
			t.Fatalf("canonical/embedded workflow schema drift: %s", name)
		}
	}
}

func TestStringSchemaPatternsFailClosed(t *testing.T) {
	c := &Catalog{}
	for _, tc := range []struct {
		name, pattern, value, errorText string
	}{
		{"typed ID matches", `^workflow:item/.`, "workflow:item/one", ""},
		{"ordinary ID fails", `^workflow:item/.`, "pl-one", "does not match pattern"},
		{"empty suffix fails", `^workflow:item/.`, "workflow:item/", "does not match pattern"},
		{"unanchored JSON Schema search", `item`, "workflow:item/one", ""},
		{"anchored prefix rejects embedded match", `^workflow:item/.`, "other/workflow:item/one", "does not match pattern"},
		{"empty pattern matches", ``, "anything", ""},
		{"invalid pattern fails closed", `[`, "workflow:item/one", "invalid schema pattern"},
	} {
		for _, request := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/request=%v", tc.name, request), func(t *testing.T) {
				schema, err := json.Marshal(map[string]any{"type": "string", "pattern": tc.pattern})
				if err != nil {
					t.Fatal(err)
				}
				method := MethodDesc{Params: []Param{{Name: "value", Required: true, Schema: schema}}, Result: schema}
				path := "result"
				if request {
					raw, _ := json.Marshal(map[string]any{"value": tc.value})
					err = c.ValidateParams(method, raw)
					path = "params.value"
				} else {
					raw, _ := json.Marshal(tc.value)
					err = c.ValidateResult(method, raw)
				}
				if tc.errorText == "" {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				if err == nil || !strings.Contains(err.Error(), tc.errorText) {
					t.Fatalf("pattern validation error=%v, want %q", err, tc.errorText)
				}
				var patternError *StringPatternError
				if !errors.As(err, &patternError) || patternError.Path != path {
					t.Fatalf("pattern validation error=%v, want typed error at %s", err, path)
				}
				var invalidPattern *syntax.Error
				if errors.As(err, &invalidPattern) != (tc.errorText == "invalid schema pattern") {
					t.Fatalf("pattern error cause=%v, invalid pattern=%v", patternError.Cause, invalidPattern)
				}
			})
		}
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

func TestDirectBlockerContractMatchesCanonicalOpenRPC(t *testing.T) {
	if os.Getenv("TEST_SRCDIR") == "" || os.Getenv("TEST_WORKSPACE") == "" {
		t.Skip("canonical OpenRPC comparison runs under Bazel with the contract runfile")
	}
	canonicalPath := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "engdocs/design/plug/contracts/openrpc.json")
	canonicalBytes, err := os.ReadFile(canonicalPath)
	if err != nil {
		t.Fatalf("read canonical OpenRPC runfile %s: %v", canonicalPath, err)
	}
	var canonical, embedded map[string]any
	if err := json.Unmarshal(canonicalBytes, &canonical); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(catalogJSON, &embedded); err != nil {
		t.Fatal(err)
	}
	for name, doc := range map[string]map[string]any{"canonical": canonical, "embedded": embedded} {
		t.Run(name, func(t *testing.T) {
			components := doc["components"].(map[string]any)
			schemas := components["schemas"].(map[string]any)
			result := schemas["ViewExplainResult"].(map[string]any)
			properties := result["properties"].(map[string]any)
			if properties["directBlockers"].(map[string]any)["$ref"] != "#/components/schemas/DirectBlockerResult" {
				t.Fatal("ViewExplainResult directBlockers reference drift")
			}
			edge := schemas["DirectBlockerEdge"].(map[string]any)
			kinds := edge["properties"].(map[string]any)["kind"].(map[string]any)["enum"]
			if !reflect.DeepEqual(kinds, []any{"blocks", "conditional-blocks", "waits-for"}) {
				t.Fatalf("direct blocker kinds drift: %v", kinds)
			}
			if !reflect.DeepEqual(edge["required"], []any{"blocker", "kind", "state"}) || edge["additionalProperties"] != false {
				t.Fatalf("edge schema is not closed/required: %+v", edge)
			}
			resultSchema := schemas["DirectBlockerResult"].(map[string]any)
			if !reflect.DeepEqual(resultSchema["required"], []any{"edges", "hiddenCount"}) || resultSchema["additionalProperties"] != false {
				t.Fatalf("result schema is not closed/required: %+v", resultSchema)
			}
			rp := resultSchema["properties"].(map[string]any)
			if rp["edges"].(map[string]any)["maxItems"] != float64(100) || rp["hiddenCount"].(map[string]any)["maximum"] != float64(100) {
				t.Fatalf("direct blocker bounds drift: %+v", rp)
			}
			var explain map[string]any
			for _, raw := range doc["methods"].([]any) {
				method := raw.(map[string]any)
				if method["name"] == "view.explain" {
					explain = method
					break
				}
			}
			if explain == nil {
				t.Fatal("view.explain missing")
			}
			foundClosure := false
			for _, raw := range explain["errors"].([]any) {
				if raw.(map[string]any)["$ref"] == "#/components/errors/ClosureTooLarge" {
					foundClosure = true
				}
			}
			if !foundClosure {
				t.Fatal("view.explain missing closure_too_large attachment")
			}
		})
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

func TestDisplayNameContract(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	open, _ := c.Descriptor("session.open")
	found := false
	for _, p := range open.Params {
		if p.Name == "displayName" {
			found = true
			if p.Required || string(p.Schema) != `{"$ref":"#/components/schemas/DisplayNameInput"}` {
				t.Fatalf("displayName must be an optional input reference: %+v", p)
			}
		}
	}
	if !found {
		t.Fatal("session.open has no displayName input")
	}
	if !oneOf("#/components/errors/InvalidParams", open.Errors...) {
		t.Fatal("displayName normalization must declare invalid_params")
	}
	// There is no session refresh method in this inventory. presence.set
	// renews idle expiry but keeps the name chosen by session.open.
	for _, d := range c.Descriptors() {
		if strings.HasPrefix(d.Name, "session.") && !oneOf(d.Name, "session.open", "session.close") {
			t.Fatalf("unexpected session method %s", d.Name)
		}
		for _, p := range d.Params {
			if d.Name != "session.open" && p.Name == "displayName" {
				t.Fatalf("displayName added to foreign input %s", d.Name)
			}
		}
	}
	input, err := c.resolve("#/components/schemas/DisplayNameInput")
	if err != nil {
		t.Fatal(err)
	}
	in := input.(map[string]any)
	if in["type"] != "string" || in["minLength"] != nil || in["maxLength"] != nil {
		t.Fatal("raw input must allow empty and padded strings; bounds apply after trimming")
	}
	normalized, err := c.resolve("#/components/schemas/DisplayName")
	if err != nil {
		t.Fatal(err)
	}
	out := normalized.(map[string]any)
	if out["type"] != "string" || out["minLength"] != json.Number("1") || out["maxLength"] != json.Number("64") {
		t.Fatal("normalized displayName must contain 1..64 Unicode scalar values")
	}
	presence, err := c.resolve("#/components/schemas/Presence")
	if err != nil {
		t.Fatal(err)
	}
	s := presence.(map[string]any)
	properties := s["properties"].(map[string]any)
	if !reflect.DeepEqual(properties["displayName"], map[string]any{"$ref": "#/components/schemas/DisplayName"}) || s["additionalProperties"] != false {
		t.Fatal("Presence must remain closed with a dedicated displayName")
	}
	if !reflect.DeepEqual(s["required"], []any{"sessionId", "principal"}) {
		t.Fatal("displayName changed required presence identity")
	}
	// These annotations are normative consumer requirements, not executable
	// normalization in the schema validator. Pin them without claiming that
	// type validation trims, rejects categories or implements trust separation.
	contract, ok := c.root["x-display-name-contract"].(map[string]any)
	if !ok {
		t.Fatal("missing normative displayName contract")
	}
	expected, err := DecodeJSON([]byte(`{
		"trim": "Unicode White_Space",
		"rejectCategories": ["Cc", "Cf"],
		"maxUnicodeScalarsAfterTrim": 64,
		"emptyAfterTrim": "absent",
		"error": "invalid_params"
	}`))
	if err != nil || !reflect.DeepEqual(contract["normalization"], expected) {
		t.Fatal("displayName normalization contract drift")
	}
	if contract["trust"] != "untrusted display-only" || !reflect.DeepEqual(contract["neverUsedFor"], []any{"authorization", "attribution", "auditActor", "idempotency", "leases", "dedup"}) {
		t.Fatal("displayName must not become trusted identity or an ownership key")
	}
	if contract["render"] != "next to authenticated principal: user name, API key name, or anonymous; never replaces or impersonates the principal" || contract["members"] != "two sessions of the same principal remain separate members identified by sessionId; never merge labels into a trusted group identity" {
		t.Fatal("displayName principal rendering or per-session identity contract drift")
	}
}

func TestRestoreImportCommentContract(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	comment, err := c.resolve("#/components/schemas/Comment")
	if err != nil {
		t.Fatal(err)
	}
	check := func(name, raw string, valid bool) {
		t.Run(name, func(t *testing.T) {
			value, err := DecodeJSON([]byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			err = c.validate(comment, value, "comment", false, 0)
			if (err == nil) != valid {
				t.Fatalf("valid=%v: %v", valid, err)
			}
		})
	}
	principal := `"principal":{"kind":"user","id":"restore-admin"}`
	check("legacy shape remains valid", `{"body":"old",`+principal+`,"time":"2026-10-30T19:05:00Z"}`, true)
	check("imported shape", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceAuthor":"Ada","sourceTime":"2024-01-02T03:04:05Z"}`, true)
	check("imported source author omitted", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceTime":"2024-01-02T03:04:05Z"}`, true)
	check("import origin requires source time", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import"}`, false)
	check("wrong origin refused", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"manual","sourceTime":"2024-01-02T03:04:05Z"}`, false)
	check("unknown trusted field refused", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceTime":"2024-01-02T03:04:05Z","actor":"forged"}`, false)
	check("source author max scalar bound", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceAuthor":"`+strings.Repeat("a", 257)+`","sourceTime":"2024-01-02T03:04:05Z"}`, false)
	check("source author 256 scalars", `{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceAuthor":"`+strings.Repeat("a", 256)+`","sourceTime":"2024-01-02T03:04:05Z"}`, true)

	result, _ := c.Descriptor("bead.get")
	if err = c.ValidateResult(result, []byte(`{"bead":{"id":"pl-1","type":"issue","revision":1},"attachments":[],"comments":[{"body":"imported",`+principal+`,"time":"2026-10-30T19:06:00Z","origin":"import","sourceAuthor":"Ada","sourceTime":"2024-01-02T03:04:05Z"}]}`)); err != nil {
		t.Fatalf("bead.get must expose optional typed comments: %v", err)
	}
	if err = c.ValidateResult(result, []byte(`{"bead":{"id":"pl-1","type":"issue","revision":1},"attachments":[],"comments":[{"body":"old",`+principal+`,"time":"2026-10-30T19:05:00Z"}]}`)); err != nil {
		t.Fatalf("bead.get must retain legacy comments: %v", err)
	}
	if err = c.ValidateResult(result, []byte(`{"bead":{"id":"pl-1","type":"issue","revision":1},"attachments":[],"comments":[{"body":"x",`+principal+`,"time":"2026-10-30T19:05:00Z","origin":"import","sourceTime":"2024-01-02T03:04:05Z","principalOverride":"attacker"}]}`)); err == nil {
		t.Fatal("bead.get accepted an unknown imported-comment field")
	}

	contract, ok := c.root["x-restore-import-contract"].(map[string]any)
	if !ok {
		t.Fatal("missing restore-import contract extension")
	}
	ordinary, ok := contract["ordinaryInputs"].(map[string]any)
	if !ok || ordinary["commentOp"] != "closed to kind, target and body" {
		t.Fatal("ordinary comment input boundary is not pinned")
	}
	trusted, ok := contract["trustedAdmission"].(map[string]any)
	if !ok || trusted["hostPrivate"] != true {
		t.Fatal("prepared import profile must remain host-private")
	}
	allocatorContract, ok := contract["allocator"].(map[string]any)
	if !ok || allocatorContract["prefixBinding"] != "targetAllocator.prefix must equal sourceAllocator.prefix; mismatch refuses before staging." || allocatorContract["sourcePersistence"] != "Persist sourceAllocator unchanged in the restore receipt and project metadata key plug.restore.sourceAllocator." {
		t.Fatal("allocator provenance/prefix binding contract drift")
	}
}

func TestRestoreImportAllocatorAndProfileContract(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	validate := func(ref, raw string, valid bool) {
		t.Helper()
		schema, err := c.resolve(ref)
		if err != nil {
			t.Fatal(err)
		}
		value, err := DecodeJSON([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		err = c.validate(schema, value, ref, false, 0)
		if (err == nil) != valid {
			t.Fatalf("%s valid=%v: %v", ref, valid, err)
		}
	}
	source := `{"prefix":"pl","id_mode":"hash","hash_length":32,"last_id":0,"child_counters":{"pl-a":2}}`
	target := `{"prefix":"pl","id_mode":"random","random_bytes":16,"suffix_encoding":"lower-hex","top_level_shape":"prefix-random","child_shape":"parent-random"}`
	validate("#/components/schemas/SourceAllocatorProvenance", source, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"hash","hash_length":1,"last_id":0}`, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"hash","hash_length":64,"last_id":0}`, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"hash","hash_length":65,"last_id":0}`, false)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"sequential","hash_length":0,"last_id":0}`, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"sequential","hash_length":64,"last_id":0}`, true)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"sequential","hash_length":65,"last_id":0}`, false)
	validate("#/components/schemas/SourceAllocatorProvenance", `{"prefix":"pl","id_mode":"hash","last_id":0}`, false)
	validate("#/components/schemas/TargetRandomAllocator", target, true)
	validate("#/components/schemas/TargetRandomAllocator", `{"prefix":"pl","id_mode":"hash","random_bytes":16,"suffix_encoding":"lower-hex","top_level_shape":"prefix-random","child_shape":"parent-random"}`, false)
	validate("#/components/schemas/RestoreAllocatorProfile", `{"sourceAllocator":`+source+`,"targetAllocator":`+target+`}`, false)
	hashTarget := `{"prefix":"pl","id_mode":"hash","hash_length":8,"min_hash_length":3,"max_hash_length":8,"max_collision_prob":0.25,"last_id":0,"child_counters":{"pl-a":2}}`
	validate("#/components/schemas/TargetHashAllocator", hashTarget, true)
	validate("#/components/schemas/RestoreAllocatorProfile", `{"sourceAllocator":`+source+`,"targetAllocator":`+hashTarget+`}`, true)
	validate("#/components/schemas/TargetHashAllocator", strings.Replace(hashTarget, `"min_hash_length":3`, `"min_hash_length":2`, 1), false)

	admission := `{"mode":"authenticated","actor":{"kind":"user","id":"admin"},"scope":"","authority":"restore-admin","admissionDigest":"` + strings.Repeat("a", 64) + `"}`
	profile := `{"admission":` + admission + `,"sourceStoreId":"store-1","method":"offline.restore.import","idempotencyKey":"0123456789abcdef","archiveDigest":"` + strings.Repeat("b", 64) + `","derivedDigest":"` + strings.Repeat("c", 64) + `"}`
	validate("#/components/schemas/RestoreImportAdmission", admission, true)
	validate("#/components/schemas/RestoreImportProfile", profile, true)
	validate("#/components/schemas/RestoreImportProfile", `{"admission":`+admission+`,"sourceStoreId":"store-1","method":"admin.restore.finish","idempotencyKey":"0123456789abcdef","archiveDigest":"`+strings.Repeat("b", 64)+`","derivedDigest":"`+strings.Repeat("c", 64)+`"}`, false)
	validate("#/components/schemas/RestoreImportAdmission", `{"mode":"authenticated","actor":{"kind":"user","id":"admin"},"scope":"","authority":"trusted-human-presence","admissionDigest":"`+strings.Repeat("a", 64)+`"}`, false)

	beadGet, _ := c.Descriptor("bead.get")
	if err = c.ValidateResult(beadGet, []byte(`{"bead":{"id":"pl-1","type":"issue","revision":1},"attachments":[],"comments":[]}`)); err != nil {
		t.Fatal("optional comments array must be valid", err)
	}
}

func TestDisplayNameInputSchemaBoundary(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	d, _ := c.Descriptor("session.open")
	for _, tc := range []struct {
		name   string
		params map[string]any
		valid  bool
	}{
		{"omitted", map[string]any{}, true},
		{"empty is normalized by consumer", map[string]any{"displayName": ""}, true},
		{"string", map[string]any{"displayName": "worker"}, true},
		{"padded 64 scalars", map[string]any{"displayName": "\u2003" + strings.Repeat("\U0001f600", 64) + "\u00a0"}, true},
		{"null", map[string]any{"displayName": nil}, false},
		{"number", map[string]any{"displayName": 1}, false},
		{"object", map[string]any{"displayName": map[string]any{"id": "admin"}}, false},
		{"wrong case", map[string]any{"displayname": "worker"}, false},
		{"metadata is closed", map[string]any{"_meta": map[string]any{"displayName": "worker"}}, false},
		{"principal remains server stamped", map[string]any{"displayName": "worker", "principal": map[string]any{"kind": "user", "id": "admin"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.params)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.ValidateParams(d, raw); (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
		})
	}
	// Whitespace trimming, empty-to-absent, Cc/Cf rejection and the input
	// bound after trimming still require real bound-RPC consumer tests.
}

func TestPresenceDisplayNameSchemaBoundary(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	d, _ := c.Descriptor("presence.set")
	for _, tc := range []struct {
		name    string
		value   any
		present bool
		valid   bool
	}{
		{"omitted", nil, false, true},
		{"string", "worker", true, true},
		{"64 ASCII scalars", strings.Repeat("a", 64), true, true},
		{"65 ASCII scalars", strings.Repeat("a", 65), true, false},
		{"64 supplementary scalars", strings.Repeat("\U0001f600", 64), true, true},
		{"65 supplementary scalars", strings.Repeat("\U0001f600", 65), true, false},
		{"64 combining scalars", strings.Repeat("e\u0301", 32), true, true},
		{"66 combining scalars", strings.Repeat("e\u0301", 33), true, false},
		{"empty must be absent", "", true, false},
		{"null", nil, true, false},
		{"boolean", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := map[string]any{"sessionId": "s-1", "principal": map[string]any{"kind": "anonymous", "id": "anonymous"}}
			if tc.present {
				result["displayName"] = tc.value
			}
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.ValidateResult(d, raw); (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
		})
	}
	list, _ := c.Descriptor("presence.list")
	if err = c.ValidateResult(list, []byte(`{"presence":[
		{"sessionId":"s-1","principal":{"kind":"user","id":"u-1"},"displayName":"worker A"},
		{"sessionId":"s-2","principal":{"kind":"user","id":"u-1"},"displayName":"worker B"}
	]}`)); err != nil {
		t.Fatal("schema must represent distinct sessions of one principal:", err)
	}
	for _, raw := range []string{
		`{"sessionId":"s-1","displayName":"worker"}`,
		`{"sessionId":"s-1","principal":{"kind":"anonymous","id":"anonymous"},"displayName":"worker","extra":true}`,
		`{"sessionId":"s-1","principal":{"kind":"anonymous","id":"anonymous","displayName":"worker"}}`,
	} {
		if err = c.ValidateResult(d, []byte(raw)); err == nil {
			t.Fatalf("displayName weakened a closed schema or replaced the principal: %s", raw)
		}
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

// Parent disclosure is a response projection, never a second request/storage
// reference shape. The hidden arm cannot carry identity or other content.
func TestBeadParentExistenceOnlyResultSchema(t *testing.T) {
	c, err := DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	get, _ := c.Descriptor("bead.get")
	for _, tc := range []struct {
		name, parent string
		valid        bool
	}{
		{"root", "", true},
		{"readable", `"parent"`, true},
		{"hidden", `{"exists":true}`, true},
		{"false", `{"exists":false}`, false},
		{"empty", `{}`, false},
		{"null", `null`, false},
		{"id", `{"exists":true,"id":"secret"}`, false},
		{"ref", `{"exists":true,"ref":{"id":"secret"}}`, false},
		{"project", `{"exists":true,"project":"secret"}`, false},
		{"status", `{"exists":true,"status":"secret"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := ""
			if tc.parent != "" {
				parent = `,"parent":` + tc.parent
			}
			raw := []byte(`{"bead":{"id":"child","type":"issue","revision":1` + parent + `},"attachments":[],"comments":[]}`)
			if err := c.ValidateResult(get, raw); (err == nil) != tc.valid {
				t.Fatalf("parent %s valid=%v: %v", tc.parent, tc.valid, err)
			}
		})
	}
	create, _ := c.Descriptor("bead.create")
	if err := c.ValidateParams(create, []byte(`{"type":"issue","fields":{},"parent":{"id":"parent"}}`)); err != nil {
		t.Fatal("canonical parent request refused", err)
	}
	if err := c.ValidateParams(create, []byte(`{"type":"issue","fields":{},"parent":{"exists":true}}`)); err == nil {
		t.Fatal("response-only existence object accepted as a parent request")
	}
}

func TestB23CountContractCanonicalAndEmbedded(t *testing.T) {
	canonicalPath := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "engdocs/design/plug/contracts/openrpc.json")
	canonical, err := os.ReadFile(canonicalPath)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"canonical": canonical, "embedded": catalogJSON} {
		t.Run(name, func(t *testing.T) {
			c, err := ParseCatalog(raw)
			if err != nil {
				t.Fatal(err)
			}
			d, _ := c.Descriptor("view.run")
			for _, params := range []string{`{"definition":{"filter":{}}}`, `{"countOnly":false,"viewId":"saved"}`, `{"countOnly":true,"definition":{"filter":{}}}`} {
				if err := c.ValidateParams(d, []byte(params)); err != nil {
					t.Fatalf("valid count option refused: %s: %v", params, err)
				}
			}
			for _, params := range []string{`{"countOnly":null}`, `{"countOnly":1}`, `{"countOnly":"true"}`, `{"definition":{"filter":{},"countOnly":true}}`} {
				if err := c.ValidateParams(d, []byte(params)); err == nil {
					t.Fatalf("invalid count option admitted: %s", params)
				}
			}
			for _, count := range []string{"", `,"count":0`, `,"count":20000`} {
				if err := c.ValidateResult(d, []byte(`{"items":[],"asOf":{"revision":1}`+count+`}`)); err != nil {
					t.Fatalf("valid ordinary result refused: count=%s: %v", count, err)
				}
			}
			if err := c.ValidateResult(d, []byte(`{"items":[],"asOf":{"revision":1},"count":0,"projection":{"binding":"core","checkpoint":{"incarnation":"inc","revision":1,"index":0}}}`)); err != nil {
				t.Fatalf("count with existing projection metadata refused: %v", err)
			}
			for _, count := range []string{"-1", "20001", "null", "true", `"0"`, "0.5"} {
				if err := c.ValidateResult(d, []byte(`{"items":[],"asOf":{"revision":1},"count":`+count+`}`)); err == nil {
					t.Fatalf("invalid ordinary count admitted: %s", count)
				}
			}
		})
	}
}

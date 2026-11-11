package wire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

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

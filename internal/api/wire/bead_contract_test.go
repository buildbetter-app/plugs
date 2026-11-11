package wire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

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

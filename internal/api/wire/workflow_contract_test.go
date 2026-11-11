package wire

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

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

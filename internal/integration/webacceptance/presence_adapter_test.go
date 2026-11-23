package webacceptance_test

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// These resources test a pure adapter with artificial non-secret DOM ports.
// They start no HTTP handler, SQL store or browser and do not qualify E3.
//
//go:embed presence_dom_adapter.mjs
var presenceAdapter []byte

//go:embed presence_adapter_check.mjs
var presenceAdapterChecks []byte

func TestPresenceDOMAdapterPreparation(t *testing.T) {
	node := hermeticNode(t)
	dir := t.TempDir()
	for name, data := range map[string][]byte{
		"presence_dom_adapter.mjs":   presenceAdapter,
		"presence_adapter_check.mjs": presenceAdapterChecks,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(node, "--test", filepath.Join(dir, "presence_adapter_check.mjs"))
	output, err := cmd.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		t.Fatal("preparation-only adapter unit checks:", err)
	}
}

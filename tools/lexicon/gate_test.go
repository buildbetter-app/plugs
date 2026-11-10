package lexicon

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var sourceInventory = flag.String("lexicon-source-inventory", "", "Bazel runfile path of the complete source inventory")

// TestLexiconTreeGate is the required hermetic B1 tree check. Its data is
// assembled by //:lexicon_tree across all source/package boundaries. It never
// invokes git or reads an ambient protected-path environment variable.
func TestLexiconTreeGate(t *testing.T) {
	if *sourceInventory == "" && os.Getenv("TEST_SRCDIR") == "" {
		t.Skip("the hermetic tree gate runs under Bazel")
	}
	if *sourceInventory == "" || os.Getenv("TEST_SRCDIR") == "" {
		t.Fatal("Bazel did not supply the complete source inventory runfile")
	}
	findings, err := CheckSourceInventory(filepath.Join(os.Getenv("TEST_SRCDIR"), filepath.FromSlash(*sourceInventory)))
	if err != nil {
		t.Fatal(err)
	}
	if findings == nil {
		findings = []Finding{}
	}
	complete, err := json.Marshal(findings)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("complete lexicon findings: %s", complete)
	if output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); output != "" {
		if err := os.WriteFile(filepath.Join(output, "lexicon-findings.json"), append(complete, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := CheckError(findings); err != nil {
		t.Fatal(err)
	}
}

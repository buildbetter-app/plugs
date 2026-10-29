package lexicon

import (
	"os"
	"path/filepath"
	"testing"
)

func bazelRunfilesRoot() (string, error) {
	runfiles := os.Getenv("TEST_SRCDIR")
	if runfiles == "" {
		return "", os.ErrNotExist
	}
	candidates := []string{os.Getenv("TEST_WORKSPACE"), "_main", "plugs", ""}
	for _, workspace := range candidates {
		root := runfiles
		if workspace != "" {
			root = filepath.Join(root, workspace)
		}
		if _, err := os.Stat(filepath.Join(root, "engdocs/style/lexicon.toml")); err == nil {
			return root, nil
		}
	}
	return "", os.ErrNotExist
}

// TestLexiconTreeGate is the required hermetic B1 tree check. Its data is
// assembled by //:lexicon_tree; CheckRunfiles never invokes git or reads an
// ambient protected-path environment variable.
func TestLexiconTreeGate(t *testing.T) {
	root, err := bazelRunfilesRoot()
	if err != nil {
		t.Skip("the hermetic tree gate runs under Bazel")
	}
	findings, err := CheckRunfiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckError(findings); err != nil {
		t.Fatal(err)
	}
}

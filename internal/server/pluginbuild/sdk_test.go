package pluginbuild

import (
	"os"
	"path/filepath"
	"testing"
)

// UseBazelSDK selects the test toolchain for direct and nested Go subprocesses.
func UseBazelSDK(t *testing.T) {
	t.Helper()
	if binary := os.Getenv("PLUG_TEST_GO_BINARY"); binary != "" {
		// Bazel supplies an rlocationpath and the complete SDK as runfiles.
		// Resolve bin/go before setting GOROOT so contained builds and child
		// test processes use the same physical SDK as the test toolchain.
		binary = filepath.Join(os.Getenv("TEST_SRCDIR"), filepath.FromSlash(binary))
		binary, err := filepath.EvalSymlinks(binary)
		if err != nil {
			t.Fatalf("resolve Bazel Go SDK: %v", err)
		}
		root := filepath.Dir(filepath.Dir(binary))
		t.Setenv("GOROOT", root)
		t.Setenv("PATH", filepath.Join(root, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
		t.Logf("pluginbuild tests use Bazel Go SDK: %s", root)
	}
}

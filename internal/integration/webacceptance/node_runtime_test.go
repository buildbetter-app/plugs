package webacceptance_test

import (
	"os"
	"path/filepath"
	"testing"
)

func hermeticNode(t *testing.T) string {
	t.Helper()
	rlocation := os.Getenv("PLUG_WEBACCEPTANCE_NODE")
	if rlocation == "" || os.Getenv("TEST_SRCDIR") == "" {
		t.Fatal("hermetic Node runtime unavailable")
	}
	node, err := filepath.EvalSymlinks(filepath.Join(os.Getenv("TEST_SRCDIR"), filepath.FromSlash(rlocation)))
	if err != nil {
		t.Fatalf("resolve hermetic Node runtime: %v", err)
	}
	return node
}

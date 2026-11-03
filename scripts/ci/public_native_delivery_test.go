package ci_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestNativeDeliveryBoundary(t *testing.T) {
	root, workspace := os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE")
	if root == "" || workspace == "" {
		t.Fatal("Bazel runfiles environment is required")
	}
	script := filepath.Join(root, workspace, "scripts/ci/public-native-delivery-test.py")
	command := exec.Command("python3", script)
	command.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("native delivery boundary: %v\n%s", err, output)
	}
}

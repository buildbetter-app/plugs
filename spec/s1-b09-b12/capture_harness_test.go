package s1b09b12

import (
	"embed"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed record_reference.py public_reference_profile.py reference_capture.py extract_reference_packet.py reference_capture_test.go capture_harness_test.go reference_capture_policy_test.py
var captureFiles embed.FS

func runOwnedCaptureScript(t *testing.T, script string, prepared bool) {
	t.Helper()
	root := t.TempDir()
	entries, err := captureFiles.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := captureFiles.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, entry.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("/usr/bin/python3", "-B", "-s", filepath.Join(root, script))
	command.Env = []string{
		"PATH=/usr/bin:/bin", "HOME=" + root, "TMPDIR=" + root,
		"LC_ALL=C", "PYTHONNOUSERSITE=1", "PYTHONDONTWRITEBYTECODE=1",
	}
	if prepared {
		// These are same-run trusted preparation inputs, never inherited tokens,
		// arbitrary executable paths, user-selected hashes or a prior VM's paths.
		for _, name := range []string{
			"PLUG_PUBLIC_REFERENCE", "PLUG_PUBLIC_REFERENCE_ZIP", "PLUG_PUBLIC_REFERENCE_SOURCE",
			"PLUG_PUBLIC_REFERENCE_MANIFEST", "PLUG_PUBLIC_REFERENCE_BUILD_RUN", "PLUG_SOURCE_MODULE_ROOT",
		} {
			command.Env = append(command.Env, name+"="+os.Getenv(name))
		}
	}
	// Keep packet lines unmodified in Bazel's retained test.log. t.Log would
	// indent/reframe those exact bytes and is deliberately not used here.
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		t.Fatalf("owned %s failed: %v", script, err)
	}
}

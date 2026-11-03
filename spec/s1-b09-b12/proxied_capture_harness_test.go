package s1b09b12

import (
	"embed"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

//go:embed record_reference.py public_reference_profile.py reference_capture.py extract_reference_packet.py reference_capture_policy_test.py proxied_reference_profile.py reference_proxied_capture.py extract_proxied_reference_packet.py proxied_capture_harness_test.go proxied_runtime.go reference_proxied_capture_test.go proxied_reference_capture_policy_test.py
var proxiedCaptureFiles embed.FS

func runOwnedProxiedCapture(t *testing.T, script string, prepared bool) {
	t.Helper()
	root := t.TempDir()
	entries, err := proxiedCaptureFiles.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := proxiedCaptureFiles.ReadFile(entry.Name())
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
		if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
			t.Fatal("fixed proxied runtime requires Linux/amd64")
		}
		archive, err := fetchProxiedDoltArchive(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		archivePath := filepath.Join(root, "same-run-dolt2.2.0.tar.gz")
		if err := os.WriteFile(archivePath, archive, 0o600); err != nil {
			t.Fatal(err)
		}
		archivePath, err = filepath.EvalSymlinks(archivePath)
		if err != nil {
			t.Fatal(err)
		}
		command.Env = append(command.Env, "S1_B09_B12_DOLT_ARCHIVE="+archivePath)
		for _, name := range []string{
			"PLUG_PUBLIC_REFERENCE", "PLUG_PUBLIC_REFERENCE_ZIP", "PLUG_PUBLIC_REFERENCE_SOURCE",
			"PLUG_PUBLIC_REFERENCE_MANIFEST", "PLUG_PUBLIC_REFERENCE_BUILD_RUN", "PLUG_SOURCE_MODULE_ROOT",
		} {
			command.Env = append(command.Env, name+"="+os.Getenv(name))
		}
	}
	// Preserve literal complete frame lines in the workflow-retained test.log.
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Run(); err != nil {
		t.Fatalf("owned proxied %s failed: %v", script, err)
	}
}

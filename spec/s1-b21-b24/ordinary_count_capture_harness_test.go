package s1b21b24

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// The capture programs are manual, source/profile-bound routes. Normal //...
// does not download or execute the public reference engine.
//
//go:embed record_ordinary_count.py ordinary_count_profile.py ordinary_count_capture.py extract_ordinary_count_packet.py ordinary_count_capture_policy_test.py
var ordinaryCountCaptureFiles embed.FS

func runOrdinaryCountScript(t *testing.T, script string, prepared bool) {
	t.Helper()
	root := t.TempDir()
	entries, err := ordinaryCountCaptureFiles.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := ordinaryCountCaptureFiles.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, entry.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.CommandContext(t.Context(), "/usr/bin/python3", "-B", "-s", filepath.Join(root, script))
	command.Env = []string{
		"PATH=/usr/bin:/bin", "HOME=" + root, "TMPDIR=" + root,
		"LC_ALL=C", "PYTHONNOUSERSITE=1", "PYTHONDONTWRITEBYTECODE=1",
	}
	if prepared {
		const descriptor = "41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2"
		if os.Getenv("PLUG_PUBLIC_REFERENCE") != descriptor || os.Getenv("PLUG_PUBLIC_REFERENCE_BUILD_RUN") != "41399124974" {
			t.Fatal("manual count capture requires the exact same-run public preparation descriptor")
		}
		for _, name := range []string{"PLUG_PUBLIC_REFERENCE_ZIP", "PLUG_PUBLIC_REFERENCE_SOURCE", "PLUG_PUBLIC_REFERENCE_MANIFEST", "PLUG_SOURCE_MODULE_ROOT"} {
			if os.Getenv(name) == "" {
				t.Fatalf("manual count capture requires prepared input %s", name)
			}
		}
		if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
			t.Fatal("pinned managed Dolt runtime requires Linux/amd64")
		}
		archive, err := fetchOrdinaryCountDoltArchive(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		archivePath := filepath.Join(root, "same-run-dolt2.2.0.tar.gz")
		if err := os.WriteFile(archivePath, archive, 0o600); err != nil {
			t.Fatal(err)
		}
		if archivePath, err = filepath.EvalSymlinks(archivePath); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{
			"PLUG_PUBLIC_REFERENCE", "PLUG_PUBLIC_REFERENCE_ZIP", "PLUG_PUBLIC_REFERENCE_SOURCE",
			"PLUG_PUBLIC_REFERENCE_MANIFEST", "PLUG_PUBLIC_REFERENCE_BUILD_RUN", "PLUG_SOURCE_MODULE_ROOT",
		} {
			command.Env = append(command.Env, name+"="+os.Getenv(name))
		}
		command.Env = append(command.Env, "S1_B23_DOLT_ARCHIVE="+archivePath)
	}
	var stdout, stderr bytes.Buffer
	// Tee immediately to the original test log as well as retaining the full
	// streams for the framed delivery proof. A launcher failure or timeout
	// must not replace partial output with a short error summary.
	command.Stdout = io.MultiWriter(os.Stdout, &stdout)
	command.Stderr = io.MultiWriter(os.Stderr, &stderr)
	err = command.Run()
	if prepared {
		code, outcome := -1, "launch-error"
		if command.ProcessState != nil {
			code, outcome = command.ProcessState.ExitCode(), "exited"
		}
		emitOrdinaryCountOuter(t, stdout.Bytes(), stderr.Bytes(), code, outcome)
	}
	if err != nil {
		t.Fatalf("ordinary-count %s failed: %v", script, err)
	}
}

func emitOrdinaryCountOuter(t *testing.T, stdout, stderr []byte, code int, outcome string) {
	t.Helper()
	checksum := func(data []byte) string {
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:])
	}
	packet := map[string]any{
		"schema": "plug.s1-b23.ordinary-count-outer.v1", "exitCode": code, "outcome": outcome,
		"stdoutBase64": base64.StdEncoding.EncodeToString(stdout), "stdoutBytes": len(stdout), "stdoutSha256": checksum(stdout),
		"stderrBase64": base64.StdEncoding.EncodeToString(stderr), "stderrBytes": len(stderr), "stderrSha256": checksum(stderr),
	}
	payload, err := json.Marshal(packet)
	if err != nil {
		t.Fatal(err)
	}
	identity := fmt.Sprintf("%d %s", len(payload), checksum(payload))
	fmt.Printf("\nS1_B23_ORDINARY_COUNT_OUTER_BEGIN %s\n%s\nS1_B23_ORDINARY_COUNT_OUTER_END %s\n", identity, payload, identity)
}

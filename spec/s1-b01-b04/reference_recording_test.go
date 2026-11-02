package s1b01b04

import (
	"archive/zip"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed record_reference.py
var referenceRecorderBytes []byte

const publicReferenceDescriptor = "41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2"

func envRequired(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required; run through the trusted public-reference RunsOn route", name)
	}
	return value
}

func sha256File(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func publicReferenceBinary(t *testing.T, archivePath, manifestPath string, destination string) {
	t.Helper()
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Source struct {
			Beads struct {
				Commit string `json:"commit"`
				Tree   string `json:"tree"`
			} `json:"beads"`
		} `json:"source"`
		Artifacts map[string]struct {
			SHA256 string `json:"sha256"`
		} `json:"artifacts"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Source.Beads.Commit != pinnedCommit || manifest.Source.Beads.Tree != pinnedTree {
		t.Fatalf("public reference source pin=%s/%s", manifest.Source.Beads.Commit, manifest.Source.Beads.Tree)
	}
	if got := manifest.Artifacts["bin/bd"].SHA256; got != pinnedBinary {
		t.Fatalf("public reference binary manifest=%s want=%s", got, pinnedBinary)
	}
	archive, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	var binary *zip.File
	for _, entry := range archive.File {
		if entry.Name == "bin/bd" {
			binary = entry
			break
		}
	}
	if binary == nil {
		t.Fatal("public reference archive has no bin/bd")
	}
	reader, err := binary.Open()
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if got := sha256File(data); got != pinnedBinary {
		t.Fatalf("public reference binary=%s want=%s", got, pinnedBinary)
	}
	if err := os.WriteFile(destination, data, 0o700); err != nil {
		t.Fatal(err)
	}
}

func TestCapturePinnedBeadsReference(t *testing.T) {
	archivePath := envRequired(t, "PLUG_PUBLIC_REFERENCE_ZIP")
	sourcePath := envRequired(t, "PLUG_PUBLIC_REFERENCE_SOURCE")
	manifestPath := envRequired(t, "PLUG_PUBLIC_REFERENCE_MANIFEST")
	if got := envRequired(t, "PLUG_PUBLIC_REFERENCE_BUILD_RUN"); got != "41399124974" {
		t.Fatalf("public reference producer run=%s", got)
	}
	if descriptor := os.Getenv("PLUG_PUBLIC_REFERENCE"); descriptor != "" && descriptor != publicReferenceDescriptor {
		t.Fatalf("public reference descriptor=%s", descriptor)
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	binaryPath := filepath.Join(root, "bd")
	publicReferenceBinary(t, archivePath, manifestPath, binaryPath)
	recorderPath := filepath.Join(root, "record_reference.py")
	if err := os.WriteFile(recorderPath, referenceRecorderBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(root, "recording.json")
	if err := os.MkdirAll("/tmp/plug-swarm/work", 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("python3", "-B", recorderPath,
		"--binary", binaryPath, "--source", sourcePath, "--output", outputPath)
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + root,
		"PYTHONNOUSERSITE=1",
		"PYTHONDONTWRITEBYTECODE=1",
		"LC_ALL=C.UTF-8",
		"TZ=UTC",
	}
	stdout, err := command.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("reference recorder failed: %v: %s", err, exit.Stderr)
		}
		t.Fatal(err)
	}
	if len(stdout) != 0 {
		t.Logf("reference recorder stdout=%s", stdout)
	}
	artifact, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(artifact, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["workspaceClass"] != "fresh-disposable" {
		t.Fatalf("reference recording workspace=%v", decoded["workspaceClass"])
	}
	if output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); output != "" {
		if err := os.WriteFile(filepath.Join(output, "reference-recording.json"), artifact, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("S1_B01_B04_CAPTURE_BEGIN")
	fmt.Print(string(artifact))
	if len(artifact) == 0 || artifact[len(artifact)-1] != '\n' {
		fmt.Println()
	}
	fmt.Println("S1_B01_B04_CAPTURE_END")
}

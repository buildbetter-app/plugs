package s1b01b04

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

//go:embed record_reference.py
var referenceRecorderBytes []byte

// This target is built separately from recorded_integrity_test. Keep its
// immutable input pins in its declared source set as well.
const (
	publicReferenceDescriptor = "41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2"
	captureSourceCommit       = "9d6fff760b7a314b476539198504d77ce0959656"
	captureSourceTree         = "cde121d6e4119e454857ad1c3ca293e2791be840"
	captureBinarySHA256       = "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4"
)

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
	if manifest.Source.Beads.Commit != captureSourceCommit || manifest.Source.Beads.Tree != captureSourceTree {
		t.Fatalf("public reference source pin=%s/%s", manifest.Source.Beads.Commit, manifest.Source.Beads.Tree)
	}
	var binaryMember string
	for name, artifact := range manifest.Artifacts {
		if artifact.SHA256 == captureBinarySHA256 {
			if binaryMember != "" {
				t.Fatal("public reference manifest has multiple pinned executables")
			}
			binaryMember = name
		}
	}
	if binaryMember == "" {
		t.Fatal("public reference manifest has no executable with the pinned digest")
	}
	archive, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	var binary *zip.File
	for _, entry := range archive.File {
		if entry.Name == binaryMember {
			if binary != nil {
				t.Fatal("public reference archive has duplicate pinned executable members")
			}
			binary = entry
		}
	}
	if binary == nil {
		t.Fatal("public reference archive has no pinned executable member")
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
	if got := sha256File(data); got != captureBinarySHA256 {
		t.Fatalf("public reference binary=%s want=%s", got, captureBinarySHA256)
	}
	if err := os.WriteFile(destination, data, 0o700); err != nil {
		t.Fatal(err)
	}
}

func TestCapturePinnedBeadsReference(t *testing.T) {
	retained := envRequired(t, "TEST_UNDECLARED_OUTPUTS_DIR")
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
	defer func() {
		if err := retainReferenceCapture(root, retained, os.Stdout); err != nil {
			t.Errorf("retain capture packet: %v", err)
		}
	}()
	binaryPath := filepath.Join(root, "reference-engine")
	publicReferenceBinary(t, archivePath, manifestPath, binaryPath)
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Fatal("the pinned capture runtime requires Linux/amd64")
	}
	archive, err := fetchReferenceDolt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	doltPath := filepath.Join(root, "dolt")
	doltDigest, err := installReferenceDolt(archive, doltPath)
	if err != nil {
		t.Fatal(err)
	}
	versionCommand := exec.Command(doltPath, "version")
	versionCommand.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + root, "LC_ALL=C", "TZ=UTC"}
	var versionError bytes.Buffer
	versionCommand.Stderr = &versionError
	version, err := versionCommand.Output()
	for name, data := range map[string][]byte{"dolt-version.stdout": version, "dolt-version.stderr": versionError.Bytes()} {
		if writeErr := os.WriteFile(filepath.Join(root, name), data, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if err != nil || strings.TrimSpace(string(version)) != "dolt version "+referenceDoltVersion {
		t.Fatalf("pinned Dolt runtime version=%q error=%v", version, err)
	}
	stat, err := os.Stat(doltPath)
	if err != nil {
		t.Fatal(err)
	}
	runtimeBytes, err := json.Marshal(map[string]any{
		"mode":                "managed-proxied-server",
		"version":             referenceDoltVersion,
		"versionOutput":       strings.TrimSpace(string(version)),
		"archiveURL":          referenceDoltURL,
		"archiveSHA256":       referenceDoltSHA256,
		"binarySHA256":        doltDigest,
		"archiveBytes":        len(archive),
		"binaryBytes":         stat.Size(),
		"member":              referenceDoltMember,
		"versionStdoutSHA256": sha256File(version),
		"versionStderrSHA256": sha256File(versionError.Bytes()),
		"pinSource":           "9d6fff760b7a314b476539198504d77ce0959656:tools/bazel/dolt.bzl",
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimePath := filepath.Join(root, "capture-runtime.json")
	if err := os.WriteFile(runtimePath, runtimeBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	recorderPath := filepath.Join(root, "record_reference.py")
	if err := os.WriteFile(recorderPath, referenceRecorderBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(root, "recording.json")
	if err := os.MkdirAll("/tmp/plug-swarm/work", 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("python3", "-B", recorderPath,
		"--binary", binaryPath, "--source", sourcePath, "--output", outputPath,
		"--runtime-manifest", runtimePath,
		"--transcript", filepath.Join(root, "raw-commands.jsonl"))
	command.Env = []string{
		"PATH=" + root + string(os.PathListSeparator) + os.Getenv("PATH"),
		"HOME=" + root,
		"PYTHONNOUSERSITE=1",
		"PYTHONDONTWRITEBYTECODE=1",
		"LC_ALL=C.UTF-8",
		"TZ=UTC",
	}
	stdout, stderr, err := runReferenceRecorder(command, root)
	if err != nil {
		t.Fatalf("reference recorder failed: %v: %s", err, stderr)
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
	if decoded["runtimeProfile"] != "managed-proxied-server" {
		t.Fatalf("reference recording runtime=%v", decoded["runtimeProfile"])
	}
	if transcript, err := os.ReadFile(filepath.Join(root, "raw-commands.jsonl")); err != nil || len(transcript) == 0 {
		t.Fatalf("reference raw transcript was not retained: %v", err)
	}
	fmt.Println("S1_B01_B04_CAPTURE_BEGIN")
	fmt.Print(string(artifact))
	if len(artifact) == 0 || artifact[len(artifact)-1] != '\n' {
		fmt.Println()
	}
	fmt.Println("S1_B01_B04_CAPTURE_END")
}

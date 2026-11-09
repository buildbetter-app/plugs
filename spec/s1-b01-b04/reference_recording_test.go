package s1b01b04

import (
	"archive/zip"
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
	captured := false
	defer func() {
		if err := retainReferenceCapture(root, retained, os.Stdout, captured); err != nil {
			t.Errorf("retain capture packet: %v", err)
		}
	}()
	profileBytes, err := json.Marshal(map[string]any{
		"format":           "plug.s1-b01-b04.capture-profile.v1",
		"publicDescriptor": publicReferenceDescriptor,
		"sourceCommit":     captureSourceCommit, "sourceTree": captureSourceTree,
		"binarySHA256":   captureBinarySHA256,
		"recorderSHA256": sha256File(referenceRecorderBytes),
		"mode":           "managed-proxied-server", "doltVersion": referenceDoltVersion,
		"doltArchiveURL": referenceDoltURL, "doltArchiveSHA256": referenceDoltSHA256,
		"doltMember":               referenceDoltMember,
		"doltVersionCheckDisabled": true,
		"doltMetricsDisabled":      true, "doltEventFlushDisabled": true,
		"doltConfigSourceSHA256":   referenceDoltConfigSourceSHA256,
		"doltEnvSourceSHA256":      referenceDoltEnvSourceSHA256,
		"doltIdentitySourceSHA256": referenceDoltIdentitySourceSHA256,
		"doltIdentity":             map[string]string{"user.name": "beads", "user.email": "beads@localhost"},
		"qualification":            "raw capture only; clocks/embedded/full-card parity unqualified",
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"capture-profile.json":           profileBytes,
		"public-reference-manifest.json": manifestBytes,
		"record_reference.py":            referenceRecorderBytes,
	} {
		if err := os.WriteFile(filepath.Join(root, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for source, destination := range map[string]string{
		"internal/testutil/localdoltserver.go":          "dolt-config-source.go",
		"tools/bazel/test_env.sh":                       "dolt-env-source.sh",
		"internal/storage/dbproxy/server/doltserver.go": "dolt-identity-source.go",
	} {
		data, err := os.ReadFile(filepath.Join(sourcePath, source))
		if err != nil {
			t.Fatal(err)
		}
		want := referenceDoltConfigSourceSHA256
		if destination == "dolt-env-source.sh" {
			want = referenceDoltEnvSourceSHA256
		} else if destination == "dolt-identity-source.go" {
			want = referenceDoltIdentitySourceSHA256
		}
		if sha256File(data) != want {
			t.Fatalf("pinned runtime configuration source differs: %s", source)
		}
		if err := os.WriteFile(filepath.Join(root, destination), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
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
	version, versionError, err := prepareReferenceDoltVersion(doltPath, root)
	if err != nil {
		t.Fatalf("pinned Dolt runtime version=%q error=%v", version, err)
	}
	runtimeCommands, err := os.ReadFile(filepath.Join(root, "runtime-commands.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, ".dolt", "config_global.json")
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	configStat, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	configDirStat, err := os.Stat(filepath.Dir(configPath))
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(doltPath)
	if err != nil {
		t.Fatal(err)
	}
	runtimeBytes, err := json.Marshal(map[string]any{
		"mode":                  "managed-proxied-server",
		"version":               referenceDoltVersion,
		"versionOutput":         strings.TrimSpace(string(version)),
		"archiveURL":            referenceDoltURL,
		"archiveSHA256":         referenceDoltSHA256,
		"binarySHA256":          doltDigest,
		"archiveBytes":          len(archive),
		"binaryBytes":           stat.Size(),
		"member":                referenceDoltMember,
		"versionStdoutSHA256":   sha256File(version),
		"versionStderrSHA256":   sha256File(versionError),
		"versionCheckDisabled":  true,
		"runtimeCommandsSHA256": sha256File(runtimeCommands),
		"configSHA256":          sha256File(configBytes),
		"configFileMode":        fmt.Sprintf("%04o", configStat.Mode().Perm()),
		"configDirectoryMode":   fmt.Sprintf("%04o", configDirStat.Mode().Perm()),
		"configSourceSHA256":    referenceDoltConfigSourceSHA256,
		"envSourceSHA256":       referenceDoltEnvSourceSHA256,
		"identitySourceSHA256":  referenceDoltIdentitySourceSHA256,
		"identity":              map[string]string{"user.name": "beads", "user.email": "beads@localhost"},
		"metricsDisabled":       true, "eventFlushDisabled": true, "privateDoltRoot": true,
		"pinSource": "9d6fff760b7a314b476539198504d77ce0959656:tools/bazel/dolt.bzl",
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
		"DOLT_ROOT_PATH=" + root,
		"DOLT_DISABLE_EVENT_FLUSH=1",
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
	captured = true
	fmt.Println("S1_B01_B04_CAPTURE_BEGIN")
	fmt.Print(string(artifact))
	if len(artifact) == 0 || artifact[len(artifact)-1] != '\n' {
		fmt.Println()
	}
	fmt.Println("S1_B01_B04_CAPTURE_END")
}

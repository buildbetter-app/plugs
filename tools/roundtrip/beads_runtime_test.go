package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

var publicBeadsRuntime = flag.Bool("public-beads-runtime", false, "Require the independently delivered public Beads artifact")

// This target is explicitly selected after trusted artifact preparation. It
// cannot turn missing runtime evidence into a skip or a successful mock.
func TestPublicBeadsArtifactReimportThreeEngines(t *testing.T) {
	if !*publicBeadsRuntime {
		t.Fatal("public runtime target requires -public-beads-runtime")
	}
	options := deliveredPublicBeads(t)
	options = withPublicCaptureFixture(t, options)
	root, err := os.MkdirTemp("/tmp", "plug-public-beads-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	postgres := publicBeadsPostgres(t, root)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, test := range []struct {
			name, file, mode, head, floor string
			complete, all                 bool
			first, sourceHead             uint64
			gaps                          int
		}{
			{"complete", "public-complete-root-990eb800.tar", "full", "8", "0", true, false, 1, 7, 0},
			{"complete-snapshot", "public-complete-root-990eb800.tar", "snapshot", "1", "1", true, false, 1, 7, 0},
			{"partial", "public-partial-990eb800.tar", "partial", "3", "1", false, true, 4, 4, 3},
			{"partial-snapshot", "public-partial-990eb800.tar", "snapshot", "1", "1", false, true, 4, 4, 3},
		} {
			t.Run(engine+"/"+test.name, func(t *testing.T) {
				output := filepath.Join(root, engine+"-"+test.name)
				args := []string{"--input", "../../internal/integration/converterroundtrip/testdata/" + test.file, "--output-dir", output, "--engine", engine, "--history-mode", test.mode,
					"--beads-binary", options.Binary, "--beads-sha256", options.SHA256, "--beads-build-run", options.BuildRun, "--beads-capture-public",
					"--beads-capture-binary", options.CaptureBinary, "--beads-capture-sha256", options.CaptureSHA256, "--beads-capture-build-run", options.CaptureBuildRun}
				// The partial fixture's three beads are all Ephemeral=true.
				// Its edit/reimport workload requires the explicit all export;
				// the complete fixture continues to exercise the default.
				if test.all {
					args = append(args, "--all")
				}
				if engine == "postgres" {
					database := "plug_roundtrip_" + strings.ReplaceAll(test.name, "-", "_")
					if _, err := postgres.db.ExecContext(context.Background(), "CREATE DATABASE "+database); err != nil {
						t.Fatal(err)
					}
					args = append(args, "--postgres-dsn", postgres.base+database+"?sslmode=disable")
				}
				defer retainPublicBeadsRuntimeEvidence(t, output, engine+"-"+test.name)
				got := run(args)
				encoded, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				if err := writePrivate(output, "outcome.json", encoded); err != nil {
					t.Fatal(err)
				}
				t.Logf("PUBLIC_BEADS_REIMPORT_OUTCOME %s", encoded)
				commands := logPublicBeadsCommands(t, output)
				if got.BeadsRefusalSHA256 != "" || got.Reason == "beads_raw_readback_invalid" || got.Reason == "beads_raw_field_changed" || got.Reason == "beads_native_opaque_fields_changed" {
					assertPublicReadbackRefusalEvidence(t, output, got)
				}
				if got.BeadsReadbackSHA256 != "" {
					assertPublicNativeCaptureReadback(t, options, output, got)
				}
				if got.Status != "PASS" || got.Stage != "public_snapshot_roundtrip_complete" || got.Reason != "" || got.BeadsReimport != "PASS" || got.BeadsReadback != "PASS" || got.BeadsReadbackSHA256 == "" || len(got.SourceFieldLosses) != 0 {
					t.Fatalf("actual import and native readback: %+v", got)
				}
				if got.BeadsCommit != beadsCommit || got.BeadsTree != beadsTree || got.BeadsBinarySHA256 != options.SHA256 || got.BeadsBuildRun != options.BuildRun || got.Claims != 1 || got.Writes != 1 || got.HistoryMode != test.mode || got.RestoredHead != test.head || got.HistoryFloor != test.floor || got.SourceHistoryComplete != test.complete || got.SourceHistoryFirst != test.first || got.SourceHistoryHead != test.sourceHead || len(got.SourceHistoryGaps) != test.gaps || got.RestoredIncarnation == "" || got.RestoredIncarnation != got.ReverseIncarnation || got.ReverseAt == "" || got.BaselineSHA256 == got.ReverseSHA256 || got.HistoryLossItems == 0 {
					t.Fatalf("actual import changed provenance or restore/workload assertions: %+v", got)
				}
				if len(commands) != 6 || len(commands[0].Args) != 2 || commands[0].Args[0] != "version" || commands[0].Args[1] != "--json" || len(commands[1].Args) == 0 || commands[1].Args[0] != "init" || len(commands[2].Args) != 3 || commands[2].Args[0] != "import" || commands[2].Args[1] != filepath.Join(output, "reverse-live.jsonl") || commands[2].Args[2] != "--json" || !sameJSON(commands[3].Args, []string{"version", "--json"}) {
					t.Fatal("actual version/init/import/capture/export command evidence is incomplete")
				}
				if !sameJSON(commands[4].Args, []string{"--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "--output", filepath.Join(output, "beads-generated-inventory.json")}) || !sameJSON(commands[5].Args, []string{"--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", filepath.Join(output, "beads-inventory.json"), "--output", filepath.Join(output, "beads-raw-readback.tar")}) {
					t.Fatal("readback commands did not capture and export the actual fresh native target")
				}
				for i, command := range commands {
					wantSHA := options.SHA256
					if i >= 3 {
						wantSHA = options.CaptureSHA256
					}
					workspace := filepath.Join(output, "beads-target")
					if command.Exit != 0 || command.ExecutableSHA256 != wantSHA || command.Directory != workspace || !sameJSON(command.Environment, beadsChildEnv(workspace, filepath.Join(workspace, "home"))) {
						t.Fatal("a required real Beads command failed")
					}
				}
				var imported struct {
					Created             int      `json:"created"`
					Updated             int      `json:"updated"`
					Unchanged           int      `json:"unchanged"`
					Skipped             int      `json:"skipped"`
					Memories            int      `json:"memories"`
					IDs                 []string `json:"ids"`
					SkippedDependencies []string `json:"skipped_dependencies"`
					DryRun              bool     `json:"dry_run"`
				}
				data, err := os.ReadFile(filepath.Join(output, "beads-3.stdout"))
				// The reverse protocol emits each config reference as one
				// additional memory, independently of ordinary memory records.
				expectedMemories := got.Counts["memory"] + got.Counts["config"]
				if err != nil || json.Unmarshal(data, &imported) != nil || imported.Created != got.Counts["bead"] || len(imported.IDs) != got.Counts["bead"] || imported.Memories != expectedMemories || imported.Updated != 0 || imported.Unchanged != 0 || imported.Skipped != 0 || len(imported.SkippedDependencies) != 0 || imported.DryRun {
					t.Fatal("real Beads import did not create every expected issue/memory in the fresh target")
				}
			})
		}
	}
}

// Reuse the existing declared B09 capture source and its one shared builder.
// This builds a separately bound fixture, never an admitted importer successor.
func withPublicCaptureFixture(t *testing.T, options beadsOptions) beadsOptions {
	t.Helper()
	parent := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	buildRun := os.Getenv("GITHUB_RUN_ID")
	if !filepath.IsAbs(parent) || buildRun == "" {
		t.Fatal("actual hosted capture fixture run/output binding required")
	}
	root, err := os.MkdirTemp(parent, "public-readback-capture-")
	if err != nil {
		t.Fatal(err)
	}
	builder := publicnative.SourceBuilder{T: t, Root: root, SourceDirectory: "../../spec/s1-b09-native/testdata", Target: publicNativeProtocolExporterLabel(t)}
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(checkout) {
		t.Fatal("actual candidate checkout required")
	}
	head := strings.TrimSpace(string(builder.Command(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if len(head) != 40 {
		t.Fatal("actual candidate head required")
	}
	options.CaptureBinary = builder.Build(publicnative.CanonicalSource{
		Name: "beads-446dbb", Commit: publicCaptureCommit, Tree: publicCaptureTree, Parent: "a367be65df60baa1ee8444f1e437904122f25f87",
		ArchiveSHA: "32034b6d242f252144f6353f0d9d2f8b7d50739faf7973d62de93cafe25f3959", ManifestSHA: "a2456b25849b281d86ac57a461e21e0cc244cd80602c20269d88e7b9e28bc246", Entries: 5158,
	})
	data, err := os.ReadFile(options.CaptureBinary)
	if err != nil {
		t.Fatal(err)
	}
	options.CaptureSHA256, options.CaptureBuildRun = hash(data), buildRun
	options.CapturePublicInventory = true
	identity, err := json.Marshal(map[string]any{"candidate": head, "capture_commit": publicCaptureCommit, "capture_tree": publicCaptureTree, "capture_sha256": options.CaptureSHA256, "capture_build_run": buildRun, "import_commit": beadsCommit, "import_tree": beadsTree, "import_sha256": options.SHA256, "import_build_run": options.BuildRun, "target": os.Getenv("TEST_TARGET")})
	if err != nil || writePrivate(root, "request-run-binding.json", identity) != nil {
		t.Fatal("capture request/run binding could not be retained")
	}
	t.Logf("PUBLIC_BEADS_SEPARATE_CAPTURE %s", identity)
	return options
}

func TestPublicBeadsMissingInventoryRemainsUnqualified(t *testing.T) {
	if !*publicBeadsRuntime {
		t.Fatal("public runtime target requires -public-beads-runtime")
	}
	options := deliveredPublicBeads(t)
	root, err := os.MkdirTemp("/tmp", "plug-public-beads-missing-inventory-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	output := filepath.Join(root, "memory-complete")
	defer retainPublicBeadsRuntimeEvidence(t, output, "missing-inventory")
	got := run([]string{"--input", "../../internal/integration/converterroundtrip/testdata/public-complete-root-990eb800.tar", "--output-dir", output, "--engine", "memory", "--history-mode", "full", "--beads-binary", options.Binary, "--beads-sha256", options.SHA256, "--beads-build-run", options.BuildRun})
	if got.Status != "WAIT" || got.Stage != "beads_inventory" || got.Reason != "beads_inventory_required" || got.BeadsReimport != "PASS" || got.BeadsReadback != "WAIT" || got.BeadsReadbackSHA256 != "" {
		t.Fatalf("missing actual target inventory gained readback credit: %+v", got)
	}
	commands := logPublicBeadsCommands(t, output)
	if len(commands) != 3 {
		t.Fatal("missing-inventory control did not preserve version/init/import boundary")
	}
	for _, command := range commands {
		if command.Exit != 0 {
			t.Fatal("missing-inventory control did not complete the real import")
		}
	}
}

// Retain real raw evidence after each leaf, including failures. Do not copy the
// executable or database: the prepared artifact already binds the executable,
// and the independently parsed raw export is the target readback evidence.
func retainPublicBeadsRuntimeEvidence(t *testing.T, output, name string) {
	t.Helper()
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) || filepath.Base(name) != name || name == "." || name == ".." {
		t.Error("public runtime requires a declared evidence output directory")
		return
	}
	destination := filepath.Join(root, "public-reimport-readbacks", name)
	if err := os.MkdirAll(destination, 0700); err != nil {
		t.Error(err)
		return
	}
	copyFile := func(source, target string) error {
		info, err := os.Lstat(source)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return os.ErrInvalid
		}
		data, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0600)
	}
	entries, err := os.ReadDir(output)
	if err != nil {
		t.Error(err)
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "beads-pinned" || name == "beads-capture-pinned" || name == "beads-target" || name != "outcome.json" && !strings.HasPrefix(name, "beads-") && !strings.HasPrefix(name, "reverse-") {
			continue
		}
		if name == "beads-generated-inventory.json.sources" {
			if err := os.Mkdir(filepath.Join(destination, name), 0700); err != nil {
				t.Error(err)
				continue
			}
			files, err := os.ReadDir(filepath.Join(output, name))
			if err != nil {
				t.Error(err)
				continue
			}
			for _, file := range files {
				if err := copyFile(filepath.Join(output, name, file.Name()), filepath.Join(destination, name, file.Name())); err != nil {
					t.Error(err)
				}
			}
			continue
		}
		if err := copyFile(filepath.Join(output, name), filepath.Join(destination, name)); err != nil {
			t.Error(err)
		}
	}
}

func deliveredPublicBeads(t *testing.T) beadsOptions {
	t.Helper()
	options := beadsOptions{Binary: os.Getenv("PLUG_PUBLIC_BEADS_BINARY"), SHA256: os.Getenv("PLUG_PUBLIC_BEADS_SHA256"), BuildRun: os.Getenv("PLUG_PUBLIC_BEADS_BUILD_RUN")}
	manifest := os.Getenv("PLUG_PUBLIC_BEADS_MANIFEST")
	for name, value := range map[string]string{"PLUG_PUBLIC_BEADS_BINARY": options.Binary, "PLUG_PUBLIC_BEADS_SHA256": options.SHA256, "PLUG_PUBLIC_BEADS_BUILD_RUN": options.BuildRun, "PLUG_PUBLIC_BEADS_MANIFEST": manifest} {
		if value == "" {
			t.Fatalf("required delivered public artifact evidence is missing: %s", name)
		}
	}
	if !filepath.IsAbs(options.Binary) || !filepath.IsAbs(manifest) {
		t.Fatal("public artifact paths must be prepared absolute paths")
	}
	if build, err := strconv.ParseUint(options.BuildRun, 10, 64); err != nil || build == 0 || strconv.FormatUint(build, 10) != options.BuildRun {
		t.Fatal("public artifact build run must be an immutable decimal run ID")
	}
	for _, c := range options.SHA256 {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			t.Fatal("public artifact digest must be lower-case SHA256")
		}
	}
	if len(options.SHA256) != 64 {
		t.Fatal("public artifact digest must be lower-case SHA256")
	}
	data, err := os.ReadFile(manifest)
	if err != nil || len(data) > 1<<20 {
		t.Fatal("verified public artifact manifest is unavailable or invalid")
	}
	// External manifest wire names are source-controlled protocol data.
	profileData, err := os.ReadFile("testdata/public-companion-profile.json")
	var profile struct {
		Schema   string `json:"schema"`
		Exporter struct {
			TargetKey string `json:"target_key"`
			Label     string `json:"label"`
			Binary    string `json:"binary"`
		} `json:"exporter"`
		VersionKey string `json:"version_key"`
	}
	if err != nil || json.Unmarshal(profileData, &profile) != nil || profile.Schema == "" || profile.Exporter.TargetKey == "" || profile.Exporter.Label == "" || profile.Exporter.Binary == "" || profile.VersionKey == "" {
		t.Fatal("public companion protocol data is unavailable or invalid")
	}
	var identity struct {
		Schema string `json:"schema"`
		Source map[string]struct {
			Commit string `json:"commit"`
			Tree   string `json:"tree"`
		} `json:"source"`
		Targets map[string]struct {
			Label  string `json:"label"`
			Binary string `json:"binary"`
		} `json:"targets"`
		Artifacts map[string]struct {
			SHA256 string `json:"sha256"`
			Size   int64  `json:"size"`
			Mode   string `json:"mode"`
		} `json:"artifacts"`
		Build struct {
			Run string `json:"run_id"`
		} `json:"build"`
		Resolution map[string]json.RawMessage `json:"resolution"`
	}
	if err := json.Unmarshal(data, &identity); err != nil {
		t.Fatalf("verified public artifact manifest is invalid: %v", err)
	}
	artifact := identity.Artifacts[profile.Exporter.Binary]
	target := identity.Targets[profile.Exporter.TargetKey]
	var version struct {
		Commit string `json:"commit"`
		Build  string `json:"build"`
	}
	if err := json.Unmarshal(identity.Resolution[profile.VersionKey], &version); err != nil {
		t.Fatalf("verified public artifact version is invalid: %v", err)
	}
	if identity.Schema != profile.Schema || identity.Source["beads"].Commit != beadsCommit || identity.Source["beads"].Tree != beadsTree || target.Label != profile.Exporter.Label || target.Binary != profile.Exporter.Binary || artifact.SHA256 != options.SHA256 || artifact.Mode != "0o755" || artifact.Size <= 0 || identity.Build.Run != options.BuildRun || version.Commit != beadsCommit || version.Build != "public-"+beadsCommit {
		t.Fatal("prepared public artifact identity does not match its manifest")
	}
	info, err := os.Lstat(options.Binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o755 || info.Size() != artifact.Size {
		t.Fatal("prepared public executable mode or size does not match its manifest")
	}
	t.Logf("PUBLIC_BEADS_ARTIFACT build_run=%s binary_sha256=%s manifest_sha256=%s", options.BuildRun, options.SHA256, hash(data))
	return options
}

func logPublicBeadsCommands(t *testing.T, output string) []commandEvidence {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(output, "beads-commands.json"))
	if err != nil {
		t.Log("no Beads subprocess evidence was produced")
		return nil
	}
	var commands []commandEvidence
	if err := json.Unmarshal(data, &commands); err != nil {
		t.Fatal(err)
	}
	t.Logf("PUBLIC_BEADS_COMMANDS %s", data)
	for i, command := range commands {
		for stream, digest := range map[string]string{"stdout": command.StdoutSHA256, "stderr": command.StderrSHA256} {
			data, err := os.ReadFile(filepath.Join(output, "beads-"+strconv.Itoa(i+1)+"."+stream))
			if err != nil || hash(data) != digest {
				t.Fatal("actual Beads subprocess output evidence is incomplete")
			}
			t.Logf("PUBLIC_BEADS_RAW command=%d stream=%s bytes=%q", i+1, stream, data)
		}
	}
	return commands
}

type publicRuntimePostgres struct {
	db   *sql.DB
	base string
}

func publicBeadsPostgres(t *testing.T, root string) publicRuntimePostgres {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	name := "plug-swarm-i6-publicbeads-" + hash([]byte(root))[:16]
	docker := func(args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...).CombinedOutput()
	}
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	if out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("owned PostgreSQL: %v %s", err, out)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if out, err := exec.CommandContext(cleanup, "docker", "--host=unix:///var/run/docker.sock", "rm", "-f", name).CombinedOutput(); err != nil {
			t.Errorf("owned PostgreSQL cleanup: %v %s", err, out)
		}
	})
	port, err := docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	address := strings.TrimSpace(string(port))
	if host, _, err := net.SplitHostPort(address); err != nil || host != "127.0.0.1" {
		t.Fatal("PostgreSQL is not loopback")
	}
	base := "postgres://postgres@" + address + "/"
	db, err := sql.Open("pgx", base+"postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for db.PingContext(ctx) != nil {
		if ctx.Err() != nil {
			t.Fatal("owned PostgreSQL did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return publicRuntimePostgres{db: db, base: base}
}

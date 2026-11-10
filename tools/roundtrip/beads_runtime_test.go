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

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

var publicBeadsRuntime = flag.Bool("public-beads-runtime", false, "Require the independently delivered public Beads artifact")

// This target is explicitly selected after trusted artifact preparation. It
// cannot turn missing runtime evidence into a skip or a successful mock.
func TestPublicBeadsArtifactReimportThreeEngines(t *testing.T) {
	if !*publicBeadsRuntime {
		t.Fatal("public runtime target requires -public-beads-runtime")
	}
	options := deliveredPublicBeads(t)
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
					"--beads-binary", options.Binary, "--beads-sha256", options.SHA256, "--beads-build-run", options.BuildRun, "--beads-inventory", autoBeadsInventory}
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
				got := run(args)
				encoded, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("PUBLIC_BEADS_REIMPORT_OUTCOME %s", encoded)
				commands := logPublicBeadsCommands(t, output)
				if got.Reason == "beads_raw_readback_invalid" {
					// Retain the actual reader refusal before the owned workspace
					// is removed, without exposing it through the public CLI.
					_, readErr := dumpformat.ReadFile(filepath.Join(output, "beads-raw-readback.tar"))
					t.Logf("PUBLIC_BEADS_RAW_READBACK_REFUSAL %v", readErr)
				}
				if got.BeadsReadbackSHA256 != "" {
					// Inspect the actual readable archive before a generic outcome
					// refusal hides the differing field and deletes the workspace.
					assertPublicNativeCaptureReadback(t, options, output, got)
				}
				if got.Status != "PASS" || got.Stage != "public_snapshot_roundtrip_complete" || got.Reason != "" || got.BeadsReimport != "PASS" || got.BeadsReadback != "PASS" || got.BeadsReadbackSHA256 == "" {
					t.Fatalf("actual import and native readback: %+v", got)
				}
				if got.BeadsCommit != beadsCommit || got.BeadsTree != beadsTree || got.BeadsBinarySHA256 != options.SHA256 || got.BeadsBuildRun != options.BuildRun || got.Claims != 1 || got.Writes != 1 || got.HistoryMode != test.mode || got.RestoredHead != test.head || got.HistoryFloor != test.floor || got.SourceHistoryComplete != test.complete || got.SourceHistoryFirst != test.first || got.SourceHistoryHead != test.sourceHead || len(got.SourceHistoryGaps) != test.gaps || got.RestoredIncarnation == "" || got.RestoredIncarnation != got.ReverseIncarnation || got.ReverseAt == "" || got.BaselineSHA256 == got.ReverseSHA256 || got.HistoryLossItems == 0 {
					t.Fatalf("actual import changed provenance or restore/workload assertions: %+v", got)
				}
				if len(commands) != 5 || len(commands[0].Args) != 2 || commands[0].Args[0] != "version" || commands[0].Args[1] != "--json" || len(commands[1].Args) == 0 || commands[1].Args[0] != "init" || len(commands[2].Args) != 3 || commands[2].Args[0] != "import" || commands[2].Args[1] != filepath.Join(output, "reverse-live.jsonl") || commands[2].Args[2] != "--json" || len(commands[3].Args) != 7 || commands[3].Args[0] != "--readonly" || commands[3].Args[1] != "export" || commands[3].Args[2] != "--format" || commands[3].Args[3] != "beads-inventory/1" || commands[3].Args[4] != "--plug-capture-public" || commands[3].Args[5] != "--output" || commands[3].Args[6] != filepath.Join(output, "beads-generated-inventory.json") || len(commands[4].Args) != 8 || commands[4].Args[0] != "--readonly" || commands[4].Args[1] != "export" || commands[4].Args[2] != "--format" || commands[4].Args[3] != "plug-dump/1" || commands[4].Args[4] != "--plug-inventory" || commands[4].Args[5] != filepath.Join(output, "beads-inventory.json") || commands[4].Args[6] != "--output" || commands[4].Args[7] != filepath.Join(output, "beads-raw-readback.tar") {
					t.Fatal("actual version/init/import/inventory/raw-export command evidence is incomplete")
				}
				for _, command := range commands {
					if command.Exit != 0 {
						t.Fatal("a required real Beads command failed")
					}
				}
				inventoryData, err := os.ReadFile(filepath.Join(output, "beads-inventory.json"))
				var inventory struct {
					Version  string `json:"version"`
					External []struct {
						Family string `json:"family"`
						Path   string `json:"path"`
						State  string `json:"state"`
					} `json:"external"`
					ConfigPolicy map[string]string `json:"config_policy"`
				}
				if err != nil || json.Unmarshal(inventoryData, &inventory) != nil || inventory.Version != "beads-inventory/1" || len(inventory.External) != 26 || len(inventory.ConfigPolicy) == 0 {
					t.Fatalf("auto inventory census is incomplete: %v %s", err, inventoryData)
				}
				for _, input := range inventory.External {
					if input.Family == "" || input.State != "json-config" || input.Path == "" {
						t.Fatalf("auto inventory external entry is incomplete: %+v", input)
					}
					if info, err := os.Stat(input.Path); err != nil || !info.Mode().IsRegular() {
						t.Fatalf("auto inventory sidecar is unavailable: %+v %v", input, err)
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
	profileData := publicNativeProtocolInput(t,
		"tools/roundtrip/testdata/native-public-profile.json",
		"67491cd9ce55d652ee99ce80144298648ce60af4c93d35f266ccf7a6fbfd739d")
	var profile struct {
		Schema   string `json:"schema"`
		Exporter struct {
			TargetKey string `json:"target_key"`
			Label     string `json:"label"`
			Binary    string `json:"binary"`
		} `json:"exporter"`
		VersionKey string `json:"version_key"`
	}
	if json.Unmarshal(profileData, &profile) != nil || profile.Schema != "plug-public-native/1" || profile.Exporter.TargetKey == "" || profile.Exporter.Label == "" || profile.Exporter.Binary == "" || profile.VersionKey == "" {
		t.Fatal("public native protocol data is unavailable or invalid")
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
	data, err = os.ReadFile(filepath.Join(output, "beads-workspace-init.json"))
	var initialized commandEvidence
	if err != nil || json.Unmarshal(data, &initialized) != nil || initialized.Exit != 0 || strings.Join(initialized.Args, " ") != "git init --quiet --template=" {
		t.Fatalf("actual isolated Git workspace evidence is incomplete: %v %s", err, data)
	}
	t.Logf("PUBLIC_BEADS_WORKSPACE_INIT %s", data)
	for stream, digest := range map[string]string{"stdout": initialized.StdoutSHA256, "stderr": initialized.StderrSHA256} {
		raw, err := os.ReadFile(filepath.Join(output, "beads-workspace-init."+stream))
		if err != nil || hash(raw) != digest {
			t.Fatal("actual Git workspace initialization output is incomplete")
		}
		t.Logf("PUBLIC_BEADS_WORKSPACE_RAW stream=%s bytes=%q", stream, raw)
	}
	if info, err := os.Lstat(filepath.Join(output, "beads-target", ".git")); err != nil || !info.IsDir() {
		t.Fatal("actual owned Git repository is missing")
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

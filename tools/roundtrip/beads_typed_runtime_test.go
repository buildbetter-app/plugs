package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type publicTypedReader struct{ Binary, SHA256, BuildRun string }

// Build the unchanged, reviewed reader through the existing single fixture
// builder. This is consumer-side public fixture execution, not a producer
// source change, new admission, or transfer from its old executed binary.
func buildPublicTypedReader(t *testing.T) publicTypedReader {
	t.Helper()
	parent, buildRun := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), os.Getenv("GITHUB_RUN_ID")
	if !filepath.IsAbs(parent) || buildRun == "" {
		t.Fatal("actual typed-reader run/output binding required")
	}
	root, err := os.MkdirTemp(parent, "public-typed-reader-")
	if err != nil {
		t.Fatal(err)
	}
	builder := publicnative.SourceBuilder{T: t, Root: root, SourceDirectory: "testdata/typed-reader", Target: "//tools/native-physical-capture:native-physical-capture"}
	binary := builder.Build(publicnative.CanonicalSource{
		Name: "beads-e25cf4", Commit: typedReaderCommit, Tree: typedReaderTree, Parent: "bf48d9464400c557edfb89abe1a20e96ce3edb86",
		ArchiveSHA: "89d43f0bc3e8f0ed58393d8982bbd5f290739c9af1228683c5338c08efa36fb6", ManifestSHA: "0a31c98fdd08dc93af919d3bb0f4c8470bb4a126d1c7388c49dff7a222889f55", Entries: 5228,
	})
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	return publicTypedReader{binary, hash(data), buildRun}
}

func collectPublicTypedReadback(t *testing.T, reader publicTypedReader, output, caseName string, got outcome) typedAssessment {
	t.Helper()
	var observed *typedManifest
	gap := ""
	dir := filepath.Join(output, "beads-typed")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	observe := func() error {
		if got.BeadsReimport != "PASS" {
			return fmt.Errorf("actual_import_target_unavailable")
		}
		workspace := filepath.Join(output, "beads-target")
		beadsDir := filepath.Join(workspace, ".beads")
		metadata, err := readProducedBeadsArchive(filepath.Join(beadsDir, "metadata.json"))
		if err != nil {
			return fmt.Errorf("actual_target_metadata_unavailable")
		}
		var config map[string]json.RawMessage
		if strictTypedJSON(metadata, &config) != nil {
			return fmt.Errorf("actual_target_metadata_invalid")
		}
		var database, mode string
		if json.Unmarshal(config["dolt_database"], &database) != nil || json.Unmarshal(config["dolt_mode"], &mode) != nil || mode != "embedded" || database == "" || strings.ContainsAny(database, "/\\.") {
			return fmt.Errorf("actual_target_database_unsupported")
		}
		dataDir := filepath.Join(beadsDir, "embeddeddolt")
		storeID := "native/" + hash([]byte(filepath.Join(dataDir, database)))
		binding := typedBinding{Case: caseName, Role: "observed-target", Workspace: workspace, Database: database, StoreID: storeID, SourceSHA256: got.ArchiveSHA256, LiveSHA256: got.ReverseSHA256, ImporterSHA256: got.BeadsBinarySHA256, ImporterBuildRun: got.BeadsBuildRun, ReaderCommit: typedReaderCommit, ReaderTree: typedReaderTree, ReaderSHA256: reader.SHA256, ReaderBuildRun: reader.BuildRun}
		request, err := json.Marshal(map[string]any{"binding": binding, "target_metadata_sha256": hash(metadata), "importer_commit": got.BeadsCommit, "importer_tree": got.BeadsTree, "capture_commit": got.BeadsCaptureCommit, "capture_tree": got.BeadsCaptureTree, "capture_sha256": got.BeadsCaptureBinarySHA256, "capture_build_run": got.BeadsCaptureBuildRun})
		if err != nil || writePrivate(dir, "target-binding.json", request) != nil {
			return fmt.Errorf("typed_target_binding_write_refused")
		}
		// The importer and both capture subprocesses have terminated. No
		// writer or embedded handle is retained by this test process.
		args := []string{"--disposable", "--data-dir=" + dataDir, "--database=" + database, "--beads-dir=" + beadsDir, "--output-dir=" + dir, "--timeout=20m"}
		ctx, cancel := context.WithTimeout(context.Background(), 21*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, reader.Binary, args...)
		cmd.Dir, cmd.Env = workspace, beadsChildEnv(workspace, filepath.Join(workspace, "home"))
		cmd.Env[0] = "PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin"
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		runErr := cmd.Run()
		exit := 0
		if runErr != nil {
			exit = -1
			if cmd.ProcessState != nil {
				exit = cmd.ProcessState.ExitCode()
			}
		}
		command := commandEvidence{ExecutableSHA256: reader.SHA256, Directory: cmd.Dir, Environment: cmd.Env, Args: args, Exit: exit, StdoutSHA256: hash(stdout.Bytes()), StderrSHA256: hash(stderr.Bytes())}
		commandData, err := json.Marshal(command)
		if err != nil || writePrivate(dir, "command.json", commandData) != nil || writePrivate(dir, "stdout", stdout.Bytes()) != nil || writePrivate(dir, "stderr", stderr.Bytes()) != nil {
			return fmt.Errorf("typed_reader_command_evidence_refused")
		}
		if runErr != nil {
			return fmt.Errorf("typed_reader_refused_target")
		}
		// Bind the reader's published inventory and every family to this
		// actual target. It may not import a previous case's certificate.
		inventoryData, err := readProducedBeadsArchive(filepath.Join(dir, "inventory.json"))
		if err != nil {
			return fmt.Errorf("typed_inventory_missing")
		}
		var inventory struct {
			StoreID  string `json:"store_id"`
			Projects []struct {
				Prefix string `json:"prefix"`
			} `json:"projects"`
			External []struct{ Family, Path string } `json:"external"`
		}
		if json.Unmarshal(inventoryData, &inventory) != nil || inventory.StoreID != storeID || len(inventory.External) != 26 {
			return fmt.Errorf("typed_inventory_source_or_family_gap")
		}
		families := map[string]bool{}
		for _, external := range inventory.External {
			if external.Family == "" || families[external.Family] || filepath.Dir(external.Path) != filepath.Join(dir, "inventory.json.sources") {
				return fmt.Errorf("typed_inventory_duplicate_or_wrong_source_family")
			}
			families[external.Family] = true
			info, err := os.Lstat(external.Path)
			if err != nil || !info.Mode().IsRegular() {
				return fmt.Errorf("typed_inventory_family_missing")
			}
		}
		data, err := readProducedBeadsArchive(filepath.Join(dir, "physical-evidence.json"))
		if err != nil {
			return fmt.Errorf("typed_physical_evidence_missing")
		}
		var header typedEvidence
		if strictTypedJSON(data, &header) != nil {
			return fmt.Errorf("typed_physical_evidence_invalid")
		}
		projects := []string{}
		for _, project := range inventory.Projects {
			projects = append(projects, project.Prefix)
		}
		// The exact head is authenticated by the reader against both its
		// immutable graph and current working root, then checked against
		// the separately produced companion archive below.
		observed, err = adaptTypedManifest(data, binding, header.ArchiveSHA256, header.Working.HeadCommitID, projects)
		if err != nil {
			observed = nil
			return fmt.Errorf("typed_adapter_source_or_schema_refusal: %w", err)
		}
		archive, err := readProducedBeadsArchive(filepath.Join(dir, "capture.tar"))
		if err != nil {
			observed = nil
			return fmt.Errorf("typed_companion_archive_missing")
		}
		if err := bindTypedArchive(archive, observed); err != nil {
			observed = nil
			return fmt.Errorf("typed_adapter_coverage_refusal: %w", err)
		}
		// The original 446 inventory already identifies this target store.
		old, err := os.ReadFile(filepath.Join(output, "beads-inventory.json"))
		var oldInventory struct {
			StoreID  string `json:"store_id"`
			Projects []struct {
				Prefix string `json:"prefix"`
			} `json:"projects"`
		}
		if err != nil || json.Unmarshal(old, &oldInventory) != nil || oldInventory.StoreID != storeID || !reflect.DeepEqual(oldInventory.Projects, inventory.Projects) {
			observed = nil
			return fmt.Errorf("typed_vs_original_target_binding_mismatch")
		}
		return nil
	}
	if err := observe(); err != nil {
		gap = err.Error()
	}
	// The untouched original archives contain no authenticated original SQL
	// types/tagged cells. A target capture must never define its own expected
	// input. There is deliberately no normalized-JSON type inference here.
	assessment := compareTypedManifests(caseName, nil, observed, got.Status == "PASS" && got.BeadsReadback == "PASS" && len(got.SourceFieldLosses) == 0)
	if gap != "" {
		assessment.Gaps = append(assessment.Gaps, gap)
	}
	encoded, err := json.Marshal(assessment)
	if err != nil || writePrivate(dir, "comparison.json", encoded) != nil {
		t.Fatal("typed comparison evidence write refused")
	}
	t.Logf("PUBLIC_TYPED_NATIVE_COMPARISON %s", encoded)
	return assessment
}

func retainPublicTypedEvidence(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("typed evidence contains unsupported entry")
		}
		data, err := readProducedBeadsArchive(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0600)
	})
	if err != nil {
		t.Error(err)
	}
}

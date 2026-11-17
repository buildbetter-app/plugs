//go:build linux

package b03catalog_test

// This is an opt-in B03 native recording.  It consumes the exact public
// native executable admitted by RunsOn, owns one pinned Dolt server, and keeps
// every command, raw response, physical SQL census, and refusal under the
// undeclared output directory.  It is deliberately a separate manual target;
// the ordinary b03catalog_test never receives a native executable.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/publicnative"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const (
	b03NativeInputPath            = "internal/integration/b03catalog/native_catalog_inputs.json"
	b03NativeProfile              = "plug-public-native/1"
	b03NativeSource               = "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724"
	b03NativeTree                 = "930405e863284a3955cbd35e73a6097b606b91a5"
	b03NativeWorkflow             = "d758fdf42a426e9e2a4b707dbcebb22ec68b264e"
	b03NativeBranch               = "swarm/native-producer-census-control-c8eb-20261104"
	b03NativeDescriptor           = "41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a"
	b03NativeBinarySHA            = "b734da8ba8b6f50d7fd32239e405b8f8ed9d171b7be5fbbccb63a923d257a123"
	b03NativeManifestSHA          = "ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a"
	b03NativeRun                  = "41800330346"
	b03NativeBinarySize     int64 = 161299592
	b03NativePreparerPath         = "scripts/ci/prepare-public-bundle.py"
	b03NativePreparerCommit       = "70230ccd92597a96000f90223c5d89b103e25426"
	b03NativePreparerTree         = "79e63be5f688d589c1080e9bfa5e6cd59bac8393"
	b03NativePreparerSHA          = "6a84191fd016ddcb637cb5a470e88727176a70e8b5c6e8e6c69f33db6a1559eb"
	b03NativeReadinessSHA         = "e85537f5246ad0d7a5b7decfa8ebbb76665b7e92f0f1525c7dd79f8f22f792cd"
)

var b03NativeTables = strings.Fields("schema_migrations ignored_schema_migrations issues wisps dependencies wisp_dependencies labels wisp_labels comments wisp_comments events wisp_events config metadata local_metadata child_counters wisp_child_counters issue_counter issue_snapshots compaction_snapshots repo_mtimes routes interactions federation_peers custom_statuses custom_types provenance_events bd_events_journal bd_events_seq issue_versions store_epoch epoch_minted_addresses leases")

type b03NativeInput struct {
	Format                    string `json:"format"`
	Consumer                  string `json:"consumer"`
	ConsumerTree              string `json:"consumerTree"`
	TrustedPreparationControl struct {
		ConsumerRef            string `json:"consumerRef"`
		ConsumerCommit         string `json:"consumerCommit"`
		ConsumerTree           string `json:"consumerTree"`
		PreparerPath           string `json:"preparerPath"`
		PreparerCommit         string `json:"preparerCommit"`
		PreparerTree           string `json:"preparerTree"`
		PreparerSHA256         string `json:"preparerSHA256"`
		WorkflowSHA            string `json:"workflowSHA"`
		WorkflowBranch         string `json:"workflowBranch"`
		WorkflowLabel          string `json:"workflowLabel"`
		ReadinessReviewRecord  string `json:"readinessReviewRecord"`
		ReadinessReviewSHA256  string `json:"readinessReviewSHA256"`
		ReadinessReviewFixture string `json:"readinessReviewFixture"`
	} `json:"trustedPreparationControl"`
	Source         string `json:"source"`
	SourceTree     string `json:"sourceTree"`
	SourceManifest string `json:"sourceManifest"`
	SourceEntries  int    `json:"sourceEntries"`
	BinarySHA256   string `json:"binarySHA256"`
	BinaryBytes    int64  `json:"binaryBytes"`
	BuildRun       string `json:"buildRun"`
	Profile        string `json:"profile"`
	ManifestSHA256 string `json:"manifestSHA256"`
	WorkflowSHA    string `json:"workflowSHA"`
	WorkflowBranch string `json:"workflowBranch"`
	Label          string `json:"label"`
	Descriptor     string `json:"descriptor"`
	ArtifactDigest string `json:"artifactDigest"`
	ArtifactSize   int64  `json:"artifactSize"`
	Catalog        struct {
		Status string `json:"status"`
		Type   string `json:"type"`
	} `json:"catalog"`
	Stores []struct {
		ID       string `json:"id"`
		Database string `json:"database"`
		Prefix   string `json:"prefix"`
		RootID   string `json:"rootID"`
	} `json:"stores"`
	SQLTables string `json:"sqlTables"`
}

type b03Manifest struct {
	Schema string `json:"schema"`
	Source map[string]struct {
		Commit          string `json:"commit"`
		Tree            string `json:"tree"`
		ManifestSHA256  string `json:"manifest_sha256"`
		ManifestEntries int    `json:"manifest_entries"`
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
		RunID string `json:"run_id"`
	} `json:"build"`
	Workflow struct {
		Ref     string `json:"ref"`
		SHA     string `json:"sha"`
		Profile string `json:"profile"`
	} `json:"workflow"`
	Archive struct {
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	} `json:"archive"`
}

type b03CommandReceipt struct {
	Label        string   `json:"label"`
	Store        string   `json:"store"`
	Argv         []string `json:"argv"`
	Directory    string   `json:"directory"`
	Environment  []string `json:"environment"`
	StdoutSHA256 string   `json:"stdout_sha256"`
	StderrSHA256 string   `json:"stderr_sha256"`
	Exit         int      `json:"exit"`
	StdoutPath   string   `json:"stdout_path"`
	StderrPath   string   `json:"stderr_path"`
}

type b03CommandRunner struct {
	t        testing.TB
	root     string
	store    string
	dir      string
	env      []string
	commands []b03CommandReceipt
}

func (r *b03CommandRunner) run(label string, argv []string) ([]byte, b03CommandReceipt) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir, cmd.Env = r.dir, append([]string(nil), r.env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exit = exitErr.ExitCode()
		}
	}
	sequence := len(r.commands) + 1
	base := fmt.Sprintf("%03d-%s", sequence, strings.NewReplacer("/", "_", " ", "_").Replace(label))
	stdoutPath := filepath.Join(r.root, base+".stdout")
	stderrPath := filepath.Join(r.root, base+".stderr")
	if writeErr := os.WriteFile(stdoutPath, stdout.Bytes(), 0o600); writeErr != nil {
		r.t.Fatal(writeErr)
	}
	if writeErr := os.WriteFile(stderrPath, stderr.Bytes(), 0o600); writeErr != nil {
		r.t.Fatal(writeErr)
	}
	receipt := b03CommandReceipt{Label: label, Store: r.store, Argv: append([]string(nil), argv...), Directory: r.dir, Environment: append([]string(nil), r.env...), StdoutSHA256: b03Hash(stdout.Bytes()), StderrSHA256: b03Hash(stderr.Bytes()), Exit: exit, StdoutPath: stdoutPath, StderrPath: stderrPath}
	r.commands = append(r.commands, receipt)
	r.writeReceipts()
	return stdout.Bytes(), receipt
}

func (r *b03CommandRunner) require(label string, argv []string) []byte {
	output, receipt := r.run(label, argv)
	if receipt.Exit != 0 {
		stderr, _ := os.ReadFile(receipt.StderrPath)
		r.t.Fatalf("native command %s failed exit=%d: %s", label, receipt.Exit, stderr)
	}
	return output
}

func (r *b03CommandRunner) writeReceipts() {
	r.t.Helper()
	data, err := json.MarshalIndent(r.commands, "", "  ")
	if err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.root, "commands.json"), append(data, '\n'), 0o600); err != nil {
		r.t.Fatal(err)
	}
}

// runRace starts two independent read-only children at the same time. Their
// exact raw responses remain separate; equal normalized SQL rows prove that
// concurrent catalog observation did not change the physical source.
func (r *b03CommandRunner) runRace(label string, commands map[string][]string) map[string][]byte {
	r.t.Helper()
	type result struct {
		label  string
		argv   []string
		stdout []byte
		stderr []byte
		exit   int
		err    error
	}
	results := make(chan result, len(commands))
	var wg sync.WaitGroup
	for name, argv := range commands {
		name, argv := name, append([]string(nil), argv...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
			cmd.Dir, cmd.Env = r.dir, append([]string(nil), r.env...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			exit := 0
			if err != nil {
				exit = -1
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) {
					exit = exitErr.ExitCode()
				}
			}
			results <- result{label: name, argv: argv, stdout: stdout.Bytes(), stderr: stderr.Bytes(), exit: exit, err: err}
		}()
	}
	wg.Wait()
	close(results)
	ordered := make([]result, 0, len(commands))
	for value := range results {
		ordered = append(ordered, value)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].label < ordered[j].label })
	out := map[string][]byte{}
	var failures []string
	for _, value := range ordered {
		if value.err != nil || value.exit != 0 {
			failures = append(failures, fmt.Sprintf("%s exit=%d error=%v", value.label, value.exit, value.err))
		}
		sequence := len(r.commands) + 1
		base := fmt.Sprintf("%03d-%s-%s", sequence, label, value.label)
		stdoutPath := filepath.Join(r.root, base+".stdout")
		stderrPath := filepath.Join(r.root, base+".stderr")
		if err := os.WriteFile(stdoutPath, value.stdout, 0o600); err != nil {
			r.t.Fatal(err)
		}
		if err := os.WriteFile(stderrPath, value.stderr, 0o600); err != nil {
			r.t.Fatal(err)
		}
		r.commands = append(r.commands, b03CommandReceipt{Label: label + "/" + value.label, Store: r.store, Argv: value.argv, Directory: r.dir, Environment: append([]string(nil), r.env...), StdoutSHA256: b03Hash(value.stdout), StderrSHA256: b03Hash(value.stderr), Exit: value.exit, StdoutPath: stdoutPath, StderrPath: stderrPath})
		out[value.label] = value.stdout
	}
	r.writeReceipts()
	if len(failures) != 0 {
		r.t.Fatalf("native race controls failed (raw streams retained): %s", strings.Join(failures, "; "))
	}
	return out
}

type b03StoreEvidence struct {
	ID                    string                `json:"id"`
	Database              string                `json:"database"`
	Prefix                string                `json:"prefix"`
	RootID                string                `json:"root_id"`
	InventoryStoreID      string                `json:"inventory_store_id,omitempty"`
	InventoryPath         string                `json:"inventory_path,omitempty"`
	InventorySHA256       string                `json:"inventory_sha256,omitempty"`
	InventoryBytes        int                   `json:"inventory_bytes,omitempty"`
	InventorySources      []b03InventorySource  `json:"inventory_sources,omitempty"`
	ArchivePath           string                `json:"archive_path,omitempty"`
	ArchiveSHA256         string                `json:"archive_sha256,omitempty"`
	ArchiveBytes          int64                 `json:"archive_bytes,omitempty"`
	ArchiveMetadata       map[string]any        `json:"archive_metadata,omitempty"`
	NormalizedCatalogSHA  string                `json:"normalized_catalog_sha256,omitempty"`
	NormalizedCatalogPath string                `json:"normalized_catalog_path,omitempty"`
	SQLCensusBeforePath   string                `json:"sql_census_before_path,omitempty"`
	SQLCensusBeforeSHA256 string                `json:"sql_census_before_sha256,omitempty"`
	SQLCensusAfterPath    string                `json:"sql_census_after_path,omitempty"`
	SQLCensusAfterSHA256  string                `json:"sql_census_after_sha256,omitempty"`
	SQLTables             []b03SQLTableEvidence `json:"sql_tables,omitempty"`
	Replay                map[string]any        `json:"replay,omitempty"`
	Blockers              []string              `json:"blockers,omitempty"`
	Commands              []b03CommandReceipt   `json:"commands"`
}

type b03InventorySource struct {
	Family string `json:"family"`
	State  string `json:"state"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int    `json:"bytes"`
}

type b03SQLTableEvidence struct {
	Table     string `json:"table"`
	RawPath   string `json:"raw_path"`
	RawSHA256 string `json:"raw_sha256"`
	RawBytes  int    `json:"raw_bytes"`
	Rows      any    `json:"rows"`
}

type b03PhysicalCensus struct {
	Tables []b03SQLTableEvidence `json:"tables"`
	Path   string                `json:"path"`
	SHA256 string                `json:"sha256"`
	Bytes  int                   `json:"bytes"`
}

// TestB03NativeCatalogRecording retains and verifies the complete custom
// catalogs while recording a root with a producer-supported built-in status.
func TestB03NativeCatalogRecording(t *testing.T) {
	input := readB03NativeInput(t)
	verifyB03NativeInput(t, input)
	root := b03OutputRoot(t)
	binary, manifest := verifyB03PreparedArtifact(t, input, root)
	server := publicnative.StartDolt(t, root)

	all := make([]b03StoreEvidence, 0, len(input.Stores))
	for _, spec := range input.Stores {
		all = append(all, recordB03NativeStore(t, root, input, spec.ID, spec.Database, spec.Prefix, spec.RootID, binary, server))
	}
	writeB03JSON(t, filepath.Join(root, "stores.json"), map[string]any{
		"schema":   "plug-s1-b03-native-recording/1",
		"consumer": input.Consumer, "consumer_tree": input.ConsumerTree,
		"descriptor": input.Descriptor, "prepared_manifest": manifest,
		"stores": all,
	})
	var blockers []string
	var archives []string
	for _, evidence := range all {
		blockers = append(blockers, evidence.Blockers...)
		if evidence.ArchivePath != "" {
			archives = append(archives, evidence.ArchivePath)
		}
	}
	if len(blockers) != 0 {
		t.Fatalf("PRODUCER_DEPENDENCY_B03_NATIVE_CATALOG: %s", strings.Join(blockers, " | "))
	}
	if len(archives) != len(input.Stores) {
		t.Fatalf("B03 native recording produced %d canonical archives, want %d", len(archives), len(input.Stores))
	}
	if len(all) == len(input.Stores) {
		wantNormalized := all[0].NormalizedCatalogSHA
		seenIDs := map[string]bool{}
		seenStores := map[string]bool{}
		seenNormalized := map[string]bool{}
		for _, evidence := range all {
			if evidence.RootID == "" || seenIDs[evidence.RootID] || evidence.InventoryStoreID == "" || seenStores[evidence.InventoryStoreID] {
				t.Fatalf("native stores did not retain distinct root IDs: %+v", all)
			}
			seenIDs[evidence.RootID] = true
			seenStores[evidence.InventoryStoreID] = true
			if evidence.NormalizedCatalogSHA == "" || evidence.NormalizedCatalogSHA != wantNormalized {
				t.Fatalf("normalized canonical catalog vectors diverged across distinct stores: %+v", all)
			}
			seenNormalized[evidence.NormalizedCatalogSHA] = true
		}
		if len(seenNormalized) != 1 {
			t.Fatalf("native stores did not produce one normalized catalog image: %+v", all)
		}
	}
	for i, path := range archives {
		all[i].Replay = replayB03Archive(t, root, input.Stores[i].ID, input.Stores[i].RootID, path, input)
	}
	compareB03ReplayVectors(t, root, all)
	writeB03JSON(t, filepath.Join(root, "stores.json"), map[string]any{
		"schema":   "plug-s1-b03-native-recording/1",
		"consumer": input.Consumer, "consumer_tree": input.ConsumerTree,
		"descriptor": input.Descriptor, "prepared_manifest": manifest,
		"stores": all,
	})
}

func readB03NativeInput(t *testing.T) b03NativeInput {
	t.Helper()
	var value b03NativeInput
	path := b03RunfilePath(b03NativeInputPath)
	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatalf("B03 native input JSON: %v", err)
			}
			return value
		}
	}
	t.Fatalf("B03 native input is unavailable")
	return value
}

// b03RunfilePath resolves a declared runfile without treating an absent
// optional local checkout path as a successful control.  The remote wrapper
// supplies TEST_SRCDIR/TEST_WORKSPACE and PLUG_SOURCE_MODULE_ROOT for the
// immutable candidate; the relative fallback is only useful for review tools.
func b03RunfilePath(name string) string {
	for _, path := range []string{
		filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), name),
		filepath.Join(os.Getenv("PLUG_SOURCE_MODULE_ROOT"), name),
		filepath.Join(".", name),
	} {
		if path != name && path != "." && path != "" {
			if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
				return path
			}
		}
	}
	return ""
}

func verifyB03NativeInput(t *testing.T, input b03NativeInput) {
	t.Helper()
	if input.Format != "plug.s1.b03.native-catalog-inputs.v2" || input.Consumer != "7a43c3c625a5c08d3a4364a321cd47d6357ef30e" || input.ConsumerTree != "be93a595dc4c44ef4d94d2333060788fd344dca0" || input.Source != b03NativeSource || input.SourceTree != b03NativeTree || input.Profile != b03NativeProfile || input.Descriptor != b03NativeDescriptor || input.WorkflowSHA != b03NativeWorkflow || input.WorkflowBranch != b03NativeBranch || input.BuildRun != b03NativeRun || input.ManifestSHA256 != b03NativeManifestSHA || input.BinarySHA256 != b03NativeBinarySHA || input.BinaryBytes != b03NativeBinarySize || input.SourceEntries != 5141 || input.ArtifactDigest != "sha256:28609d7c072c281a748cb53a361d9a3ffada826d6f2e8866a3b1e62b23d5ffc4" || input.ArtifactSize != 111646601 {
		t.Fatalf("B03 native input is not the accepted R418/current-main tuple: %+v", input)
	}
	c := input.TrustedPreparationControl
	if c.ConsumerRef != "refs/heads/main" || c.ConsumerCommit != input.Consumer || c.ConsumerTree != input.ConsumerTree || c.PreparerPath != b03NativePreparerPath || c.PreparerCommit != b03NativePreparerCommit || c.PreparerTree != b03NativePreparerTree || c.PreparerSHA256 != b03NativePreparerSHA || c.WorkflowSHA != b03NativeWorkflow || c.WorkflowBranch != b03NativeBranch || c.WorkflowLabel != "native-census-c8eb-20261104" || c.ReadinessReviewRecord == "" || c.ReadinessReviewFixture == "" || c.ReadinessReviewSHA256 != b03NativeReadinessSHA {
		t.Fatalf("B03 trusted preparation control is incomplete: %+v", c)
	}
	preparer := b03RunfilePath(c.PreparerPath)
	if preparer == "" || b03FileHash(preparer) != c.PreparerSHA256 {
		t.Fatalf("trusted preparer path/digest is not present in the candidate: path=%q", preparer)
	}
	readiness := b03RunfilePath(c.ReadinessReviewFixture)
	if readiness == "" || b03FileHash(readiness) != c.ReadinessReviewSHA256 {
		t.Fatalf("trusted readiness record fixture/digest is not present: path=%q", readiness)
	}
	if named := c.ReadinessReviewRecord; named != "" {
		if data, err := os.ReadFile(named); err == nil && b03Hash(data) != c.ReadinessReviewSHA256 {
			t.Fatalf("named readiness record bytes differ from accepted digest: %s", named)
		}
	}
	if strings.Join(strings.Fields(input.SQLTables), " ") != strings.Join(b03NativeTables, " ") || input.Catalog.Status != "awaiting_review:active,qa_testing:wip,on_hold:frozen,archived:done" || input.Catalog.Type != "delivery" || len(input.Stores) != 4 {
		t.Fatalf("B03 native catalog input does not retain the complete catalog/census: %+v", input)
	}
	seen := map[string]bool{}
	for _, store := range input.Stores {
		if store.ID == "" || store.Database == "" || store.Prefix == "" || store.RootID == "" || !strings.HasPrefix(store.RootID, store.Prefix+"-") || seen[store.ID] || seen[store.Database] || seen[store.Prefix] || seen[store.RootID] {
			t.Fatalf("B03 native stores do not have distinct physical identities: %+v", input.Stores)
		}
		seen[store.ID], seen[store.Database], seen[store.Prefix], seen[store.RootID] = true, true, true, true
	}
	if got := os.Getenv("PLUG_PUBLIC_BUNDLE"); got != input.Descriptor {
		t.Fatalf("native test descriptor=%q want accepted descriptor", got)
	}
}

func verifyB03PreparedArtifact(t *testing.T, input b03NativeInput, root string) (string, b03Manifest) {
	t.Helper()
	binary := os.Getenv("PLUG_PUBLIC_BEADS_BINARY")
	manifestPath := os.Getenv("PLUG_PUBLIC_BEADS_MANIFEST")
	if binary == "" || manifestPath == "" || os.Getenv("PLUG_PUBLIC_BEADS_SHA256") != input.BinarySHA256 || os.Getenv("PLUG_PUBLIC_BEADS_BUILD_RUN") != input.BuildRun || !filepath.IsAbs(binary) || !filepath.IsAbs(manifestPath) {
		t.Fatal("accepted public native preparation did not publish the four immutable executable values")
	}
	info, err := os.Stat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o755 || info.Size() != input.BinaryBytes || b03FileHash(binary) != input.BinarySHA256 {
		t.Fatalf("prepared native executable digest/size differs: %v", err)
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil || b03Hash(manifestBytes) != input.ManifestSHA256 {
		t.Fatalf("prepared native manifest digest differs: %v", err)
	}
	var manifest b03Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	beads := manifest.Source["beads"]
	artifact := manifest.Artifacts["bin/bd-export"]
	if manifest.Schema != input.Profile || beads.Commit != input.Source || beads.Tree != input.SourceTree || beads.ManifestSHA256 != input.SourceManifest || beads.ManifestEntries != input.SourceEntries || manifest.Build.RunID != input.BuildRun || manifest.Workflow.SHA != input.WorkflowSHA || manifest.Workflow.Ref != "refs/heads/"+input.WorkflowBranch || manifest.Workflow.Profile != input.Profile || artifact.SHA256 != input.BinarySHA256 || artifact.Size != input.BinaryBytes || artifact.Mode != "0o755" || manifest.Archive.SHA256 == "" || manifest.Archive.Size <= 0 {
		t.Fatalf("prepared native manifest is not the accepted source/binary/archive binding: %+v", manifest)
	}
	archivePath := filepath.Join(filepath.Dir(manifestPath), "bundle.tar")
	archiveInfo, archiveErr := os.Stat(archivePath)
	if archiveErr != nil || !archiveInfo.Mode().IsRegular() || archiveInfo.Size() != manifest.Archive.Size || b03FileHash(archivePath) != manifest.Archive.SHA256 {
		t.Fatalf("prepared canonical archive bytes differ from its manifest: path=%s err=%v", archivePath, archiveErr)
	}
	zipPath := filepath.Dir(manifestPath) + ".zip"
	zipInfo, zipErr := os.Stat(zipPath)
	if zipErr != nil || !zipInfo.Mode().IsRegular() || zipInfo.Size() != input.ArtifactSize || "sha256:"+b03FileHash(zipPath) != input.ArtifactDigest {
		t.Fatalf("prepared official ZIP bytes differ from admitted artifact: path=%s err=%v", zipPath, zipErr)
	}
	preparation := retainB03Preparation(t, input, root, manifestPath, binary, manifest)
	writeB03JSON(t, filepath.Join(root, "prepared-artifact-binding.json"), map[string]any{
		"descriptor":           input.Descriptor,
		"bundle_artifact":      map[string]any{"sha256": input.ArtifactDigest, "size": input.ArtifactSize, "kind": "official ZIP artifact"},
		"executable":           map[string]any{"path": binary, "sha256": input.BinarySHA256, "size": input.BinaryBytes},
		"manifest":             map[string]any{"path": manifestPath, "sha256": input.ManifestSHA256},
		"canonical_archive":    map[string]any{"path": archivePath, "sha256": manifest.Archive.SHA256, "size": manifest.Archive.Size, "kind": "prepared bundle.tar"},
		"same_run_preparation": preparation,
	})
	return binary, manifest
}

// These receipts are read only from the committed workflow's fixed sibling
// directory. No caller-selected helper, receipt path or admission is accepted.
// Historical downloaded runs lacking these files remain incomplete evidence.
func retainB03Preparation(t *testing.T, input b03NativeInput, root, manifestPath, binary string, manifest b03Manifest) map[string]any {
	t.Helper()
	evidenceDir := filepath.Join(filepath.Dir(filepath.Dir(manifestPath)), "remote-bazel-evidence")
	if override := os.Getenv("PLUG_PUBLIC_BUNDLE_EVIDENCE_DIR"); override != "" {
		if !filepath.IsAbs(override) {
			t.Fatalf("trusted preparation evidence override must be absolute: %q", override)
		}
		evidenceDir = override
	}
	bindingPath := filepath.Join(evidenceDir, "public-bundle-helper-binding.txt")
	binding, err := os.ReadFile(bindingPath)
	wantBinding := "trusted_preparer_sha=" + b03NativePreparerCommit + "\ntrusted_preparer_sha256=" + b03NativePreparerSHA + "\ndescriptor=" + input.Descriptor + "\n"
	if err != nil || string(binding) != wantBinding {
		t.Fatalf("same-run trusted preparation binding unavailable or differs: %s: %v", bindingPath, err)
	}
	preparationPath := filepath.Join(evidenceDir, "public-bundle.json")
	preparation, err := os.ReadFile(preparationPath)
	if err != nil {
		t.Fatalf("same-run accepted preparation receipt unavailable: %s: %v", preparationPath, err)
	}
	var receipt struct {
		b03Manifest
		Status              string            `json:"status"`
		Descriptor          string            `json:"descriptor"`
		ProducerRun         string            `json:"producer_run"`
		ProducerWorkflowSHA string            `json:"producer_workflow_sha"`
		ArtifactID          string            `json:"artifact_id"`
		ArtifactDigest      string            `json:"artifact_digest"`
		ManifestSHA256      string            `json:"manifest_sha256"`
		Environment         map[string]string `json:"environment"`
	}
	if err := json.Unmarshal(preparation, &receipt); err != nil {
		t.Fatal(err)
	}
	wantEnvironment := map[string]string{"PLUG_PUBLIC_BEADS_BINARY": binary, "PLUG_PUBLIC_BEADS_SHA256": input.BinarySHA256, "PLUG_PUBLIC_BEADS_BUILD_RUN": input.BuildRun, "PLUG_PUBLIC_BEADS_MANIFEST": manifestPath}
	if receipt.Status != "PASS" || receipt.Descriptor != input.Descriptor || receipt.ProducerRun != input.BuildRun || receipt.ProducerWorkflowSHA != input.WorkflowSHA || receipt.ArtifactID != "13307042737" || receipt.ArtifactDigest != input.ArtifactDigest || receipt.ManifestSHA256 != input.ManifestSHA256 || receipt.Schema != input.Profile || !equalB03JSON(receipt.Source, manifest.Source) || !equalB03JSON(receipt.Targets, manifest.Targets) || !equalB03JSON(receipt.Artifacts, manifest.Artifacts) || !equalB03JSON(receipt.Archive, manifest.Archive) || !equalB03JSON(receipt.Environment, wantEnvironment) {
		t.Fatal("same-run preparation does not match the admitted source/artifact/executable/manifest tuple")
	}
	result := map[string]any{}
	for name, content := range map[string][]byte{"public-bundle.json": preparation, "public-bundle-helper-binding.txt": binding} {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
		result[name] = map[string]any{"path": path, "sha256": b03Hash(content), "bytes": len(content), "workflow_path": filepath.Join(evidenceDir, name)}
	}
	return result
}

func recordB03NativeStore(t *testing.T, root string, input b03NativeInput, id, database, prefix, rootID, binary string, server *publicnative.Server) b03StoreEvidence {
	t.Helper()
	storeRoot := filepath.Join(root, id)
	workspace := filepath.Join(storeRoot, "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, "home", ".config"), 0o700); err != nil {
		t.Fatal(err)
	}
	baseEnv := []string{"PATH=/usr/bin:/bin", "HOME=" + filepath.Join(workspace, "home"), "XDG_CONFIG_HOME=" + filepath.Join(workspace, "home", ".config"), "LANG=C.UTF-8", "TZ=UTC", "CI=true", "DO_NOT_TRACK=1", "BEADS_NO_DAEMON=1", "BEADS_ACTOR=b03-native-recorder", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=B03 native recorder", "GIT_AUTHOR_EMAIL=b03@example.invalid", "GIT_COMMITTER_NAME=B03 native recorder", "GIT_COMMITTER_EMAIL=b03@example.invalid"}
	env := server.ChildEnv(baseEnv)
	runner := &b03CommandRunner{t: t, root: storeRoot, store: id, dir: workspace, env: env}
	evidence := b03StoreEvidence{ID: id, Database: database, Prefix: prefix, RootID: rootID}
	blockers := []string{}
	git := func(label string, args ...string) ([]byte, b03CommandReceipt) {
		return runner.run(label, append([]string{"git"}, args...))
	}
	command := func(label string, args ...string) ([]byte, b03CommandReceipt) {
		return runner.run(label, append([]string{binary}, args...))
	}
	if _, receipt := git("git-init", "-c", "init.defaultBranch=main", "init", "--quiet"); receipt.Exit != 0 {
		blockers = append(blockers, exactNativeRefusal(runner, receipt, "git init"))
		return finishB03Store(t, evidence, runner, blockers)
	}
	version, receipt := command("version", "version", "--json")
	if receipt.Exit != 0 {
		blockers = append(blockers, exactNativeRefusal(runner, receipt, "version"))
		return finishB03Store(t, evidence, runner, blockers)
	}
	var versionObject map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(version), &versionObject); err != nil || fmt.Sprint(versionObject["commit"]) != input.Source {
		blockers = append(blockers, fmt.Sprintf("native version identity mismatch store=%s output=%q", id, version))
		return finishB03Store(t, evidence, runner, blockers)
	}
	initArgs := []string{"init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(server.Port), "--database", database, "--prefix", prefix, "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive"}
	if _, receipt = command("init", initArgs...); receipt.Exit != 0 {
		blockers = append(blockers, exactNativeRefusal(runner, receipt, "native init"))
		return finishB03Store(t, evidence, runner, blockers)
	}
	databaseDir := server.RequireDatabaseDir(t, database)
	writeB03JSON(t, filepath.Join(storeRoot, "server-binding.json"), map[string]any{"database": database, "database_directory": databaseDir, "server": map[string]any{"pid": server.PID, "port": server.Port, "data_root": server.DataRoot}})
	if _, receipt = command("config-export-auto", "--json", "config", "set", "export.auto", "false"); receipt.Exit != 0 {
		blockers = append(blockers, exactNativeRefusal(runner, receipt, "export.auto config"))
	}
	if _, receipt = command("status-catalog", "--json", "config", "set", "status.custom", input.Catalog.Status); receipt.Exit != 0 {
		blockers = append(blockers, exactNativeRefusal(runner, receipt, "status catalog"))
	}
	// `types.custom=delivery` is the native producer's actual catalog input. A
	// task fallback would be a synthetic substitute, so a refusal is retained
	// and never relabelled as B03 success.
	_, typeReceipt := command("type-catalog", "--json", "config", "set", "types.custom", input.Catalog.Type)
	if typeReceipt.Exit != 0 {
		blockers = append(blockers, exactNativeRefusal(runner, typeReceipt, "type catalog"))
	} else {
		_, createReceipt := command("catalog-root-create", "--json", "create", "--id", rootID, "--type", input.Catalog.Type, "--status", "open", "--title", "B03 native catalog root", "--description", "fresh native catalog source")
		if createReceipt.Exit != 0 {
			blockers = append(blockers, exactNativeRefusal(runner, createReceipt, "catalog root create"))
		} else {
			row, showReceipt := command("catalog-root-show", "--json", "show", "--include-comments", rootID)
			if showReceipt.Exit != 0 {
				blockers = append(blockers, exactNativeRefusal(runner, showReceipt, "catalog root show"))
			} else {
				var shown []struct {
					ID     string `json:"id"`
					Status string `json:"status"`
					Type   string `json:"issue_type"`
				}
				if err := json.Unmarshal(row, &shown); err != nil || len(shown) != 1 || shown[0].ID != rootID || shown[0].Status != "open" || shown[0].Type != input.Catalog.Type {
					blockers = append(blockers, fmt.Sprintf("native catalog root did not retain requested ID/status/type store=%s raw=%s", id, row))
				}
			}
		}
	}
	if _, receipt = command("config-list", "--json", "config", "list"); receipt.Exit != 0 {
		blockers = append(blockers, exactNativeRefusal(runner, receipt, "config list"))
	}
	before := recordB03PhysicalCensus(t, runner, binary, "before")
	// These source catalog rows are acceptance inputs, never live-claim cleanup.
	wantStatuses := []any{}
	for _, declaration := range strings.Split(input.Catalog.Status, ",") {
		name, category, ok := strings.Cut(declaration, ":")
		if !ok || name == "" || category == "" {
			t.Fatalf("invalid fixture status declaration %q", declaration)
		}
		wantStatuses = append(wantStatuses, map[string]any{"name": name, "category": category})
	}
	wantCatalogs := map[string]any{
		"custom_statuses": normalizeB03Rows(wantStatuses),
		"custom_types":    []any{map[string]any{"name": input.Catalog.Type}},
	}
	for _, table := range before.Tables {
		if want, ok := wantCatalogs[table.Table]; ok {
			if !equalB03JSON(table.Rows, want) {
				t.Fatalf("source %s catalog differs: got=%v want=%v", table.Table, table.Rows, want)
			}
			delete(wantCatalogs, table.Table)
		}
	}
	if len(wantCatalogs) != 0 {
		t.Fatalf("source catalog census missing tables: %v", wantCatalogs)
	}
	inventoryPath := filepath.Join(storeRoot, "native-inventory.json")
	_, inventoryReceipt := command("inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventoryPath)
	if inventoryReceipt.Exit == 0 {
		if data, err := os.ReadFile(inventoryPath); err != nil || len(data) == 0 {
			blockers = append(blockers, fmt.Sprintf("native inventory is empty store=%s err=%v", id, err))
		} else {
			evidence.InventoryPath, evidence.InventorySHA256, evidence.InventoryBytes = inventoryPath, b03Hash(data), len(data)
			storeID, sources, err := validateB03Inventory(inventoryPath, prefix, data)
			if err != nil {
				blockers = append(blockers, fmt.Sprintf("native inventory closure store=%s: %v", id, err))
			} else {
				evidence.InventoryStoreID, evidence.InventorySources = storeID, sources
			}
		}
	}
	archivePath := filepath.Join(storeRoot, "native-plug-dump.tar")
	_, archiveReceipt := command("canonical-plug-dump", "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "-o", archivePath)
	if archiveReceipt.Exit != 0 {
		stderr, _ := os.ReadFile(archiveReceipt.StderrPath)
		if len(stderr) == 0 {
			blockers = append(blockers, exactNativeRefusal(runner, archiveReceipt, "canonical plug-dump"))
		} else {
			blockers = append(blockers, fmt.Sprintf("canonical plug-dump refusal store=%s exit=%d stderr=%q receipt=%s", id, archiveReceipt.Exit, string(stderr), archiveReceipt.StderrPath))
		}
	} else if inventoryReceipt.Exit != 0 {
		blockers = append(blockers, exactNativeRefusal(runner, inventoryReceipt, "native inventory"))
	} else if archive, err := dumpformat.ReadFile(archivePath); err != nil {
		blockers = append(blockers, fmt.Sprintf("canonical plug-dump was not a valid archive store=%s err=%v", id, err))
	} else {
		if err := validateB03CanonicalArchive(input, archive, archivePath, rootID, prefix, evidence.InventoryStoreID); err != nil {
			blockers = append(blockers, fmt.Sprintf("canonical catalog metadata refused store=%s: %v", id, err))
		} else {
			info, statErr := os.Stat(archivePath)
			if statErr != nil {
				blockers = append(blockers, fmt.Sprintf("canonical archive stat failed store=%s: %v", id, statErr))
			} else {
				evidence.ArchivePath, evidence.ArchiveSHA256, evidence.ArchiveBytes = archivePath, archive.SHA256(), info.Size()
				evidence.ArchiveMetadata = b03ArchiveMetadata(archive.Manifest())
				vector := marshalB03CatalogVector(archive, rootID)
				evidence.NormalizedCatalogPath = filepath.Join(storeRoot, "canonical-catalog-correspondence.json")
				if err := os.WriteFile(evidence.NormalizedCatalogPath, vector, 0o600); err != nil {
					t.Fatal(err)
				}
				evidence.NormalizedCatalogSHA = b03Hash(vector)
			}
		}
	}
	after := recordB03PhysicalCensus(t, runner, binary, "after")
	evidence.SQLCensusBeforePath, evidence.SQLCensusBeforeSHA256 = before.Path, before.SHA256
	evidence.SQLCensusAfterPath, evidence.SQLCensusAfterSHA256 = after.Path, after.SHA256
	evidence.SQLTables = after.Tables
	// The command receipts intentionally use fresh paths for each census.  A
	// read-only export must be compared by the physical table bytes and parsed
	// rows, while both phase-specific raw paths remain in the immutable receipt.
	// Comparing the full structs here would turn those expected path changes
	// into a false mutation blocker.
	if !equalB03JSON(normalizeB03Census(before.Tables), normalizeB03Census(after.Tables)) {
		blockers = append(blockers, fmt.Sprintf("read-only export changed the complete SQL census store=%s", id))
	}
	race := runner.runRace("catalog-read-race", map[string][]string{
		"left":  {binary, "--readonly", "--json", "sql", "SELECT * FROM `custom_statuses` ORDER BY `name`"},
		"right": {binary, "--readonly", "--json", "sql", "SELECT * FROM `custom_statuses` ORDER BY `name`"},
	})
	if !equalB03JSON(parseNativeJSON(t, race["left"]), parseNativeJSON(t, race["right"])) {
		blockers = append(blockers, fmt.Sprintf("concurrent catalog SQL observations diverged store=%s", id))
	}
	return finishB03Store(t, evidence, runner, blockers)
}

func finishB03Store(t *testing.T, evidence b03StoreEvidence, runner *b03CommandRunner, blockers []string) b03StoreEvidence {
	t.Helper()
	evidence.Blockers = append([]string(nil), blockers...)
	evidence.Commands = append([]b03CommandReceipt(nil), runner.commands...)
	writeB03JSON(t, filepath.Join(runner.root, "store-evidence.json"), evidence)
	return evidence
}

func recordB03PhysicalCensus(t *testing.T, runner *b03CommandRunner, binary, phase string) b03PhysicalCensus {
	t.Helper()
	image := make([]b03SQLTableEvidence, 0, len(b03NativeTables))
	for _, table := range b03NativeTables {
		raw, receipt := runner.run("sql-census/"+table, []string{binary, "--readonly", "--json", "sql", "SELECT * FROM `" + table + "`"})
		if receipt.Exit != 0 {
			t.Fatalf("complete native SQL census table=%s failed: %s", table, raw)
		}
		rows := parseNativeJSON(t, raw)
		if _, ok := rows.([]any); !ok {
			t.Fatalf("native SQL census table=%s is not an array", table)
		}
		image = append(image, b03SQLTableEvidence{Table: table, RawPath: receipt.StdoutPath, RawSHA256: receipt.StdoutSHA256, RawBytes: len(raw), Rows: normalizeB03Rows(rows)})
	}
	path := filepath.Join(runner.root, "sql-census-"+phase+".json")
	writeB03JSON(t, path, map[string]any{"schema": "plug-s1-b03-sql-census/1", "store": runner.store, "phase": phase, "tables": image})
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return b03PhysicalCensus{Tables: image, Path: path, SHA256: b03FileHash(path), Bytes: int(info.Size())}
}

func normalizeB03Census(tables []b03SQLTableEvidence) []b03SQLTableEvidence {
	result := make([]b03SQLTableEvidence, len(tables))
	for i, table := range tables {
		result[i] = b03SQLTableEvidence{
			Table:     table.Table,
			RawSHA256: table.RawSHA256,
			RawBytes:  table.RawBytes,
			Rows:      table.Rows,
		}
	}
	return result
}

func validateB03Inventory(path, prefix string, data []byte) (string, []b03InventorySource, error) {
	var value struct {
		Version         string               `json:"version"`
		StoreID         string               `json:"store_id"`
		Machine         string               `json:"machine"`
		History         string               `json:"history"`
		Projects        []dumpformat.Project `json:"projects"`
		Quiesced        bool                 `json:"quiesced"`
		RemotesMerged   bool                 `json:"remotes_merged"`
		WorkingSetClean bool                 `json:"working_set_clean"`
		External        []struct {
			Family string `json:"family"`
			Path   string `json:"path"`
			State  string `json:"state"`
		} `json:"external"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return "", nil, err
	}
	if value.Version != "beads-inventory/1" || value.StoreID == "" || value.Machine == "" || value.History == "" || !value.Quiesced || !value.RemotesMerged || !value.WorkingSetClean || len(value.Projects) != 1 || value.Projects[0].Prefix != prefix || len(value.External) == 0 {
		return "", nil, errors.New("native inventory metadata does not bind the owned quiesced store/project")
	}
	var sources []b03InventorySource
	seen := map[string]bool{}
	for _, entry := range value.External {
		if entry.Family == "" || seen[entry.Family] || (entry.State != "json-config" && entry.State != "absent") || !filepath.IsAbs(entry.Path) || filepath.Dir(entry.Path) != path+".sources" {
			return "", nil, fmt.Errorf("native inventory sidecar is not closed over owned output: %+v", entry)
		}
		seen[entry.Family] = true
		if entry.State == "absent" {
			if _, err := os.Lstat(entry.Path); !errors.Is(err, os.ErrNotExist) {
				return "", nil, fmt.Errorf("absent native sidecar exists: %s", entry.Path)
			}
			sources = append(sources, b03InventorySource{Family: entry.Family, State: entry.State, Path: entry.Path})
			continue
		}
		info, err := os.Lstat(entry.Path)
		if err != nil || !info.Mode().IsRegular() {
			return "", nil, fmt.Errorf("native sidecar is not a regular file: %s", entry.Path)
		}
		content, err := os.ReadFile(entry.Path)
		if err != nil || !json.Valid(content) {
			return "", nil, fmt.Errorf("native sidecar is not JSON: %s", entry.Path)
		}
		sources = append(sources, b03InventorySource{Family: entry.Family, State: entry.State, Path: entry.Path, SHA256: b03Hash(content), Bytes: len(content)})
	}
	return value.StoreID, sources, nil
}

func b03CatalogRows(archive *dumpformat.Archive, table string) []dumpformat.Record {
	var result []dumpformat.Record
	for _, row := range archive.Records("beads-archive") {
		if row.Source.Table == table {
			result = append(result, row)
		}
	}
	return result
}

func validateB03CanonicalArchive(input b03NativeInput, archive *dumpformat.Archive, path, rootID, prefix, storeID string) error {
	if archive == nil || archive.Native() {
		return errors.New("canonical plug-dump must be a non-native plug-dump/1 archive")
	}
	data := archive.TarBytes()
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("archive stat: %w", err)
	}
	if info.Size() != int64(len(data)) || b03Hash(data) != archive.SHA256() {
		return fmt.Errorf("archive bytes do not match validated tar: file=%d validated=%d", info.Size(), len(data))
	}
	manifest := archive.Manifest()
	if manifest.Format != dumpformat.Format || manifest.Schema != 1 || manifest.Reference != dumpformat.BeadsReference || manifest.Scope != dumpformat.Scope || manifest.StoreID == "" || manifest.Machine == "" || manifest.MainVersion != 72 || manifest.IgnoredVersion != 28 || len(manifest.Warnings) != 2 || len(manifest.Files) == 0 || len(manifest.Kinds) == 0 || len(manifest.Sources) == 0 || len(manifest.Verification) == 0 {
		return fmt.Errorf("archive manifest metadata is incomplete: %+v", b03ArchiveMetadata(manifest))
	}
	if manifest.StoreID != storeID || len(manifest.Projects) != 1 || manifest.Projects[0].Prefix != prefix {
		return fmt.Errorf("archive identity/project does not match its own native inventory: %+v", manifest.Projects)
	}
	if _, ok := archive.ImportMigration(); !ok {
		return errors.New("canonical native catalog lacks the production strict-union ImportMigration; no input-derived document may substitute")
	}
	beads := archive.Records("bead")
	if len(beads) != 1 || beads[0].ID != rootID || beads[0].Project != prefix || beads[0].Source.Table != "issues" || beads[0].Source.Fields["status"] != "open" || beads[0].Source.Fields["issue_type"] != input.Catalog.Type {
		return fmt.Errorf("canonical native root ID/project/status/type differs from physical source: %+v", beads)
	}
	statusWant := map[string]string{}
	for _, item := range strings.Split(input.Catalog.Status, ",") {
		name, category, ok := strings.Cut(item, ":")
		if !ok || name == "" || category == "" {
			return fmt.Errorf("invalid input status declaration %q", item)
		}
		statusWant[name] = category
	}
	statusRows := b03CatalogRows(archive, "custom_statuses")
	if len(statusRows) != len(statusWant) {
		return fmt.Errorf("custom status row count=%d want=%d", len(statusRows), len(statusWant))
	}
	seenStatus := map[string]bool{}
	for _, row := range statusRows {
		if row.Kind != "beads-archive" || row.Source.Table != "custom_statuses" || !equalB03JSON(row.Body, map[string]any{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false}) || len(row.Source.Fields) != 2 {
			return fmt.Errorf("custom status row metadata is not canonical: %+v", row)
		}
		name, nameOK := row.Source.Fields["name"].(string)
		category, categoryOK := row.Source.Fields["category"].(string)
		if !nameOK || !categoryOK || statusWant[name] != category || seenStatus[name] {
			return fmt.Errorf("custom status value/category mismatch: %+v", row.Source.Fields)
		}
		seenStatus[name] = true
	}
	if len(seenStatus) != len(statusWant) {
		return fmt.Errorf("custom status catalog omitted a declared value")
	}
	typeRows := b03CatalogRows(archive, "custom_types")
	if len(typeRows) != 1 {
		return fmt.Errorf("custom type row count=%d want=1", len(typeRows))
	}
	typeRow := typeRows[0]
	if typeRow.Kind != "beads-archive" || typeRow.Source.Table != "custom_types" || !equalB03JSON(typeRow.Body, map[string]any{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false}) || len(typeRow.Source.Fields) != 1 || typeRow.Source.Fields["name"] != input.Catalog.Type {
		return fmt.Errorf("custom type value/metadata mismatch: %+v", typeRow)
	}
	for _, want := range []string{"custom_statuses", "custom_types"} {
		found := false
		for _, disposition := range manifest.Sources {
			if disposition.Family == want {
				found = true
				if disposition.Rows != len(b03CatalogRows(archive, want)) || disposition.Reason == "" || disposition.Action != "carry" {
					return fmt.Errorf("catalog disposition incomplete for %s: %+v", want, disposition)
				}
			}
		}
		if !found {
			return fmt.Errorf("catalog disposition missing for %s", want)
		}
	}
	return nil
}

func b03ArchiveMetadata(manifest dumpformat.Manifest) map[string]any {
	return map[string]any{
		"format": manifest.Format, "schema": manifest.Schema, "reference": manifest.Reference,
		"store_id": manifest.StoreID, "machine": manifest.Machine, "scope": manifest.Scope,
		"history": manifest.History, "main_version": manifest.MainVersion,
		"ignored_version": manifest.IgnoredVersion, "projects": manifest.Projects,
		"kinds": manifest.Kinds, "files": manifest.Files, "sources": manifest.Sources, "ontology": manifest.Ontology,
		"warnings": manifest.Warnings, "verification": manifest.Verification,
	}
}

func normalizeB03Rows(value any) any {
	rows, ok := value.([]any)
	if !ok {
		return value
	}
	rows = append([]any(nil), rows...)
	sort.SliceStable(rows, func(i, j int) bool {
		left, _ := json.Marshal(rows[i])
		right, _ := json.Marshal(rows[j])
		return bytes.Compare(left, right) < 0
	})
	return rows
}

// The correspondence vector compares B03 semantics, not unrelated source
// configuration paths or producer timestamps. Full validated archives, their
// metadata, and every physical table byte remain independently retained.
func marshalB03CatalogVector(archive *dumpformat.Archive, rootID string) []byte {
	rows := []any{}
	for _, table := range []string{"custom_statuses", "custom_types"} {
		for _, row := range b03CatalogRows(archive, table) {
			rows = append(rows, map[string]any{"table": table, "fields": row.Source.Fields, "metadata": row.Body})
		}
	}
	rows = normalizeB03Rows(rows).([]any)
	var root map[string]any
	for _, row := range archive.Records("bead") {
		if row.ID == rootID {
			root = map[string]any{"id": "$ROOT", "project": "$PROJECT", "type": row.Source.Fields["issue_type"], "status": row.Source.Fields["status"], "title": row.Source.Fields["title"], "description": row.Source.Fields["description"]}
		}
	}
	data, _ := json.Marshal(map[string]any{"schema": "plug-s1-b03-catalog-correspondence/1", "catalog": rows, "root": root})
	return data
}

func replayB03Archive(t *testing.T, root, id, rootID, path string, input b03NativeInput) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("B03 native archive %s inspection: %v", id, err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	migration, ok := archive.ImportMigration()
	if !ok {
		t.Fatal("native archive has no admitted production catalog migration")
	}
	// Only the production validator may derive the custom document. The input
	// manifest supplies expected values, never a replacement ontology fixture.
	document := migration.Document()
	project := archive.Manifest().Projects[0].Prefix
	var nativeRoot dumpformat.Record
	for _, row := range archive.Records("bead") {
		if row.ID == rootID && row.Project == project {
			nativeRoot = row
		}
	}
	if nativeRoot.ID == "" {
		t.Fatal("validated archive omitted its actual native root")
	}
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "b03-native-reviewer"}, Authority: "restore-admin", AdmissionDigest: b03Hash([]byte("s1-b03-native-admission"))}, SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: "s1-b03-native-" + id, ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot}
	replayRoot := filepath.Join(root, id, "replay")
	if err := os.MkdirAll(replayRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	documentPath := filepath.Join(replayRoot, "archive-import-document.json")
	writeB03JSON(t, documentPath, document)
	writeB03JSON(t, filepath.Join(replayRoot, "archive-native-root.json"), nativeRoot)
	receipts := map[string]any{"archive_sha256": inspection.ArchiveSHA256, "source_store_id": inspection.SourceStoreID, "derived_sha256": inspection.DerivedSHA256, "import_document_path": documentPath, "import_document_sha256": b03FileHash(documentPath), "native_root_id": rootID, "native_project": project, "engines": map[string]any{}}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(id+"/replay/"+engine, func(t *testing.T) {
			engineRoot := filepath.Join(replayRoot, engine)
			if err := os.MkdirAll(engineRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			store, dsn, reopen := openB03ReplayStore(t, engine)
			t.Cleanup(func() { _ = store.Close() })
			if _, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), store, profile); err != nil {
				t.Fatalf("native archive replay on %s: %v", engine, err)
			}
			imported := b03ReplayDocumentAt(t, store, rootID)
			if imported.Project != project || imported.Type != input.Catalog.Type || imported.Fields["status"] != "todo" {
				t.Fatalf("restore lost actual native root/project/catalog: %+v", imported)
			}
			assertB03ImportedSource(t, imported, nativeRoot)
			writeB03JSON(t, filepath.Join(engineRoot, "imported-root.json"), imported)
			if err := bootstrapB03ReplaySource(ctx, store, id); err != nil {
				_ = store.Close()
				t.Fatalf("bootstrap built-in source on %s: %v", engine, err)
			}
			// SQL startup must own a real close/open, not another handle to an
			// author-seeded persistent runtime. Memory uses its explicit seam.
			if reopen != nil {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			}
			runtime, transport := openB03ReplayRuntime(t, store, dsn, engine, id, project)
			rpc := &b03ReplayRPC{t: t, transport: transport, root: engineRoot, project: project}
			lifecycle := exerciseB03ReplayRuntime(t, rpc, runtime.Store, input, rootID, document, imported)
			beforeClose := b03ReplaySnapshot(t, runtime.Store, project, rpc.calls)
			writeB03JSON(t, filepath.Join(engineRoot, "before-close.json"), beforeClose)
			if err := runtime.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			engineEvidence := map[string]any{"engine": engine, "lifecycle": lifecycle, "before_close_sha256": b03FileHash(filepath.Join(engineRoot, "before-close.json"))}
			if reopen != nil {
				runtime, transport = openB03ReplayRuntime(t, nil, dsn, engine, id, project)
				defer runtime.Close(context.Background())
				afterReopen := b03ReplaySnapshot(t, runtime.Store, project, rpc.calls)
				writeB03JSON(t, filepath.Join(engineRoot, "after-reopen.json"), afterReopen)
				if !equalB03JSON(beforeClose, afterReopen) {
					t.Fatalf("%s reopen changed complete records/versions/log/derived/allocator/receipt census", engine)
				}
				engineEvidence["after_reopen_sha256"] = b03FileHash(filepath.Join(engineRoot, "after-reopen.json"))
				rpc.transport = transport
				reopenedRoot := b03ReplayPublicRoot(t, rpc, runtime.Store, rootID, input.Catalog.Type, "awaiting_review")
				assertB03ImportedConservation(t, imported, reopenedRoot)
				b03ReplayReady(t, rpc, rootID, true)
				b03ReplayCatalog(t, rpc, lifecycle["document"].(ontology.Document))
				engineEvidence["durable_reopen"] = "exact census conserved and installed runtime reloaded"
			} else {
				engineEvidence["durable_reopen"] = "not applicable to volatile memory store"
			}
			rawPath := filepath.Join(replayRoot, engine+".json")
			writeB03JSON(t, rawPath, engineEvidence)
			if info, err := os.Stat(rawPath); err == nil {
				engineEvidence["receipt_path"] = rawPath
				engineEvidence["receipt_sha256"] = b03FileHash(rawPath)
				engineEvidence["receipt_bytes"] = info.Size()
			}
			receipts["engines"].(map[string]any)[engine] = engineEvidence
		})
	}
	replayEvidencePath := filepath.Join(replayRoot, "replay-evidence.json")
	writeB03JSON(t, replayEvidencePath, receipts)
	receipts["path"], receipts["sha256"] = replayEvidencePath, b03FileHash(replayEvidencePath)
	return receipts
}

func bootstrapB03ReplaySource(ctx context.Context, store driver.Store, id string) error {
	lease, err := store.Lease(ctx, "b03-native-replay-seed-"+id, driver.LeaseTTL)
	if err != nil {
		return err
	}
	// This bootstrap is only the built-in source. Custom catalog bytes below
	// must come from the validated native archive and public plan/apply.
	body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
	if err != nil {
		_ = lease.Release(ctx)
		return err
	}
	_, err = conformance.Seed(ctx, store, lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "b03-native-replay"}}, []conformance.FixtureChange{{Ref: model.Ref{ID: "ontology/b03-native-" + id}, Kind: "ontology", Op: model.OpCreate, Body: body, ChangedPaths: []string{"base"}}})
	releaseErr := lease.Release(ctx)
	return errors.Join(err, releaseErr)
}

func openB03ReplayRuntime(t *testing.T, store driver.Store, dsn, engine, id, project string) (*composition.Runtime, *inprocess.Transport) {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configBody := fmt.Sprintf("driver: %s\ndsn: %q\nproject: %s\naddress: 127.0.0.1:0\nui: false\nontology:\n  source: %s\n", engine, dsn, project, "ontology/b03-native-"+id)
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := composition.LoadConfigFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if engine == "memory" {
		config.StoreFactory = func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil }
	}
	runtime, err := composition.Open(context.Background(), config)
	if err != nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatalf("composition.Open replay %s: %v", engine, err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	return runtime, inprocess.New(runtime.RPC)
}

type b03RPCCall struct {
	Method         string `json:"method"`
	Key            string `json:"key"`
	RequestPath    string `json:"request_path"`
	RequestSHA256  string `json:"request_sha256"`
	ResponsePath   string `json:"response_path"`
	ResponseSHA256 string `json:"response_sha256"`
	RefusalCode    string `json:"refusal_code,omitempty"`
}

type b03ReplayRPC struct {
	t         *testing.T
	transport *inprocess.Transport
	root      string
	project   string
	calls     []b03RPCCall
}

func (r *b03ReplayRPC) call(method string, params map[string]any, wantCode string) json.RawMessage {
	r.t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	key := fmt.Sprintf("%032x", len(r.calls)+1)
	params["_meta"] = map[string]any{"project": r.project, "idempotencyKey": key, "agent": "b03-native-replay"}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": key, "method": method, "params": params})
	if err != nil {
		r.t.Fatal(err)
	}
	raw, err := r.transport.RoundTrip(context.Background(), request)
	if err != nil {
		r.t.Fatal(err)
	}
	base := filepath.Join(r.root, fmt.Sprintf("%03d-%s", len(r.calls)+1, method))
	if err := os.WriteFile(base+".request.json", request, 0o600); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(base+".response.json", raw, 0o600); err != nil {
		r.t.Fatal(err)
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Data struct {
				Code string `json:"code"`
			} `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		r.t.Fatal(err)
	}
	code := ""
	if envelope.Error != nil {
		code = envelope.Error.Data.Code
	}
	r.calls = append(r.calls, b03RPCCall{Method: method, Key: key, RequestPath: base + ".request.json", RequestSHA256: b03Hash(request), ResponsePath: base + ".response.json", ResponseSHA256: b03Hash(raw), RefusalCode: code})
	writeB03JSON(r.t, filepath.Join(r.root, "rpc-stream.json"), r.calls)
	wantID, _ := json.Marshal(key)
	if envelope.JSONRPC != "2.0" || !bytes.Equal(envelope.ID, wantID) {
		r.t.Fatalf("replay %s envelope identity differs: %s", method, raw)
	}
	if code != wantCode || (wantCode == "" && (envelope.Error != nil || len(envelope.Result) == 0)) || (wantCode != "" && (envelope.Error == nil || len(envelope.Result) != 0)) {
		r.t.Fatalf("replay %s outcome code=%q want=%q raw=%s", method, code, wantCode, raw)
	}
	// Results are decoded from the envelope. Exact request/response bytes are
	// retained above, including the complete refusal envelope on failures.
	return envelope.Result
}

func applyB03ReplayCatalog(t *testing.T, rpc *b03ReplayRPC, doc ontology.Document) {
	t.Helper()
	planRaw := rpc.call("ontology.plan", map[string]any{"document": doc}, "")
	var plan ontologyops.PlanResult
	if err := json.Unmarshal(planRaw, &plan); err != nil || len(plan.PlanDigest) != 64 || !plan.Changes.Ready || len(plan.Violations) != 0 {
		t.Fatalf("replay ontology.plan did not return a digest: %s", planRaw)
	}
	applyRaw := rpc.call("ontology.apply", map[string]any{"planDigest": plan.PlanDigest, "expect": plan.Changes.Expect, "humanPresence": map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest}}, "")
	var commit model.Commit
	if err := json.Unmarshal(applyRaw, &commit); err != nil || commit.Revision.IsZero() {
		t.Fatalf("replay ontology.apply did not publish: %s", applyRaw)
	}
	b03ReplayCatalog(t, rpc, doc)
}

func b03ReplayCatalog(t *testing.T, rpc *b03ReplayRPC, want ontology.Document) {
	t.Helper()
	raw := rpc.call("ontology.get", nil, "")
	var got ontologyops.Result
	if err := json.Unmarshal(raw, &got); err != nil || got.Document == nil || len(got.Problems) != 0 || got.Drifted || !equalB03JSON(*got.Document, want) {
		t.Fatalf("installed public catalog differs from adopted native archive document: %s", raw)
	}
}

func b03ReplayDocumentAt(t *testing.T, store driver.Store, id string) pipeline.Document {
	t.Helper()
	for _, row := range b03StoreRows(t, context.Background(), store) {
		if row.Ref.ID == id {
			if row.Kind != "bead" {
				t.Fatalf("native root has kind %q", row.Kind)
			}
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				t.Fatal(err)
			}
			return doc
		}
	}
	t.Fatalf("native imported root %s is absent", id)
	return pipeline.Document{}
}

func b03ReplayPublicRoot(t *testing.T, rpc *b03ReplayRPC, store driver.Store, id, typ, status string) pipeline.Document {
	t.Helper()
	raw := rpc.call("bead.get", map[string]any{"id": model.Ref{ID: id}}, "")
	var result struct {
		Bead struct {
			ID     string `json:"id"`
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"bead"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.Bead.ID != id || result.Bead.Type != typ || result.Bead.Status != status {
		t.Fatalf("public native root ID/type/status mismatch: %s", raw)
	}
	doc := b03ReplayDocumentAt(t, store, id)
	if doc.ID != id || doc.Project != rpc.project || doc.Type != typ || doc.Fields["status"] != status {
		t.Fatalf("stored native root ID/project/type/status mismatch: %+v", doc)
	}
	return doc
}

func b03ReplayReady(t *testing.T, rpc *b03ReplayRPC, rootID string, want bool) {
	t.Helper()
	raw := rpc.call("bead.ready", map[string]any{"limit": 1000}, "")
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		NextCursor string `json:"nextCursor"`
	}
	if err := json.Unmarshal(raw, &page); err != nil || page.NextCursor != "" {
		t.Fatalf("native ready census is incomplete: %s", raw)
	}
	found := false
	for _, row := range page.Items {
		found = found || row.ID == rootID
	}
	if found != want {
		t.Fatalf("native root ready membership=%t want=%t raw=%s", found, want, raw)
	}
}

func exerciseB03ReplayRuntime(t *testing.T, rpc *b03ReplayRPC, store driver.Store, input b03NativeInput, rootID string, imported ontology.Document, original pipeline.Document) map[string]any {
	t.Helper()
	// The first public adoption uses exactly the production migration document
	// from this archive. Slot edits below are a separate public catalog action,
	// never an assertion about what the native producer's slots implemented.
	applyB03ReplayCatalog(t, rpc, imported)
	adopted := b03ReplayPublicRoot(t, rpc, store, rootID, input.Catalog.Type, "todo")
	if !equalB03JSON(original, adopted) {
		t.Fatal("public adoption changed the pre-bootstrap imported native document")
	}
	b03ReplayReady(t, rpc, rootID, true)
	docBytes, err := json.Marshal(imported)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := ontology.Decode(docBytes)
	if err != nil {
		t.Fatal(err)
	}
	workflowKey := doc.Types[input.Catalog.Type].Workflow
	workflow := doc.Workflows[workflowKey]
	workflow.Initial = "awaiting_review"
	workflow.Slots[ontology.Claim], workflow.Slots[ontology.Close], workflow.Slots[ontology.Reopen], workflow.Slots[ontology.Defer] = "qa_testing", "archived", "awaiting_review", "on_hold"
	doc.Workflows[workflowKey] = workflow
	applyB03ReplayCatalog(t, rpc, doc)
	initial := b03ReplayPublicRoot(t, rpc, store, rootID, input.Catalog.Type, "todo")
	if !equalB03JSON(original, initial) {
		t.Fatal("public slot adoption changed the original imported native document")
	}
	if initial.CreatedAt == nil || initial.CreatedAt.IsZero() || initial.StartedAt != nil || initial.ClosedAt != nil {
		t.Fatalf("native imported root initial timestamps: %+v", initial)
	}
	ref := model.Ref{ID: rootID}
	rpc.call("bead.claim", map[string]any{"id": ref}, "")
	claimed := b03ReplayPublicRoot(t, rpc, store, rootID, input.Catalog.Type, "qa_testing")
	if claimed.StartedAt == nil || claimed.StartedAt.IsZero() || claimed.ClosedAt != nil || claimed.CreatedAt == nil || !claimed.CreatedAt.Equal(*initial.CreatedAt) {
		t.Fatalf("native root claim timestamps: %+v", claimed)
	}
	startedAt := *claimed.StartedAt
	b03ReplayReady(t, rpc, rootID, false)
	rpc.call("bead.close", map[string]any{"id": ref, "reason": "native catalog replay close"}, "")
	closed := b03ReplayPublicRoot(t, rpc, store, rootID, input.Catalog.Type, "archived")
	if closed.ClosedAt == nil || closed.ClosedAt.IsZero() || closed.StartedAt == nil || !closed.StartedAt.Equal(startedAt) {
		t.Fatalf("native root close timestamps: %+v", closed)
	}
	closedAt := *closed.ClosedAt
	rpc.call("bead.close", map[string]any{"id": ref, "reason": "native catalog repeated close"}, "")
	repeated := b03ReplayPublicRoot(t, rpc, store, rootID, input.Catalog.Type, "archived")
	if repeated.ClosedAt == nil || !repeated.ClosedAt.Equal(closedAt) {
		t.Fatal("native root repeated close changed its first terminal timestamp")
	}
	b03ReplayReady(t, rpc, rootID, false)
	refusalScopes := b03RefusalScopes(rpc, "bead.claim", "ontology.plan", "ontology.plan")
	beforeRefusals := b03ReplaySnapshot(t, store, rpc.project, refusalScopes)
	rpc.call("bead.claim", map[string]any{"id": ref}, "invalid_params")
	rpc.call("ontology.plan", map[string]any{"document": json.RawMessage(`{"statuses":{"broken":{"name":"broken","category":"unsupported"}}}`)}, "invalid_params")
	rpc.call("ontology.plan", map[string]any{"document": json.RawMessage(`{"map_status":[{"from":"broken","to":"todo"}]}`)}, "invalid_params")
	if !equalB03JSON(beforeRefusals, b03ReplaySnapshot(t, store, rpc.project, refusalScopes)) {
		t.Fatal("closed claim/malformed/unsupported catalog refusal changed durable state")
	}
	writeB03JSON(t, filepath.Join(rpc.root, "closed-catalog-refusal-conserved.json"), beforeRefusals)
	rpc.call("bead.reopen", map[string]any{"id": ref, "reason": "native catalog replay reopen"}, "")
	reopened := b03ReplayPublicRoot(t, rpc, store, rootID, input.Catalog.Type, "awaiting_review")
	if reopened.ClosedAt != nil || reopened.StartedAt == nil || !reopened.StartedAt.Equal(startedAt) || reopened.CreatedAt == nil || !reopened.CreatedAt.Equal(*initial.CreatedAt) {
		t.Fatalf("native root reopen timestamps: %+v", reopened)
	}
	b03ReplayReady(t, rpc, rootID, true)
	rpc.call("bead.defer", map[string]any{"id": ref}, "")
	frozen := b03ReplayPublicRoot(t, rpc, store, rootID, input.Catalog.Type, "on_hold")
	b03ReplayReady(t, rpc, rootID, false)
	frozenRefusalScopes := b03RefusalScopes(rpc, "bead.claim")
	beforeFrozenRefusal := b03ReplaySnapshot(t, store, rpc.project, frozenRefusalScopes)
	rpc.call("bead.claim", map[string]any{"id": ref}, "invalid_params")
	if !equalB03JSON(beforeFrozenRefusal, b03ReplaySnapshot(t, store, rpc.project, frozenRefusalScopes)) {
		t.Fatal("frozen claim refusal changed durable state")
	}
	writeB03JSON(t, filepath.Join(rpc.root, "frozen-refusal-conserved.json"), beforeFrozenRefusal)
	rpc.call("bead.reopen", map[string]any{"id": ref, "reason": "native catalog replay final reopen"}, "")
	final := b03ReplayPublicRoot(t, rpc, store, rootID, input.Catalog.Type, "awaiting_review")
	b03ReplayReady(t, rpc, rootID, true)
	if final.ClosedAt != nil || final.StartedAt == nil || !final.StartedAt.Equal(startedAt) || final.CreatedAt == nil || !final.CreatedAt.Equal(*initial.CreatedAt) {
		t.Fatal("final reopen lost the imported creation/started instants")
	}
	var images []map[string]any
	for _, stage := range []pipeline.Document{initial, claimed, closed, repeated, reopened, frozen, final} {
		assertB03ImportedConservation(t, original, stage)
		images = append(images, map[string]any{"id": "$ROOT", "project": "$PROJECT", "type": stage.Type, "fields": stage.Fields, "labels": stage.Labels, "parent": stage.Parent, "links": stage.Links, "attachments": stage.Attachments, "created": stage.CreatedAt != nil, "started": stage.StartedAt != nil, "closed": stage.ClosedAt != nil, "claimed": stage.Lease != nil})
	}
	// Correspondence normalizes only physical root/project identity and wall
	// instants. The actual semantic field images are compared across all twelve
	// replays; exact provenance and timestamps remain in raw/state receipts and
	// are conserved within each replay above and through durable reopen.
	return map[string]any{"root": "$ROOT", "project": "$PROJECT", "type": input.Catalog.Type, "document": doc, "archive_document_sha256": b03Hash(docBytes), "images": images, "ready": []bool{true, false, false, true, false, true}, "claim_refusal_code": "invalid_params", "malformed_catalog_code": "invalid_params", "unsupported_catalog_code": "invalid_params", "created_time_preserved": true, "started_time_preserved": true, "first_close_preserved": true, "reopen_clears_close": true, "imported_metadata_conserved": true}
}

// Bind the pre-bootstrap imported image to the actual validated archive row.
// Catalog adoption is not permitted to replace native creation or provenance
// and then compare only against that replacement as its own baseline.
func assertB03ImportedSource(t *testing.T, imported pipeline.Document, source dumpformat.Record) {
	t.Helper()
	wantMetadata := map[string]json.RawMessage{}
	put := func(key string, value any) {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		wantMetadata[key] = raw
	}
	if metadata, ok := source.Body["metadata"].(map[string]any); ok {
		for key, value := range metadata {
			put(key, value)
		}
	}
	for _, key := range dumpformat.OpaqueIssueFields() {
		if value, present := source.Source.Fields[key]; present {
			put("beads."+key, value)
		}
	}
	for key, value := range source.Body {
		switch key {
		case "title", "description", "priority", "metadata", "labels":
			continue
		}
		put("beads.source."+key, value)
	}
	put("beads.source.status", source.Source.Fields["status"])
	put("beads.source_fields", source.Source.Fields)
	if imported.ID != source.ID || imported.Project != source.Project || imported.Type != source.Body["type"] || !equalB03JSON(imported.Metadata, wantMetadata) {
		t.Fatal("pre-bootstrap imported native identity/type/provenance differs from the validated archive row")
	}
	stamp, ok := source.Source.Fields["created_at"].(string)
	if !ok || stamp == "" {
		t.Fatal("fresh native root has no source creation instant")
	}
	created, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil || imported.CreatedAt == nil || !imported.CreatedAt.Equal(created) || imported.StartedAt != nil || imported.ClosedAt != nil {
		t.Fatal("pre-bootstrap imported creation instant differs from native archive provenance")
	}
}

func assertB03ImportedConservation(t *testing.T, original, current pipeline.Document) {
	t.Helper()
	if current.ID != original.ID || current.Project != original.Project || current.Type != original.Type || !equalB03JSON(original.Metadata, current.Metadata) || original.CreatedAt == nil || current.CreatedAt == nil || !current.CreatedAt.Equal(*original.CreatedAt) {
		t.Fatal("catalog adoption/lifecycle/reopen changed original imported native identity, type, provenance or creation instant")
	}
}

func compareB03ReplayVectors(t *testing.T, root string, stores []b03StoreEvidence) {
	t.Helper()
	var want any
	var correspondence []map[string]any
	for _, store := range stores {
		engines, ok := store.Replay["engines"].(map[string]any)
		if !ok || len(engines) != 3 {
			t.Fatalf("native store %s lacks three actual engine replays", store.ID)
		}
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			result, ok := engines[engine].(map[string]any)
			if !ok || result["lifecycle"] == nil {
				t.Fatalf("native store %s lacks %s semantic replay", store.ID, engine)
			}
			vector := result["lifecycle"]
			if want == nil {
				want = vector
			}
			if !equalB03JSON(want, vector) {
				t.Fatalf("native different-ID/runtime correspondence differs store=%s engine=%s", store.ID, engine)
			}
			raw, _ := json.Marshal(vector)
			correspondence = append(correspondence, map[string]any{"store": store.ID, "native_store_id": store.InventoryStoreID, "root_id": store.RootID, "prefix": store.Prefix, "engine": engine, "normalized_sha256": b03Hash(raw)})
		}
	}
	writeB03JSON(t, filepath.Join(root, "different-id-runtime-correspondence.json"), correspondence)
}

type b03ReplayState struct {
	Meta         driver.Meta
	Records      []driver.Record
	Versions     map[string][]driver.Version
	Log          []model.Changeset
	Derived      map[driver.DerivedTable][]driver.DerivedRow
	Observations []driver.LockState
	Allocator    *driver.OfflineAllocator
	Receipts     map[string]*driver.IdempotencyRecord
	ByKey        map[string]*model.Changeset
}

// b03ReplaySnapshot observes one pinned durable image. Limits are assertions:
// an incomplete inventory cannot establish refusal or reopen conservation.
func b03ReplaySnapshot(t *testing.T, store driver.Store, project string, calls []b03RPCCall) b03ReplayState {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	at := meta.Head
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := b03ReplayState{Meta: meta, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Receipts: map[string]*driver.IdempotencyRecord{}, ByKey: map[string]*model.Changeset{}}
	keys := map[string]driver.LockKey{}
	addKey := func(key driver.LockKey) string {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			t.Fatal(err)
		}
		keys[encoded] = key
		return encoded
	}
	for _, key := range []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock} {
		addKey(key)
	}
	seenRecords, seenCursors := map[string]bool{}, map[string]bool{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Limit: 1000, Cursor: cursor})
		if err != nil || page.AsOf != at || (cursor != nil && len(page.Items) == 0) {
			t.Fatalf("incomplete replay record inventory: %+v %v", page, err)
		}
		for _, row := range page.Items {
			ref := row.Ref
			key := addKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
			if seenRecords[key] {
				t.Fatalf("duplicate replay record %s", key)
			}
			seenRecords[key] = true
			s.Records = append(s.Records, row)
			versions, err := r.Versions(ctx, ref, driver.PageReq{Limit: 1000})
			if err != nil || versions.NextCursor != nil || len(versions.Items) == 0 {
				t.Fatalf("incomplete replay versions for %s: %v", key, err)
			}
			s.Versions[key] = versions.Items
			if row.Kind == "bead" {
				bead, err := driver.NewBeadSentinel(ref.ID)
				if err != nil {
					t.Fatal(err)
				}
				addKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
			}
		}
		if len(s.Records) > 10000 {
			t.Fatal("replay record inventory exceeds bounded owned store")
		}
		if page.NextCursor == nil {
			break
		}
		encoded, err := json.Marshal(page.NextCursor)
		if err != nil || seenCursors[string(encoded)] {
			t.Fatal("replay record cursor failed to advance")
		}
		seenCursors[string(encoded)] = true
		cursor = page.NextCursor
	}
	sort.Slice(s.Records, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &s.Records[i].Ref})
		b, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &s.Records[j].Ref})
		return a < b
	})
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		s.Derived[table] = rows
		for _, row := range rows {
			addKey(driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}})
		}
	}
	allocators, ok := r.(driver.AllocatorReader)
	if !ok {
		t.Fatal("replay store lacks allocator census capability")
	}
	allocator, err := allocators.LoadAllocator(ctx, project)
	if err != nil && !errors.Is(err, driver.ErrNotFound) {
		t.Fatal(err)
	}
	if err == nil {
		s.Allocator = &allocator
	}
	allocatorLock, err := driver.AllocatorLock(project)
	if err != nil {
		t.Fatal(err)
	}
	addKey(allocatorLock)
	for _, typ := range []string{"issue", "delivery"} {
		ready, err := driver.NewReadySentinel(project, typ)
		if err != nil {
			t.Fatal(err)
		}
		addKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
	}
	orderedKeys := make([]string, 0, len(keys))
	for key := range keys {
		orderedKeys = append(orderedKeys, key)
	}
	sort.Strings(orderedKeys)
	ordered := make([]driver.LockKey, 0, len(keys))
	for _, key := range orderedKeys {
		ordered = append(ordered, keys[key])
	}
	s.Observations, err = r.Observe(ctx, ordered)
	if err != nil {
		t.Fatal(err)
	}
	s.Log, err = r.Changesets(ctx, nil, 10000)
	if err != nil || len(s.Log) >= 10000 {
		t.Fatalf("incomplete replay changeset inventory: %v", err)
	}
	// The no-policy in-process composition uses the canonical anonymous actor.
	// Its admitted _meta.agent label supplies the anonymous G4 receipt scope.
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		key := call.Method + "/" + call.Key
		s.Receipts[key], err = r.IdempotencyByKey(ctx, principal, "b03-native-replay", call.Method, call.Key)
		if err != nil {
			t.Fatal(err)
		}
		s.ByKey[key], err = r.ChangesetByKey(ctx, principal, "b03-native-replay", call.Method, call.Key)
		if err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// Include future refused keys on both sides of the conservation comparison,
// so an unexpected no-op receipt cannot escape a head/log-only observation.
func b03RefusalScopes(rpc *b03ReplayRPC, methods ...string) []b03RPCCall {
	scopes := append([]b03RPCCall(nil), rpc.calls...)
	for i, method := range methods {
		scopes = append(scopes, b03RPCCall{Method: method, Key: fmt.Sprintf("%032x", len(rpc.calls)+i+1)})
	}
	return scopes
}

func openB03ReplayStore(t *testing.T, engine string) (driver.Store, string, func(string) (driver.Store, error)) {
	t.Helper()
	ctx := context.Background()
	switch engine {
	case "memory":
		store, err := memory.NewWithOptions(memory.Options{StoreID: "b03-native-memory", Incarnation: "b03-native-memory"})
		if err != nil {
			t.Fatal(err)
		}
		return store, "", nil
	case "sqlite":
		path := filepath.Join(t.TempDir(), "replay.db")
		store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
		if err != nil {
			t.Fatal(err)
		}
		return store, path, func(dsn string) (driver.Store, error) { return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: dsn}) }
	case "postgres":
		base := testclock.PostgresDSN(t)
		admin, err := sql.Open("pgx", base)
		if err != nil {
			t.Fatal(err)
		}
		name := "b03_native_replay_" + b03RandomID()
		if _, err := admin.ExecContext(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
			admin.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = admin.ExecContext(context.Background(), `DROP DATABASE "`+name+`" WITH (FORCE)`)
			_ = admin.Close()
		})
		u, err := url.Parse(base)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/" + name
		dsn := u.String()
		store, err := (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn})
		if err != nil {
			t.Fatal(err)
		}
		return store, dsn, func(next string) (driver.Store, error) {
			return (postgres.Driver{}).Open(ctx, driver.Config{DSN: next})
		}
	default:
		t.Fatalf("unknown replay engine %s", engine)
		return nil, "", nil
	}
}

func b03StoreRows(t *testing.T, ctx context.Context, store driver.Store) []driver.Record {
	t.Helper()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Query(ctx, driver.Query{Limit: 10000})
	if err != nil || page.NextCursor != nil || len(page.Items) == 0 {
		t.Fatalf("replayed catalog rows incomplete: err=%v cursor=%v rows=%d", err, page.NextCursor, len(page.Items))
	}
	sort.Slice(page.Items, func(i, j int) bool { return page.Items[i].Ref.ID < page.Items[j].Ref.ID })
	return page.Items
}

func exactNativeRefusal(runner *b03CommandRunner, receipt b03CommandReceipt, name string) string {
	stderr, _ := os.ReadFile(receipt.StderrPath)
	return fmt.Sprintf("%s refusal exit=%d stderr=%q receipt=%s", name, receipt.Exit, string(stderr), receipt.StderrPath)
}

func parseNativeJSON(t *testing.T, data []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("native JSON decode: %v: %s", err, data)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("native JSON had trailing data: %v", err)
	}
	return value
}

func equalB03JSON(left, right any) bool {
	a, err := json.Marshal(left)
	if err != nil {
		return false
	}
	b, err := json.Marshal(right)
	return err == nil && bytes.Equal(a, b)
}

func b03OutputRoot(t *testing.T) string {
	t.Helper()
	parent := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if parent == "" {
		parent = "/tmp/plug-swarm/work"
	}
	if !filepath.IsAbs(parent) {
		t.Fatal("B03 native output root must be absolute")
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(parent, "s1-b03-native-catalog-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func writeB03JSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func b03FileHash(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return b03Hash(data)
}

func b03Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func b03RandomID() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}

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
	for _, value := range ordered {
		if value.err != nil || value.exit != 0 {
			r.t.Fatalf("native race control %s failed exit=%d: %s", value.label, value.exit, value.stderr)
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
	return out
}

type b03StoreEvidence struct {
	ID                    string                `json:"id"`
	Database              string                `json:"database"`
	Prefix                string                `json:"prefix"`
	RootID                string                `json:"root_id"`
	ExportPath            string                `json:"export_path,omitempty"`
	ExportSHA256          string                `json:"export_sha256,omitempty"`
	ExportBytes           int                   `json:"export_bytes,omitempty"`
	InventoryPath         string                `json:"inventory_path,omitempty"`
	InventorySHA256       string                `json:"inventory_sha256,omitempty"`
	InventoryBytes        int                   `json:"inventory_bytes,omitempty"`
	ArchivePath           string                `json:"archive_path,omitempty"`
	ArchiveSHA256         string                `json:"archive_sha256,omitempty"`
	ArchiveBytes          int64                 `json:"archive_bytes,omitempty"`
	ArchiveMetadata       map[string]any        `json:"archive_metadata,omitempty"`
	NormalizedArchiveSHA  string                `json:"normalized_archive_sha256,omitempty"`
	SQLCensusBeforePath   string                `json:"sql_census_before_path,omitempty"`
	SQLCensusBeforeSHA256 string                `json:"sql_census_before_sha256,omitempty"`
	SQLCensusAfterPath    string                `json:"sql_census_after_path,omitempty"`
	SQLCensusAfterSHA256  string                `json:"sql_census_after_sha256,omitempty"`
	SQLTables             []b03SQLTableEvidence `json:"sql_tables,omitempty"`
	Replay                map[string]any        `json:"replay,omitempty"`
	Blockers              []string              `json:"blockers,omitempty"`
	Commands              []b03CommandReceipt   `json:"commands"`
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

// TestB03NativeCatalogRecording is manual and intentionally fails closed when
// the admitted c8eb producer cannot emit a canonical catalog archive. That
// failure is the producer-owned dependency, not a successful fixture or skip.
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
		wantNormalized := all[0].NormalizedArchiveSHA
		seenIDs := map[string]bool{}
		seenNormalized := map[string]bool{}
		for _, evidence := range all {
			if evidence.RootID == "" || seenIDs[evidence.RootID] {
				t.Fatalf("native stores did not retain distinct root IDs: %+v", all)
			}
			seenIDs[evidence.RootID] = true
			if evidence.NormalizedArchiveSHA == "" || evidence.NormalizedArchiveSHA != wantNormalized {
				t.Fatalf("normalized canonical archives diverged across distinct stores: %+v", all)
			}
			seenNormalized[evidence.NormalizedArchiveSHA] = true
		}
		if len(seenNormalized) != 1 {
			t.Fatalf("native stores did not produce one normalized catalog image: %+v", all)
		}
	}
	for i, path := range archives {
		all[i].Replay = replayB03Archive(t, root, input.Stores[i].ID, path, input)
	}
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
	writeB03JSON(t, filepath.Join(root, "prepared-artifact-binding.json"), map[string]any{
		"descriptor":        input.Descriptor,
		"bundle_artifact":   map[string]any{"sha256": input.ArtifactDigest, "size": input.ArtifactSize, "kind": "official ZIP artifact"},
		"executable":        map[string]any{"path": binary, "sha256": input.BinarySHA256, "size": input.BinaryBytes},
		"manifest":          map[string]any{"path": manifestPath, "sha256": input.ManifestSHA256},
		"canonical_archive": map[string]any{"path": archivePath, "sha256": manifest.Archive.SHA256, "size": manifest.Archive.Size, "kind": "prepared bundle.tar"},
	})
	return binary, manifest
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
		_, createReceipt := command("catalog-root-create", "--json", "create", "--id", rootID, "--type", input.Catalog.Type, "--status", "awaiting_review", "--title", "B03 native catalog root", "--description", "fresh native catalog source")
		if createReceipt.Exit != 0 {
			blockers = append(blockers, exactNativeRefusal(runner, createReceipt, "catalog root create"))
		} else {
			row, showReceipt := command("catalog-root-show", "--json", "show", "--include-comments", rootID)
			if showReceipt.Exit != 0 {
				blockers = append(blockers, exactNativeRefusal(runner, showReceipt, "catalog root show"))
			} else if !bytes.Contains(row, []byte(`"status":"awaiting_review"`)) || !bytes.Contains(row, []byte(`"issue_type":"delivery"`)) {
				blockers = append(blockers, fmt.Sprintf("native catalog root did not retain requested status/type store=%s raw=%s", id, row))
			}
		}
	}
	if _, receipt = command("config-list", "--json", "config", "list"); receipt.Exit != 0 {
		blockers = append(blockers, exactNativeRefusal(runner, receipt, "config list"))
	}
	before := recordB03PhysicalCensus(t, runner, binary, "before")
	inventoryPath := filepath.Join(storeRoot, "native-inventory.json")
	_, inventoryReceipt := command("inventory", "--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventoryPath)
	if inventoryReceipt.Exit == 0 {
		if data, err := os.ReadFile(inventoryPath); err != nil || len(data) == 0 {
			blockers = append(blockers, fmt.Sprintf("native inventory is empty store=%s err=%v", id, err))
		} else {
			evidence.InventoryPath, evidence.InventorySHA256, evidence.InventoryBytes = inventoryPath, b03Hash(data), len(data)
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
		if err := validateB03CanonicalArchive(input, archive, archivePath); err != nil {
			blockers = append(blockers, fmt.Sprintf("canonical catalog metadata refused store=%s: %v", id, err))
		} else {
			info, statErr := os.Stat(archivePath)
			if statErr != nil {
				blockers = append(blockers, fmt.Sprintf("canonical archive stat failed store=%s: %v", id, statErr))
			} else {
				evidence.ArchivePath, evidence.ArchiveSHA256, evidence.ArchiveBytes = archivePath, archive.SHA256(), info.Size()
				evidence.ArchiveMetadata = b03ArchiveMetadata(archive.Manifest())
				evidence.NormalizedArchiveSHA = b03Hash(marshalB03NormalizedArchive(archive, rootID))
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

func validateB03CanonicalArchive(input b03NativeInput, archive *dumpformat.Archive, path string) error {
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
	statusWant := map[string]string{}
	for _, item := range strings.Split(input.Catalog.Status, ",") {
		name, category, ok := strings.Cut(item, ":")
		if !ok || name == "" || category == "" {
			return fmt.Errorf("invalid input status declaration %q", item)
		}
		statusWant[name] = category
	}
	statusRows := archive.Records("custom_statuses")
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
	typeRows := archive.Records("custom_types")
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
				if disposition.Rows != len(archive.Records(want)) || disposition.Reason == "" || disposition.Action == "" {
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
		"kinds": manifest.Kinds, "files": manifest.Files, "sources": manifest.Sources,
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

func marshalB03NormalizedArchive(archive *dumpformat.Archive, rootID string) []byte {
	manifest := archive.Manifest()
	kinds := make([]string, 0, len(manifest.Kinds))
	for _, kind := range manifest.Kinds {
		kinds = append(kinds, kind.Name)
	}
	sort.Strings(kinds)
	records := make([]map[string]any, 0)
	for _, kind := range kinds {
		for _, row := range archive.Records(kind) {
			records = append(records, map[string]any{
				"kind": row.Kind, "project": row.Project, "source_table": row.Source.Table,
				"source": normalizeB03Value(row.Source.Fields, rootID), "body": normalizeB03Value(row.Body, rootID),
			})
		}
	}
	sort.Slice(records, func(i, j int) bool {
		left, _ := json.Marshal(records[i])
		right, _ := json.Marshal(records[j])
		return bytes.Compare(left, right) < 0
	})
	data, _ := json.Marshal(map[string]any{"schema": "plug-s1-b03-normalized-archive/1", "kinds": kinds, "records": records})
	return data
}

func normalizeB03Value(value any, rootID string) any {
	switch value := value.(type) {
	case string:
		if value == rootID {
			return "$ROOT"
		}
		return value
	case map[string]any:
		result := make(map[string]any, len(value))
		for key, item := range value {
			result[key] = normalizeB03Value(item, rootID)
		}
		return result
	case []any:
		result := make([]any, len(value))
		for i, item := range value {
			result[i] = normalizeB03Value(item, rootID)
		}
		return result
	default:
		return value
	}
}

func replayB03Archive(t *testing.T, root, id, path string, input b03NativeInput) map[string]any {
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
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "b03-native-reviewer"}, Authority: "restore-admin", AdmissionDigest: b03Hash([]byte("s1-b03-native-admission"))}, SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: "s1-b03-native-" + id, ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot}
	replayRoot := filepath.Join(root, id, "replay")
	if err := os.MkdirAll(replayRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	receipts := map[string]any{"archive_sha256": inspection.ArchiveSHA256, "source_store_id": inspection.SourceStoreID, "derived_sha256": inspection.DerivedSHA256, "engines": map[string]any{}}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(id+"/replay/"+engine, func(t *testing.T) {
			store, dsn, reopen := openB03ReplayStore(t, engine)
			if _, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), store, profile); err != nil {
				t.Fatalf("native archive replay on %s: %v", engine, err)
			}
			before := b03StoreRows(t, ctx, store)
			if err := installB03ReplaySource(t, ctx, store, id, input); err != nil {
				_ = store.Close()
				t.Fatalf("install replay source on %s: %v", engine, err)
			}
			runtime, transport := openB03ReplayRuntime(t, store, dsn, engine, id)
			lifecycle := exerciseB03ReplayRuntime(t, transport, input, id)
			if err := runtime.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			engineEvidence := map[string]any{"engine": engine, "pre_runtime_rows": len(before), "lifecycle": lifecycle}
			if reopen != nil {
				var err error
				store, err = reopen(dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				after := b03StoreRows(t, ctx, store)
				engineEvidence["post_reopen_rows"] = len(after)
				engineEvidence["post_reopen_sha256"] = b03Hash(marshalB03Records(after))
				if len(after) < len(before) {
					t.Fatalf("%s reopen lost replayed catalog rows: before=%d after=%d", engine, len(before), len(after))
				}
			} else {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
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

func installB03ReplaySource(t *testing.T, ctx context.Context, store driver.Store, id string, input b03NativeInput) error {
	lease, err := store.Lease(ctx, "b03-native-replay-seed-"+id, driver.LeaseTTL)
	if err != nil {
		return err
	}
	doc := b03ReplayDocument(input)
	body, err := service.MarshalOntologySource(service.OntologySource{Base: doc})
	if err != nil {
		_ = lease.Release(ctx)
		return err
	}
	_, err = conformance.Seed(ctx, store, lease, driver.ChangesetHeader{Principal: model.PrincipalRef{Kind: "system", ID: "b03-native-replay"}}, []conformance.FixtureChange{{Ref: model.Ref{ID: "ontology/b03-native-" + id}, Kind: "ontology", Op: model.OpCreate, Body: body, ChangedPaths: []string{"base"}}})
	releaseErr := lease.Release(ctx)
	return errors.Join(err, releaseErr)
}

func b03ReplayDocument(input b03NativeInput) ontology.Document {
	doc := ontology.DefaultDocument()
	workflow := doc.Workflows["issue"]
	statusNames := make([]string, 0, 4)
	for _, item := range strings.Split(input.Catalog.Status, ",") {
		name, category, _ := strings.Cut(item, ":")
		status := ontology.Status{Name: name}
		switch category {
		case "active":
			status.Category = ontology.Unstarted
		case "wip":
			status.Category = ontology.Started
		case "frozen":
			status.Category = ontology.Backlog
		case "done":
			status.Category, status.Outcome = ontology.Completed, ontology.Success
		}
		doc.Statuses[name] = status
		statusNames = append(statusNames, name)
	}
	if len(statusNames) >= 4 {
		workflow.Statuses = append(workflow.Statuses, statusNames...)
		workflow.Initial, workflow.Slots[ontology.Claim], workflow.Slots[ontology.Close], workflow.Slots[ontology.Reopen] = statusNames[0], statusNames[1], statusNames[3], statusNames[0]
		doc.Workflows["issue"] = workflow
	}
	issue := doc.Types["issue"]
	doc.Types[input.Catalog.Type] = ontology.Type{Name: input.Catalog.Type, Workflow: issue.Workflow, Fields: issue.Fields}
	return doc
}

func openB03ReplayRuntime(t *testing.T, store driver.Store, dsn, engine, id string) (*composition.Runtime, *inprocess.Transport) {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configBody := fmt.Sprintf("driver: %s\ndsn: %q\nproject: b03-native\naddress: 127.0.0.1:0\nui: false\nontology:\n  source: %s\n", engine, dsn, "ontology/b03-native-"+id)
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
		_ = store.Close()
		t.Fatalf("composition.Open replay %s: %v", engine, err)
	}
	return runtime, inprocess.New(runtime.RPC)
}

func exerciseB03ReplayRuntime(t *testing.T, transport *inprocess.Transport, input b03NativeInput, id string) map[string]any {
	t.Helper()
	project := "b03-native"
	key := func(n int) string { return fmt.Sprintf("%032x", n) }
	doc := b03ReplayDocument(input)
	planRaw := b03ReplayCall(t, transport, "ontology.plan", map[string]any{"document": doc}, project, key(1), true)
	var plan ontologyops.PlanResult
	if err := json.Unmarshal(planRaw, &plan); err != nil || plan.PlanDigest == "" {
		t.Fatalf("replay ontology.plan did not return a digest: %s", planRaw)
	}
	applyRaw := b03ReplayCall(t, transport, "ontology.apply", map[string]any{"planDigest": plan.PlanDigest, "expect": plan.Changes.Expect, "humanPresence": map[string]any{"mode": "presence_check", "challenge": plan.PlanDigest, "proof": plan.PlanDigest}}, project, key(2), true)
	var commit model.Commit
	if err := json.Unmarshal(applyRaw, &commit); err != nil || commit.Revision.IsZero() {
		t.Fatalf("replay ontology.apply did not publish: %s", applyRaw)
	}
	createdRaw := b03ReplayCall(t, transport, "bead.create", map[string]any{"type": input.Catalog.Type, "fields": map[string]any{"title": "B03 native replay"}}, project, key(3), true)
	var created model.Commit
	if err := json.Unmarshal(createdRaw, &created); err != nil || len(created.Results) != 1 {
		t.Fatalf("replay bead.create did not publish: %s", createdRaw)
	}
	ref := created.Results[0].Ref
	readyRaw := b03ReplayCall(t, transport, "bead.ready", map[string]any{"limit": 1000}, project, key(4), true)
	claimRaw := b03ReplayCall(t, transport, "bead.claim", map[string]any{"id": ref}, project, key(5), true)
	closeRaw := b03ReplayCall(t, transport, "bead.close", map[string]any{"id": ref, "reason": "B03 native replay"}, project, key(6), true)
	refusalRaw := b03ReplayCall(t, transport, "bead.claim", map[string]any{"id": ref}, project, key(7), false)
	reopenRaw := b03ReplayCall(t, transport, "bead.reopen", map[string]any{"id": ref, "reason": "B03 native replay reopen"}, project, key(8), true)
	return map[string]any{"store_id": id, "created_id": ref, "plan": b03Hash(planRaw), "apply": b03Hash(applyRaw), "ready": b03Hash(readyRaw), "claim": b03Hash(claimRaw), "close": b03Hash(closeRaw), "refusal": b03Hash(refusalRaw), "reopen": b03Hash(reopenRaw)}
}

func b03ReplayCall(t *testing.T, transport *inprocess.Transport, method string, params map[string]any, project, key string, wantSuccess bool) []byte {
	t.Helper()
	params["_meta"] = map[string]any{"project": project, "idempotencyKey": key, "agent": "b03-native-replay"}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": key, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := transport.RoundTrip(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if wantSuccess && len(envelope.Error) != 0 {
		t.Fatalf("replay %s refused: %s", method, raw)
	}
	if !wantSuccess && len(envelope.Error) == 0 {
		t.Fatalf("replay refusal control %s unexpectedly succeeded: %s", method, raw)
	}
	return raw
}

func marshalB03Records(rows []driver.Record) []byte {
	data, _ := json.Marshal(rows)
	return data
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

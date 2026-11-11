package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/publicnative"
)

var freshNativeSource = publicnative.CanonicalSource{
	Name: "beads-ef3f15", Commit: "ef3f1566ccc2dddafc55d86cff44ce4200237eb8", Tree: "90ccafb0dd45198390da28821550bc555ef70712", Parent: "437f377e7134714152d2cf2d49a465bbc3984485",
	ArchiveSHA: "034e39dba83ce13eaa4af01feb753bf4f2fe360551f49de40094573d7dd330cd", ManifestSHA: "c1d92931dfce093264a1ba155869b2bffbb9dd184e9778e2c75abeb081ab1bdb", Entries: 5237,
}

type freshResult struct {
	Case                  freshCase `json:"case"`
	Status                string    `json:"status"`
	Gaps                  []string  `json:"gaps"`
	EqualityCredit        int       `json:"equality_credit"`
	RecipeExportSHA256    string    `json:"recipe_export_sha256,omitempty"`
	RetainedArchiveSHA256 string    `json:"retained_archive_sha256,omitempty"`
	SourceRoot            string    `json:"source_root"`
	RecipeExecutableSHA   string    `json:"recipe_executable_sha256,omitempty"`
	CaptureArchiveSHA256  string    `json:"capture_archive_sha256,omitempty"`
	PhysicalSHA256        string    `json:"physical_sha256,omitempty"`
	WorkingRoot           string    `json:"working_root,omitempty"`
	Head                  string    `json:"head,omitempty"`
	WorkingFamilies       int       `json:"working_families"`
	ImmutableImages       int       `json:"immutable_images"`
	NativeContinuation    string    `json:"native_continuation"`
	PlugRestore           string    `json:"plug_restore"`
	PlugContinuation      string    `json:"plug_continuation"`
}

// A separate manual target: old historical recordings and their assertions
// remain untouched. Each leaf really invokes the existing native recipe once.
func TestFreshSameRunIDStyleATwelveFixtures(t *testing.T) {
	parent, run := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), os.Getenv("GITHUB_RUN_ID")
	if !filepath.IsAbs(parent) || run == "" || os.Getenv("TEST_TARGET") != "//tools/roundtrip:fresh_same_run_native_test" {
		t.Fatal("explicit fresh recording target and actual run binding required")
	}
	if _, err := os.Lstat("/tmp/plug-swarm/PAUSE"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fresh public recording paused")
	}
	root, err := os.MkdirTemp(parent, "fresh-id-style-a-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("/tmp/plug-swarm", 0700); err != nil {
		t.Fatal(err)
	}
	private, err := os.MkdirTemp("/tmp/plug-swarm", "b09-fresh-"+run+"-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll(private)
		freshJSON(t, filepath.Join(root, "owned-source-cleanup.json"), map[string]any{"path": private, "removed": err == nil, "error": fmt.Sprint(err)})
		if err != nil {
			t.Error(err)
		}
	}()
	reader := buildPublicTypedReader(t)
	build := filepath.Join(root, "native-build")
	if err := os.Mkdir(build, 0700); err != nil {
		t.Fatal(err)
	}
	builder := publicnative.SourceBuilder{T: t, Root: build, SourceDirectory: "testdata/fresh-native", Target: "//cmd/bd:bd"}
	var results []freshResult
	builder.BuildWithSource(freshNativeSource, func(source publicnative.SourceSession) {
		for _, item := range freshNativeCases() {
			t.Run(item.Engine+"/"+item.Name, func(t *testing.T) {
				dir := filepath.Join(root, item.Engine+"-"+item.Name)
				workspace := filepath.Join(private, item.Engine+"-"+item.Name)
				for _, path := range []string{dir, workspace, filepath.Join(workspace, ".beads"), filepath.Join(workspace, "home")} {
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				}
				result := freshResult{Case: item, Status: "GAP", SourceRoot: filepath.Join(workspace, ".beads", "embeddeddolt"), NativeContinuation: "NOT_RUN", PlugRestore: "NOT_RUN", PlugContinuation: "NOT_RUN"}
				defer func() {
					results = append(results, result)
					freshJSON(t, filepath.Join(dir, "result.json"), result)
					t.Logf("FRESH_SAME_RUN_CASE %s", freshJSONBytes(t, result))
					if result.EqualityCredit != 1 {
						t.Errorf("fresh native equality unproved: %s/%s gaps=%v", item.Engine, item.Name, result.Gaps)
					}
				}()
				freshJSON(t, filepath.Join(dir, "request.json"), map[string]any{
					"case": item, "run_id": run, "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "target": os.Getenv("TEST_TARGET"),
					"native_source": freshNativeSource, "retained_root": result.SourceRoot, "database": "beads_test", "reader_commit": typedReaderCommit, "reader_tree": typedReaderTree, "reader_sha256": reader.SHA256, "reader_build_run": reader.BuildRun,
					"initial_audit": "fully-exact", "continuation_policy": "escalations/1657/1689/1701", "expected_inputs": "native-only; never target values", "capture_precedes_mutation": true,
				})
				if err := freshRecordRecipe(t, source, item, dir, result.SourceRoot, &result); err != nil {
					result.Gaps = append(result.Gaps, "native_recipe: "+err.Error())
					return
				}
				// Preserve the clean handoff before any reader or continuation
				// opens it; partial's later dirty original is never consumed.
				freshTreeManifest(t, result.SourceRoot, filepath.Join(dir, "retained-export-files.json"))
				storeArchive := filepath.Join(dir, "retained-export-store.tar.gz")
				if _, err := freshCommand(t, dir, "retain-export-store", workspace, "/usr/bin/tar", []string{"-czf", storeArchive, "-C", result.SourceRoot, "."}, freshChildEnv(workspace), 10*time.Minute); err != nil {
					result.Gaps = append(result.Gaps, "source_store_retention_refused")
					return
				}
				result.RetainedArchiveSHA256 = freshFileSHA(t, storeArchive)
				capture := filepath.Join(dir, "intended-source")
				if err := os.Mkdir(capture, 0700); err != nil {
					t.Fatal(err)
				}
				args := []string{"--disposable", "--data-dir=" + result.SourceRoot, "--database=beads_test", "--beads-dir=" + filepath.Join(workspace, ".beads"), "--output-dir=" + capture, "--timeout=20m"}
				if _, err := freshCommand(t, dir, "intended-reader", workspace, reader.Binary, args, freshChildEnv(workspace), 21*time.Minute); err != nil {
					result.Gaps = append(result.Gaps, "intended_source_reader_refused: "+err.Error())
					freshTreeManifest(t, result.SourceRoot, filepath.Join(dir, "retained-after-reader-files.json"))
					return
				}
				freshTreeManifest(t, result.SourceRoot, filepath.Join(dir, "retained-after-reader-files.json"))
				archive, err := freshAcceptSourceCapture(t, capture, result.SourceRoot, &result)
				if err != nil {
					result.Gaps = append(result.Gaps, "intended_source_adapter_refused: "+err.Error())
					return
				}
				// Ask the existing restore inspection seam about the actual
				// produced representation. A refusal is retained, not bypassed
				// by relabelling normalized tables as a typed native restore.
				inspection, err := converterroundtrip.InspectImport(context.Background(), bytes.NewReader(archive))
				freshJSON(t, filepath.Join(dir, "restore-inspection.json"), map[string]any{"inspection": inspection, "error": fmt.Sprint(err), "input_sha256": hash(archive), "engine": item.Engine, "history_mode": item.Mode})
				if err != nil {
					result.PlugRestore = "REFUSED_INSPECTION"
					result.Gaps = append(result.Gaps, "supported_restore_inspection: "+err.Error())
					return
				}
				result.Gaps = append(result.Gaps, "typed_restore_and_same_source_continuation_not_yet_observed")
			})
		}
	})
	freshJSON(t, filepath.Join(root, "twelve-case-inventory.json"), results)
	if len(results) != 12 {
		t.Fatalf("fresh native census incomplete: %d/12", len(results))
	}
}

func freshRecordRecipe(t *testing.T, source publicnative.SourceSession, item freshCase, dir, destination string, result *freshResult) error {
	t.Helper()
	dolt := os.Getenv("PLUG_PINNED_DOLT_BINARY")
	if !filepath.IsAbs(dolt) {
		dolt = filepath.Join(os.Getenv("TEST_SRCDIR"), dolt)
	}
	dolt, err := filepath.EvalSymlinks(dolt)
	if err != nil || freshFileSHA(t, dolt) != "72352f1d882472a7a8d6124e9c4dc12f80e2c2359ea95f1424c032e9250e35e6" {
		return fmt.Errorf("pinned public Dolt runtime missing")
	}
	binary, args := source.TestArguments(item.Target,
		"--test_sharding_strategy=disabled", "--test_arg=-test.v", "--test_arg=-test.timeout=25m", "--test_timeout=1800", "--test_arg=-test.run=^"+item.Recipe+"$", "--test_arg=-plug-recipe-retain-store="+destination,
		"--test_env=BEADS_TEST_DOLT_SERVER=local", "--test_env=BEADS_TEST_DOLT_BINARY="+dolt, "--test_env=BEADS_TEST_REQUIRE_DOLT_CONTAINER=1", "--test_env=BEADS_TEST_SKIP=", "--test_env=PLUG_TEST_EXPORT_PUBLIC_HISTORY=1", "--test_env=PATH=/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	_, runErr := freshCommand(t, dir, "native-recipe", source.Directory, binary, args, nil, 40*time.Minute)
	targetPath := strings.ReplaceAll(strings.TrimPrefix(item.Target, "//"), ":", "/")
	for _, name := range []string{"test.log", "test.xml", "test.outputs/outputs.zip"} {
		data, err := readProducedBeadsArchive(filepath.Join(source.Directory, "bazel-testlogs", targetPath, name))
		if err != nil {
			if name == "test.outputs/outputs.zip" {
				continue
			}
			return fmt.Errorf("original recipe %s unavailable: %w", name, err)
		}
		if err := writePrivate(dir, "recipe-"+strings.ReplaceAll(name, "/", "-"), data); err != nil {
			return err
		}
	}
	if runErr != nil {
		return runErr
	}
	query, queryArgs := source.QueryArguments(item.Target)
	files, err := freshCommand(t, dir, "recipe-executable-query", source.Directory, query, queryArgs, nil, 5*time.Minute)
	if err != nil {
		return err
	}
	paths := strings.Fields(string(files))
	if len(paths) != 1 {
		return fmt.Errorf("recipe executable outputs=%d", len(paths))
	}
	executable := filepath.Join(source.Directory, paths[0])
	result.RecipeExecutableSHA = freshFileSHA(t, executable)
	retained := filepath.Join(filepath.Dir(dir), "native-build", strings.ReplaceAll(strings.TrimPrefix(item.Target, "//"), "/", "-")+"-executable")
	retained = strings.ReplaceAll(retained, ":", "-")
	if _, err := os.Lstat(retained); errors.Is(err, os.ErrNotExist) {
		input, err := os.Open(executable)
		if err != nil {
			return err
		}
		output, err := os.OpenFile(retained, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0500)
		if err != nil {
			_ = input.Close()
			return err
		}
		_, copyErr := io.Copy(output, input)
		if err := errors.Join(copyErr, input.Close(), output.Close()); err != nil {
			return err
		}
	}
	if freshFileSHA(t, retained) != result.RecipeExecutableSHA {
		return fmt.Errorf("native recipe executable changed within recording")
	}
	log, _ := os.ReadFile(filepath.Join(dir, "recipe-test.log"))
	xmlBytes, _ := os.ReadFile(filepath.Join(dir, "recipe-test.xml"))
	if err := freshSingleRecipeXML(xmlBytes, item.Recipe); err != nil {
		return err
	}
	archive, err := freshArchiveMarker(log, item.Marker)
	if err != nil {
		return err
	}
	if err := writePrivate(dir, "recipe-export.tar", archive); err != nil {
		return err
	}
	result.RecipeExportSHA256 = hash(archive)
	if _, err := os.Stat(filepath.Join(destination, "beads_test", ".dolt")); err != nil {
		return fmt.Errorf("actual retained database absent")
	}
	return nil
}

func freshAcceptSourceCapture(t *testing.T, dir, dataRoot string, result *freshResult) ([]byte, error) {
	t.Helper()
	data, err := readProducedBeadsArchive(filepath.Join(dir, "physical-evidence.json"))
	if err != nil {
		return nil, err
	}
	archive, err := readProducedBeadsArchive(filepath.Join(dir, "capture.tar"))
	if err != nil {
		return nil, err
	}
	var observed typedEvidence
	if err := strictTypedJSON(data, &observed); err != nil {
		return nil, err
	}
	if observed.Format != "beads-physical-evidence/1" || observed.SQLDefaults != "not-captured" || observed.StoreID != "native/"+hash([]byte(filepath.Join(dataRoot, "beads_test"))) || observed.ArchiveSHA256 != hash(archive) || observed.Working.Format != "beads-physical-working-root/1" || observed.Working.Database != "beads_test" || !observed.Working.ReadOnlyScope || !observed.Working.Complete || observed.Working.WorkingRootHash == "" || observed.Working.HeadCommitID == "" || len(observed.Projects) != 1 || observed.Projects[0] != "demo" {
		return nil, fmt.Errorf("fresh same-source typed identity/completeness mismatch")
	}
	if err := validateTypedTables(observed.Working.Tables); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, image := range observed.Images {
		if image.CommitID == "" || seen[image.CommitID] || !image.ReadOnlyScope || image.Tables == nil {
			return nil, fmt.Errorf("fresh immutable source image missing/duplicate/unauthenticated")
		}
		seen[image.CommitID] = true
		if len(image.Tables) > 0 {
			if err := validateTypedTables(image.Tables); err != nil {
				return nil, err
			}
		}
	}
	if !seen[observed.Working.HeadCommitID] {
		return nil, fmt.Errorf("fresh immutable head missing")
	}
	manifest := &typedManifest{Evidence: observed, EvidenceSHA256: hash(data)}
	if err := bindTypedArchive(archive, manifest); err != nil {
		return nil, err
	}
	result.CaptureArchiveSHA256, result.PhysicalSHA256 = hash(archive), hash(data)
	result.Head, result.WorkingRoot = observed.Working.HeadCommitID, observed.Working.WorkingRootHash
	result.WorkingFamilies, result.ImmutableImages = len(observed.Working.Tables), len(observed.Images)
	freshJSON(t, filepath.Join(dir, "accepted-source.json"), map[string]any{"case": result.Case, "source_root": dataRoot, "recipe_export_sha256": result.RecipeExportSHA256, "retained_export_archive_sha256": result.RetainedArchiveSHA256, "evidence": observed, "physical_sha256": hash(data), "reader_commit": typedReaderCommit, "reader_tree": typedReaderTree, "role": "intended-source", "equality_credit": 0})
	return archive, nil
}

func freshChildEnv(workspace string) []string {
	env := beadsChildEnv(workspace, filepath.Join(workspace, "home"))
	env[0] = "PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin"
	return env
}

func freshCommand(t *testing.T, evidence, label, dir, binary string, args, environment []string, timeout time.Duration) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = dir
	if environment == nil {
		environment = append(os.Environ(), "PATH=/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	}
	cmd.Env = environment
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now().UTC()
	err := cmd.Run()
	end := time.Now().UTC()
	exit := 0
	if err != nil {
		exit = -1
		if cmd.ProcessState != nil {
			exit = cmd.ProcessState.ExitCode()
		}
	}
	for _, stream := range []struct {
		name string
		data []byte
	}{{"stdout", stdout.Bytes()}, {"stderr", stderr.Bytes()}} {
		if err := writePrivate(evidence, label+"."+stream.name, stream.data); err != nil {
			t.Fatal(err)
		}
	}
	// Retain only the explicit public environment, not inherited CI secrets.
	publicEnv := []string{}
	for _, value := range environment {
		if strings.HasPrefix(value, "PATH=") || strings.HasPrefix(value, "BEADS_") || strings.HasPrefix(value, "HOME=") || strings.HasPrefix(value, "XDG_") || strings.HasPrefix(value, "GIT_CONFIG_NOSYSTEM=") {
			publicEnv = append(publicEnv, value)
		}
	}
	freshJSON(t, filepath.Join(evidence, label+".command.json"), map[string]any{"executable": binary, "executable_sha256": freshFileSHA(t, binary), "args": args, "directory": dir, "public_environment": publicEnv, "started_utc": start.Format(time.RFC3339Nano), "ended_utc": end.Format(time.RFC3339Nano), "exit": exit, "error": fmt.Sprint(err), "stdout_sha256": hash(stdout.Bytes()), "stderr_sha256": hash(stderr.Bytes())})
	return stdout.Bytes(), err
}

func freshFileSHA(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, readErr := io.Copy(h, file)
	if err := errors.Join(readErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func freshTreeManifest(t *testing.T, root, output string) {
	t.Helper()
	var entries []map[string]any
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular retained source entry")
		}
		row := map[string]any{"path": relative, "mode": info.Mode().String(), "size": info.Size()}
		if info.Mode().IsRegular() {
			row["sha256"] = freshFileSHA(t, path)
		}
		entries = append(entries, row)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	freshJSON(t, output, entries)
}

func freshJSONBytes(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func freshJSON(t *testing.T, path string, value any) {
	t.Helper()
	if err := writePrivate(filepath.Dir(path), filepath.Base(path), freshJSONBytes(t, value)); err != nil {
		t.Fatal(err)
	}
}

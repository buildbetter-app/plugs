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
	SourceRoot            string    `json:"source_root,omitempty"`
	RecipeExecutableSHA   string    `json:"recipe_executable_sha256,omitempty"`
	NativeContinuation    string    `json:"native_continuation"`
	PlugRestore           string    `json:"plug_restore"`
	PlugContinuation      string    `json:"plug_continuation"`
	CaptureSHA256         string    `json:"capture_sha256,omitempty"`
	RequestSHA256         string    `json:"request_sha256,omitempty"`
	ConsumerELFSHA256     string    `json:"consumer_elf_sha256,omitempty"`
}

func freshGuard(t *testing.T) {
	t.Helper()
	for _, path := range []string{"/tmp/plug-swarm/STOP", "/tmp/plug-swarm/PAUSE"} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("fresh public recording stopped/paused")
		}
	}
}

// This entry remains explicitly selected. Without the additional mode opt-in,
// only Full runs. The other nine retain their original mode and dependency WAIT.
func TestFreshSameRunIDStyleATwelveFixtures(t *testing.T) {
	freshGuard(t)
	selected := os.Getenv("PLUG_ID_STYLE_A_FRESH_RUN")
	if selected == "" {
		t.Skip("explicit fresh public native recording opt-in required")
	}
	if selected != "1" {
		t.Fatal("invalid fresh recording opt-in")
	}
	modeOptIn := os.Getenv("PLUG_ID_STYLE_A_FRESH_MODES")
	if modeOptIn != "" && modeOptIn != "1" {
		t.Fatal("invalid selected-mode recording opt-in")
	}
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	parent, run := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"), os.Getenv("GITHUB_RUN_ID")
	if !filepath.IsAbs(parent) || run == "" || os.Getenv("TEST_TARGET") != "//tools/roundtrip:fresh_same_run_native_test" {
		t.Fatal("explicit fresh recording target and actual run binding required")
	}
	root, err := os.MkdirTemp(parent, "fresh-id-style-a-")
	if err != nil {
		t.Fatal(err)
	}
	buildRoot := filepath.Join(root, "native-build")
	if err := os.Mkdir(buildRoot, 0700); err != nil {
		t.Fatal(err)
	}
	producer := freshBuildProducer(t, buildRoot)
	consumer := freshRetainConsumer(t, buildRoot)
	var results []freshResult
	builder := publicnative.SourceBuilder{T: t, Root: buildRoot, SourceDirectory: "testdata/fresh-native", Target: publicNativeProtocolExporterLabel(t)}
	builder.BuildWithSource(freshNativeSource, func(source publicnative.SourceSession) {
		for _, item := range freshNativeCases() {
			t.Run(item.Engine+"/"+item.Name, func(t *testing.T) {
				result := freshResult{Case: item, Status: "NOT_RUN", Gaps: []string{}, NativeContinuation: "NOT_RUN", PlugRestore: "NOT_RUN", PlugContinuation: "NOT_RUN"}
				defer func() {
					if t.Failed() {
						result.Status = "FAILED"
					}
					results = append(results, result)
					freshJSON(t, filepath.Join(root, item.Engine+"-"+item.Name+".json"), result)
					t.Logf("FRESH_SAME_RUN_CASE %s", freshJSONBytes(t, result))
				}()
				if item.Mode != "full" && modeOptIn != "1" {
					result.Status = "WAIT"
					result.Gaps = []string{"LANDED_MODE_LAYER_AND_MATCHING_GLUE_REQUIRED"}
					t.Skip("retained original mode awaits landed receiver mode and matching shared glue")
				}
				freshNativeCase(t, root, source, item, producer, consumer, &result)
			})
		}
	})
	freshJSON(t, filepath.Join(root, "twelve-case-inventory.json"), results)
	credit := 0
	for _, result := range results {
		credit += result.EqualityCredit
	}
	modeWait := 0
	if modeOptIn != "1" {
		modeWait = 9
	}
	freshJSON(t, filepath.Join(root, "equality-census.json"), map[string]any{"total": 12, "equal": credit, "mode_wait": modeWait, "run_id": run, "selected_mode_opt_in": modeOptIn == "1"})
	if len(results) != 12 {
		t.Fatalf("fresh native census incomplete: %d/12", len(results))
	}
}

func freshRecordRecipe(t *testing.T, source publicnative.SourceSession, item freshCase, dir, destination string, result *freshResult) error {
	t.Helper()
	profileData := publicNativeProtocolInput(t, "tools/roundtrip/testdata/fresh-native/recipe-profile.json", freshRecipeProfileSHA)
	target, normalArguments, err := freshRecipeInvocation(profileData, freshNativeSource.Commit, item.Target)
	if err != nil {
		return err
	}
	dolt := os.Getenv("PLUG_PINNED_DOLT_BINARY")
	if !filepath.IsAbs(dolt) {
		dolt = filepath.Join(os.Getenv("TEST_SRCDIR"), dolt)
	}
	dolt, err = filepath.EvalSymlinks(dolt)
	if err != nil || freshFileSHA(t, dolt) != "72352f1d882472a7a8d6124e9c4dc12f80e2c2359ea95f1424c032e9250e35e6" {
		return fmt.Errorf("pinned public Dolt runtime missing")
	}
	arguments := append(normalArguments,
		"--test_sharding_strategy=disabled", "--test_arg=-test.v", "--test_arg=-test.short=false", "--test_arg=-test.timeout=25m", "--test_timeout=1800", "--test_arg=-test.run=^"+item.Recipe+"$", "--test_arg=-plug-recipe-retain-store="+destination,
		"--test_env=BEADS_TEST_DOLT_SERVER=local", "--test_env=BEADS_TEST_DOLT_BINARY="+dolt, "--test_env=BEADS_TEST_REQUIRE_DOLT_CONTAINER=1", "--test_env=BEADS_TEST_SKIP=", "--test_env=PLUG_TEST_EXPORT_PUBLIC_HISTORY=1", "--test_env=PATH=/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	binary, args := source.TestArguments(target, arguments...)
	_, runErr := freshCommand(t, dir, "native-recipe", source.Directory, binary, args, nil, 40*time.Minute)
	targetPath := strings.ReplaceAll(strings.TrimPrefix(target, "//"), ":", "/")
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
	query, queryArgs := source.QueryArguments(target)
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
	retained := filepath.Join(filepath.Dir(dir), "native-build", item.Target+"-recipe-executable")
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
	nativeEnv := map[string]bool{}
	for _, value := range freshContinuationEnvironment(t) {
		nativeEnv[value] = true
	}
	for _, value := range environment {
		if nativeEnv[value] || strings.HasPrefix(value, "PATH=") || strings.HasPrefix(value, "BEADS_") || strings.HasPrefix(value, "HOME=") || strings.HasPrefix(value, "XDG_") || strings.HasPrefix(value, "GIT_CONFIG_NOSYSTEM=") {
			publicEnv = append(publicEnv, value)
		} else if strings.HasPrefix(value, "PLUG_PUBLIC_PROOF_") || strings.HasPrefix(value, "PLUG_ID_STYLE_A_") || strings.HasPrefix(value, "GIT_AUTHOR_") || strings.HasPrefix(value, "GIT_COMMITTER_") || strings.HasPrefix(value, "GITHUB_RUN_ID=") {
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

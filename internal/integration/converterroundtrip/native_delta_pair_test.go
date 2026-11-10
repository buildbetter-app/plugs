package converterroundtrip

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// These public inputs and both product ELFs are original retained outputs.
// The test does not recapture or synthesize them. Production remains the
// reviewed 26e executable; the pinned overlay supplies the independent caller.
const retainedPairManifestSHA256 = "ba8a8de60b2180c0d999a45fc10a69881dbccb7368f25cccc3813b8b749160c0"

func retainedPairFileBinding(t *testing.T, path string) retainedPairFile {
	t.Helper()
	retainedPairGuard(t)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatal("retained pair input is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	n, readErr := io.Copy(h, file)
	closeErr := file.Close()
	after, statErr := os.Lstat(path)
	if readErr != nil || closeErr != nil || statErr != nil ||
		!os.SameFile(info, after) || info.Size() != n ||
		!info.ModTime().Equal(after.ModTime()) {
		t.Fatal("retained pair file changed while hashing")
	}
	return retainedPairFile{SHA256: hex.EncodeToString(h.Sum(nil)), Bytes: uint64(n)}
}

func retainedPairExtract(t *testing.T, path, root string, expected retainedPairFile) {
	t.Helper()
	// Bazel exposes declared source data through runfile symlinks. Bind the
	// resolved compressed original before extracting into the owned directory.
	path, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	if retainedPairFileBinding(t, path) != expected {
		t.Fatal("retained pair compressed transport binding differs")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	compressed, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	archive := tar.NewReader(compressed)
	for {
		retainedPairGuard(t)
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeReg || header.Mode != 0o444 || header.Size < 0 ||
			!filepath.IsLocal(header.Name) || filepath.Clean(header.Name) != header.Name {
			t.Fatal("retained pair transport has an unexpected member")
		}
		path := filepath.Join(root, header.Name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal("retained pair transport has a duplicate or inaccessible member")
		}
		n, copyErr := io.Copy(out, archive)
		syncErr := out.Sync()
		modeErr := out.Chmod(0o444)
		closeErr := out.Close()
		if copyErr != nil || syncErr != nil || modeErr != nil || closeErr != nil || n != header.Size {
			t.Fatal("retained pair transport extraction differs")
		}
	}
}

func retainedPairSaveJSON(t *testing.T, path string, value any) {
	t.Helper()
	retainedPairGuard(t)
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.Write(append(data, '\n'))
	syncErr := file.Sync()
	modeErr := file.Chmod(0o444)
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || modeErr != nil || closeErr != nil {
		t.Fatal("retained pair original could not be frozen")
	}
}

func TestNativeDeltaRetainedPhysicalProducerReceiverPair(t *testing.T) {
	retainedPairGuard(t)
	outputs := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if outputs == "" {
		t.Fatal("retained pair requires the ordinary remote test output directory")
	}
	outputs, err := filepath.EvalSymlinks(outputs)
	if err != nil {
		t.Fatal(err)
	}
	outputs, err = filepath.Abs(outputs)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(outputs, "native-delta-retained-pair")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal("retained pair output must be an absent owned directory")
	}
	bundle := filepath.Join(root, "bundle")
	if err := os.Mkdir(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	data := filepath.Join("testdata", "native-delta-retained-pair")
	retainedPairExtract(t, filepath.Join(data, "base.tar.gz"), bundle, retainedPairFile{
		SHA256: "d879a0b2fb8b251ee095becfeac6f2b3dbefcc9490d2b044703bb7ae3ea14bd3", Bytes: 47309369,
	})
	retainedPairExtract(t, filepath.Join(data, "caller-overlay.tar.gz"), bundle, retainedPairFile{
		SHA256: "be7ceb20085be9b23e9732e2e442074edf61ef10ede1e54e7f25ba75ad37f185", Bytes: 87205,
	})
	var manifest struct{ Files map[string]retainedPairFile }
	manifestBinding := retainedPairFile{SHA256: retainedPairManifestSHA256, Bytes: 10468}
	raw := retainedPairBoundBytes(t, filepath.Join(bundle, "manifest.json"), manifestBinding)
	if err := json.Unmarshal(raw, &manifest); err != nil || len(manifest.Files) != 57 {
		t.Fatal("retained pair combined manifest differs")
	}
	seen := 0
	if err := filepath.WalkDir(bundle, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		name, err := filepath.Rel(bundle, path)
		if err != nil {
			return err
		}
		if name != "manifest.json" {
			binding, ok := manifest.Files[filepath.ToSlash(name)]
			if !ok || !entry.Type().IsRegular() || retainedPairFileBinding(t, path) != binding {
				t.Fatal("retained pair combined member census or bytes differ")
			}
		}
		seen++
		return nil
	}); err != nil || seen != 58 {
		t.Fatal("retained pair combined member census incomplete")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("ordinary pair route requires Python 3")
	}
	run := filepath.Join(root, "run")
	args := []string{filepath.Join(bundle, "caller", "native_delta_pair_harness_bound_body_v2.py"),
		"--bundle", bundle, "--manifest-sha256", retainedPairManifestSHA256,
		"--manifest-bytes", "10468", "--output", run}
	retainedPairSaveJSON(t, filepath.Join(root, "harness.argv.json"), append([]string{python}, args...))
	command := exec.Command(python, args...)
	logs := make([]*os.File, 0, 2)
	for _, name := range []string{"harness.stdout", "harness.stderr"} {
		file, err := os.OpenFile(filepath.Join(root, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		logs = append(logs, file)
		defer file.Close()
	}
	command.Stdout, command.Stderr = logs[0], logs[1]
	started := time.Now()
	runErr := command.Run()
	ended := time.Now()
	for _, file := range logs {
		syncErr, modeErr, closeErr := file.Sync(), file.Chmod(0o444), file.Close()
		if syncErr != nil || modeErr != nil || closeErr != nil {
			t.Fatal("retained pair harness original freeze failed")
		}
	}
	exit := -1
	if command.ProcessState != nil {
		exit = command.ProcessState.ExitCode()
	}
	retainedPairSaveJSON(t, filepath.Join(root, "harness.wait.json"), map[string]any{
		"direct_wait_completed": command.ProcessState != nil, "exit_code": exit,
		"start_unix_ns": started.UnixNano(), "end_unix_ns": ended.UnixNano(),
		"elapsed_monotonic_ns": ended.Sub(started).Nanoseconds(), "publication_authorized": false,
	})
	if runErr != nil {
		t.Fatalf("retained genuine pair harness did not pass; originals retained at %s", root)
	}
	retainedPairGuard(t)
	sqliteCase := filepath.Join(run, "sqlite")
	raw, err = os.ReadFile(filepath.Join(sqliteCase, "expectation-binding.json"))
	if err != nil {
		t.Fatal(err)
	}
	var expectationBinding retainedPairFile
	retainedPairDecode(t, raw, &expectationBinding)
	inspectRetainedNativePair(t, sqliteCase, expectationBinding)
	retainedPairSaveJSON(t, filepath.Join(root, "integration-result.json"), map[string]any{
		"format":         "native-delta-retained-pair-integration/1",
		"producer_head":  "1a75f75feae282890c4b9b8769c54e8b6a23c2f4",
		"receiver_head":  "26e90d85a0be080538b34b97e17849e4ced61d8b",
		"input_manifest": manifestBinding, "sqlite_expectations": expectationBinding,
		"constructor_default_receiver_and_independent_replay": "MATCHED",
		"sqlite_persistent_readback":                          "MATCHED_ON_EXACT_COPY_ORIGINAL_PRESERVED",
		"all_accounting_category_preimages":                   "PENDING", "reverse_and_undo_execution": "NOT_RUN",
		"public_1200_loose_archived": "NOT_RUN", "publication_authorized": false, "private": "HARD_DISABLED",
	})
	t.Log("retained genuine native pair and independent persistent readback matched; publication=false")
}

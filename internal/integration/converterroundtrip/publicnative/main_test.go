package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

const smallFixture = "../../../core/dumpformat/testdata/native-joined-public-ca26683b"
const smallCreationSHA256 = "2e0e02826753f83f9bf72c621a2863ea7312c16af8875bb685495fa7fdc9c0e6"
const policySHA256 = "3afb19b934e69e90b9f8e966d5e4e4d1fab27a47686eacf38543e23d1c7542d9"

func testRegistry(t *testing.T) publicRegistry {
	t.Helper()
	r, err := registry()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func smallSource(t *testing.T, r publicRegistry, name string) (publicSource, string) {
	t.Helper()
	chosen, err := selected(r, []string{name})
	if err != nil || len(chosen) != 1 {
		t.Fatal("pinned public source missing", err)
	}
	source := chosen[0]
	creation, err := os.ReadFile(filepath.Join(smallFixture, "creation-record.json"))
	if err != nil || hash(creation) != smallCreationSHA256 {
		t.Fatal("physical creation original differs", err)
	}
	if source.Category != "small" || source.Commits != 97 {
		t.Fatal("small physical fixture identity differs")
	}
	dir := t.TempDir()
	for _, file := range source.Files {
		raw, err := os.ReadFile(filepath.Join(smallFixture, source.Project, file.Name))
		if err != nil || uint64(len(raw)) != file.Bytes || hash(raw) != file.SHA256 {
			t.Fatal("pinned original differs", file.Name, err)
		}
		path := filepath.Join(dir, file.Name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return source, dir
}

func assertReleased(t *testing.T, usage dumpformat.NativeDeltaDecodedUsage) {
	t.Helper()
	if usage.CurrentAccountedDecodedBytes != 0 || len(usage.CurrentCategories) != 0 || len(usage.CurrentCharges) != 0 {
		t.Fatal("byte/count owner cleanup differs", usage)
	}
	if usage.LimitBytes != 2<<30 || usage.PeakAccountedDecodedBytes > usage.LimitBytes {
		t.Fatal("fixed decoded ceiling differs", usage)
	}
}

func TestPublicNativeSourceAuthenticationAndOwnerLifecycle(t *testing.T) {
	r := testRegistry(t)
	for _, name := range []string{"small-ops", "small-sv2"} {
		t.Run(name, func(t *testing.T) {
			source, dir := smallSource(t, r, name)
			budget := dumpformat.NewNativeDeltaBudget(0)
			result, err := sourceProof(context.Background(), source, dir, budget)
			if err != nil || !reflect.DeepEqual(result.ReaderCalls, []int{1}) || !reflect.DeepEqual(result.VerifiedCommits, []uint64{97}) || result.BeforeClose.CurrentAccountedDecodedBytes == 0 {
				t.Fatal("actual pinned source authentication failed", result, err)
			}
			assertReleased(t, result.AfterClose)
			assertReleased(t, budget.Usage())
		})
	}
}

func TestPublicNativeSourceRefusalReleasesEveryReservation(t *testing.T) {
	r := testRegistry(t)
	source, dir := smallSource(t, r, "small-sv2")
	part := filepath.Join(dir, source.Files[2].Name)
	raw, err := os.ReadFile(part)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 1
	if err := os.WriteFile(part, raw, 0600); err != nil {
		t.Fatal(err)
	}
	budget := dumpformat.NewNativeDeltaBudget(0)
	result, err := sourceProof(context.Background(), source, dir, budget)
	if err == nil || !reflect.DeepEqual(result.ReaderCalls, []int{0}) || len(result.VerifiedCommits) != 0 {
		t.Fatal("corrupt physical original acquired positive proof", result, err)
	}
	assertReleased(t, result.AfterClose)
	assertReleased(t, budget.Usage())
}

func TestPublicNativeInputPinsOrderAndScaleBoundaries(t *testing.T) {
	r := testRegistry(t)
	want := []struct {
		name, category, project string
		commits                 uint64
	}{
		{"small-ops", "small", "ops", 97}, {"small-sv2", "small", "sv2", 97},
		{"scale-ops", "scale", "ops", 1614}, {"scale-sv2", "scale", "sv2", 1614},
		{"public1200-loose", "collision-negative", "publicv7", 1200}, {"public1200-archived", "collision-negative", "publicv7", 1200},
	}
	for i, expected := range want {
		s := r.Sources[i]
		if s.Name != expected.name || s.Category != expected.category || s.Project != expected.project || s.Commits != expected.commits || len(s.Files) != 3 || s.Files[0].Name != "capture-set.json" || s.Files[1].Name != "head.tar" || s.Files[2].Name != "parts/00000000000000000001.part" {
			t.Fatal("public input identity/count/layout differs", i)
		}
	}
	if r.Sources[2].StoreID == r.Sources[3].StoreID || r.Sources[4].StoreID == r.Sources[5].StoreID {
		t.Fatal("separate public sources lost distinct store identities")
	}
	// The public1200 histories share logical commit/row metadata. Their
	// original part encodings have the same length but distinct authentic
	// hashes; logical equality must never substitute one original for another.
	loosePart, archivedPart := r.Sources[4].Files[2], r.Sources[5].Files[2]
	if loosePart.Bytes != 32661956 || archivedPart.Bytes != 32661956 || loosePart.SHA256 != "c95eb5f891957289af8e6bdfee3b21782a4c3c4a5cf330cc9528e1b9d5fd7929" || archivedPart.SHA256 != "5cf25dc8ec815c2cddbe217572febb8d87fa8c9e0c7e378fe63457084f3ea4d6" {
		t.Fatal("same-history negative original-file commitments differ")
	}
	for _, names := range [][]string{{"small-ops", "small-sv2"}, {"scale-ops", "scale-sv2"}} {
		out := filepath.Join(t.TempDir(), "inputs")
		if _, err := run(context.Background(), []string{"--mode", "inputs", "--source", names[0], "--source", names[1], "--output", out}); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(out, "binding.json"))
		if err != nil {
			t.Fatal(err)
		}
		var b dumpformat.NativeJoinedBinding
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatal(err)
		}
		profile, err := os.ReadFile(filepath.Join(out, "profile.json"))
		if err != nil || b.Format != dumpformat.NativeJoinedInputsFormatV2 || b.PolicySHA256 != policySHA256 || b.ProfileSHA256 != hash(profile) || len(b.Sources) != 2 || b.Sources[0].SourceProject != "ops" || b.Sources[1].SourceProject != "sv2" {
			t.Fatal("ordered policy/profile binding differs", err)
		}
		for i, name := range names {
			sources, err := selected(r, []string{name})
			if err != nil || !reflect.DeepEqual(b.Sources[i].Files, sources[0].Files) || b.Sources[i].ArchiveSHA256 != sources[0].Files[0].SHA256 {
				t.Fatal("input construction changed pinned file ledger", i, err)
			}
		}
		provenance, err := os.ReadFile(filepath.Join(out, "provenance.json"))
		if err != nil || !bytes.Equal(provenance, registryBytes) {
			t.Fatal("input construction lost provenance", err)
		}
	}
	for _, names := range [][]string{{"small-sv2", "small-ops"}, {"public1200-loose", "public1200-archived"}, {"small-ops", "scale-sv2"}, {"small-ops", "small-ops"}} {
		out := filepath.Join(t.TempDir(), "refused")
		_, err := run(context.Background(), []string{"--mode", "inputs", "--source", names[0], "--source", names[1], "--output", out})
		if err == nil {
			t.Fatal("wrong/swapped/same-project pair admitted", names)
		}
		if _, err := os.Stat(out); !os.IsNotExist(err) {
			t.Fatal("refused pair wrote output", err)
		}
	}
}

// Bazel runfiles may be symlinks. These regular executable copies preserve
// the exact remote-built bytes while exercising the external runner's guard.
func proofExecutable(t *testing.T, variable, name string) (string, string) {
	t.Helper()
	source := os.Getenv(variable)
	if source == "" {
		t.Fatal("remote-built executable required", variable)
	}
	if !filepath.IsAbs(source) {
		source = filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), source)
	}
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	dest := filepath.Join(t.TempDir(), name)
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(f, h), input)
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatal(copyErr, closeErr)
	}
	return dest, hex.EncodeToString(h.Sum(nil))
}

func retainProofFile(t *testing.T, source, dest string, mode os.FileMode) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(f, input)
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatal(copyErr, closeErr)
	}
}

// Give Python time to terminate and wait for its separately sessioned child
// and retain the wait receipt. CommandContext's default SIGKILL skips that.
func proofPythonCommand(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "python3", args...)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 30 * time.Second
	return cmd
}

func TestPublicNativeRunnerCancellationReapsChildGroup(t *testing.T) {
	script, err := filepath.Abs("run-public-proof.py")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if undeclared := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); undeclared != "" {
		root = filepath.Join(undeclared, "public-native-cancel")
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	ready := filepath.Join(root, "child-started.pid")
	// This child is a timeout probe, with no source, mapper or store. It
	// records its actual PID before waiting, so cancellation is deterministic.
	probe := `import os,runpy,sys,time
from pathlib import Path
os.umask(0o077)
runner=runpy.run_path(sys.argv[1],run_name="public_native_probe")
runner["install_signal_handlers"]()
runner["invoke"](Path(sys.argv[2]),"cancelled-child",[sys.executable,"-c","import os,sys,time; from pathlib import Path; Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(600)",sys.argv[3]],time.monotonic()+7200)
`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := proofPythonCommand(ctx, "-c", probe, script, root, ready)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var child int
	until := time.Now().Add(15 * time.Second)
	for time.Now().Before(until) {
		raw, err := os.ReadFile(ready)
		if err == nil {
			child, err = strconv.Atoi(string(raw))
			if err == nil && child > 0 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	err = cmd.Wait()
	for name, raw := range map[string][]byte{"supervisor-stdout.txt": stdout.Bytes(), "supervisor-stderr.txt": stderr.Bytes()} {
		if writeErr := os.WriteFile(filepath.Join(root, name), raw, 0600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if child == 0 || err == nil {
		t.Fatal("cancellation probe did not start and refuse", child, err, stderr.String())
	}
	if err := syscall.Kill(child, 0); err != syscall.ESRCH {
		// Keep even a failed oracle from leaving its probe running.
		_ = syscall.Kill(-child, syscall.SIGKILL)
		t.Fatal("runner left an unreaped active child", child, err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "cancelled-child", "wait.json"))
	if err != nil {
		t.Fatal("interrupted command lost its wait original", err)
	}
	var wait struct {
		Exit        int  `json:"exit"`
		Interrupted bool `json:"interrupted"`
	}
	if err := json.Unmarshal(raw, &wait); err != nil || !wait.Interrupted || wait.Exit >= 0 {
		t.Fatal("interrupted child was not terminated and waited", wait, err)
	}
}

func TestPublicNativeRunnerExercisesActualSmallJoinedReceiver(t *testing.T) {
	r := testRegistry(t)
	_, ops := smallSource(t, r, "small-ops")
	_, sv2 := smallSource(t, r, "small-sv2")
	proofBinary, proofHash := proofExecutable(t, "PUBLIC_NATIVE_PROOF_BINARY", "publicnative")
	receiver, receiverHash := proofExecutable(t, "NATIVE_RECEIVE_BINARY", "native-receive")
	policy, err := filepath.Abs("../testdata/real-store-import-policy-v2.md")
	if err != nil {
		t.Fatal(err)
	}
	// The CLI requires a regular intent file; Bazel runfiles may be symlinks.
	regularPolicy := filepath.Join(t.TempDir(), "policy.md")
	retainProofFile(t, policy, regularPolicy, 0600)
	policy = regularPolicy
	script, err := filepath.Abs("run-public-proof.py")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if undeclared := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); undeclared != "" {
		root = filepath.Join(undeclared, "public-native-proof")
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
		for _, file := range []struct {
			source, name string
			mode         os.FileMode
		}{
			{proofBinary, "publicnative", 0700}, {receiver, "native-receive", 0700},
			{script, "run-public-proof.py", 0600}, {policy, "policy.md", 0600},
		} {
			retainProofFile(t, file.source, filepath.Join(root, file.name), file.mode)
		}
		if err := os.WriteFile(filepath.Join(root, "inputs.json"), registryBytes, 0600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{script, "--case", "positive", "--source", "small-ops", "--source", "small-sv2", "--directory", ops, "--directory", sv2, "--proof-binary", proofBinary, "--proof-sha256", proofHash, "--receiver-binary", receiver, "--receiver-sha256", receiverHash, "--policy", policy, "--output", filepath.Join(root, "small-memory")}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	cmd := proofPythonCommand(ctx, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	argv, marshalErr := json.Marshal(append([]string{"python3"}, args...))
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	for name, raw := range map[string][]byte{"runner-argv.json": argv, "runner-stdout.txt": stdout.Bytes(), "runner-stderr.txt": stderr.Bytes()} {
		if writeErr := os.WriteFile(filepath.Join(root, name), raw, 0600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if err != nil {
		t.Fatal("actual Python/receiver public proof failed; originals retained", err, stderr.String())
	}
	raw, err := os.ReadFile(filepath.Join(root, "small-memory", "PASS.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Format   string   `json:"format"`
		Commits  []uint64 `json:"verified_commits"`
		Receiver string   `json:"receiver_sha256"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.Format != "plug-public-native-joined-proof/1" || !reflect.DeepEqual(result.Commits, []uint64{97, 97}) || result.Receiver != receiverHash {
		t.Fatal("independent public proof summary differs", result, err)
	}
	t.Logf("PUBLIC_NATIVE_SMALL_JOINED_PROOF sources=2 commits=194 driver=memory receiver_sha256=%s proof_sha256=%s", receiverHash, proofHash)
}

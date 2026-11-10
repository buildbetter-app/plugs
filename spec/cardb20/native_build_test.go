package cardb20

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"debug/buildinfo"
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
)

// This is the admitted-main B09 exact-source build recipe. The B20 recorder
// reuses its declared archive and Git manifest; it creates no producer or
// exporter and never invokes an official bundle action.
type nativeSource struct {
	Name, Commit, Tree, Parent, ArchiveSHA, ManifestSHA string
	Entries                                             int
}

var pinnedSource = nativeSource{
	"beads-9d6fff", "9d6fff760b7a314b476539198504d77ce0959656",
	"cde121d6e4119e454857ad1c3ca293e2791be840", "cadad184cefce7da159ea4b18c9bd44865e72f6b",
	"8aa8d07655ec6903703a4c94a4b1aa12d4fb509ab647cd1958f73f8342d3fe6e",
	"23d84a56302ad19c50d9d25af733b2da50f9137aaf9308b273cab039b6023ae3", 5087,
}

type nativeRecorder struct {
	t      *testing.T
	root   string
	target string
}

func hashBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}
func (r *nativeRecorder) writeJSON(path string, value any) {
	r.t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		r.t.Fatal(err)
	}
}
func (r *nativeRecorder) buildCanonicalSource(spec nativeSource) string {
	r.t.Helper()
	archive, err := filepath.Abs(filepath.Join(os.Getenv("PLUG_SOURCE_MODULE_ROOT"), "spec/s1-b09-native/testdata/"+spec.Name+".tar.gz"))
	if err != nil {
		r.t.Fatal(err)
	}
	archiveSHA, archiveSize, err := hashFile(archive)
	if err != nil || archiveSHA != spec.ArchiveSHA {
		r.t.Fatalf("declared source archive mismatch: %s %v", archiveSHA, err)
	}
	manifest, err := os.ReadFile(filepath.Join(os.Getenv("PLUG_SOURCE_MODULE_ROOT"), "spec/s1-b09-native/testdata/"+spec.Name+".git-tree"))
	if err != nil || hashBytes(manifest) != spec.ManifestSHA {
		r.t.Fatal("declared plain Git source manifest mismatch")
	}
	source := filepath.Join(r.t.TempDir(), "beads-source")
	if err := os.Mkdir(source, 0700); err != nil {
		r.t.Fatal(err)
	}
	r.buildCommand(source, spec.Name+"-extract-source", "/usr/bin/tar", []string{"-xzf", archive, "-C", source})
	entries := strings.Split(strings.TrimSuffix(string(manifest), "\n"), "\n")
	if len(entries) != spec.Entries {
		r.t.Fatalf("source manifest entries=%d", len(entries))
	}
	for _, entry := range entries {
		identity, name, ok := strings.Cut(entry, "\t")
		parts := strings.Fields(identity)
		if !ok || len(parts) != 3 || parts[1] != "blob" || filepath.IsAbs(name) || filepath.Clean(name) != name || strings.HasPrefix(name, "../") {
			r.t.Fatal("invalid pinned source manifest entry")
		}
		path := filepath.Join(source, name)
		var data []byte
		if parts[0] == "120000" {
			target, err := os.Readlink(path)
			if err != nil {
				r.t.Fatal(err)
			}
			data = []byte(target)
		} else {
			info, err := os.Lstat(path)
			if err != nil || !info.Mode().IsRegular() || parts[0] != "100644" && parts[0] != "100755" || (info.Mode().Perm()&0111 != 0) != (parts[0] == "100755") {
				r.t.Fatalf("source mode mismatch: %s %v", name, err)
			}
			data, err = os.ReadFile(path)
			if err != nil {
				r.t.Fatal(err)
			}
		}
		h := sha1.New()
		fmt.Fprintf(h, "blob %d\x00", len(data))
		h.Write(data)
		if hex.EncodeToString(h.Sum(nil)) != parts[2] {
			r.t.Fatalf("source Git blob mismatch: %s", name)
		}
	}
	bazel := os.Getenv("BAZEL_BIN")
	if !filepath.IsAbs(bazel) {
		r.t.Fatal("trusted RunsOn Bazel binary path required")
	}
	outputPath := filepath.Join(r.t.TempDir(), "bazel")
	outputBase := "--output_base=" + outputPath
	// Bazel materializes read-only output directories. After the dedicated
	// server has shut down, restore owner access only on directories inside
	// this private output base so testing.TempDir can remove them. WalkDir
	// never follows symlinks into the shared repository cache; retained source
	// archives, executable bytes and build evidence are not modified.
	r.t.Cleanup(func() {
		changed := 0
		err := filepath.WalkDir(outputPath, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Mode().Perm()&0700 != 0700 {
				if err := os.Chmod(path, info.Mode().Perm()|0700); err != nil {
					return err
				}
				changed++
			}
			return nil
		})
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		r.writeJSON(filepath.Join(r.root, spec.Name+"-outputbase-cleanup.json"), map[string]any{"output_base": outputPath, "directories_made_owner_writable": changed, "error": fmt.Sprint(err), "follows_symlinks": false})
		if err != nil {
			r.t.Errorf("private Bazel output-base cleanup: %v", err)
		}
	})
	if got := strings.TrimSpace(string(r.buildCommand(source, spec.Name+"-bazel-version", bazel, []string{outputBase, "version", "--gnu_format"}))); got != "Bazelisk v1.29.0\nbazel 9.3.0" {
		r.t.Fatalf("Bazel version=%q", got)
	}
	defer r.buildCommand(source, spec.Name+"-bazel-shutdown", bazel, []string{outputBase, "shutdown"})
	settings := []string{"--repo_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1", "--extra_execution_platforms=//platforms:rbe_worker", "--incompatible_strict_action_env", "--@rules_go//go/config:tags=gms_pure_go", "--@rules_go//go/config:gc_linkopts=-X=main.Commit=" + spec.Commit + ",-X=main.Build=b20-public-fixture-" + spec.Commit}
	config := append([]string{outputBase, "build", "--jobs=2", "--local_resources=memory=8192", "--run_validations", "--lockfile_mode=error"}, settings...)
	config = append(config, r.target)
	r.buildCommand(source, spec.Name+"-canonical-bazel-build", bazel, config)
	query := append([]string{outputBase, "cquery"}, settings...)
	query = append(query, "--output=files", r.target)
	outputs := strings.Fields(string(r.buildCommand(source, spec.Name+"-canonical-cquery", bazel, query)))
	if len(outputs) != 1 {
		r.t.Fatalf("canonical executable outputs=%v", outputs)
	}
	input, err := os.Open(filepath.Join(source, outputs[0]))
	if err != nil {
		r.t.Fatal(err)
	}
	defer input.Close()
	binary := filepath.Join(r.root, spec.Name+"-cli")
	output, err := os.OpenFile(binary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
	if err != nil {
		r.t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		r.t.Fatalf("retain actual canonical binary: %v %v", copyErr, closeErr)
	}
	if err := os.Chmod(binary, 0755); err != nil {
		r.t.Fatal(err)
	}
	mode, err := os.Lstat(binary)
	if err != nil || !mode.Mode().IsRegular() || mode.Mode().Perm() != 0755 {
		r.t.Fatalf("canonical binary mode/type mismatch: %v", err)
	}
	sha, size, err := hashFile(binary)
	if err != nil {
		r.t.Fatal(err)
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil || info.GoVersion != "go1.26.10" {
		r.t.Fatalf("actual canonical binary Go toolchain: %+v %v", info, err)
	}
	wrapper, err := os.ReadFile(bazel)
	if err != nil || !bytes.Contains(wrapper, []byte("--nosystem_rc --nohome_rc --noworkspace_rc")) {
		r.t.Fatalf("trusted RunsOn Bazel wrapper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(r.root, spec.Name+"-runs-on-bazel-wrapper.sh"), wrapper, 0600); err != nil {
		r.t.Fatal(err)
	}
	rcPath := os.Getenv("BAZEL_RC")
	rc, err := os.ReadFile(rcPath)
	if err != nil {
		r.t.Fatalf("trusted RunsOn rc required: %v", err)
	}
	if err := os.WriteFile(filepath.Join(r.root, spec.Name+"-runs-on.bazelrc"), rc, 0600); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.root, spec.Name+".git-tree"), manifest, 0600); err != nil {
		r.t.Fatal(err)
	}
	for _, name := range []string{".bazelversion", ".bazelrc", "MODULE.bazel", "MODULE.bazel.lock"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			r.t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(r.root, spec.Name+"-"+strings.TrimPrefix(name, ".")), data, 0600); err != nil {
			r.t.Fatal(err)
		}
	}
	r.writeJSON(filepath.Join(r.root, spec.Name+"-build.json"), map[string]any{
		"commit": spec.Commit, "tree": spec.Tree, "parent": spec.Parent,
		"source_archive_sha256": archiveSHA, "source_archive_size": archiveSize,
		"plain_git_manifest_sha256": spec.ManifestSHA, "source_entries": len(entries),
		"binary_path": binary, "binary_sha256": sha, "binary_size": size, "binary_mode": fmt.Sprintf("%04o", mode.Mode().Perm()), "target": r.target,
		"build_argv":    append([]string{bazel}, config...),
		"bazel_version": "9.3.0", "go_version": info.GoVersion, "go_build_info": info,
		"wrapper_sha256": hashBytes(wrapper), "rc_sha256": hashBytes(rc), "canonical_validations": true,
	})
	return binary
}

func (r *nativeRecorder) buildCommand(dir, label, binary string, args []string) []byte {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -2
		var failure *exec.ExitError
		if errors.As(err, &failure) {
			exit = failure.ExitCode()
		}
	}
	outPath, errPath := filepath.Join(r.root, label+".stdout"), filepath.Join(r.root, label+".stderr")
	if writeErr := os.WriteFile(outPath, stdout.Bytes(), 0600); writeErr != nil {
		r.t.Fatal(writeErr)
	}
	if writeErr := os.WriteFile(errPath, stderr.Bytes(), 0600); writeErr != nil {
		r.t.Fatal(writeErr)
	}
	r.writeJSON(filepath.Join(r.root, label+".request.json"), map[string]any{"argv": append([]string{binary}, args...), "directory": dir, "exit": exit, "error": fmt.Sprint(err), "stdout": outPath, "stderr": errPath, "stdout_sha256": hashBytes(stdout.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes())})
	if err != nil {
		r.t.Fatalf("%s: %v\n%s", label, err, stderr.Bytes())
	}
	return stdout.Bytes()
}

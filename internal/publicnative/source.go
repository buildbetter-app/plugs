// Package publicnative shares the existing canonical public fixture build.
package publicnative

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

// CanonicalSource binds a declared immutable archive and its complete Git tree.
type CanonicalSource struct {
	Name, Commit, Tree, Parent, ArchiveSHA, ManifestSHA string
	Entries                                             int
}

// SourceBuilder is the one fixture build implementation shared by consumers.
// It does not fetch or admit producer bundles. Commands and actual build bytes
// remain in Root; extracted source and the private Bazel server are temporary.
type SourceBuilder struct {
	T                             testing.TB
	Root, SourceDirectory, Target string
}

func (b SourceBuilder) Build(spec CanonicalSource) string {
	b.T.Helper()
	archive, err := filepath.Abs(filepath.Join(b.SourceDirectory, spec.Name+".tar.gz"))
	if err != nil {
		b.T.Fatal(err)
	}
	archiveSHA, archiveSize, err := hashFile(archive)
	if err != nil || archiveSHA != spec.ArchiveSHA {
		b.T.Fatalf("declared source archive mismatch: %s %v", archiveSHA, err)
	}
	manifest, err := os.ReadFile(filepath.Join(b.SourceDirectory, spec.Name+".git-tree"))
	if err != nil || hashBytes(manifest) != spec.ManifestSHA {
		b.T.Fatal("declared plain Git source manifest mismatch")
	}
	source := filepath.Join(b.T.TempDir(), "beads-source")
	if err := os.Mkdir(source, 0700); err != nil {
		b.T.Fatal(err)
	}
	b.Command(source, spec.Name+"-extract-source", "/usr/bin/tar", []string{"-xzf", archive, "-C", source})
	entries := strings.Split(strings.TrimSuffix(string(manifest), "\n"), "\n")
	if len(entries) != spec.Entries {
		b.T.Fatalf("source manifest entries=%d", len(entries))
	}
	for _, entry := range entries {
		identity, name, ok := strings.Cut(entry, "\t")
		parts := strings.Fields(identity)
		if !ok || len(parts) != 3 || parts[1] != "blob" || filepath.IsAbs(name) || filepath.Clean(name) != name || strings.HasPrefix(name, "../") {
			b.T.Fatal("invalid pinned source manifest entry")
		}
		path := filepath.Join(source, name)
		var data []byte
		if parts[0] == "120000" {
			target, err := os.Readlink(path)
			if err != nil {
				b.T.Fatal(err)
			}
			data = []byte(target)
		} else {
			info, err := os.Lstat(path)
			if err != nil || !info.Mode().IsRegular() || parts[0] != "100644" && parts[0] != "100755" || (info.Mode().Perm()&0111 != 0) != (parts[0] == "100755") {
				b.T.Fatalf("source mode mismatch: %s %v", name, err)
			}
			data, err = os.ReadFile(path)
			if err != nil {
				b.T.Fatal(err)
			}
		}
		h := sha1.New()
		fmt.Fprintf(h, "blob %d\x00", len(data))
		h.Write(data)
		if hex.EncodeToString(h.Sum(nil)) != parts[2] {
			b.T.Fatalf("source Git blob mismatch: %s", name)
		}
	}
	bazel := os.Getenv("BAZEL_BIN")
	if !filepath.IsAbs(bazel) {
		b.T.Fatal("trusted RunsOn Bazel binary path required")
	}
	// The automatic PR workflow supplies the same pinned Bazelisk directly;
	// the remote workflow supplies its hermetic wrapper. Give the former the
	// identical explicit rc policy before reusing the existing build below.
	if binarySHA, _, err := hashFile(bazel); err == nil && binarySHA == "5a408715e932c0250d28bd84555f12edbf70117de42f9181691c736eacc4a992" {
		rc := os.Getenv("BAZEL_RC")
		if !filepath.IsAbs(rc) {
			b.T.Fatal("trusted RunsOn rc required")
		}
		quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
		wrapper := "#!/usr/bin/env bash\nset -euo pipefail\nexec " + quote(bazel) + " --nosystem_rc --nohome_rc --noworkspace_rc --bazelrc=" + quote(rc) + " \"$@\"\n"
		bazel = filepath.Join(b.Root, spec.Name+"-hermetic-bazel")
		if err := os.WriteFile(bazel, []byte(wrapper), 0500); err != nil {
			b.T.Fatal(err)
		}
	}
	outputPath := filepath.Join(b.T.TempDir(), "bazel")
	outputBase := "--output_base=" + outputPath
	// Bazel materializes read-only output directories. After the dedicated
	// server has shut down, restore owner access only on directories inside
	// this private output base so testing.TempDir can remove them. WalkDir
	// never follows symlinks into the shared repository cache; retained source
	// archives, executable bytes and build evidence are not modified.
	b.T.Cleanup(func() {
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
		b.writeJSON(filepath.Join(b.Root, spec.Name+"-outputbase-cleanup.json"), map[string]any{"output_base": outputPath, "directories_made_owner_writable": changed, "error": fmt.Sprint(err), "follows_symlinks": false})
		if err != nil {
			b.T.Errorf("private Bazel output-base cleanup: %v", err)
		}
	})
	if got := strings.TrimSpace(string(b.Command(source, spec.Name+"-bazel-version", bazel, []string{outputBase, "version", "--gnu_format"}))); got != "Bazelisk v1.29.0\nbazel 9.3.0" {
		b.T.Fatalf("Bazel version=%q", got)
	}
	defer b.Command(source, spec.Name+"-bazel-shutdown", bazel, []string{outputBase, "shutdown"})
	settings := []string{"--repo_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1", "--extra_execution_platforms=//platforms:rbe_worker", "--incompatible_strict_action_env", "--@rules_go//go/config:tags=gms_pure_go", "--@rules_go//go/config:gc_linkopts=-X=main.Commit=" + spec.Commit + ",-X=main.Build=id-style-a-fixture-" + spec.Commit}
	config := append([]string{outputBase, "build", "--jobs=2", "--local_resources=memory=8192", "--run_validations", "--lockfile_mode=error"}, settings...)
	config = append(config, b.Target)
	b.Command(source, spec.Name+"-canonical-bazel-build", bazel, config)
	query := append([]string{outputBase, "cquery"}, settings...)
	query = append(query, "--output=files", b.Target)
	outputs := strings.Fields(string(b.Command(source, spec.Name+"-canonical-cquery", bazel, query)))
	if len(outputs) != 1 {
		b.T.Fatalf("canonical executable outputs=%v", outputs)
	}
	input, err := os.Open(filepath.Join(source, outputs[0]))
	if err != nil {
		b.T.Fatal(err)
	}
	defer input.Close()
	binary := filepath.Join(b.Root, spec.Name+"-cli")
	output, err := os.OpenFile(binary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
	if err != nil {
		b.T.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		b.T.Fatalf("retain actual canonical binary: %v %v", copyErr, closeErr)
	}
	if err := os.Chmod(binary, 0755); err != nil {
		b.T.Fatal(err)
	}
	mode, err := os.Lstat(binary)
	if err != nil || !mode.Mode().IsRegular() || mode.Mode().Perm() != 0755 {
		b.T.Fatalf("canonical binary mode/type mismatch: %v", err)
	}
	sha, size, err := hashFile(binary)
	if err != nil {
		b.T.Fatal(err)
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil || info.GoVersion != "go1.26.10" {
		b.T.Fatalf("actual canonical binary Go toolchain: %+v %v", info, err)
	}
	wrapper, err := os.ReadFile(bazel)
	if err != nil || !bytes.Contains(wrapper, []byte("--nosystem_rc --nohome_rc --noworkspace_rc")) {
		b.T.Fatalf("trusted RunsOn Bazel wrapper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(b.Root, spec.Name+"-runs-on-bazel-wrapper.sh"), wrapper, 0600); err != nil {
		b.T.Fatal(err)
	}
	rcPath := os.Getenv("BAZEL_RC")
	rc, err := os.ReadFile(rcPath)
	if err != nil {
		b.T.Fatalf("trusted RunsOn rc required: %v", err)
	}
	if err := os.WriteFile(filepath.Join(b.Root, spec.Name+"-runs-on.bazelrc"), rc, 0600); err != nil {
		b.T.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b.Root, spec.Name+".git-tree"), manifest, 0600); err != nil {
		b.T.Fatal(err)
	}
	for _, name := range []string{".bazelversion", ".bazelrc", "MODULE.bazel", "MODULE.bazel.lock"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			b.T.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(b.Root, spec.Name+"-"+strings.TrimPrefix(name, ".")), data, 0600); err != nil {
			b.T.Fatal(err)
		}
	}
	b.writeJSON(filepath.Join(b.Root, spec.Name+"-build.json"), map[string]any{
		"commit": spec.Commit, "tree": spec.Tree, "parent": spec.Parent,
		"source_archive_sha256": archiveSHA, "source_archive_size": archiveSize,
		"plain_git_manifest_sha256": spec.ManifestSHA, "source_entries": len(entries),
		"binary_path": binary, "binary_sha256": sha, "binary_size": size, "binary_mode": fmt.Sprintf("%04o", mode.Mode().Perm()), "target": b.Target,
		"build_argv":    append([]string{bazel}, config...),
		"bazel_version": "9.3.0", "go_version": info.GoVersion, "go_build_info": info,
		"wrapper_sha256": hashBytes(wrapper), "rc_sha256": hashBytes(rc), "canonical_validations": true,
	})
	return binary
}

func (b SourceBuilder) Command(dir, label, binary string, args []string) []byte {
	b.T.Helper()
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
	outPath, errPath := filepath.Join(b.Root, label+".stdout"), filepath.Join(b.Root, label+".stderr")
	if writeErr := os.WriteFile(outPath, stdout.Bytes(), 0600); writeErr != nil {
		b.T.Fatal(writeErr)
	}
	if writeErr := os.WriteFile(errPath, stderr.Bytes(), 0600); writeErr != nil {
		b.T.Fatal(writeErr)
	}
	b.writeJSON(filepath.Join(b.Root, label+".request.json"), map[string]any{"argv": append([]string{binary}, args...), "directory": dir, "exit": exit, "error": fmt.Sprint(err), "stdout": outPath, "stderr": errPath, "stdout_sha256": hashBytes(stdout.Bytes()), "stderr_sha256": hashBytes(stderr.Bytes())})
	if err != nil {
		b.T.Fatalf("%s: %v\n%s", label, err, stderr.Bytes())
	}
	return stdout.Bytes()
}

func hashBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func hashFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	h := sha256.New()
	n, err := io.Copy(h, file)
	return hex.EncodeToString(h.Sum(nil)), n, err
}
func (b SourceBuilder) writeJSON(path string, value any) {
	b.T.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		b.T.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		b.T.Fatal(err)
	}
}

package pluginmanifest_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
)

func TestPinnedCheckoutCompleteDeterministicInventory(t *testing.T) {
	source := checkout(t)
	bundle := load(t, source)
	if bundle.Commit() != source.Commit || bundle.Root() != source.Root || bundle.ManifestPath() != "plug-plugin.json" {
		t.Fatal("bundle lost the actual source pin")
	}
	files := bundle.Files()
	names := []string{}
	for _, file := range files {
		names = append(names, file.Path)
		data, err := os.ReadFile(filepath.Join(source.Root, file.Path))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		if file.Size != int64(len(data)) || file.SHA256 != hex.EncodeToString(digest[:]) || file.Executable {
			t.Fatalf("inventory does not describe actual bytes/mode: %+v", file)
		}
	}
	if !reflect.DeepEqual(names, []string{"BUILD.bazel", "native.go", "plug-plugin.json"}) {
		t.Fatalf("incomplete inventory: %v", names)
	}
	other := load(t, checkout(t))
	if bundle.Digest() != other.Digest() || bundle.ManifestDigest() != other.ManifestDigest() {
		t.Fatal("equivalent source checkouts produced different digests")
	}
	files[0].Path = "changed"
	manifest, err := bundle.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	manifest.Plugins[0].Provides[0].Reads[0] = "fields.private"
	if bundle.Files()[0].Path != "BUILD.bazel" {
		t.Fatal("inventory accessor leaked mutable storage")
	}
	if _, err := bundle.Natives(context.Background(), natives(t)); err != nil {
		t.Fatalf("manifest accessor changed effective grants: %v", err)
	}
	source.ExpectedDigest = bundle.Digest()
	load(t, source)
	source.ExpectedDigest = strings.Repeat("0", 64)
	_, err = pluginmanifest.Load(context.Background(), source)
	requireCode(t, err, "digest_mismatch")
}

func TestPinDirtySourceAndDigestDriftRefuse(t *testing.T) {
	for _, change := range []string{"modify", "add", "ignored", "remove", "executable"} {
		t.Run(change, func(t *testing.T) {
			source := checkout(t)
			if change == "ignored" {
				write(t, source, ".gitignore", []byte("ignored.txt\n"))
				source = commit(t, source)
			}
			bundle := load(t, source)
			switch change {
			case "modify":
				write(t, source, "native.go", []byte("changed source\n"))
			case "add":
				write(t, source, "new.txt", []byte("extra source"))
			case "ignored":
				write(t, source, "ignored.txt", []byte("ignored source is still inventory"))
			case "remove":
				if err := os.Remove(filepath.Join(source.Root, "native.go")); err != nil {
					t.Fatal(err)
				}
			case "executable":
				if err := os.Chmod(filepath.Join(source.Root, "native.go"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			requireCode(t, bundle.Verify(context.Background()), "source_changed")
			if change != "ignored" {
				newSource := commit(t, source)
				fresh := load(t, newSource)
				if fresh.Digest() == bundle.Digest() {
					t.Fatal("changed source inventory kept its digest")
				}
				requireCode(t, bundle.Verify(context.Background()), "pin_mismatch")
				newSource.ExpectedDigest = bundle.Digest()
				_, err := pluginmanifest.Load(context.Background(), newSource)
				requireCode(t, err, "digest_mismatch")
			}
		})
	}
}

func TestInvalidPinsAndCancellationRefuse(t *testing.T) {
	source := checkout(t)
	for _, pin := range []string{"", "main", source.Commit[:8], strings.ToUpper(source.Commit)} {
		candidate := source
		candidate.Commit = pin
		_, err := pluginmanifest.Load(context.Background(), candidate)
		requireCode(t, err, "pin_invalid")
	}
	candidate := source
	candidate.Commit = strings.Repeat("0", 40)
	_, err := pluginmanifest.Load(context.Background(), candidate)
	requireCode(t, err, "pin_mismatch")
	candidate = source
	candidate.ExpectedDigest = "short"
	_, err = pluginmanifest.Load(context.Background(), candidate)
	requireCode(t, err, "pin_invalid")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = pluginmanifest.Load(ctx, source)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
}

func TestStaticSymlinkNonregularAndTraversalRefuse(t *testing.T) {
	for _, name := range []string{"file symlink", "directory symlink", "manifest symlink", "root symlink", "fifo", "parent path", "absolute path"} {
		t.Run(name, func(t *testing.T) {
			source := checkout(t)
			outside := t.TempDir()
			switch name {
			case "file symlink":
				if err := os.Symlink("native.go", filepath.Join(source.Root, "link")); err != nil {
					t.Fatal(err)
				}
			case "directory symlink":
				if err := os.Symlink(outside, filepath.Join(source.Root, "linked")); err != nil {
					t.Fatal(err)
				}
			case "manifest symlink":
				if err := os.Rename(filepath.Join(source.Root, "plug-plugin.json"), filepath.Join(outside, "manifest")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(outside, "manifest"), filepath.Join(source.Root, "plug-plugin.json")); err != nil {
					t.Fatal(err)
				}
			case "root symlink":
				if err := os.Symlink(source.Root, filepath.Join(outside, "root")); err != nil {
					t.Fatal(err)
				}
				source.Root = filepath.Join(outside, "root")
			case "fifo":
				if err := syscall.Mkfifo(filepath.Join(source.Root, "pipe"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "parent path":
				source.ManifestFile = "../manifest.json"
			case "absolute path":
				source.ManifestFile = filepath.Join(source.Root, "plug-plugin.json")
			}
			_, err := pluginmanifest.Load(context.Background(), source)
			requireCode(t, err, "containment")
		})
	}
}

func TestCanonicalManifestDigestAndJSONSubsetYAML(t *testing.T) {
	source := checkout(t)
	bundle := load(t, source)
	manifest, err := bundle.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	write(t, source, "plug-plugin.json", data)
	source = commit(t, source)
	fresh := load(t, source)
	if fresh.ManifestDigest() != bundle.ManifestDigest() || fresh.Digest() == bundle.Digest() {
		t.Fatal("canonical manifest identity and raw source identity were conflated")
	}
	if err := os.Rename(filepath.Join(source.Root, "plug-plugin.json"), filepath.Join(source.Root, "plug-plugin.yaml")); err != nil {
		t.Fatal(err)
	}
	source = commit(t, source)
	if load(t, source).ManifestPath() != "plug-plugin.yaml" {
		t.Fatal("JSON representation at the canonical YAML path was not loaded")
	}
}

func TestLocalSHA256CheckoutAndLinkedWorktree(t *testing.T) {
	sha1Source := checkout(t)
	sha256Source := checkoutFormat(t, "sha256")
	if len(sha256Source.Commit) != 64 {
		t.Fatal("fixture did not create an actual SHA-256 repository")
	}
	first, second := load(t, sha1Source), load(t, sha256Source)
	if first.Digest() != second.Digest() {
		t.Fatal("source inventory depends on Git's object naming algorithm")
	}
	linked := filepath.Join(t.TempDir(), "checkout")
	git(t, sha1Source.Root, "worktree", "add", "--quiet", "--detach", linked, sha1Source.Commit)
	info, err := os.Lstat(filepath.Join(linked, ".git"))
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("fixture did not create an actual linked checkout: %v", err)
	}
	sha1Source.Root = linked
	// Ambient Git redirects/configuration must not change the inspected root.
	t.Setenv("GIT_DIR", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("GIT_WORK_TREE", t.TempDir())
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.bare")
	t.Setenv("GIT_CONFIG_VALUE_0", "true")
	if load(t, sha1Source).Digest() != first.Digest() {
		t.Fatal("linked checkout or ambient Git settings changed source identity")
	}
}

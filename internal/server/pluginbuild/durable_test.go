package pluginbuild_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/pluginapi"
)

func TestWASIDurableArtifactRepositoryAcrossProcessRestart(t *testing.T) {
	if os.Getenv("PLUG_DURABLE_ARTIFACT_CHILD") == "1" {
		durableArtifactChild(t)
		return
	}
	root := moduleRoot(t)
	ctx := context.Background()
	source := wasiCheckout(t, root)
	artifact, err := pluginbuild.Build(ctx, pluginbuild.Request{Source: source, GoBinary: goPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()
	storeRoot := filepath.Join(t.TempDir(), "artifacts")
	store, err := pluginbuild.OpenArtifactStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := store.Put(ctx, artifact)
	if err != nil {
		t.Fatal(err)
	}
	originalBytes, err := os.ReadFile(artifact.Path())
	if err != nil {
		t.Fatal(err)
	}
	if mode := mustFileMode(t, storeRoot); mode.Perm() != 0o700 {
		t.Fatalf("store root is not private: %o", mode.Perm())
	}

	// A content-addressed put is safe for concurrent callers and returns the
	// same detached lock after the first complete directory wins the rename.
	puts := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			_, putErr := store.Put(ctx, artifact)
			puts <- putErr
		}()
	}
	for i := 0; i < 8; i++ {
		if putErr := <-puts; putErr != nil {
			t.Fatalf("concurrent idempotent put failed: %v", putErr)
		}
	}

	// Leave a complete persistent object behind, then remove the source
	// checkout before the child process opens and executes it.
	lockJSON, err := json.Marshal(lock)
	if err != nil {
		t.Fatal(err)
	}
	if err := artifact.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(source.Root); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=TestWASIDurableArtifactRepository")
	child.Env = append(os.Environ(),
		"PLUG_DURABLE_ARTIFACT_CHILD=1",
		"PLUG_DURABLE_STORE="+storeRoot,
		"PLUG_DURABLE_LOCK="+string(lockJSON),
	)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("reopened child could not execute the exact artifact: %v\n%s", err, output)
	}

	object := filepath.Join(storeRoot, "objects", lock.ArtifactDigest)
	bytesPath := filepath.Join(object, "artifact")
	lockPath := filepath.Join(object, "lock.json")
	// Tampering and truncation are detected before a caller receives bytes.
	if err := os.WriteFile(bytesPath, []byte("truncated"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Restore(ctx, lock); err == nil {
		t.Fatal("tampered bytes were restored")
	}
	if err := os.WriteFile(bytesPath, originalBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if _, err := pluginbuild.OpenArtifactStore(storeRoot); err == nil {
		t.Fatal("missing lock entry was accepted on reopen")
	}
	if err := os.WriteFile(lockPath, lockJSON, 0o600); err != nil {
		t.Fatal(err)
	}

	// A partial final directory is never treated as a publishable object.
	partialDigest := strings.Repeat("b", 64)
	partial := filepath.Join(storeRoot, "objects", partialDigest)
	partialLock, err := pluginapi.CloneJSON(lock)
	if err != nil {
		t.Fatal(err)
	}
	partialLock.ArtifactDigest = partialDigest
	partialLock.Files[0].SHA256 = partialDigest
	partialJSON, err := json.Marshal(partialLock)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(partial, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partial, "lock.json"), partialJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := pluginbuild.OpenArtifactStore(storeRoot); err == nil {
		t.Fatal("partial final publication was accepted")
	}
	if err := os.RemoveAll(partial); err != nil {
		t.Fatal(err)
	}
	interrupted := filepath.Join(storeRoot, "objects", ".staging-interrupted")
	if err := os.Mkdir(interrupted, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := pluginbuild.OpenArtifactStore(storeRoot); err != nil {
		t.Fatalf("incomplete staging was allowed to block a reopen: %v", err)
	}

	// Valid digest-shaped symlinks cannot redirect a load outside the store.
	symlinkDigest := strings.Repeat("c", 64)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(storeRoot, "objects", symlinkDigest)); err != nil {
		t.Fatal(err)
	}
	if _, err := pluginbuild.OpenArtifactStore(storeRoot); err == nil {
		t.Fatal("digest-derived symlink was accepted")
	}
	if err := os.Remove(filepath.Join(storeRoot, "objects", symlinkDigest)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Restore(ctx, pluginbuild.ArtifactLock{ArtifactDigest: "../outside"}); err == nil {
		t.Fatal("traversal digest was accepted")
	}

	// The lock and manifest returned by Put/Restore are detached values.
	originalLock, err := pluginapi.CloneJSON(lock)
	if err != nil {
		t.Fatal(err)
	}
	mutated, err := pluginapi.CloneJSON(lock)
	if err != nil {
		t.Fatal(err)
	}
	mutated.Manifest.Plugins[0].Alias = "mutated"
	if _, err := store.Restore(ctx, mutated); err == nil {
		t.Fatal("mutated detached lock changed stored admission")
	}
	restored, err := store.Restore(ctx, originalLock)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := restored.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	manifest.Plugins[0].Alias = "mutated"
	if restored.ManifestDigest() != lock.ManifestDigest {
		t.Fatal("restored manifest accessor changed the artifact identity")
	}
	_ = restored.Close()

	// Restore happens first; a rebuilt source with different bytes only reports
	// a mismatch and cannot replace the digest that was admitted above.
	changed := wasiCheckout(t, root)
	pluginPath := filepath.Join(changed.Root, "plugin.go")
	pluginBytes, err := os.ReadFile(pluginPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pluginPath, bytes.Replace(pluginBytes, []byte("strings.ToUpper"), []byte("strings.ToLower"), 1), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, changed.Root, "add", "plugin.go")
	git(t, changed.Root, "commit", "-q", "-m", "changed durable fixture")
	changed.Commit = git(t, changed.Root, "rev-parse", "HEAD")
	restored, checks, err := store.RestoreAndCheck(ctx, lock, func(ctx context.Context) (*pluginbuild.Artifact, error) {
		return pluginbuild.Build(ctx, pluginbuild.Request{Source: changed, GoBinary: goPath(t)})
	})
	if err != nil {
		t.Fatal(err)
	}
	check := <-checks
	if check.Err != nil || check.Match || check.Actual.ArtifactDigest == lock.ArtifactDigest {
		t.Fatalf("mismatching rebuild was not reported: %+v", check)
	}
	if restored.ArtifactDigest() != lock.ArtifactDigest || restored.Verify(ctx) != nil {
		t.Fatal("mismatching rebuild replaced the restored artifact")
	}
	_ = restored.Close()
	t.Log("WASI fixture persisted atomically, reopened and executed in a source-free process; tamper, partial, traversal, symlink and rebuild mismatch checks refused safely")
}

func durableArtifactChild(t *testing.T) {
	t.Helper()
	var lock pluginbuild.ArtifactLock
	if err := json.Unmarshal([]byte(os.Getenv("PLUG_DURABLE_LOCK")), &lock); err != nil {
		t.Fatal(err)
	}
	store, err := pluginbuild.OpenArtifactStore(os.Getenv("PLUG_DURABLE_STORE"))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := store.Restore(context.Background(), lock)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.Source().Root != "" {
		t.Fatal("durable restore retained a source checkout")
	}
	host, err := restored.Host(context.Background(), pluginhost.Config{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := host.Invoke(context.Background(), "x.wasi.echo", json.RawMessage(`{"id":"restart"}`))
	if err != nil || string(result) != `{"id":"restart"}` {
		t.Fatalf("reopened artifact did not execute: %s (%v)", result, err)
	}
}

func mustFileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode()
}

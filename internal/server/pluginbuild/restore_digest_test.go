package pluginbuild_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
)

func TestRestoredWASIDigestRecheckedBeforeGuestInitialization(t *testing.T) {
	ctx := context.Background()
	source := wasiCheckout(t, moduleRoot(t))
	path := filepath.Join(source.Root, "plugin.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Observe execution inside the real source-built guest initializer, using
	// the same capability-free import barrier as the initializer tests.
	data = append(data, []byte("\nfunc init() { _ = snapshotCall(0, 0) }\n")...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, source.Root, "add", "plugin.go")
	git(t, source.Root, "commit", "-q", "--no-verify", "-m", "test: observe restored guest initialization")
	source.Commit = git(t, source.Root, "rev-parse", "HEAD")
	artifact, err := pluginbuild.Build(ctx, pluginbuild.Request{Source: source, GoBinary: goPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()
	repository, err := pluginbuild.OpenArtifactStore(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	lock, err := repository.Put(ctx, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if err := artifact.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(source.Root); err != nil {
		t.Fatal(err)
	}
	restored, err := repository.Restore(ctx, lock)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	original, err := os.ReadFile(restored.Path())
	if err != nil {
		t.Fatal(err)
	}
	// An empty custom section keeps the module valid and executable. If the
	// recorded digest were ignored, runtime validation would accept these bytes.
	tampered := append(append([]byte(nil), original...), 0, 1, 0)
	if err := pluginhost.ValidateWASMArtifact(tampered); err != nil {
		t.Fatalf("tamper must remain a valid WASM artifact: %v", err)
	}
	if err := os.WriteFile(restored.Path(), tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	var initialized atomic.Int32
	observed := pluginhost.WithWASMInitializerObserver(ctx, func() { initialized.Add(1) })
	host, err := restored.Host(observed, pluginhost.Config{})
	if host != nil {
		_ = host.Close()
		t.Fatal("digest drift returned an active plugin host")
	}
	requireBuildCode(t, err, pluginbuild.ErrDigest)
	if got := initialized.Load(); got != 0 {
		t.Fatalf("digest refusal initialized the guest %d times", got)
	}

	// The same restored object, configuration and observation must actually
	// initialize and execute when its original digest-locked bytes are present.
	if err := os.WriteFile(restored.Path(), original, 0o600); err != nil {
		t.Fatal(err)
	}
	host, err = restored.Host(observed, pluginhost.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if got := initialized.Load(); got == 0 {
		t.Fatal("untampered control did not execute the real guest initializer")
	}
	result, err := host.Invoke(ctx, "x.wasi.echo", json.RawMessage(`{"id":"restored-digest"}`))
	if err != nil || string(result) != `{"id":"restored-digest"}` {
		t.Fatalf("untampered restored guest result=%s err=%v", result, err)
	}
	t.Logf("restored digest drift refused before guest initialization; untampered source-free restore initialized=%d and executed echo", initialized.Load())
}

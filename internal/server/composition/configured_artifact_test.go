package composition

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/pluginmanifest"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// This test calls the production Open directly. No restore-then-Open test
// adapter, presence verifier, native factory or manufactured lock is involved.
func TestConfiguredRestoredWASIArtifact(t *testing.T) {
	ctx := context.Background()
	repository, lock := configuredWASIFixture(t)
	database := filepath.Join(t.TempDir(), "configured-artifact.db")
	base := Config{Driver: "sqlite", DSN: database, Project: "artifact", Address: DefaultAddress}
	seed := startRuntime(t, base)
	if _, err := seed.client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "baseline"}}, "550e8400-e29b-41d4-a716-446655440011"); err != nil {
		t.Fatal(err)
	}
	const checkpointKey = "plugin/wasi/default/durable"
	checkpoints := seed.r.Store.(driver.PersistentCheckpointStore)
	checkpoint, err := checkpoints.SaveCheckpoint(ctx, seed.r.Lease, checkpointKey, 0, []byte(`{"before":"startup"}`))
	if err != nil {
		t.Fatal(err)
	}
	baseline := captureActualArchiveCensus(t, seed.r.Store)
	seed.stop(t)

	// Reopen the real private repository after the build/source lifetimes end.
	repository, err = pluginbuild.OpenArtifactStore(repository.Root())
	if err != nil {
		t.Fatal(err)
	}
	artifactPath := filepath.Join(repository.Root(), "objects", lock.ArtifactDigest, "artifact")
	original, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	tampered := renamedWASICustomSection(t, original)
	if err := pluginhost.ValidateWASMArtifact(tampered); err != nil {
		t.Fatalf("digest tamper must remain a statically valid guest: %v", err)
	}
	newConfig := func() Config {
		config := base
		copy, err := pluginapi.CloneJSON(lock)
		if err != nil {
			t.Fatal(err)
		}
		config.RestoredWASI = &RestoredWASIConfig{
			Repository: repository, Lock: copy,
			Bindings: []pluginhost.Binding{{ID: "wasi-normalize", Alias: "wasi", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.title"}}},
		}
		return config
	}
	assertConserved := func(t *testing.T, checkLease bool) {
		t.Helper()
		store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: database})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if actual := captureActualArchiveCensus(t, store); !reflect.DeepEqual(actual, baseline) {
			t.Fatalf("startup refusal changed persisted graph: before=%+v after=%+v", baseline, actual)
		}
		actual, err := store.(driver.PersistentCheckpointStore).LoadCheckpoint(ctx, checkpointKey)
		if err != nil || !reflect.DeepEqual(actual, checkpoint) {
			t.Fatalf("startup changed checkpoint: before=%+v after=%+v err=%v", checkpoint, actual, err)
		}
		if checkLease {
			lease, err := store.Lease(ctx, "after-failed-startup", driver.LeaseTTL)
			if err != nil {
				t.Fatalf("failed startup retained the active lease: %v", err)
			}
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	tests := []struct {
		name  string
		code  string
		alter func(*Config)
	}{
		{"source-identity-lock", pluginbuild.ErrDigest, func(c *Config) {
			c.RestoredWASI.Lock.SourceDigest = otherConfiguredDigest(c.RestoredWASI.Lock.SourceDigest)
		}},
		{"pinned-commit-lock", pluginbuild.ErrDigest, func(c *Config) { c.RestoredWASI.Lock.Commit = otherConfiguredDigest(c.RestoredWASI.Lock.Commit) }},
		{"manifest-bytes", pluginbuild.ErrDigest, func(c *Config) { c.RestoredWASI.Lock.Manifest.Plugins[0].Version = "1.0.1" }},
		{"artifact-bytes", pluginbuild.ErrDigest, func(c *Config) {
			if err := os.WriteFile(artifactPath, tampered, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"mixed-native-bindings", pluginbuild.ErrConfig, func(c *Config) { c.NativeBindings = []pluginhost.Binding{{ID: "unsupported-mix"}} }},
		{"native-artifact", pluginbuild.ErrConfig, func(c *Config) { c.RestoredWASI.Lock.Runtime = pluginapi.RuntimeNative }},
		{"missing-repository", pluginbuild.ErrConfig, func(c *Config) { c.RestoredWASI.Repository = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := newConfig()
			test.alter(&config)
			t.Cleanup(func() {
				if err := os.WriteFile(artifactPath, original, 0o600); err != nil {
					t.Error(err)
				}
			})
			var initialized, opened atomic.Int32
			config.StoreFactory = func(ctx context.Context, _ string, cfg driver.Config) (driver.Store, error) {
				opened.Add(1)
				return (sqlite.Driver{}).Open(ctx, cfg)
			}
			observed := pluginhost.WithWASMInitializerObserver(ctx, func() { initialized.Add(1) })
			r, err := Open(observed, config)
			if r != nil {
				_ = r.Close(ctx)
				t.Fatal("refused configuration returned an active runtime")
			}
			var refusal *pluginbuild.Error
			if !errors.As(err, &refusal) || refusal.Code != test.code {
				t.Fatalf("refusal=%v, want typed %s", err, test.code)
			}
			if initialized.Load() != 0 || opened.Load() != 0 {
				t.Fatalf("refusal occurred after side effects: initialized=%d storage_opens=%d", initialized.Load(), opened.Load())
			}
			if server, err := r.Listen(ctx, DefaultAddress, nil); server != nil || err == nil {
				t.Fatal("refused runtime could start a listener")
			}
			assertConserved(t, false)
			t.Logf("typed %s before guest/storage/listener; exact persisted graph and checkpoint conserved", test.code)
		})
	}

	t.Run("rollback-after-initialization", func(t *testing.T) {
		config := newConfig()
		config.HTTP.MaxBodyBytes = -1 // Real late handler-configuration failure.
		var initialized, opened atomic.Int32
		config.StoreFactory = observedArtifactStore(&opened)
		observed := pluginhost.WithWASMInitializerObserver(ctx, func() { initialized.Add(1) })
		scratch := t.TempDir()
		t.Setenv("TMPDIR", scratch)
		r, err := Open(observed, config)
		if r != nil {
			_ = r.Close(ctx)
			t.Fatal("failed late composition retained a runtime")
		}
		if err == nil || !strings.Contains(err.Error(), "create HTTP handler") || initialized.Load() == 0 || opened.Load() != 1 {
			t.Fatalf("late rollback did not follow real guest/store initialization: initialized=%d storage_opens=%d err=%v", initialized.Load(), opened.Load(), err)
		}
		assertConfiguredScratchEmpty(t, scratch)
		assertConserved(t, true)
		t.Logf("late startup failure after %d guest initializations removed restored output and released lease; graph/checkpoint conserved", initialized.Load())
	})

	for i, name := range []string{"untampered", "source-free-restart"} {
		t.Run(name, func(t *testing.T) {
			config := newConfig()
			var initialized, opened atomic.Int32
			config.StoreFactory = observedArtifactStore(&opened)
			observed := pluginhost.WithWASMInitializerObserver(ctx, func() { initialized.Add(1) })
			scratch := t.TempDir()
			t.Setenv("TMPDIR", scratch)
			f := startConfiguredArtifact(t, observed, config)
			if initialized.Load() == 0 || opened.Load() != 1 || f.r.NativeHost == nil || f.r.restoredWASI == nil {
				t.Fatal("configured runtime bypassed real restored guest initialization")
			}
			artifact := f.r.restoredWASI.artifact
			path := artifact.Path()
			title := "configured source restore"
			result, err := f.client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}}, fmt.Sprintf("550e8400-e29b-41d4-a716-44665544002%d", i))
			if err != nil {
				t.Fatal(err)
			}
			ref := resultObject(t, result)["results"].([]any)[0].(map[string]any)["ref"]
			read, err := f.client.Call(ctx, "bead.get", map[string]any{"id": ref}, "")
			if err != nil {
				t.Fatal(err)
			}
			bead := resultObject(t, read)["bead"].(map[string]any)
			if got := bead["fields"].(map[string]any)["title"]; got != strings.ToUpper(title) {
				t.Fatalf("public HTTP operation bypassed source-built WASI mutator: title=%v", got)
			}
			actual, err := f.r.Store.(driver.PersistentCheckpointStore).LoadCheckpoint(ctx, checkpointKey)
			if err != nil || !reflect.DeepEqual(actual, checkpoint) {
				t.Fatalf("non-durable fixture changed checkpoint: %+v err=%v", actual, err)
			}
			f.stop(t)
			var closed *pluginbuild.Error
			if err := artifact.Verify(ctx); !errors.As(err, &closed) || closed.Code != pluginbuild.ErrClosed {
				t.Fatalf("Runtime.Close retained artifact admission: %v", err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("Runtime.Close retained disposable artifact: %v", err)
			}
			assertConfiguredScratchEmpty(t, scratch)
			t.Logf("source-free configured Restore->Host->Listen->HTTP mutated title; guest_initializations=%d; Close removed owned output", initialized.Load())
		})
	}
}

func observedArtifactStore(opened *atomic.Int32) StoreFactory {
	return func(ctx context.Context, _ string, config driver.Config) (driver.Store, error) {
		opened.Add(1)
		return (sqlite.Driver{}).Open(ctx, config)
	}
}

func startConfiguredArtifact(t *testing.T, ctx context.Context, config Config) *runtimeFixture {
	t.Helper()
	r, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	server, err := r.Listen(ctx, config.Address, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	transport, err := client.NewHTTP(client.HTTPConfig{Server: "http://" + server.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(transport, config.Project, "configured-artifact-test")
	if err != nil {
		transport.Close()
		t.Fatal(err)
	}
	f := &runtimeFixture{r: r, server: server, done: done, transport: transport, client: c}
	t.Cleanup(func() { f.stop(t) })
	return f
}

func configuredWASIFixture(t *testing.T) (*pluginbuild.ArtifactStore, pluginbuild.ArtifactLock) {
	t.Helper()
	root := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if root == "" || os.Getenv("PLUG_TEST_GO_BINARY") == "" {
		t.Fatal("source-built integration requires runner source root and Bazel Go SDK")
	}
	goBinary, err := filepath.EvalSymlinks(filepath.Join(os.Getenv("TEST_SRCDIR"), filepath.FromSlash(os.Getenv("PLUG_TEST_GO_BINARY"))))
	if err != nil {
		t.Fatalf("resolve Bazel Go SDK: %v", err)
	}
	goRoot := filepath.Dir(filepath.Dir(goBinary))
	t.Setenv("GOROOT", goRoot)
	t.Setenv("PATH", filepath.Join(goRoot, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	source := t.TempDir()
	for _, name := range []string{"plugin.go", "acceptance.go", "plug-plugin.json", "go.mod.template"} {
		data, err := os.ReadFile(filepath.Join(root, "internal/server/pluginbuild/fixture-wasi", name))
		if err != nil {
			t.Fatal(err)
		}
		// The default configured ontology has title but no normalized field.
		// Derive the public fixture before pinning: both manifest ownership and
		// actual guest patches now target title, with unchanged ABI/containment.
		data = bytes.ReplaceAll(data, []byte("fields.normalized"), []byte("fields.title"))
		if name == "go.mod.template" {
			name = "go.mod"
			data = bytes.ReplaceAll(data, []byte("__PLUG_ROOT__"), []byte(root))
		}
		if err := os.WriteFile(filepath.Join(source, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "init.go"), []byte("package main\nfunc init() { _ = snapshotCall(0, 0) }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = source
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Public Fixture", "GIT_AUTHOR_EMAIL=fixture@example.invalid", "GIT_COMMITTER_NAME=Public Fixture", "GIT_COMMITTER_EMAIL=fixture@example.invalid")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("fixture git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "--template=", "--initial-branch=main")
	git("add", "--all")
	git("commit", "-q", "--no-verify", "-m", "test: public configured WASI title normalizer")
	artifact, err := pluginbuild.Build(context.Background(), pluginbuild.Request{Source: pluginmanifest.Checkout{Root: source, Commit: git("rev-parse", "HEAD")}, GoBinary: goBinary})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Close()
	repository, err := pluginbuild.OpenArtifactStore(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	lock, err := repository.Put(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	t.Log("public fixture derived before pin/Build/Put: manifest+guest own title; capability-free initializer import; source removed before configured startup")
	return repository, lock
}

func renamedWASICustomSection(t *testing.T, original []byte) []byte {
	t.Helper()
	data := append([]byte(nil), original...)
	for offset := 8; offset < len(data); {
		id := data[offset]
		size, n := binary.Uvarint(data[offset+1:])
		if n <= 0 || size > uint64(len(data)-offset-1-n) {
			t.Fatal("invalid source-built section framing")
		}
		payload := offset + 1 + n
		if id == 0 {
			nameSize, nameN := binary.Uvarint(data[payload:])
			if nameN <= 0 || uint64(nameN)+nameSize > size {
				t.Fatal("invalid source-built custom section name")
			}
			if nameSize > 0 {
				index := payload + nameN
				if data[index] == 'x' {
					data[index] = 'y'
				} else {
					data[index] = 'x'
				}
				return data
			}
		}
		offset = payload + int(size)
	}
	t.Fatal("source-built WASI has no named custom section")
	return nil
}

func otherConfiguredDigest(digest string) string {
	if digest[0] == '0' {
		return "1" + digest[1:]
	}
	return "0" + digest[1:]
}

func assertConfiguredScratchEmpty(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("restored runtime retained disposable files: %v err=%v", entries, err)
	}
}

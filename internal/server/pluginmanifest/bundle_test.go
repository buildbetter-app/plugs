package pluginmanifest

import (
	"context"
	_ "embed"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/pluginapi"
	"github.com/buildbetter-app/plugs/sdk/go/conformance"
)

// Keep the fixture as source data, then materialize it into a disposable
// checkout. This exercises the same filesystem loader used by a local source
// checkout while keeping Bazel hermetic.
//
//go:embed testdata/fixture/plug-plugin.json
var fixtureManifest []byte

//go:embed testdata/fixture/source.txt
var fixtureSource []byte

func materializeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "plug-plugin.json"), fixtureManifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.txt"), fixtureSource, 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func fixtureRegistration(t *testing.T) NativeRegistration {
	t.Helper()
	native := conformance.Native(conformance.Options{})
	return NativeRegistration{ID: "app.example.fixture", Instance: "default", New: native.New}
}

func TestLoadSourceBundleAndRunActualNativeHost(t *testing.T) {
	root := materializeFixture(t)
	bundle, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ManifestPath != "plug-plugin.json" || len(bundle.Manifest.Plugins) != 1 || len(bundle.Files) != 2 {
		t.Fatalf("bundle identity: %+v", bundle)
	}
	if bundle.Digest == "" || bundle.CanonicalDigest() != bundle.Digest {
		t.Fatalf("missing canonical digest: %q", bundle.Digest)
	}
	if bundle.Files[0].Path != "plug-plugin.json" || bundle.Files[1].Path != "source.txt" {
		t.Fatalf("inventory is not complete and sorted: %+v", bundle.Files)
	}
	if err := bundle.Verify(); err != nil {
		t.Fatal(err)
	}

	natives, err := bundle.NativeDescriptors([]NativeRegistration{fixtureRegistration(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateNativeDescriptors(natives); err != nil {
		t.Fatal(err)
	}
	if err := ValidateNativeDescriptors(append(append(natives[:0:0], natives...), natives[0])); err == nil {
		t.Fatal("duplicate alias/instance descriptor accepted")
	}
	host, err := pluginhost.New(pluginhost.Config{Natives: natives})
	if err != nil {
		t.Fatal(err)
	}
	result, err := host.Invoke(context.Background(), "x.fixture.echo", json.RawMessage(`{"id":"pl-a","value":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != `{"id":"pl-a","value":9007199254740993}` {
		t.Fatalf("actual native registration returned %s", result)
	}
}

func TestSourceBundleDigestBindsCompleteInventory(t *testing.T) {
	root := materializeFixture(t)
	bundle, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.txt"), []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := bundle.Verify(); err == nil || !strings.Contains(err.Error(), "bundle_digest_mismatch") {
		t.Fatalf("changed source accepted: %v", err)
	}
	if _, err := LoadPinned(root, bundle.Digest); err == nil || !strings.Contains(err.Error(), "bundle_digest_mismatch") {
		t.Fatalf("pinned digest accepted changed source: %v", err)
	}
}

func TestSourceBundleRefusesContainmentViolations(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		root := materializeFixture(t)
		outside := filepath.Join(t.TempDir(), "outside.txt")
		if err := os.WriteFile(outside, []byte("outside"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "escape.txt")); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(root); err == nil || !strings.Contains(err.Error(), "bundle_containment") {
			t.Fatalf("symlink accepted: %v", err)
		}
	})
	t.Run("nonregular", func(t *testing.T) {
		root := materializeFixture(t)
		if err := os.Mkdir(filepath.Join(root, "directory-as-file"), 0o755); err != nil {
			t.Fatal(err)
		}
		// A directory is a valid container, but a FIFO is a non-regular source
		// member and must be rejected before any read could block.
		fifo := filepath.Join(root, "pipe")
		if err := makeFIFO(fifo); err != nil {
			t.Skipf("FIFO unavailable: %v", err)
		}
		if _, err := Load(root); err == nil || !strings.Contains(err.Error(), "bundle_containment") {
			t.Fatalf("non-regular member accepted: %v", err)
		}
	})
}

func TestSourceBundleRequiresExactRegistrationsAndInstances(t *testing.T) {
	root := materializeFixture(t)
	bundle, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	for name, registrations := range map[string][]NativeRegistration{
		"missing":          nil,
		"extra":            {fixtureRegistration(t), fixtureRegistration(t)},
		"unknown":          {{ID: "app.example.missing", Instance: "default", New: fixtureRegistration(t).New}},
		"invalid instance": {{ID: "app.example.fixture", Instance: "bad/instance", New: fixtureRegistration(t).New}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := bundle.NativeDescriptors(registrations); err == nil {
				t.Fatal("invalid registration set accepted")
			}
		})
	}
	duplicate := fixtureRegistration(t)
	if _, err := bundle.NativeDescriptors([]NativeRegistration{duplicate, duplicate}); err == nil {
		t.Fatal("duplicate registration accepted")
	}
}

func TestSourceBundleUsesPublicManifestValidationAndHostGrantNarrowing(t *testing.T) {
	root := materializeFixture(t)
	manifestPath := filepath.Join(root, "plug-plugin.json")
	original, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"unknown field":  func(doc map[string]any) { doc["unknown"] = true },
		"future version": func(doc map[string]any) { doc["manifest_version"] = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			var doc map[string]any
			if err := json.Unmarshal(original, &doc); err != nil {
				t.Fatal(err)
			}
			mutate(doc)
			data, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(root); err == nil {
				t.Fatal("invalid manifest accepted")
			}
			if err := os.WriteFile(manifestPath, original, 0o644); err != nil {
				t.Fatal(err)
			}
		})
	}
	bundle, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	natives, err := bundle.NativeDescriptors([]NativeRegistration{fixtureRegistration(t)})
	if err != nil {
		t.Fatal(err)
	}
	bindings := []pluginhost.Binding{{ID: "too-wide", Alias: "fixture", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.private"}, Owns: []string{"fields.normalized"}}}
	if _, err := pluginhost.New(pluginhost.Config{Natives: natives, Bindings: bindings}); err == nil {
		t.Fatal("binding widened the manifest read grant")
	}
}

func TestSourceBundleAcceptsPinnedDigestOnlyWhenCanonical(t *testing.T) {
	root := materializeFixture(t)
	bundle, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPinned(root, strings.ToUpper(bundle.Digest)); err == nil {
		t.Fatal("uppercase digest accepted")
	}
	if _, err := LoadPinned(root, bundle.Digest); err != nil {
		t.Fatal(err)
	}
}

func makeFIFO(path string) error { return syscall.Mkfifo(path, 0o644) }

package rehearsal

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A matching archive hash and image config do not make an incomplete image
// reloadable. Exercise the real image-save output, without starting a service.
func TestRollbackRefusesIncompleteSavedImageBeforeActivation(t *testing.T) {
	sha, tree := testSourcePins(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := ensureWorkRoot(); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(workRoot, "plug-swarm-rollback-fault-")
	if err != nil {
		t.Fatal(err)
	}
	d := newOwnedDocker(root)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := d.cleanup(cleanupCtx); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	seed, err := openFixture(ctx, "memory", "")
	if err != nil {
		t.Fatal(err)
	}
	_, saveErr := seed.rpc.Call(ctx, "memory.save", map[string]any{"key": "rollback-fault", "content": "public disposable checkpoint"}, "")
	snapshot, _, dumpErr := captureDump(ctx, seed.adminRPC)
	if err := errors.Join(saveErr, dumpErr, seed.close()); err != nil {
		t.Fatal(err)
	}
	old, err := buildRuntimeBundle(ctx, d, "old", sha, tree, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := buildRuntimeBundle(ctx, d, "candidate", sha, tree, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := os.ReadFile(old.ImagePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"missing-layer", "corrupt-layer"} {
		t.Run(mode, func(t *testing.T) {
			service := filepath.Join(root, mode)
			if err := installRuntimeFiles(service, candidate, "memory", nil); err != nil {
				t.Fatal(err)
			}
			before, err := os.Readlink(filepath.Join(service, "current"))
			if err != nil {
				t.Fatal(err)
			}
			contents := map[string][]byte{}
			for _, name := range []string{runtimeExecutableName, runtimePromptName, runtimePluginName, runtimeDataName, "launch.json"} {
				contents[name], err = os.ReadFile(filepath.Join(service, "current", name))
				if err != nil {
					t.Fatal(err)
				}
			}
			bad := old
			bad.ImagePath = filepath.Join(root, mode+".tar")
			broken := rollbackArchiveWithBrokenLayer(t, archive, mode)
			if err := os.WriteFile(bad.ImagePath, broken, 0600); err != nil {
				t.Fatal(err)
			}
			// Bind the actual broken archive bytes. Its config/image/source pins
			// remain correct; this is a required-member failure, not a bad hash.
			bad.ImageArchiveDigest = digestBytes(broken)
			if err := installRuntimeFiles(service, bad, "memory", nil); err == nil {
				t.Error("incomplete saved image accepted before activation")
			}
			after, err := os.Readlink(filepath.Join(service, "current"))
			if err != nil || before != after {
				t.Error("incomplete image changed active generation")
			}
			for name, want := range contents {
				got, err := os.ReadFile(filepath.Join(service, "current", name))
				if err != nil || !bytes.Equal(got, want) {
					t.Errorf("incomplete image replaced active %s bytes", name)
				}
			}
			staged, err := os.ReadDir(filepath.Join(service, "generations"))
			if err != nil || len(staged) != 1 {
				t.Error("incomplete image left a replacement generation")
			}
			if _, err := os.Lstat(filepath.Join(service, ".next")); !os.IsNotExist(err) {
				t.Error("incomplete image left an activation link")
			}
		})
	}
}

func rollbackArchiveWithBrokenLayer(t *testing.T, archive []byte, mode string) []byte {
	t.Helper()
	var manifests []struct{ Layers []string }
	reader := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := reader.Next()
		if err != nil {
			t.Fatalf("real saved image manifest unavailable: %v", err)
		}
		if header.Name == "manifest.json" {
			if err := json.NewDecoder(reader).Decode(&manifests); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if len(manifests) != 1 || len(manifests[0].Layers) != 1 {
		t.Fatalf("expected one real scratch-image layer: %+v", manifests)
	}
	layer := manifests[0].Layers[0]
	reader = tar.NewReader(bytes.NewReader(archive))
	var broken bytes.Buffer
	writer := tar.NewWriter(&broken)
	changed := false
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == layer {
			if changed || len(content) == 0 {
				t.Fatal("real saved image layer is empty or repeated")
			}
			changed = true
			t.Logf("R1_IMAGE_FAULT mode=%s required_layer=%s original_bytes=%d", mode, layer, len(content))
			if mode == "missing-layer" {
				continue
			}
			content[len(content)/2] ^= 1
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if !changed {
		t.Fatal("required real saved image layer was not found")
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return broken.Bytes()
}

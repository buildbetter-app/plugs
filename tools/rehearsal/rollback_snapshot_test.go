package rehearsal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Installation owns the snapshot used by the public restore. The original
// bundle source is no longer authoritative after the generation is activated.
func TestRuntimeRestoreConsumesPinnedInstalledSnapshot(t *testing.T) {
	sha, tree := testSourcePins(t)
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := ensureWorkRoot(); err != nil {
				t.Fatal(err)
			}
			root, err := os.MkdirTemp(workRoot, "plug-swarm-rollback-snapshot-")
			if err != nil {
				t.Fatal(err)
			}
			d := newOwnedDocker(root)
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if err := d.cleanup(cleanup); err != nil {
					t.Error(err)
				}
				if err := os.RemoveAll(root); err != nil {
					t.Error(err)
				}
			})
			seed, err := openFixture(ctx, kind, fixtureDSN(kind, root, 0, "snapshot-seed"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if seed != nil {
					if err := seed.close(); err != nil {
						t.Error(err)
					}
				}
			})
			for i := 0; i < 2; i++ {
				if _, err := seed.rpc.Call(ctx, "memory.save", map[string]any{"key": fmt.Sprintf("snapshot.%d", i), "content": fmt.Sprintf("installed value %d", i)}, ""); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := seed.rpc.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "public snapshot boundary", "priority": 1}}, ""); err != nil {
				t.Fatal(err)
			}
			wantState, err := publicState(ctx, seed.rpc)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, _, err := captureDump(ctx, seed.adminRPC)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := seed.rpc.Call(ctx, "memory.save", map[string]any{"key": "snapshot.0", "content": "unbound replacement"}, ""); err != nil {
				t.Fatal(err)
			}
			replacementState, err := publicState(ctx, seed.rpc)
			if err != nil {
				t.Fatal(err)
			}
			replacement, _, dumpErr := captureDump(ctx, seed.adminRPC)
			closeErr := seed.close()
			seed = nil
			if err := errors.Join(dumpErr, closeErr); err != nil {
				t.Fatal(err)
			}
			if digestBytes(snapshot) == digestBytes(replacement) || wantState == replacementState {
				t.Fatal("replacement must be a distinct valid native snapshot with different public data")
			}
			bundle, err := buildRuntimeBundle(ctx, d, "old", sha, tree, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := d.createNetwork(ctx); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"source-changed", "source-removed", "installed-changed", "installed-missing"} {
				t.Run(name, func(t *testing.T) {
					b := bundle
					b.DataPath = filepath.Join(root, name+"-source.dump")
					if err := writeRollbackArtifact(b.DataPath, snapshot); err != nil {
						t.Fatal(err)
					}
					service := filepath.Join(root, name)
					if err := installRuntimeFiles(service, b, kind, nil); err != nil {
						t.Fatal(err)
					}
					h, err := startOwnedRuntime(ctx, d, service, b)
					if h != nil {
						t.Cleanup(func() {
							cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
							defer cancel()
							if err := h.stop(cleanup); err != nil {
								t.Error(err)
							}
						})
					}
					if err != nil {
						t.Fatal(err)
					}
					beforeIncarnation, err := currentRuntimeIncarnation(ctx, h)
					if err != nil {
						t.Fatal(err)
					}
					beforeState, err := publicState(ctx, h.rpc)
					if err != nil {
						t.Fatal(err)
					}
					installed := filepath.Join(service, "current", runtimeDataName)
					// Every fault happens after the actual container has validated its
					// installed files and exchanged its authenticated TLS bootstrap.
					switch name {
					case "source-changed":
						err = writeRollbackArtifact(b.DataPath, replacement)
					case "source-removed":
						err = os.Remove(b.DataPath)
					case "installed-changed":
						err = writeRollbackArtifact(installed, replacement)
					case "installed-missing":
						err = os.Remove(installed)
					}
					if err != nil {
						t.Fatal(err)
					}
					observation, restoreErr := restoreAndReadRuntime(ctx, h, b, wantState, "")
					if strings.HasPrefix(name, "installed-") {
						if restoreErr == nil || observation.PublicDataReadback || observation.RestoredIncarnation != "" {
							t.Fatalf("invalid installed snapshot was not refused before restore: error=%v observation=%+v", restoreErr, observation)
						}
						afterIncarnation, err := currentRuntimeIncarnation(ctx, h)
						if err != nil || afterIncarnation != beforeIncarnation {
							t.Fatal("refused installed snapshot changed the public incarnation")
						}
						afterState, err := publicState(ctx, h.rpc)
						if err != nil || afterState != beforeState {
							t.Fatal("refused installed snapshot changed public data")
						}
						// Correct only the installed input. A subsequent valid restore
						// must work in this same empty runtime, proving no restore session
						// was admitted by the failed attempt.
						if err := writeRollbackArtifact(installed, snapshot); err != nil {
							t.Fatal(err)
						}
						observation, restoreErr = restoreAndReadRuntime(ctx, h, b, wantState, "")
					}
					if restoreErr != nil || !observation.PublicDataReadback || observation.DataDigest != digestBytes(snapshot) || observation.RestoredIncarnation == beforeIncarnation {
						t.Fatalf("exact installed snapshot did not restore: error=%v observation=%+v", restoreErr, observation)
					}
					if err := runtimeCanary(ctx, h, b, &observation); err != nil {
						t.Fatal(err)
					}
					t.Logf("R1_INSTALLED_SNAPSHOT driver=%s case=%s snapshot_sha256=%s public_readback=PASS canary=PASS", kind, name, observation.DataDigest)
				})
			}
		})
	}
}

package composition

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type digestObservedStore struct {
	driver.Store
	driver.PersistentCheckpointStore
	driver.ResponseFinalizer
	writes atomic.Int32
	saves  atomic.Int32
}

func (s *digestObservedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes.Add(1)
	// Preserve the real transaction's restore and allocator capabilities.
	return s.Store.Write(ctx, lease)
}

func (s *digestObservedStore) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, generation uint64, payload []byte) (driver.Checkpoint, error) {
	s.saves.Add(1)
	return s.PersistentCheckpointStore.SaveCheckpoint(ctx, lease, key, generation, payload)
}

func TestActualArchiveDigestRefusalPrecedesConverterPublication(t *testing.T) {
	data, manifest, manifestHash := readActualArchive(t)
	sourceDrift := changeActualArchiveMember(t, data, "config/sources.json", func(member []byte) []byte {
		// Change only JSON whitespace: the same valid source value and size
		// now have different bytes from the manifest's recorded SHA-256.
		for index, value := range member {
			if value == '\n' || value == '\t' || value == '\r' {
				member[index] = ' '
				if !json.Valid(member) {
					t.Fatal("source tamper invalidated JSON")
				}
				return member
			}
		}
		t.Fatal("source fixture has no JSON whitespace to change")
		return nil
	})
	manifestDrift := changeActualArchiveMember(t, data, "manifest.json", func(member []byte) []byte {
		var value map[string]any
		decoder := json.NewDecoder(bytes.NewReader(member))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		value["machine"] = "public-digest-regression"
		changed, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return append(changed, '\n')
	})
	if _, err := dumpformat.Read(bytes.NewReader(manifestDrift)); err != nil {
		t.Fatalf("substituted manifest archive must remain independently valid: %v", err)
	}
	for _, test := range []struct {
		name, finishHash string
		archive          []byte
		valid            bool
	}{
		{"source-member-digest", manifestHash, sourceDrift, false},
		{"finish-manifest-digest", strings.Repeat("0", 64), data, false},
		{"archive-manifest-substitution", manifestHash, manifestDrift, false},
		{"untampered", manifestHash, data, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			var observed *digestObservedStore
			var planned atomic.Int32
			_, verifier := loggedTestPresenceVerifier(t)
			f := startRuntime(t, Config{
				Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "restore.db"),
				Project: "pl", Address: DefaultAddress, RestorePresence: verifier,
				RestoreImportPlanner: func(ctx context.Context, input io.Reader) (converterroundtrip.ImportInspection, error) {
					planned.Add(1)
					return converterroundtrip.InspectImport(ctx, input)
				},
				StoreFactory: func(ctx context.Context, _ string, config driver.Config) (driver.Store, error) {
					store, err := (sqlite.Driver{}).Open(ctx, config)
					if err != nil {
						return nil, err
					}
					observed = &digestObservedStore{Store: store,
						PersistentCheckpointStore: store.(driver.PersistentCheckpointStore),
						ResponseFinalizer:         store.(driver.ResponseFinalizer)}
					return observed, nil
				},
			})
			checkpoint, err := observed.SaveCheckpoint(ctx, f.r.Lease, "restore-digest/baseline", 0, []byte(`{"before":"restore"}`))
			if err != nil {
				t.Fatal(err)
			}
			before := captureActualArchiveCensus(t, observed)
			if !before.Meta.Head.IsZero() || len(before.Rows) != 0 {
				t.Fatalf("restore target was not empty: %+v", before)
			}
			begin, err := f.client.Call(ctx, "admin.restore.begin", map[string]any{
				"manifest": manifest, "history": "head", "humanPresence": actualArchivePresence,
			}, strings.Repeat("1", 32))
			if err != nil {
				t.Fatal(err)
			}
			restoreID := compositionRestoreStatus(t, begin)["restoreId"].(string)
			// Bind transport integrity to the actual supplied bytes, including
			// tampered ones, so Finish must check the archive's inner digests.
			pageHash := sha256.Sum256(test.archive)
			if _, err := f.client.Call(ctx, "admin.restore.chunk", map[string]any{
				"restoreId": restoreID, "page": 0, "data": base64.StdEncoding.EncodeToString(test.archive),
				"sha256": hex.EncodeToString(pageHash[:]), "humanPresence": actualArchivePresence,
			}, strings.Repeat("2", 32)); err != nil {
				t.Fatalf("transport must accept the checksummed page: %v", err)
			}
			writes, saves := observed.writes.Load(), observed.saves.Load()
			finished, err := f.client.Call(ctx, "admin.restore.finish", map[string]any{
				"restoreId": restoreID, "manifestSha256": test.finishHash, "humanPresence": actualArchivePresence,
			}, strings.Repeat("3", 32))
			after := captureActualArchiveCensus(t, observed)
			if test.valid {
				if err != nil || compositionRestoreStatus(t, finished)["state"] != "complete" {
					t.Fatalf("untampered restore failed: result=%s err=%v", finished.JSON, err)
				}
				if planned.Load() != 1 || observed.writes.Load() <= writes || after.Meta.Head.String() != "1" || len(after.Rows) != 136 {
					t.Fatalf("positive control bypassed converter publication: plans=%d writes=%d census=%+v", planned.Load(), observed.writes.Load()-writes, after)
				}
			} else {
				var remote *wire.Error
				if !errors.As(err, &remote) || remote.Data.Code != "restore_checksum_failed" {
					t.Fatalf("digest refusal=%v want restore_checksum_failed", err)
				}
				if planned.Load() != 0 || observed.writes.Load() != writes || observed.saves.Load() != saves {
					t.Fatalf("digest refusal reached planner or storage publication: plans=%d writes=%d saves=%d", planned.Load(), observed.writes.Load()-writes, observed.saves.Load()-saves)
				}
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("digest refusal changed graph state: before=%+v after=%+v", before, after)
				}
			}
			// Checkpoints are intentionally excluded from archive imports. This
			// assertion checks preservation, not plugin checkpoint execution.
			persisted, err := observed.LoadCheckpoint(ctx, "restore-digest/baseline")
			if err != nil || !reflect.DeepEqual(persisted, checkpoint) {
				t.Fatalf("restore changed the existing durable checkpoint: checkpoint=%+v err=%v", persisted, err)
			}
			t.Logf("configured archive valid=%t plans=%d writes=%d checkpoint_saves=%d rows=%d head=%s", test.valid, planned.Load(), observed.writes.Load()-writes, observed.saves.Load()-saves, len(after.Rows), after.Meta.Head)
		})
	}
}

func changeActualArchiveMember(t *testing.T, data []byte, name string, change func([]byte) []byte) []byte {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(data))
	var out bytes.Buffer
	writer := tar.NewWriter(&out)
	changed := false
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		member, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == name {
			member = change(member)
			changed = true
		}
		header.Size = int64(len(member))
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(member); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if !changed || bytes.Equal(data, out.Bytes()) {
		t.Fatalf("archive member %s was not changed", name)
	}
	return out.Bytes()
}

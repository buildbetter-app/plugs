package restore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

var presence = map[string]any{"mode": "presence_check", "challenge": "restore-test", "proof": "restore-test"}

func trustedPresenceVerifier() restore.PresenceVerifier {
	return restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, got restore.Presence) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if method == "" || digest == "" || got["proof"] != "restore-test" {
			return fmt.Errorf("test presence is not bound to the admitted operation")
		}
		return nil
	})
}

func TestDumpRestoreClaimAndApplyAcrossDrivers(t *testing.T) {
	for _, driverName := range []string{"memory", "sqlite"} {
		driverName := driverName
		t.Run(driverName, func(t *testing.T) {
			ctx := context.Background()
			source, err := cli.OpenLocal(ctx, cli.LocalConfig{Project: "fixture", Agent: "source", Storage: cli.StorageConfig{Driver: "memory"}, PresenceVerifier: trustedPresenceVerifier()})
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			created, err := source.Client.Call(ctx, "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": "restored fixture", "description": "secret reference stays out of the bead"},
			}, "")
			if err != nil {
				t.Fatal(err)
			}
			var commit model.Commit
			if err := json.Unmarshal(created.JSON, &commit); err != nil || len(commit.Results) != 1 {
				t.Fatalf("create response: %s %v", created.JSON, err)
			}
			id := commit.Results[0].Ref.ID
			archiveBytes, archive := captureDump(t, ctx, source.Client)
			// The offline merger accepts exporter archives, while the live
			// restore writer accepts native head images. Neither merge API may
			// reinterpret a native dump; refusal must leave it restorable.
			if data, err := dumpformat.Merge(bytes.NewReader(archiveBytes)); err == nil || data != nil {
				t.Fatalf("offline merge accepted a native dump: %d bytes, %v", len(data), err)
			}
			if data, err := dumpformat.MergeArchives(archive); err == nil || data != nil {
				t.Fatalf("offline merge accepted a native archive: %d bytes, %v", len(data), err)
			}

			var targetStore driver.Store
			var cleanup func() error
			targetPath := ""
			if driverName == "memory" {
				targetStore = memory.New()
				cleanup = targetStore.Close
			} else {
				targetPath = filepath.Join(t.TempDir(), "restore.db")
				var err error
				targetStore, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: targetPath})
				if err != nil {
					t.Fatal(err)
				}
				cleanup = targetStore.Close
			}
			target, err := cli.OpenLocal(ctx, cli.LocalConfig{
				Project: "fixture", Agent: "restorer", Storage: cli.StorageConfig{Driver: "provided"},
				PresenceVerifier: trustedPresenceVerifier(),
				Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
					return targetStore, cleanup, nil
				},
			})
			if err != nil {
				_ = cleanup()
				t.Fatal(err)
			}

			restoreArchive(t, ctx, target.Client, archiveBytes, archive)
			if _, err := target.Client.Call(ctx, "admin.restore.begin", map[string]any{"manifest": map[string]any{"format": dumpformat.Format, "schema": 1, "scope": "plug-native-store"}, "humanPresence": presence}, "cccccccccccccccccccccccccccccccc"); err == nil {
				t.Fatal("non-empty restore target was accepted")
			}
			ready, err := target.Client.Call(ctx, "bead.ready", map[string]any{"limit": 10}, "")
			if err != nil || !bytes.Contains(ready.JSON, []byte(id)) {
				t.Fatalf("restored bead was not ready: %s %v", ready.JSON, err)
			}
			if _, err := target.Client.Call(ctx, "bead.claim", map[string]any{"id": map[string]any{"id": id}}, ""); err != nil {
				t.Fatalf("restored bead could not be claimed: %v", err)
			}
			cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(id, model.Natural64(0)), Fields: map[string]any{"title": "updated after restore"}}}}}
			if _, err := target.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": cs}, ""); err != nil {
				t.Fatalf("restored bead could not pass tx.apply: %v", err)
			}
			read, err := target.Client.Call(ctx, "bead.get", map[string]any{"id": map[string]any{"id": id}}, "")
			if err != nil || !bytes.Contains(read.JSON, []byte("updated after restore")) {
				t.Fatalf("post-restore readback: %s %v", read.JSON, err)
			}
			meta, err := targetStore.Meta(ctx)
			if err != nil || meta.Head.String() != "3" {
				t.Fatalf("unexpected restored head: %+v %v", meta, err)
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}
			if driverName == "sqlite" {
				reopened, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: targetPath})
				if err != nil {
					t.Fatal(err)
				}
				meta, err := reopened.Meta(ctx)
				if err != nil {
					_ = reopened.Close()
					t.Fatal(err)
				}
				reader, err := reopened.Read(ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					_ = reopened.Close()
					t.Fatal(err)
				}
				page, err := reader.Query(ctx, driver.Query{Limit: 10})
				_ = reader.Close()
				_ = reopened.Close()
				if err != nil || len(page.Items) != 1 {
					t.Fatalf("SQLite readback after restart: %d records, %v", len(page.Items), err)
				}
			}
		})
	}
}

func TestRestoreTerminalStatusStillRequiresInitiatingPrincipal(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "restore-principal", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	mgr := restore.NewManager(store, lease, trustedPresenceVerifier())
	manifest := map[string]any{"format": dumpformat.Format, "schema": 1, "scope": dumpformat.Scope}
	canonical, err := json.Marshal(dumpformat.Manifest{Format: dumpformat.Format, Schema: 1, Scope: dumpformat.Scope})
	if err != nil {
		t.Fatal(err)
	}
	status, err := mgr.BeginAs(ctx, model.PrincipalRef{Kind: "user", ID: "restore-owner"}, manifest, "head", restore.Presence(presence))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Finish(ctx, status.RestoreID, sha(canonical), model.PrincipalRef{Kind: "user", ID: "restore-owner"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "restore-principal"}, restore.Presence(presence)); err == nil {
		t.Fatal("invalid empty archive unexpectedly committed")
	}
	_, err = mgr.Finish(ctx, status.RestoreID, sha(canonical), model.PrincipalRef{Kind: "user", ID: "other-user"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "restore-principal-other"}, restore.Presence(presence))
	var forbidden *model.Error
	if err == nil || !errors.As(err, &forbidden) || forbidden.Data.Code != "forbidden" || forbidden.Data.Hint != "restore session caller changed" {
		t.Fatalf("terminal session disclosed to a different principal: %v", err)
	}
}

func TestRestoreImportConflictClassificationAcrossStores(t *testing.T) {
	ctx := context.Background()
	var archiveBytes []byte
	var err error
	paths := []string{"../../integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar", "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar"}
	if testSrcDir := os.Getenv("TEST_SRCDIR"); testSrcDir != "" {
		workspace := os.Getenv("TEST_WORKSPACE")
		if workspace == "" {
			workspace = "_main"
		}
		paths = append([]string{filepath.Join(testSrcDir, workspace, "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")}, paths...)
	}
	for _, path := range paths {
		archiveBytes, err = os.ReadFile(path)
		if err == nil {
			break
		}
	}
	if len(archiveBytes) == 0 {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(archiveBytes))
	if err != nil {
		t.Fatal(err)
	}
	manifestValue, err := wire.DecodeJSON(mustMember(t, archive, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	manifestHash := sha(canonical)

	for _, driverName := range []string{"memory", "sqlite"} {
		for _, testCase := range []struct {
			name  string
			err   error
			code  model.ErrorCode
			hint  string
			retry bool
		}{
			{name: "binding", err: driver.ErrRestoreBindingMismatch{Field: "requestHash"}, code: "restore_violation", hint: "driver transaction conflict: restore import binding field requestHash does not match the durable receipt"},
			{name: "commit", err: driver.ErrRestoreCommitConflict{Cause: driver.ErrConflict}, code: "busy", hint: "restore import commit conflicted with concurrent store state: driver transaction conflict", retry: true},
			{name: "lease", err: driver.ErrFenced{ExpectedEpoch: 1, ObservedEpoch: 2}, code: "busy", hint: "active storage lease was lost", retry: true},
		} {
			t.Run(driverName+"/"+testCase.name, func(t *testing.T) {
				var target driver.Store
				var closeTarget func() error
				if driverName == "memory" {
					target = memory.New()
					closeTarget = target.Close
				} else {
					path := filepath.Join(t.TempDir(), "restore.db")
					target, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
					if err != nil {
						t.Fatal(err)
					}
					closeTarget = target.Close
				}
				defer closeTarget()
				lease, err := target.Lease(ctx, "restore-conflict-test", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release(ctx)
				manager := restore.NewManager(target, lease, trustedPresenceVerifier())
				manager.SetImportHook(func(context.Context, string, model.PrincipalRef, []byte, string, driver.RestoreHistoryMode, string, string, model.WriteMeta, restore.Presence) (restore.RestoreStatus, error) {
					return restore.RestoreStatus{}, testCase.err
				})
				status, err := manager.BeginAs(ctx, model.PrincipalRef{Kind: "user", ID: "restore-owner"}, manifestValue.(map[string]any), "head", restore.Presence(presence))
				if err != nil {
					t.Fatal(err)
				}
				for pageNo, offset := uint64(0), 0; offset < len(archiveBytes); pageNo++ {
					end := offset + 512
					if end > len(archiveBytes) {
						end = len(archiveBytes)
					}
					chunk := archiveBytes[offset:end]
					if _, err := manager.ChunkAs(ctx, model.PrincipalRef{Kind: "user", ID: "restore-owner"}, status.RestoreID, pageNo, chunk, sha(chunk), restore.Presence(presence)); err != nil {
						t.Fatal(err)
					}
					offset = end
				}
				_, err = manager.Finish(ctx, status.RestoreID, manifestHash, model.PrincipalRef{Kind: "user", ID: "restore-owner"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "restore-conflict-test"}, restore.Presence(presence))
				var typed *model.Error
				if err == nil || !errors.As(err, &typed) || typed.Data.Code != testCase.code || typed.Data.Hint != testCase.hint || typed.Data.Retryable != testCase.retry {
					t.Fatalf("classification=%v want code=%s retry=%t hint=%q", err, testCase.code, testCase.retry, testCase.hint)
				}
			})
		}
	}
}

func TestDumpCursorBindsHistoryAndPageSize(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "dump-cursor", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	mgr := restore.NewManager(store, lease, trustedPresenceVerifier())
	first, err := mgr.DumpPage(ctx, "full", "", 512, restore.Presence(presence))
	if err != nil || first.Done || first.NextPage == nil {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	cursor := fmt.Sprintf("%s:%d", first.DumpID, *first.NextPage)
	if _, err := mgr.DumpPage(ctx, "full", cursor, 1024, restore.Presence(presence)); err == nil {
		t.Fatal("cursor accepted a different page size")
	}
	if _, err := mgr.DumpPage(ctx, "head", cursor, 512, restore.Presence(presence)); err == nil {
		t.Fatal("cursor accepted a different history mode")
	}
	if _, err := mgr.DumpPage(ctx, "full", cursor, 512, restore.Presence(presence)); err != nil {
		t.Fatalf("bound cursor refused: %v", err)
	}
}

func TestRestoreRefusalsDoNotMutateTarget(t *testing.T) {
	ctx := context.Background()
	source, err := cli.OpenLocal(ctx, cli.LocalConfig{Project: "fixture", Agent: "source", Storage: cli.StorageConfig{Driver: "memory"}, PresenceVerifier: trustedPresenceVerifier()})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	archiveBytes, archive := captureDump(t, ctx, source.Client)
	manifestValue, err := wire.DecodeJSON(mustMember(t, archive, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := manifestValue.(map[string]any)
	target, err := cli.OpenLocal(ctx, cli.LocalConfig{Project: "fixture", Agent: "target", Storage: cli.StorageConfig{Driver: "memory"}, PresenceVerifier: trustedPresenceVerifier()})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	key := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := target.Client.Call(ctx, "admin.restore.begin", map[string]any{"manifest": manifest, "history": "head", "humanPresence": presence}, key); err != nil {
		t.Fatal(err)
	}
	// A malformed page is rejected before it can enter the session.
	if _, err := target.Client.Call(ctx, "admin.restore.chunk", map[string]any{"restoreId": "missing", "page": 0, "data": "YQ==", "sha256": "00", "humanPresence": presence}, key+"01"); err == nil {
		t.Fatal("malformed/missing restore page was accepted")
	}
	// Unknown versions are rejected at begin, before any target write.
	manifest["format"] = "plug-dump/99"
	if _, err := target.Client.Call(ctx, "admin.restore.begin", map[string]any{"manifest": manifest, "humanPresence": presence}, key+"02"); err == nil {
		t.Fatal("unknown dump version was accepted")
	}
	// The original session remains receiving and a cancellation never writes.
	manifestValue, _ = wire.DecodeJSON(mustMember(t, archive, "manifest.json"))
	manifest = manifestValue.(map[string]any)
	begin, err := target.Client.Call(ctx, "admin.restore.begin", map[string]any{"manifest": manifest, "history": "head", "humanPresence": presence}, key+"03")
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	if err := json.Unmarshal(begin.JSON, &status); err != nil {
		t.Fatal(err)
	}
	restoreID := status["restoreId"].(string)
	chunk := archiveBytes[:512]
	sum := sha256.Sum256(chunk)
	if _, err := target.Client.Call(ctx, "admin.restore.chunk", map[string]any{"restoreId": restoreID, "page": 0, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": hex.EncodeToString(sum[:]), "humanPresence": presence}, key+"04"); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Client.Call(ctx, "admin.restore.chunk", map[string]any{"restoreId": restoreID, "page": 0, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": hex.EncodeToString(sum[:]), "humanPresence": presence}, key+"05"); err == nil {
		t.Fatal("duplicate restore page was accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := target.Client.Call(canceled, "admin.restore.finish", map[string]any{"restoreId": restoreID, "manifestSha256": "00", "humanPresence": presence}, key+"06"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled restore was not refused: %v", err)
	}
	before, err := targetStoreHead(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if before != "0" {
		t.Fatalf("refusal changed empty target head to %s", before)
	}
}

func TestNativeFullHistoryRefusesBeforeMutation(t *testing.T) {
	ctx := context.Background()
	sourceStore := memory.New()
	source, err := cli.OpenLocal(ctx, cli.LocalConfig{
		Project: "history", Agent: "source", Storage: cli.StorageConfig{Driver: "provided"},
		PresenceVerifier: trustedPresenceVerifier(),
		Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
			return sourceStore, sourceStore.Close, nil
		},
	})
	if err != nil {
		sourceStore.Close()
		t.Fatal(err)
	}
	defer source.Close()
	created, err := source.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "before"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var commit model.Commit
	if err := json.Unmarshal(created.JSON, &commit); err != nil || len(commit.Results) != 1 {
		t.Fatalf("create response: %s %v", created.JSON, err)
	}
	if _, err := source.Client.Call(ctx, "bead.update", map[string]any{"id": map[string]any{"id": commit.Results[0].Ref.ID}, "fields": map[string]any{"title": "after"}}, ""); err != nil {
		t.Fatal(err)
	}
	data, _, err := restore.Dump(ctx, sourceStore, "full")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := manifestForRestore(t, archive)
	if archive.Manifest().History != "full" || archive.Manifest().VersionCount < 2 || archive.Manifest().ChangesetCount < 2 {
		t.Fatalf("fixture did not contain full history: %+v", archive.Manifest())
	}

	targetStore := memory.New()
	defer targetStore.Close()
	lease, err := targetStore.Lease(ctx, "full-history-target", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	mgr := restore.NewManager(targetStore, lease, trustedPresenceVerifier())
	_, err = mgr.Begin(ctx, manifest, "full", restore.Presence(presence))
	var typed *model.Error
	if err == nil || !errors.As(err, &typed) || typed.Data.Code != "restore_violation" || !strings.Contains(typed.Data.Hint, "full-history") {
		t.Fatalf("full-history archive was not explicitly refused: %T %v", err, err)
	}
	assertEmptyRestoreTarget(t, ctx, targetStore)
}

func TestRestoreManifestBindsUploadedArchive(t *testing.T) {
	ctx := context.Background()
	sourceA := memory.New()
	defer sourceA.Close()
	dataA, _, err := restore.Dump(ctx, sourceA, "head")
	if err != nil {
		t.Fatal(err)
	}
	archiveA, err := dumpformat.Read(bytes.NewReader(dataA))
	if err != nil {
		t.Fatal(err)
	}
	manifestA, hashA := manifestForRestore(t, archiveA)

	sourceBStore := memory.New()
	sourceB, err := cli.OpenLocal(ctx, cli.LocalConfig{
		Project: "manifest", Agent: "source", Storage: cli.StorageConfig{Driver: "provided"},
		PresenceVerifier: trustedPresenceVerifier(),
		Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
			return sourceBStore, sourceBStore.Close, nil
		},
	})
	if err != nil {
		sourceBStore.Close()
		t.Fatal(err)
	}
	defer sourceB.Close()
	if _, err := sourceB.Client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "archive B"}}, ""); err != nil {
		t.Fatal(err)
	}
	dataB, _, err := restore.Dump(ctx, sourceBStore, "head")
	if err != nil {
		t.Fatal(err)
	}
	archiveB, err := dumpformat.Read(bytes.NewReader(dataB))
	if err != nil {
		t.Fatal(err)
	}
	_, hashB := manifestForRestore(t, archiveB)
	if hashA == hashB {
		t.Fatal("test archives unexpectedly share a manifest digest")
	}

	targetStore := memory.New()
	defer targetStore.Close()
	lease, err := targetStore.Lease(ctx, "manifest-target", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	mgr := restore.NewManager(targetStore, lease, trustedPresenceVerifier())
	status, err := mgr.Begin(ctx, manifestA, "head", restore.Presence(presence))
	if err != nil {
		t.Fatal(err)
	}
	uploadManagerArchive(t, ctx, mgr, status.RestoreID, dataB, restore.Presence(presence))
	_, err = mgr.Finish(ctx, status.RestoreID, hashA, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "manifest-target"}, restore.Presence(presence))
	var typed *model.Error
	if err == nil || !errors.As(err, &typed) || typed.Data.Code != "restore_checksum_failed" {
		t.Fatalf("mismatched uploaded archive was accepted: %T %v", err, err)
	}
	assertEmptyRestoreTarget(t, ctx, targetStore)
}

func captureDump(t *testing.T, ctx context.Context, client *client.Client) ([]byte, *dumpformat.Archive) {
	t.Helper()
	var data bytes.Buffer
	cursor := ""
	var dumpID string
	for {
		params := map[string]any{"history": "head", "pageSize": 512, "humanPresence": presence}
		if cursor != "" {
			params["cursor"] = cursor
		}
		page, err := client.Call(ctx, "admin.dump", params, "")
		if err != nil {
			t.Fatal(err)
		}
		value, err := wire.DecodeJSON(page.JSON)
		if err != nil {
			t.Fatal(err)
		}
		object := value.(map[string]any)
		gotID := object["dumpId"].(string)
		if dumpID != "" && dumpID != gotID {
			t.Fatalf("dump ID changed: %s -> %s", dumpID, gotID)
		}
		dumpID = gotID
		chunk, err := base64.StdEncoding.DecodeString(object["data"].(string))
		if err != nil {
			t.Fatal(err)
		}
		data.Write(chunk)
		if object["done"].(bool) {
			break
		}
		cursor = fmt.Sprintf("%s:%s", dumpID, object["nextPage"].(json.Number).String())
	}
	archive, err := dumpformat.Read(bytes.NewReader(data.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	return data.Bytes(), archive
}

func restoreArchive(t *testing.T, ctx context.Context, client *client.Client, data []byte, archive *dumpformat.Archive) {
	t.Helper()
	manifestValue, err := wire.DecodeJSON(mustMember(t, archive, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	key := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	begin, err := client.Call(ctx, "admin.restore.begin", map[string]any{"manifest": manifestValue, "history": "head", "humanPresence": presence}, key)
	if err != nil {
		t.Fatal(err)
	}
	status, err := wire.DecodeJSON(begin.JSON)
	if err != nil {
		t.Fatal(err)
	}
	restoreID := status.(map[string]any)["restoreId"].(string)
	for page, offset := uint64(0), 0; offset < len(data); page++ {
		end := offset + 512
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		sum := sha256.Sum256(chunk)
		if _, err := client.Call(ctx, "admin.restore.chunk", map[string]any{"restoreId": restoreID, "page": page, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": hex.EncodeToString(sum[:]), "humanPresence": presence}, fmt.Sprintf("%s%02d", key, page)); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	if _, err := client.Call(ctx, "admin.restore.finish", map[string]any{"restoreId": restoreID, "manifestSha256": hex.EncodeToString(sum[:]), "humanPresence": presence}, key+"ff"); err != nil {
		t.Fatal(err)
	}
}

func manifestForRestore(t *testing.T, archive *dumpformat.Archive) (map[string]any, string) {
	t.Helper()
	value, err := wire.DecodeJSON(mustMember(t, archive, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("manifest is not an object: %T", value)
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	return manifest, hex.EncodeToString(sum[:])
}

func uploadManagerArchive(t *testing.T, ctx context.Context, mgr *restore.Manager, restoreID string, data []byte, presence restore.Presence) {
	t.Helper()
	for page, offset := uint64(0), 0; offset < len(data); page++ {
		end := offset + 512
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		if _, err := mgr.Chunk(ctx, restoreID, page, chunk, sha(chunk), presence); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
}

func assertEmptyRestoreTarget(t *testing.T, ctx context.Context, store driver.Store) {
	t.Helper()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Head.IsZero() || !meta.HistoryFloor.IsZero() {
		t.Fatalf("restore refusal changed target metadata: %+v", meta)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Query(ctx, driver.Query{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("restore refusal wrote %d records", len(page.Items))
	}
}

func targetStoreHead(ctx context.Context, local *cli.Local) (string, error) {
	// The public client is sufficient for mutation checks; an empty ready page
	// is the stable readback for a target that has not committed anything.
	page, err := local.Client.Call(ctx, "bead.ready", map[string]any{"limit": 1}, "")
	if err != nil {
		return "", err
	}
	if bytes.Contains(page.JSON, []byte(`"items":[]`)) {
		return "0", nil
	}
	return "1", nil
}

func mustMember(t *testing.T, archive *dumpformat.Archive, name string) []byte {
	t.Helper()
	data, ok := archive.Member(name)
	if !ok {
		t.Fatalf("archive member %s missing", name)
	}
	return data
}

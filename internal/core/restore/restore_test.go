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
	"path/filepath"
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

func TestDumpRestoreClaimAndApplyAcrossDrivers(t *testing.T) {
	for _, driverName := range []string{"memory", "sqlite"} {
		driverName := driverName
		t.Run(driverName, func(t *testing.T) {
			ctx := context.Background()
			source, err := cli.OpenLocal(ctx, cli.LocalConfig{Project: "fixture", Agent: "source", Storage: cli.StorageConfig{Driver: "memory"}})
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

func TestDumpCursorBindsHistoryAndPageSize(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "dump-cursor", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	mgr := restore.NewManager(store, lease)
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
	source, err := cli.OpenLocal(ctx, cli.LocalConfig{Project: "fixture", Agent: "source", Storage: cli.StorageConfig{Driver: "memory"}})
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
	target, err := cli.OpenLocal(ctx, cli.LocalConfig{Project: "fixture", Agent: "target", Storage: cli.StorageConfig{Driver: "memory"}})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	key := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := target.Client.Call(ctx, "admin.restore.begin", map[string]any{"manifest": manifest, "humanPresence": presence}, key); err != nil {
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
	begin, err := target.Client.Call(ctx, "admin.restore.begin", map[string]any{"manifest": manifest, "humanPresence": presence}, key+"03")
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

func captureDump(t *testing.T, ctx context.Context, client *client.Client) ([]byte, *dumpformat.Archive) {
	t.Helper()
	var data bytes.Buffer
	cursor := ""
	var dumpID string
	for {
		params := map[string]any{"history": "full", "pageSize": 512, "humanPresence": presence}
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
	begin, err := client.Call(ctx, "admin.restore.begin", map[string]any{"manifest": manifestValue, "history": "full", "humanPresence": presence}, key)
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

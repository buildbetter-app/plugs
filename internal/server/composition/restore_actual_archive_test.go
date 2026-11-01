package composition

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
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

const actualArchiveSHA256 = "1b70534a5ff3e1da6cd9826fbdfae6ffea3436853b5e26162c588ab58fda5f67"

var actualArchivePresence = restore.Presence{
	"mode":      "presence_check",
	"challenge": "restore-actual-archive-test",
	"proof":     "restore-actual-archive-test",
}

type archiveCensusSnapshot struct {
	Meta  driver.Meta
	Rows  []string
	Kinds map[string]int
}

func readActualArchive(t *testing.T) ([]byte, map[string]any, string) {
	t.Helper()
	path := filepath.Join("internal", "server", "composition", "testdata", "server-representative-plug-dump-1.tar")
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		path = filepath.Join(root, os.Getenv("TEST_WORKSPACE"), path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 91136 {
		t.Fatalf("actual archive size=%d want 91136", len(data))
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != actualArchiveSHA256 {
		t.Fatalf("actual archive SHA-256=%s want %s", got, actualArchiveSHA256)
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("actual archive validation: %v", err)
	}
	manifestBytes, ok := archive.Member("manifest.json")
	if !ok {
		t.Fatal("actual archive has no manifest.json")
	}
	var manifest map[string]any
	decoder := json.NewDecoder(bytes.NewReader(manifestBytes))
	decoder.UseNumber()
	if err := decoder.Decode(&manifest); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	manifestSum := sha256.Sum256(canonical)
	return data, manifest, hex.EncodeToString(manifestSum[:])
}

func captureActualArchiveCensus(t *testing.T, store driver.Store) archiveCensusSnapshot {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	page, err := reader.Query(ctx, driver.Query{Limit: 10000})
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if page.NextCursor != nil {
		t.Fatalf("row census exceeded test limit")
	}
	census := archiveCensusSnapshot{Meta: meta, Rows: make([]string, 0, len(page.Items)), Kinds: map[string]int{}}
	for _, row := range page.Items {
		sum := sha256.Sum256(row.Body)
		census.Rows = append(census.Rows, fmt.Sprintf("%s/%s/%s", row.Kind, row.Ref.ID, hex.EncodeToString(sum[:])))
		census.Kinds[row.Kind]++
	}
	sort.Strings(census.Rows)
	return census
}

type loggedTestPresence struct {
	t     *testing.T
	calls int
}

func loggedTestPresenceVerifier(t *testing.T) (*loggedTestPresence, restore.PresenceVerifier) {
	t.Helper()
	state := &loggedTestPresence{t: t}
	return state, restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, presence restore.Presence) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		state.calls++
		const actor = "anonymous (test presence)"
		t.Logf("TEST-ONLY PresenceVerifier STUB actor=%q method=%s digest=%s", actor, method, digest)
		if method == "" || digest == "" || presence["proof"] != "restore-actual-archive-test" {
			return errors.New("test presence is not bound to restore operation")
		}
		return nil
	})
}

func TestProductionCompositionRestoreFailsClosedWithoutPresenceVerifier(t *testing.T) {
	_, manifest, manifestHash := readActualArchive(t)
	f := startRuntime(t, Config{Driver: "memory", Project: "pl", Address: DefaultAddress, AuthMode: "anonymous"})
	before := captureActualArchiveCensus(t, f.r.Store)
	_, err := f.client.Call(context.Background(), "admin.restore.begin", map[string]any{
		"manifest": manifest, "history": "head", "humanPresence": actualArchivePresence,
	}, strings.Repeat("0", 32))
	var remote *wire.Error
	if !errors.As(err, &remote) || remote.Data.Code != "human_presence_required" {
		t.Fatalf("nil production verifier error=%v", err)
	}
	if manifestHash == "" {
		t.Fatal("manifest hash was empty")
	}
	after := captureActualArchiveCensus(t, f.r.Store)
	if before.Meta != after.Meta || !equalStringSlices(before.Rows, after.Rows) {
		t.Fatalf("nil production verifier changed target: before=%+v after=%+v", before, after)
	}
	t.Logf("production nil verifier refused before write: code=%s rows=%d head=%s", remote.Data.Code, len(after.Rows), after.Meta.Head)
}

func TestActualArchiveRestoreClaimNextAndTxApplyWithLoggedTestPresence(t *testing.T) {
	data, manifest, manifestHash := readActualArchive(t)
	presenceState, verifier := loggedTestPresenceVerifier(t)
	ctx := context.Background()
	startupIncarnation := "restore-composition-startup"
	cursorKey := bytes.Repeat([]byte{0x4a}, 32)
	// Mint a cursor under the target's startup generation. The target starts
	// empty for the native restore, so a separate composition with the same
	// startup metadata supplies the pre-restore page token.
	source := startRuntime(t, Config{
		Driver: "memory", Project: "pl", Address: DefaultAddress, AuthMode: "anonymous", CursorKey: cursorKey,
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) {
			return memory.NewWithOptions(memory.Options{StoreID: "restore-cursor-source", Incarnation: startupIncarnation})
		},
	})
	for i := 0; i < 2; i++ {
		if _, err := source.client.Call(ctx, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{"title": fmt.Sprintf("cursor source %d", i)},
		}, ""); err != nil {
			t.Fatal(err)
		}
	}
	sourcePage, err := source.client.Call(ctx, "bead.ready", map[string]any{"limit": 1}, "")
	if err != nil {
		t.Fatalf("source cursor page: %v", err)
	}
	oldCursor, ok := resultObject(t, sourcePage)["nextCursor"].(string)
	if !ok || oldCursor == "" {
		t.Fatalf("source page did not issue a cursor: %s", sourcePage.JSON)
	}
	f := startRuntime(t, Config{
		Driver: "memory", Project: "pl", Address: DefaultAddress, AuthMode: "anonymous", CursorKey: cursorKey,
		RestorePresence: verifier,
		StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) {
			return memory.NewWithOptions(memory.Options{StoreID: "restore-cursor-target", Incarnation: startupIncarnation})
		},
	})
	before := captureActualArchiveCensus(t, f.r.Store)
	if !before.Meta.Head.IsZero() || len(before.Rows) != 0 {
		t.Fatalf("fresh target is not empty: %+v", before)
	}

	beginKey := strings.Repeat("1", 32)
	pageHash := sha256.Sum256(data)
	pageDigest := hex.EncodeToString(pageHash[:])
	begin, err := f.client.Call(context.Background(), "admin.restore.begin", map[string]any{
		"manifest": manifest, "history": "head", "humanPresence": actualArchivePresence,
	}, beginKey)
	if err != nil {
		t.Fatalf("actual archive begin: %v", err)
	}
	restoreID := compositionRestoreStatus(t, begin)["restoreId"].(string)
	if _, err := f.client.Call(context.Background(), "admin.restore.chunk", map[string]any{
		"restoreId": restoreID, "page": 0, "data": base64.StdEncoding.EncodeToString(data), "sha256": pageDigest, "humanPresence": actualArchivePresence,
	}, strings.Repeat("2", 32)); err != nil {
		t.Fatalf("actual archive chunk: %v", err)
	}
	finished, err := f.client.Call(context.Background(), "admin.restore.finish", map[string]any{
		"restoreId": restoreID, "manifestSha256": manifestHash, "humanPresence": actualArchivePresence,
	}, strings.Repeat("3", 32))
	if err != nil {
		t.Fatalf("actual archive finish: %v", err)
	}
	if got := compositionRestoreStatus(t, finished)["state"]; got != "complete" {
		t.Fatalf("actual archive finish state=%v", got)
	}
	afterRestore := captureActualArchiveCensus(t, f.r.Store)
	wantKinds := map[string]int{"bead": 4, "edge": 1, "comment": 1, "config": 12, "beads-archive": 115, "restore-archive": 1, "instance_config": 2}
	if afterRestore.Meta.Head.String() != "1" || len(afterRestore.Rows) != 136 || !reflectDeepEqualMap(afterRestore.Kinds, wantKinds) {
		t.Fatalf("actual archive census=%+v want head=1 rows=136 kinds=%v", afterRestore, wantKinds)
	}
	for _, id := range []string{"pl-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "pl-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "pl-cccccccccccccccccccccccccccccccc", "pl-dddddddddddddddddddddddddddddddd"} {
		if !containsCensusRow(afterRestore.Rows, "bead/"+id+"/") {
			t.Fatalf("restored census omitted bead %s: %v", id, afterRestore.Rows)
		}
	}
	t.Logf("actual archive restored: pre_meta=%+v pre_rows=%d post_meta=%+v post_rows=%d kinds=%v", before.Meta, len(before.Rows), afterRestore.Meta, len(afterRestore.Rows), afterRestore.Kinds)

	for _, method := range []string{"bead.ready", "history.query"} {
		params := map[string]any{"limit": 1}
		if method == "history.query" {
			params["asOf"] = map[string]any{"revision": afterRestore.Meta.Head}
		}
		page, err := f.client.Call(context.Background(), method, params, "")
		if err != nil {
			t.Fatalf("%s after restore: %v", method, err)
		}
		var decoded struct {
			Items []json.RawMessage `json:"items"`
			AsOf  struct {
				Incarnation string `json:"incarnation"`
			} `json:"asOf"`
		}
		if err := json.Unmarshal(page.JSON, &decoded); err != nil || len(decoded.Items) != 1 || decoded.AsOf.Incarnation != afterRestore.Meta.Incarnation {
			t.Fatalf("%s retained stale composition generation: page=%s err=%v", method, page.JSON, err)
		}
	}
	if _, err := f.client.Call(ctx, "memory.list", map[string]any{"limit": 1}, ""); err != nil {
		t.Fatalf("memory.list after restore retained stale composition generation: %v", err)
	}
	hello := f.r.RPC.Dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":"hello","method":"server.hello","params":{}}`))
	var helloResponse struct {
		Result struct {
			Incarnation string `json:"incarnation"`
		} `json:"result"`
		Error *wire.Error `json:"error"`
	}
	if err := json.Unmarshal(hello, &helloResponse); err != nil || helloResponse.Error != nil || helloResponse.Result.Incarnation != afterRestore.Meta.Incarnation {
		t.Fatalf("hello retained startup incarnation: page=%s err=%v", hello, err)
	}
	_, err = f.client.Call(context.Background(), "bead.ready", map[string]any{"limit": 1, "cursor": oldCursor}, "")
	var oldCursorError *wire.Error
	if !errors.As(err, &oldCursorError) || oldCursorError.Data.Code != "incarnation_changed" {
		t.Fatalf("pre-restore cursor was not typed as incarnation_changed: %v", err)
	}
	if oldCursorError.Data.Details["fromIncarnation"] != before.Meta.Incarnation || oldCursorError.Data.Details["toIncarnation"] != afterRestore.Meta.Incarnation {
		t.Fatalf("old cursor generation details=%v", oldCursorError.Data.Details)
	}

	claim, err := f.client.Call(context.Background(), "bead.claimNext", map[string]any{
		"filter": map[string]any{"types": []string{"issue"}},
	}, strings.Repeat("4", 32))
	if err != nil {
		t.Fatalf("claimNext after actual restore: %v", err)
	}
	claimObject := resultObject(t, claim)
	claimResults, ok := claimObject["results"].([]any)
	if !ok || len(claimResults) != 1 {
		t.Fatalf("claimNext result=%s", claim.JSON)
	}
	claimedID := claimResults[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
	if claimedID != "pl-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("claimNext selected %q", claimedID)
	}
	claimMeta := captureActualArchiveCensus(t, f.r.Store).Meta
	if claimMeta.Head.String() != "2" {
		t.Fatalf("claimNext did not advance head: %+v", claimMeta)
	}

	changeSet := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{
		Target: model.RefByID(claimedID, model.Natural64(0)), Fields: map[string]any{"description": "updated after actual import"},
	}}}}
	applied, err := f.client.Call(context.Background(), "tx.apply", map[string]any{"changeSet": changeSet}, strings.Repeat("5", 32))
	if err != nil {
		t.Fatalf("tx.apply after actual restore: %v", err)
	}
	applyObject := resultObject(t, applied)
	if len(applyObject["results"].([]any)) != 1 {
		t.Fatalf("tx.apply result=%s", applied.JSON)
	}
	replayed, err := f.client.Call(context.Background(), "tx.apply", map[string]any{"changeSet": changeSet}, strings.Repeat("5", 32))
	if err != nil || !replayed.Replayed || !bytes.Equal(applied.JSON, replayed.JSON) {
		t.Fatalf("tx.apply replay=%s replayed=%t err=%v", replayed.JSON, replayed.Replayed, err)
	}
	postWorkload := captureActualArchiveCensus(t, f.r.Store)
	if postWorkload.Meta.Head.String() != "3" || len(postWorkload.Rows) != 136 {
		t.Fatalf("post-workload census=%+v", postWorkload)
	}

	converted, err := reverseconverter.ConvertArchive(data)
	if err != nil {
		t.Fatalf("actual archive reverse conversion: %v", err)
	}
	assertActualArchiveOpaqueMetadata(t, data, converted.JSONL)
	if path := os.Getenv("PR196_REVERSE_PATH"); path != "" {
		if err := os.WriteFile(path, converted.JSONL, 0o600); err != nil {
			t.Fatalf("write reverse JSONL: %v", err)
		}
	}
	t.Logf("actual archive reverse PASS: bytes=%d digest=%s", len(converted.JSONL), converted.Digest)

	// Same-bound production retry must recover the original receipt read-only;
	// it must not replay the archive into the now-nonempty target.
	retryBegin, err := f.client.Call(context.Background(), "admin.restore.begin", map[string]any{
		"manifest": manifest, "history": "head", "humanPresence": actualArchivePresence,
	}, beginKey)
	if err != nil {
		t.Fatalf("same-bound recovery begin: %v", err)
	}
	retryID := compositionRestoreStatus(t, retryBegin)["restoreId"].(string)
	if _, err := f.client.Call(context.Background(), "admin.restore.chunk", map[string]any{
		"restoreId": retryID, "page": 0, "data": base64.StdEncoding.EncodeToString(data), "sha256": pageDigest, "humanPresence": actualArchivePresence,
	}, strings.Repeat("6", 32)); err != nil {
		t.Fatalf("same-bound recovery chunk: %v", err)
	}
	recovered, err := f.client.Call(context.Background(), "admin.restore.finish", map[string]any{
		"restoreId": retryID, "manifestSha256": manifestHash, "humanPresence": actualArchivePresence,
	}, strings.Repeat("7", 32))
	if err != nil || compositionRestoreStatus(t, recovered)["state"] != "complete" {
		t.Fatalf("same-bound recovery result=%s err=%v", recovered.JSON, err)
	}
	afterRecovery := captureActualArchiveCensus(t, f.r.Store)
	if afterRecovery.Meta != postWorkload.Meta || !equalStringSlices(afterRecovery.Rows, postWorkload.Rows) {
		t.Fatalf("same-bound recovery wrote state: before=%+v after=%+v", postWorkload, afterRecovery)
	}
	if presenceState.calls < 8 {
		t.Fatalf("logged test verifier calls=%d; expected begin/chunk/finish and recovery checks", presenceState.calls)
	}
	t.Logf("actual archive workload PASS with test-only stubbed gate: calls=%d final_meta=%+v rows=%d", presenceState.calls, afterRecovery.Meta, len(afterRecovery.Rows))
}

func TestComposedMemoryListCursorAcrossRestore(t *testing.T) {
	data, manifest, manifestHash := readActualArchive(t)
	pageHash := sha256.Sum256(data)
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			_, verifier := loggedTestPresenceVerifier(t)
			cursorKey := bytes.Repeat([]byte{0x6b}, 32)
			config := Config{
				Driver: engine, Project: "pl", Address: DefaultAddress, AuthMode: "anonymous",
				CursorKey: cursorKey, RestorePresence: verifier,
			}
			if engine == "sqlite" {
				config.DSN = filepath.Join(t.TempDir(), "restore-memory-cursor.db")
			}
			target := startRuntime(t, config)
			before := captureActualArchiveCensus(t, target.r.Store)
			if !before.Meta.Head.IsZero() || len(before.Rows) != 0 {
				t.Fatalf("restore target is not empty: %+v", before)
			}

			// Restore requires an empty target. A real sibling runtime sharing
			// its startup incarnation, signing key and caller scope issues the
			// pre-restore token without populating or resetting the target.
			source := startRuntime(t, Config{
				Driver: "memory", Project: "pl", Address: DefaultAddress, AuthMode: "anonymous", CursorKey: cursorKey,
				StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) {
					return memory.NewWithOptions(memory.Options{Incarnation: before.Meta.Incarnation})
				},
			})
			save := func(f *runtimeFixture, key, content string) model.Revision {
				t.Helper()
				result, err := f.client.Call(ctx, "memory.save", map[string]any{"key": key, "content": content}, "")
				if err != nil {
					t.Fatalf("save memory %s: %v", key, err)
				}
				var commit struct {
					Revision model.Revision `json:"revision"`
				}
				if err := json.Unmarshal(result.JSON, &commit); err != nil {
					t.Fatal(err)
				}
				return commit.Revision
			}
			list := func(f *runtimeFixture, cursor, key, content string, revision model.Revision, more bool) string {
				t.Helper()
				params := map[string]any{"limit": 1}
				if cursor != "" {
					params["cursor"] = cursor
				}
				result, err := f.client.Call(ctx, "memory.list", params, "")
				if err != nil {
					t.Fatalf("memory.list: %v", err)
				}
				var page struct {
					Items []struct {
						Key      string         `json:"key"`
						Content  string         `json:"content"`
						Revision model.Revision `json:"revision"`
					} `json:"items"`
					NextCursor *string `json:"nextCursor"`
				}
				if err := json.Unmarshal(result.JSON, &page); err != nil {
					t.Fatal(err)
				}
				if len(page.Items) != 1 || page.Items[0].Key != key || page.Items[0].Content != content || page.Items[0].Revision != revision {
					t.Fatalf("memory page=%s want key=%s content=%s revision=%s", result.JSON, key, content, revision)
				}
				if !more {
					if page.NextCursor != nil {
						t.Fatalf("final memory page retained a cursor: %s", result.JSON)
					}
					return ""
				}
				if page.NextCursor == nil || *page.NextCursor == "" || *page.NextCursor == cursor {
					t.Fatalf("memory page did not advance: %s", result.JSON)
				}
				return *page.NextCursor
			}
			alphaRevision := save(source, "alpha", "before restore")
			zetaRevision := save(source, "zeta", "before restore")
			oldCursor := list(source, "", "alpha", "before restore", alphaRevision, true)
			list(source, oldCursor, "zeta", "before restore", zetaRevision, false)

			restoreArchive := func() archiveCensusSnapshot {
				t.Helper()
				begin, err := target.client.Call(ctx, "admin.restore.begin", map[string]any{
					"manifest": manifest, "history": "head", "humanPresence": actualArchivePresence,
				}, strings.Repeat("a", 32))
				if err != nil {
					t.Fatalf("restore begin: %v", err)
				}
				id := compositionRestoreStatus(t, begin)["restoreId"].(string)
				if _, err := target.client.Call(ctx, "admin.restore.chunk", map[string]any{
					"restoreId": id, "page": 0, "data": base64.StdEncoding.EncodeToString(data),
					"sha256": hex.EncodeToString(pageHash[:]), "humanPresence": actualArchivePresence,
				}, ""); err != nil {
					t.Fatalf("restore chunk: %v", err)
				}
				finish, err := target.client.Call(ctx, "admin.restore.finish", map[string]any{
					"restoreId": id, "manifestSha256": manifestHash, "humanPresence": actualArchivePresence,
				}, "")
				if err != nil || compositionRestoreStatus(t, finish)["state"] != "complete" {
					t.Fatalf("restore finish=%s err=%v", finish.JSON, err)
				}
				return captureActualArchiveCensus(t, target.r.Store)
			}
			restored := restoreArchive()
			if restored.Meta.Incarnation == "" || restored.Meta.Incarnation == before.Meta.Incarnation || restored.Meta.Head.String() != "1" || len(restored.Rows) != 136 {
				t.Fatalf("restore did not publish a new generation: before=%+v after=%+v", before.Meta, restored.Meta)
			}
			refused, err := target.client.Call(ctx, "memory.list", map[string]any{"limit": 1, "cursor": oldCursor}, "")
			var remote *wire.Error
			if !errors.As(err, &remote) || remote.Data.Code != "incarnation_changed" || len(refused.JSON) != 0 {
				t.Fatalf("stale memory cursor result=%s err=%v", refused.JSON, err)
			}
			if remote.Data.Details["fromIncarnation"] != before.Meta.Incarnation || remote.Data.Details["toIncarnation"] != restored.Meta.Incarnation {
				t.Fatalf("stale memory cursor generation details=%v", remote.Data.Details)
			}
			afterRefusal := captureActualArchiveCensus(t, target.r.Store)
			if afterRefusal.Meta != restored.Meta || !equalStringSlices(afterRefusal.Rows, restored.Rows) {
				t.Fatal("stale memory cursor refusal changed the restored store")
			}

			alphaRevision = save(target, "alpha", "after restore")
			save(target, "zeta", "after restore")
			paged := captureActualArchiveCensus(t, target.r.Store)
			cursor := list(target, "", "alpha", "after restore", alphaRevision, true)
			// Same-bound recovery is read-only and must keep this cursor valid.
			recovered := restoreArchive()
			if recovered.Meta != paged.Meta || !equalStringSlices(recovered.Rows, paged.Rows) {
				t.Fatal("restore receipt recovery changed the memory cursor generation or rows")
			}
			zetaRevision = save(target, "zeta", "updated after first page")
			middleRevision := save(target, "middle", "inserted after first page")
			cursor = list(target, cursor, "middle", "inserted after first page", middleRevision, true)
			list(target, cursor, "zeta", "updated after first page", zetaRevision, false)
			t.Logf("memory.list restore cursor PASS: driver=%s from=%s to=%s stale=incarnation_changed recovery=read-only live-pages=alpha,middle,zeta", engine, before.Meta.Incarnation, restored.Meta.Incarnation)
		})
	}
}

func assertActualArchiveOpaqueMetadata(t *testing.T, data, jsonl []byte) {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("actual archive opaque read: %v", err)
	}
	lines := map[string]map[string]json.RawMessage{}
	for _, raw := range bytes.Split(bytes.TrimSpace(jsonl), []byte{'\n'}) {
		var line struct {
			ID       string                     `json:"id"`
			Metadata map[string]json.RawMessage `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &line); err != nil || line.ID == "" {
			continue
		}
		lines[line.ID] = line.Metadata
	}
	for _, record := range archive.Records("bead") {
		metadata, ok := lines[record.ID]
		if !ok {
			t.Fatalf("reverse JSONL omitted bead %s", record.ID)
		}
		for _, field := range dumpformat.OpaqueIssueFields() {
			value, present := record.Source.Fields[field]
			if !present {
				continue
			}
			want, err := json.Marshal(value)
			if err != nil {
				t.Fatalf("marshal source %s/%s: %v", record.ID, field, err)
			}
			got, present := metadata["beads."+field]
			if !present || !bytes.Equal(got, want) {
				t.Fatalf("reverse opaque %s/%s=%s want %s", record.ID, field, got, want)
			}
		}
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func reflectDeepEqualMap(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func containsCensusRow(rows []string, prefix string) bool {
	for _, row := range rows {
		if strings.HasPrefix(row, prefix) {
			return true
		}
	}
	return false
}

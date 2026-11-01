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
	for _, key := range []string{"cursor-source-memory-0", "cursor-source-memory-1"} {
		if _, err := source.client.Call(ctx, "memory.save", map[string]any{
			"key": key, "content": "pre-restore memory",
		}, ""); err != nil {
			t.Fatalf("source memory %s: %v", key, err)
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
	sourceMemoryPage, err := source.client.Call(ctx, "memory.list", map[string]any{"limit": 1}, "")
	if err != nil {
		t.Fatalf("source memory cursor page: %v", err)
	}
	oldMemoryCursor, ok := resultObject(t, sourceMemoryPage)["nextCursor"].(string)
	if !ok || oldMemoryCursor == "" {
		t.Fatalf("source memory page did not issue a cursor: %s", sourceMemoryPage.JSON)
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

	_, err = f.client.Call(ctx, "memory.list", map[string]any{"limit": 1, "cursor": oldMemoryCursor}, "")
	var oldMemoryCursorError *wire.Error
	if !errors.As(err, &oldMemoryCursorError) || oldMemoryCursorError.Data.Code != "incarnation_changed" {
		t.Fatalf("pre-restore memory cursor was not typed as incarnation_changed: %v", err)
	}
	if oldMemoryCursorError.Data.Details["fromIncarnation"] != before.Meta.Incarnation || oldMemoryCursorError.Data.Details["toIncarnation"] != afterRestore.Meta.Incarnation {
		t.Fatalf("old memory cursor generation details=%v", oldMemoryCursorError.Data.Details)
	}

	for _, key := range []string{"restore-memory-a", "restore-memory-b"} {
		if _, err := f.client.Call(ctx, "memory.save", map[string]any{
			"key": key, "content": "post-restore memory",
		}, ""); err != nil {
			t.Fatalf("post-restore memory %s: %v", key, err)
		}
	}
	postRestorePage, err := f.client.Call(ctx, "memory.list", map[string]any{"limit": 1}, "")
	if err != nil {
		t.Fatalf("post-restore memory cursor page: %v", err)
	}
	postRestoreObject := resultObject(t, postRestorePage)
	postRestoreItems, ok := postRestoreObject["items"].([]any)
	if !ok || len(postRestoreItems) != 1 || postRestoreItems[0].(map[string]any)["key"] != "restore-memory-a" {
		t.Fatalf("post-restore memory page=%s", postRestorePage.JSON)
	}
	postRestoreAsOf, ok := postRestoreObject["asOf"].(map[string]any)
	if !ok || postRestoreAsOf["incarnation"] != afterRestore.Meta.Incarnation {
		t.Fatalf("post-restore memory page generation=%v want %s", postRestoreObject["asOf"], afterRestore.Meta.Incarnation)
	}
	freshMemoryCursor, ok := postRestoreObject["nextCursor"].(string)
	if !ok || freshMemoryCursor == "" {
		t.Fatalf("post-restore memory page did not issue a cursor: %s", postRestorePage.JSON)
	}
	if _, err := f.client.Call(ctx, "memory.save", map[string]any{
		"key": "restore-memory-c", "content": "post-restore memory",
	}, ""); err != nil {
		t.Fatalf("post-restore later memory: %v", err)
	}
	continuedMemoryPage, err := f.client.Call(ctx, "memory.list", map[string]any{"limit": 1, "cursor": freshMemoryCursor}, "")
	if err != nil {
		t.Fatalf("post-restore memory continuation: %v", err)
	}
	continuedObject := resultObject(t, continuedMemoryPage)
	continuedItems, ok := continuedObject["items"].([]any)
	if !ok || len(continuedItems) != 1 || continuedItems[0].(map[string]any)["key"] != "restore-memory-b" {
		t.Fatalf("post-restore memory continuation=%s", continuedMemoryPage.JSON)
	}
	lastMemoryCursor, ok := continuedObject["nextCursor"].(string)
	if !ok || lastMemoryCursor == "" {
		t.Fatalf("post-restore memory continuation did not advance: %s", continuedMemoryPage.JSON)
	}
	lastMemoryPage, err := f.client.Call(ctx, "memory.list", map[string]any{"limit": 1, "cursor": lastMemoryCursor}, "")
	if err != nil {
		t.Fatalf("post-restore final memory continuation: %v", err)
	}
	lastObject := resultObject(t, lastMemoryPage)
	lastItems, ok := lastObject["items"].([]any)
	if !ok || len(lastItems) != 1 || lastItems[0].(map[string]any)["key"] != "restore-memory-c" || lastObject["nextCursor"] != nil {
		t.Fatalf("post-restore final memory page=%s", lastMemoryPage.JSON)
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

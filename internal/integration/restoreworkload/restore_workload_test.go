package restoreworkload_test

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
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

var testPresence = map[string]any{
	"mode": "presence_check", "challenge": "restore-workload", "proof": "restore-workload",
}

func trustedPresence() restore.PresenceVerifier {
	return restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, got restore.Presence) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.HasPrefix(method, "admin.") || digest == "" || got["proof"] != "restore-workload" {
			return fmt.Errorf("test presence is not bound to the admitted admin operation")
		}
		return nil
	})
}

func openSQLiteLocal(t *testing.T, path, project, agent string) *cli.Local {
	t.Helper()
	local, err := cli.OpenLocal(context.Background(), cli.LocalConfig{
		Project: project,
		Agent:   agent,
		Storage: cli.StorageConfig{Driver: "sqlite", DSN: path},
		Factory: func(ctx context.Context, cfg cli.StorageConfig) (driver.Store, func() error, error) {
			store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: cfg.DSN})
			if err != nil {
				return nil, nil, err
			}
			return store, store.Close, nil
		},
		PresenceVerifier: trustedPresence(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return local
}

type restoredRuntime struct {
	local              *cli.Local
	archive            *dumpformat.Archive
	sourceIDs          []string
	restoreIncarnation string
}

func prepareRestoredRuntime(t *testing.T) restoredRuntime {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	source := openSQLiteLocal(t, filepath.Join(dir, "source.db"), "restore", "source")
	ids := make([]string, 0, 2)
	for _, title := range []string{"restored first", "restored second"} {
		result, err := source.Client.Call(ctx, "bead.create", map[string]any{
			"type": "issue", "fields": map[string]any{"title": title, "priority": 1},
		}, "")
		if err != nil {
			_ = source.Close()
			t.Fatal(err)
		}
		var commit model.Commit
		if err := json.Unmarshal(result.JSON, &commit); err != nil || len(commit.Results) != 1 {
			_ = source.Close()
			t.Fatalf("source create=%s err=%v", result.JSON, err)
		}
		ids = append(ids, commit.Results[0].Ref.ID)
	}
	data, archive := captureHeadDump(t, source.Client)
	if archive.Manifest().NativeHead != "2" || archive.Manifest().History != "head" || archive.Manifest().NativeStoreID == "" {
		_ = source.Close()
		t.Fatalf("source archive does not expose the native head contract: %+v", archive.Manifest())
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	targetPath := filepath.Join(dir, "target.db")
	target := openSQLiteLocal(t, targetPath, "restore", "restorer")
	restoredIncarnation := restoreHead(t, target.Client, data, archive)
	if restoredIncarnation == "" || restoredIncarnation == archive.Manifest().NativeIncarnation {
		_ = target.Close()
		t.Fatalf("restore did not establish a new incarnation: source=%q target=%q", archive.Manifest().NativeIncarnation, restoredIncarnation)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	// This is a same-process SQLite close/reopen. From here onward, the test uses
	// only the client exposed by a fresh bound service/RPC composition over the
	// file.
	reopened := openSQLiteLocal(t, targetPath, "restore", "worker")
	t.Cleanup(func() { _ = reopened.Close() })
	return restoredRuntime{local: reopened, archive: archive, sourceIDs: ids, restoreIncarnation: restoredIncarnation}
}

func TestRestoredSQLiteReopenServesClaimNextAndTxApply(t *testing.T) {
	runtime := prepareRestoredRuntime(t)
	ctx := context.Background()

	ready := readyPage(t, runtime.local.Client)
	if ready.revision != "1" || ready.incarnation != runtime.restoreIncarnation || len(ready.ids) != 2 {
		t.Fatalf("reopened restored ready page=%+v", ready)
	}
	for _, id := range runtime.sourceIDs {
		if !contains(ready.ids, id) {
			t.Fatalf("restore changed source identity %q: %v", id, ready.ids)
		}
	}

	claimKey := strings.Repeat("1", 32)
	claim, err := runtime.local.Client.Call(ctx, "bead.claimNext", map[string]any{
		"filter": map[string]any{"types": []string{"issue"}},
	}, claimKey)
	if err != nil {
		t.Fatal(err)
	}
	var claimed model.Commit
	if err := json.Unmarshal(claim.JSON, &claimed); err != nil || claimed.Revision.String() != "2" || len(claimed.Results) != 1 || claimed.LeaseTtlMs == nil {
		t.Fatalf("claimNext result=%s err=%v", claim.JSON, err)
	}
	claimedID := claimed.Results[0].Ref.ID
	if !contains(runtime.sourceIDs, claimedID) {
		t.Fatalf("claimNext changed restored identity: %q not in %v", claimedID, runtime.sourceIDs)
	}
	claimReplay, err := runtime.local.Client.Call(ctx, "bead.claimNext", map[string]any{
		"filter": map[string]any{"types": []string{"issue", "issue"}},
	}, claimKey)
	if err != nil || !claimReplay.Replayed || !bytes.Equal(claim.JSON, claimReplay.JSON) {
		t.Fatalf("claimNext replay=%s replayed=%t err=%v", claimReplay.JSON, claimReplay.Replayed, err)
	}
	afterClaim := readyPage(t, runtime.local.Client)
	if afterClaim.revision != "2" || len(afterClaim.ids) != 1 || afterClaim.ids[0] == claimedID {
		t.Fatalf("claimNext replay claimed another restored record: %+v", afterClaim)
	}

	txKey := strings.Repeat("2", 32)
	changeSet := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(claimedID, model.Natural64(0)), Fields: map[string]any{"title": "updated after reopen"}}},
		{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: "restore", Fields: map[string]any{"title": "created after reopen"}}},
	}}
	applied, err := runtime.local.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": changeSet}, txKey)
	if err != nil {
		t.Fatal(err)
	}
	var commit model.Commit
	if err := json.Unmarshal(applied.JSON, &commit); err != nil || commit.Revision.String() != "3" || len(commit.Results) != 2 {
		t.Fatalf("tx.apply=%s err=%v", applied.JSON, err)
	}
	createdID := commit.Results[1].Ref.ID
	if len(createdID) != 35 || !strings.HasPrefix(createdID, "pl-") || contains(runtime.sourceIDs, createdID) {
		t.Fatalf("ordinary native-restored allocator produced %q", createdID)
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(createdID, "pl-")); err != nil || strings.ToLower(createdID) != createdID {
		t.Fatalf("ordinary allocator ID is not canonical lowercase hex: %q", createdID)
	}
	for _, result := range commit.Results {
		if result.Revision.String() != "3" {
			t.Fatalf("tx.apply was not one atomic revision: %+v", commit.Results)
		}
	}
	txReplay, err := runtime.local.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": changeSet}, txKey)
	if err != nil || !txReplay.Replayed || !bytes.Equal(applied.JSON, txReplay.JSON) {
		t.Fatalf("tx.apply replay=%s replayed=%t err=%v", txReplay.JSON, txReplay.Replayed, err)
	}
	if got := readyPage(t, runtime.local.Client); got.revision != "3" || len(got.ids) != 2 {
		t.Fatalf("tx.apply replay changed head or ready membership: %+v", got)
	}

	original := historyPage(t, runtime.local.Client, claimedID, "1", runtime.restoreIncarnation)
	if len(original) != 1 || original[0].title != titleForID(runtime, claimedID) || original[0].revision != "1" {
		t.Fatalf("restore-floor history=%+v", original)
	}
	current, err := runtime.local.Client.Call(ctx, "bead.get", map[string]any{"id": map[string]any{"id": claimedID}}, "")
	if err != nil || !bytes.Contains(current.JSON, []byte("updated after reopen")) {
		t.Fatalf("post-tx read=%s err=%v", current.JSON, err)
	}

	_, postArchive := captureHeadDump(t, runtime.local.Client)
	manifest := postArchive.Manifest()
	if manifest.NativeStoreID != runtime.archive.Manifest().NativeStoreID || manifest.StoreID != runtime.archive.Manifest().StoreID || manifest.NativeIncarnation != runtime.restoreIncarnation || manifest.NativeHead != "3" {
		t.Fatalf("post-workload identity/revision=%+v", manifest)
	}
}

func TestRestoredSQLiteRefusalsDoNotWriteOrConsumeReplayKey(t *testing.T) {
	runtime := prepareRestoredRuntime(t)
	ctx := context.Background()
	before := readyPage(t, runtime.local.Client)
	manifestValue, err := wire.DecodeJSON(mustMember(t, runtime.archive, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.local.Client.Call(ctx, "admin.restore.begin", map[string]any{
		"manifest": manifestValue, "history": "head", "humanPresence": testPresence,
	}, strings.Repeat("3", 32))
	requireCode(t, err, "restore_requires_empty")
	if got := readyPage(t, runtime.local.Client); got.revision != before.revision || got.incarnation != before.incarnation || !sameSet(got.ids, before.ids) {
		t.Fatalf("populated restore refusal wrote state: before=%+v after=%+v", before, got)
	}

	id := runtime.sourceIDs[0]
	retryKey := strings.Repeat("4", 32)
	stale := model.Natural64(0)
	patch := model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(id, model.Natural64(0)), Fields: map[string]any{"title": "accepted only after corrected expectation"}}}
	refused := model.ChangeSet{Ops: []model.Op{patch}, Expect: []model.Precondition{{Ref: model.RefByID(id, model.Natural64(0)), Revision: &stale}}}
	_, err = runtime.local.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": refused}, retryKey)
	requireCode(t, err, "precondition_failed")
	if got := readyPage(t, runtime.local.Client); got.revision != "1" {
		t.Fatalf("precondition refusal advanced head: %+v", got)
	}

	current := model.Natural64(1)
	accepted := model.ChangeSet{Ops: []model.Op{patch}, Expect: []model.Precondition{{Ref: model.RefByID(id, model.Natural64(0)), Revision: &current}}}
	first, err := runtime.local.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": accepted}, retryKey)
	if err != nil {
		t.Fatalf("refusal consumed the idempotency key: %v", err)
	}
	var commit model.Commit
	if err := json.Unmarshal(first.JSON, &commit); err != nil || commit.Revision.String() != "2" {
		t.Fatalf("corrected tx.apply=%s err=%v", first.JSON, err)
	}
	replayed, err := runtime.local.Client.Call(ctx, "tx.apply", map[string]any{"changeSet": accepted}, retryKey)
	if err != nil || !replayed.Replayed || !bytes.Equal(first.JSON, replayed.JSON) {
		t.Fatalf("corrected replay=%s replayed=%t err=%v", replayed.JSON, replayed.Replayed, err)
	}
	if got := readyPage(t, runtime.local.Client); got.revision != "2" {
		t.Fatalf("replay advanced head: %+v", got)
	}

	_, err = runtime.local.Client.Call(ctx, "history.query", map[string]any{
		"asOf": map[string]any{"revision": 0, "incarnation": runtime.restoreIncarnation}, "limit": 10,
	}, "")
	requireCode(t, err, "cursor_expired")
	if got := readyPage(t, runtime.local.Client); got.revision != "2" {
		t.Fatalf("below-floor history refusal wrote state: %+v", got)
	}
}

type readySnapshot struct {
	ids         []string
	revision    string
	incarnation string
}

func readyPage(t *testing.T, c *client.Client) readySnapshot {
	t.Helper()
	result, err := c.Call(context.Background(), "bead.ready", map[string]any{"limit": 100}, "")
	if err != nil {
		t.Fatal(err)
	}
	value, err := wire.DecodeJSON(result.JSON)
	if err != nil {
		t.Fatal(err)
	}
	object := value.(map[string]any)
	asOf := object["asOf"].(map[string]any)
	got := readySnapshot{revision: asOf["revision"].(json.Number).String(), incarnation: asOf["incarnation"].(string)}
	for _, item := range object["items"].([]any) {
		got.ids = append(got.ids, item.(map[string]any)["id"].(string))
	}
	return got
}

type historicalBead struct {
	title    string
	revision string
}

func historyPage(t *testing.T, c *client.Client, id, revision, incarnation string) []historicalBead {
	t.Helper()
	result, err := c.Call(context.Background(), "history.query", map[string]any{
		"asOf":   map[string]any{"revision": json.Number(revision), "incarnation": incarnation},
		"filter": map[string]any{"resources": []string{id}}, "limit": 10,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	value, err := wire.DecodeJSON(result.JSON)
	if err != nil {
		t.Fatal(err)
	}
	var out []historicalBead
	for _, item := range value.(map[string]any)["items"].([]any) {
		bead := item.(map[string]any)
		out = append(out, historicalBead{title: bead["fields"].(map[string]any)["title"].(string), revision: bead["revision"].(json.Number).String()})
	}
	return out
}

func captureHeadDump(t *testing.T, c *client.Client) ([]byte, *dumpformat.Archive) {
	t.Helper()
	var data bytes.Buffer
	cursor, dumpID := "", ""
	for {
		params := map[string]any{"history": "head", "pageSize": 512, "humanPresence": testPresence}
		if cursor != "" {
			params["cursor"] = cursor
		}
		result, err := c.Call(context.Background(), "admin.dump", params, "")
		if err != nil {
			t.Fatal(err)
		}
		value, err := wire.DecodeJSON(result.JSON)
		if err != nil {
			t.Fatal(err)
		}
		page := value.(map[string]any)
		if dumpID == "" {
			dumpID = page["dumpId"].(string)
		} else if dumpID != page["dumpId"].(string) {
			t.Fatal("dump identity changed between pages")
		}
		chunk, err := base64.StdEncoding.DecodeString(page["data"].(string))
		if err != nil {
			t.Fatal(err)
		}
		data.Write(chunk)
		if page["done"].(bool) {
			break
		}
		cursor = fmt.Sprintf("%s:%s", dumpID, page["nextPage"].(json.Number).String())
	}
	archive, err := dumpformat.Read(bytes.NewReader(data.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	return data.Bytes(), archive
}

func restoreHead(t *testing.T, c *client.Client, data []byte, archive *dumpformat.Archive) string {
	t.Helper()
	manifestValue, err := wire.DecodeJSON(mustMember(t, archive, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	baseKey := strings.Repeat("a", 32)
	begin, err := c.Call(context.Background(), "admin.restore.begin", map[string]any{
		"manifest": manifestValue, "history": "head", "humanPresence": testPresence,
	}, baseKey)
	if err != nil {
		t.Fatal(err)
	}
	value, err := wire.DecodeJSON(begin.JSON)
	if err != nil {
		t.Fatal(err)
	}
	restoreID := value.(map[string]any)["restoreId"].(string)
	for page, offset := uint64(0), 0; offset < len(data); page++ {
		end := offset + 512
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		sum := sha256.Sum256(chunk)
		key := fmt.Sprintf("%s%08x", baseKey, page)
		_, err := c.Call(context.Background(), "admin.restore.chunk", map[string]any{
			"restoreId": restoreID, "page": page, "data": base64.StdEncoding.EncodeToString(chunk),
			"sha256": hex.EncodeToString(sum[:]), "humanPresence": testPresence,
		}, key)
		if err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	finish, err := c.Call(context.Background(), "admin.restore.finish", map[string]any{
		"restoreId": restoreID, "manifestSha256": hex.EncodeToString(sum[:]), "humanPresence": testPresence,
	}, baseKey+"ff")
	if err != nil {
		t.Fatal(err)
	}
	status, err := wire.DecodeJSON(finish.JSON)
	if err != nil {
		t.Fatal(err)
	}
	object := status.(map[string]any)
	if object["state"] != "complete" {
		t.Fatalf("restore did not complete: %s", finish.JSON)
	}
	return object["incarnation"].(string)
}

func mustMember(t *testing.T, archive *dumpformat.Archive, name string) []byte {
	t.Helper()
	data, ok := archive.Member(name)
	if !ok {
		t.Fatalf("archive member %s missing", name)
	}
	return data
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var remote *wire.Error
	if !errors.As(err, &remote) || remote.Data.Code != code {
		t.Fatalf("error=%v want code=%s", err, code)
	}
}

func titleForID(runtime restoredRuntime, id string) string {
	if id == runtime.sourceIDs[0] {
		return "restored first"
	}
	return "restored second"
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, value := range a {
		if !contains(b, value) {
			return false
		}
	}
	return true
}

type thinSliceWarningSubscriber struct{}

func (thinSliceWarningSubscriber) After(context.Context, port.CommittedChange, port.SnapshotReader) error {
	return errors.New("thin-slice post subscriber unavailable")
}

func openThinSliceLocal(t *testing.T, backend, dsn, agent string, warnings bool) *cli.Local {
	t.Helper()
	cfg := cli.LocalConfig{
		Project:          "restore-thin-slice",
		Agent:            agent,
		Storage:          cli.StorageConfig{Driver: backend, DSN: dsn},
		PresenceVerifier: trustedPresence(),
	}
	if backend == "sqlite" {
		cfg.Factory = func(ctx context.Context, storageCfg cli.StorageConfig) (driver.Store, func() error, error) {
			store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: storageCfg.DSN})
			if err != nil {
				return nil, nil, err
			}
			return store, store.Close, nil
		}
	} else if backend != "memory" {
		t.Fatalf("unknown thin-slice backend %q", backend)
	}
	if warnings {
		cfg.PipelineOptions = []pipeline.Option{pipeline.WithPostSubscribers(pipeline.PostBinding{
			ID:         "thin-slice-warning",
			Subscriber: thinSliceWarningSubscriber{},
		})}
	}
	local, err := cli.OpenLocal(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := local.Close(); err != nil {
			t.Error(err)
		}
	})
	return local
}

func thinSliceKey(n int) string { return fmt.Sprintf("%032x", n) }

func thinSliceCommit(t *testing.T, c *client.Client, method string, params map[string]any, key string) (model.Commit, client.Result) {
	t.Helper()
	result, err := c.Call(context.Background(), method, params, key)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	var commit model.Commit
	if err := json.Unmarshal(result.JSON, &commit); err != nil {
		t.Fatalf("%s response=%s: %v", method, result.JSON, err)
	}
	if len(commit.Warnings) != 1 || commit.Warnings[0].Code != "post_subscriber_failed" || commit.Warnings[0].Details["binding"] != "thin-slice-warning" {
		t.Fatalf("%s warnings=%+v response=%s", method, commit.Warnings, result.JSON)
	}
	return commit, result
}

func thinSliceCreate(t *testing.T, c *client.Client, title string, key int) model.Ref {
	t.Helper()
	result, err := c.Call(context.Background(), "bead.create", map[string]any{
		"type": "issue", "fields": map[string]any{"title": title},
	}, thinSliceKey(key))
	if err != nil {
		t.Fatal(err)
	}
	var commit model.Commit
	if err := json.Unmarshal(result.JSON, &commit); err != nil || len(commit.Results) != 1 {
		t.Fatalf("create=%s err=%v", result.JSON, err)
	}
	if len(commit.Warnings) != 0 {
		t.Fatalf("source create unexpectedly warned: %+v", commit.Warnings)
	}
	return commit.Results[0].Ref
}

func assertThinSliceRegistry(t *testing.T, c *client.Client) {
	t.Helper()
	result, err := c.Call(context.Background(), "rpc.discover", map[string]any{}, "")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		OpenRPC     string `json:"openrpc"`
		MethodsHash string `json:"methodsHash"`
		Methods     []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(result.JSON, &document); err != nil {
		t.Fatalf("rpc.discover=%s: %v", result.JSON, err)
	}
	if document.OpenRPC == "" || document.MethodsHash == "" {
		t.Fatalf("generated registry discovery missing version/hash: %+v", document)
	}
	want := map[string]bool{
		"bead.ready": true, "bead.claimNext": true, "bead.update": true,
		"bead.comment": true, "bead.close": true, "history.query": true, "tx.apply": true,
	}
	for _, method := range document.Methods {
		delete(want, method.Name)
	}
	if len(want) != 0 {
		t.Fatalf("generated registry omitted thin-slice methods: %v", want)
	}
	t.Logf("generated registry path: OpenRPC=%s methodsHash=%s methods=%d", document.OpenRPC, document.MethodsHash, len(document.Methods))
}

func TestRestoredThinSliceLifecycleAcrossMemoryAndSQLite(t *testing.T) {
	source := openThinSliceLocal(t, "memory", "", "thin-source", false)
	blocker := thinSliceCreate(t, source.Client, "thin blocker", 1)
	dependent := thinSliceCreate(t, source.Client, "thin dependent", 2)
	linkSet := model.ChangeSet{Ops: []model.Op{{
		Kind: model.OpLink,
		Link: &model.LinkOp{Kind: model.OpLink, From: dependent, To: blocker, LinkKind: "blocks"},
	}}}
	if _, err := source.Client.Call(context.Background(), "tx.apply", map[string]any{"changeSet": linkSet}, thinSliceKey(3)); err != nil {
		t.Fatalf("source link: %v", err)
	}
	archiveData, archive := captureHeadDump(t, source.Client)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			dsn := ""
			if backend == "sqlite" {
				dsn = filepath.Join(t.TempDir(), "thin-slice.db")
			}
			target := openThinSliceLocal(t, backend, dsn, "thin-worker", true)
			restoredIncarnation := restoreHead(t, target.Client, archiveData, archive)
			assertThinSliceRegistry(t, target.Client)

			ready := readyPage(t, target.Client)
			if ready.revision != "1" || ready.incarnation != restoredIncarnation || !sameSet(ready.ids, []string{blocker.ID}) {
				t.Fatalf("fresh restored ready page=%+v", ready)
			}

			claimParams := map[string]any{"filter": map[string]any{"types": []string{"issue"}}}
			claimed, claimResult := thinSliceCommit(t, target.Client, "bead.claimNext", claimParams, thinSliceKey(10))
			if claimed.Revision.String() != "2" || len(claimed.Results) != 1 || claimed.Results[0].Ref.ID != blocker.ID {
				t.Fatalf("claimNext=%+v", claimed)
			}
			claimReplay, err := target.Client.Call(context.Background(), "bead.claimNext", claimParams, thinSliceKey(10))
			if err != nil || !claimReplay.Replayed || !bytes.Equal(claimResult.JSON, claimReplay.JSON) {
				t.Fatalf("claimNext replay=%s replayed=%t err=%v", claimReplay.JSON, claimReplay.Replayed, err)
			}

			updated, _ := thinSliceCommit(t, target.Client, "bead.update", map[string]any{
				"id": map[string]any{"id": blocker.ID}, "fields": map[string]any{"title": "thin blocker updated"},
			}, thinSliceKey(11))
			if updated.Revision.String() != "3" {
				t.Fatalf("update revision=%s", updated.Revision)
			}
			commented, _ := thinSliceCommit(t, target.Client, "bead.comment", map[string]any{
				"id": map[string]any{"id": blocker.ID}, "body": "thin slice comment",
			}, thinSliceKey(12))
			if commented.Revision.String() != "4" {
				t.Fatalf("comment revision=%s", commented.Revision)
			}

			beforeGuard := readyPage(t, target.Client)
			_, err = target.Client.Call(context.Background(), "bead.close", map[string]any{
				"id": map[string]any{"id": dependent.ID}, "reason": "blocked close must refuse",
			}, thinSliceKey(13))
			requireCode(t, err, "ontology_guard_failed")
			afterGuard := readyPage(t, target.Client)
			if beforeGuard.revision != afterGuard.revision || !sameSet(beforeGuard.ids, afterGuard.ids) {
				t.Fatalf("close guard changed ready state: before=%+v after=%+v", beforeGuard, afterGuard)
			}

			history := historyPage(t, target.Client, blocker.ID, "1", restoredIncarnation)
			if len(history) != 1 || history[0].title != "thin blocker" || history[0].revision != "1" {
				t.Fatalf("restored history=%+v", history)
			}

			changeSet := model.ChangeSet{Ops: []model.Op{{
				Kind:  model.OpPatch,
				Patch: &model.PatchOp{Target: blocker, Fields: map[string]any{"description": "tx.apply after history"}},
			}}}
			applied, applyResult := thinSliceCommit(t, target.Client, "tx.apply", map[string]any{"changeSet": changeSet}, thinSliceKey(14))
			if applied.Revision.String() != "5" || len(applied.Results) != 1 {
				t.Fatalf("tx.apply=%+v", applied)
			}
			applyReplay, err := target.Client.Call(context.Background(), "tx.apply", map[string]any{"changeSet": changeSet}, thinSliceKey(14))
			if err != nil || !applyReplay.Replayed || !bytes.Equal(applyResult.JSON, applyReplay.JSON) {
				t.Fatalf("tx.apply replay=%s replayed=%t err=%v", applyReplay.JSON, applyReplay.Replayed, err)
			}

			closedBlocker, _ := thinSliceCommit(t, target.Client, "bead.close", map[string]any{
				"id": map[string]any{"id": blocker.ID}, "reason": "unblock dependent",
			}, thinSliceKey(15))
			if closedBlocker.Revision.String() != "6" {
				t.Fatalf("blocker close revision=%s", closedBlocker.Revision)
			}
			readyAfterBlocker := readyPage(t, target.Client)
			if !sameSet(readyAfterBlocker.ids, []string{dependent.ID}) {
				t.Fatalf("dependent did not become ready after blocker close: %+v", readyAfterBlocker)
			}
			closedDependent, _ := thinSliceCommit(t, target.Client, "bead.close", map[string]any{
				"id": map[string]any{"id": dependent.ID}, "reason": "thin slice complete",
			}, thinSliceKey(16))
			if closedDependent.Revision.String() != "7" {
				t.Fatalf("dependent close revision=%s", closedDependent.Revision)
			}
			if final := readyPage(t, target.Client); len(final.ids) != 0 || final.revision != "7" {
				t.Fatalf("final ready page=%+v", final)
			}
			t.Logf("thin restored lifecycle passed on %s: restore=%s claim=2 update=3 comment=4 tx.apply=5 close=6/7 warnings=replayed", backend, restoredIncarnation)
		})
	}
}

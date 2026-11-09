package gatecontract_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type b14DumpHost struct {
	local *cli.Local
	store driver.Store
}

// This is an actual Plug producer/receiving-RPC control. It neither rewrites
// an archive fixture nor establishes native Beads producer compatibility.
func TestB14PublicPlugDumpRestoreExternalWaitsForAcrossEngines(t *testing.T) {
	engines(t, func(t *testing.T, sourceEngine engine) {
		source := openB14DumpHost(t, sourceEngine)
		created := b14DumpCall(t, source, "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
			{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "named external waiter"}}},
			{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "transaction external waiter"}}},
			{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "genuine local waiter"}}},
			{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Fields: map[string]any{"title": "local spelling collision"}}},
		}}})
		refs := make([]model.Ref, 4)
		for i := range refs {
			refs[i] = model.RefByID(resultID(created, i), model.Natural64(0))
		}
		opaque := model.RefByID(" external:tracker/外部 ", model.Natural64(0))
		all := &model.LinkMetadata{Gate: "all-children"}
		anyAlso := &model.LinkMetadata{Gate: "any-children", AlsoBlocks: true}
		var ops []model.Op
		for _, to := range []model.Ref{refs[3], opaque} {
			b14DumpCall(t, source, "bead.link", map[string]any{"from": refs[0], "to": to, "kind": "waits-for", "targetKind": "external", "metadata": all})
			ops = append(ops, model.Op{Kind: model.OpLink, Link: &model.LinkOp{Kind: model.OpLink, From: refs[1], To: to, LinkKind: "waits-for", TargetKind: model.LinkTargetExternal, Metadata: anyAlso}})
		}
		b14DumpCall(t, source, "tx.apply", map[string]any{"changeSet": model.ChangeSet{Ops: ops}})
		b14DumpCall(t, source, "bead.link", map[string]any{"from": refs[2], "to": refs[3], "kind": "waits-for", "metadata": all})
		want := map[string][]pipeline.Link{
			refs[0].ID: {
				{To: refs[3], Kind: "waits-for", TargetKind: model.LinkTargetExternal, Metadata: all},
				{To: opaque, Kind: "waits-for", TargetKind: model.LinkTargetExternal, Metadata: all},
			},
			refs[1].ID: {
				{To: refs[3], Kind: "waits-for", TargetKind: model.LinkTargetExternal, Metadata: anyAlso},
				{To: opaque, Kind: "waits-for", TargetKind: model.LinkTargetExternal, Metadata: anyAlso},
			},
			refs[2].ID: {{To: refs[3], Kind: "waits-for", Metadata: all}},
		}
		assertB14DumpHost(t, source, refs, want)
		data, archive := captureB14PublicDump(t, source)
		t.Logf("actual Plug admin.dump source=%s scope=%s bytes=%d sha256=%x", sourceEngine.name, archive.Manifest().Scope, len(data), sha256.Sum256(data))
		engines(t, func(t *testing.T, targetEngine engine) {
			target := openB14DumpHost(t, targetEngine)
			restoreB14PublicDump(t, target, data, archive)
			assertB14DumpHost(t, target, refs, want)
			if err := target.local.Close(); err != nil {
				t.Fatal(err)
			}
			if targetEngine.name != "memory" {
				reopened := openB14DumpHost(t, targetEngine)
				assertB14DumpHost(t, reopened, refs, want)
				t.Logf("physical %s close/reopen retains public external metadata and local-index isolation", targetEngine.name)
			}
		})
	})
}

func openB14DumpHost(t *testing.T, e engine) b14DumpHost {
	t.Helper()
	ctx := context.Background()
	store, err := e.driver.Open(ctx, driver.Config{DSN: e.dsn})
	if err != nil {
		t.Fatal(err)
	}
	local, err := cli.OpenLocal(ctx, cli.LocalConfig{
		Project: "b14-public-dump", Agent: "b14-public-dump", Storage: cli.StorageConfig{Driver: "provided"},
		Factory: func(context.Context, cli.StorageConfig) (driver.Store, func() error, error) {
			return store, store.Close, nil
		},
		PresenceVerifier: restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if method == "" || digest == "" || p["proof"] != "b14-public-dump" {
				return fmt.Errorf("public dump/restore presence is not bound to the operation")
			}
			return nil
		}),
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := local.Close(); err != nil {
			t.Error(err)
		}
	})
	return b14DumpHost{local: local, store: store}
}

func b14DumpCall(t *testing.T, host b14DumpHost, method string, params map[string]any) map[string]any {
	t.Helper()
	reply, err := host.local.Client.Call(context.Background(), method, params, "")
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	value, err := wire.DecodeJSON(reply.JSON)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s returned %T", method, value)
	}
	return object
}

func assertB14DumpHost(t *testing.T, host b14DumpHost, refs []model.Ref, want map[string][]pipeline.Link) {
	t.Helper()
	ctx := context.Background()
	meta, err := host.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for id, links := range want {
		bead := b14DumpCall(t, host, "bead.get", map[string]any{"id": ref(id)})["bead"].(map[string]any)
		body := b14DumpCall(t, host, "history.get", map[string]any{"ref": ref(id), "asOf": map[string]any{"revision": bead["revision"]}})["body"].(map[string]any)
		gotJSON, err := json.Marshal(body["links"])
		if err != nil {
			t.Fatal(err)
		}
		wantJSON, err := json.Marshal(links)
		if err != nil {
			t.Fatal(err)
		}
		wantValue, err := wire.DecodeJSON(wantJSON)
		if err != nil || !reflect.DeepEqual(body["links"], wantValue) {
			t.Fatalf("public links %s: got %s want %s err=%v", id, gotJSON, wantJSON, err)
		}
	}
	ready := b14DumpCall(t, host, "bead.ready", map[string]any{"limit": 100})
	ids := map[string]bool{}
	for _, item := range ready["items"].([]any) {
		ids[item.(map[string]any)["id"].(string)] = true
	}
	if ids[refs[0].ID] || ids[refs[1].ID] || !ids[refs[2].ID] {
		t.Fatalf("public readiness bound an external edge locally or lost the local positive: %v", ids)
	}
	r, err := host.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	inverse, ok := r.(driver.InverseSourceLinksReader)
	if !ok {
		t.Fatal("restored store lacks the inverse-source-link capability")
	}
	rows, more, err := inverse.InverseSourceLinks(ctx, []model.Ref{refs[3]}, []driver.IncomingLinkKind{driver.IncomingLinkWaitsFor}, nil, 10)
	if err != nil || more || len(rows) != 1 {
		t.Fatalf("external waits-for entered local index: rows=%+v more=%v err=%v", rows, more, err)
	}
	local, err := r.Get(ctx, []model.Ref{refs[2]})
	if err != nil || len(local) != 1 || !bytes.Equal(rows[0].Body, local[0].Body) {
		t.Fatalf("inverse index lost the genuine local waiter: %+v err=%v", rows, err)
	}
	after, err := host.store.Meta(ctx)
	if err != nil || after.Head.String() != meta.Head.String() {
		t.Fatalf("public readback changed head: before=%+v after=%+v err=%v", meta, after, err)
	}
}

func b14DumpPresence() map[string]any {
	return map[string]any{"mode": "presence_check", "challenge": "b14-public-dump", "proof": "b14-public-dump"}
}

func captureB14PublicDump(t *testing.T, source b14DumpHost) ([]byte, *dumpformat.Archive) {
	t.Helper()
	var data bytes.Buffer
	cursor, dumpID := "", ""
	for {
		params := map[string]any{"history": "head", "pageSize": 512, "humanPresence": b14DumpPresence()}
		if cursor != "" {
			params["cursor"] = cursor
		}
		page := b14DumpCall(t, source, "admin.dump", params)
		id := page["dumpId"].(string)
		if dumpID != "" && dumpID != id {
			t.Fatal("actual public dump changed its pinned ID")
		}
		dumpID = id
		chunk, err := base64.StdEncoding.DecodeString(page["data"].(string))
		if err != nil {
			t.Fatal(err)
		}
		data.Write(chunk)
		if page["done"].(bool) {
			break
		}
		cursor = fmt.Sprintf("%s:%s", dumpID, page["nextPage"].(json.Number))
	}
	archive, err := dumpformat.Read(bytes.NewReader(data.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if archive.Manifest().Scope != "plug-native-store" {
		t.Fatalf("actual public dump has scope %s", archive.Manifest().Scope)
	}
	return data.Bytes(), archive
}

func restoreB14PublicDump(t *testing.T, target b14DumpHost, data []byte, archive *dumpformat.Archive) {
	t.Helper()
	manifest, ok := archive.Member("manifest.json")
	if !ok {
		t.Fatal("actual public dump omitted manifest")
	}
	value, err := wire.DecodeJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	begin := b14DumpCall(t, target, "admin.restore.begin", map[string]any{"manifest": value, "history": "head", "humanPresence": b14DumpPresence()})
	id := begin["restoreId"].(string)
	for page, offset := uint64(0), 0; offset < len(data); page++ {
		end := min(offset+512, len(data))
		chunk := data[offset:end]
		sum := sha256.Sum256(chunk)
		b14DumpCall(t, target, "admin.restore.chunk", map[string]any{"restoreId": id, "page": page, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": hex.EncodeToString(sum[:]), "humanPresence": b14DumpPresence()})
		offset = end
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	b14DumpCall(t, target, "admin.restore.finish", map[string]any{"restoreId": id, "manifestSha256": hex.EncodeToString(sum[:]), "humanPresence": b14DumpPresence()})
}

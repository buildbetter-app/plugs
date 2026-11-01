package epicprogress_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var presence = map[string]any{"mode": "presence_check", "challenge": "ep6-disposable-challenge", "proof": "ep6-test-verifier"}

// Admin is a separate, admitted ANONYMOUS LOOPBACK endpoint over the borrowed
// store/lease. An API-key User is never promoted to an interactive admin. The
// verifier is a fixture-only D32 authority, not a production presence claim.
func admin(t *testing.T, store driver.Store, lease driver.ActiveLease) *endpoint {
	t.Helper()
	ctx := context.Background()
	meta, e := store.Meta(ctx)
	if e != nil {
		t.Fatal(e)
	}
	verifier := restore.PresenceVerifierFunc(func(_ context.Context, method, digest string, p restore.Presence) error {
		if method == "" || digest == "" || p["challenge"] != presence["challenge"] || p["proof"] != presence["proof"] {
			return fmt.Errorf("ep6 test presence binding refused")
		}
		return nil
	})
	registry, e := rpc.NewRegistry()
	if e != nil {
		t.Fatal(e)
	}
	if e = rpc.BindAdmin(registry, restore.NewManager(store, lease, verifier)); e != nil {
		t.Fatal(e)
	}
	server, e := rpc.New(registry, rpc.Config{Incarnation: meta.Incarnation, DefaultProject: epics, AuthMode: "anonymous", CurrentIncarnation: func(ctx context.Context) (string, error) { m, e := store.Meta(ctx); return m.Incarnation, e }})
	if e != nil {
		t.Fatal(e)
	}
	admission, e := authn.New(authn.Config{})
	if e != nil {
		t.Fatal(e)
	}
	handler, e := httpjson.New(server, admission, httpjson.Config{})
	if e != nil {
		t.Fatal(e)
	}
	return listen(t, handler)
}

func dumpHead(t *testing.T, e *endpoint) ([]byte, *dumpformat.Archive) {
	t.Helper()
	var data bytes.Buffer
	cursor, dumpID := "", ""
	for page := 0; page < 4096; page++ {
		params := map[string]any{"history": "head", "pageSize": 1024, "humanPresence": presence}
		if cursor != "" {
			params["cursor"] = cursor
		}
		value := result(t, e.call(t, "", "admin.dump", params))
		id, ok := value["dumpId"].(string)
		if !ok || id == "" {
			t.Fatal("dump has no identity")
		}
		if dumpID != "" && id != dumpID {
			t.Fatal("dump changed identity while paging")
		}
		dumpID = id
		chunk, err := base64.StdEncoding.DecodeString(value["data"].(string))
		if err != nil {
			t.Fatal(err)
		}
		data.Write(chunk)
		if value["done"] == true {
			archive, err := dumpformat.Read(bytes.NewReader(data.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			return data.Bytes(), archive
		}
		cursor = dumpID + ":" + value["nextPage"].(json.Number).String()
	}
	t.Fatal("dump did not finish within the admitted page bound")
	return nil, nil
}

func restoreHead(t *testing.T, e *endpoint, data []byte, archive *dumpformat.Archive) string {
	t.Helper()
	raw, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err = d.Decode(&manifest); err != nil {
		t.Fatal(err)
	}
	meta := func(n int) map[string]any {
		return map[string]any{"project": epics, "idempotencyKey": fmt.Sprintf("ep6-restore-%032x", n)}
	}
	begin := result(t, e.call(t, "", "admin.restore.begin", map[string]any{"manifest": manifest, "history": "head", "humanPresence": presence, "_meta": meta(0)}))
	id := begin["restoreId"].(string)
	for page, offset := 0, 0; offset < len(data); page++ {
		end := offset + 1024
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		sum := sha256.Sum256(chunk)
		result(t, e.call(t, "", "admin.restore.chunk", map[string]any{"restoreId": id, "page": page, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": hex.EncodeToString(sum[:]), "humanPresence": presence, "_meta": meta(page + 1)}))
		offset = end
	}
	sum := sha256.Sum256(raw)
	finish := result(t, e.call(t, "", "admin.restore.finish", map[string]any{"restoreId": id, "manifestSha256": hex.EncodeToString(sum[:]), "humanPresence": presence, "_meta": meta(1000000)}))
	if finish["state"] != "complete" {
		t.Fatalf("restore incomplete: %v", finish)
	}
	incarnation, ok := finish["incarnation"].(string)
	if !ok || incarnation == "" {
		t.Fatal("restore omitted the new incarnation")
	}
	return incarnation
}

func TestEpicProgressSurvivesNativeRestoreAndSQLiteReopen(t *testing.T) {
	dir := t.TempDir()
	source := newFixture(t, "sqlite", filepath.Join(dir, "source.db"), true)
	g := source.graph(t)
	viewer := source.viewer(t, epics, visible)
	before := inspectProgress(t, source.api.call(t, viewer, "view.run", progressParams(g.parent, epics)), map[string]int{"total": 2, "status:todo": 1, "status:done": 1}, 1)
	sourceAdmin := admin(t, source.store, source.lease)
	data, archive := dumpHead(t, sourceAdmin)
	sourceAdmin.close(t)
	source.close(t)
	if archive.Manifest().History != "head" {
		t.Fatal("fixture silently requested another history mode")
	}
	path := filepath.Join(dir, "restored.db")
	target, lease := openStore(t, "sqlite", path)
	// Do not construct authorization facts or seed ontology in the EMPTY target.
	// Restore must establish all source state through its admitted native path.
	targetAdmin := admin(t, target, lease)
	incarnation := restoreHead(t, targetAdmin, data, archive)
	targetAdmin.close(t)
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if incarnation == before.asOf["incarnation"] {
		t.Fatal("restore retained the source incarnation")
	}
	// A fresh host/checker/key ring over the reopened physical SQLite file is
	// essential: no in-memory parent/count/authority cache may satisfy this read.
	reopened := newFixture(t, "sqlite", path, true)
	read := reopened.viewer(t, epics, visible)
	after := inspectProgress(t, reopened.api.call(t, read, "view.run", progressParams(g.parent, epics)), before.counts, 1)
	if after.asOf["incarnation"] != incarnation {
		t.Fatal("reopened service lost restored incarnation")
	}
	old := progressParams(g.parent, epics)
	old["asOf"] = before.asOf
	requireCode(t, reopened.api.call(t, read, "view.run", old), "incarnation_changed")
	// Restored hierarchy must still participate in actual future graph writes:
	// a descendant cycle refuses atomically, then a valid detach updates counts.
	head := reopened.head(t)
	cycle := model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: g.parent, Parent: &g.grandchild}}}}
	requireCode(t, reopened.write(t, reopened.owner, "tx.apply", epics, map[string]any{"changeSet": cycle}), "ontology_guard_failed")
	if reopened.head(t) != head {
		t.Fatal("restored cycle refusal changed head")
	}
	detach := model.ChangeSet{Ops: []model.Op{{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: g.done}}}}
	committed(t, reopened.write(t, reopened.owner, "tx.apply", visible, map[string]any{"changeSet": detach}))
	inspectProgress(t, reopened.api.call(t, read, "view.run", after.refresh), map[string]int{"total": 1, "status:todo": 1}, 1)
	t.Logf("EP6 native head restore, physical SQLite close/reopen, current policy, cycle refusal, and count refresh proven at restored incarnation %s", incarnation)
}

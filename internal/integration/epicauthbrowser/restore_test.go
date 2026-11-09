package epicauthbrowser_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
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
		params := map[string]any{"history": "head", "pageSize": 65536, "humanPresence": presence}
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
	// Keep the public envelope unchanged while retaining the exact failed
	// phase in test.log before result's caller-level fatal attribution.
	call := func(phase, method string, params map[string]any) map[string]any {
		t.Helper()
		t.Logf("NATIVE_RESTORE_RPC phase=%s boundary=begin method=%s", phase, method)
		reply := e.call(t, "", method, params)
		t.Logf("NATIVE_RESTORE_RPC phase=%s boundary=returned method=%s http=%d reply=%s", phase, method, reply.status, reply.raw)
		return result(t, reply)
	}
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
	begin := call("begin", "admin.restore.begin", map[string]any{"manifest": manifest, "history": "head", "humanPresence": presence, "_meta": meta(0)})
	id := begin["restoreId"].(string)
	for page, offset := 0, 0; offset < len(data); page++ {
		end := offset + 65536
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		sum := sha256.Sum256(chunk)
		call(fmt.Sprintf("chunk/%d", page), "admin.restore.chunk", map[string]any{"restoreId": id, "page": page, "data": base64.StdEncoding.EncodeToString(chunk), "sha256": hex.EncodeToString(sum[:]), "humanPresence": presence, "_meta": meta(page + 1)})
		offset = end
	}
	sum := sha256.Sum256(raw)
	finish := call("finish", "admin.restore.finish", map[string]any{"restoreId": id, "manifestSha256": hex.EncodeToString(sum[:]), "humanPresence": presence, "_meta": meta(1000000)})
	if finish["state"] != "complete" {
		t.Fatalf("restore incomplete: %v", finish)
	}
	incarnation, ok := finish["incarnation"].(string)
	if !ok || incarnation == "" {
		t.Fatal("restore omitted the new incarnation")
	}
	return incarnation
}

// Derived hierarchy state is part of a usable restore: count reads can succeed
// from source bodies even when the next real Writer refuses missing closure.
// Compare every canonical key/value before host composition and after reopen;
// the native restore owner must rebuild it atomically, never this fixture.
func persistedClosure(t *testing.T, store driver.Store) map[string]string {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	images := make(map[string]string, len(rows))
	for _, row := range rows {
		if _, _, _, err := pipeline.DecodeAncestorRow(row); err != nil {
			t.Fatalf("persisted hierarchy closure is not canonical: %v", err)
		}
		if _, exists := images[row.Key]; exists {
			t.Fatal("duplicate persisted hierarchy closure key")
		}
		images[row.Key] = hex.EncodeToString(row.Value)
	}
	return images
}

// Enumerate both the complete bead census and the exact protected ontology
// before any fixture construction can seed or normalize a restored store.
func persistedImages(t *testing.T, store driver.Store) map[string]any {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out := map[string]any{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range page.Items {
			if _, exists := out[row.Ref.ID]; exists {
				t.Fatal("duplicate persisted source identity")
			}
			var value map[string]any
			d := json.NewDecoder(bytes.NewReader(row.Body))
			d.UseNumber()
			if err := d.Decode(&value); err != nil {
				t.Fatal(err)
			}
			out[row.Ref.ID] = value
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	rows, err := r.Get(ctx, []model.Ref{ref("ontology/ep6")})
	if err != nil || len(rows) != 1 || rows[0].Kind != "ontology" {
		t.Fatalf("protected source missing from persisted census: %v", err)
	}
	var ontology map[string]any
	d := json.NewDecoder(bytes.NewReader(rows[0].Body))
	d.UseNumber()
	if err := d.Decode(&ontology); err != nil {
		t.Fatal(err)
	}
	out["ontology/ep6"] = ontology
	return out
}

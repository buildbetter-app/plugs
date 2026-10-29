package httpjson_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestAuthenticatedLoopbackLifecycle(t *testing.T) {
	f := newFixture(t, true, true, httpjson.Config{})
	params := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "HTTP lifecycle", "priority": 1}})
	createdResponse := f.call(t, "bead.create", params)
	created := result(t, createdResponse)
	ref := createdRef(t, created)
	warnings := created["warnings"].([]any)
	if len(warnings) != 1 || warnings[0].(map[string]any)["code"] != "post_subscriber_failed" {
		t.Fatal("committed post warning missing from HTTP result")
	}
	replayedResponse := f.call(t, "bead.create", params)
	replayed := result(t, replayedResponse)
	if !reflect.DeepEqual(created, replayed) || f.post.calls.Load() != 1 {
		t.Fatal("HTTP retry changed durable result or repeated post subscriber")
	}
	got := result(t, f.call(t, "bead.get", map[string]any{"id": ref}))
	if got["bead"].(map[string]any)["fields"].(map[string]any)["title"] != "HTTP lifecycle" {
		t.Fatal("HTTP get did not read persisted create")
	}
	claim := result(t, f.call(t, "bead.claim", writeParams(t, map[string]any{"id": ref})))
	if claim["leaseTtlMs"] != json.Number("300000") || claim["leaseExpiresAt"] == nil {
		t.Fatal("HTTP claim lost durable lease values")
	}
	got = result(t, f.call(t, "bead.get", map[string]any{"id": ref}))
	lease := got["lease"].(map[string]any)
	if lease["holder"] != f.principal.ID || lease["epoch"] != json.Number(model.Natural64(f.lease.Epoch()).String()) {
		t.Fatal("bearer principal was not the durable lease holder")
	}
	if lease["agent"] != nil {
		t.Fatal("authenticated lease trusted a client agent label")
	}
	comment := result(t, f.call(t, "bead.comment", writeParams(t, map[string]any{"id": ref, "body": "HTTP evidence"})))
	closed := result(t, f.call(t, "bead.close", writeParams(t, map[string]any{"id": ref, "reason": "Verified"})))
	got = result(t, f.call(t, "bead.get", map[string]any{"id": ref}))
	bead := got["bead"].(map[string]any)
	if bead["revision"] != closed["revision"] || bead["status"] != "done" {
		t.Fatalf("close did not persist: %v", bead)
	}
	ready := result(t, f.call(t, "bead.ready", map[string]any{"limit": 10}))
	if len(ready["items"].([]any)) != 0 {
		t.Fatal("closed bead remained ready")
	}
	history := result(t, f.call(t, "history.query", map[string]any{"asOf": map[string]any{"revision": created["revision"]}, "limit": 10}))
	if history["items"].([]any)[0].(map[string]any)["status"] != "todo" {
		t.Fatal("HTTP history did not preserve pre-claim state")
	}
	// Check actual append-only storage as well as the exposed RPC slice.
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err := reader.Get(context.Background(), []model.Ref{model.RefByID(ref["id"].(string), model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatal("missing persisted bead", err)
	}
	var doc struct {
		Comments []struct {
			Body string `json:"body"`
		} `json:"comments"`
	}
	if err := json.Unmarshal(rows[0].Body, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Comments) != 2 || doc.Comments[0].Body != "HTTP evidence" || doc.Comments[1].Body != "Verified" {
		t.Fatal("HTTP comment/close evidence did not reach storage")
	}
	changes, err := reader.Changesets(context.Background(), nil, 10)
	if err != nil || len(changes) != 4 {
		t.Fatalf("changesets=%d err=%v", len(changes), err)
	}
	for _, change := range changes {
		if !reflect.DeepEqual(change.Principal, f.principal) {
			t.Fatal("changeset trusted client identity")
		}
	}
	if changes[2].Revision.String() != comment["revision"].(json.Number).String() {
		t.Fatal("comment revision missing from log")
	}
	beforeReads, beforeWrites := f.store.reads.Load(), f.store.writes.Load()
	spoof := writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "spoof"}})
	spoof["_meta"].(map[string]any)["principal"] = map[string]any{"kind": "user", "id": "user:admin"}
	requireCode(t, f.call(t, "bead.create", spoof), http.StatusOK, "invalid_params")
	if f.store.reads.Load() != beforeReads || f.store.writes.Load() != beforeWrites {
		t.Fatal("spoofed identity reached storage")
	}
}

func TestHTTPAndInprocessByteParity(t *testing.T) {
	f := newFixture(t, true, true, httpjson.Config{})
	created := result(t, f.call(t, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "parity"}})))
	ref := createdRef(t, created)
	identity, err := f.auth.Admit(context.Background(), f.token)
	if err != nil {
		t.Fatal(err)
	}
	ctx := rpc.WithPrincipal(context.Background(), identity.Principal())
	local := inprocess.New(f.rpc)
	for _, rawID := range []string{`900719925474099312345678901`, `-123456789012345678901`, `9.007199254740993e+30`, `null`, `"escaped\u002did"`} {
		t.Run(rawID, func(t *testing.T) {
			data := message(t, "bead.get", map[string]any{"id": ref}, json.RawMessage(rawID))
			want, err := local.RoundTrip(ctx, data)
			if err != nil {
				t.Fatal(err)
			}
			got := f.send(t, f.request(t, data))
			if got.status != http.StatusOK || !bytes.Equal(got.body, want) {
				t.Fatal("HTTP changed dispatcher bytes")
			}
			if !bytes.Contains(got.body, []byte(`"id":`+rawID+`,`)) {
				t.Fatal("HTTP lost raw JSON ID representation")
			}
		})
	}
	batch := append([]byte{'['}, message(t, "bead.get", map[string]any{"id": ref}, json.RawMessage(`9007199254740993123`))...)
	batch = append(batch, ',')
	batch = append(batch, message(t, "bead.ready", map[string]any{}, nil)...)
	batch = append(batch, ',')
	batch = append(batch, message(t, "bead.get", map[string]any{"id": ref}, json.RawMessage(`-1.2345e+20`))...)
	batch = append(batch, ']')
	want, err := local.RoundTrip(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	got := f.send(t, f.request(t, batch))
	if !bytes.Equal(got.body, want) {
		t.Fatal("HTTP changed ordered batch bytes")
	}
	// Generated trace IDs differ across independent dispatches. Only those
	// trace values are removed for error parity; codes, IDs and details match.
	for _, data := range [][]byte{[]byte(`{`), []byte(`[]`), []byte(`{"jsonrpc":"2.0","method":"unsupported.method","id":9007199254740993}`), message(t, "bead.get", map[string]any{"id": map[string]any{"id": "missing", "branch": 0}}, json.RawMessage(`7e+30`))} {
		want, err := local.RoundTrip(ctx, data)
		if err != nil {
			t.Fatal(err)
		}
		got := f.send(t, f.request(t, data))
		if got.status != http.StatusOK {
			t.Fatal("RPC error changed HTTP status")
		}
		left, right := decoded(t, want), decoded(t, got.body)
		for _, v := range []map[string]any{left, right} {
			data := v["error"].(map[string]any)["data"].(map[string]any)
			if data["traceId"] == "" {
				t.Fatal("missing generated trace")
			}
			delete(data, "traceId")
		}
		if !reflect.DeepEqual(left, right) {
			t.Fatal("HTTP changed RPC error semantics")
		}
	}
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Error *wire.Error `json:"error"`
	}
	bad := f.call(t, "bead.get", map[string]any{})
	if err := json.Unmarshal(bad.body, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error == nil || catalog.ValidateError(envelope.Error) != nil {
		t.Fatal("RPC error escaped shared wire catalog")
	}
}

func TestNotificationsDoNotWriteAndReturnNoContent(t *testing.T) {
	f := newFixture(t, false, false, httpjson.Config{})
	create := message(t, "bead.create", writeParams(t, map[string]any{"type": "issue", "fields": map[string]any{"title": "must not exist"}}), nil)
	read := message(t, "bead.ready", map[string]any{}, nil)
	batch := append([]byte{'['}, create...)
	batch = append(batch, ',')
	batch = append(batch, read...)
	batch = append(batch, ']')
	for _, data := range [][]byte{create, read, batch} {
		got := f.send(t, f.request(t, data))
		if got.status != http.StatusNoContent || len(got.body) != 0 || got.header.Get("Content-Type") != "" {
			t.Fatal("notification was not HTTP 204 without content")
		}
	}
	if f.domain.creates.Load() != 0 || f.store.writes.Load() != 0 {
		t.Fatal("write notification reached domain/storage")
	}
	ready := result(t, f.call(t, "bead.ready", map[string]any{}))
	if len(ready["items"].([]any)) != 0 {
		t.Fatal("notification wrote a bead")
	}
}

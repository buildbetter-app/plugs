package gatecontract_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type fixture struct {
	t                *testing.T
	engine, path     string
	driver           driver.Driver
	lastResponse     map[string]any
	store            driver.Store
	lease            driver.ActiveLease
	server           *rpc.Server
	sequence, nextID int
	lastKey          string
}

func eachStore(t *testing.T, run func(*testing.T, *fixture)) {
	t.Helper()
	engines(t, func(t *testing.T, e engine) {
		f := &fixture{t: t, engine: e.name, path: e.dsn, driver: e.driver}
		f.open()
		t.Cleanup(f.close)
		run(t, f)
	})
}

func (f *fixture) open() {
	f.t.Helper()
	var err error
	f.store, err = f.driver.Open(context.Background(), driver.Config{DSN: f.path})
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease, err = f.store.Lease(context.Background(), "gate-contract", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	// The built-in ontology and real named service are the public host path.
	// No test-only schema compilation, record seed or visibility bypass.
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
		f.nextID++
		return fmt.Sprintf("gc-%06d", f.nextID), nil
	}))
	domain := service.New(f.store, f.lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err = rpc.BindService(registry, domain); err != nil {
		f.t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: "local", Incarnation: f.meta().Incarnation, CursorKey: []byte("gate-contract-cursor-key-00000001")})
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) close() {
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			f.t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) meta() driver.Meta {
	f.t.Helper()
	m, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	return m
}

func (f *fixture) call(method string, params map[string]any, write bool, code string) map[string]any {
	f.t.Helper()
	f.sequence++
	key := ""
	if write {
		key = fmt.Sprintf("%032x", f.sequence)
	}
	return f.request(method, params, key, code)
}

func (f *fixture) request(method string, params map[string]any, key, code string) map[string]any {
	f.t.Helper()
	f.lastKey = key
	copyParams := make(map[string]any, len(params)+1)
	for k, v := range params {
		copyParams[k] = v
	}
	if id, ok := copyParams["id"].(string); ok && strings.HasPrefix(method, "bead.") {
		copyParams["id"] = map[string]any{"id": id}
	}
	if key != "" {
		meta := map[string]any{"agent": "gate-contract", "idempotencyKey": key}
		if supplied, ok := copyParams["_meta"].(map[string]any); ok {
			for k, v := range supplied {
				meta[k] = v
			}
		}
		copyParams["_meta"] = meta
	}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": f.sequence, "method": method, "params": copyParams})
	if err != nil {
		f.t.Fatal(err)
	}
	raw, err = inprocess.New(f.server).RoundTrip(context.Background(), raw)
	if err != nil {
		f.t.Fatal(err)
	}
	var response map[string]any
	if err = json.Unmarshal(raw, &response); err != nil {
		f.t.Fatal(err)
	}
	f.lastResponse = response
	if code != "" {
		e, ok := response["error"].(map[string]any)
		if !ok {
			f.t.Fatalf("%s wanted %s: %s", method, code, raw)
		}
		data, _ := e["data"].(map[string]any)
		if data["code"] != code || response["result"] != nil {
			f.t.Fatalf("%s wanted typed %s without result: %s", method, code, raw)
		}
		return response
	}
	if response["error"] != nil {
		f.t.Fatalf("%s: %s", method, raw)
	}
	result, ok := response["result"].(map[string]any)
	if !ok {
		f.t.Fatalf("%s has no result: %s", method, raw)
	}
	return result
}

func (f *fixture) create(fields map[string]any, parent string) (string, any) {
	f.t.Helper()
	p := map[string]any{"type": "issue", "fields": fields}
	if parent != "" {
		p["parent"] = map[string]any{"id": parent}
	}
	commit := f.call("bead.create", p, true, "")
	return resultID(commit, 0), commit["revision"]
}

func resultID(commit map[string]any, i int) string {
	return commit["results"].([]any)[i].(map[string]any)["ref"].(map[string]any)["id"].(string)
}

func (f *fixture) bead(id string) map[string]any {
	f.t.Helper()
	return f.call("bead.get", map[string]any{"id": id}, false, "")["bead"].(map[string]any)
}

func (f *fixture) refuse(method string, params map[string]any, code string) {
	f.t.Helper()
	before := f.meta()
	f.call(method, params, true, code)
	if after := f.meta(); !reflect.DeepEqual(before, after) {
		f.t.Fatalf("refusal wrote metadata: %+v -> %+v", before, after)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: before.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		f.t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(context.Background(), principal, "gate-contract", method, f.lastKey)
	if err != nil || receipt != nil {
		f.t.Fatalf("refusal left receipt: %+v %v", receipt, err)
	}
}

func expect(id string, revision any) []any {
	return []any{map[string]any{"ref": map[string]any{"id": id}, "revision": revision}}
}

func ref(id string) map[string]any { return map[string]any{"id": id} }

func (f *fixture) body(id string, revision any) map[string]any {
	f.t.Helper()
	return f.call("history.get", map[string]any{"ref": ref(id), "asOf": map[string]any{"revision": revision}}, false, "")["body"].(map[string]any)
}

func (f *fixture) currentBody(id string) map[string]any {
	return f.body(id, f.bead(id)["revision"])
}

func (f *fixture) historyCount() int {
	f.t.Helper()
	m := f.meta()
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: m.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	entries, err := r.Changesets(context.Background(), nil, 1000)
	if err != nil {
		f.t.Fatal(err)
	}
	return len(entries)
}

func (f *fixture) ready(id string, want bool) {
	f.t.Helper()
	result := f.call("bead.ready", map[string]any{"limit": 1000, "filter": map[string]any{"resources": []string{id}}}, false, "")
	items := result["items"].([]any)
	found := false
	for _, v := range items {
		if v.(map[string]any)["id"] == id {
			found = true
		}
	}
	if found != want {
		f.t.Fatalf("bead.ready(%s)=%v want %v: %v", id, found, want, result)
	}
}

func (f *fixture) link(from, to string, metadata any, include bool) map[string]any {
	p := map[string]any{"from": ref(from), "to": ref(to), "kind": "waits-for"}
	if include {
		p["metadata"] = metadata
	}
	return f.call("bead.link", p, true, "")
}

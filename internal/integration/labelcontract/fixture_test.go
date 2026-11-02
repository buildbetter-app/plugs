package labelcontract_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type fixture struct {
	t                *testing.T
	engine, path     string
	store            driver.Store
	lease            driver.ActiveLease
	server           *rpc.Server
	sequence, nextID int
	lastKey          string
}

func eachStore(t *testing.T, run func(*testing.T, *fixture)) {
	t.Helper()
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := &fixture{t: t, engine: engine, path: filepath.Join(t.TempDir(), "labels.db")}
			f.open()
			t.Cleanup(f.close)
			run(t, f)
		})
	}
}

func (f *fixture) open() {
	f.t.Helper()
	var err error
	if f.engine == "memory" {
		f.store = memory.New()
	} else {
		f.store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: f.path})
		if err != nil {
			f.t.Fatal(err)
		}
	}
	f.lease, err = f.store.Lease(context.Background(), "label-contract", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	// The built-in ontology and real named service are the public host path.
	// No test-only schema compilation, record seed or visibility bypass.
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
		f.nextID++
		return fmt.Sprintf("lc-%06d", f.nextID), nil
	}))
	domain := service.New(f.store, f.lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err = rpc.BindService(registry, domain); err != nil {
		f.t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: "local", Incarnation: f.meta().Incarnation, CursorKey: []byte("label-contract-cursor-key-00000001")})
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
	if key != "" {
		copyParams["_meta"] = map[string]any{"agent": "label-contract", "idempotencyKey": key}
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

func stringsValue(t *testing.T, raw any) []string {
	t.Helper()
	result := []string{}
	if raw == nil {
		return result
	}
	values, ok := raw.([]any)
	if !ok {
		t.Fatalf("labels are not a JSON array: %v", raw)
	}
	for _, v := range values {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("non-string label %v", v)
		}
		result = append(result, s)
	}
	return result
}

func (f *fixture) labels(id string, want ...string) {
	f.t.Helper()
	bead := f.bead(id)
	if want == nil {
		want = []string{}
	}
	for _, raw := range []any{bead["labels"], bead["fields"].(map[string]any)["labels"]} {
		if got := stringsValue(f.t, raw); !reflect.DeepEqual(got, want) {
			f.t.Fatalf("public labels for %s: %v want %v; bead=%v", id, got, want, bead)
		}
	}
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
	receipt, err := r.IdempotencyByKey(context.Background(), principal, "label-contract", method, f.lastKey)
	if err != nil || receipt != nil {
		f.t.Fatalf("refusal left receipt: %+v %v", receipt, err)
	}
}

func patch(id string, labels any) map[string]any {
	return map[string]any{"kind": "patch", "target": map[string]any{"id": id}, "fields": map[string]any{"labels": labels}}
}

func expect(id string, revision any) []any {
	return []any{map[string]any{"ref": map[string]any{"id": id}, "revision": revision}}
}

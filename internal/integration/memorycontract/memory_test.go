package memorycontract_test

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
	t        *testing.T
	engine   string
	path     string
	store    driver.Store
	lease    driver.ActiveLease
	server   *rpc.Server
	nextID   int
	request  int
	readable bool
	policy   []service.Memory
}

func newFixture(t *testing.T, engine string) *fixture {
	t.Helper()
	f := &fixture{t: t, engine: engine, path: filepath.Join(t.TempDir(), "memory.db"), readable: true}
	f.open()
	t.Cleanup(f.close)
	return f
}

func (f *fixture) open() {
	f.t.Helper()
	ctx := context.Background()
	var err error
	if f.engine == "memory" {
		f.store = memory.New()
	} else {
		f.store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: f.path})
		if err != nil {
			f.t.Fatal(err)
		}
	}
	f.lease, err = f.store.Lease(ctx, "memory-contract", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithIDGenerator(func() (string, error) {
		f.nextID++
		return fmt.Sprintf("mc-%06d", f.nextID), nil
	}))
	domain := service.New(f.store, f.lease, writer, service.WithOntology(ontology),
		service.WithMemoryReadAuthorizer(service.MemoryReadAuthorizerFunc(func(_ context.Context, _ model.PrincipalRef, value service.Memory) (bool, error) {
			f.policy = append(f.policy, value)
			return f.readable, nil
		})))
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		f.t.Fatal(err)
	}
	meta := f.meta()
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: "local", Incarnation: meta.Incarnation, CursorKey: []byte("memory-contract-cursor-key-0000001")})
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
	v, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	return v
}

func (f *fixture) call(method string, params map[string]any, write bool, wantCode string) map[string]any {
	f.t.Helper()
	f.request++
	copyParams := make(map[string]any, len(params)+1)
	for k, v := range params {
		copyParams[k] = v
	}
	if write {
		copyParams["_meta"] = map[string]any{"agent": "memory-contract", "idempotencyKey": fmt.Sprintf("%032x", f.request)}
	}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": f.request, "method": method, "params": copyParams})
	if err != nil {
		f.t.Fatal(err)
	}
	raw, err = inprocess.New(f.server).RoundTrip(context.Background(), raw)
	if err != nil {
		f.t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(raw, &response); err != nil {
		f.t.Fatal(err)
	}
	if wantCode != "" {
		e, ok := response["error"].(map[string]any)
		if !ok {
			f.t.Fatalf("%s wanted %s: %s", method, wantCode, raw)
		}
		data, _ := e["data"].(map[string]any)
		if data["code"] != wantCode || response["result"] != nil {
			f.t.Fatalf("%s wanted typed %s without a result: %s", method, wantCode, raw)
		}
		return response
	}
	if response["error"] != nil {
		f.t.Fatalf("%s: %s", method, raw)
	}
	result, ok := response["result"].(map[string]any)
	if !ok {
		f.t.Fatalf("%s returned no object result: %s", method, raw)
	}
	return result
}

func (f *fixture) save(params map[string]any) (string, any) {
	f.t.Helper()
	commit := f.call("memory.save", params, true, "")
	rows, ok := commit["results"].([]any)
	if !ok || len(rows) != 1 {
		f.t.Fatalf("memory save results: %v", commit)
	}
	ref := rows[0].(map[string]any)["ref"].(map[string]any)
	return ref["id"].(string), commit["revision"]
}

func (f *fixture) refuseWrite(method string, params map[string]any, code string) {
	f.t.Helper()
	before := f.meta()
	f.call(method, params, true, code)
	after := f.meta()
	if !reflect.DeepEqual(before, after) {
		f.t.Fatalf("refusal changed store metadata: before=%+v after=%+v", before, after)
	}
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: after.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		f.t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(context.Background(), principal, "memory-contract", method, fmt.Sprintf("%032x", f.request))
	if err != nil || receipt != nil {
		f.t.Fatalf("refused write left a receipt: %+v %v", receipt, err)
	}
}

// These vectors are the unchanged source-pinned Beads golden rows, not
// recordings made by this package: 9d6fff760b7a314b476539198504d77ce0959656,
// internal/memoryapi/memoryapi_test.go:TestDeriveKeyGoldenTable.
func TestB13PublicMemorySaveDerivesOmittedKey(t *testing.T) {
	vectors := []struct{ name, content, key string }{
		{"help", "always run tests with -race flag", "always-run-tests-with-race-flag"},
		{"case", "UPPER CASE SHOUTING", "upper-case-shouting"},
		{"whitespace", "a  b\tc\nd", "a-b-c-d"},
		{"dots", "v1.2.3 release notes", "v1-2-3-release-notes"},
		{"underscore", "snake_case_words here", "snake-case-words-here"},
		{"eight-segments", "one two three four five six seven eight", "one-two-three-four-five-six-seven-eight"},
		{"ninth-segment", "one two three four five six seven eight nine", "one-two-three-four-five-six-seven-eight"},
		{"sixty-bytes", "supercalifragilisticexpialidocious antidisestablishmentarianism pneumonoultramicroscopic", "supercalifragilisticexpialidocious-antidisestablishmentarian"},
		{"separator-at-cap", "aaaaaaaaaaa bbbbbbbbbbb ccccccccccc ddddddddddd eeeeeeeeeee fff", "aaaaaaaaaaa-bbbbbbbbbbb-ccccccccccc-ddddddddddd-eeeeeeeeeee"},
		{"unicode-separators", "Café naïve résumé", "caf-na-ve-r-sum"},
	}
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			for _, v := range vectors {
				t.Run(v.name, func(t *testing.T) {
					f := newFixture(t, engine)
					id, _ := f.save(map[string]any{"content": v.content})
					got := f.call("memory.get", map[string]any{"id": id}, false, "")
					if got["key"] != v.key || got["content"] != v.content {
						t.Fatalf("derived key or verbatim content: %v", got)
					}
				})
			}
		})
	}
}

func TestB13PublicMemoryIdentityRefusalsAndCurrentHistory(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			key, body := " Has Spaces.✓ ", "  literal\nbody ✓  "
			id, first := f.save(map[string]any{"key": key, "content": body})
			if got := f.call("memory.get", map[string]any{"id": id}, false, ""); got["key"] != key || got["content"] != body {
				t.Fatalf("literal memory changed: %v", got)
			}
			updated, _ := f.save(map[string]any{"key": key, "content": "current body"})
			if updated != id {
				t.Fatalf("replacement changed memory identity: %s != %s", updated, id)
			}
			for _, query := range []string{"SPACES.✓", "CURRENT BODY"} {
				page := f.call("memory.list", map[string]any{"query": query}, false, "")
				items, ok := page["items"].([]any)
				if !ok || len(items) != 1 || items[0].(map[string]any)["id"] != id {
					t.Fatalf("case-insensitive key/content search %q: %v", query, page)
				}
			}
			for _, p := range []map[string]any{
				{"content": ""}, {"content": " \t\n"}, {"content": "!!!"}, {"content": "日本語のメモ"},
				{"key": " \t", "content": "valid"},
			} {
				f.refuseWrite("memory.save", p, "invalid_params")
			}
			ref := map[string]any{"id": id}
			history := map[string]any{"ref": ref, "asOf": map[string]any{"revision": first}}
			checkHistory := func() {
				t.Helper()
				f.policy = nil
				value := f.call("history.get", history, false, "")
				if value["body"].(map[string]any)["content"] != body || len(f.policy) < 2 {
					t.Fatalf("historical value/current revalidation: %v policy=%v", value, f.policy)
				}
				for _, input := range f.policy {
					if input.ID != id || input.Content != "current body" {
						t.Fatalf("policy received a historical value: %+v", input)
					}
				}
				f.readable = false
				f.call("history.get", history, false, "not_found")
				f.call("memory.get", map[string]any{"id": id}, false, "not_found")
				page := f.call("memory.list", map[string]any{}, false, "")
				if items, ok := page["items"].([]any); !ok || len(items) != 0 {
					t.Fatalf("denied memory leaked through list: %v", page)
				}
				f.readable = true
			}
			checkHistory()
			if engine == "sqlite" {
				before := f.meta()
				f.close()
				f.open()
				if after := f.meta(); !reflect.DeepEqual(before, after) {
					t.Fatalf("reopen changed memory identity/head: before=%+v after=%+v", before, after)
				}
				checkHistory()
			}
			f.call("memory.delete", map[string]any{"id": id}, true, "")
			f.call("memory.get", map[string]any{"id": id}, false, "not_found")
			f.call("history.get", history, false, "not_found")
			newID, _ := f.save(map[string]any{"key": key, "content": "new lifetime"})
			if newID == id {
				t.Fatal("forget/recreate reused the deleted memory identity")
			}
			f.call("history.get", history, false, "not_found")
			f.refuseWrite("memory.delete", map[string]any{"id": id}, "not_found")
		})
	}
}

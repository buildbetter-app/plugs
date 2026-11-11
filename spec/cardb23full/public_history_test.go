// B23 public consumer controls. These execute ordinary public Plug history;
// they do not manufacture restored native history or native provenance.
// Setup follows the unchanged public RPC wiring of spec/s1-b21-b24/fixture_test.go
// (Nikhil Shinday and the original S1 fixture authors).
package cardb23full

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const project = "b23-public-history"

type frame struct{ Request, Response json.RawMessage }
type fixture struct {
	t           *testing.T
	ctx         context.Context
	engine, dsn string
	store       driver.Store
	lease       driver.ActiveLease
	server      *rpc.Server
	clock       *testclock.Clock
	denied      string
	next        int
	frames      []frame
}

func open(t *testing.T, engine string) *fixture {
	f := &fixture{t: t, ctx: context.Background(), engine: engine, clock: testclock.New(t, time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC))}
	switch engine {
	case "sqlite":
		f.dsn = filepath.Join(t.TempDir(), "b23.sqlite")
	case "postgres":
		f.dsn = f.clock.BindPostgres(t, testclock.PostgresDSN(t))
	}
	f.connect()
	t.Cleanup(func() { f.close(); f.save() })
	return f
}
func (f *fixture) connect() {
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: f.clock.Now, StoreID: "b23-public-history", Incarnation: "b23-public-history"})
	case "sqlite":
		f.store, err = (sqlite.Driver{Clock: f.clock.Now}).Open(f.ctx, driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = postgres.Open(f.ctx, driver.Config{DSN: f.dsn})
	}
	if err != nil {
		f.t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "b23-public-history", driver.LeaseTTL)
	if err != nil {
		f.t.Fatal(err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithClock(f.clock.Now), pipeline.WithIDGenerator(func() (string, error) { f.next++; return fmt.Sprintf("b23-history-%03d", f.next), nil }))
	svc := service.New(f.store, f.lease, writer, service.WithOntology(ontology), service.WithClock(f.clock.Now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err = rpc.BindService(registry, svc); err != nil {
		f.t.Fatal(err)
	}
	views, err := viewops.New(f.store, f.lease, viewops.Config{OntologyRevision: model.Natural64(1), StaleDefaultOntology: ontology, Now: f.clock.Now, AuthorizeRead: func(_ context.Context, _ model.PrincipalRef, resource model.ResourceRef, _ string) error {
		if resource.Ref.ID == f.denied {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
		return nil
	}})
	if err != nil {
		f.t.Fatal(err)
	}
	key := []byte("b23-public-history-cursor-key-0123456789")
	if err = viewrpc.BindWithConfig(registry, views, viewrpc.Config{CursorKey: key}); err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: meta.Incarnation, CursorKey: key, OntologyRevision: model.Natural64(1)})
	if err != nil {
		f.t.Fatal(err)
	}
}
func (f *fixture) close() {
	if f.lease != nil {
		if err := f.lease.Release(f.ctx); err != nil {
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
func (f *fixture) call(method, label string, params map[string]any) json.RawMessage {
	f.t.Helper()
	digest := sha256.Sum256([]byte("b23-public-history/" + label))
	params["_meta"] = map[string]any{"project": project, "agent": "b23-recorder", "idempotencyKey": hex.EncodeToString(digest[:])}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": label, "method": method, "params": params})
	if err != nil {
		f.t.Fatal(err)
	}
	response := f.server.Dispatch(f.ctx, request)
	f.frames = append(f.frames, frame{append(json.RawMessage(nil), request...), append(json.RawMessage(nil), response...)})
	var envelope struct {
		Result json.RawMessage
		Error  json.RawMessage
	}
	if err = json.Unmarshal(response, &envelope); err != nil || len(envelope.Error) > 0 && !bytes.Equal(envelope.Error, []byte("null")) {
		f.t.Fatalf("%s: %s (%v)", label, response, err)
	}
	return envelope.Result
}
func (f *fixture) mutation(method, label string, params map[string]any) model.Commit {
	f.t.Helper()
	var commit model.Commit
	if err := json.Unmarshal(f.call(method, label, params), &commit); err != nil {
		f.t.Fatal(err)
	}
	if len(commit.Results) != 1 {
		f.t.Fatalf("%s: unexpected commit %+v", label, commit)
	}
	return commit
}
func (f *fixture) save() {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if root == "" {
		root = f.t.TempDir()
	}
	raw, err := json.MarshalIndent(map[string]any{"engine": f.engine, "native_source_history_claim": false, "frames": f.frames}, "", "  ")
	if err != nil {
		f.t.Error(err)
		return
	}
	if err = os.WriteFile(filepath.Join(root, "b23-public-history-"+f.engine+".json"), raw, 0600); err != nil {
		f.t.Error(err)
	}
}

// Compare list identity/order, exact count shape and returned snapshot at each
// committed content window. Current authorization must affect every window.
func (f *fixture) check(label, query string, at viewops.AsOf, want []string) {
	f.t.Helper()
	definition := map[string]any{"filter": map[string]any{"query": query}, "fields": []string{"id", "fields.title"}, "sort": []string{"id"}, "budgetTokens": 1000}
	for _, countOnly := range []bool{false, true} {
		raw := f.call("view.run", fmt.Sprintf("%s-count-%t", label, countOnly), map[string]any{"definition": definition, "asOf": at, "countOnly": countOnly})
		var body map[string]json.RawMessage
		if err := json.Unmarshal(raw, &body); err != nil {
			f.t.Fatal(err)
		}
		var gotAt viewops.AsOf
		if err := json.Unmarshal(body["asOf"], &gotAt); err != nil || gotAt != at {
			f.t.Fatalf("snapshot: %s %v", raw, err)
		}
		if countOnly {
			var count int
			if err := json.Unmarshal(body["count"], &count); err != nil || count != len(want) {
				f.t.Fatalf("count: %s want=%d %v", raw, len(want), err)
			}
			if !bytes.Equal(bytes.TrimSpace(body["items"]), []byte("[]")) {
				f.t.Fatalf("count items: %s", raw)
			}
			for _, field := range []string{"groups", "elisions", "nextCursor"} {
				if _, ok := body[field]; ok {
					f.t.Fatalf("count emitted %s: %s", field, raw)
				}
			}
		} else {
			var items []struct{ ID string }
			if err := json.Unmarshal(body["items"], &items); err != nil {
				f.t.Fatal(err)
			}
			ids := []string{}
			for _, item := range items {
				ids = append(ids, item.ID)
			}
			if !reflect.DeepEqual(ids, want) {
				f.t.Fatalf("list identity/order: got=%v want=%v raw=%s", ids, want, raw)
			}
			if _, ok := body["nextCursor"]; ok {
				f.t.Fatalf("unexpected continuation: %s", raw)
			}
		}
	}
}
func TestPublicHistoricalSearchListCountReplayReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := open(t, engine)
			first := f.mutation("bead.create", "create-first", map[string]any{"type": "issue", "fields": map[string]any{"title": "Needle first"}})
			a := first.Results[0].Ref.ID
			second := f.mutation("bead.create", "create-second", map[string]any{"type": "issue", "fields": map[string]any{"title": "Needle second"}})
			b := second.Results[0].Ref.ID
			updated := f.mutation("bead.update", "update-first", map[string]any{"id": first.Results[0].Ref, "fields": map[string]any{"title": "ordinary first"}})
			// Replaying the same public request must retain the winning commit, without
			// inserting another version or advancing the store head.
			replay := f.mutation("bead.update", "update-first", map[string]any{"id": first.Results[0].Ref, "fields": map[string]any{"title": "ordinary first"}})
			if !reflect.DeepEqual(updated, replay) {
				t.Fatal("replay changed winning commit")
			}
			meta, err := f.store.Meta(f.ctx)
			if err != nil || meta.Head != updated.Revision {
				t.Fatal("replay advanced head", meta, err)
			}
			windows := []struct {
				at          viewops.AsOf
				needle, all []string
			}{
				{viewops.AsOf{Incarnation: meta.Incarnation, Revision: first.Revision}, []string{a}, []string{a}},
				{viewops.AsOf{Incarnation: meta.Incarnation, Revision: second.Revision}, []string{a, b}, []string{a, b}},
				{viewops.AsOf{Incarnation: meta.Incarnation, Revision: updated.Revision}, []string{b}, []string{a, b}},
			}
			for _, phase := range []string{"before-reopen", "after-reopen"} {
				if phase == "after-reopen" && engine != "memory" {
					f.close()
					f.connect()
				}
				for i, w := range windows {
					prefix := fmt.Sprintf("%s-window-%d", phase, i)
					f.denied = ""
					f.check(prefix+"-search", "title=Needle", w.at, w.needle)
					f.check(prefix+"-all", "", w.at, w.all)
					f.denied = a
					without := []string{}
					for _, id := range w.needle {
						if id != a {
							without = append(without, id)
						}
					}
					f.check(prefix+"-denied", "title=Needle", w.at, without)
					f.denied = ""
					f.check(prefix+"-regrant", "title=Needle", w.at, w.needle)
				}
			}
			t.Logf("B23_PUBLIC_HISTORY engine=%s windows=3 replay=winning-commit reopen=%t native_source_history=false", engine, engine != "memory")
		})
	}
}

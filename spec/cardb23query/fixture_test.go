// Independent B23 public query recorder. Setup follows the unchanged B23
// public_history fixture; no shared fixture or inherited test is modified.
// Every request and result is retained; memory refreshes the same store.
package cardb20

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
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const project = "b23-public-query"

type frame struct{ Request, Response json.RawMessage }
type fixture struct {
	t            *testing.T
	ctx          context.Context
	baseCtx      context.Context
	holdLease    bool
	releaseLease func() error
	engine, dsn  string
	store        driver.Store
	lease        driver.ActiveLease
	server       *rpc.Server
	clock        *testclock.Clock
	denied       string
	next         int
	frames       []frame
	ids          []string
}

func open(t *testing.T, engine string) *fixture {
	return openQueryFixture(t, engine, false)
}

func openQueryFixture(t *testing.T, engine string, holdLease bool) *fixture {
	ctx := context.Background()
	if holdLease {
		ctx = t.Context()
	}
	f := &fixture{t: t, ctx: ctx, baseCtx: ctx, holdLease: holdLease, engine: engine, clock: testclock.New(t, time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC))}
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
	f.ctx = f.baseCtx
	var err error
	switch f.engine {
	case "memory":
		f.store, err = memory.NewWithOptions(memory.Options{Clock: f.clock.Now, StoreID: "b23-public-query", Incarnation: "b23-public-query"})
	case "sqlite":
		f.store, err = (sqlite.Driver{Clock: f.clock.Now}).Open(f.ctx, driver.Config{DSN: f.dsn})
	case "postgres":
		f.store, err = postgres.Open(f.ctx, driver.Config{DSN: f.dsn})
	}
	if err != nil {
		f.t.Fatal(err)
	}
	if f.holdLease {
		f.ctx, f.lease, f.releaseLease, err = converterroundtrip.HoldRestoreLease(f.ctx, f.store, "b23-public-query")
	} else {
		f.lease, err = f.store.Lease(f.ctx, "b23-public-query", driver.LeaseTTL)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	document := ontology.DefaultDocument()
	document.Types["bug"] = ontology.Type{Name: "Bug", Workflow: "issue", Fields: document.Types["issue"].Fields}
	compiled, problems := ontology.Compile(document, nil, nil)
	if len(problems) != 0 {
		f.t.Fatal(problems)
	}
	ontology := service.NewOntology(func(project string) ontology.Effective { return compiled.Effective(project) })
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithClock(f.clock.Now), pipeline.WithIDGenerator(func() (string, error) {
		f.next++
		if f.next <= len(f.ids) {
			return f.ids[f.next-1], nil
		}
		return fmt.Sprintf("b23-query-%d", f.next), nil
	}))
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
	key := []byte("b23-public-query-cursor-key-0123456789")
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
		var err error
		if f.releaseLease != nil {
			op := f.ctx
			err = f.releaseLease()
			f.releaseLease = nil
			if op.Err() != context.Canceled {
				f.t.Error("held restore lease did not cancel its operation on release")
			}
			f.t.Logf("B23_RESTORE_LEASE_RELEASED engine=%s operation_cancelled=%t", f.engine, op.Err() == context.Canceled)
		} else {
			err = f.lease.Release(f.ctx)
		}
		if err != nil {
			f.t.Error(err)
		}
		f.lease = nil
		f.ctx = f.baseCtx
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			f.t.Error(err)
		}
		f.store = nil
	}
}
func (f *fixture) request(method, label string, params map[string]any) []byte {
	f.t.Helper()
	digest := sha256.Sum256([]byte("b23-public-query/" + label))
	params["_meta"] = map[string]any{"project": project, "agent": "b23-recorder", "idempotencyKey": hex.EncodeToString(digest[:])}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": label, "method": method, "params": params})
	if err != nil {
		f.t.Fatal(err)
	}
	response := f.server.Dispatch(f.ctx, request)
	f.frames = append(f.frames, frame{append(json.RawMessage(nil), request...), append(json.RawMessage(nil), response...)})
	return response
}
func (f *fixture) call(method, label string, params map[string]any) json.RawMessage {
	f.t.Helper()
	response := f.request(method, label, params)
	var envelope struct {
		Result json.RawMessage
		Error  json.RawMessage
	}
	if err := json.Unmarshal(response, &envelope); err != nil || len(envelope.Error) > 0 && !bytes.Equal(envelope.Error, []byte("null")) {
		f.t.Fatalf("%s: %s (%v)", label, response, err)
	}
	return envelope.Result
}
func (f *fixture) refusal(method, label, code string, params map[string]any) {
	f.t.Helper()
	before, err := f.store.Meta(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	response := f.request(method, label, params)
	var envelope struct {
		Result json.RawMessage
		Error  *struct{ Data model.ErrorData }
	}
	if err = json.Unmarshal(response, &envelope); err != nil || envelope.Error == nil || envelope.Error.Data.Code != model.ErrorCode(code) || len(envelope.Result) > 0 {
		f.t.Fatalf("%s refusal=%s want=%s: %v", label, response, code, err)
	}
	after, err := f.store.Meta(f.ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		f.t.Fatalf("refusal mutated metadata: %+v -> %+v (%v)", before, after, err)
	}
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
	name := strings.NewReplacer("/", "_", " ", "_").Replace(f.t.Name())
	if err = os.WriteFile(filepath.Join(root, "b23-public-query-"+name+"-"+f.engine+".json"), raw, 0600); err != nil {
		f.t.Error(err)
	}
}

func (f *fixture) reopen() {
	f.t.Helper()
	if f.engine == "memory" {
		return
	}
	f.close()
	f.connect()
}

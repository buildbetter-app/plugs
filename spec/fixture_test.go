package spec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/acceptance"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

const incarnation = "acceptance-incarnation"

// The fixture composes production layers. It never opens a WriteTx, seeds a
// record directly, replaces a handler, or implements an alternate store.
type core struct {
	ctx      context.Context
	by       model.PrincipalRef
	store    *memory.Store
	lease    driver.ActiveLease
	writer   *pipeline.Writer
	ontology *service.Ontology
	inputs   *acceptance.Inputs
}

func newCore(t *testing.T, ids []string, options ...pipeline.Option) *core {
	t.Helper()
	inputs, err := acceptance.NewInputs(time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), ids...)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memory.NewWithOptions(memory.Options{
		Clock: inputs.Now, StoreID: driver.StoreID("acceptance-store"), Incarnation: incarnation,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	lease, err := store.Lease(ctx, "acceptance-server", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	o := service.NewOntology(nil)
	options = append([]pipeline.Option{
		pipeline.WithClock(inputs.Now), pipeline.WithIDGenerator(inputs.NextID), pipeline.WithOntology(o),
	}, options...)
	return &core{ctx: ctx, by: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"},
		store: store, lease: lease, writer: pipeline.New(store, lease, options...), ontology: o, inputs: inputs}
}

func key(label string) string {
	digest := sha256.Sum256([]byte("plug-acceptance/" + label))
	return hex.EncodeToString(digest[:])
}

func (f *core) meta(label string) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", Project: "acceptance", Agent: "acceptance-worker",
		AgentLabel: "acceptance-worker", IdempotencyKey: key(label)}
}

func (f *core) apply(t *testing.T, label string, cs model.ChangeSet) model.Commit {
	t.Helper()
	commit, err := f.writer.Apply(f.ctx, f.by, cs, f.meta(label))
	if err != nil {
		t.Fatalf("real Writer %s: %v", label, err)
	}
	return commit
}

// Some lifecycle slots are not RPC-bound yet. Graph setup still goes through
// the actual Writer and ontology rather than writing a seed document directly.
func (f *core) slot(t *testing.T, method, id string) {
	t.Helper()
	meta := f.meta(method + "/" + id)
	meta.Method = method
	_, err := f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: []model.Op{
		patchOp(model.RefByID(id, model.Natural64(0)), map[string]any{}),
	}}, meta)
	if err != nil {
		t.Fatalf("real Writer setup slot %s for %s: %v", method, id, err)
	}
}

func (f *core) reader(t *testing.T) driver.Reader {
	t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: incarnation})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func closeReader(t *testing.T, r driver.Reader) {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Error(err)
	}
}

func (f *core) document(t *testing.T, id string) pipeline.Document {
	t.Helper()
	r := f.reader(t)
	defer closeReader(t, r)
	rows, err := r.Get(f.ctx, []model.Ref{model.RefByID(id, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatalf("read persisted %s: %d rows, %v", id, len(rows), err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func (f *core) assertAbsent(t *testing.T, id string) {
	t.Helper()
	r := f.reader(t)
	defer closeReader(t, r)
	rows, err := r.Get(f.ctx, []model.Ref{model.RefByID(id, model.Natural64(0))})
	if err != nil || len(rows) != 0 {
		t.Fatalf("rollback published %s: %d rows, %v", id, len(rows), err)
	}
}

func (f *core) assertLog(t *testing.T, count int) {
	t.Helper()
	r := f.reader(t)
	defer closeReader(t, r)
	changes, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil || len(changes) != count {
		t.Fatalf("changesets: want %d, got %d, error %v", count, len(changes), err)
	}
	if r.Snapshot().At.String() != fmt.Sprint(count) {
		t.Fatalf("head %s differs from complete log length %d", r.Snapshot().At, count)
	}
	for i, change := range changes {
		if change.Revision.String() != fmt.Sprint(i+1) || change.Incarnation != incarnation || len(change.Changes) == 0 {
			t.Fatalf("gap, empty changeset or wrong incarnation at %d: %+v", i, change)
		}
	}
}

func (f *core) versions(t *testing.T, ref model.Ref) []driver.Version {
	t.Helper()
	r := f.reader(t)
	defer closeReader(t, r)
	page, err := r.Versions(f.ctx, ref, driver.PageReq{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("versions incomplete: %v, cursor %+v", err, page.NextCursor)
	}
	return page.Items
}

type stack struct {
	core        *core
	trace       *acceptance.Trace
	incarnation string
	next        int
}

func newStack(t *testing.T, ids []string, options ...pipeline.Option) *stack {
	t.Helper()
	f := newCore(t, ids, options...)
	svc := service.New(f.store, f.lease, f.writer, service.WithOntology(f.ontology), service.WithClock(f.inputs.Now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, svc); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "acceptance", Incarnation: incarnation,
		CursorKey: []byte("acceptance-fixture-cursor-key-32b")})
	if err != nil {
		t.Fatal(err)
	}
	trace, err := acceptance.NewTrace(inprocess.New(server), f.inputs.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
			data, err := json.MarshalIndent(trace.Frames(), "", "  ")
			if err != nil {
				t.Error(err)
				return
			}
			name := strings.ReplaceAll(t.Name(), "/", "_") + ".rpc.json"
			if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0600); err != nil {
				t.Error(err)
			}
		}
	})
	return &stack{core: f, trace: trace, incarnation: incarnation}
}

func (f *stack) callAs(t *testing.T, ctx context.Context, method string, params map[string]any) json.RawMessage {
	t.Helper()
	f.next++
	copy := make(map[string]any, len(params)+1)
	for name, value := range params {
		copy[name] = value
	}
	copy["_meta"] = map[string]any{"project": "acceptance", "agent": "acceptance-worker",
		"idempotencyKey": key(fmt.Sprintf("rpc-%d", f.next))}
	response, err := f.trace.Call(ctx, method, copy)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func (f *stack) call(t *testing.T, method string, params map[string]any) json.RawMessage {
	t.Helper()
	return f.callAs(t, f.core.ctx, method, params)
}

func (f *stack) success(t *testing.T, method string, params map[string]any) json.RawMessage {
	t.Helper()
	response := f.call(t, method, params)
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	if refusal, ok := envelope["error"]; ok {
		t.Fatalf("real RPC %s refused: %s", method, refusal)
	}
	return response
}

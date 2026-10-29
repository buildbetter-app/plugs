package extended

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/acceptance"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

const project = "extended"
const incarnation = "extended-acceptance-incarnation"
const agent = "extended-worker"

// This fixture composes real production layers with the existing deterministic
// harness. All setup uses Writer.Apply. No test opens a WriteTx or binds a
// substitute RPC handler. The resolver contains compiled configuration only.
type fixture struct {
	ctx    context.Context
	by     model.PrincipalRef
	inputs *acceptance.Inputs
	store  *memory.Store
	writer *pipeline.Writer
	svc    *service.Service
	trace  *acceptance.Trace
	next   int
}

func newFixture(t *testing.T, ids []string, resolve func(string) ontology.Effective) *fixture {
	t.Helper()
	inputs, err := acceptance.NewInputs(time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), ids...)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memory.NewWithOptions(memory.Options{Clock: inputs.Now,
		StoreID: "extended-acceptance-store", Incarnation: incarnation})
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
	// Keep the real storage lease alive across explicit five-minute clock
	// advances; the independent bead claim still uses its production TTL.
	lease, err := store.Lease(ctx, "extended-server", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	o := service.NewOntology(resolve)
	w := pipeline.New(store, lease, pipeline.WithClock(inputs.Now),
		pipeline.WithIDGenerator(inputs.NextID), pipeline.WithOntology(o))
	svc := service.New(store, lease, w, service.WithOntology(o), service.WithClock(inputs.Now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, svc); err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: incarnation,
		CursorKey: []byte("extended-acceptance-cursor-key-32b")})
	if err != nil {
		t.Fatal(err)
	}
	trace, err := acceptance.NewTrace(inprocess.New(server), inputs.Now)
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
	return &fixture{ctx: ctx, by: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"},
		inputs: inputs, store: store, writer: w, svc: svc, trace: trace}
}

func compile(t *testing.T, base ontology.Document, overlays map[string]ontology.Document) func(string) ontology.Effective {
	t.Helper()
	snapshot, problems := ontology.Compile(base, overlays, nil)
	if len(problems) != 0 {
		t.Fatalf("fixture ontology must compile: %+v", problems)
	}
	return snapshot.Effective
}

func ref(id string) model.Ref          { return model.RefByID(id, model.Natural64(0)) }
func wireRef(id string) map[string]any { return map[string]any{"id": id} }

func patch(id string, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref(id), Fields: fields}}
}

func create(key, inProject string, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{
		Key: key, Type: "issue", Project: inProject, Fields: fields}}
}

func edge(kind model.OpKind, from, to, dependency string) model.Op {
	return model.Op{Kind: kind, Link: &model.LinkOp{From: ref(from), To: ref(to), LinkKind: dependency}}
}

func (f *fixture) meta(label, method string) model.WriteMeta {
	sum := sha256.Sum256([]byte("plug-extended/" + label))
	return model.WriteMeta{Method: method, Project: project, Agent: agent, AgentLabel: agent,
		IdempotencyKey: hex.EncodeToString(sum[:])}
}

func (f *fixture) apply(t *testing.T, label string, ops ...model.Op) model.Commit {
	t.Helper()
	commit, err := f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: ops}, f.meta(label, "tx.apply"))
	if err != nil {
		t.Fatalf("Writer %s: %v", label, err)
	}
	return commit
}

// These lifecycle methods are not RPC-bound. The method here is trusted host
// lowering context passed directly to the actual Writer, never a new handler.
func (f *fixture) slot(t *testing.T, label, method, id string, fields map[string]any) model.Commit {
	t.Helper()
	if fields == nil {
		fields = map[string]any{}
	}
	commit, err := f.writer.Apply(f.ctx, f.by, model.ChangeSet{Ops: []model.Op{patch(id, fields)}}, f.meta(label, method))
	if err != nil {
		t.Fatalf("direct Writer %s %s: %v", method, id, err)
	}
	return commit
}

func (f *fixture) call(t *testing.T, label, method string, params map[string]any) json.RawMessage {
	t.Helper()
	copy := make(map[string]any, len(params)+1)
	for k, v := range params {
		copy[k] = v
	}
	meta := f.meta(label, method)
	copy["_meta"] = map[string]any{"project": project, "agent": agent, "idempotencyKey": meta.IdempotencyKey}
	data, err := f.trace.Call(f.ctx, method, copy)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (f *fixture) success(t *testing.T, method string, params map[string]any) json.RawMessage {
	t.Helper()
	f.next++
	data := f.call(t, fmt.Sprintf("rpc-%d", f.next), method, params)
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || len(envelope.Error) != 0 {
		t.Fatalf("real RPC %s failed: %s (%v)", method, data, err)
	}
	return data
}

func at[T any](t *testing.T, data []byte, path string) T {
	t.Helper()
	projected, err := acceptance.Project(data, path)
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(projected, &values); err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(values[path], &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func sameJSON(t *testing.T, got, want any) {
	t.Helper()
	a, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := acceptance.EqualJSON(b, a); err != nil {
		t.Fatal(err)
	}
}

func pageIDs(t *testing.T, data []byte, want ...string) {
	t.Helper()
	items := at[[]struct {
		ID string `json:"id"`
	}](t, data, "/result/items")
	got := make([]string, 0, len(items))
	for _, item := range items {
		got = append(got, item.ID)
	}
	sameJSON(t, got, append([]string{}, want...))
}

func typed(t *testing.T, err error, want model.ErrorCode) *model.Error {
	t.Helper()
	var v *model.Error
	if !errors.As(err, &v) || v.Data.Code != want {
		t.Fatalf("want typed %s refusal, got %T: %v", want, err, err)
	}
	return v
}

// Capture complete committed rows, versions and changesets, plus head and the
// graph sentinel. Refusal/no-op comparisons detect hidden versions, lost fields,
// an extra revision, partial graph publication, or an extra changeset.
type image struct {
	View     driver.View
	Rows     []driver.Record
	Versions map[string][]driver.Version
	Log      []model.Changeset
	Graph    []driver.LockState
}

func (f *fixture) image(t *testing.T) image {
	t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	v := image{View: r.Snapshot(), Versions: map[string][]driver.Version{}}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		v.Rows = append(v.Rows, page.Items...)
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			t.Fatal("non-advancing storage page")
		}
		cursor = page.NextCursor
	}
	for _, row := range v.Rows {
		page, err := r.Versions(f.ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || page.NextCursor != nil {
			t.Fatalf("incomplete history for %s: %v", row.Ref.ID, err)
		}
		v.Versions[row.Ref.ID] = page.Items
	}
	v.Log, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		t.Fatal(err)
	}
	v.Graph, err = r.Observe(f.ctx, []driver.LockKey{driver.GraphBlockingSentinelLock})
	if err != nil {
		t.Fatal(err)
	}
	if v.View.At == nil || v.View.At.String() != fmt.Sprint(len(v.Log)) {
		t.Fatalf("head/log disagreement: %+v, %d changesets", v.View, len(v.Log))
	}
	for i, cs := range v.Log {
		if cs.Revision.String() != fmt.Sprint(i+1) || cs.Incarnation != incarnation || len(cs.Changes) == 0 {
			t.Fatalf("non-gapless/empty/wrong-incarnation changeset %d: %+v", i, cs)
		}
	}
	return v
}

func (f *fixture) unchanged(t *testing.T, before image) {
	t.Helper()
	after := f.image(t)
	if !reflect.DeepEqual(before, after) {
		sameJSON(t, after, before)
		t.Fatal("committed storage image changed")
	}
}

func (v image) row(t *testing.T, id string) driver.Record {
	t.Helper()
	for _, row := range v.Rows {
		if row.Ref.ID == id {
			return row
		}
	}
	t.Fatalf("missing committed row %s", id)
	return driver.Record{}
}

func (v image) document(t *testing.T, id string) pipeline.Document {
	t.Helper()
	doc, err := pipeline.DecodeDocument(v.row(t, id).Body)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func (f *fixture) noReceipt(t *testing.T, label, method string) {
	t.Helper()
	if v := f.receipt(t, label, method); v != nil {
		t.Fatalf("refused write created a receipt: %+v", v)
	}
}

func (f *fixture) receipt(t *testing.T, label, method string) *driver.IdempotencyRecord {
	t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	principal, err := driver.PrincipalKey(f.by)
	if err != nil {
		t.Fatal(err)
	}
	v, err := r.IdempotencyByKey(f.ctx, principal, agent, method, f.meta(label, method).IdempotencyKey)
	if err != nil {
		t.Fatalf("read actual receipt: %v", err)
	}
	return v
}

func (f *fixture) advance(t *testing.T, d time.Duration) {
	t.Helper()
	if err := f.inputs.Advance(d); err != nil {
		t.Fatal(err)
	}
}

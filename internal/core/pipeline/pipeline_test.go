package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

type testOntology struct{ reject bool }

func (o testOntology) Normalize(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
	patches := []port.Patch{}
	for _, c := range p.Changes {
		if len(c.After) == 0 || string(c.After) == "null" {
			continue
		}
		doc, err := DecodeDocument(c.After)
		if err != nil {
			return nil, err
		}
		status := ""
		if c.Op.Kind == model.OpCreate && doc.Fields["status"] == nil {
			status = "todo"
		}
		if p.Meta.Method == "bead.claim" {
			status = "in_progress"
		}
		if p.Meta.Method == "bead.close" {
			status = "done"
		}
		if status != "" {
			raw, _ := json.Marshal(status)
			patches = append(patches, port.Patch{Resource: c.Resource, Paths: map[string]json.RawMessage{"fields.status": raw}})
		}
	}
	return patches, nil
}
func (o testOntology) Validate(_ context.Context, _ port.ProposedChange, _ port.SnapshotReader) error {
	if o.reject {
		return failure("ontology_guard_failed", "test guard refused")
	}
	return nil
}

type mutatorFunc func(context.Context, port.ProposedChange, port.SnapshotReader) ([]port.Patch, error)

func (f mutatorFunc) Mutate(c context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	return f(c, p, r)
}

type validatorFunc func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error)

func (f validatorFunc) Validate(c context.Context, p port.ProposedChange, r port.SnapshotReader) (port.Verdict, error) {
	return f(c, p, r)
}

type postFunc func(context.Context, port.CommittedChange, port.SnapshotReader) error

func (f postFunc) After(c context.Context, p port.CommittedChange, r port.SnapshotReader) error {
	return f(c, p, r)
}

type harness struct {
	store *memory.Store
	lease driver.ActiveLease
	now   time.Time
	ids   atomic.Uint64
}

func setup(t *testing.T) *harness {
	t.Helper()
	h := &harness{now: time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)}
	store, err := memory.NewWithOptions(memory.Options{Clock: func() time.Time { return h.now }, Incarnation: "pipeline-test", StoreID: "pipeline-test"})
	if err != nil {
		t.Fatal(err)
	}
	h.store = store
	t.Cleanup(func() { _ = store.Close() })
	h.lease, err = store.Lease(context.Background(), "pipeline-test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func (h *harness) writer(options ...Option) *Writer {
	base := []Option{WithClock(func() time.Time { return h.now }), WithIDGenerator(func() (string, error) { return fmt.Sprintf("pl-%d", h.ids.Add(1)), nil }), WithOntology(testOntology{})}
	return New(h.store, h.lease, append(base, options...)...)
}
func meta(n int) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", IdempotencyKey: fmt.Sprintf("%032x", n), Project: "test"}
}

var by = model.PrincipalRef{Kind: "user", ID: "nikhil"}

func create(key string) model.Op {
	return model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Key: key, Type: "issue", Fields: map[string]any{"title": "initial"}}}
}
func patch(ref model.Ref, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: fields}}
}
func apply(t *testing.T, w *Writer, cs model.ChangeSet, m model.WriteMeta) model.Commit {
	t.Helper()
	result, err := w.Apply(context.Background(), by, cs, m)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func (h *harness) read(t *testing.T) driver.Reader {
	t.Helper()
	m, err := h.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := h.store.Read(context.Background(), driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}
func code(t *testing.T, err error, want string) {
	t.Helper()
	var e *model.Error
	if !errors.As(err, &e) || string(e.Data.Code) != want {
		t.Fatalf("error=%v, want code %s", err, want)
	}
}

func TestAtomicBatchAndReplay(t *testing.T) {
	h := setup(t)
	w := h.writer()
	cs := model.ChangeSet{Ops: []model.Op{create("new"), patch(model.Ref{Key: "new"}, map[string]any{"title": "patched"})}}
	original, _ := json.Marshal(cs)
	result := apply(t, w, cs, meta(1))
	if len(result.Results) != 2 || result.Revision.String() != "1" || result.Results[0].Ref.ID != result.Results[1].Ref.ID {
		t.Fatalf("commit=%+v", result)
	}
	doc, err := DecodeDocument(result.Results[0].Record)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Fields["title"] != "patched" {
		t.Fatalf("body=%+v", doc)
	}
	again := apply(t, w, cs, meta(1))
	if !reflect.DeepEqual(result, again) {
		t.Fatalf("replay differs: %+v %+v", result, again)
	}
	after, _ := json.Marshal(cs)
	if string(original) != string(after) {
		t.Fatal("caller input changed")
	}
	log, err := h.read(t).Changesets(context.Background(), nil, 10)
	if err != nil || len(log) != 1 || len(log[0].Changes) != 1 {
		t.Fatalf("log=%+v err=%v", log, err)
	}
	versions, err := h.read(t).Versions(context.Background(), result.Results[0].Ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 1 {
		t.Fatalf("versions=%+v err=%v", versions, err)
	}
}

func TestReparentRetryUsesFreshOperationCopy(t *testing.T) {
	h := setup(t)
	interferer := h.writer()
	var calls atomic.Int32
	var retriedTarget, retriedParent model.Ref
	w := h.writer(WithValidators(ValidatorBinding{ID: "reparent-retry", Validator: validatorFunc(func(_ context.Context, proposal port.ProposedChange, _ port.SnapshotReader) (port.Verdict, error) {
		if calls.Add(1) == 1 {
			if _, err := interferer.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{create("interference")}}, meta(102)); err != nil {
				return port.Verdict{}, err
			}
		} else {
			for _, change := range proposal.Changes {
				if change.Op.Kind == model.OpReparent {
					retriedTarget = change.Op.Reparent.Target
					retriedParent = *change.Op.Reparent.Parent
					break
				}
			}
		}
		return port.Verdict{Allow: true}, nil
	})}))
	parent := model.Ref{Key: "parent"}
	change := model.ChangeSet{Ops: []model.Op{
		create("parent"),
		create("child"),
		{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: model.Ref{Key: "child"}, Parent: &parent}},
	}}
	commit, err := w.Apply(context.Background(), by, change, meta(103))
	if err != nil {
		t.Fatalf("reparent retry failed: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("validator calls=%d, want 2", calls.Load())
	}
	if len(commit.Results) != 3 {
		t.Fatalf("commit results=%d, want 3", len(commit.Results))
	}
	if retriedTarget != commit.Results[1].Ref || retriedParent != commit.Results[0].Ref {
		t.Fatalf("retry reused resolved refs from the first attempt: target=%v parent=%v results=%v", retriedTarget, retriedParent, commit.Results)
	}
	child, err := DecodeDocument(commit.Results[1].Record)
	if err != nil {
		t.Fatal(err)
	}
	if child.Parent == nil || *child.Parent != commit.Results[0].Ref {
		t.Fatalf("reparented child parent=%v, want %v", child.Parent, commit.Results[0].Ref)
	}
}

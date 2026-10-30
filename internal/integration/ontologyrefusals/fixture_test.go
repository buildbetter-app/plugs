package ontologyrefusals_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const project = "ontology-refusals"

var owner = model.PrincipalRef{Kind: "user", ID: "ontology-owner"}

type writeCall func(model.WriteMeta) (model.Commit, error)

// This fixture only composes production components. No evaluator, Writer,
// transaction, record, receipt or guard observation is substituted or seeded.
type fixture struct {
	ctx      context.Context
	backend  string
	path     string
	now      func() time.Time
	store    driver.Store
	lease    driver.ActiveLease
	writer   *pipeline.Writer
	svc      *service.Service
	stop     context.CancelFunc
	renewed  chan error
	sequence uint64
	requests []model.WriteMeta
}

func eachStore(t *testing.T, run func(*testing.T, *fixture)) {
	t.Helper()
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			t.Cleanup(cancel)
			now := time.Now().UTC().Truncate(time.Second)
			f := &fixture{ctx: ctx, backend: backend, path: filepath.Join(t.TempDir(), "ontology.db"), now: func() time.Time { return now }}
			t.Cleanup(func() { f.close(t) })
			f.open(t)
			run(t, f)
		})
	}
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	if f.backend == "memory" {
		f.store = memory.New()
	} else {
		f.store, err = (sqlite.Driver{}).Open(f.ctx, driver.Config{DSN: f.path})
		if err != nil {
			t.Fatal(err)
		}
		// The production SQLite driver must have opened a real regular DB file.
		info, err := os.Stat(f.path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("SQLite file: %v %v", info, err)
		}
		data, err := os.ReadFile(f.path)
		if err != nil || !bytes.HasPrefix(data, []byte("SQLite format 3\x00")) {
			t.Fatalf("not a SQLite database: %v", err)
		}
	}
	f.lease, err = f.store.Lease(f.ctx, "ontology-refusals", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(f.ctx)
	f.stop, f.renewed = stop, make(chan error, 1)
	lease, done := f.lease, f.renewed
	go func() {
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil {
					if ctx.Err() != nil {
						err = nil
					}
					done <- err
					return
				}
			}
		}
	}()
	ontology := service.NewOntology(nil) // canonical default, without overrides
	f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(ontology), pipeline.WithClock(f.now))
	f.svc = service.New(f.store, f.lease, f.writer, service.WithOntology(ontology), service.WithClock(f.now))
}

func (f *fixture) close(t *testing.T) {
	t.Helper()
	if f.stop != nil {
		f.stop()
		if err := <-f.renewed; err != nil {
			t.Error("storage lease renewal:", err)
		}
		f.stop = nil
	}
	if f.lease != nil {
		if err := f.lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
		f.lease = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
		f.store = nil
	}
}

func (f *fixture) meta(t *testing.T) driver.Meta {
	t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (f *fixture) request(method string) model.WriteMeta {
	f.sequence++
	meta := model.WriteMeta{Method: method, Project: project, IdempotencyKey: fmt.Sprintf("%032x", f.sequence)}
	f.requests = append(f.requests, meta)
	return meta
}

func (f *fixture) commit(t *testing.T, method string, call writeCall) model.Commit {
	t.Helper()
	head, err := f.meta(t).Head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	meta := f.request(method)
	commit, err := call(meta)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Revision.String() != model.Natural64(head+1).String() || commit.Revision.String() != f.meta(t).Head.String() || len(commit.Warnings) != 0 {
		t.Fatalf("expected one clean durable commit after %d: %+v", head, commit)
	}
	f.requireReceipt(t, meta, commit)
	return commit
}

func (f *fixture) create(t *testing.T, fields map[string]any) model.Ref {
	t.Helper()
	c := f.commit(t, "bead.create", func(meta model.WriteMeta) (model.Commit, error) {
		return f.svc.Create(f.ctx, owner, service.CreateRequest{Type: "issue", Fields: fields}, meta)
	})
	if len(c.Results) != 1 || c.Results[0].Ref.ID == "" || !c.Results[0].Ref.Branch.IsZero() {
		t.Fatalf("create result: %+v", c)
	}
	return c.Results[0].Ref
}

func (f *fixture) get(t *testing.T, ref model.Ref) service.GetResult {
	t.Helper()
	value, err := f.svc.Get(f.ctx, owner, service.GetRequest{ID: ref})
	if err != nil {
		t.Fatal(err)
	}
	if value.Bead.ID != ref.ID || value.Bead.Type != "issue" || value.Bead.Project != project {
		t.Fatalf("immutable identity changed: %+v", value.Bead)
	}
	return value
}

func patch(ref model.Ref, fields map[string]any) model.Op {
	return model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: fields}}
}

func (f *fixture) apply(ops ...model.Op) writeCall {
	return func(meta model.WriteMeta) (model.Commit, error) {
		return f.writer.Apply(f.ctx, owner, model.ChangeSet{Ops: ops}, meta)
	}
}

func (f *fixture) update(ref model.Ref, fields map[string]any) writeCall {
	return func(meta model.WriteMeta) (model.Commit, error) {
		return f.svc.Update(f.ctx, owner, service.UpdateRequest{ID: ref, Fields: fields}, meta)
	}
}

func (f *fixture) claim(ref model.Ref) writeCall {
	return func(meta model.WriteMeta) (model.Commit, error) {
		result, err := f.svc.Claim(f.ctx, owner, service.ClaimRequest{ID: ref}, meta)
		return result.Commit, err
	}
}

func (f *fixture) settle(ref model.Ref, reason string) writeCall {
	return func(meta model.WriteMeta) (model.Commit, error) {
		return f.svc.Close(f.ctx, owner, service.CloseRequest{ID: ref, Reason: reason}, meta)
	}
}

// Every refusal compares exact public storage images and service views. The
// denied key is registered BEFORE the snapshot so both snapshots require its
// receipt and changeset lookup to remain absent; all older receipts also match.
func (f *fixture) refuse(t *testing.T, method string, code model.ErrorCode, call writeCall) {
	t.Helper()
	meta := f.request(method)
	before := f.snapshot(t)
	scope := meta.Method + "/" + meta.IdempotencyKey
	if before.Receipts[scope] != nil || before.ByKey[scope] != nil {
		t.Fatal("refusal probe must start with an unused receipt scope")
	}
	commit, err := call(meta)
	var domain *model.Error
	if !errors.As(err, &domain) || domain.Data.Code != code {
		t.Fatalf("want %s, got commit=%+v error=%v", code, commit, err)
	}
	if !reflect.DeepEqual(commit, model.Commit{}) {
		t.Fatalf("refusal returned a commit: %+v", commit)
	}
	requireEqual(t, "refusal durable state", before, f.snapshot(t))
	t.Logf("%s refused %s; exact records, versions, log/head, observations, views and receipts conserved", method, code)
}

type durableState struct {
	Meta         driver.Meta
	Records      []driver.Record
	Versions     map[string][]driver.Version
	Log          []model.Changeset
	Observations []driver.LockState
	Ready        service.BeadPage
	History      service.BeadPage
	Views        map[string]service.GetResult
	Receipts     map[string]*driver.IdempotencyRecord
	ByKey        map[string]*model.Changeset
}

func (f *fixture) snapshot(t *testing.T) durableState {
	t.Helper()
	s := durableState{Meta: f.meta(t), Versions: map[string][]driver.Version{}, Views: map[string]service.GetResult{}, Receipts: map[string]*driver.IdempotencyRecord{}, ByKey: map[string]*model.Changeset{}}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: s.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(f.ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		t.Fatalf("incomplete full record inventory: %+v %v", page, err)
	}
	s.Records = page.Items
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock, driver.OntologySentinelLock, driver.PluginsSentinelLock}
	ready, err := driver.NewReadySentinel(project, "issue")
	if err != nil {
		t.Fatal(err)
	}
	keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: ready})
	for _, record := range s.Records {
		versions, err := r.Versions(f.ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("incomplete versions: %+v %v", versions, err)
		}
		s.Versions[record.Ref.ID] = versions.Items
		s.Views[record.Ref.ID] = f.get(t, record.Ref)
		ref := record.Ref
		bead, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i]) // all keys constructed above are valid
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	s.Observations, err = r.Observe(f.ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	s.Log, err = r.Changesets(f.ctx, nil, 1000)
	if err != nil || len(s.Log) >= 1000 {
		t.Fatalf("incomplete log: %v", err)
	}
	principal, err := driver.PrincipalKey(owner)
	if err != nil {
		t.Fatal(err)
	}
	for _, meta := range f.requests {
		scope := meta.Method + "/" + meta.IdempotencyKey
		s.Receipts[scope], err = r.IdempotencyByKey(f.ctx, principal, "", meta.Method, meta.IdempotencyKey)
		if err != nil {
			t.Fatal(err)
		}
		s.ByKey[scope], err = r.ChangesetByKey(f.ctx, principal, "", meta.Method, meta.IdempotencyKey)
		if err != nil {
			t.Fatal(err)
		}
	}
	s.Ready, err = f.svc.Ready(f.ctx, owner, service.ReadyRequest{Project: project, Limit: 1000})
	if err != nil || s.Ready.Next != nil {
		t.Fatalf("incomplete ready: %+v %v", s.Ready, err)
	}
	s.History, err = f.svc.History(f.ctx, owner, service.HistoryRequest{Project: project, AsOf: service.AsOf{Revision: s.Meta.Head, Incarnation: s.Meta.Incarnation}, Limit: 1000})
	if err != nil || s.History.Next != nil {
		t.Fatalf("incomplete history: %+v %v", s.History, err)
	}
	return s
}

func (f *fixture) requireReceipt(t *testing.T, meta model.WriteMeta, commit model.Commit) {
	t.Helper()
	s := f.snapshot(t)
	r := s.Receipts[meta.Method+"/"+meta.IdempotencyKey]
	if r == nil || r.ResponsePending || r.RequestHash == "" {
		t.Fatalf("missing final receipt for %s: %+v", meta.Method, r)
	}
	got, err := json.Marshal(r.Commit)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(commit)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("receipt differs from response: got=%s want=%s error=%v", got, want, err)
	}
}

func requireEqual(t *testing.T, label string, before, after any) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		old, _ := json.MarshalIndent(before, "", "  ")
		new, _ := json.MarshalIndent(after, "", "  ")
		t.Fatalf("%s changed:\nbefore=%s\nafter=%s", label, old, new)
	}
}

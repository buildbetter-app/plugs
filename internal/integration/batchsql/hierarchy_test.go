package batchsql_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// This wrapper keeps the real SQL queue capability while refusing a graph
// scan or point-read fallback. The ordinary public Ready oracle uses the same
// committed records independently.
type mixedParentQueueReader struct {
	driver.Reader
	queue               driver.QueueProjectionReader
	projections, images int
	queries, gets       int
}

func (r *mixedParentQueueReader) QueueProjection(ctx context.Context) (driver.QueueProjection, error) {
	r.projections++
	return r.queue.QueueProjection(ctx)
}

func (r *mixedParentQueueReader) QueueProjectionImages(ctx context.Context, view driver.View, refs []model.Ref) ([]driver.Record, error) {
	r.images++
	return r.queue.QueueProjectionImages(ctx, view, refs)
}

func (r *mixedParentQueueReader) Query(context.Context, driver.Query) (driver.Page[driver.Record], error) {
	r.queries++
	return driver.Page[driver.Record]{}, errors.New("mixed-parent queue fell back to a source scan")
}

func (r *mixedParentQueueReader) Get(context.Context, []model.Ref) ([]driver.Record, error) {
	r.gets++
	return nil, errors.New("mixed-parent queue fell back to source point reads")
}

func TestQueueReadyCandidatesMixedParentSnapshotAndReopen(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			svc := m03ReadyService(t, f)
			child := model.RefByID(m03CreateDurable(t, f, 0), model.Natural64(0))
			grandchild := model.RefByID(m03CreateDurable(t, f, 1), model.Natural64(0))
			free := m03CreateDurable(t, f, 2)
			meta := f.meta("queue-mixed-parent-invoke")
			meta.Method = "x.workflow.invoke"
			result, err := svc.WorkflowInvoke(f.ctx, m03ReadyPrincipal, workflowplugin.InvokeRequest{
				Template: "queue-mixed-parents", Project: "batch-local", Mode: "ephemeral",
				Checklist: []workflowplugin.ChecklistItem{{Label: "parent", Title: "blocked parent", Type: "task", Blocked: true}},
			}, meta)
			if err != nil || len(result.Items) != 1 {
				t.Fatalf("mixed parent invocation: result=%+v err=%v", result, err)
			}
			parent := model.RefByID(result.Items[0].ID, model.Natural64(0))
			for i, request := range []service.LinkRequest{
				{From: child, To: parent, Kind: "parent-child"},
				{From: grandchild, To: child, Kind: "parent-child"},
			} {
				meta := f.meta(fmt.Sprintf("queue-mixed-parent-link-%d", i))
				meta.Method = "bead.link"
				if _, err := svc.Link(f.ctx, m03ReadyPrincipal, request, meta); err != nil {
					t.Fatal(err)
				}
			}
			includeEphemeral := false
			req := service.ReadyRequest{Project: "batch-local", Limit: 10, Filter: &service.Filter{
				Resources: []string{child.ID, grandchild.ID, free}, IncludeEphemeral: &includeEphemeral,
			}}
			for _, stage := range []string{"blocked", "blocked_reopened", "unblocked", "unblocked_reopened"} {
				t.Run(stage, func(t *testing.T) {
					if stage == "blocked_reopened" || stage == "unblocked_reopened" {
						f.reopen(t)
						svc = m03ReadyService(t, f)
					}
					if stage == "unblocked" {
						meta := f.meta("queue-mixed-parent-unblock")
						meta.Method = "bead.update"
						if _, err := svc.Update(f.ctx, m03ReadyPrincipal, service.UpdateRequest{ID: parent, Fields: map[string]any{"blocked": false}}, meta); err != nil {
							t.Fatal(err)
						}
					}
					want := []string{free}
					if stage == "unblocked" || stage == "unblocked_reopened" {
						want = []string{child.ID, grandchild.ID, free}
					}
					ordinary, err := svc.Ready(f.ctx, m03ReadyPrincipal, req)
					if err != nil || ordinary.Next != nil || !reflect.DeepEqual(m03ReadyIDs(ordinary), want) {
						t.Fatalf("ordinary mixed-parent oracle: page=%+v err=%v want=%v", ordinary, err, want)
					}
					probe := func(t *testing.T, reader driver.Reader) {
						t.Helper()
						queue, ok := reader.(driver.QueueProjectionReader)
						if !ok {
							t.Fatal("real SQL reader lost its queue capability")
						}
						bound := &mixedParentQueueReader{Reader: reader, queue: queue}
						items, err := svc.ReadyCandidates(f.ctx, m03ReadyPrincipal, req, bound)
						ids := m03ReadyIDs(service.BeadPage{Items: items})
						if err != nil || !reflect.DeepEqual(ids, m03ReadyIDs(ordinary)) {
							t.Fatalf("typed queue differs from same-record ordinary Ready: got=%v want=%v err=%v", ids, m03ReadyIDs(ordinary), err)
						}
						if bound.projections != 1 || bound.images != 0 || bound.queries != 0 || bound.gets != 0 {
							t.Fatalf("queue did not use one complete thin graph: projections=%d images=%d queries=%d gets=%d", bound.projections, bound.images, bound.queries, bound.gets)
						}
					}
					t.Run("reader", func(t *testing.T) {
						reader := hierarchyReader(t, f)
						defer reader.Close()
						probe(t, reader)
					})
					t.Run("terminal_transaction", func(t *testing.T) {
						tx, err := f.store.Write(f.ctx, f.lease)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Close()
						if _, err := tx.Lock(f.ctx, []driver.LockRequest{{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockShare}}); err != nil {
							t.Fatal(err)
						}
						if _, err := tx.Head(f.ctx); err != nil {
							t.Fatal(err)
						}
						probe(t, tx)
						if err := tx.Rollback(f.ctx); err != nil {
							t.Fatal(err)
						}
					})
				})
			}
		})
	}
}

func hierarchyCreate(key string, parent *model.Ref, project string) model.Op {
	op := createOp(key, key, project)
	op.Create.Parent = parent
	return op
}

func hierarchyMove(child model.Ref, parent *model.Ref) model.Op {
	return model.Op{Kind: model.OpReparent, Reparent: &model.ReparentOp{Kind: model.OpReparent, Target: child, Parent: parent}}
}

func hierarchyRef(key string) *model.Ref { ref := model.RefByKey(key); return &ref }

func hierarchyApply(t *testing.T, f *fixture, ops ...model.Op) model.Commit {
	t.Helper()
	commit, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: ops}, f.meta("hierarchy"))
	if err != nil {
		t.Fatal(err)
	}
	return commit
}

func hierarchyReader(t *testing.T, f *fixture) driver.Reader {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func hierarchyRows(t *testing.T, f *fixture) []driver.DerivedRow {
	t.Helper()
	r := hierarchyReader(t, f)
	rows, err := r.DerivedRows(f.ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func hierarchyFacts(t *testing.T, f *fixture) []driver.LockState {
	t.Helper()
	keys := map[string]driver.LockKey{}
	keys["graph"] = driver.GraphBlockingSentinelLock
	for _, row := range hierarchyRows(t, f) {
		descendant, _, _, err := pipeline.DecodeAncestorRow(row)
		if err != nil {
			t.Fatal(err)
		}
		s, _ := driver.NewClosureSentinel(descendant)
		keys[descendant] = driver.LockKey{Kind: driver.LockSentinel, Sentinel: s}
	}
	list := make([]driver.LockKey, 0, len(keys))
	for _, key := range keys {
		list = append(list, key)
	}
	sort.Slice(list, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(list[i])
		b, _ := driver.EncodeLockKey(list[j])
		return a < b
	})
	states, err := hierarchyReader(t, f).Observe(f.ctx, list)
	if err != nil {
		t.Fatal(err)
	}
	return states
}

// Derive the oracle exclusively from persisted direct Parent edges, never from
// the planner or its delta. Enumerating the whole table also catches orphan and
// extra rows that exact positive pair lookups alone would miss.
func verifyHierarchy(t *testing.T, f *fixture) map[string]uint64 {
	t.Helper()
	r := hierarchyReader(t, f)
	docs := map[string]pipeline.Document{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range page.Items {
			docs[record.Ref.ID] = decodeDocument(t, record)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	want := map[string]uint64{}
	for id := range docs {
		seen := map[string]bool{}
		for current, depth := id, uint64(0); current != ""; depth++ {
			doc, exists := docs[current]
			if !exists || seen[current] {
				t.Fatalf("dangling/cyclic persisted parent at %s", current)
			}
			seen[current] = true
			want[id+"\x00"+current] = depth
			current = ""
			if doc.Parent != nil {
				current = doc.Parent.ID
			}
		}
	}
	rows, err := r.DerivedRows(f.ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]uint64{}
	for i, row := range rows {
		descendant, ancestor, depth, err := pipeline.DecodeAncestorRow(row)
		if err != nil || (i > 0 && rows[i-1].Key >= row.Key) {
			t.Fatalf("noncanonical/unsorted row %+v: %v", row, err)
		}
		pair := descendant + "\x00" + ancestor
		if _, duplicate := got[pair]; duplicate {
			t.Fatalf("duplicate pair %q", pair)
		}
		got[pair] = depth
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted closure differs from independent Parent traversal: got=%v want=%v", got, want)
	}
	return got
}

func TestHierarchyOrderedBatchAndSubtreeAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			// This case has no conditional links. Enable the complete event plan
			// with the real audit adapter, which fails closed if one is introduced
			// without a pinned ontology source.
			f.writer = pipeline.New(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithOntologyAudit(service.NewOntologyAudit(nil)), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID))
			seed := hierarchyApply(t, f,
				hierarchyCreate("a", nil, "parent-project"),
				hierarchyCreate("b", hierarchyRef("a"), "parent-project"),
				hierarchyCreate("c", hierarchyRef("b"), "child-project"),
				hierarchyCreate("d", hierarchyRef("c"), "child-project"),
			)
			a, b, c, d := seed.Results[0].Ref, seed.Results[1].Ref, seed.Results[2].Ref, seed.Results[3].Ref
			if seed.Revision.String() != "1" {
				t.Fatalf("ordered batch was not atomic: %+v", seed)
			}
			initial := verifyHierarchy(t, f)
			if initial[d.ID+"\x00"+a.ID] != 3 {
				t.Fatal("initial grandchild depth was not 3")
			}
			old := hierarchyReader(t, f)
			oldRows, err := old.DerivedRows(f.ctx, driver.DerivedAncestorClosure, "")
			if err != nil {
				t.Fatal(err)
			}
			moveMeta := f.meta("move")
			move := model.ChangeSet{Ops: []model.Op{hierarchyMove(c, &a), commentOp(c, "subtree moved")}}
			dryMeta := f.meta("dry-run")
			dryMeta.DryRun = true
			dryState := f.state(t)
			dry, err := f.writer.Apply(f.ctx, batchPrincipal, move, dryMeta)
			if err != nil || !dry.WouldCommit {
				t.Fatalf("hierarchy dry run failed: %+v %v", dry, err)
			}
			requireSameState(t, dryState, f.state(t))
			if !reflect.DeepEqual(oldRows, hierarchyRows(t, f)) || f.receipt(t, dryMeta) != nil {
				t.Fatal("dry run published closure/receipt")
			}
			moved, err := f.writer.Apply(f.ctx, batchPrincipal, move, moveMeta)
			if err != nil {
				t.Fatal(err)
			}
			got := verifyHierarchy(t, f)
			movedLog := f.state(t).Log
			last := movedLog[len(movedLog)-1]
			if len(last.Events) != 1 || last.Events[0].Type != model.EventBeadReparented || last.Changes[0].Op != model.OpReparent || last.Events[0].Provenance.SetBy["parent"] != "request" {
				t.Fatalf("reparent event/provenance was lost behind comment: %+v", last)
			}
			if got[c.ID+"\x00"+a.ID] != 1 || got[d.ID+"\x00"+a.ID] != 2 {
				t.Fatal("same-key depth replacements were lost")
			}
			if _, stale := got[d.ID+"\x00"+b.ID]; stale {
				t.Fatal("old external ancestor survived subtree move")
			}
			for _, ref := range []model.Ref{c, d} {
				sentinel, _ := driver.NewClosureSentinel(ref.ID)
				states, err := hierarchyReader(t, f).Observe(f.ctx, []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: sentinel}})
				if err != nil || len(states) != 1 || states[0].Revision.String() != moved.Revision.String() {
					t.Fatalf("descendant sentinel not bumped: %s %+v %v", ref.ID, states, err)
				}
			}
			again, err := f.writer.ApplyWithOutcome(f.ctx, batchPrincipal, move, moveMeta)
			if err != nil || !again.Replayed || !reflect.DeepEqual(again.Commit, moved) {
				t.Fatalf("hierarchy replay changed commit: %+v %v", again, err)
			}
			historical, err := old.DerivedRows(f.ctx, driver.DerivedAncestorClosure, "")
			if err != nil || !reflect.DeepEqual(historical, oldRows) {
				t.Fatalf("pinned closure snapshot changed: %v", err)
			}
			hierarchyApply(t, f, hierarchyMove(c, nil))
			got = verifyHierarchy(t, f)
			if len(got) != 6 || got[d.ID+"\x00"+c.ID] != 1 {
				t.Fatalf("root move did not preserve only internal subtree pairs: %v", got)
			}
			state := f.state(t)
			if doc := decodeDocument(t, state.Records[c.ID]); doc.Project != "child-project" || doc.Parent != nil {
				t.Fatalf("root move changed ownership or parent: %+v", doc)
			}
			versions := state.Versions[c.ID]
			if len(versions) != 3 || decodeDocument(t, driver.Record{Body: versions[0].Body}).Parent.ID != b.ID {
				t.Fatalf("source Parent was not retained in immutable history: %+v", versions)
			}
			_ = old.Close()
			if engine != "memory" {
				f.reopen(t)
				verifyHierarchy(t, f)
			}
		})
	}
}

func TestHierarchyRefusalsAreAtomicAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seed := hierarchyApply(t, f, hierarchyCreate("parent", nil, "epic"), hierarchyCreate("child", hierarchyRef("parent"), "work"))
			parent, child := seed.Results[0].Ref, seed.Results[1].Ref
			missing, malformed := model.Ref{ID: "pl-missing"}, model.RefByID("", model.Natural64(0))
			projectMove := hierarchyMove(child, nil)
			projectMove.Reparent.Project = "other"
			cases := []struct {
				name string
				ops  []model.Op
				code model.ErrorCode
			}{
				{"forward-alias", []model.Op{hierarchyCreate("x", hierarchyRef("later"), "work"), hierarchyCreate("later", nil, "epic")}, "invalid_params"},
				{"unknown-alias", []model.Op{hierarchyMove(child, hierarchyRef("unknown"))}, "invalid_params"},
				{"malformed-parent", []model.Op{hierarchyMove(child, &malformed)}, "invalid_params"},
				{"missing-parent", []model.Op{hierarchyMove(child, &missing)}, "not_found"},
				{"self-cycle", []model.Op{hierarchyMove(child, &child)}, "ontology_guard_failed"},
				{"descendant-cycle", []model.Op{hierarchyMove(parent, &child)}, "ontology_guard_failed"},
				{"project-move", []model.Op{projectMove}, "invalid_params"},
				{"delete-parent", []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: parent}}}, "ontology_guard_failed"},
				{"deleted-parent", []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: parent}}, hierarchyCreate("x", &parent, "work")}, "not_found"},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					before, rows := f.state(t), hierarchyRows(t, f)
					m := f.meta(tc.name)
					ops := append([]model.Op{commentOp(child, "must roll back")}, tc.ops...)
					_, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: ops}, m)
					requireCode(t, err, tc.code)
					requireSameState(t, before, f.state(t))
					if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || f.receipt(t, m) != nil {
						t.Fatal("refusal changed closure or wrote a receipt")
					}
				})
			}
			verifyHierarchy(t, f)
		})
	}
}

func TestHierarchyRealChildGuardsAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			// Child terminality follows its own project's ontology, not a literal
			// status key or its parent's project ontology.
			base := ontology.DefaultDocument()
			closed := base.Statuses["done"]
			delete(base.Statuses, "done")
			base.Statuses["shipped"] = closed
			workflow := base.Workflows["issue"]
			for i, status := range workflow.Statuses {
				if status == "done" {
					workflow.Statuses[i] = "shipped"
				}
			}
			workflow.Slots[ontology.Close] = "shipped"
			for i := range workflow.Transitions {
				if workflow.Transitions[i].To == "done" {
					workflow.Transitions[i].To = "shipped"
				}
			}
			base.Workflows["issue"] = workflow
			compiled, problems := ontology.Compile(base, nil, nil)
			if len(problems) != 0 {
				t.Fatal(problems)
			}
			f.ontology.Refresh(compiled)
			seed := hierarchyApply(t, f, hierarchyCreate("p", nil, "epic"), hierarchyCreate("c", hierarchyRef("p"), "work"))
			parent, child := seed.Results[0].Ref, seed.Results[1].Ref
			close := func(ref model.Ref) error {
				m := f.meta("close")
				m.Method = "bead.close"
				_, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{patchOp(ref, nil)}}, m)
				return err
			}
			requireCode(t, close(parent), "ontology_guard_failed")
			if err := close(child); err != nil {
				t.Fatal(err)
			}
			if err := close(parent); err != nil {
				t.Fatal(err)
			}
			before := f.state(t)
			_, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyCreate("open", &parent, "work")}}, f.meta("attach-open"))
			requireCode(t, err, "ontology_guard_failed")
			m := f.meta("reopen")
			m.Method = "bead.reopen"
			_, err = f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{patchOp(child, nil)}}, m)
			requireCode(t, err, "ontology_guard_failed")
			requireSameState(t, before, f.state(t))
			verifyHierarchy(t, f)
		})
	}
}

type hierarchyFaultStore struct {
	driver.Store
	stage string
}
type hierarchyFaultTx struct {
	driver.WriteTx
	stage string
}

var errHierarchyFault = errors.New("hierarchy staged-write fault")

func (s hierarchyFaultStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &hierarchyFaultTx{WriteTx: tx, stage: s.stage}, nil
}
func (t *hierarchyFaultTx) Put(ctx context.Context, record driver.Record) error {
	if err := t.WriteTx.Put(ctx, record); err != nil {
		return err
	}
	if t.stage == "record" {
		return errHierarchyFault
	}
	return nil
}
func (t *hierarchyFaultTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	var err error
	if writer, ok := t.WriteTx.(driver.GraphSourceWriter); ok {
		err = writer.PutGraphRecord(ctx, record, parent, links)
	} else if writer, ok := t.WriteTx.(driver.HierarchySourceWriter); ok {
		err = writer.PutHierarchyRecord(ctx, record, parent)
	} else {
		err = t.WriteTx.Put(ctx, record)
	}
	if err != nil {
		return err
	}
	if t.stage == "record" {
		return errHierarchyFault
	}
	return nil
}
func (t *hierarchyFaultTx) PutDerived(ctx context.Context, table driver.DerivedTable, remove, add []driver.DerivedRow) error {
	if err := t.WriteTx.PutDerived(ctx, table, remove, add); err != nil {
		return err
	}
	if t.stage == "derived" {
		return errHierarchyFault
	}
	return nil
}
func (t *hierarchyFaultTx) BumpSentinels(ctx context.Context, sentinels []driver.Sentinel) error {
	if err := t.WriteTx.BumpSentinels(ctx, sentinels); err != nil {
		return err
	}
	if t.stage == "sentinels" {
		return errHierarchyFault
	}
	return nil
}
func (t *hierarchyFaultTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	if t.stage == "commit" {
		return model.Changeset{}, errHierarchyFault
	}
	return t.WriteTx.Commit(ctx, header)
}

func TestHierarchyStagedFaultRollbackAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seed := hierarchyApply(t, f, hierarchyCreate("a", nil, "work"), hierarchyCreate("b", nil, "work"), hierarchyCreate("c", hierarchyRef("a"), "work"))
			b, child := seed.Results[1].Ref, seed.Results[2].Ref
			for _, stage := range []string{"record", "derived", "sentinels", "commit"} {
				t.Run(stage, func(t *testing.T) {
					before, rows := f.state(t), hierarchyRows(t, f)
					facts := hierarchyFacts(t, f)
					writer := pipeline.New(hierarchyFaultStore{Store: f.store, stage: stage}, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now))
					m := f.meta(stage)
					_, err := writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &b)}}, m)
					if !errors.Is(err, errHierarchyFault) {
						t.Fatalf("fault was not reached: %v", err)
					}
					requireSameState(t, before, f.state(t))
					if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || f.receipt(t, m) != nil {
						t.Fatal("staged failure published closure/receipt")
					}
					if !reflect.DeepEqual(facts, hierarchyFacts(t, f)) {
						t.Fatal("staged failure published a predicate bump")
					}
				})
			}
			verifyHierarchy(t, f)
		})
	}
}

// Inject invalid persisted derived state through the real driver, then verify
// the writer refuses it without treating a missing row as a legacy root or
// silently backfilling a populated store.
func TestHierarchyMalformedClosureRefusesAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, defect := range []string{"missing-self", "extra-ancestor", "wrong-depth", "noncanonical-depth"} {
			t.Run(engine+"/"+defect, func(t *testing.T) {
				f := newFixture(t, engine)
				seed := hierarchyApply(t, f, hierarchyCreate("a", nil, "work"), hierarchyCreate("b", nil, "work"), hierarchyCreate("child", hierarchyRef("a"), "work"))
				a, b, child := seed.Results[0].Ref, seed.Results[1].Ref, seed.Results[2].Ref
				self, _ := pipeline.AncestorRow(child.ID, child.ID, 0)
				parent, _ := pipeline.AncestorRow(child.ID, a.ID, 1)
				var remove, add []driver.DerivedRow
				switch defect {
				case "missing-self":
					remove = []driver.DerivedRow{self}
				case "extra-ancestor":
					row, _ := pipeline.AncestorRow(child.ID, b.ID, 2)
					add = []driver.DerivedRow{row}
				case "wrong-depth":
					parent.Value = []byte("2")
					add = []driver.DerivedRow{parent}
				case "noncanonical-depth":
					parent.Value = []byte("01")
					add = []driver.DerivedRow{parent}
				}
				hierarchyRawRows(t, f, child.ID, remove, add)
				before, rows := f.state(t), hierarchyRows(t, f)
				m := f.meta("refuse-malformed")
				_, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyMove(child, &b)}}, m)
				requireCode(t, err, "ontology_guard_failed")
				requireSameState(t, before, f.state(t))
				if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || f.receipt(t, m) != nil {
					t.Fatal("malformed closure was repaired or partially rewritten")
				}
			})
		}
	}
}

// A missing descendant closure row must not hide that descendant from a move.
// The source-child index remains complete when only derived rows are damaged.
func TestHierarchyMalformedDescendantClosureRefusesAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, defect := range []string{"missing-direct-parent", "missing-external-ancestor", "missing-all-descendant-rows"} {
			t.Run(engine+"/"+defect, func(t *testing.T) {
				f := newFixture(t, engine)
				seed := hierarchyApply(t, f,
					hierarchyCreate("a", nil, "work"),
					hierarchyCreate("b", hierarchyRef("a"), "work"),
					hierarchyCreate("c", hierarchyRef("b"), "work"),
					hierarchyCreate("d", hierarchyRef("c"), "work"),
				)
				b, c, d := seed.Results[1].Ref, seed.Results[2].Ref, seed.Results[3].Ref
				original := hierarchyRows(t, f)
				var remove []driver.DerivedRow
				for _, row := range original {
					descendant, ancestor, _, err := pipeline.DecodeAncestorRow(row)
					if err != nil {
						t.Fatal(err)
					}
					if descendant == d.ID && (defect == "missing-all-descendant-rows" ||
						(defect == "missing-direct-parent" && ancestor == c.ID) ||
						(defect == "missing-external-ancestor" && ancestor == b.ID)) {
						remove = append(remove, row)
					}
				}
				if len(remove) == 0 {
					t.Fatal("descendant corruption fixture removed no rows")
				}
				hierarchyRawRows(t, f, d.ID, remove, nil)
				keys := []driver.LockKey{driver.GraphBlockingSentinelLock}
				for _, result := range seed.Results {
					sentinel, _ := driver.NewClosureSentinel(result.Ref.ID)
					keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
				}
				for _, row := range original {
					keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}})
				}
				sort.Slice(keys, func(i, j int) bool {
					a, _ := driver.EncodeLockKey(keys[i])
					b, _ := driver.EncodeLockKey(keys[j])
					return a < b
				})
				before, rows := f.state(t), hierarchyRows(t, f)
				facts, err := hierarchyReader(t, f).Observe(f.ctx, keys)
				if err != nil {
					t.Fatal(err)
				}
				m := f.meta("refuse-malformed-descendant")
				_, err = f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyMove(c, nil)}}, m)
				requireCode(t, err, "ontology_guard_failed")
				requireSameState(t, before, f.state(t))
				if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || f.receipt(t, m) != nil {
					t.Fatal("malformed descendant closure was repaired or partially rewritten")
				}
				afterFacts, err := hierarchyReader(t, f).Observe(f.ctx, keys)
				if err != nil || !reflect.DeepEqual(facts, afterFacts) {
					t.Fatalf("refusal changed predicate or exact closure observations: %v", err)
				}
			})
		}
	}
}

func hierarchyRawRows(t *testing.T, f *fixture, id string, remove, add []driver.DerivedRow) {
	t.Helper()
	// This deliberately corrupts logical rows through the ordinary API. Typed
	// annotations belong to the trusted publication arm, and are not deletions.
	logical := func(rows []driver.DerivedRow) []driver.DerivedRow {
		result := make([]driver.DerivedRow, len(rows))
		for i, row := range rows {
			result[i] = driver.DerivedRow{Key: row.Key, Value: append([]byte(nil), row.Value...)}
		}
		return result
	}
	remove, add = logical(remove), logical(add)
	tx, err := f.store.Write(f.ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	s, _ := driver.NewClosureSentinel(id)
	locks := []driver.LockRequest{{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: s}, Mode: driver.LockUpdate}}
	for _, rows := range [][]driver.DerivedRow{remove, add} {
		for _, row := range rows {
			locks = append(locks, driver.LockRequest{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}}, Mode: driver.LockUpdate})
		}
	}
	sort.Slice(locks, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(locks[i].Key)
		b, _ := driver.EncodeLockKey(locks[j].Key)
		return a < b
	})
	if _, err := tx.Lock(f.ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(f.ctx, driver.DerivedAncestorClosure, remove, add); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(f.ctx, []driver.Sentinel{s}); err != nil {
		t.Fatal(err)
	}
	keys := make([]driver.LockKey, len(locks))
	for i := range locks {
		keys[i] = locks[i].Key
	}
	m := f.meta("inject-derived-corruption")
	if _, err := tx.Commit(f.ctx, driver.ChangesetHeader{Incarnation: tx.Snapshot().Incarnation, Kind: model.ChangesetWrite, Time: f.clock, Principal: batchPrincipal, Method: "test.hierarchy-corruption", IdempotencyKey: m.IdempotencyKey, RequestHash: "test-only", Response: model.Commit{Results: []model.OpResult{}}, Changes: []model.ChangeRecord{}, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
}

type hierarchyPause struct {
	once             sync.Once
	entered, release chan struct{}
}

func (p *hierarchyPause) Validate(ctx context.Context, _ port.ProposedChange, _ port.SnapshotReader) (port.Verdict, error) {
	p.once.Do(func() {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	})
	return port.Verdict{Allow: true}, ctx.Err()
}

func TestHierarchyChildPhantomRestartsAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, operation := range []string{"close", "move"} {
			t.Run(engine+"/"+operation, func(t *testing.T) {
				f := newFixture(t, engine)
				seed := hierarchyApply(t, f, hierarchyCreate("parent", nil, "work"), hierarchyCreate("destination", nil, "work"))
				parent, destination := seed.Results[0].Ref, seed.Results[1].Ref
				pause := &hierarchyPause{entered: make(chan struct{}), release: make(chan struct{})}
				writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithValidators(pipeline.ValidatorBinding{ID: "hierarchy-race", Validator: pause}))
				m := f.meta(operation)
				op := hierarchyMove(parent, &destination)
				if operation == "close" {
					m.Method = "bead.close"
					op = patchOp(parent, nil)
				}
				done := make(chan error, 1)
				go func() {
					_, err := writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{op}}, m)
					done <- err
				}()
				select {
				case <-pause.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("writer did not reach guarded snapshot")
				}
				child := hierarchyApply(t, f, hierarchyCreate("concurrent-child", &parent, "work")).Results[0].Ref
				close(pause.release)
				select {
				case err := <-done:
					if operation == "close" {
						requireCode(t, err, "ontology_guard_failed")
					} else if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("hierarchy transaction deadlocked")
				}
				closure := verifyHierarchy(t, f)
				if operation == "move" && closure[child.ID+"\x00"+destination.ID] != 2 {
					t.Fatal("restarted move omitted concurrent descendant")
				}
			})
		}
	}
}

func TestHierarchyThousandOperationBoundaryAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			writer, metrics := pipeline.NewWithTelemetry(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID))
			f.writer = writer
			cs := model.ChangeSet{Ops: make([]model.Op, 1000)}
			for i := range cs.Ops {
				cs.Ops[i] = hierarchyCreate(fmt.Sprintf("row-%d", i), nil, "work")
			}
			m := f.meta("1000-op-boundary")
			before := f.state(t)
			oversized := model.ChangeSet{Ops: append(append([]model.Op{}, cs.Ops...), hierarchyCreate("row-1000", nil, "work"))}
			_, err := f.writer.Apply(f.ctx, batchPrincipal, oversized, m)
			requireCode(t, err, "invalid_params")
			requireSameState(t, before, f.state(t))
			if f.receipt(t, m) != nil {
				t.Fatal("1001 refusal published a receipt")
			}
			out, err := f.writer.ApplyWithOutcome(f.ctx, batchPrincipal, cs, m)
			phaseMillis := map[pipeline.Phase]float64{}
			for phase, results := range metrics.Snapshot().LaneMillis {
				for _, millis := range results {
					phaseMillis[phase] += millis
				}
			}
			t.Logf("%s full 1000-op create phase times (ms): %v", engine, phaseMillis)
			if err != nil || out.Replayed || out.Commit.Revision.String() != "1" || len(out.Commit.Results) != 1000 {
				t.Fatalf("1000-op commit: %+v %v", out, err)
			}
			ids := map[string]bool{}
			for i, result := range out.Commit.Results {
				doc, err := pipeline.DecodeDocument(result.Record)
				if err != nil || !result.Ref.IsID() || ids[result.Ref.ID] || result.Revision.String() != "1" || doc.ID != result.Ref.ID || doc.Parent != nil || doc.Fields["title"] != fmt.Sprintf("row-%d", i) {
					t.Fatalf("1000-op order/identity/image at result %d: %+v %v", i, result, err)
				}
				ids[result.Ref.ID] = true
			}
			committed := f.state(t)
			if len(committed.Records) != 1000 || len(committed.Versions) != 1000 || len(committed.Log) != 1 || len(committed.Log[0].Changes) != 1000 {
				t.Fatal("1000-op commit did not publish exactly one complete changeset")
			}
			for id, versions := range committed.Versions {
				if !ids[id] || len(versions) != 1 || !versions[0].Base || versions[0].Deleted || versions[0].Revision.String() != "1" {
					t.Fatalf("1000-op base version differs for %s", id)
				}
			}
			if receipt := f.receipt(t, m); receipt == nil || receipt.ResponsePending || !reflect.DeepEqual(receipt.Commit, out.Commit) {
				t.Fatal("1000-op receipt is incomplete")
			}
			replayed, err := f.writer.ApplyWithOutcome(f.ctx, batchPrincipal, cs, m)
			if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Commit, out.Commit) {
				t.Fatalf("1000-op replay differs: %v", err)
			}
			requireSameState(t, committed, f.state(t))
			t.Logf("%s full operation boundary: 1000 IDs/rows/base versions, one changeset and exact replay PASS; 1001 atomic refusal", engine)
		})
	}
}

type hierarchyQueryTraceKey struct{}
type hierarchyQuerySpanKey struct{}

type hierarchyQuerySpan struct {
	query     string
	started   time.Time
	elapsed   time.Duration
	completed bool
	failed    bool
	rows      int64
}

type hierarchyQueryTrace struct {
	mu    sync.Mutex
	spans []*hierarchyQuerySpan
}

func (trace *hierarchyQueryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(hierarchyQueryTraceKey{}) != trace {
		return ctx
	}
	span := &hierarchyQuerySpan{query: data.SQL, started: time.Now()}
	trace.mu.Lock()
	trace.spans = append(trace.spans, span)
	trace.mu.Unlock()
	return context.WithValue(ctx, hierarchyQuerySpanKey{}, span)
}

func (trace *hierarchyQueryTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span, _ := ctx.Value(hierarchyQuerySpanKey{}).(*hierarchyQuerySpan)
	if span == nil {
		return
	}
	elapsed := time.Since(span.started)
	trace.mu.Lock()
	span.elapsed, span.completed, span.failed, span.rows = elapsed, true, data.Err != nil, data.CommandTag.RowsAffected()
	trace.mu.Unlock()
}

// These are driver query boundaries, including row consumption, not executor
// CPU timings. Record no bind values. Formatting happens after the unchanged
// Apply deadline and all queries continue through the actual pgx connection.
func (trace *hierarchyQueryTrace) report(t *testing.T) {
	t.Helper()
	trace.mu.Lock()
	defer trace.mu.Unlock()
	type sample struct {
		SQLSHA256, SQLPrefix string
		Calls, Ends, Failed  int
		ElapsedNS, MaxNS     int64
		Rows                 int64
	}
	groups := map[string]*sample{}
	for _, span := range trace.spans {
		group := groups[span.query]
		if group == nil {
			prefix := strings.Join(strings.Fields(span.query), " ")
			group = &sample{SQLSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(span.query))), SQLPrefix: prefix[:min(len(prefix), 300)]}
			groups[span.query] = group
		}
		group.Calls++
		if span.completed {
			group.Ends++
		}
		if span.failed {
			group.Failed++
		}
		group.ElapsedNS += span.elapsed.Nanoseconds()
		group.MaxNS = max(group.MaxNS, span.elapsed.Nanoseconds())
		group.Rows += span.rows
	}
	ordered := make([]*sample, 0, len(groups))
	for _, group := range groups {
		ordered = append(ordered, group)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ElapsedNS > ordered[j].ElapsedNS })
	for _, group := range ordered {
		raw, err := json.Marshal(group)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("W3_HIERARCHY_MOVE_PG_QUERY %s", raw)
	}
}

// This optional profile brackets only the actual move. File setup and profile
// finalization happen outside Apply and its unchanged request deadline. The
// profile samples this Go process, not the PostgreSQL server's executor CPU.
func hierarchyMoveCPUProfile(t *testing.T, engine string) func() {
	t.Helper()
	if os.Getenv("PLUG_W3_PROFILE_HIERARCHY_MOVE") != "1" || engine == "memory" {
		return func() {}
	}
	directory := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if directory == "" {
		directory = t.TempDir()
	}
	path := filepath.Join(directory, "w3-hierarchy-move-"+engine+".pprof")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			pprof.StopCPUProfile()
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestHierarchyClosure5000And5001AcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			trace := &hierarchyQueryTrace{}
			f := newFixture(t, engine, func(f *fixture) {
				if engine != "postgres" {
					return
				}
				config, err := pgx.ParseConfig(f.dsn)
				if err != nil {
					t.Fatal(err)
				}
				config.Tracer = trace
				f.dsn = stdlib.RegisterConnConfig(config)
				t.Cleanup(func() { stdlib.UnregisterConnConfig(f.dsn) })
			})
			writer, metrics := pipeline.NewWithTelemetry(f.store, f.lease, pipeline.WithOntology(f.ontology), pipeline.WithClock(f.now), pipeline.WithIDGenerator(f.nextID), pipeline.WithHierarchyMaxDepth(10))
			f.writer = writer
			ops := []model.Op{hierarchyCreate("subtree", nil, "work"), hierarchyCreate("extra", nil, "work")}
			for i := 1; i < 500; i++ {
				ops = append(ops, hierarchyCreate(fmt.Sprintf("leaf-%d", i), hierarchyRef("subtree"), "work"))
			}
			for i := 0; i < 10; i++ {
				var parent *model.Ref
				if i > 0 {
					parent = hierarchyRef(fmt.Sprintf("ancestor-%d", i-1))
				}
				ops = append(ops, hierarchyCreate(fmt.Sprintf("ancestor-%d", i), parent, "work"))
			}
			seed, err := f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: ops}, f.meta("511-op-seed"))
			phaseMillis := map[pipeline.Phase]float64{}
			for phase, results := range metrics.Snapshot().LaneMillis {
				for _, millis := range results {
					phaseMillis[phase] += millis
				}
			}
			t.Logf("%s unchanged 511-op hierarchy seed phase times (ms): %v", engine, phaseMillis)
			if err != nil {
				t.Fatalf("511-op hierarchy seed: %v", err)
			}
			subtree, extra, destination, root := seed.Results[0].Ref, seed.Results[1].Ref, seed.Results[len(ops)-1].Ref, seed.Results[len(ops)-10].Ref
			before, rows := f.state(t), hierarchyRows(t, f)
			m := f.meta("5001")
			_, err = f.writer.Apply(f.ctx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyMove(subtree, &destination), hierarchyMove(extra, &root)}}, m)
			requireCode(t, err, "closure_too_large")
			requireSameState(t, before, f.state(t))
			if !reflect.DeepEqual(rows, hierarchyRows(t, f)) || f.receipt(t, m) != nil {
				t.Fatal("5001 refusal published a partial rewrite")
			}
			beforeMove := metrics.Snapshot()
			moveCtx := context.WithValue(f.ctx, hierarchyQueryTraceKey{}, trace)
			var sqliteTrace *sqlite.CommitTrace
			if engine == "sqlite" && os.Getenv("PLUG_W3_PROFILE_HIERARCHY_MOVE") == "1" {
				sqliteTrace = &sqlite.CommitTrace{}
				moveCtx = sqlite.WithCommitTrace(moveCtx, sqliteTrace)
			}
			stopProfile := hierarchyMoveCPUProfile(t, engine)
			commit, err := f.writer.Apply(moveCtx, batchPrincipal, model.ChangeSet{Ops: []model.Op{hierarchyMove(subtree, &destination)}}, f.meta("hierarchy"))
			stopProfile()
			for _, sample := range sqliteTrace.Snapshot() {
				t.Logf("W3_HIERARCHY_MOVE_SQLITE_PHASE phase=%s elapsed=%s staged=%d failed=%t", sample.Phase, sample.Duration, sample.Staged, sample.Failed)
			}
			moveMillis := map[pipeline.Phase]float64{}
			for phase, results := range metrics.Snapshot().LaneMillis {
				for result, millis := range results {
					moveMillis[phase] += millis - beforeMove.LaneMillis[phase][result]
				}
			}
			t.Logf("%s unchanged 5000-row hierarchy move phase times (ms): %v", engine, moveMillis)
			trace.report(t)
			if err != nil {
				t.Fatal(err)
			}
			got := verifyHierarchy(t, f)
			if len(got)-len(rows) != 5000 {
				t.Fatalf("expected exactly 5000 added rows, got %d", len(got)-len(rows))
			}
			t.Logf("%s exact closure boundary: 5000 PASS at revision %s; 5001 closure_too_large with atomic rollback", engine, commit.Revision)
		})
	}
}

func TestHierarchyDerivedReaderOverlayAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			seed := hierarchyApply(t, f, hierarchyCreate("root", nil, "work"))
			id := seed.Results[0].Ref.ID
			row, _ := pipeline.AncestorRow(id, id, 0)
			prefix, _ := pipeline.AncestorPrefix(id)
			r := hierarchyReader(t, f)
			tx, err := f.store.Write(f.ctx, f.lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			sentinel, _ := driver.NewClosureSentinel(id)
			locks := []driver.LockRequest{{Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}, Mode: driver.LockUpdate}, {Key: driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}}, Mode: driver.LockUpdate}}
			// Restoring a typed annotation also attests its acquired source.
			ref := seed.Results[0].Ref
			locks = append(locks, driver.LockRequest{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockShare})
			sort.Slice(locks, func(i, j int) bool {
				a, _ := driver.EncodeLockKey(locks[i].Key)
				b, _ := driver.EncodeLockKey(locks[j].Key)
				return a < b
			})
			if _, err := tx.Lock(f.ctx, locks); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Head(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := tx.PutDerived(f.ctx, driver.DerivedAncestorClosure, []driver.DerivedRow{row}, nil); err != nil {
				t.Fatal(err)
			}
			staged, err := tx.DerivedRows(f.ctx, driver.DerivedAncestorClosure, prefix)
			if err != nil || len(staged) != 0 {
				t.Fatalf("transaction did not see staged removal: %+v %v", staged, err)
			}
			visible, err := r.DerivedRows(f.ctx, driver.DerivedAncestorClosure, prefix)
			if err != nil || len(visible) != 1 {
				t.Fatalf("uncommitted removal leaked: %+v %v", visible, err)
			}
			// Restore the exact committed image, including its typed annotation.
			// Reconstructing only Key/Value would be a raw untyped replacement.
			if err := pipeline.PutSelectionDerived(f.ctx, tx, driver.DerivedAncestorClosure, nil, visible); err != nil {
				t.Fatal(err)
			}
			staged, err = tx.DerivedRows(f.ctx, driver.DerivedAncestorClosure, prefix)
			if err != nil || !reflect.DeepEqual(staged, visible) {
				t.Fatalf("transaction did not see staged replacement: %+v %v", staged, err)
			}
			if err := tx.Rollback(f.ctx); err != nil {
				t.Fatal(err)
			}
			verifyHierarchy(t, f)
		})
	}
}

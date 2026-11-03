package postsql_test

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
)

// The existing mandatory SQL engines own PostgreSQL on the runner. Memory is
// an additional control, never a substitute for either SQL engine.
func approvalEngines(t *testing.T, run func(*testing.T, engine)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { run(t, engine{name: "memory", driver: memory.Driver{}}) })
	engines(t, run)
}

type approvalFixture struct {
	*fixture
	ontology *service.Ontology
	domain   *service.Service
}

func newApprovalFixture(t *testing.T, e engine) *approvalFixture {
	t.Helper()
	f := &approvalFixture{fixture: newFixture(t, e)}
	doc := ontology.DefaultDocument()
	doc.GateKinds = map[string]ontology.GateKind{
		"human_approval": {Resolver: "builtin.manual", Params: map[string]any{"min_approvals": 2}},
	}
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("compile approval ontology: %+v", problems)
	}
	body, err := service.MarshalOntologySource(service.OntologySource{Base: doc})
	if err != nil {
		t.Fatal(err)
	}
	// Only protected ontology configuration uses the fixture seam. Every gate
	// image, approval, slot transition, event and receipt uses the real service.
	if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Principal: writerPrincipal},
		[]conformance.FixtureChange{{Ref: sourceRef, Kind: "ontology", Op: model.OpPatch, Body: body}}); err != nil {
		t.Fatal(err)
	}
	f.ontology = service.NewOntology(func(project string) ontology.Effective { return snapshot.Effective(project) })
	f.bind()
	return f
}

func (f *approvalFixture) bind() {
	w := pipeline.New(f.store, f.lease,
		pipeline.WithOntology(f.ontology),
		pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(sourceRef))),
		pipeline.WithIDGenerator(func() (string, error) { return fmt.Sprintf("pl-approval-%d", f.ids.Add(1)), nil }))
	f.domain = service.New(f.store, f.lease, w, service.WithOntology(f.ontology))
}

func approvalMeta(n int) model.WriteMeta {
	return model.WriteMeta{Method: "gate.approve", Project: "post-sql", IdempotencyKey: fmt.Sprintf("%032x", n)}
}

func approvalUser(id string) model.PrincipalRef {
	return model.PrincipalRef{Kind: "user", ID: "user:" + id}
}

func approvalAgent(id, sponsor string) model.PrincipalRef {
	human := approvalUser(sponsor)
	return model.PrincipalRef{Kind: "agent", ID: "agent:" + id, Sponsor: &human}
}

func (f *approvalFixture) createGate(t *testing.T, template bool) (model.Ref, model.Revision) {
	t.Helper()
	out, err := f.domain.CreateWithOutcome(f.ctx, approvalUser("alice"), service.CreateRequest{
		Type: "issue", Fields: map[string]any{"title": "two human gate", "gate_kind": "human_approval", "template": template},
	}, approvalMeta(1001))
	if err != nil || len(out.Commit.Results) != 1 {
		t.Fatalf("create gate through Writer: %+v %v", out, err)
	}
	return out.Commit.Results[0].Ref, out.Commit.Results[0].Revision
}

func (f *approvalFixture) approve(t *testing.T, by model.PrincipalRef, ref model.Ref, n int, state string, count uint64) port.GateOutcome {
	t.Helper()
	out, err := f.domain.ApproveGate(f.ctx, by, port.GateApproveRequest{ID: ref}, approvalMeta(n))
	if err != nil {
		t.Fatalf("real gate approval: %v", err)
	}
	assertApproval(t, out, state, count)
	return out
}

func assertApproval(t *testing.T, out port.GateOutcome, state string, count uint64) {
	t.Helper()
	if out.Result.State != state || out.Result.Approvals.String() != fmt.Sprint(count) || len(out.Result.Commit.Results) != 1 {
		t.Fatalf("gate result: %+v", out)
	}
	doc, err := pipeline.DecodeDocument(out.Result.Commit.Results[0].Record)
	if err != nil {
		t.Fatal(err)
	}
	wantStatus := "open"
	if state == "resolved" {
		wantStatus = "done"
	}
	if doc.Fields["gate_state"] != state || doc.Fields["status"] != wantStatus || fmt.Sprint(doc.Fields["gate_approval_count"]) != fmt.Sprint(count) {
		t.Fatalf("evaluated state/count/slot absent from committed image: %+v", doc)
	}
	var observations []ontology.GateApproval
	if err := json.Unmarshal(encoded(t, doc.Fields["gate_approvals"]), &observations); err != nil || len(observations) != int(count) {
		t.Fatalf("durable approval observations: %+v %v", observations, err)
	}
}

type approvalState struct {
	Graph graphState
	Locks []driver.LockState
}

func (f *approvalFixture) state(t *testing.T, ref model.Ref) approvalState {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	ready, err := driver.NewReadySentinel("post-sql", "issue")
	if err != nil {
		t.Fatal(err)
	}
	bead, err := driver.NewBeadSentinel(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	keys := []driver.LockKey{
		{Kind: driver.LockRecord, Ref: &ref}, {Kind: driver.LockRecord, Ref: &sourceRef},
		driver.OntologySentinelLock, driver.GraphBlockingSentinelLock,
		{Kind: driver.LockSentinel, Sentinel: ready}, {Kind: driver.LockSentinel, Sentinel: bead},
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks, err := r.Observe(f.ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	return approvalState{Graph: f.graph(t, ref), Locks: locks}
}

func (f *approvalFixture) approvalReceipt(t *testing.T, by model.PrincipalRef, m model.WriteMeta) *driver.IdempotencyRecord {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := r.IdempotencyByKey(f.ctx, principal, "", "gate.approve", m.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func (f *approvalFixture) noApprovalReceipt(t *testing.T, by model.PrincipalRef, m model.WriteMeta) {
	t.Helper()
	if entry := f.approvalReceipt(t, by, m); entry != nil {
		t.Fatalf("refused approval persisted a receipt: %+v", entry)
	}
}

func (f *approvalFixture) approvalEvents(t *testing.T, out port.GateOutcome, types ...model.DurableEventType) {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	changes, err := r.Changesets(f.ctx, nil, 100)
	if err != nil || len(changes) == 0 {
		t.Fatalf("read committed event log: %+v %v", changes, err)
	}
	last := changes[len(changes)-1]
	if last.Revision != out.Result.Commit.Revision || len(last.Changes) != 1 || len(last.Events) != len(types) {
		t.Fatalf("approval and event plan did not commit together: %+v", last)
	}
	for i, typ := range types {
		if last.Events[i].Type != typ {
			t.Fatalf("event %d: got %s want %s", i, last.Events[i].Type, typ)
		}
	}
}

func TestApprovalGateCountsSponsorResolvedHumansAcrossDrivers(t *testing.T) {
	approvalEngines(t, func(t *testing.T, e engine) {
		f := newApprovalFixture(t, e)
		ref, _ := f.createGate(t, false)
		first := f.approve(t, approvalAgent("alice-one", "alice"), ref, 1011, "pending", 1)
		f.approvalEvents(t, first, model.EventGateApproved)
		before := f.state(t, ref)
		for i, by := range []model.PrincipalRef{approvalAgent("alice-two", "alice"), approvalUser("alice")} {
			f.approve(t, by, ref, 1012+i, "pending", 1)
			same(t, before, f.state(t, ref))
		}
		if e.name != "memory" {
			f.reopen(t)
			f.bind()
			same(t, before, f.state(t, ref))
		}
		second := f.approve(t, approvalAgent("bob-one", "bob"), ref, 1014, "resolved", 2)
		f.approvalEvents(t, second, model.EventBeadClosed, model.EventGateApproved)
		entry := f.approvalReceipt(t, approvalAgent("bob-one", "bob"), approvalMeta(1014))
		if entry == nil {
			t.Fatal("successful resolution has no durable receipt")
		}
		same(t, second.Result.Commit, entry.Commit)
		resolved := f.state(t, ref)
		replay := f.approve(t, approvalAgent("bob-one", "bob"), ref, 1014, "resolved", 2)
		if !replay.Replayed {
			t.Fatal("terminal approval did not replay its receipt")
		}
		same(t, second.Result, replay.Result)
		same(t, resolved, f.state(t, ref))
	})
}

func TestApprovalGateRefusalsLeaveNoPartialStateAcrossDrivers(t *testing.T) {
	approvalEngines(t, func(t *testing.T, e engine) {
		f := newApprovalFixture(t, e)
		ref, createdRevision := f.createGate(t, true)
		f.approve(t, approvalUser("alice"), ref, 1021, "pending", 1)
		before := f.state(t, ref)
		for i, tc := range []struct {
			name   string
			by     model.PrincipalRef
			expect []model.Precondition
			code   model.ErrorCode
		}{
			{name: "anonymous", by: model.PrincipalRef{Kind: "anonymous"}, code: "forbidden"},
			{name: "unsponsored", by: model.PrincipalRef{Kind: "agent", ID: "agent:orphan"}, code: "forbidden"},
			{name: "stale-revision", by: approvalUser("bob"), expect: []model.Precondition{{Ref: ref, Revision: &createdRevision}}, code: "precondition_failed"},
			{name: "close-guard", by: approvalUser("bob"), code: "ontology_guard_failed"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				m := approvalMeta(1022 + i)
				_, err := f.domain.ApproveGate(f.ctx, tc.by, port.GateApproveRequest{ID: ref, Expect: tc.expect}, m)
				if code(err) != tc.code {
					t.Fatalf("refusal: got %v want %s", err, tc.code)
				}
				same(t, before, f.state(t, ref))
				f.noApprovalReceipt(t, tc.by, m)
			})
		}
		if e.name != "memory" {
			f.reopen(t)
			f.bind()
			same(t, before, f.state(t, ref))
			f.noApprovalReceipt(t, approvalUser("bob"), approvalMeta(1025))
		}
		if _, err := f.domain.Update(f.ctx, approvalUser("alice"), service.UpdateRequest{ID: ref, Fields: map[string]any{"template": false}}, approvalMeta(1026)); err != nil {
			t.Fatal(err)
		}
		// A guard refusal must leave the key available for the complete approval.
		retried := f.approve(t, approvalUser("bob"), ref, 1025, "resolved", 2)
		if retried.Replayed {
			t.Fatal("refused approval was treated as a committed replay")
		}
		f.approvalEvents(t, retried, model.EventBeadClosed, model.EventGateApproved)
	})
}

func TestApprovalGateLateEventFailureRollsBackOnSQL(t *testing.T) {
	engines(t, func(t *testing.T, e engine) {
		f := newApprovalFixture(t, e)
		ref, _ := f.createGate(t, false)
		f.approve(t, approvalUser("alice"), ref, 1031, "pending", 1)
		before := f.state(t, ref)
		install := `CREATE TRIGGER i4_gate_reject_log BEFORE INSERT ON changesets BEGIN
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM versions WHERE rev=NEW.rev) THEN RAISE(ABORT,'i4_gate_version_not_staged') END;
SELECT RAISE(ABORT,'i4_gate_reject_log'); END;`
		drop := "DROP TRIGGER i4_gate_reject_log;"
		if e.name == "postgres" {
			install = `CREATE FUNCTION plug_postgres.i4_gate_reject_log() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM plug_postgres.versions WHERE revision=NEW.revision) THEN RAISE EXCEPTION 'i4_gate_version_not_staged'; END IF;
RAISE EXCEPTION 'i4_gate_reject_log'; RETURN NEW; END $$;
CREATE TRIGGER i4_gate_reject_log BEFORE INSERT ON plug_postgres.changesets FOR EACH ROW EXECUTE FUNCTION plug_postgres.i4_gate_reject_log();`
			drop = "DROP TRIGGER i4_gate_reject_log ON plug_postgres.changesets; DROP FUNCTION plug_postgres.i4_gate_reject_log();"
		}
		e.sql(t, install)
		_, err := f.domain.ApproveGate(f.ctx, approvalUser("bob"), port.GateApproveRequest{ID: ref}, approvalMeta(1032))
		if err == nil || !strings.Contains(err.Error(), "i4_gate_reject_log") {
			t.Fatalf("approval did not reach late event-log insertion after version staging: %v", err)
		}
		same(t, before, f.state(t, ref))
		f.noApprovalReceipt(t, approvalUser("bob"), approvalMeta(1032))
		f.reopen(t)
		f.bind()
		same(t, before, f.state(t, ref))
		f.noApprovalReceipt(t, approvalUser("bob"), approvalMeta(1032))
		e.sql(t, drop)
		out := f.approve(t, approvalUser("bob"), ref, 1032, "resolved", 2)
		if out.Replayed {
			t.Fatal("late rollback left a replay receipt")
		}
		f.approvalEvents(t, out, model.EventBeadClosed, model.EventGateApproved)
		entry := f.approvalReceipt(t, approvalUser("bob"), approvalMeta(1032))
		if entry == nil || len(entry.Changeset.Events) != 2 {
			t.Fatal("successful retry did not persist the complete event/receipt plan")
		}
		same(t, out.Result.Commit, entry.Commit)
	})
}

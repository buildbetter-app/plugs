package b01readyorder_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// This fixture creates every work item and relationship through public RPC.
// Only the host ontology, clock and deterministic root-ID source are configured
// directly. The workflow provider uses the selected driver's actual lifetime;
// memory remains process-local and SQL checkpoints survive actual close/open.
type publicAcceptance struct {
	*fixture
	engine       string
	dsn          string
	storage      *testclock.Clock
	workflow     bool
	rootID       string
	writes       []publicAcceptanceWrite
	workflowRows map[string]publicWorkflowValue
	childCounts  map[string]int
}

type publicWorkflowValue struct {
	ID, Type, Project, Status, Plane, Kind string
	WorkflowRunID                          string `json:"workflowRunId"`
	CreatedAt                              time.Time
	Fields                                 map[string]any
	Links                                  []workflowplugin.RelationView
	Parent                                 string
}

type publicAcceptanceWrite struct {
	method string
	key    string
	commit model.Commit
}

func publicAcceptanceOntology(t *testing.T) *service.Ontology {
	t.Helper()
	d := ontology.DefaultDocument()
	aliases := map[string]string{"todo": "native-active", "in_progress": "native-wip", "done": "native-done", "backlog": "native-frozen"}
	categories := map[string]ontology.Category{"todo": ontology.Active, "in_progress": ontology.WIP, "done": ontology.Done, "backlog": ontology.Frozen}
	// Clone the built-in workflow with exact native category names, retaining
	// every transition and guard. No ordinary status patch bypass is installed.
	raw := jsonBytes(t, d.Workflows["issue"])
	var w ontology.Workflow
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	name := func(status string) string {
		if alias, ok := aliases[status]; ok {
			return alias
		}
		return status
	}
	for original, alias := range aliases {
		status := d.Statuses[original]
		status.Name, status.Category = alias, categories[original]
		d.Statuses[alias] = status
	}
	for i := range w.Statuses {
		w.Statuses[i] = name(w.Statuses[i])
	}
	w.Initial = name(w.Initial)
	for slot, status := range w.Slots {
		w.Slots[slot] = name(status)
	}
	for i := range w.Transitions {
		w.Transitions[i].To = name(w.Transitions[i].To)
		for j := range w.Transitions[i].From {
			w.Transitions[i].From[j] = name(w.Transitions[i].From[j])
		}
	}
	d.Workflows["native-work"] = w
	native := d.Types["issue"]
	native.Name, native.Workflow = "Native work", "native-work"
	d.Types["native-work"] = native
	no := false
	infrastructure := d.Types["issue"]
	infrastructure.Name, infrastructure.ReadyEligible = "Hidden infrastructure", &no
	d.Types["infrastructure"] = infrastructure
	snapshot, problems := ontology.Compile(d, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("public B01 ontology: %+v", problems)
	}
	return service.NewOntology(snapshot.Effective)
}

func newPublicAcceptance(t *testing.T, engine string, withWorkflow bool) *publicAcceptance {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	at := time.Date(2026, 11, 3, 12, 0, 0, 123456000, time.UTC)
	p := &publicAcceptance{fixture: &fixture{ctx: ctx, clock: testclock.New(t, at), onto: publicAcceptanceOntology(t)}, engine: engine, storage: testclock.New(t, at.Add(7*time.Hour)), workflow: withWorkflow, workflowRows: map[string]publicWorkflowValue{}, childCounts: map[string]int{}}
	switch engine {
	case "sqlite_wal":
		p.dsn = filepath.Join(t.TempDir(), "public-ready.db")
	case "postgres":
		p.dsn = p.storage.BindPostgres(t, testclock.PostgresDSN(t))
		p.sqlDSN = p.dsn
	}
	p.open(t)
	t.Cleanup(func() {
		if p.lease != nil {
			if err := p.lease.Release(context.Background()); err != nil {
				t.Error(err)
			}
		}
		if err := p.store.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func (p *publicAcceptance) open(t *testing.T) {
	t.Helper()
	var err error
	switch p.engine {
	case "memory":
		p.store, err = memory.NewWithOptions(memory.Options{Clock: p.storage.Now})
	case "sqlite_wal":
		p.store, err = (sqlite.Driver{Clock: p.storage.Now}).Open(p.ctx, driver.Config{DSN: p.dsn})
	case "postgres":
		p.store, err = (postgres.Driver{}).Open(p.ctx, driver.Config{DSN: p.dsn})
	default:
		t.Fatalf("unknown public B01 engine %s", p.engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	p.bind(t)
}

func (p *publicAcceptance) bind(t *testing.T) {
	t.Helper()
	var err error
	p.lease, err = p.store.Lease(p.ctx, "b01-public-acceptance", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	w := pipeline.New(p.store, p.lease, pipeline.WithOntology(p.onto), pipeline.WithClock(p.clock.Now), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "b01-publication", Subscriber: publicationProbe{&p.posts}}), pipeline.WithIDGenerator(func() (string, error) {
		if p.rootID == "" {
			return "", fmt.Errorf("public acceptance exhausted its declared root identity")
		}
		id := p.rootID
		p.rootID = ""
		return id, nil
	}))
	options := []service.Option{service.WithOntology(p.onto), service.WithClock(p.clock.Now)}
	if p.workflow {
		checkpoints, err := driver.RequireWorkflowCheckpointStore(p.store, p.lease)
		if err != nil {
			t.Fatal(err)
		}
		state, err := pluginhost.NewWorkflowRuntimeState(checkpoints, p.lease)
		if err != nil {
			t.Fatal(err)
		}
		wantLifetime, wantPersistent := driver.CheckpointLifetimeRestart, true
		if p.engine == "memory" {
			wantLifetime, wantPersistent = driver.CheckpointLifetimeProcess, false
		}
		marker, ok := state.(interface{ Persistent() bool })
		if !ok || marker.Persistent() != wantPersistent || checkpoints.WorkflowCheckpointLifetime() != wantLifetime {
			t.Fatalf("workflow runtime changed selected-store lifetime: engine=%s lifetime=%s want=%s persistent=%v marker=%t", p.engine, checkpoints.WorkflowCheckpointLifetime(), wantLifetime, state, ok)
		}
		t.Logf("B01 workflow_lifetime engine=%s checkpoint_lifetime=%s persistent=%t", p.engine, wantLifetime, marker.Persistent())
		provider, err := workflowplugin.New(state)
		if err != nil {
			t.Fatal(err)
		}
		options = append(options, service.WithWorkflow(provider.WithClock(p.clock.Now).WithIdentity(workflowplugin.Alias, "default", p.meta(t).Incarnation)))
	}
	p.svc = service.New(p.store, p.lease, w, options...)
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, p.svc); err != nil {
		t.Fatal(err)
	}
	p.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: p.meta(t).Incarnation, CursorKey: []byte("b01readyorder-disposable-stable-key")})
	if err != nil {
		t.Fatal(err)
	}
}

func (p *publicAcceptance) write(t *testing.T, method, key string, params map[string]any) model.Commit {
	t.Helper()
	commit := publicWrite(t, p.fixture, method, key, params)
	p.writes = append(p.writes, publicAcceptanceWrite{method, key, commit})
	return commit
}

func (p *publicAcceptance) create(t *testing.T, label, typ, target string, priority int, parent *model.Ref) model.Ref {
	t.Helper()
	p.rootID = "pl-" + label
	params := map[string]any{"type": typ, "fields": map[string]any{"title": label, "priority": priority}}
	if target != "" {
		params["targetStatus"] = target
	}
	if parent != nil {
		params["parent"] = *parent
	}
	commit := p.write(t, "bead.create", "create-"+label, params)
	ref := commit.Results[0].Ref
	wantID := "pl-" + label
	if parent != nil {
		p.childCounts[parent.ID]++
		wantID = fmt.Sprintf("%s.%d", parent.ID, p.childCounts[parent.ID])
	}
	if ref.ID != wantID {
		t.Fatalf("public creation returned undeclared identity: %+v parent=%+v", ref, parent)
	}
	connectedFact(t, p.fixture, ref, commit.Revision, p.clock.Now(), p.clock.Now(), 0, priority)
	status := target
	if status == "" {
		status = "todo"
		if typ == "native-work" {
			status = "native-active"
		}
	}
	p.document(t, ref, typ, status, label, priority, parent)
	return ref
}

func (p *publicAcceptance) document(t *testing.T, ref model.Ref, typ, status, title string, priority int, parent *model.Ref) {
	t.Helper()
	r := p.reader(t)
	rows, err := r.Get(p.ctx, []model.Ref{ref})
	_ = r.Close()
	if err != nil || len(rows) != 1 {
		t.Fatalf("actual committed readback %s: %+v %v", ref.ID, rows, err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	wantFields := map[string]any{"title": title, "priority": priority, "status": status, "template": false}
	if err != nil || doc.ID != ref.ID || doc.Type != typ || doc.Project != project || !reflect.DeepEqual(doc.Parent, parent) || !reflect.DeepEqual(jsonBytes(t, doc.Fields), jsonBytes(t, wantFields)) {
		t.Fatalf("actual committed values %s: %+v err=%v wantFields=%s parent=%+v", ref.ID, doc, err, jsonBytes(t, wantFields), parent)
	}
	response := p.call(t, "bead.get", map[string]any{"id": ref, "_meta": map[string]any{"project": project}})
	var got struct {
		Bead struct {
			ID, Type, Project, Status string
			Revision                  model.Revision
			Fields                    map[string]any
			Parent                    string
		}
	}
	wantParent := ""
	if parent != nil {
		wantParent = parent.ID
	}
	if response.Error != nil || json.Unmarshal(response.Result, &got) != nil || got.Bead.ID != ref.ID || got.Bead.Revision != rows[0].Revision || got.Bead.Type != typ || got.Bead.Project != project || got.Bead.Status != status || got.Bead.Parent != wantParent || !reflect.DeepEqual(jsonBytes(t, got.Bead.Fields), jsonBytes(t, wantFields)) {
		t.Fatalf("public get does not match exact committed values: %s", response.Result)
	}
	t.Logf("B01 committed_readback engine=%s id=%s revision=%s status=%s fields=%s", p.engine, ref.ID, rows[0].Revision.String(), status, jsonBytes(t, wantFields))
}

func (p *publicAcceptance) checkpoint(t *testing.T) driver.Checkpoint {
	t.Helper()
	cp, err := p.store.(driver.CheckpointStore).LoadCheckpoint(p.ctx, readyWorkflowCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	return cp
}

func (p *publicAcceptance) receipts(t *testing.T) []*driver.IdempotencyRecord {
	t.Helper()
	result := make([]*driver.IdempotencyRecord, len(p.writes))
	for i, write := range p.writes {
		receipt := connectedReceipt(t, p.fixture, write.method, "", write.key)
		if receipt == nil || receipt.ResponsePending || receipt.Commit.Revision != write.commit.Revision {
			t.Fatalf("public write lost its exact final receipt: %+v receipt=%+v", write, receipt)
		}
		result[i] = receipt
	}
	return result
}

func (p *publicAcceptance) point(t *testing.T, label string, filter map[string]any, want ...string) {
	t.Helper()
	t.Run(label, func(t *testing.T) {
		before, checkpoint, receipts := p.state(t), p.checkpoint(t), p.receipts(t)
		p.pages(t, filter, want...)
		p.workflowReadback(t)
		if !reflect.DeepEqual(before, p.state(t)) || !reflect.DeepEqual(checkpoint, p.checkpoint(t)) || !reflect.DeepEqual(receipts, p.receipts(t)) {
			t.Fatal("public ready changed committed records, ranks, history, receipt or workflow bytes")
		}
		if p.engine != "memory" {
			if err := p.lease.Release(p.ctx); err != nil {
				t.Fatal(err)
			}
			p.lease = nil
			if err := p.store.Close(); err != nil {
				t.Fatal(err)
			}
			p.open(t)
			if !reflect.DeepEqual(before, p.state(t)) || !reflect.DeepEqual(checkpoint, p.checkpoint(t)) || !reflect.DeepEqual(receipts, p.receipts(t)) {
				t.Fatal("actual SQL close/open changed committed records, ranks, history, receipts or workflow bytes")
			}
			p.pages(t, filter, want...)
			p.workflowReadback(t)
			if !reflect.DeepEqual(before, p.state(t)) || !reflect.DeepEqual(checkpoint, p.checkpoint(t)) || !reflect.DeepEqual(receipts, p.receipts(t)) {
				t.Fatal("reopened public reads changed committed records, ranks, history, receipts or workflow bytes")
			}
		}
		if p.engine == "postgres" && !testclock.PostgresNow(t, p.dsn).Equal(p.storage.Now()) {
			t.Fatal("SQL reopen lost the independently controlled driver clock")
		}
		t.Logf("B01 public_point engine=%s point=%s application=%s driver=%s sql_reopen=%t expected=%v", p.engine, label, p.clock.Now().Format(time.RFC3339Nano), p.storage.Now().Format(time.RFC3339Nano), p.engine != "memory", want)
	})
}

func TestB01PublicMembershipTransitionsPersistBeforeEveryPage(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite_wal", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPublicAcceptance(t, engine, false)
			blocker := p.create(t, "blocker", "issue", "", 0, nil)
			dependent := p.create(t, "dependent", "issue", "", 0, nil)
			parent := p.create(t, "parent", "issue", "", 3, nil)
			child := p.create(t, "child", "issue", "", 1, &parent)
			grandchild := p.create(t, "grandchild", "issue", "", 2, &child)
			active := p.create(t, "custom-active", "native-work", "native-active", 2, nil)
			wip := p.create(t, "custom-wip", "native-work", "native-wip", 0, nil)
			frozen := p.create(t, "custom-frozen", "native-work", "native-frozen", 0, nil)
			closed := p.create(t, "closed", "issue", "", 0, nil)
			p.create(t, "hidden", "infrastructure", "", 0, nil)
			template := p.create(t, "template", "issue", "", 0, nil)
			p.write(t, "bead.update", "mark-template", map[string]any{"id": template, "fields": map[string]any{"template": true}})
			p.write(t, "bead.close", "close-nonready", map[string]any{"id": closed})
			p.write(t, "bead.link", "dependent-blocker", map[string]any{"from": dependent, "to": blocker, "kind": "blocks"})
			p.document(t, active, "native-work", "native-active", "custom-active", 2, nil)
			p.document(t, wip, "native-work", "native-wip", "custom-wip", 0, nil)
			p.document(t, frozen, "native-work", "native-frozen", "custom-frozen", 0, nil)
			p.point(t, "open-parent-is-not-blocking", nil, blocker.ID, child.ID, active.ID, grandchild.ID, parent.ID)
			p.point(t, "transitive-parent-before-page", map[string]any{"parent": parent.ID}, child.ID, grandchild.ID)
			p.point(t, "priority-before-page", map[string]any{"priority": 2}, active.ID, grandchild.ID)
			p.write(t, "bead.defer", "defer-parent", map[string]any{"id": parent, "reason": "public defer"})
			p.document(t, parent, "issue", "backlog", "parent", 3, nil)
			p.point(t, "deferred-parent-suppresses-descendants-immediately", nil, blocker.ID, active.ID)
			p.write(t, "bead.reopen", "reopen-parent", map[string]any{"id": parent})
			p.write(t, "bead.link", "parent-blocker", map[string]any{"from": parent, "to": blocker, "kind": "blocks"})
			p.point(t, "blocked-parent-suppresses-descendants", nil, blocker.ID, active.ID)
			p.write(t, "bead.close", "close-blocker", map[string]any{"id": blocker})
			p.document(t, blocker, "issue", "done", "blocker", 0, nil)
			p.point(t, "closed-blocker-releases-full-set-before-page", nil, dependent.ID, child.ID, active.ID, grandchild.ID, parent.ID)
			p.write(t, "bead.reopen", "reopen-blocker", map[string]any{"id": blocker})
			p.point(t, "reopened-blocker-restores-exact-exclusions", nil, blocker.ID, active.ID)
		})
	}
}

func TestB01IndependentClockRankLagSurvivesActualSQLReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite_wal", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPublicAcceptance(t, engine, false)
			base := p.clock.Now()
			old := p.create(t, "old-p4", "issue", "", 4, nil)
			p.clock.Advance(t, 24*time.Hour)
			aging := p.create(t, "aging-p0", "issue", "", 0, nil)
			p.clock.Advance(t, 48*time.Hour-time.Nanosecond)
			recentZ := p.create(t, "recent-z", "issue", "", 2, nil)
			recentA := p.create(t, "recent-a", "issue", "", 2, nil)
			p.clock.Advance(t, time.Nanosecond)
			recentLater := p.create(t, "recent-0-later", "issue", "", 2, nil)
			cutoff := p.clock.Now()
			update := p.write(t, "bead.update", "rank-at-inclusive-cutoff", map[string]any{"id": aging, "fields": map[string]any{"title": "ranked at cutoff"}})
			stored := connectedFact(t, p.fixture, aging, update.Revision, base.Add(24*time.Hour), cutoff, 0, 0)
			// Explicit expected tuples: recent p0, two equal-created p2 IDs,
			// later-created p2 despite its earlier ID, then old bucket rank999.
			p.point(t, "inclusive-48h-created-id-ties", nil, aging.ID, recentA.ID, recentZ.ID, recentLater.ID, old.ID)
			p.clock.Advance(t, time.Nanosecond)
			p.point(t, "P74-before-60s", nil, aging.ID, recentA.ID, recentZ.ID, recentLater.ID, old.ID)
			p.clock.Advance(t, time.Minute-time.Nanosecond)
			p.point(t, "P74-at-60s", nil, aging.ID, recentA.ID, recentZ.ID, recentLater.ID, old.ID)
			p.clock.Advance(t, time.Nanosecond)
			p.point(t, "P74-after-60s-old-bucket-ignores-priority", nil, recentA.ID, recentZ.ID, recentLater.ID, old.ID, aging.ID)
			if !reflect.DeepEqual(stored, connectedFact(t, p.fixture, aging, update.Revision, base.Add(24*time.Hour), cutoff, 0, 0)) || !p.storage.Now().Equal(base.Add(7*time.Hour)) {
				t.Fatal("read fallback changed stored rank or coupled independent clocks")
			}
			p.document(t, aging, "issue", "todo", "ranked at cutoff", 0, nil)
		})
	}
}

func (p *publicAcceptance) invoke(t *testing.T, label string, checklist []map[string]any) []workflowplugin.ItemView {
	t.Helper()
	response := p.call(t, "x.workflow.invoke", map[string]any{"template": label, "project": project, "mode": "ephemeral", "checklist": checklist, "_meta": map[string]any{"project": project, "idempotencyKey": fixtureKey("invoke-" + label)}})
	var result workflowplugin.InvokeResult
	if response.Error != nil || json.Unmarshal(response.Result, &result) != nil || result.Plane != "ephemeral" || len(result.Items) != len(checklist) || result.Run == nil {
		t.Fatalf("actual public workflow invocation failed: %+v %s", response.Error, response.Result)
	}
	for i, item := range result.Items {
		if item.ID == "" || item.Label != checklist[i]["label"] || item.Plane != "ephemeral" || item.Kind != "workflow.item" || item.Title != checklist[i]["title"] || item.Project != project || item.RunID != result.Run.ID || !result.Run.CreatedAt.Equal(p.clock.Now()) {
			t.Fatalf("public workflow returned different committed values: %+v", item)
		}
		wantFields := map[string]any{"title": checklist[i]["title"]}
		if fields, ok := checklist[i]["fields"].(map[string]any); ok {
			for key, value := range fields {
				wantFields[key] = value
			}
		}
		var wantLinks []workflowplugin.RelationView
		if links, ok := checklist[i]["links"].([]map[string]any); ok {
			for _, link := range links {
				wantLinks = append(wantLinks, workflowplugin.RelationView{From: item.ID, To: link["to"].(model.Ref), Kind: link["kind"].(string)})
			}
		}
		wantParent := ""
		if parent, ok := checklist[i]["parent"].(model.Ref); ok {
			wantParent = parent.ID
		}
		// Expectations are the literal public inputs plus the returned opaque
		// item/run identities. Derived Blocked can legitimately change after a
		// core blocker closes; it is checked independently by the ready vectors.
		p.workflowRows[item.ID] = publicWorkflowValue{ID: item.ID, Type: checklist[i]["type"].(string), Project: project, Status: "open", Plane: "ephemeral", Kind: "workflow.item", WorkflowRunID: result.Run.ID, CreatedAt: p.clock.Now(), Fields: wantFields, Links: wantLinks, Parent: wantParent}
	}
	p.workflowReadback(t)
	return result.Items
}

func (p *publicAcceptance) workflowReadback(t *testing.T) {
	t.Helper()
	for id, want := range p.workflowRows {
		response := p.call(t, "bead.get", map[string]any{"id": model.RefByID(id, model.Natural64(0)), "_meta": map[string]any{"project": project}})
		var get struct{ Bead publicWorkflowValue }
		if response.Error != nil || json.Unmarshal(response.Result, &get) != nil || !reflect.DeepEqual(jsonBytes(t, get.Bead), jsonBytes(t, want)) {
			t.Fatalf("public workflow exact input/readback differs for %s: got=%s want=%s", id, response.Result, jsonBytes(t, want))
		}
	}
}

func TestB01PositiveDurableAndEphemeralMembershipPrecedesEveryPage(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite_wal", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPublicAcceptance(t, engine, true)
			blocker := p.create(t, "mixed-blocker", "issue", "in_progress", 0, nil)
			blocked := p.create(t, "mixed-blocked", "issue", "", 0, nil)
			p.write(t, "bead.link", "mixed-core-blocker", map[string]any{"from": blocked, "to": blocker, "kind": "blocks"})
			free := p.create(t, "mixed-free", "issue", "", 3, nil)
			items := p.invoke(t, "public-mixed", []map[string]any{
				{"label": "blocked", "title": "blocked workflow", "type": "issue", "fields": map[string]any{"priority": 0}, "links": []map[string]any{{"kind": "blocks", "to": blocker}}},
				{"label": "ready", "title": "ready workflow", "type": "issue", "fields": map[string]any{"priority": 1}},
			})
			workflowBlocked, workflowFree := model.RefByID(items[0].ID, model.Natural64(0)), model.RefByID(items[1].ID, model.Natural64(0))
			p.point(t, "positive-two-plane-filter-before-page", nil, workflowFree.ID, free.ID)
			p.point(t, "resources-filter-selects-actual-ephemeral-row", map[string]any{"resources": []string{workflowFree.ID}}, workflowFree.ID)
			p.point(t, "exclude-ephemeral-keeps-complete-durable-page", map[string]any{"includeEphemeral": false}, free.ID)
			p.write(t, "bead.close", "mixed-close-blocker", map[string]any{"id": blocker})
			// The p0 durable ID sorts before the workflow ID. Both formerly
			// blocked endpoints must join before limit one is applied.
			p.point(t, "cross-plane-blocker-close-releases-both", nil, blocked.ID, workflowBlocked.ID, workflowFree.ID, free.ID)
			p.write(t, "bead.reopen", "mixed-reopen-blocker", map[string]any{"id": blocker})
			p.point(t, "cross-plane-blocker-reopen-excludes-both", nil, workflowFree.ID, free.ID)
		})
	}
}

func TestB01MixedParentAndPriorityFiltersUseCompleteAncestryBeforePage(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite_wal", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPublicAcceptance(t, engine, true)
			root := p.create(t, "mixed-root", "issue", "", 4, nil)
			middle := p.create(t, "nonready-middle", "issue", "in_progress", 0, &root)
			leaf := p.create(t, "durable-leaf", "issue", "", 2, &middle)
			outsideBlocker := p.create(t, "outside-blocker", "issue", "in_progress", 0, nil)
			blocked := p.create(t, "subtree-blocked", "issue", "", 0, &root)
			p.write(t, "bead.link", "outside-subtree-blocker", map[string]any{"from": blocked, "to": outsideBlocker, "kind": "blocks"})
			p.create(t, "outside-ready", "issue", "", 1, nil)
			first := p.invoke(t, "mixed-parent", []map[string]any{{"label": "parent", "title": "workflow parent", "type": "issue", "parent": middle, "fields": map[string]any{"priority": 1}}})
			workflowParent := model.RefByID(first[0].ID, model.Natural64(0))
			second := p.invoke(t, "mixed-grandchild", []map[string]any{{"label": "grandchild", "title": "workflow grandchild", "type": "issue", "parent": workflowParent, "fields": map[string]any{"priority": 0}}})
			workflowGrandchild := model.RefByID(second[0].ID, model.Natural64(0))
			underWorkflow := p.create(t, "under-workflow", "issue", "", 2, nil)
			linkParams := map[string]any{"from": underWorkflow, "to": workflowParent, "kind": "parent-child", "_meta": map[string]any{"project": project, "idempotencyKey": fixtureKey("ordinary-under-workflow-parent")}}
			link := p.call(t, "bead.link", linkParams)
			var linked model.Commit
			if link.Error != nil || json.Unmarshal(link.Result, &linked) != nil || !linked.Revision.IsZero() || len(linked.Results) != 1 || linked.Results[0].Ref != workflowParent || linked.Results[0].Revision.String() != "1" {
				t.Fatalf("public mixed link lost its actual plugin commit: error=%+v result=%s", link.Error, link.Result)
			}
			// This public link writes an inbound provider relation, independently
			// expected here; it never fabricates an ordinary stored parent field.
			parentValue := p.workflowRows[workflowParent.ID]
			parentValue.Links = append(parentValue.Links, workflowplugin.RelationView{From: underWorkflow.ID, To: workflowParent, Kind: "parent-child"})
			p.workflowRows[workflowParent.ID] = parentValue
			p.workflowRows[underWorkflow.ID] = publicWorkflowValue{ID: underWorkflow.ID, Type: "issue", Project: project, Status: "todo", CreatedAt: p.clock.Now(), Fields: map[string]any{"title": "under-workflow", "priority": 2, "status": "todo", "template": false}, Parent: workflowParent.ID, Links: []workflowplugin.RelationView{{From: underWorkflow.ID, To: workflowParent, Kind: "parent-child"}}}
			p.point(t, "mixed-transitive-through-nonready-middle", map[string]any{"parent": root.ID}, workflowGrandchild.ID, workflowParent.ID, leaf.ID, underWorkflow.ID)
			p.point(t, "mixed-priority-selects-ephemeral-before-page", map[string]any{"parent": root.ID, "priority": 1}, workflowParent.ID)
			p.point(t, "mixed-priority-excludes-blocked-and-started-front", map[string]any{"parent": root.ID, "priority": 0}, workflowGrandchild.ID)
			p.point(t, "workflow-root-selects-both-child-planes", map[string]any{"parent": workflowParent.ID}, workflowGrandchild.ID, underWorkflow.ID)
			p.point(t, "excluded-ephemeral-remains-an-ancestor", map[string]any{"parent": root.ID, "includeEphemeral": false}, leaf.ID, underWorkflow.ID)
			p.point(t, "mixed-resources-intersect-complete-subtree", map[string]any{"parent": root.ID, "resources": []string{blocked.ID, workflowGrandchild.ID, underWorkflow.ID}}, workflowGrandchild.ID, underWorkflow.ID)
			p.point(t, "unknown-parent-is-empty", map[string]any{"parent": "pl-unknown-root"})
			p.write(t, "bead.defer", "defer-mixed-middle", map[string]any{"id": middle})
			p.point(t, "defer-suppresses-descendants-across-both-planes", map[string]any{"parent": root.ID})
			p.write(t, "bead.reopen", "reopen-mixed-middle", map[string]any{"id": middle})
			p.point(t, "reopen-restores-two-plane-descendants", map[string]any{"parent": root.ID}, middle.ID, workflowGrandchild.ID, workflowParent.ID, leaf.ID, underWorkflow.ID)
			p.write(t, "bead.close", "close-outside-subtree-blocker", map[string]any{"id": outsideBlocker})
			p.point(t, "outside-blocker-close-releases-selected-member", map[string]any{"parent": root.ID, "priority": 0}, middle.ID, blocked.ID, workflowGrandchild.ID)
			beforeReplay, replayCheckpoint, replayReceipts := p.state(t), p.checkpoint(t), p.receipts(t)
			replay := p.call(t, "bead.link", linkParams)
			if replay.Error != nil || !reflect.DeepEqual(replay.Result, link.Result) || !reflect.DeepEqual(beforeReplay, p.state(t)) || !reflect.DeepEqual(replayCheckpoint, p.checkpoint(t)) || !reflect.DeepEqual(replayReceipts, p.receipts(t)) {
				t.Fatal("SQL-reopened mixed relation lost its exact plugin receipt or republished state")
			}
		})
	}
}

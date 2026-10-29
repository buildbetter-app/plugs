package auditsql_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var writerPrincipal = model.PrincipalRef{Kind: "user", ID: "audit-sql-writer"}
var readerPrincipal = model.PrincipalRef{Kind: "user", ID: "audit-sql-reader"}
var fixturePrincipal = model.PrincipalRef{Kind: "system", ID: "audit-sql-source-fixture"}

const blockerProject = "blocker-project"
const dependentProject = "dependent-project"

func key(n int) string { return fmt.Sprintf("%032x", n) }

type fixture struct {
	ctx       context.Context
	engine    engine
	store     driver.Store
	lease     driver.ActiveLease
	inc       string
	source    service.OntologySource
	sourceRef model.Ref
	compiled  ontology.Snapshot
	writer    *pipeline.Writer
	svc       *service.Service
	nextID    int
}

func newFixture(t *testing.T, e engine) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, engine: e, sourceRef: model.Ref{ID: "ontology/audit-sql"}}
	f.open(t)
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	f.source = service.OntologySource{Base: ontology.DefaultDocument()}
	// "failed" is success in the dependent's project, failure in the
	// blocker's project. The adapter must resolve the blocker's own project.
	workflow := ontology.DefaultDocument().Workflows["issue"]
	delete(workflow.Slots, ontology.Fail)
	f.source.Overlays = map[string]ontology.Document{dependentProject: {
		Statuses:  map[string]ontology.Status{"failed": {Name: "Successful end", Category: ontology.Completed, Outcome: ontology.Success}},
		Workflows: map[string]ontology.Workflow{"issue": workflow},
	}}
	f.seedSource(t, model.OpCreate)
	f.compose(t)

	db := e.db(t)
	defer db.Close()
	if e.name == "sqlite_wal" {
		var mode, version string
		if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
			t.Fatalf("real SQLite journal mode=%q err=%v", mode, err)
		}
		if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
			t.Fatal(err)
		}
		t.Logf("real SQLite %s, file-backed WAL", version)
	}
	return f
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	var err error
	f.store, err = f.engine.storage.Open(f.ctx, driver.Config{DSN: f.engine.dsn})
	if err != nil {
		t.Fatal(err)
	}
	f.lease, err = f.store.Lease(f.ctx, "audit-sql", driver.LeaseTTL)
	if err != nil {
		_ = f.store.Close()
		t.Fatal(err)
	}
	f.inc = f.meta(t).Incarnation
}

func (f *fixture) compose(t *testing.T) {
	t.Helper()
	compiled, problems := ontology.Compile(f.source.Base, f.source.Overlays, f.source.Plugins)
	if len(problems) != 0 {
		t.Fatalf("protected source does not compile: %+v", problems)
	}
	f.compiled = compiled
	normalizer := service.NewOntology(compiled.Effective)
	f.writer = pipeline.New(f.store, f.lease,
		pipeline.WithOntology(normalizer),
		pipeline.WithOntologyAudit(service.NewTrackedOntologyAudit(service.StaticOntologySource(f.sourceRef))),
		pipeline.WithIDGenerator(func() (string, error) {
			ids := []string{"pl-sql-blocker", "pl-sql-dependent-z", "pl-sql-dependent-a"}
			if f.nextID >= len(ids) {
				return "", fmt.Errorf("unexpected extra create")
			}
			id := ids[f.nextID]
			f.nextID++
			return id, nil
		}),
	)
	f.svc = service.New(f.store, f.lease, f.writer, service.WithOntology(normalizer))
}

func (f *fixture) reopen(t *testing.T) {
	t.Helper()
	before, epoch := f.meta(t), f.lease.Epoch()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.open(t)
	if after := f.meta(t); !reflect.DeepEqual(before, after) || f.lease.Epoch() <= epoch {
		t.Fatalf("store reopen changed metadata or reused lease epoch: before=%+v after=%+v epoch=%d->%d", before, after, epoch, f.lease.Epoch())
	}
	f.compose(t)
}

func (f *fixture) seedSource(t *testing.T, op model.OpKind) model.Changeset {
	t.Helper()
	body, err := service.MarshalOntologySource(f.source)
	if err != nil {
		t.Fatal(err)
	}
	// Only the protected ontology source bypasses product writes. Attribution
	// is explicit and retained in real SQL version/changeset history.
	entry, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Principal: fixturePrincipal}, []conformance.FixtureChange{{
		Ref: f.sourceRef, Kind: "ontology", Op: op, Body: body,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

type graph struct {
	blocker    model.Ref
	dependents []model.Ref
	linked     model.Commit
}

func (f *fixture) graph(t *testing.T) graph {
	t.Helper()
	refs := []model.Ref{}
	for i, project := range []string{blockerProject, dependentProject, dependentProject} {
		commit, err := f.svc.Create(f.ctx, writerPrincipal, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": fmt.Sprintf("SQL bead %d", i)}}, model.WriteMeta{Project: project, IdempotencyKey: key(i + 2)})
		if err != nil || len(commit.Results) != 1 {
			t.Fatalf("service create=%+v err=%v", commit, err)
		}
		refs = append(refs, commit.Results[0].Ref)
	}
	// Deliberately request z before a; the committed notice plan must sort a,z.
	linked, err := f.writer.Apply(f.ctx, writerPrincipal, model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpLink, Link: &model.LinkOp{From: refs[1], To: refs[0], LinkKind: "conditional-blocks"}},
		{Kind: model.OpLink, Link: &model.LinkOp{From: refs[2], To: refs[0], LinkKind: "conditional-blocks"}},
	}}, model.WriteMeta{Method: "bead.update", Project: dependentProject, IdempotencyKey: key(5)})
	if err != nil {
		t.Fatal(err)
	}
	return graph{blocker: refs[0], dependents: []model.Ref{refs[2], refs[1]}, linked: linked}
}

func (f *fixture) close(g graph) (port.WriteOutcome, error) {
	return f.svc.CloseWithOutcome(f.ctx, writerPrincipal, service.CloseRequest{ID: g.blocker, Reason: "SQL audited close"}, model.WriteMeta{Project: blockerProject, IdempotencyKey: key(6)})
}

func (f *fixture) meta(t *testing.T) driver.Meta {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func (f *fixture) read(t *testing.T) driver.Reader {
	t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: f.inc})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (f *fixture) set(t *testing.T, revision model.Revision) model.Changeset {
	t.Helper()
	sets, err := (serverevents.Log{Store: f.store}).Scan(f.ctx, coreevents.Position{Incarnation: f.inc, Revision: revision}, 1)
	if err != nil || len(sets) != 1 || sets[0].Revision.String() != revision.String() {
		t.Fatalf("persisted changeset at %s: %+v err=%v", revision, sets, err)
	}
	if err := sets[0].ValidateEvents(); err != nil {
		t.Fatal(err)
	}
	return sets[0]
}

func (f *fixture) eventReader() *coreevents.Reader {
	return &coreevents.Reader{Log: serverevents.Log{Store: f.store}, Materializer: serverevents.HistoryMaterializer{Store: f.store}, Authorization: coreevents.AllowAll{}}
}

func (f *fixture) checkpoints(t *testing.T) *serverevents.Checkpoints {
	t.Helper()
	cp, err := serverevents.NewCheckpoints(f.store, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	return cp
}

func (f *fixture) receipt(t *testing.T) *driver.IdempotencyRecord {
	t.Helper()
	r := f.read(t)
	defer r.Close()
	principal, err := driver.PrincipalKey(writerPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(f.ctx, principal, "", "bead.close", key(6))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

type history struct {
	Record   driver.Record
	Versions []driver.Version
}

func (f *fixture) histories(t *testing.T, refs []model.Ref) map[string]history {
	t.Helper()
	r := f.read(t)
	defer r.Close()
	result := map[string]history{}
	for _, ref := range refs {
		rows, err := r.Get(f.ctx, []model.Ref{ref})
		if err != nil || len(rows) != 1 {
			t.Fatalf("record %s: %+v err=%v", ref.ID, rows, err)
		}
		versions, err := r.Versions(f.ctx, ref, driver.PageReq{})
		if err != nil || versions.NextCursor != nil {
			t.Fatalf("history %s: %+v err=%v", ref.ID, versions, err)
		}
		result[ref.ID] = history{Record: rows[0], Versions: versions.Items}
	}
	return result
}

func startPosition(f *fixture, g graph) coreevents.Position {
	return coreevents.Position{Incarnation: f.inc, Revision: g.linked.Revision, Index: model.Natural64(1)}
}

package events

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This is an injected decision fixture, not the production ontology adapter.
// The real service normalizer, Writer edge discovery/guard/descriptor producer,
// memory publication, event materializer and dispatcher execute unchanged.
type writerDecisionFixture struct{ after model.ConditionDecision }

func (d writerDecisionFixture) DecideConditional(ctx context.Context, _ port.Change, reader port.SnapshotReader) (port.ConditionalDecision, error) {
	ref := auditRef("ontology/delivery-fixture")
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		return port.ConditionalDecision{}, fmt.Errorf("decision fixture source unavailable: %v", err)
	}
	project := auditRef("pl-blocker-project")
	return port.ConditionalDecision{
		Ontology: model.OntologyPin{Ref: ref, Revision: rows[0].Revision, Digest: strings.Repeat("ab", 32), Project: &project},
		Before:   model.ConditionDecision{Status: "todo", Category: "unstarted"}, After: d.after,
	}, nil
}

func writerDeliverySources(t *testing.T, f *fixture) model.Changeset {
	t.Helper()
	fixtures := []conformance.FixtureChange{
		{Ref: auditRef("ontology/delivery-fixture"), Kind: "ontology", Op: model.OpCreate, Body: []byte(`{"fixture":"pinned decisions, not a production ontology document"}`)},
		{Ref: auditRef("pl-blocker-project"), Kind: "project", Op: model.OpCreate, Body: []byte(`{"name":"blocker project"}`)},
		{Ref: auditRef("pl-dependent-project"), Kind: "project", Op: model.OpCreate, Body: []byte(`{"name":"dependent project"}`)},
	}
	for _, id := range []string{"pl-blocker", "pl-dependent-b", "pl-dependent-a"} {
		doc := pipeline.Document{ID: id, Type: "issue", Project: "pl-blocker-project", Fields: map[string]any{"title": id, "status": "todo"}}
		if id != "pl-blocker" {
			doc.Project = "pl-dependent-project"
			doc.Links = []pipeline.Link{{To: auditRef("pl-blocker"), Kind: "conditional-blocks"}}
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		fixtures = append(fixtures, conformance.FixtureChange{Ref: auditRef(id), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	set, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{}, fixtures)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestActualWriterMemoryAndEventsWithFixtureDecisions(t *testing.T) {
	for _, tc := range []struct {
		method   string
		after    model.ConditionDecision
		ordinary model.DurableEventType
		notices  int
	}{
		{"bead.close", model.ConditionDecision{Status: "done", Category: "completed", Outcome: "success", Terminal: true}, model.EventBeadClosed, 2},
		{"bead.cancel", model.ConditionDecision{Status: "canceled", Category: "canceled", Terminal: true}, model.EventBeadCanceled, 2},
		{"bead.fail", model.ConditionDecision{Status: "failed", Category: "completed", Outcome: "failure", Terminal: true, Satisfied: true}, model.EventBeadFailed, 0},
		{"bead.defer", model.ConditionDecision{Status: "backlog", Category: "backlog"}, model.EventBeadDeferred, 0},
	} {
		t.Run(tc.method, func(t *testing.T) {
			f := newFixture(t)
			initial := writerDeliverySources(t, f)
			start := core.Position{Incarnation: f.incarnation, Revision: initial.Revision, Index: model.Natural64(uint64(len(initial.Changes) - 1))}
			w := pipeline.New(f.store, f.lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithOntologyAudit(writerDecisionFixture{after: tc.after}))
			cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: auditRef("pl-blocker"), Fields: map[string]any{}}}, {Kind: model.OpComment, Comment: &model.CommentOp{Target: auditRef("pl-blocker"), Body: "terminal reason"}}}}
			meta := model.WriteMeta{Method: tc.method, IdempotencyKey: strings.Repeat("a", 32)}
			by := model.PrincipalRef{Kind: "user", ID: "author"}
			outcome, err := w.ApplyWithOutcome(f.ctx, by, cs, meta)
			if err != nil || outcome.Replayed {
				t.Fatalf("actual Writer result=%+v %v", outcome, err)
			}
			sets, err := (Log{Store: f.store}).Scan(f.ctx, core.Position{Incarnation: f.incarnation, Revision: outcome.Commit.Revision}, 1)
			if err != nil || len(sets) != 1 || len(sets[0].Changes) != 1 || len(sets[0].Events) != 1+tc.notices {
				t.Fatalf("actual Writer plan not persisted: %+v %v", sets, err)
			}
			page, err := f.reader.Read(f.ctx, core.ReadRequest{After: start, Principal: request().Principal, Limit: 1000})
			if err != nil || len(page.Events()) != 1+tc.notices || page.Events()[0].Durable.Type != tc.ordinary {
				t.Fatalf("actual produced plan did not reach event reader: %+v %v", page, err)
			}
			ordinary := page.Events()[0].Durable
			if ordinary.Before["fields"].(map[string]any)["status"] != "todo" || ordinary.After["fields"].(map[string]any)["status"] != tc.after.Status {
				t.Fatalf("Writer slot images were not retained: %+v", ordinary)
			}
			for i, event := range page.Events()[1:] {
				e := event.Durable
				if e.Type != model.EventDependencyConditionUnmet || e.Index.String() != fmt.Sprint(i+1) || e.Resource != []string{"bead/pl-dependent-a", "bead/pl-dependent-b"}[i] || e.Before != nil || e.After != nil || e.Derived || len(e.Changed) != 0 || !reflect.DeepEqual(e.Details.After, tc.after) || e.Details.Ontology.Project.ID != "pl-blocker-project" {
					t.Fatalf("actual Writer notice envelope=%+v", e)
				}
			}
			replayed, err := w.ApplyWithOutcome(f.ctx, by, cs, meta)
			if err != nil || !replayed.Replayed || !reflect.DeepEqual(outcome.Commit, replayed.Commit) {
				t.Fatalf("actual replay changed receipt: %+v %v", replayed, err)
			}
			metaNow, err := f.store.Meta(f.ctx)
			if err != nil || metaNow.Head.String() != outcome.Commit.Revision.String() {
				t.Fatalf("replay emitted an extra revision: %+v %v", metaNow, err)
			}
			var delivered []model.Event
			b := core.Binding{ID: "writer-audit-listener", Class: core.Listener, Principal: request().Principal, Handler: handlerFunc(func(_ context.Context, item core.Delivery) error {
				delivered = append(delivered, item.Event)
				return nil
			})}
			cp := f.checkpoints(t)
			if _, err := cp.Save(f.ctx, b.ID, 0, core.State{Position: start, Class: b.Class}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := f.dispatcher(t).Step(f.ctx, b); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(delivered, page.Events()) {
				t.Fatal("listener did not consume exactly the produced plan after idempotency replay")
			}
			reader, err := f.store.Read(f.ctx, driver.View{Incarnation: f.incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			for _, id := range []string{"pl-dependent-a", "pl-dependent-b"} {
				versions, err := reader.Versions(f.ctx, auditRef(id), driver.PageReq{})
				if err != nil || len(versions.Items) != 1 {
					t.Fatalf("actual producer invented a dependent change: %+v %v", versions, err)
				}
			}
		})
	}
}

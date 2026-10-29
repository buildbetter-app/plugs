package events

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// These port tests exercise reader ordering independently of persistence.
// The server package separately proves the actual storage boundary.
type planLog []model.Changeset

func (l planLog) Head(context.Context) (Head, error) {
	return Head{Incarnation: "store", Revision: l[len(l)-1].Revision}, nil
}
func (l planLog) Scan(_ context.Context, after Position, limit int) ([]model.Changeset, error) {
	var result []model.Changeset
	for _, set := range l {
		cmp, _ := CompareNatural(set.Revision, after.Revision)
		if cmp >= 0 && len(result) < limit {
			result = append(result, set)
		}
	}
	return result, nil
}

type planMaterializer struct{}

func (planMaterializer) Materialize(_ context.Context, set model.Changeset, i int) (model.DurableEvent, error) {
	e := model.DurableEvent{Type: model.EventBeadUpdated, Resource: "bead/" + set.Changes[0].Ref.ID, Changed: []string{}, Principal: set.Principal, Chain: []string{}, SetBy: map[string]string{}, Cause: model.EventCause{Root: "root", Chain: []string{}}}
	if set.Events != nil {
		d := set.Events[i]
		e.Type, e.Resource = d.Type, d.Resource.Kind+"/"+d.Resource.Ref.ID
		e.SetBy, e.Cause, e.Chain, e.Details = d.Provenance.SetBy, d.Provenance.Cause, d.Provenance.Chain, d.Details
	} else {
		e.Resource = "bead/" + set.Changes[i].Ref.ID
	}
	return e, nil
}

func readerPlan() model.Changeset {
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	p := model.EventProvenance{Method: "tx.apply", Slot: "close", SetBy: map[string]string{"status": "request"}, Cause: model.EventCause{Root: "root", Chain: []string{}}, Chain: []string{}}
	set := model.Changeset{Incarnation: "store", Revision: "2", Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "user", ID: "author"}, Chain: []string{}, Changes: []model.ChangeRecord{{Ref: ref("blocker"), Op: model.OpPatch, FromRevision: "1"}, {Ref: ref("other"), Op: model.OpPatch, FromRevision: "1"}}, Events: []model.EventDescriptor{{Type: model.EventBeadClosed, Resource: model.AuditResource{Kind: "bead", Ref: ref("blocker")}, Provenance: p}}}
	for i, id := range []string{"dependent-a", "dependent-b"} {
		project := ref("project")
		d := &model.ConditionUnmetDetails{Dependent: model.EventImage{Ref: ref(id), Revision: "1"}, BlockerBefore: model.EventImage{Ref: ref("blocker"), Revision: "1"}, BlockerAfter: model.EventImage{Ref: ref("blocker"), Revision: "2"}, Edge: model.ConditionalEdge{Dependent: ref(id), Blocker: ref("blocker"), Kind: "conditional-blocks", Revision: "1"}, Ontology: model.OntologyPin{Ref: ref("ontology"), Revision: "1", Digest: strings.Repeat("ab", 32), Project: &project}, Before: model.ConditionDecision{Status: "working", Category: "started"}, After: model.ConditionDecision{Status: "done", Category: "completed", Outcome: "success", Terminal: true}}
		set.Events = append(set.Events, model.EventDescriptor{Index: model.Natural64(uint64(i + 1)), Type: model.EventDependencyConditionUnmet, Resource: model.AuditResource{Kind: "bead", Ref: ref(id)}, Provenance: p, Details: d})
	}
	p.Slot = ""
	set.Events = append(set.Events, model.EventDescriptor{Index: "3", ChangeIndex: "1", Type: model.EventBeadUpdated, Resource: model.AuditResource{Kind: "bead", Ref: ref("other")}, Provenance: p})
	return set
}

func TestReaderLogicalPlanAndLegacyResume(t *testing.T) {
	set := readerPlan()
	if err := set.ValidateEvents(); err != nil {
		t.Fatal(err)
	}
	legacy := set
	legacy.Revision, legacy.Events = "3", nil
	r := Reader{Log: planLog{set, legacy}, Materializer: planMaterializer{}, Authorization: AllowAll{}}
	position := Position{}
	var seen []string
	for range 7 {
		page, err := r.Read(context.Background(), ReadRequest{After: position, Limit: 1, Changesets: 1})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Events() {
			seen = append(seen, e.Durable.Revision.String()+":"+e.Durable.Index.String())
		}
		position = page.Position
	}
	if got := strings.Join(seen, ","); got != "2:0,2:1,2:2,2:3,3:0,3:1" {
		t.Fatalf("logical/legacy order = %s", got)
	}
	page, err := r.Read(context.Background(), ReadRequest{After: Position{Incarnation: "store", Revision: "2", Index: "1"}, Limit: 1000, Changesets: 1})
	if err != nil || len(page.Events()) != 4 || page.Events()[0].Durable.Index.String() != "2" {
		t.Fatalf("index1 continuation = %+v, %v", page, err)
	}
	for _, index := range []string{"4", "999999999999999999999999"} {
		if _, err := r.Read(context.Background(), ReadRequest{After: Position{Incarnation: "store", Revision: "2", Index: model.Natural(index)}, Limit: 1}); err == nil {
			t.Fatal("accepted impossible logical index", index)
		}
	}
}

type primaryOnlyAuthorization struct{}

func (primaryOnlyAuthorization) Read(_ context.Context, _ model.PrincipalRef, e model.DurableEvent) (Visibility, error) {
	return Visibility{Readable: true, Before: e.Before, After: e.After}, nil
}

func TestReaderRefusesUnvalidatedPlansAndUnavailableResourceAuthorization(t *testing.T) {
	for _, name := range []string{"empty", "missing ordinary", "wrong index", "resource authorization"} {
		t.Run(name, func(t *testing.T) {
			set := readerPlan()
			switch name {
			case "empty":
				set.Events = []model.EventDescriptor{}
			case "missing ordinary":
				set.Events = set.Events[:3]
			case "wrong index":
				set.Events[1].Index = "9"
			}
			r := Reader{Log: planLog{set}, Materializer: planMaterializer{}, Authorization: primaryOnlyAuthorization{}}
			if _, err := r.Read(context.Background(), ReadRequest{Limit: 1000}); err == nil {
				t.Fatal(fmt.Sprintf("accepted %s", name))
			}
		})
	}
}

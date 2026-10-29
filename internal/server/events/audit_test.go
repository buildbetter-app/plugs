package events

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func auditRef(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

// These storage fixtures supply descriptors through the real commit header.
// They test delivery and retained sources, independently of Writer production.
func auditImages(t *testing.T) (*fixture, model.Changeset) {
	t.Helper()
	return auditImagesWithPlan(t, auditPlan("2"))
}

func auditImagesWithPlan(t *testing.T, plan []model.EventDescriptor) (*fixture, model.Changeset) {
	t.Helper()
	f := newFixture(t)
	f.commit(t, time.Time{},
		change{id: "pl-blocker", kind: "bead", op: model.OpCreate, fields: map[string]any{"status": "working"}},
		change{id: "pl-dependent-a", kind: "bead", op: model.OpCreate, fields: map[string]any{"title": "dependent-a", "dependencies": []any{map[string]any{"kind": "conditional-blocks", "blocker": "pl-blocker"}}}},
		change{id: "pl-dependent-b", kind: "bead", op: model.OpCreate, fields: map[string]any{"title": "dependent-b", "dependencies": []any{map[string]any{"kind": "conditional-blocks", "blocker": "pl-blocker"}}}}, bead("pl-other", "before"),
		change{id: "pl-ontology", kind: "ontology", op: model.OpCreate, fields: map[string]any{"decision": "original"}},
		change{id: "pl-project", kind: "project", op: model.OpCreate, fields: map[string]any{"name": "project"}},
	)
	set := commitAudit(t, f, plan,
		change{id: "pl-blocker", kind: "bead", op: model.OpPatch, fields: map[string]any{"status": "done"}, paths: []string{"/status"}},
		change{id: "pl-other", kind: "bead", op: model.OpPatch, fields: map[string]any{"title": "after"}, paths: []string{"/title"}},
	)
	if err := set.ValidateEvents(); err != nil {
		t.Fatal(err)
	}
	return f, set
}

func auditPlan(revision model.Revision) []model.EventDescriptor {
	p := model.EventProvenance{Method: "bead.close", Slot: "close", SetBy: map[string]string{"status": "request"}, Cause: model.EventCause{Root: "audit-root", Chain: []string{}}, Chain: []string{}}
	plan := []model.EventDescriptor{{Type: model.EventBeadClosed, Resource: model.AuditResource{Kind: "bead", Ref: auditRef("pl-blocker")}, Provenance: p}}
	for i, id := range []string{"pl-dependent-a", "pl-dependent-b"} {
		project := auditRef("pl-project")
		details := &model.ConditionUnmetDetails{
			Dependent:     model.EventImage{Ref: auditRef(id), Revision: "1"},
			BlockerBefore: model.EventImage{Ref: auditRef("pl-blocker"), Revision: "1"},
			BlockerAfter:  model.EventImage{Ref: auditRef("pl-blocker"), Revision: revision},
			Edge:          model.ConditionalEdge{Dependent: auditRef(id), Blocker: auditRef("pl-blocker"), Kind: "conditional-blocks", Revision: "1"},
			Ontology:      model.OntologyPin{Ref: auditRef("pl-ontology"), Revision: "1", Digest: strings.Repeat("ab", 32), Project: &project},
			Before:        model.ConditionDecision{Status: "working", Category: "started"},
			After:         model.ConditionDecision{Status: "done", Category: "completed", Outcome: "success", Terminal: true},
		}
		plan = append(plan, model.EventDescriptor{Index: model.Natural64(uint64(i + 1)), Type: model.EventDependencyConditionUnmet, Resource: model.AuditResource{Kind: "bead", Ref: auditRef(id)}, Provenance: p, Details: details})
	}
	p.Slot = ""
	plan = append(plan, model.EventDescriptor{Index: "3", ChangeIndex: "1", Type: model.EventBeadUpdated, Resource: model.AuditResource{Kind: "bead", Ref: auditRef("pl-other")}, Provenance: p})
	return model.CloneEventDescriptors(plan)
}

func TestDescriptorMaterializerUsesExactRetainedSources(t *testing.T) {
	f, set := auditImages(t)
	// Later writes do not reinterpret the immutable committed source decisions.
	f.commit(t, time.Time{},
		change{id: "pl-blocker", kind: "bead", op: model.OpPatch, fields: map[string]any{"status": "working"}},
		change{id: "pl-dependent-a", kind: "bead", op: model.OpPatch, fields: map[string]any{"title": "later-dependent"}},
		change{id: "pl-ontology", kind: "ontology", op: model.OpPatch, fields: map[string]any{"decision": "later"}},
	)
	store := &readerTracker{ReadStore: f.store}
	m := HistoryMaterializer{Store: store}
	ordinary, err := m.Materialize(f.ctx, set, 0)
	if err != nil || ordinary.Before["status"] != "working" || ordinary.After["status"] != "done" || ordinary.Type != model.EventBeadClosed {
		t.Fatalf("ordinary pinned images: %+v, %v", ordinary, err)
	}
	notice, err := m.Materialize(f.ctx, set, 1)
	if err != nil || notice.Type != model.EventDependencyConditionUnmet || notice.Before != nil || notice.After != nil || notice.Derived || len(notice.Changed) != 0 || !reflect.DeepEqual(notice.Details, set.Events[1].Details) {
		t.Fatalf("notice envelope: %+v, %v", notice, err)
	}
	notice.Details.After.Status = "caller-mutated"
	notice.Details.Ontology.Project.ID = "caller-mutated"
	notice.SetBy["status"] = "caller-mutated"
	notice, err = m.Materialize(f.ctx, set, 1)
	if err != nil || notice.Details.After.Status != "done" || notice.Details.Ontology.Project.ID != "pl-project" || notice.SetBy["status"] != "request" {
		t.Fatalf("materialized details alias the log: %+v, %v", notice, err)
	}
	last, err := m.Materialize(f.ctx, set, 3)
	if err != nil || last.Resource != "bead/pl-other" || last.Before["title"] != "before" || last.After["title"] != "after" {
		t.Fatalf("logical index used as change anchor: %+v, %v", last, err)
	}
	if store.opened != 12 || store.closed != store.opened {
		t.Fatalf("source readers opened=%d closed=%d", store.opened, store.closed)
	}
}

func TestDescriptorMaterializerRefusesWrongPinsAndMissingOrdinaryImages(t *testing.T) {
	f, original := auditImages(t)
	for _, source := range []string{"dependent", "blocker-before", "blocker-after", "ontology", "ordinary"} {
		t.Run(source, func(t *testing.T) {
			set := original
			set.Events = model.CloneEventDescriptors(original.Events)
			index := 1
			switch source {
			case "dependent":
				set.Events[index].Details.Dependent.Revision = "2"
				set.Events[index].Details.Edge.Revision = "2"
			case "blocker-before":
				set.Events[index].Details.BlockerBefore.Revision = "2"
			case "blocker-after":
				set.Events[index].Details.BlockerAfter.Revision = "3"
			case "ontology":
				set.Events[index].Details.Ontology.Revision = "2"
			case "ordinary":
				index = 3
				set.Changes = append([]model.ChangeRecord{}, original.Changes...)
				set.Changes[1].Ref = auditRef("pl-absent")
				set.Events[index].Resource.Ref = auditRef("pl-absent")
			}
			store := &readerTracker{ReadStore: f.store}
			_, err := (HistoryMaterializer{Store: store}).Materialize(f.ctx, set, index)
			if err == nil {
				t.Fatal("accepted unavailable or mismatched pinned record")
			}
			if store.opened != store.closed {
				t.Fatalf("readers opened=%d closed=%d", store.opened, store.closed)
			}
		})
	}
}

type sourceFailureStore struct {
	ReadStore
	err error
}

func (s sourceFailureStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	if view.At != nil {
		return nil, s.err
	}
	return s.ReadStore.Read(ctx, view)
}

func TestDescriptorSourceAbsenceAndHistoryErrorsRemainDistinct(t *testing.T) {
	f, set := auditImages(t)
	set.Events[1].Details.Ontology.Ref = auditRef("pl-absent")
	_, err := (HistoryMaterializer{Store: f.store}).Materialize(f.ctx, set, 1)
	if !errors.Is(err, core.ErrNoticeUnavailable) {
		t.Fatalf("missing pinned source: %v", err)
	}
	set.Events = auditPlan(set.Revision)
	for _, failure := range []error{driver.ErrTruncated{Floor: "2"}, driver.ErrFenced{}, context.Canceled} {
		_, err := (HistoryMaterializer{Store: sourceFailureStore{ReadStore: f.store, err: failure}}).Materialize(f.ctx, set, 1)
		if err == nil || errors.Is(err, core.ErrNoticeUnavailable) {
			t.Fatalf("history/fencing failure suppressed: %v => %v", failure, err)
		}
	}
}

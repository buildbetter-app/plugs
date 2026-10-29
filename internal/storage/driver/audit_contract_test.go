package driver

import (
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func auditHeaderPlan() ChangesetHeader {
	ref := model.RefByID("bead-a", model.Natural64(0))
	return ChangesetHeader{
		Chain:   []string{"authz.team/default"},
		Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpPatch, FromRevision: model.Natural64(1)}},
		Events: []model.EventDescriptor{{
			Index: model.Natural64(0), ChangeIndex: model.Natural64(0), Type: model.EventBeadUpdated,
			Resource: model.AuditResource{Kind: "bead", Ref: ref},
			Provenance: model.EventProvenance{
				Method: "bead.update", SetBy: map[string]string{"title": "request"},
				Cause: model.EventCause{Root: "request-1", Chain: []string{}},
				Chain: []string{"authz.team/default"},
			},
		}},
	}
}

func TestChangesetHeaderCarriesCanonicalAuditPlan(t *testing.T) {
	typ := reflect.TypeFor[ChangesetHeader]()
	field, ok := typ.FieldByName("Events")
	if !ok || !field.IsExported() || field.Type != reflect.TypeFor[[]model.EventDescriptor]() {
		t.Fatalf("ChangesetHeader must expose Events []model.EventDescriptor: %s", typ)
	}

	header := auditHeaderPlan()
	committed := model.Changeset{
		Revision: model.Natural64(2), Chain: header.Chain,
		Changes: header.Changes, Events: header.Events,
	}
	if err := committed.ValidateEvents(); err != nil {
		t.Fatalf("canonical ordered plan rejected through header: %v", err)
	}

	owned := model.CloneEventDescriptors(header.Events)
	header.Events[0].Provenance.SetBy["title"] = "mutated"
	header.Events[0].Provenance.Cause.Chain = append(header.Events[0].Provenance.Cause.Chain, "listener")
	if owned[0].Provenance.SetBy["title"] != "request" || len(owned[0].Provenance.Cause.Chain) != 0 {
		t.Fatal("driver-owned event plan aliases caller-owned nested values")
	}

	legacy := header
	legacy.Events = nil
	legacyChangeset := model.Changeset{Revision: model.Natural64(2), Chain: legacy.Chain, Changes: legacy.Changes, Events: legacy.Events}
	if err := legacyChangeset.ValidateEvents(); err != nil {
		t.Fatalf("nil Events must retain legacy validation path: %v", err)
	}
	if legacy.Events != nil {
		t.Fatal("legacy header plan must remain nil")
	}
}

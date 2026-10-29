package model

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func conditionFixture() ConditionUnmetDetails {
	dependent, blocker, ontology, project := RefByID("dependent-a", Natural64(0)), RefByID("blocker", Natural64(0)), RefByID("ontology", Natural64(0)), RefByID("blocker-project", Natural64(0))
	return ConditionUnmetDetails{
		Dependent:     EventImage{Ref: dependent, Revision: Natural64(3)},
		BlockerBefore: EventImage{Ref: blocker, Revision: Natural64(4)},
		BlockerAfter:  EventImage{Ref: blocker, Revision: Natural64(5)},
		Edge:          ConditionalEdge{Dependent: dependent, Blocker: blocker, Kind: "conditional-blocks", Revision: Natural64(3)},
		Ontology:      OntologyPin{Ref: ontology, Revision: Natural64(2), Digest: strings.Repeat("ab", 32), Project: &project},
		Before:        ConditionDecision{Status: "working", Category: "started"},
		After:         ConditionDecision{Status: "finished", Category: "completed", Outcome: "success", Terminal: true},
	}
}

func auditFixture() Changeset {
	details := conditionFixture()
	provenance := EventProvenance{Method: "bead.close", Slot: "close", SetBy: map[string]string{"status": "request"}, Cause: EventCause{Root: "request-1", Depth: Natural64(0), Chain: []string{}}, Chain: []string{"authz.team/default"}}
	return Changeset{
		Revision: Natural64(5), Branch: Natural64(0), Incarnation: "store", Kind: ChangesetWrite,
		Time: time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC), Principal: PrincipalRef{Kind: "user", ID: "u"}, Chain: []string{"authz.team/default"},
		Changes: []ChangeRecord{{Ref: details.BlockerAfter.Ref, Op: OpPatch, FromRevision: details.BlockerBefore.Revision, ChangedPaths: []string{"/status"}}},
		Events: []EventDescriptor{
			{Index: Natural64(0), ChangeIndex: Natural64(0), Type: EventBeadClosed, Resource: AuditResource{Kind: "bead", Ref: details.BlockerAfter.Ref}, Provenance: provenance},
			{Index: Natural64(1), ChangeIndex: Natural64(0), Type: EventDependencyConditionUnmet, Resource: AuditResource{Kind: "bead", Ref: details.Dependent.Ref}, Provenance: provenance, Details: &details},
		},
	}
}

func TestAuditPlanRoundtripAndLegacyCompatibility(t *testing.T) {
	c := auditFixture()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Changeset
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, c) {
		t.Fatalf("audit plan changed: %s", raw)
	}
	c.Events = nil
	legacy, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(legacy), `"events"`) {
		t.Fatal("legacy row acquired event plan")
	}
	if err := json.Unmarshal(legacy, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Events != nil {
		t.Fatal("legacy row lost absent-plan discriminator")
	}
	again, err := json.Marshal(decoded)
	if err != nil || string(again) != string(legacy) {
		t.Fatal("legacy bytes changed", err)
	}
}

func TestAuditPlanRetainsOrdinaryAndOrderedNotices(t *testing.T) {
	c := auditFixture()
	second := CloneEventDescriptors(c.Events[1:])[0]
	second.Index = Natural64(2)
	second.Resource.Ref = RefByID("dependent-b", Natural64(0))
	second.Details.Dependent.Ref = second.Resource.Ref
	second.Details.Edge.Dependent = second.Resource.Ref
	c.Events = append(c.Events, second)
	if err := c.ValidateEvents(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Changeset){
		"missing ordinary": func(c *Changeset) {
			c.Events = c.Events[1:]
			for i := range c.Events {
				c.Events[i].Index = Natural64(uint64(i))
			}
		},
		"duplicate index": func(c *Changeset) { c.Events[1].Index = Natural64(0) },
		"wrong anchor":    func(c *Changeset) { c.Events[1].ChangeIndex = Natural64(1) },
		"reversed notices": func(c *Changeset) {
			c.Events[1], c.Events[2] = c.Events[2], c.Events[1]
			c.Events[1].Index = Natural64(1)
			c.Events[2].Index = Natural64(2)
		},
		"duplicate edge":        func(c *Changeset) { c.Events[2] = c.Events[1]; c.Events[2].Index = Natural64(2) },
		"wrong source revision": func(c *Changeset) { c.Events[1].Details.BlockerAfter.Revision = Natural64(6) },
		"wrong provenance":      func(c *Changeset) { c.Events[1].Provenance.Method = "bead.cancel" },
		"no changes":            func(c *Changeset) { c.Changes = nil },
	} {
		t.Run(name, func(t *testing.T) {
			v := c
			v.Events = CloneEventDescriptors(c.Events)
			mutate(&v)
			if err := v.ValidateEvents(); err == nil {
				t.Fatal("invalid ordered plan accepted")
			}
		})
	}
}

func TestAuditCopiesAndRequiredResources(t *testing.T) {
	c := auditFixture()
	copy := CloneEventDescriptors(c.Events)
	if !reflect.DeepEqual(copy, c.Events) {
		t.Fatal("clone changed values")
	}
	copy[1].Provenance.SetBy["status"] = "mutator"
	copy[1].Provenance.Chain[0] = "other"
	copy[1].Details.Ontology.Project.ID = "other-project"
	copy[1].Details.After.Status = "other-status"
	if c.Events[1].Provenance.SetBy["status"] != "request" || c.Events[1].Provenance.Chain[0] != "authz.team/default" || c.Events[1].Details.Ontology.Project.ID != "blocker-project" || c.Events[1].Details.After.Status != "finished" {
		t.Fatal("clone aliases source")
	}
	if CloneEventDescriptors(nil) != nil {
		t.Fatal("clone lost absent plan")
	}
	resources, err := c.Events[1].RequiredResources()
	if err != nil {
		t.Fatal(err)
	}
	want := []AuditResource{{Kind: "bead", Ref: c.Events[0].Resource.Ref}, {Kind: "bead", Ref: c.Events[1].Resource.Ref}, {Kind: "ontology", Ref: c.Events[1].Details.Ontology.Ref}, {Kind: "project", Ref: *c.Events[1].Details.Ontology.Project}}
	if !reflect.DeepEqual(resources, want) {
		t.Fatalf("incomplete protection set: %+v", resources)
	}
}

package model

import (
	"encoding/json"
	"fmt"
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

func TestLegacyChangesetNilCollectionsRoundtrip(t *testing.T) {
	// Memory copies committed history through JSON. Before event descriptors,
	// encoding/json retained nil slices as null, including nil changed paths.
	// Use the old encoder shape so the input is independent of the new codec.
	type legacyChangeset Changeset
	chains := map[string][]string{"nil": nil, "empty": {}, "populated": {"authz.team/default"}}
	changes := map[string][]ChangeRecord{
		"nil":   nil,
		"empty": {},
	}
	for name, paths := range map[string][]string{"nil paths": nil, "empty paths": {}, "populated paths": {"/status"}} {
		change := auditFixture().Changes[0]
		change.ChangedPaths = paths
		changes[name] = []ChangeRecord{change}
	}
	for chainName, chain := range chains {
		for changeName, records := range changes {
			t.Run(chainName+"/"+changeName, func(t *testing.T) {
				original := auditFixture()
				original.Events, original.Chain, original.Changes = nil, chain, records
				raw, err := json.Marshal(legacyChangeset(original))
				if err != nil {
					t.Fatal(err)
				}
				var decoded Changeset
				if err := json.Unmarshal(raw, &decoded); err != nil {
					t.Fatalf("legacy history copy failed: %s: %v", raw, err)
				}
				if !reflect.DeepEqual(decoded, original) {
					t.Fatalf("legacy nil/empty distinction changed: got %#v, want %#v", decoded, original)
				}
				again, err := json.Marshal(decoded)
				if err != nil || string(again) != string(raw) {
					t.Fatalf("legacy bytes changed: %s, %v", again, err)
				}
			})
		}
	}
}

func TestLegacyChangesetCollectionsRemainTyped(t *testing.T) {
	legacy := auditFixture()
	legacy.Events = nil
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"null chain element":    func(v map[string]any) { v["chain"] = []any{nil} },
		"null change element":   func(v map[string]any) { v["changes"] = []any{nil} },
		"wrong chain type":      func(v map[string]any) { v["chain"] = "authz" },
		"wrong changes type":    func(v map[string]any) { v["changes"] = map[string]any{} },
		"unknown history field": func(v map[string]any) { v["replayed"] = true },
		"missing chain":         func(v map[string]any) { delete(v, "chain") },
		"missing changes":       func(v map[string]any) { delete(v, "changes") },
		"null path element": func(v map[string]any) {
			v["changes"].([]any)[0].(map[string]any)["changedPaths"] = []any{nil}
		},
		"wrong paths type": func(v map[string]any) {
			v["changes"].([]any)[0].(map[string]any)["changedPaths"] = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := parseJSON(t, raw).(map[string]any)
			mutate(value)
			bad, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			decoded := auditFixture()
			if err := json.Unmarshal(bad, &decoded); err == nil {
				t.Fatal("invalid history collection accepted")
			}
			if !reflect.DeepEqual(decoded, auditFixture()) {
				t.Fatal("failed history decode changed receiver")
			}
		})
	}
	for _, raw := range []string{
		`{"ops":null}`, `{"ops":[null]}`, `{"ops":[],"chain":null}`,
		`{"ops":[],"changes":null}`, `{"ops":[],"events":null}`,
	} {
		rejectComponent(t, "ChangeSet", raw, &ChangeSet{})
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

func TestGateEventPlanRetainsItsSlotEventOnce(t *testing.T) {
	for _, pair := range []struct {
		slot DurableEventType
		gate DurableEventType
	}{
		{EventBeadClosed, EventGateApproved},
		{EventBeadClosed, EventGateReported},
		{EventBeadFailed, EventGateReported},
	} {
		t.Run(string(pair.slot)+"/"+string(pair.gate), func(t *testing.T) {
			c := auditFixture()
			c.Events = c.Events[:1]
			c.Events[0].Type = pair.slot
			if pair.slot == EventBeadFailed {
				c.Events[0].Provenance.Slot = "fail"
			}
			c.Events[0].Provenance.Method = "gate.report"
			if pair.gate == EventGateApproved {
				c.Events[0].Provenance.Method = "gate.approve"
			}
			gate := CloneEventDescriptors(c.Events)[0]
			gate.Index, gate.Type = Natural64(1), pair.gate
			c.Events = append(c.Events, gate)
			if err := c.ValidateEvents(); err != nil {
				t.Fatal(err)
			}
			for name, mutate := range map[string]func(*Changeset){
				"duplicate companion": func(v *Changeset) {
					duplicate := CloneEventDescriptors(v.Events[1:])[0]
					duplicate.Index = Natural64(2)
					v.Events = append(v.Events, duplicate)
				},
				"duplicate standalone gate": func(v *Changeset) { v.Events[0].Type = pair.gate },
				"ordinary update companion": func(v *Changeset) { v.Events[0].Type = EventBeadUpdated },
				"different resource":        func(v *Changeset) { v.Events[1].Resource.Ref = RefByID("other", Natural64(0)) },
			} {
				t.Run(name, func(t *testing.T) {
					v := c
					v.Events = CloneEventDescriptors(c.Events)
					mutate(&v)
					if v.ValidateEvents() == nil {
						t.Fatal("invalid gate event plan accepted")
					}
				})
			}
		})
	}
	standalone := auditFixture()
	standalone.Events = standalone.Events[:1]
	standalone.Events[0].Type = EventGateApproved
	standalone.Events[0].Provenance.Method, standalone.Events[0].Provenance.Slot = "gate.approve", ""
	if err := standalone.ValidateEvents(); err != nil {
		t.Fatalf("pending approval must have its single gate event: %v", err)
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

func noticeFixture() DurableEvent {
	c := auditFixture()
	p := c.Events[1].Provenance
	return DurableEvent{EventPosition: EventPosition{Cursor: "opaque", Incarnation: c.Incarnation, Revision: c.Revision, Index: Natural64(1)}, Type: EventDependencyConditionUnmet, Resource: "bead/" + c.Events[1].Details.Dependent.Ref.ID, Changed: []string{}, Principal: c.Principal, Chain: p.Chain, SetBy: p.SetBy, Cause: p.Cause, Details: c.Events[1].Details}
}

func TestConditionNoticeAgainstCanonicalUnion(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		e := noticeFixture()
		if canceled {
			e.Details.After = ConditionDecision{Status: "abandoned", Category: "canceled", Terminal: true}
		}
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"Event", "DurableEvent", "DependencyConditionUnmetEvent"} {
			roundtripComponent(t, name, string(raw), &Event{})
		}
		var direct DurableEvent
		if err := json.Unmarshal(raw, &direct); err != nil || !reflect.DeepEqual(direct, e) {
			t.Fatal("direct durable codec mismatch", err)
		}
		if err := checkComponent(t, "OrdinaryDurableEvent", raw); err == nil {
			t.Fatal("condition notice matched ordinary arm")
		}
	}
	ordinary := durableJSON("bead.closed")
	roundtripComponent(t, "OrdinaryDurableEvent", ordinary, &DurableEvent{})
	if err := checkComponent(t, "DependencyConditionUnmetEvent", []byte(ordinary)); err == nil {
		t.Fatal("ordinary event matched condition arm")
	}
}

func TestAuditRequiredMembersTypesAndUnknownFields(t *testing.T) {
	c := auditFixture()
	cases := []struct {
		name     string
		value    any
		newValue func() any
		optional map[string]bool
	}{
		{"AuditResource", c.Events[0].Resource, func() any { return &AuditResource{} }, nil},
		{"EventImage", c.Events[1].Details.Dependent, func() any { return &EventImage{} }, nil},
		{"ConditionalEdge", c.Events[1].Details.Edge, func() any { return &ConditionalEdge{} }, nil},
		{"OntologyPin", c.Events[1].Details.Ontology, func() any { return &OntologyPin{} }, map[string]bool{"project": true}},
		{"ConditionDecision", c.Events[1].Details.After, func() any { return &ConditionDecision{} }, nil},
		{"ConditionUnmetDetails", *c.Events[1].Details, func() any { return &ConditionUnmetDetails{} }, nil},
		{"EventProvenance", c.Events[0].Provenance, func() any { return &EventProvenance{} }, nil},
		{"OrdinaryEventDescriptor", c.Events[0], func() any { return &EventDescriptor{} }, nil},
		{"ConditionUnmetDescriptor", c.Events[1], func() any { return &EventDescriptor{} }, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			roundtripComponent(t, tc.name, string(raw), tc.newValue())
			base := parseJSON(t, raw).(map[string]any)
			for key, value := range base {
				for _, mode := range []string{"omit", "null", "wrongType"} {
					if mode == "omit" && tc.optional[key] {
						continue
					}
					v := parseJSON(t, raw).(map[string]any)
					switch mode {
					case "omit":
						delete(v, key)
					case "null":
						v[key] = nil
					case "wrongType":
						switch value.(type) {
						case string:
							v[key] = false
						default:
							v[key] = "wrong"
						}
					}
					bad, _ := json.Marshal(v)
					t.Run(key+"/"+mode, func(t *testing.T) { rejectComponent(t, tc.name, string(bad), tc.newValue()) })
				}
			}
			for _, unknown := range []string{"extra", "Unexpected"} {
				v := parseJSON(t, raw).(map[string]any)
				v[unknown] = true
				bad, _ := json.Marshal(v)
				rejectComponent(t, tc.name, string(bad), tc.newValue())
			}
		})
	}
}

func TestConditionFactsRefuseContradictions(t *testing.T) {
	for name, mutate := range map[string]func(*ConditionUnmetDetails){
		"failure satisfies":              func(d *ConditionUnmetDetails) { d.After.Outcome = "failure"; d.After.Satisfied = true },
		"nonterminal false":              func(d *ConditionUnmetDetails) { d.After = ConditionDecision{Status: "todo", Category: "unstarted"} },
		"unknown category":               func(d *ConditionUnmetDetails) { d.After.Category = "future-category" },
		"unknown outcome":                func(d *ConditionUnmetDetails) { d.After.Outcome = "future" },
		"false terminal":                 func(d *ConditionUnmetDetails) { d.After.Terminal = false },
		"false condition assertion":      func(d *ConditionUnmetDetails) { d.Before.Satisfied = true },
		"unchanged status":               func(d *ConditionUnmetDetails) { d.Before.Status = d.After.Status },
		"unchanged image":                func(d *ConditionUnmetDetails) { d.BlockerAfter.Revision = d.BlockerBefore.Revision },
		"reversed revisions":             func(d *ConditionUnmetDetails) { d.BlockerBefore.Revision = Natural64(6) },
		"future dependent":               func(d *ConditionUnmetDetails) { d.Dependent.Revision = Natural64(6); d.Edge.Revision = Natural64(6) },
		"future ontology":                func(d *ConditionUnmetDetails) { d.Ontology.Revision = Natural64(6) },
		"unresolved ref":                 func(d *ConditionUnmetDetails) { d.Dependent.Ref = RefByKey("display-key") },
		"empty audit id":                 func(d *ConditionUnmetDetails) { d.Ontology.Ref = RefByID("", Natural64(0)) },
		"branch":                         func(d *ConditionUnmetDetails) { d.Ontology.Ref.Branch = Natural64(1) },
		"wrong blocker":                  func(d *ConditionUnmetDetails) { d.BlockerBefore.Ref = RefByID("other", Natural64(0)) },
		"wrong edge source":              func(d *ConditionUnmetDetails) { d.Edge.Dependent = RefByID("other", Natural64(0)) },
		"wrong edge target":              func(d *ConditionUnmetDetails) { d.Edge.Blocker = RefByID("other", Natural64(0)) },
		"wrong edge membership revision": func(d *ConditionUnmetDetails) { d.Edge.Revision = Natural64(1) },
		"wrong edge kind":                func(d *ConditionUnmetDetails) { d.Edge.Kind = "blocks" },
		"invalid digest":                 func(d *ConditionUnmetDetails) { d.Ontology.Digest = "sha256:fake" },
		"noncanonical digest":            func(d *ConditionUnmetDetails) { d.Ontology.Digest = strings.Repeat("AB", 32) },
		"future decision field":          func(d *ConditionUnmetDetails) { d.Before.Outcome = "success" },
	} {
		t.Run(name, func(t *testing.T) {
			d := conditionFixture()
			mutate(&d)
			if err := d.Validate(); err == nil {
				t.Fatal("contradictory facts accepted")
			}
			if _, err := json.Marshal(d); err == nil {
				t.Fatal("contradictory facts marshaled")
			}
		})
	}
}

func TestConditionEnvelopeAndDescriptorArmsRefuse(t *testing.T) {
	for name, mutate := range map[string]func(*DurableEvent){
		"missing details":   func(e *DurableEvent) { e.Details = nil },
		"wrong type":        func(e *DurableEvent) { e.Type = EventBeadClosed },
		"alias":             func(e *DurableEvent) { e.Type = "dependency.conditionunmet" },
		"wrong resource":    func(e *DurableEvent) { e.Resource = "bead/blocker" },
		"fake change":       func(e *DurableEvent) { e.Changed = []string{"/status"} },
		"fake before image": func(e *DurableEvent) { e.Before = map[string]any{} },
		"fake after image":  func(e *DurableEvent) { e.After = map[string]any{} },
		"wrong revision":    func(e *DurableEvent) { e.Revision = Natural64(6) },
		"derived":           func(e *DurableEvent) { e.Derived = true },
	} {
		t.Run(name, func(t *testing.T) {
			e := noticeFixture()
			mutate(&e)
			if err := (Event{Durable: &e}).Validate(); err == nil {
				t.Fatal("invalid condition envelope accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*EventDescriptor){
		"control type":       func(d *EventDescriptor) { d.Type = "event.gap" },
		"unknown type":       func(d *EventDescriptor) { d.Type = "future.type" },
		"ordinary arm":       func(d *EventDescriptor) { d.Type = EventBeadClosed },
		"missing condition":  func(d *EventDescriptor) { d.Details = nil },
		"wrong kind":         func(d *EventDescriptor) { d.Resource.Kind = "view" },
		"wrong slot":         func(d *EventDescriptor) { d.Provenance.Slot = "fail" },
		"unknown slot":       func(d *EventDescriptor) { d.Provenance.Slot = "future" },
		"unresolved project": func(d *EventDescriptor) { r := RefByKey("key"); d.Details.Ontology.Project = &r },
		"oversized index":    func(d *EventDescriptor) { d.Index = Natural64(1000) },
		"invalid number":     func(d *EventDescriptor) { d.ChangeIndex = Natural("0.5") },
		"overflow":           func(d *EventDescriptor) { d.Index = Natural("1e1000000000") },
		"missing method":     func(d *EventDescriptor) { d.Provenance.Method = "" },
		"missing cause":      func(d *EventDescriptor) { d.Provenance.Cause.Root = "" },
		"missing chain":      func(d *EventDescriptor) { d.Provenance.Chain = nil },
		"missing setBy":      func(d *EventDescriptor) { d.Provenance.SetBy = nil },
	} {
		t.Run(name, func(t *testing.T) {
			d := CloneEventDescriptors(auditFixture().Events)[1]
			mutate(&d)
			if _, err := json.Marshal(d); err == nil {
				t.Fatal("invalid descriptor accepted")
			}
		})
	}
}

func TestAuditPlanRefusesOverflowRatherThanTruncating(t *testing.T) {
	c := auditFixture()
	c.Events = c.Events[:1]
	for i := 1; i < MaxChangesetEvents; i++ {
		d := CloneEventDescriptors(auditFixture().Events[1:])[0]
		d.Index = Natural64(uint64(i))
		d.Resource.Ref = RefByID(fmt.Sprintf("dependent-%04d", i), Natural64(0))
		d.Details.Dependent.Ref = d.Resource.Ref
		d.Details.Edge.Dependent = d.Resource.Ref
		c.Events = append(c.Events, d)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal("bound must be accepted", err)
	}
	roundtripComponent(t, "Changeset", string(raw), &Changeset{})
	d := c.Events[len(c.Events)-1]
	d.Index = Natural64(1000)
	c.Events = append(c.Events, d)
	if _, err := json.Marshal(c); err == nil {
		t.Fatal("overflow plan accepted")
	}
	if len(c.Events) != 1001 {
		t.Fatal("overflow plan was truncated")
	}
	v := parseJSON(t, raw).(map[string]any)
	events := v["events"].([]any)
	v["events"] = append(events, events[len(events)-1])
	bad, _ := json.Marshal(v)
	rejectComponent(t, "Changeset", string(bad), &Changeset{})
	for _, empty := range []string{`[]`, `null`} {
		v := parseJSON(t, raw).(map[string]any)
		v["events"] = json.RawMessage(empty)
		bad, _ := json.Marshal(v)
		rejectComponent(t, "Changeset", string(bad), &Changeset{})
	}
}

func TestAuditProjectlessLargeRevisionAndDeterministicEncoding(t *testing.T) {
	c := auditFixture()
	c.Revision = Natural("1e30")
	c.Events[1].Details.BlockerAfter.Revision = c.Revision
	c.Events[1].Details.Ontology.Project = nil
	for i := range c.Events {
		c.Events[i].Provenance.SetBy = map[string]string{"z": "request", "a": "mutator"}
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	roundtripComponent(t, "Changeset", string(raw), &Changeset{})
	for i := range c.Events {
		c.Events[i].Provenance.SetBy = map[string]string{"a": "mutator", "z": "request"}
	}
	again, err := json.Marshal(c)
	if err != nil || string(again) != string(raw) {
		t.Fatal("map iteration changed encoding", err)
	}
	resources, err := c.Events[1].RequiredResources()
	if err != nil || len(resources) != 3 {
		t.Fatal("projectless protection set", err, resources)
	}
	c.Events[0].Provenance.Cause.Chain = []string{"listener"}
	clone := CloneEventDescriptors(c.Events)
	clone[0].Provenance.Cause.Chain[0] = "changed"
	if c.Events[0].Provenance.Cause.Chain[0] != "listener" {
		t.Fatal("cause chain aliases source")
	}
}

func TestAuditProvenanceAndTextBounds(t *testing.T) {
	p := auditFixture().Events[0].Provenance
	p.SetBy = map[string]string{}
	for i := 0; i < 1000; i++ {
		p.SetBy[fmt.Sprintf("field-%04d", i)] = "request"
	}
	p.Chain = make([]string, 200)
	p.Cause.Chain = make([]string, 200)
	for i := range p.Chain {
		p.Chain[i] = "authz/link"
		p.Cause.Chain[i] = "listener"
	}
	p.Method = strings.Repeat("界", 1024)
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal("bounds refused", err)
	}
	roundtripComponent(t, "EventProvenance", string(raw), &EventProvenance{})
	for name, mutate := range map[string]func(*EventProvenance){
		"field owners":        func(p *EventProvenance) { p.SetBy["one-more"] = "request" },
		"authorization chain": func(p *EventProvenance) { p.Chain = append(p.Chain, "extra") },
		"causal chain":        func(p *EventProvenance) { p.Cause.Chain = append(p.Cause.Chain, "extra") },
		"text":                func(p *EventProvenance) { p.Method += "界" },
		"cause depth":         func(p *EventProvenance) { p.Cause.Depth = Natural64(9) },
	} {
		t.Run(name, func(t *testing.T) {
			var copy EventProvenance
			if err := json.Unmarshal(raw, &copy); err != nil {
				t.Fatal(err)
			}
			mutate(&copy)
			if _, err := json.Marshal(copy); err == nil {
				t.Fatal("audit bound exceeded")
			}
		})
	}
	set := auditFixture()
	raw, err = json.Marshal(set.Events)
	if err != nil {
		t.Fatal(err)
	}
	rejectComponent(t, "ChangeSet", `{"ops":[],"events":`+string(raw)+`}`, &ChangeSet{})
}

func TestAuditFailedDecodePreservesOriginal(t *testing.T) {
	original := auditFixture()
	set := original
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	v := parseJSON(t, raw).(map[string]any)
	v["events"].([]any)[1].(map[string]any)["type"] = "dependency.conditionunmet"
	bad, _ := json.Marshal(v)
	if err := json.Unmarshal(bad, &set); err == nil {
		t.Fatal("invalid descriptor type accepted")
	}
	if !reflect.DeepEqual(set, original) {
		t.Fatal("failed changeset decode published partial metadata")
	}
	descriptor := original.Events[1]
	if err := json.Unmarshal([]byte(`{"index":0}`), &descriptor); err == nil {
		t.Fatal("incomplete descriptor accepted")
	}
	if !reflect.DeepEqual(descriptor, original.Events[1]) {
		t.Fatal("failed descriptor decode published partial metadata")
	}
}

func TestAuditRevisionOrderPreservesUnboundedDomain(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"0.0", "0", 0}, {"0", "1e1000000000", -1}, {"5", "4", 1},
		{"1e30", "2e30", -1}, {"10e29", "1e30", 0},
		{"123e999999999", "1231e999999998", -1},
		{"1e1000000000", "9e999999999", 1}, {"1001", "1e3", 1},
	} {
		if got := compareAuditRevision(Natural(tc.a), Natural(tc.b)); got != tc.want {
			t.Fatalf("compare %s %s = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
	c := auditFixture()
	c.Revision = Natural("1e1000000000")
	c.Events[1].Details.BlockerAfter.Revision = c.Revision
	c.Events[1].Provenance.Cause.Depth = Natural("0.0")
	if err := c.ValidateEvents(); err != nil {
		t.Fatal("equivalent depth or symbolic revision refused", err)
	}
}

func TestAuditIntegralSpellingsAndReferenceDiscriminators(t *testing.T) {
	d := auditFixture().Events[1]
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	v := parseJSON(t, raw).(map[string]any)
	v["index"], v["changeIndex"] = json.Number("1.0"), json.Number("0e0")
	resource := v["resource"].(map[string]any)
	resource["ref"].(map[string]any)["branch"] = json.Number("0.0")
	integral, _ := json.Marshal(v)
	roundtripComponent(t, "EventDescriptor", string(integral), &EventDescriptor{})
	for _, ref := range []string{`{}`, `{"key":"display"}`, `{"id":""}`, `{"id":"a","key":"b"}`, `{"id":"a","branch":1}`, `{"id":"a","extra":true}`, `{"id":null}`, `"a"`} {
		bad := parseJSON(t, raw).(map[string]any)
		bad["resource"].(map[string]any)["ref"] = json.RawMessage(ref)
		encoded, _ := json.Marshal(bad)
		rejectComponent(t, "EventDescriptor", string(encoded), &EventDescriptor{})
	}
}

func TestCloseAuditOperationIsRestrictedToItsSlotAndReason(t *testing.T) {
	for _, event := range []DurableEventType{EventBeadClosed, EventBeadCommented} {
		t.Run(string(event), func(t *testing.T) {
			c := auditFixture()
			c.Changes[0].Op = OpClose
			c.Events = c.Events[:1]
			c.Events[0].Type = event
			raw, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			var decoded Changeset
			if err := json.Unmarshal(raw, &decoded); err != nil || !reflect.DeepEqual(c, decoded) {
				t.Fatalf("close audit did not round trip: %s %v", raw, err)
			}
		})
	}
	for _, event := range []DurableEventType{EventBeadCreated, EventBeadUpdated, EventBeadDeleted, EventBeadReparented, EventBeadLinked, EventBeadUnlinked, EventBeadAttached, EventBeadClaimed, EventBeadReleased, EventBeadFailed, EventBeadCanceled, EventBeadReopened, EventBeadDeferred, EventBeadLeaseReclaimed, EventMemorySaved, EventMemoryDeleted, EventGateApproved, EventGateReported} {
		t.Run("refuse-"+string(event), func(t *testing.T) {
			c := auditFixture()
			c.Changes[0].Op = OpClose
			c.Events = c.Events[:1]
			c.Events[0].Type = event
			if _, err := json.Marshal(c); err == nil {
				t.Fatal("close fabricated another operation's event")
			}
		})
	}
}

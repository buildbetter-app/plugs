package model

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
)

// MaxChangesetEvents bounds the complete atomic event plan, ordinary events
// included. Overflow refuses the write; it never truncates condition notices.
const MaxChangesetEvents = MaxChangeSetOps

// EventProvenance is stamped by the trusted Writer, not accepted from ChangeSet
// input. Slot is empty for a non-slot write. Chain records authorization links.
type EventProvenance struct {
	Method string            `json:"method"`
	Slot   string            `json:"slot"`
	SetBy  map[string]string `json:"setBy"`
	Cause  EventCause        `json:"cause"`
	Chain  []string          `json:"chain"`
}

// EventDescriptor is a closed durable event plan. ChangeIndex anchors an actual
// changed record, including notices whose resource is an unchanged dependent.
// Index is the position in Changeset.Events. Images are reconstructed from the
// pinned history, not duplicated here. Only conditionUnmet has Details.
type EventDescriptor struct {
	Index       Natural                `json:"index"`
	ChangeIndex Natural                `json:"changeIndex"`
	Type        DurableEventType       `json:"type"`
	Resource    AuditResource          `json:"resource"`
	Provenance  EventProvenance        `json:"provenance"`
	Details     *ConditionUnmetDetails `json:"details,omitempty"`
}

func (p EventProvenance) Validate() error {
	if err := auditText(p.Method, "method"); err != nil {
		return err
	}
	switch p.Slot {
	case "", "claim", "close", "reopen", "fail", "cancel", "defer":
	default:
		return fmt.Errorf("unknown source slot %q", p.Slot)
	}
	if p.SetBy == nil || p.Chain == nil {
		return fmt.Errorf("provenance collections must be present")
	}
	if len(p.SetBy) > MaxChangeSetOps || len(p.Chain) > 200 || len(p.Cause.Chain) > 200 {
		return fmt.Errorf("provenance exceeds audit collection bound")
	}
	for key, value := range p.SetBy {
		if err := auditText(key, "setBy field"); err != nil {
			return err
		}
		if err := auditText(value, "setBy owner"); err != nil {
			return err
		}
	}
	for _, chain := range [][]string{p.Chain, p.Cause.Chain} {
		for _, id := range chain {
			if err := auditText(id, "chain member"); err != nil {
				return err
			}
		}
	}
	if err := auditText(p.Cause.Root, "cause root"); err != nil {
		return err
	}
	return p.Cause.Validate()
}
func (p EventProvenance) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	type wire EventProvenance
	return json.Marshal(wire(p))
}
func (p *EventProvenance) UnmarshalJSON(data []byte) error {
	type wire EventProvenance
	var v wire
	if err := decodeStrict(data, &v, "method", "slot", "setBy", "cause", "chain"); err != nil {
		return err
	}
	d := EventProvenance(v)
	if err := d.Validate(); err != nil {
		return err
	}
	*p = d
	return nil
}

func eventResourceKind(t DurableEventType) string {
	switch t {
	case EventOntologyApplied:
		return "ontology"
	case EventPluginApplied:
		return "plugin"
	case EventViewSaved, EventViewDeleted:
		return "view"
	case EventMemorySaved, EventMemoryDeleted:
		return "memory"
	case EventSyncPushed, EventSyncImported:
		return "sync"
	case EventDerivedUpdated:
		return "derived"
	default:
		return "bead"
	}
}

func (d EventDescriptor) Validate() error {
	if index, err := d.Index.Uint64(); err != nil || index >= MaxChangesetEvents {
		return fmt.Errorf("event index must be below %d", MaxChangesetEvents)
	}
	if index, err := d.ChangeIndex.Uint64(); err != nil || index >= MaxChangeSetOps {
		return fmt.Errorf("change index must be below %d", MaxChangeSetOps)
	}
	if !validDurableEventType(d.Type) {
		return fmt.Errorf("unknown descriptor type %q", d.Type)
	}
	if err := d.Resource.Validate(); err != nil {
		return err
	}
	if d.Resource.Kind != eventResourceKind(d.Type) {
		return fmt.Errorf("event/resource kind mismatch")
	}
	if err := d.Provenance.Validate(); err != nil {
		return err
	}
	if d.Type == EventDependencyConditionUnmet {
		if d.Details == nil {
			return fmt.Errorf("condition-unmet descriptor needs details")
		}
		if err := d.Details.Validate(); err != nil {
			return err
		}
		if !sameAuditRef(d.Resource.Ref, d.Details.Dependent.Ref) {
			return fmt.Errorf("notice resource must be the dependent")
		}
		if (d.Provenance.Slot != "close" || d.Details.After.Category != "completed" && d.Details.After.Category != "done") && (d.Provenance.Slot != "cancel" || d.Details.After.Category != "canceled") {
			return fmt.Errorf("condition decision/source slot mismatch")
		}
	} else if d.Details != nil {
		return fmt.Errorf("ordinary descriptor forbids condition details")
	}
	for event, slot := range map[DurableEventType]string{EventBeadClaimed: "claim", EventBeadClosed: "close", EventBeadFailed: "fail", EventBeadCanceled: "cancel", EventBeadReopened: "reopen", EventBeadDeferred: "defer"} {
		if d.Type == event && d.Provenance.Slot != slot {
			return fmt.Errorf("event/source slot mismatch")
		}
	}
	return nil
}

func isGateEvent(kind DurableEventType) bool {
	return kind == EventGateApproved || kind == EventGateReported
}
func (d EventDescriptor) MarshalJSON() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	type wire EventDescriptor
	return json.Marshal(wire(d))
}
func (d *EventDescriptor) UnmarshalJSON(data []byte) error {
	type wire EventDescriptor
	var v wire
	if err := decodeStrict(data, &v, "index", "changeIndex", "type", "resource", "provenance"); err != nil {
		return err
	}
	decoded := EventDescriptor(v)
	if err := decoded.Validate(); err != nil {
		return err
	}
	*d = decoded
	return nil
}

// RequiredResources is the complete set whose current read policies protect
// the descriptor, sorted and deduplicated. An unreadable/redacted member hides
// the ENTIRE condition notice, including its existence and edge/status facts.
// Watch visibility markers may only name an already-visible primary resource.
func (d EventDescriptor) RequiredResources() ([]AuditResource, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	r := []AuditResource{d.Resource}
	if d.Details != nil {
		r = append(r, AuditResource{Kind: "bead", Ref: d.Details.BlockerAfter.Ref}, AuditResource{Kind: "ontology", Ref: d.Details.Ontology.Ref})
		if d.Details.Ontology.Project != nil {
			r = append(r, AuditResource{Kind: "project", Ref: *d.Details.Ontology.Project})
		}
	}
	sort.Slice(r, func(i, j int) bool {
		if r[i].Kind != r[j].Kind {
			return r[i].Kind < r[j].Kind
		}
		return r[i].Ref.ID < r[j].Ref.ID
	})
	out := make([]AuditResource, 0, len(r))
	for _, resource := range r {
		if len(out) == 0 || out[len(out)-1].Kind != resource.Kind || out[len(out)-1].Ref.ID != resource.Ref.ID {
			out = append(out, resource)
		}
	}
	return out, nil
}

// CloneEventDescriptors gives drivers an owned copy, retaining nil as the
// legacy-row discriminator. Validate the COMPLETE plan before storing it.
func CloneEventDescriptors(events []EventDescriptor) []EventDescriptor {
	if events == nil {
		return nil
	}
	out := make([]EventDescriptor, len(events))
	copy(out, events)
	for i := range out {
		p := &out[i].Provenance
		p.SetBy = cloneAuditMap(p.SetBy)
		p.Chain = cloneAuditStrings(p.Chain)
		p.Cause.Chain = cloneAuditStrings(p.Cause.Chain)
		if out[i].Details != nil {
			d := *out[i].Details
			if d.Ontology.Project != nil {
				project := *d.Ontology.Project
				d.Ontology.Project = &project
			}
			out[i].Details = &d
		}
	}
	return out
}
func cloneAuditMap(v map[string]string) map[string]string {
	if v == nil {
		return nil
	}
	out := make(map[string]string, len(v))
	for k, value := range v {
		out[k] = value
	}
	return out
}
func cloneAuditStrings(v []string) []string {
	if v == nil {
		return nil
	}
	out := make([]string, len(v))
	copy(out, v)
	return out
}

// ValidateEvents checks the complete ordered plan without consulting live
// graph or ontology state. Only the trusted Writer can establish those facts.
func (c Changeset) ValidateEvents() error {
	if c.Events == nil {
		return nil
	}
	if len(c.Events) == 0 || len(c.Events) > MaxChangesetEvents || len(c.Changes) == 0 {
		return fmt.Errorf("event plan must contain 1..%d events for actual changes", MaxChangesetEvents)
	}
	change := -1
	var ordinary EventDescriptor
	lastDependent := ""
	gateForChange := false
	for index, event := range c.Events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("event %d: %w", index, err)
		}
		i, _ := event.Index.Uint64()
		anchor, _ := event.ChangeIndex.Uint64()
		if i != uint64(index) || anchor >= uint64(len(c.Changes)) {
			return fmt.Errorf("event indexes must be contiguous and anchored in changes")
		}
		if !slices.Equal(event.Provenance.Chain, c.Chain) {
			return fmt.Errorf("event/header authorization chain mismatch")
		}
		if isGateEvent(event.Type) && change >= 0 && anchor == uint64(change) {
			ordinaryIndex, _ := ordinary.Index.Uint64()
			slotEvent := ordinary.Type == EventBeadClosed || ordinary.Type == EventBeadFailed && event.Type == EventGateReported
			if gateForChange || !slotEvent || i != ordinaryIndex+1 || !sameAuditRef(event.Resource.Ref, ordinary.Resource.Ref) || !sameEventProvenance(event.Provenance, ordinary.Provenance) {
				return fmt.Errorf("gate event must follow its bead event exactly once")
			}
			gateForChange = true
			continue
		}
		if event.Type != EventDependencyConditionUnmet {
			if anchor != uint64(change+1) {
				return fmt.Errorf("each change needs exactly one ordinary event in change order")
			}
			change++
			ordinary, lastDependent = event, ""
			if !sameAuditRef(event.Resource.Ref, c.Changes[change].Ref) {
				return fmt.Errorf("ordinary event/changed resource mismatch")
			}
			if err := validateOrdinaryOp(event.Type, c.Changes[change].Op); err != nil {
				return err
			}
			gateForChange = false
		} else {
			if change < 0 || anchor != uint64(change) {
				return fmt.Errorf("notice must follow its ordinary event")
			}
			d := event.Details
			if (ordinary.Type != EventBeadClosed && ordinary.Type != EventBeadCanceled) || !sameEventProvenance(event.Provenance, ordinary.Provenance) {
				return fmt.Errorf("notice must retain ordinary terminal slot event and provenance")
			}
			if !sameAuditRef(d.BlockerAfter.Ref, c.Changes[change].Ref) || d.BlockerBefore.Revision.String() != c.Changes[change].FromRevision.String() || d.BlockerAfter.Revision.String() != c.Revision.String() {
				return fmt.Errorf("notice must pin changed blocker source revisions")
			}
			if d.Dependent.Ref.ID <= lastDependent {
				return fmt.Errorf("notices must have unique edges ordered by dependent ID")
			}
			lastDependent = d.Dependent.Ref.ID
		}
	}
	if change+1 != len(c.Changes) {
		return fmt.Errorf("event plan omits ordinary events")
	}
	return nil
}

func sameEventProvenance(a, b EventProvenance) bool {
	a.Cause.Depth = Natural(a.Cause.Depth.String())
	b.Cause.Depth = Natural(b.Cause.Depth.String())
	return reflect.DeepEqual(a, b)
}

func validateOrdinaryOp(event DurableEventType, op OpKind) error {
	if err := op.Validate(); err != nil {
		return err
	}
	if op == OpClose {
		if event == EventBeadClosed || event == EventBeadCommented {
			return nil
		}
		return fmt.Errorf("close event needs its close slot or same-slot reason")
	}
	expected := map[DurableEventType]OpKind{EventBeadCreated: OpCreate, EventBeadReparented: OpReparent, EventBeadLinked: OpLink, EventBeadUnlinked: OpUnlink, EventBeadCommented: OpComment, EventBeadAttached: OpAttach, EventBeadDeleted: OpDelete, EventBeadClaimed: OpPatch, EventBeadReleased: OpPatch, EventBeadClosed: OpPatch, EventBeadFailed: OpPatch, EventBeadCanceled: OpPatch, EventBeadReopened: OpPatch, EventBeadDeferred: OpPatch, EventBeadLeaseReclaimed: OpPatch}
	if kind, ok := expected[event]; ok && op != kind {
		return fmt.Errorf("ordinary event/operation mismatch")
	}
	if event == EventBeadUpdated && op != OpPatch && op != OpSetMeta {
		return fmt.Errorf("update event needs patch or setMeta")
	}
	return nil
}

func (c Changeset) MarshalJSON() ([]byte, error) {
	if err := c.ValidateEvents(); err != nil {
		return nil, err
	}
	type wire Changeset
	return json.Marshal(wire(c))
}
func (c *Changeset) UnmarshalJSON(data []byte) error {
	type wire Changeset
	var v wire
	if err := decodeStrict(data, &v, "revision", "branch", "incarnation", "kind", "time", "principal", "chain", "changes"); err != nil {
		return err
	}
	d := Changeset(v)
	if err := d.ValidateEvents(); err != nil {
		return err
	}
	*c = d
	return nil
}

package model

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type EventCategory string
type DurableEventType string
type ControlEventType string

const (
	EventDurable EventCategory = "durable"
	EventControl EventCategory = "control"

	EventBeadCreated        DurableEventType = "bead.created"
	EventBeadUpdated        DurableEventType = "bead.updated"
	EventBeadReparented     DurableEventType = "bead.reparented"
	EventBeadLinked         DurableEventType = "bead.linked"
	EventBeadUnlinked       DurableEventType = "bead.unlinked"
	EventBeadCommented      DurableEventType = "bead.commented"
	EventBeadAttached       DurableEventType = "bead.attached"
	EventBeadDeleted        DurableEventType = "bead.deleted"
	EventBeadClaimed        DurableEventType = "bead.claimed"
	EventBeadReleased       DurableEventType = "bead.released"
	EventBeadClosed         DurableEventType = "bead.closed"
	EventBeadFailed         DurableEventType = "bead.failed"
	EventBeadCanceled       DurableEventType = "bead.canceled"
	EventBeadReopened       DurableEventType = "bead.reopened"
	EventBeadDeferred       DurableEventType = "bead.deferred"
	EventBeadLeaseReclaimed DurableEventType = "bead.lease.reclaimed"
	EventGateApproved       DurableEventType = "gate.approved"
	EventGateReported       DurableEventType = "gate.reported"
	EventGateEscalated      DurableEventType = "gate.escalated"
	EventOntologyApplied    DurableEventType = "ontology.applied"
	EventPluginApplied      DurableEventType = "plugin.applied"
	EventViewSaved          DurableEventType = "view.saved"
	EventViewDeleted        DurableEventType = "view.deleted"
	EventMemorySaved        DurableEventType = "memory.saved"
	EventMemoryDeleted      DurableEventType = "memory.deleted"
	EventSyncPushed         DurableEventType = "sync.pushed"
	EventSyncImported       DurableEventType = "sync.imported"
	EventDerivedUpdated     DurableEventType = "derived.updated"

	EventGap            ControlEventType = "event.gap"
	EventWatchDelta     ControlEventType = "watch.delta"
	EventWatchReset     ControlEventType = "watch.reset"
	EventVisibilityLost ControlEventType = "visibility.lost"
	EventRPCRejected    ControlEventType = "rpc.rejected"
	EventStoreRestored  ControlEventType = "store.restored"
)

type EventPosition struct {
	Cursor      string   `json:"cursor"`
	Incarnation string   `json:"incarnation"`
	Revision    Revision `json:"revision"`
	Index       Natural  `json:"index"`
}
type EventCause struct {
	Root  string   `json:"root"`
	Depth Natural  `json:"depth"`
	Chain []string `json:"chain"`
}

func (c *EventCause) UnmarshalJSON(data []byte) error {
	type wireEventCause EventCause
	var value wireEventCause
	if err := decodeStrict(data, &value, "root", "depth", "chain"); err != nil {
		return err
	}
	if err := validateEventCause(EventCause(value)); err != nil {
		return err
	}
	*c = EventCause(value)
	return nil
}

func (c EventCause) MarshalJSON() ([]byte, error) {
	if err := validateEventCause(c); err != nil {
		return nil, err
	}
	type wireEventCause EventCause
	return json.Marshal(wireEventCause(c))
}

type DurableEvent struct {
	EventPosition
	Type      DurableEventType  `json:"type"`
	Resource  string            `json:"resource"`
	Changed   []string          `json:"changed"`
	Before    map[string]any    `json:"before,omitempty"`
	After     map[string]any    `json:"after,omitempty"`
	Principal PrincipalRef      `json:"principal"`
	Chain     []string          `json:"chain"`
	SetBy     map[string]string `json:"setBy"`
	Cause     EventCause        `json:"cause"`
	Derived   bool              `json:"derived"`
}
type ControlDetails interface{ controlDetails() }
type EventGapDetails struct {
	FromIncarnation string   `json:"fromIncarnation"`
	ToIncarnation   string   `json:"toIncarnation"`
	Floor           Revision `json:"floor"`
	Reason          string   `json:"reason"`
}

func (EventGapDetails) controlDetails() {}

func (d EventGapDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventGap, d); err != nil {
		return nil, err
	}
	type wireDetails EventGapDetails
	return json.Marshal(wireDetails(d))
}

func (d *EventGapDetails) UnmarshalJSON(data []byte) error {
	type wireDetails EventGapDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "fromIncarnation", "toIncarnation", "floor", "reason"); err != nil {
		return err
	}
	decoded := EventGapDetails(value)
	if err := validateControlDetails(EventGap, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type StoreRestoredDetails struct {
	FromIncarnation string `json:"fromIncarnation"`
	ToIncarnation   string `json:"toIncarnation"`
	Reason          string `json:"reason"`
}

func (StoreRestoredDetails) controlDetails() {}

func (d StoreRestoredDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventStoreRestored, d); err != nil {
		return nil, err
	}
	type wireDetails StoreRestoredDetails
	return json.Marshal(wireDetails(d))
}

func (d *StoreRestoredDetails) UnmarshalJSON(data []byte) error {
	type wireDetails StoreRestoredDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "fromIncarnation", "toIncarnation", "reason"); err != nil {
		return err
	}
	decoded := StoreRestoredDetails(value)
	if err := validateControlDetails(EventStoreRestored, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type WatchDeltaDetails struct {
	SubscriptionID string  `json:"subscriptionId"`
	FromCursor     string  `json:"fromCursor"`
	ToCursor       string  `json:"toCursor"`
	Events         []Event `json:"events"`
}

func (WatchDeltaDetails) controlDetails() {}

func (d WatchDeltaDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventWatchDelta, d); err != nil {
		return nil, err
	}
	type wireDetails WatchDeltaDetails
	return json.Marshal(wireDetails(d))
}

func (d *WatchDeltaDetails) UnmarshalJSON(data []byte) error {
	type wireDetails WatchDeltaDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "subscriptionId", "fromCursor", "toCursor", "events"); err != nil {
		return err
	}
	decoded := WatchDeltaDetails(value)
	if err := validateControlDetails(EventWatchDelta, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type WatchResetDetails struct {
	SubscriptionID string `json:"subscriptionId"`
	Reason         string `json:"reason"`
	SnapshotCursor string `json:"snapshotCursor"`
}

func (WatchResetDetails) controlDetails() {}

func (d WatchResetDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventWatchReset, d); err != nil {
		return nil, err
	}
	type wireDetails WatchResetDetails
	return json.Marshal(wireDetails(d))
}

func (d *WatchResetDetails) UnmarshalJSON(data []byte) error {
	type wireDetails WatchResetDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "subscriptionId", "reason", "snapshotCursor"); err != nil {
		return err
	}
	decoded := WatchResetDetails(value)
	if err := validateControlDetails(EventWatchReset, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type VisibilityLostDetails struct {
	Reason string `json:"reason"`
}

func (VisibilityLostDetails) controlDetails() {}

func (d VisibilityLostDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventVisibilityLost, d); err != nil {
		return nil, err
	}
	type wireDetails VisibilityLostDetails
	return json.Marshal(wireDetails(d))
}

func (d *VisibilityLostDetails) UnmarshalJSON(data []byte) error {
	type wireDetails VisibilityLostDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "reason"); err != nil {
		return err
	}
	decoded := VisibilityLostDetails(value)
	if err := validateControlDetails(EventVisibilityLost, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type RPCRejectedDetails struct {
	RequestID any    `json:"requestId"`
	Reason    string `json:"reason"`
}

func (RPCRejectedDetails) controlDetails() {}

// marshalRequestID retains the complete string/integer schema domain. JSON
// numbers are checked mathematically, without int64 or float64 conversion.
func marshalRequestID(value any) ([]byte, error) {
	switch value.(type) {
	case string:
		return json.Marshal(value)
	case json.Number, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		canonical, err := canonicalInteger(raw)
		if err != nil {
			return nil, fmt.Errorf("rpc.rejected requestId must be an integer: %w", err)
		}
		return []byte(canonical), nil
	default:
		return nil, fmt.Errorf("rpc.rejected requestId must be a string or integer")
	}
}

func (d RPCRejectedDetails) MarshalJSON() ([]byte, error) {
	if err := validateControlDetails(EventRPCRejected, d); err != nil {
		return nil, err
	}
	requestID, err := marshalRequestID(d.RequestID)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		RequestID json.RawMessage `json:"requestId"`
		Reason    string          `json:"reason"`
	}{requestID, d.Reason})
}

func (d *RPCRejectedDetails) UnmarshalJSON(data []byte) error {
	type wireDetails RPCRejectedDetails
	var value wireDetails
	if err := decodeStrict(data, &value, "requestId", "reason"); err != nil {
		return err
	}
	decoded := RPCRejectedDetails(value)
	if err := validateControlDetails(EventRPCRejected, decoded); err != nil {
		return err
	}
	*d = decoded
	return nil
}

type ControlEvent struct {
	EventPosition
	Type     ControlEventType `json:"type"`
	Resource string           `json:"resource,omitempty"`
	Details  ControlDetails   `json:"-"`
}
type Event struct {
	Category EventCategory `json:"-"`
	Durable  *DurableEvent `json:"-"`
	Control  *ControlEvent `json:"-"`
}

func validDurableEventType(kind DurableEventType) bool {
	switch kind {
	case EventBeadCreated, EventBeadUpdated, EventBeadReparented, EventBeadLinked, EventBeadUnlinked,
		EventBeadCommented, EventBeadAttached, EventBeadDeleted, EventBeadClaimed, EventBeadReleased,
		EventBeadClosed, EventBeadFailed, EventBeadCanceled, EventBeadReopened, EventBeadDeferred,
		EventBeadLeaseReclaimed, EventGateApproved, EventGateReported, EventGateEscalated, EventOntologyApplied,
		EventPluginApplied, EventViewSaved, EventViewDeleted, EventMemorySaved, EventMemoryDeleted,
		EventSyncPushed, EventSyncImported, EventDerivedUpdated:
		return true
	default:
		return false
	}
}

func validControlEventType(kind ControlEventType) bool {
	switch kind {
	case EventGap, EventWatchDelta, EventWatchReset, EventVisibilityLost, EventRPCRejected, EventStoreRestored:
		return true
	default:
		return false
	}
}

func validPrincipalKind(kind string) bool {
	switch kind {
	case "anonymous", "user", "agent", "service", "plugin", "system":
		return true
	default:
		return false
	}
}

func validGapReason(reason string) bool {
	switch reason {
	case "restore", "failover", "history_floor", "checkpoint":
		return true
	default:
		return false
	}
}

func validResetReason(reason string) bool {
	switch reason {
	case "policy_changed", "ontology_changed", "view_changed", "overflow", "lag", "visibility_changed", "view_deleted":
		return true
	default:
		return false
	}
}

func validVisibilityReason(reason string) bool {
	switch reason {
	case "authorization_changed", "redacted", "resource_deleted":
		return true
	default:
		return false
	}
}

func validateEventPosition(position EventPosition) error {
	if err := position.Revision.Validate(); err != nil {
		return fmt.Errorf("event revision: %w", err)
	}
	return position.Index.Validate()
}

func validateDurableEvent(value DurableEvent) error {
	if err := validateEventPosition(value.EventPosition); err != nil {
		return err
	}
	if !validDurableEventType(value.Type) {
		return fmt.Errorf("unknown durable event type %q", value.Type)
	}
	if value.Changed == nil || value.Chain == nil || value.SetBy == nil {
		return fmt.Errorf("durable event has a missing required collection")
	}
	if err := validatePrincipal(value.Principal); err != nil {
		return err
	}
	return validateEventCause(value.Cause)
}

func validatePrincipal(value PrincipalRef) error {
	seen := map[*PrincipalRef]bool{}
	for {
		if !validPrincipalKind(value.Kind) {
			return fmt.Errorf("unknown principal kind %q", value.Kind)
		}
		if value.Sponsor == nil {
			return nil
		}
		if seen[value.Sponsor] {
			return fmt.Errorf("cyclic principal sponsor")
		}
		seen[value.Sponsor] = true
		value = *value.Sponsor
	}
}

func validateEventCause(value EventCause) error {
	depth, err := value.Depth.Uint64()
	if err != nil || depth > 8 || value.Chain == nil {
		return fmt.Errorf("invalid event cause")
	}
	return nil
}

func validateControlDetails(kind ControlEventType, details ControlDetails) error {
	switch kind {
	case EventGap:
		value, ok := details.(EventGapDetails)
		if !ok || !validGapReason(value.Reason) || value.Floor.Validate() != nil {
			return fmt.Errorf("invalid event.gap details")
		}
	case EventStoreRestored:
		value, ok := details.(StoreRestoredDetails)
		if !ok || value.Reason != "restore" {
			return fmt.Errorf("invalid store.restored details")
		}
	case EventWatchDelta:
		value, ok := details.(WatchDeltaDetails)
		if !ok || value.Events == nil {
			return fmt.Errorf("invalid watch.delta details")
		}
	case EventWatchReset:
		value, ok := details.(WatchResetDetails)
		if !ok || !validResetReason(value.Reason) {
			return fmt.Errorf("invalid watch.reset details")
		}
	case EventVisibilityLost:
		value, ok := details.(VisibilityLostDetails)
		if !ok || !validVisibilityReason(value.Reason) {
			return fmt.Errorf("invalid visibility.lost details")
		}
	case EventRPCRejected:
		value, ok := details.(RPCRejectedDetails)
		if !ok || value.Reason != "notification_no_result" || value.RequestID == nil {
			return fmt.Errorf("invalid rpc.rejected details")
		}
		if _, err := marshalRequestID(value.RequestID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown control event type %q", kind)
	}
	return nil
}

func (e Event) MarshalJSON() ([]byte, error) {
	if (e.Durable == nil) == (e.Control == nil) {
		return nil, fmt.Errorf("event must select exactly one durable or control member")
	}
	if e.Durable != nil {
		if e.Category != "" && e.Category != EventDurable {
			return nil, fmt.Errorf("durable event has category %q", e.Category)
		}
		if err := validateDurableEvent(*e.Durable); err != nil {
			return nil, err
		}
		return json.Marshal(e.Durable)
	}
	if e.Category != "" && e.Category != EventControl {
		return nil, fmt.Errorf("control event has category %q", e.Category)
	}
	if !validControlEventType(e.Control.Type) || e.Control.Details == nil || !controlDetailsMatch(e.Control.Type, e.Control.Details) {
		return nil, fmt.Errorf("control event %q has the wrong details type", e.Control.Type)
	}
	if err := validateEventPosition(e.Control.EventPosition); err != nil {
		return nil, err
	}
	if err := validateControlDetails(e.Control.Type, e.Control.Details); err != nil {
		return nil, err
	}
	var resource *string
	if e.Control.Type == EventVisibilityLost {
		resource = &e.Control.Resource // required even when the string is empty
	} else if e.Control.Resource != "" {
		return nil, fmt.Errorf("only visibility.lost carries a resource")
	}
	details, err := json.Marshal(e.Control.Details)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Category    EventCategory    `json:"category"`
		Cursor      string           `json:"cursor"`
		Incarnation string           `json:"incarnation"`
		Revision    Revision         `json:"revision"`
		Index       Natural          `json:"index"`
		Type        ControlEventType `json:"type"`
		Resource    *string          `json:"resource,omitempty"`
		Details     json.RawMessage  `json:"details"`
		Derived     bool             `json:"derived"`
	}{EventControl, e.Control.Cursor, e.Control.Incarnation, e.Control.Revision, e.Control.Index, e.Control.Type, resource, details, false})
}

func controlDetailsMatch(kind ControlEventType, details ControlDetails) bool {
	switch kind {
	case EventGap:
		_, ok := details.(EventGapDetails)
		return ok
	case EventWatchDelta:
		_, ok := details.(WatchDeltaDetails)
		return ok
	case EventWatchReset:
		_, ok := details.(WatchResetDetails)
		return ok
	case EventVisibilityLost:
		_, ok := details.(VisibilityLostDetails)
		return ok
	case EventRPCRejected:
		_, ok := details.(RPCRejectedDetails)
		return ok
	case EventStoreRestored:
		_, ok := details.(StoreRestoredDetails)
		return ok
	default:
		return false
	}
}

func (d *DurableEvent) UnmarshalJSON(data []byte) error {
	var wire struct {
		Category EventCategory `json:"category"`
		EventPosition
		Type      DurableEventType  `json:"type"`
		Resource  string            `json:"resource"`
		Changed   []string          `json:"changed"`
		Before    map[string]any    `json:"before,omitempty"`
		After     map[string]any    `json:"after,omitempty"`
		Principal PrincipalRef      `json:"principal"`
		Chain     []string          `json:"chain"`
		SetBy     map[string]string `json:"setBy"`
		Cause     EventCause        `json:"cause"`
		Derived   bool              `json:"derived"`
	}
	if err := decodeStrict(data, &wire, "category", "cursor", "incarnation", "revision", "index", "type", "resource", "changed", "principal", "chain", "setBy", "cause", "derived"); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"before", "after"} {
		if raw, ok := fields[name]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("durable event %s must be an object when present", name)
		}
	}
	if wire.Category != EventDurable {
		return fmt.Errorf("durable event has category %q", wire.Category)
	}
	value := DurableEvent{
		EventPosition: wire.EventPosition,
		Type:          wire.Type,
		Resource:      wire.Resource,
		Changed:       wire.Changed,
		Before:        wire.Before,
		After:         wire.After,
		Principal:     wire.Principal,
		Chain:         wire.Chain,
		SetBy:         wire.SetBy,
		Cause:         wire.Cause,
		Derived:       wire.Derived,
	}
	if err := validateDurableEvent(value); err != nil {
		return err
	}
	*d = value
	return nil
}

func (d DurableEvent) MarshalJSON() ([]byte, error) {
	if err := validateDurableEvent(d); err != nil {
		return nil, err
	}
	type wireDurableEvent DurableEvent
	return json.Marshal(struct {
		Category EventCategory `json:"category"`
		wireDurableEvent
	}{EventDurable, wireDurableEvent(d)})
}

func (e *Event) UnmarshalJSON(data []byte) error {
	var head struct {
		Category EventCategory `json:"category"`
		Type     string        `json:"type"`
	}
	if err := decodeJSON(data, &head); err != nil {
		return err
	}
	decoded := Event{Category: head.Category}
	switch head.Category {
	case EventDurable:
		var value DurableEvent
		if err := decodeJSON(data, &value); err != nil {
			return err
		}
		decoded.Durable = &value
	case EventControl:
		var envelope struct {
			Category EventCategory `json:"category"`
			EventPosition
			Type     ControlEventType `json:"type"`
			Resource string           `json:"resource,omitempty"`
			Details  json.RawMessage  `json:"details"`
			Derived  bool             `json:"derived"`
		}
		if err := decodeStrict(data, &envelope, "category", "cursor", "incarnation", "revision", "index", "type", "details", "derived"); err != nil {
			return err
		}
		if envelope.Category != EventControl || !validControlEventType(envelope.Type) {
			return fmt.Errorf("unknown control event type %q", envelope.Type)
		}
		var fields map[string]json.RawMessage
		if err := decodeJSON(data, &fields); err != nil {
			return err
		}
		_, hasResource := fields["resource"]
		if (envelope.Type == EventVisibilityLost) != hasResource {
			return fmt.Errorf("only visibility.lost carries a required resource")
		}
		if err := validateEventPosition(envelope.EventPosition); err != nil {
			return err
		}
		if envelope.Derived {
			return fmt.Errorf("control event cannot be derived")
		}
		value := ControlEvent{EventPosition: envelope.EventPosition, Type: envelope.Type, Resource: envelope.Resource}
		switch envelope.Type {
		case EventGap:
			var details EventGapDetails
			if err := decodeStrict(envelope.Details, &details, "fromIncarnation", "toIncarnation", "floor", "reason"); err != nil {
				return err
			}
			value.Details = details
		case EventStoreRestored:
			var details StoreRestoredDetails
			if err := decodeStrict(envelope.Details, &details, "fromIncarnation", "toIncarnation", "reason"); err != nil {
				return err
			}
			value.Details = details
		case EventWatchDelta:
			var details WatchDeltaDetails
			if err := decodeStrict(envelope.Details, &details, "subscriptionId", "fromCursor", "toCursor", "events"); err != nil {
				return err
			}
			value.Details = details
		case EventWatchReset:
			var details WatchResetDetails
			if err := decodeStrict(envelope.Details, &details, "subscriptionId", "reason", "snapshotCursor"); err != nil {
				return err
			}
			value.Details = details
		case EventVisibilityLost:
			var details VisibilityLostDetails
			if err := decodeStrict(envelope.Details, &details, "reason"); err != nil {
				return err
			}
			value.Details = details
		case EventRPCRejected:
			var details RPCRejectedDetails
			if err := decodeStrict(envelope.Details, &details, "requestId", "reason"); err != nil {
				return err
			}
			value.Details = details
		default:
			return fmt.Errorf("unknown control event type %q", envelope.Type)
		}
		decoded.Control = &value
		if err := validateControlDetails(value.Type, value.Details); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown event category %q", head.Category)
	}
	*e = decoded
	return nil
}

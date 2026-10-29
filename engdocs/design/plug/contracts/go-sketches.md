# Plug Go contract sketches

These are public-shape sketches, not implementation code. `pluginapi` and
`sdk/go` are the only public Go surfaces (P47); the driver and core model
packages are internal. Names and fields are deliberately wire-oriented so a
native plugin and a source-built WASM plugin receive the same JSON-RPC
messages.

## Shared model and `ChangeSet`

```go
// package model (internal/core/model)
package model

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Revision int64
type Branch uint32
type ErrorCode string   // closed by components.schemas.ErrorCode in OpenRPC
type WarningCode string // closed by components.schemas.WarningCode in OpenRPC
type Ref struct {
	// The JSON representation is always an object. Exactly one of ID or Key is
	// set; Branch is valid only when ID is set. This is the same oneOf as the
	// OpenRPC Ref schema (there is no string shorthand on the wire).
	ID     string `json:"id,omitempty"`
	Branch Branch `json:"branch,omitempty"`
	Key    string `json:"key,omitempty"`
}

func (r Ref) MarshalJSON() ([]byte, error) {
	if (r.ID == "") == (r.Key == "") {
		return nil, fmt.Errorf("ref must contain exactly one of id or key")
	}
	if r.Key != "" && r.Branch != 0 {
		return nil, fmt.Errorf("ref key cannot carry a branch")
	}
	type wireRef Ref
	return json.Marshal(wireRef(r))
}

func (r *Ref) UnmarshalJSON(data []byte) error {
	type wireRef Ref
	var value wireRef
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	decoded := Ref(value)
	if (decoded.ID == "") == (decoded.Key == "") {
		return fmt.Errorf("ref must contain exactly one of id or key")
	}
	if decoded.Key != "" && decoded.Branch != 0 {
		return fmt.Errorf("ref key cannot carry a branch")
	}
	*r = decoded
	return nil
}

type ResourceRef struct {
	Kind string `json:"kind"`
	Ref  Ref    `json:"ref"`
}
type FieldSet []string

type Precondition struct {
	Ref      Ref       `json:"ref"`
	Revision *Revision `json:"revision,omitempty"`
	Field    string    `json:"field,omitempty"`
	Hash     string    `json:"hash,omitempty"`
}

type ChangeSet struct {
	Ops    []Op           `json:"ops"` // one atomic revision; up to 1,000 ops
	Expect []Precondition `json:"expect,omitempty"`
}

type Op struct {
	Kind     OpKind      `json:"-"`
	Create   *CreateOp   `json:"-"`
	Patch    *PatchOp    `json:"-"`
	Reparent *ReparentOp `json:"-"`
	Link     *LinkOp     `json:"-"` // carries both link and unlink
	Comment  *CommentOp  `json:"-"`
	Attach   *AttachOp   `json:"-"`
	Delete   *DeleteOp   `json:"-"`
	SetMeta  *SetMetaOp  `json:"-"`
}

type OpKind string

const (
	OpCreate   OpKind = "create"
	OpPatch    OpKind = "patch"
	OpReparent OpKind = "reparent"
	OpLink     OpKind = "link"
	OpUnlink   OpKind = "unlink"
	OpComment  OpKind = "comment"
	OpAttach   OpKind = "attach"
	OpDelete   OpKind = "delete"
	OpSetMeta  OpKind = "setMeta"
)

type CreateOp struct {
	Kind    OpKind         `json:"kind"`
	Key     string         `json:"key,omitempty"`
	Type    string         `json:"type"`
	Fields  map[string]any `json:"fields"`
	Parent  *Ref           `json:"parent,omitempty"`
	Project string         `json:"project,omitempty"`
}
type PatchOp struct {
	Kind   OpKind         `json:"kind"`
	Target Ref            `json:"target"`
	Fields map[string]any `json:"fields"`
}
type ReparentOp struct {
	Kind    OpKind `json:"kind"`
	Target  Ref    `json:"target"`
	Parent  *Ref   `json:"parent,omitempty"`
	Project string `json:"project,omitempty"`
}
type LinkOp struct {
	Kind     OpKind `json:"kind"`
	From     Ref    `json:"from"`
	To       Ref    `json:"to"`
	LinkKind string `json:"linkKind"`
}
type CommentOp struct {
	Kind   OpKind `json:"kind"`
	Target Ref    `json:"target"`
	Body   string `json:"body"`
}
type AttachOp struct {
	Kind       OpKind     `json:"kind"`
	Target     Ref        `json:"target"`
	Attachment Attachment `json:"attachment"`
}
type DeleteOp struct {
	Kind   OpKind `json:"kind"`
	Target Ref    `json:"target"`
}
type SetMetaOp struct {
	Kind   OpKind          `json:"kind"`
	Target Ref             `json:"target"`
	Key    string          `json:"key"`
	Value  json.RawMessage `json:"value"`
}
type Attachment struct {
	Kind       string `json:"kind"`
	URI        string `json:"uri,omitempty"`
	ContentRef string `json:"contentRef,omitempty"`
	MediaType  string `json:"mediaType"`
	Summary    string `json:"summary"`
}

// MarshalJSON flattens the selected member. The result is exactly one of the
// OpenRPC ChangeSet.ops oneOf schemas; it is never a {kind, create: {...}}
// wrapper. UnmarshalJSON performs the inverse discriminator selection.
func (o Op) MarshalJSON() ([]byte, error) {
	switch o.Kind {
	case OpCreate:
		if o.Create == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Create
		value.Kind = OpCreate
		return json.Marshal(value)
	case OpPatch:
		if o.Patch == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Patch
		value.Kind = OpPatch
		return json.Marshal(value)
	case OpReparent:
		if o.Reparent == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Reparent
		value.Kind = OpReparent
		return json.Marshal(value)
	case OpLink, OpUnlink:
		if o.Link == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Link
		value.Kind = o.Kind
		return json.Marshal(value)
	case OpComment:
		if o.Comment == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Comment
		value.Kind = OpComment
		return json.Marshal(value)
	case OpAttach:
		if o.Attach == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Attach
		value.Kind = OpAttach
		return json.Marshal(value)
	case OpDelete:
		if o.Delete == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.Delete
		value.Kind = OpDelete
		return json.Marshal(value)
	case OpSetMeta:
		if o.SetMeta == nil {
			return nil, fmt.Errorf("operation %q has no payload", o.Kind)
		}
		value := *o.SetMeta
		value.Kind = OpSetMeta
		return json.Marshal(value)
	default:
		return nil, fmt.Errorf("unknown operation kind %q", o.Kind)
	}
}

func (o *Op) UnmarshalJSON(data []byte) error {
	var tag struct {
		Kind OpKind `json:"kind"`
	}
	if err := json.Unmarshal(data, &tag); err != nil {
		return err
	}
	*o = Op{Kind: tag.Kind}
	switch tag.Kind {
	case OpCreate:
		var value CreateOp
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		o.Create = &value
	case OpPatch:
		var value PatchOp
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		o.Patch = &value
	case OpReparent:
		var value ReparentOp
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		o.Reparent = &value
	case OpLink, OpUnlink:
		var value LinkOp
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		o.Link = &value
	case OpComment:
		var value CommentOp
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		o.Comment = &value
	case OpAttach:
		var value AttachOp
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		o.Attach = &value
	case OpDelete:
		var value DeleteOp
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		o.Delete = &value
	case OpSetMeta:
		var value SetMetaOp
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		o.SetMeta = &value
	default:
		return fmt.Errorf("unknown operation kind %q", tag.Kind)
	}
	return nil
}

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
	EventStoreRestored      DurableEventType = "store.restored"

	EventGap            ControlEventType = "event.gap"
	EventWatchDelta     ControlEventType = "watch.delta"
	EventWatchReset     ControlEventType = "watch.reset"
	EventVisibilityLost ControlEventType = "visibility.lost"
	EventRPCRejected    ControlEventType = "rpc.rejected"
)

type EventPosition struct {
	Cursor      string   `json:"cursor"`
	Incarnation string   `json:"incarnation"`
	Revision    Revision `json:"revision"`
	Index       uint64   `json:"index"`
}
type EventCause struct {
	Root  string   `json:"root"`
	Depth uint8    `json:"depth"`
	Chain []string `json:"chain"`
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

type WatchDeltaDetails struct {
	SubscriptionID string  `json:"subscriptionId"`
	FromCursor     string  `json:"fromCursor"`
	ToCursor       string  `json:"toCursor"`
	Events         []Event `json:"events"`
}

func (WatchDeltaDetails) controlDetails() {}

type WatchResetDetails struct {
	SubscriptionID string `json:"subscriptionId"`
	Reason         string `json:"reason"`
	SnapshotCursor string `json:"snapshotCursor"`
}

func (WatchResetDetails) controlDetails() {}

type VisibilityLostDetails struct {
	Reason string `json:"reason"`
}

func (VisibilityLostDetails) controlDetails() {}

type RPCRejectedDetails struct {
	RequestID any    `json:"requestId"`
	Reason    string `json:"reason"`
}

func (RPCRejectedDetails) controlDetails() {}

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

func (e Event) MarshalJSON() ([]byte, error) {
	if (e.Durable == nil) == (e.Control == nil) {
		return nil, fmt.Errorf("event must select exactly one durable or control member")
	}
	if e.Durable != nil {
		if e.Category != "" && e.Category != EventDurable {
			return nil, fmt.Errorf("durable event has category %q", e.Category)
		}
		return json.Marshal(struct {
			Category EventCategory `json:"category"`
			DurableEvent
		}{EventDurable, *e.Durable})
	}
	if e.Category != "" && e.Category != EventControl {
		return nil, fmt.Errorf("control event has category %q", e.Category)
	}
	if e.Control.Details == nil || !controlDetailsMatch(e.Control.Type, e.Control.Details) {
		return nil, fmt.Errorf("control event %q has the wrong details type", e.Control.Type)
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
		Index       uint64           `json:"index"`
		Type        ControlEventType `json:"type"`
		Resource    string           `json:"resource,omitempty"`
		Details     json.RawMessage  `json:"details"`
		Derived     bool             `json:"derived"`
	}{EventControl, e.Control.Cursor, e.Control.Incarnation, e.Control.Revision, e.Control.Index, e.Control.Type, e.Control.Resource, details, false})
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
	default:
		return false
	}
}

func (e *Event) UnmarshalJSON(data []byte) error {
	var head struct {
		Category EventCategory `json:"category"`
		Type     string        `json:"type"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return err
	}
	*e = Event{Category: head.Category}
	switch head.Category {
	case EventDurable:
		var value DurableEvent
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		e.Durable = &value
	case EventControl:
		var envelope struct {
			EventPosition
			Type     ControlEventType `json:"type"`
			Resource string           `json:"resource,omitempty"`
			Details  json.RawMessage  `json:"details"`
			Derived  bool             `json:"derived"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			return err
		}
		if envelope.Derived {
			return fmt.Errorf("control event cannot be derived")
		}
		value := ControlEvent{EventPosition: envelope.EventPosition, Type: envelope.Type, Resource: envelope.Resource}
		switch envelope.Type {
		case EventGap:
			var details EventGapDetails
			if err := json.Unmarshal(envelope.Details, &details); err != nil {
				return err
			}
			value.Details = details
		case EventWatchDelta:
			var details WatchDeltaDetails
			if err := json.Unmarshal(envelope.Details, &details); err != nil {
				return err
			}
			value.Details = details
		case EventWatchReset:
			var details WatchResetDetails
			if err := json.Unmarshal(envelope.Details, &details); err != nil {
				return err
			}
			value.Details = details
		case EventVisibilityLost:
			var details VisibilityLostDetails
			if err := json.Unmarshal(envelope.Details, &details); err != nil {
				return err
			}
			value.Details = details
		case EventRPCRejected:
			var details RPCRejectedDetails
			if err := json.Unmarshal(envelope.Details, &details); err != nil {
				return err
			}
			value.Details = details
		default:
			return fmt.Errorf("unknown control event type %q", envelope.Type)
		}
		e.Control = &value
	default:
		return fmt.Errorf("unknown event category %q", head.Category)
	}
	return nil
}

type PrincipalRef struct {
	Kind    string         `json:"kind"` // anonymous, user, agent, service, plugin, system
	ID      string         `json:"id"`
	Sponsor *PrincipalRef  `json:"sponsor,omitempty"`
	Caveats map[string]any `json:"caveats,omitempty"`
}
type Commit struct {
	Revision Revision   `json:"revision"`
	Results  []OpResult `json:"results"`
	Warnings []Warning  `json:"warnings"`
	// Dry-run responses also carry these fields. They are omitted on a real
	// commit unless the caller requested a dry run.
	WouldCommit bool        `json:"wouldCommit,omitempty"`
	Violations  []Violation `json:"violations,omitempty"`
}
type OpResult struct {
	Ref      Ref               `json:"ref"`
	Revision Revision          `json:"revision"`
	Record   json.RawMessage   `json:"record,omitempty"`
	SetBy    map[string]string `json:"setBy,omitempty"`
}
type Warning struct {
	Code    WarningCode    `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}
type Violation struct {
	Guard   string         `json:"guard"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// Writer is the only core entry point for graph writes. Named verbs lower to
// this same pipeline; tx.apply supplies ChangeSet directly.
type Writer interface {
	Apply(ctx context.Context, by PrincipalRef, cs ChangeSet, meta WriteMeta) (Commit, error)
}
type WriteMeta struct {
	Project        string
	IdempotencyKey string
	DryRun         bool
	Return         string // full or minimal
	Traceparent    string
	Agent          string
}

type ChangesetKind string

const (
	ChangesetWrite     ChangesetKind = "write"
	ChangesetDerived   ChangesetKind = "derived"
	ChangesetRevert    ChangesetKind = "revert"
	ChangesetMigration ChangesetKind = "migration"
	ChangesetRedact    ChangesetKind = "redact"
	ChangesetRestore   ChangesetKind = "restore"
)

type Changeset struct {
	Revision       Revision       `json:"revision"`
	Branch         Branch         `json:"branch"`
	Incarnation    string         `json:"incarnation"`
	Kind           ChangesetKind  `json:"kind"`
	Time           time.Time      `json:"time"`
	Principal      PrincipalRef   `json:"principal"`
	Chain          []string       `json:"chain"`
	IdempotencyKey string         `json:"idempotencyKey,omitempty"`
	Mutators       []string       `json:"mutators,omitempty"`
	Changes        []ChangeRecord `json:"changes"`
}
type ChangeRecord struct {
	Ref          Ref      `json:"ref"`
	Op           OpKind   `json:"op"`
	FromRevision Revision `json:"fromRevision"`
	ChangedPaths []string `json:"changedPaths"`
}
```

`Op.Kind` is a closed union: the selected pointer must match `Kind`, and
startup registry lint rejects an operation or method whose effect lowers to no
member. Protected plugin-set, lock, instance-config, and ontology records are
not legal targets for `tx.apply`, `setMeta`, or `history.revert`.

## `pluginapi` (public)

```go
// package pluginapi
package pluginapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// These wire-safe types live in pluginapi itself. A public SDK cannot import
// Plug's internal core/model package; the host converts these values at the
// boundary.
type Revision int64
type Ref struct {
	// Host conversion applies the same object-form oneOf as model.Ref; this
	// public alias never uses the old string shorthand.
	ID     string `json:"id,omitempty"`
	Branch uint32 `json:"branch,omitempty"`
	Key    string `json:"key,omitempty"`
}

func (r Ref) MarshalJSON() ([]byte, error) {
	if (r.ID == "") == (r.Key == "") {
		return nil, fmt.Errorf("ref must contain exactly one of id or key")
	}
	if r.Key != "" && r.Branch != 0 {
		return nil, fmt.Errorf("ref key cannot carry a branch")
	}
	type wireRef Ref
	return json.Marshal(wireRef(r))
}

func (r *Ref) UnmarshalJSON(data []byte) error {
	type wireRef Ref
	var value wireRef
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	decoded := Ref(value)
	if (decoded.ID == "") == (decoded.Key == "") {
		return fmt.Errorf("ref must contain exactly one of id or key")
	}
	if decoded.Key != "" && decoded.Branch != 0 {
		return fmt.Errorf("ref key cannot carry a branch")
	}
	*r = decoded
	return nil
}

type ResourceRef struct {
	Kind string `json:"kind"`
	Ref  Ref    `json:"ref"`
}
type PrincipalRef struct {
	Kind    string         `json:"kind"`
	ID      string         `json:"id"`
	Sponsor *PrincipalRef  `json:"sponsor,omitempty"`
	Caveats map[string]any `json:"caveats,omitempty"`
}
type Violation struct {
	Guard   string         `json:"guard"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}
type Op struct {
	Kind string `json:"kind"`
	// Payload is the selected flat operation object (including its kind), not
	// a nested {kind, create: {...}} wrapper. The host validates it with the
	// model.Op/OpenRPC union before dispatch.
	Payload json.RawMessage `json:"-"`
}
type Record struct {
	Ref      Ref             `json:"ref"`
	Kind     string          `json:"kind"`
	Revision Revision        `json:"revision"`
	Body     json.RawMessage `json:"body"`
}

type ConfigDoc = map[string]any
type ConfigProblem struct {
	Path, Code, Message string
	Details             map[string]any
}
type IntegrityParams struct {
	Revision Revision
	Project  string
}
type IntegrityProblem struct {
	Path, Code, Message string
	Details             map[string]any
}

type Configurable interface {
	ValidateConfig(context.Context, ConfigDoc) ([]ConfigProblem, error)
	ApplyConfig(context.Context, ConfigDoc) error
	CheckIntegrity(context.Context, IntegrityParams) ([]IntegrityProblem, error)
}

type Host interface {
	// Call is the plugin's only Plug API. The host validates the method,
	// manifest prefix, owned-field and bridge/egress grants before dispatch.
	Call(ctx context.Context, method string, params, result any) error
	State() StateStore
	HTTP() *http.Client                                  // host egress proxy; unavailable in hot calls
	Secret(context.Context, string) (SecretValue, error) // audited injection
}

type StateStore interface {
	Get(ctx context.Context, key string, dst any) (found bool, err error)
	Put(ctx context.Context, key string, value any, expect Revision) (Revision, error)
	Delete(ctx context.Context, key string, expect Revision) error
	Revision(ctx context.Context, key string) (Revision, error)
}
type SecretValue struct {
	Name     string
	Redacted bool
}
type RuntimeState string

const (
	RuntimeHealthy  RuntimeState = "healthy"
	RuntimeDegraded RuntimeState = "degraded"
)

type ProposedChange struct {
	RequestID    string       `json:"requestId"`
	Principal    PrincipalRef `json:"principal"`
	BaseRevision Revision     `json:"baseRevision"`
	DryRun       bool         `json:"dryRun"`
	Changes      []Change     `json:"changes"`
}
type CommittedChange struct {
	RequestID string       `json:"requestId"`
	Principal PrincipalRef `json:"principal"`
	Revision  Revision     `json:"revision"`
	Changes   []Change     `json:"changes"`
}
type Change struct {
	Resource ResourceRef       `json:"resource"`
	Op       Op                `json:"op"`
	Before   json.RawMessage   `json:"before,omitempty"`
	After    json.RawMessage   `json:"after,omitempty"`
	SetBy    map[string]string `json:"setBy,omitempty"`
}
type Patch struct {
	Resource ResourceRef
	Paths    map[string]json.RawMessage
}
type Verdict struct {
	Allow      bool
	Violations []Violation
}

type SnapshotReader interface {
	// Reads are pinned to ProposedChange.BaseRevision and join the commit
	// guard's read set. A hot call cannot write or use egress.
	Get(context.Context, []Ref) ([]Record, error)
	Query(context.Context, Query) (Page[Record], error)
	Beads(context.Context, []Ref) ([]View, error)
	Closure(context.Context, Ref, bool) ([]Ref, error)
	Grants(context.Context, []SubjectRef, []Ref) ([]Grant, error)
}
type Mutator interface {
	Mutate(context.Context, ProposedChange, SnapshotReader) ([]Patch, error)
}
type Validator interface {
	Validate(context.Context, ProposedChange, SnapshotReader) (Verdict, error)
}
type PostSubscriber interface {
	// Runs after commit, before the response, at the committed revision. It
	// cannot change the committed ChangeSet; failures are warnings and DLQ.
	After(context.Context, CommittedChange, SnapshotReader) error
}

type Query struct {
	Filter map[string]any
	Cursor *Cursor
	Limit  int
}
type Cursor struct {
	// The JSON-RPC adapter encodes this bound value as the opaque cursor string;
	// plugins never choose or reinterpret the token.
	Incarnation      string   `json:"incarnation"`
	Revision         Revision `json:"revision"`
	Index            uint64   `json:"index"`
	Method           string   `json:"method"`
	FilterDigest     string   `json:"filterDigest"`
	PrincipalDigest  string   `json:"principalDigest"`
	OntologyRevision Revision `json:"ontologyRevision"`
}
type Page[T any] struct {
	Items      []T
	NextCursor *Cursor
	AsOf       Revision
}
type View struct {
	ID     string
	Type   string
	Fields map[string]any
}
type SubjectRef struct{ Kind, ID string }
type Grant struct {
	Subject SubjectRef
	Scope   Ref
	Role    string
}

// A native or WASM plugin registers only methods under x.<alias>.*. Authz
// links and synchronous subscribers are compiled in for v1; mode-2 authz is
// not permitted.
type Method struct {
	Name       string
	Params     any
	Result     any
	Permission string
	Resource   func(params any) []Ref
}
type Registry interface{ Register(Method) error }
```

Hot-path calls (`authn.authenticate`, `authz.*`, `mutate`, `validate`) receive
only a revision-pinned `SnapshotReader`; `Host.Call` writes fail with
`plugin_host_write_in_sync_call`, and HTTP egress fails with
`plugin_egress_in_hot_path`. Post subscribers may use their granted egress and
state, but are capped by the one-second host deadline. A guest trap or pool
exhaustion is mapped by the host to the public error catalog; plugin-internal
`plugin_*` errors never cross the client boundary.

## Storage driver

```go
// package driver (internal/storage/driver)
package driver

import (
	"context"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type SchemaVersion uint64
type Hash string
type StoreID string

type Config struct {
	DSN     string
	Options map[string]any
}
type Info struct {
	Name         string
	Version      string
	Capabilities []string
}
type Meta struct {
	StoreID       StoreID
	Incarnation   string
	SchemaVersion SchemaVersion
	State         string
	Head          model.Revision
	HistoryFloor  model.Revision
}
type View struct {
	Branch      model.Branch
	At          *model.Revision
	Incarnation string // required; must match Meta.Incarnation
}
type Ref = model.Ref
type Record struct {
	Ref      Ref
	Kind     string
	Revision model.Revision
	Body     []byte
}
type Query struct {
	Filter map[string]any
	Cursor *Cursor
	Limit  int
	AsOf   *model.Revision
}
type PageReq struct {
	Cursor *Cursor
	Limit  int
}
type Version struct {
	Ref       Ref
	Revision  model.Revision
	Base      bool
	Patch     []byte
	Body      []byte
	CreatedAt time.Time
	Principal model.PrincipalRef
}
type DerivedTable string
type DerivedRow struct {
	Key   string
	Value []byte
}
type LockKeyKind string
type ReadFact string
type Sentinel string

const (
	LockRecord   LockKeyKind = "record"
	LockReadFact LockKeyKind = "read_fact"
	LockSentinel LockKeyKind = "sentinel"
	LockDerived  LockKeyKind = "derived"
)

// LockKey is the engine-neutral name for every item in the transaction lock
// set. Ref is used for records, ReadFact for recorded predicate/read facts,
// Sentinel for predicates, and DerivedRowKey for derived rows. Exactly one
// key form matches Kind. Drivers compare the tuple (kind-rank, encoded-key)
// bytewise.
type LockKey struct {
	Kind     LockKeyKind
	Ref      *Ref
	Fact     ReadFact
	Sentinel Sentinel
	Derived  *DerivedRowKey
}
type DerivedRowKey struct {
	Table DerivedTable
	Key   string
}
type LockRequest struct {
	Key  LockKey
	Mode LockMode
}
type LockState struct {
	Key      LockKey
	Revision model.Revision
	Record   *Record
	Derived  *DerivedRow
}
type LockSet []LockRequest

var (
	SentinelGraphBlocking = LockKey{Kind: LockSentinel, Sentinel: "graph/blocking"}
	SentinelReadySet      = LockKey{Kind: LockSentinel, Sentinel: "ready-set"}
	SentinelClosure       = LockKey{Kind: LockSentinel, Sentinel: "closure"}
	SentinelGrant         = LockKey{Kind: LockSentinel, Sentinel: "grant"}
	SentinelOntology      = LockKey{Kind: LockSentinel, Sentinel: "ontology"}
	SentinelPlugin        = LockKey{Kind: LockSentinel, Sentinel: "plugin"}
)

const (
	DerivedReadyState DerivedTable = "ready_state"
	DerivedClosure    DerivedTable = "closure"
	DerivedGrant      DerivedTable = "grant"
	DerivedOntology   DerivedTable = "ontology"
	DerivedPlugin     DerivedTable = "plugin"
)

// Cursor is the decoded driver form of the opaque RPC cursor. The binding
// fields prevent a cursor issued for another method, filter, principal, or
// ontology from being replayed against this reader.
type Cursor struct {
	Incarnation      string
	Revision         model.Revision
	Index            uint64
	Method           string
	FilterDigest     string
	PrincipalDigest  string
	OntologyRevision model.Revision
}

type ErrFenced struct {
	ExpectedEpoch uint64
	ObservedEpoch uint64
}

func (e ErrFenced) Error() string { return "driver lease fenced" }

type ErrTruncated struct {
	Floor model.Revision
}

func (e ErrTruncated) Error() string { return "history is truncated" }

type ErrIndeterminate struct {
	IdempotencyKey string
}

func (e ErrIndeterminate) Error() string { return "commit outcome indeterminate" }

type ErrIncarnationChanged struct {
	From string
	To   string
}

func (e ErrIncarnationChanged) Error() string { return "store incarnation changed" }

type LockMode string

const (
	LockShare      LockMode = "share"
	LockUpdate     LockMode = "update"
	LockSkipLocked LockMode = "skipLocked"
)

type ChangesetHeader struct {
	Branch         model.Branch
	Incarnation    string
	Kind           model.ChangesetKind
	Time           time.Time
	Principal      model.PrincipalRef
	Chain          []string
	IdempotencyKey string
	Mutators       []string
	Changes        []model.ChangeRecord
	LockKeys       []LockKey // complete, sorted set acquired before any write
}
type StoreHealth struct {
	Driver               string
	Role                 string // active, standby, unknown
	ReplicationLag       time.Duration
	LastBackupCheckpoint time.Time
	LeaseEpoch           uint64
	Head                 model.Revision
	Incarnation          string
	Ontology             string
	PluginSet            string
	AuthorizationChain   []string
}
type MigrationReport struct {
	From, To SchemaVersion
	Applied  []string
	Warnings []string
}
type DumpOptions struct {
	History  string
	PageSize int
}

type Driver interface {
	Info() Info
	Open(context.Context, Config) (Store, error)
}
type Store interface {
	Meta(context.Context) (Meta, error)
	Migrate(context.Context, SchemaVersion) (MigrationReport, error)

	// Lease blocks until the holder is active or ctx ends. The active holder
	// is fenced by epoch in every write, heartbeat and checkpoint commit.
	Lease(ctx context.Context, holder string, ttl time.Duration) (ActiveLease, error)
	Health(context.Context) (StoreHealth, error)

	// Read never waits for writers. The returned Reader is bound to the
	// requested branch/as-of view.
	Read(context.Context, View) (Reader, error)
	Write(context.Context, ActiveLease) (WriteTx, error)
}
type ActiveLease interface {
	Epoch() uint64
	Renew(context.Context) error
	Lost() <-chan struct{}
	Release(context.Context) error
}
type Reader interface {
	Get(context.Context, []Ref) ([]Record, error)
	Query(context.Context, Query) (Page[Record], error)
	Versions(context.Context, Ref, PageReq) (Page[Version], error)
	Changesets(context.Context, *Cursor, int) ([]model.Changeset, error)
	ChangesetByKey(context.Context, string, string, string, string) (*model.Changeset, error)
}
type WriteTx interface {
	Reader
	// keys must be globally sorted and duplicate-free. The returned state is
	// the revision observed while holding each lock.
	Lock(context.Context, []LockRequest) ([]LockState, error)
	Head(context.Context) (model.Revision, error)
	Put(context.Context, Record) error
	PutBlob(context.Context, []byte) (Hash, error)
	PutDerived(context.Context, DerivedTable, []DerivedRow, []DerivedRow) error
	Commit(context.Context, ChangesetHeader) (model.Changeset, error)
	Rollback(context.Context) error
}
```

The driver conformance contract is:

- **G1:** revisions are gapless and commit ordered; the head row is taken last
  in Postgres and `BEGIN IMMEDIATE` provides the SQLite equivalent.
- **G2:** `Lock` accepts the complete sorted lock set: written refs, recorded
  read facts, predicate sentinels, and derived rows. The global order is
  `record < read_fact < sentinel < derived`, then the canonical encoded key;
  an unsorted or duplicate set is rejected. It returns the latest committed
  state and revision (`FOR SHARE`/`FOR UPDATE` on Postgres; the driver's
  transaction lock on SQLite; per-key locks in memory).
- **G3:** records, versions, blobs, derived rows, and the changeset commit
  atomically. Every `PutDerived` call must have the corresponding
  `LockDerived` rows in the lock set; sentinel revisions are bumped in the
  same transaction as the operation or derived-row write. The commit header
  repeats the exact lock-key set so conformance can verify the declaration.
- **G4:** `(principal, anonymous-agent label when applicable, method, key)` is
  unique for 24 hours and the key is retained on the changeset.
- **G5:** every write, heartbeat, and checkpoint commits only under the active
  lease epoch. A deposed holder receives `ErrFenced`; after `Lost()` closes,
  the server stops admitting reads and writes until a new lease is acquired.
  A cursor or `View.Incarnation` that differs from `Meta.Incarnation` is
  rejected before any read with `ErrIncarnationChanged`, never interpreted as
  a revision in the new incarnation. `ErrTruncated` reports the history floor.

The sentinel catalog and bump rules are fixed for this contract:

| Sentinel | Read facts that record it | Operations or derived tables that bump it |
| --- | --- | --- |
| `graph/blocking` | cycle and blocking-edge predicates | `link`/`unlink` of a blocking edge |
| `ready-set` | ready-set membership and claim-next selection | bead status, dependency, lease, and `ready_state` changes |
| `closure` | ancestor/cut-closure predicates | reparent, restricted-toggle, and `closure` rows |
| `grant` | authorization grant and compiled-filter reads | grant writes and `grant` rows |
| `ontology` | ontology revision and guard reads | ontology apply and `ontology` rows |
| `plugin` | plugin-set, lock, and binding reads | plugin apply and `plugin` rows |

Each lock returns the current committed revision for its key. A bump writes the
new key revision in the same transaction and changeset as the operation or
`PutDerived` rows. Deferred fan-out acquires the same typed derived-row keys in
the follow-up `derived` changeset; it may lag ordering, but it cannot bypass
the lock set or revise a sentinel without recording that changeset.

The lease policy selected by P71 is a ten-second TTL and three-second renewal.
`ErrFenced`, `ErrTruncated`, `ErrIndeterminate`, and
`ErrIncarnationChanged` are driver errors with typed wire mappings in
[errors.md](errors.md); they are not additional JSON-RPC methods. A driver
must preserve the `ErrIndeterminate` idempotency key so the service can resolve
the outcome by key before retrying.

## Deliberate boundaries

The sketches leave JSON-RPC wire types in `contracts/openrpc.json` and keep
storage implementation details behind `driver`. `pluginapi` may depend on
wire-safe model aliases, but plugin code must not receive a driver transaction,
SQL handle, filesystem path, process environment, or unmediated network
client.

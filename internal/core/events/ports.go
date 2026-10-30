// Package events delivers committed changesets through engine-neutral ports.
// It neither writes graph state nor accepts plugin-defined event records.
package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

var (
	ErrCursorInvalid         = errors.New("invalid event cursor")
	ErrHalted                = errors.New("event binding halted")
	ErrStalled               = errors.New("event binding stalled")
	ErrSkipForbidden         = errors.New("event skip requires an audited listener or post failure")
	ErrResourceAuthorization = errors.New("event resource authorization unavailable")
	// ErrNoticeUnavailable suppresses a notice whose pinned source is absent.
	// History/fencing errors must remain errors, never this suppression signal.
	ErrNoticeUnavailable = errors.New("event notice source unavailable")
)

type Position struct {
	Incarnation string         `json:"incarnation"`
	Revision    model.Revision `json:"revision"`
	Index       model.Natural  `json:"index"`
}

func (p Position) Validate() error {
	if p.Incarnation == "" || p.Revision.Validate() != nil || p.Index.Validate() != nil || (p.Revision.IsZero() && !p.Index.IsZero()) {
		return ErrCursorInvalid
	}
	return nil
}

type Head struct {
	Incarnation string
	Revision    model.Revision
	Floor       model.Revision
}

// BoundaryError carries no historical record values. A transport can map it to
// a pull error or an obligatory event.gap on a subscription.
type BoundaryError struct {
	Code   string
	From   string
	Head   Head
	Reason string
}

func (e *BoundaryError) Error() string { return e.Code }

// Log.Scan returns complete committed changesets in ascending revision order,
// INCLUDING after.Revision. Implementations reread R via a driver cursor R-1;
// the delivery reader alone discards acknowledged indexes in R. Revision zero
// means the beginning. No log implementation accepts arbitrary event payloads.
type Log interface {
	Head(context.Context) (Head, error)
	Scan(context.Context, Position, int) ([]model.Changeset, error)
}

// Materializer reconstructs images from version history and resolves the
// canonical operation/slot type and provenance. The index is a logical Events
// position for a nonnil plan, and a ChangeRecord position only for legacy nil.
// The reader validates the complete plan before calling this port.
type Materializer interface {
	Materialize(context.Context, model.Changeset, int) (model.DurableEvent, error)
}

type Visibility struct {
	Readable          bool
	PreviouslyVisible bool
	Redacted          bool
	Before            map[string]any
	After             map[string]any
}

// Authorization evaluates CURRENT read permissions, including for old events,
// and returns only images permitted by the active policy. Errors fail closed.
// PreviouslyVisible is supplied by the caller's watch visibility tracker.
type Authorization interface {
	Read(context.Context, model.PrincipalRef, model.DurableEvent) (Visibility, error)
}

// ResourceAuthorization is required for condition notices. Each required
// resource must exist and be currently readable without redacting audit facts.
// Denial/redaction hides the entire notice, including any visibility marker.
// This capability is optional for legacy/ordinary events; absence refuses a
// notice explicitly instead of treating primary-resource access as sufficient.
type ResourceAuthorization interface {
	ReadResource(context.Context, model.PrincipalRef, model.AuditResource) (ResourceVisibility, error)
}

type ResourceVisibility struct {
	Readable bool
	Redacted bool
}

// AllowAll is selected explicitly when no authorization chain is installed.
type AllowAll struct{}

func (AllowAll) Read(_ context.Context, _ model.PrincipalRef, e model.DurableEvent) (Visibility, error) {
	return Visibility{Readable: true, Before: e.Before, After: e.After}, nil
}

func (AllowAll) ReadResource(context.Context, model.PrincipalRef, model.AuditResource) (ResourceVisibility, error) {
	return ResourceVisibility{Readable: true}, nil
}

type Filter struct {
	Types     []string `json:"types,omitempty"`
	Resources []string `json:"resources,omitempty"`
	// BeadTypes is an internal view-membership predicate. Types remains the
	// event-type filter used by event.subscribe; keeping the vocabularies
	// separate prevents a view's "issue" type from being mistaken for an
	// event named "issue".
	BeadTypes []string `json:"beadTypes,omitempty"`
}

// ViewBinding is the checkpoint compiled by the real view service for a
// view.watch subscription. The event/watch layer consumes only this bounded
// membership filter and its cursor generations; it never reconstructs a view
// definition or invents an authorization decision.
type ViewBinding struct {
	Filter        Filter
	Ontology      model.Revision
	PolicyVersion string
	Incarnation   string
	Projection    *ProjectionStatus
}

// ViewResolver is the narrow seam between the view service and the watch
// manager. A resolver must authorize the saved view and return an exact
// event-reader membership filter or an explicit refusal.
type ViewResolver interface {
	WatchBinding(context.Context, model.PrincipalRef, string) (ViewBinding, error)
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func (f Filter) accepts(e model.Event) bool {
	if e.Control != nil {
		return e.Control.Type == model.EventGap || len(f.Types) == 0 || contains(f.Types, string(e.Control.Type))
	}
	if len(f.Types) != 0 && !contains(f.Types, string(e.Durable.Type)) {
		return false
	}
	if len(f.Resources) != 0 && !contains(f.Resources, e.Durable.Resource) {
		return false
	}
	if len(f.BeadTypes) != 0 {
		image := e.Durable.After
		if image == nil {
			image = e.Durable.Before
		}
		typ, _ := image["type"].(string)
		if !contains(f.BeadTypes, typ) {
			return false
		}
	}
	return true
}

type Class string

const (
	Post       Class = "post"
	Listener   Class = "listener"
	Projection Class = "projection"
)

type DeliveryKey struct {
	Position
	Binding string `json:"binding"`
}

// String uses length-prefixed JSON strings, so binding/resource delimiters
// cannot collide. Revision/index spellings are canonical across retries.
func (k DeliveryKey) String() string {
	return fmt.Sprintf("%q/%s/%s/%q", k.Incarnation, k.Revision.String(), k.Index.String(), k.Binding)
}

type Failure struct {
	Key        DeliveryKey `json:"key"`
	Attempts   uint64      `json:"attempts"`
	FirstAt    time.Time   `json:"firstAt"`
	DeadLetter bool        `json:"deadLetter"`
	Reason     string      `json:"reason"`
}

type SkipAudit struct {
	Key       DeliveryKey        `json:"key"`
	Principal model.PrincipalRef `json:"principal"`
	Reason    string             `json:"reason"`
	At        time.Time          `json:"at"`
}

// State retains delivery metadata only, never before/after images. Failure and
// skip audit records live in the same lease-fenced CAS as the checkpoint.
type State struct {
	Position           Position       `json:"position"`
	Class              Class          `json:"class"`
	Failure            *Failure       `json:"failure,omitempty"`
	Halted             bool           `json:"halted,omitempty"`
	Stalled            bool           `json:"stalled,omitempty"`
	Skips              []SkipAudit    `json:"skips,omitempty"`
	LagAcceptedThrough model.Revision `json:"lagAcceptedThrough,omitempty"`
}

// ProjectionResume is the operator-facing continuation point for a failed
// or stalled projection. It is deliberately a tuple rather than a signed
// event cursor: projection repair/replay is owned by the dispatcher, while
// views and watches only report the durable checkpoint they observed.
type ProjectionResume struct {
	Position           Position       `json:"position"`
	LagAcceptedThrough model.Revision `json:"lagAcceptedThrough,omitempty"`
}

// ProjectionStatus is the read-only projection metadata consumed by views and
// watches. BUILD-15's durable checkpoint adapter implements the reader below;
// this package does not own registration, replay, stall transitions, or
// resume writes.
type ProjectionStatus struct {
	Binding            string            `json:"binding"`
	Position           Position          `json:"checkpoint"`
	Failure            *Failure          `json:"failure,omitempty"`
	Halted             bool              `json:"halted,omitempty"`
	Stalled            bool              `json:"stalled,omitempty"`
	LagAcceptedThrough model.Revision    `json:"lagAcceptedThrough,omitempty"`
	Resume             *ProjectionResume `json:"resume,omitempty"`
}

// ProjectionStatusReader is a narrow, durable status port. Implementations
// must read persisted checkpoint state, never a process-local dispatcher map.
type ProjectionStatusReader interface {
	Status(context.Context, string) (ProjectionStatus, error)
}

// ProjectionStatusSource is an expressive alias for composition roots that
// expose the same checkpoint-backed port under a source-oriented name.
type ProjectionStatusSource = ProjectionStatusReader

// NormalizeProjectionStatus copies a status and makes the repair checkpoint
// explicit whenever a projection is halted, stalled, or has a recorded
// failure. An empty checkpoint is valid for a projection that has not run.
func NormalizeProjectionStatus(status ProjectionStatus, binding string) ProjectionStatus {
	if status.Binding == "" {
		status.Binding = binding
	}
	if status.Binding == "" {
		status.Binding = "core"
	}
	status.Failure = cloneFailure(status.Failure)
	if status.Resume == nil && (status.Halted || status.Stalled || status.Failure != nil) {
		status.Resume = &ProjectionResume{Position: status.Position, LagAcceptedThrough: status.LagAcceptedThrough}
	} else if status.Resume != nil {
		resume := *status.Resume
		status.Resume = &resume
	}
	return status
}

func cloneFailure(failure *Failure) *Failure {
	if failure == nil {
		return nil
	}
	copy := *failure
	return &copy
}

// Checkpoints must be backed by driver metadata. Missing capability is an
// error, never a request to substitute a map in the dispatcher.
type Checkpoints interface {
	Load(context.Context, string) (State, uint64, error)
	Save(context.Context, string, uint64, State) (uint64, error)
}

// Active closes when the active storage lease is lost. Save must additionally
// check that epoch atomically in storage: this signal alone is not fencing.
type Active interface{ Lost() <-chan struct{} }

type Delivery struct {
	Key       DeliveryKey
	Event     model.Event
	Partition string
	Late      bool
}
type ChangeDelivery struct {
	Incarnation string
	Revision    model.Revision
	Events      []Delivery
	Late        bool
}
type Handler interface {
	Deliver(context.Context, Delivery) error
}
type Projector interface {
	Apply(context.Context, ChangeDelivery) error
}

type Notice struct {
	Binding string
	Class   Class
	Reason  string
	Failure *Failure
}

// Observer connects alerts and per-binding breaker accounting to the host.
type Observer interface{ Notify(context.Context, Notice) }

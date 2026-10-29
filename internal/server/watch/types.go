// Package watch owns the bounded, ephemeral watch registry. It consumes the
// actual event reader through server/events and never writes graph state.
//
// A watch is deliberately transport neutral. The manager emits Frame values;
// watchhttp is the explicit byte/HTTP adapter. This keeps cancellation,
// authorization and cursor ownership in one place while allowing a future
// WebSocket or in-process adapter to use the same delivery contract.
package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
)

const (
	// CoalesceWindow is the canonical watch coalescing window from A7.
	CoalesceWindow = 250 * time.Millisecond
	// MaxWatchesPerSession is a hard admission bound. The manager never claims
	// the separate 500-watch performance target.
	MaxWatchesPerSession = 32
	// DefaultQueueSize bounds undelivered frames per watch.
	DefaultQueueSize = 64
	// DefaultMaxLag bounds time spent retaining a frame before it is emitted.
	DefaultMaxLag = time.Second
	// DefaultPageLimit is the event reader's largest permitted page.
	DefaultPageLimit = 1000
)

var (
	ErrClosed                 = errors.New("watch manager is closed")
	ErrSubscriptionClosed     = errors.New("watch subscription is closed")
	ErrTooManyWatches         = errors.New("watch session has reached its 32-watch limit")
	ErrDuplicateSubscription  = errors.New("watch subscription already exists")
	ErrUnsupportedMethod      = errors.New("watch method is unsupported")
	ErrViewCheckpointRequired = errors.New("view.watch requires an actual view checkpoint")
	ErrInvalidRequest         = errors.New("invalid watch request")
	ErrBindingChanged         = errors.New("watch binding changed outside a reset boundary")
)

// Method names are the admitted watch slice. event.subscribe is the generic
// stream; bead.watch narrows it to one canonical bead resource; view.watch
// obtains its filter from ViewResolver rather than emulating a view locally.
type Method string

const (
	MethodEventSubscribe Method = "event.subscribe"
	MethodBeadWatch      Method = "bead.watch"
	MethodViewWatch      Method = "view.watch"
)

func (m Method) valid() bool {
	return m == MethodEventSubscribe || m == MethodBeadWatch || m == MethodViewWatch
}

// Request is the trusted, domain-level input to Manager.Open. Principal is
// stamped by the authentication boundary; it is never read from transport
// metadata. Filter and Resource use the same canonical event vocabulary as
// core/events.
type Request struct {
	SessionID      string
	SubscriptionID string
	Method         Method
	Principal      model.PrincipalRef
	Filter         coreevents.Filter
	Resource       string
	ViewID         string
	From           string
	Ontology       model.Revision
	PolicyVersion  string
}

// Binding is the current authorization/cursor binding for a watch. A source
// may change ontology, policy or incarnation; the manager then emits a reset
// before reading later events. Principal and filter are immutable request
// identity and must match the opening request.
type Binding struct {
	Principal     model.PrincipalRef
	Filter        coreevents.Filter
	Ontology      model.Revision
	PolicyVersion string
	Incarnation   string
}

// BindingSource supplies authoritative current binding state. A nil source
// uses the opening request for principal/filter/ontology/policy and reads the
// incarnation from the actual event log head. Production compositions with a
// mutable policy should provide this interface so grants are not reused after
// revocation.
type BindingSource interface {
	Current(context.Context, Request) (Binding, error)
}

// StaticBinding is useful for an installation whose ontology and policy
// generations are immutable for the manager lifetime. It still validates the
// actual event-log incarnation on each call.
type StaticBinding struct {
	Binding Binding
}

func (s StaticBinding) Current(_ context.Context, _ Request) (Binding, error) {
	return cloneBinding(s.Binding), nil
}

// ViewBinding is the exact checkpoint returned by the view service. The watch
// manager consumes it but never interprets a view definition or reconstructs
// membership. A V3-VIEWS-RPC composition can implement ViewResolver without
// changing this package.
type ViewBinding struct {
	Filter        coreevents.Filter
	Ontology      model.Revision
	PolicyVersion string
	Incarnation   string
}

// ViewResolver is intentionally narrow: view.watch is admitted only when the
// real view service supplies a checkpoint and compiled filter. Returning a
// hand-built filter from the watch manager is unsupported.
type ViewResolver interface {
	WatchBinding(context.Context, model.PrincipalRef, string) (ViewBinding, error)
}

// Config wires the actual event service and optional current/view boundaries.
// Events.Reader.Authorization must be a real authorization implementation; a
// nil reader or nil authorization is rejected rather than replaced with an
// allow-all fixture.
type Config struct {
	Events      *serverevents.Service
	Bindings    BindingSource
	Views       ViewResolver
	QueueSize   int
	MaxLag      time.Duration
	Coalesce    time.Duration
	PageLimit   int
	Clock       func() time.Time
	IDGenerator func() (string, error)
}

// FrameKind identifies the explicit snapshot/delta/reset transport phase.
// Snapshot frames carry the initial/catch-up event union. Delta and reset
// frames are encoded as canonical watch.delta/watch.reset control events by
// Frame.WireEvents.
type FrameKind string

const (
	FrameSnapshot FrameKind = "snapshot"
	FrameDelta    FrameKind = "delta"
	FrameReset    FrameKind = "reset"
)

// Frame is the manager's transport-neutral delivery unit. Events contains
// payload events for snapshots/deltas; delta/reset wire controls are generated
// from the phase and cursor fields, so callers cannot fabricate a control
// envelope by passing an arbitrary model.Event.
type Frame struct {
	SubscriptionID string
	Kind           FrameKind
	FromCursor     string
	Cursor         string
	Events         []model.Event
	Reason         string

	// position is the decoded cursor tuple used to form the canonical control
	// envelope. It is intentionally private; callers use Cursor as the opaque
	// acknowledgement token.
	position coreevents.Position
	created  time.Time
}

// WireEvents converts a frame into the closed model.Event union. Snapshot
// payloads pass through after validation; delta/reset phases always use the
// contract's typed control details.
func (f Frame) WireEvents() []model.Event {
	events := cloneEvents(f.Events)
	position := model.EventPosition{Cursor: f.Cursor, Incarnation: f.position.Incarnation, Revision: f.position.Revision, Index: f.position.Index}
	switch f.Kind {
	case FrameSnapshot:
		return events
	case FrameDelta:
		control := model.ControlEvent{
			EventPosition: position,
			Type:          model.EventWatchDelta,
			Details:       model.WatchDeltaDetails{SubscriptionID: f.SubscriptionID, FromCursor: f.FromCursor, ToCursor: f.Cursor, Events: events},
		}
		out := []model.Event{{Category: model.EventControl, Control: &control}}
		if f.Reason != "" {
			out = append(out, resetEvent(f.SubscriptionID, f.Reason, f.Cursor, f.position))
		}
		return out
	case FrameReset:
		return []model.Event{resetEvent(f.SubscriptionID, f.Reason, f.Cursor, f.position)}
	default:
		return nil
	}
}

// MarshalJSON is the canonical event.notify envelope used by a streaming
// transport. The event members themselves use model's tagged-union codec;
// watch.delta and watch.reset therefore remain ordinary control events in the
// same byte stream as the initial snapshot.
func (f Frame) MarshalJSON() ([]byte, error) {
	if err := f.validate(); err != nil {
		return nil, err
	}
	type wire struct {
		SubscriptionID string        `json:"subscriptionId"`
		Events         []model.Event `json:"events"`
		Cursor         string        `json:"cursor"`
	}
	return json.Marshal(wire{SubscriptionID: f.SubscriptionID, Events: f.WireEvents(), Cursor: f.Cursor})
}

func (f Frame) validate() error {
	if f.SubscriptionID == "" || f.Cursor == "" || f.position.Validate() != nil {
		return ErrInvalidRequest
	}
	if f.Kind != FrameSnapshot && f.Kind != FrameDelta && f.Kind != FrameReset {
		return ErrInvalidRequest
	}
	if f.Kind == FrameDelta && f.FromCursor == "" {
		return ErrInvalidRequest
	}
	for _, event := range f.WireEvents() {
		if _, err := json.Marshal(event); err != nil {
			return fmt.Errorf("invalid watch event: %w", err)
		}
	}
	return nil
}

func resetEvent(subscriptionID, reason, cursor string, position coreevents.Position) model.Event {
	return model.Event{Category: model.EventControl, Control: &model.ControlEvent{
		EventPosition: model.EventPosition{Cursor: cursor, Incarnation: position.Incarnation, Revision: position.Revision, Index: position.Index},
		Type:          model.EventWatchReset,
		Details:       model.WatchResetDetails{SubscriptionID: subscriptionID, Reason: reason, SnapshotCursor: cursor},
	}}
}

func cloneEvents(events []model.Event) []model.Event {
	if events == nil {
		return []model.Event{}
	}
	// Event values contain maps and typed details. JSON is the model's own
	// closed-union boundary and avoids aliases between a queue and a caller.
	raw, err := json.Marshal(events)
	if err != nil {
		return nil
	}
	var copied []model.Event
	if json.Unmarshal(raw, &copied) != nil {
		return nil
	}
	return copied
}

func cloneBinding(binding Binding) Binding {
	binding.Filter.Types = append([]string(nil), binding.Filter.Types...)
	binding.Filter.Resources = append([]string(nil), binding.Filter.Resources...)
	if binding.Principal.Sponsor != nil || binding.Principal.Caveats != nil {
		raw, err := json.Marshal(binding.Principal)
		if err == nil {
			var principal model.PrincipalRef
			if json.Unmarshal(raw, &principal) == nil {
				binding.Principal = principal
			}
		}
	}
	return binding
}

func sameBindingIdentity(a, b Binding) bool {
	return reflect.DeepEqual(a.Principal, b.Principal) && reflect.DeepEqual(a.Filter, b.Filter)
}

// Subscription is a bounded consumer handle. Frames are received through
// Next, which revalidates queued durable events against current authorization
// before exposing their bytes. Close is idempotent and releases the manager's
// watch slot, reader copies and delivery loop.
type Subscription struct {
	watch *watch
}

func (s *Subscription) ID() string {
	if s == nil || s.watch == nil {
		return ""
	}
	return s.watch.id
}

func (s *Subscription) Cursor() string {
	if s == nil || s.watch == nil {
		return ""
	}
	s.watch.mu.Lock()
	defer s.watch.mu.Unlock()
	return s.watch.cursor
}

func (s *Subscription) Next(ctx context.Context) (Frame, error) {
	if s == nil || s.watch == nil {
		return Frame{}, ErrSubscriptionClosed
	}
	return s.watch.next(ctx)
}

func (s *Subscription) Close() error {
	if s == nil || s.watch == nil {
		return nil
	}
	return s.watch.manager.cancel(s.watch.id, ErrSubscriptionClosed)
}

// isNilInterface is used at construction boundaries where a typed nil pointer
// may be stored in an interface.
func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return v.IsNil()
	default:
		return false
	}
}

// Keep the imports/contracts visible to package documentation and prevent an
// accidental transport implementation from bypassing server/events.
var _ = serverevents.Request{}

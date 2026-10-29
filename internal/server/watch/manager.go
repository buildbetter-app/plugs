package watch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
)

// Manager owns only ephemeral watch state. It never stores records, cursors or
// authorization decisions beyond the live queue and visibility tracker.
type Manager struct {
	mu           sync.Mutex
	config       Config
	ctx          context.Context
	stopLifetime context.CancelFunc
	done         chan struct{}
	closed       bool
	watches      map[string]*watch
	session      map[string]map[string]*watch
}

type watch struct {
	manager *Manager
	id      string
	session string
	request Request

	mu            sync.Mutex
	cursor        string
	openingCursor string
	emittedCursor string
	position      coreevents.Position
	binding       Binding
	closed        bool
	closeErr      error
	halted        bool
	replacement   *Frame
	consumer      *Subscription
	nextMu        sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	queue  chan Frame
	// queueMu serializes the only producer (the polling loop, plus the
	// initial snapshot before it starts) with close/drain. A cancellation may
	// arrive while a poll is returning, so closing a channel without this
	// boundary would race a send and panic.
	queueMu sync.Mutex

	// auth is a per-watch copy of the actual Reader authorization. It tracks
	// currently visible resources so a queued frame can become visibility.lost
	// without exposing a stale before/after image.
	auth *visibilityAuthorization
}

// New validates the actual event reader and starts the manager lifetime. The
// caller owns the storage/lease; Manager only observes its context.
func New(lifetime context.Context, config Config) (*Manager, error) {
	if lifetime == nil || isNilInterface(config.Events) || config.Events.Reader == nil || config.Events.Cursors == nil {
		return nil, fmt.Errorf("watch requires a real event service")
	}
	if isNilInterface(config.Events.Reader.Log) || isNilInterface(config.Events.Reader.Materializer) || isNilInterface(config.Events.Reader.Authorization) {
		return nil, fmt.Errorf("watch requires the actual event log, materializer and authorization")
	}
	if config.QueueSize == 0 {
		config.QueueSize = DefaultQueueSize
	}
	if config.QueueSize < 1 || config.QueueSize > 1024 {
		return nil, fmt.Errorf("watch queue size must be between 1 and 1024")
	}
	if config.MaxLag == 0 {
		config.MaxLag = DefaultMaxLag
	}
	if config.MaxLag < 0 || config.MaxLag > 5*time.Minute {
		return nil, fmt.Errorf("watch max lag must be between zero and five minutes")
	}
	if config.Coalesce == 0 {
		config.Coalesce = CoalesceWindow
	}
	if config.Coalesce < time.Millisecond || config.Coalesce > CoalesceWindow {
		return nil, fmt.Errorf("watch coalesce window must be between one millisecond and 250 milliseconds")
	}
	if config.PageLimit == 0 {
		config.PageLimit = DefaultPageLimit
	}
	if config.PageLimit < 1 || config.PageLimit > DefaultPageLimit {
		return nil, fmt.Errorf("watch page limit must be between 1 and 1000")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	ctx, cancel := context.WithCancel(lifetime)
	m := &Manager{config: config, ctx: ctx, stopLifetime: cancel, done: make(chan struct{}), watches: map[string]*watch{}, session: map[string]map[string]*watch{}}
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		m.closed = true
		watches := make([]*watch, 0, len(m.watches))
		for _, current := range m.watches {
			watches = append(watches, current)
			current.cancel()
		}
		m.mu.Unlock()
		for _, current := range watches {
			<-current.done
		}
		close(m.done)
	}()
	return m, nil
}

// Open takes the trusted connection/session LIFETIME, not an HTTP RPC request
// context. The initial frame is queued before
// the worker starts, so a write racing the snapshot is observed after the
// exact returned cursor rather than being lost.
func (m *Manager) Open(ctx context.Context, request Request) (*Subscription, error) {
	if ctx == nil || request.SessionID == "" {
		return nil, ErrInvalidRequest
	}
	if !request.Method.valid() {
		return nil, ErrUnsupportedMethod
	}
	if request.Principal.Kind == "" || request.Principal.ID == "" {
		return nil, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Method == MethodViewWatch || request.ViewID != "" {
		return nil, ErrViewCheckpointRequired
	}
	// Own all request maps/slices before the caller can reuse its arguments.
	frozen := cloneBinding(Binding{Principal: request.Principal, Filter: request.Filter})
	request.Principal, request.Filter = frozen.Principal, frozen.Filter
	if request.Method == MethodBeadWatch {
		if request.Resource == "" {
			return nil, ErrInvalidRequest
		}
		if !strings.HasPrefix(request.Resource, "bead/") || len(request.Resource) <= len("bead/") {
			return nil, ErrInvalidRequest
		}
	}

	m.mu.Lock()
	if m.closed || m.ctx.Err() != nil {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if request.SubscriptionID == "" {
		var err error
		request.SubscriptionID, err = m.newID()
		if err != nil {
			m.mu.Unlock()
			return nil, err
		}
	}
	if _, exists := m.watches[request.SubscriptionID]; exists {
		m.mu.Unlock()
		return nil, ErrDuplicateSubscription
	}
	sessions := m.session[request.SessionID]
	if len(sessions) >= MaxWatchesPerSession {
		m.mu.Unlock()
		return nil, ErrTooManyWatches
	}
	if sessions == nil {
		sessions = map[string]*watch{}
		m.session[request.SessionID] = sessions
	}
	watchCtx, cancel := context.WithCancel(m.ctx)
	watchCtx, stopCaller := context.WithCancel(watchCtx)
	// stopCaller is linked to the caller's context without retaining a
	// goroutine per watch after cancellation.
	stop := context.AfterFunc(ctx, stopCaller)
	current := &watch{manager: m, id: request.SubscriptionID, session: request.SessionID, request: request, ctx: watchCtx, cancel: func() { stop(); cancel(); stopCaller() }, done: make(chan struct{}), queue: make(chan Frame, m.config.QueueSize), auth: &visibilityAuthorization{base: m.config.Events.Reader.Authorization, visible: map[string]bool{}}}
	m.watches[current.id] = current
	sessions[current.id] = current
	m.mu.Unlock()

	if err := current.open(); err != nil {
		m.mu.Lock()
		delete(m.watches, current.id)
		if sessions := m.session[current.session]; sessions != nil {
			delete(sessions, current.id)
			if len(sessions) == 0 {
				delete(m.session, current.session)
			}
		}
		m.mu.Unlock()
		current.mu.Lock()
		current.closeErr = err
		current.mu.Unlock()
		current.cancel()
		current.closeQueue()
		close(current.done)
		return nil, err
	}
	go current.run()
	return &Subscription{watch: current}, nil
}

func (m *Manager) newID() (string, error) {
	if m.config.IDGenerator != nil {
		id, err := m.config.IDGenerator()
		if err != nil || id == "" {
			return "", fmt.Errorf("watch subscription ID generation failed: %w", err)
		}
		return id, nil
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("watch subscription ID generation failed: %w", err)
	}
	return "sub-" + hex.EncodeToString(random[:]), nil
}

// Cancel releases a watch by its subscription ID. It is safe to call more
// than once and does not wait while holding the manager lock.
func (m *Manager) Cancel(ctx context.Context, id string) error {
	if ctx == nil {
		return ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.cancel(id, ErrSubscriptionClosed)
}

func (m *Manager) cancel(id string, reason error) error {
	m.mu.Lock()
	current := m.watches[id]
	m.mu.Unlock()
	return m.cancelWatch(current, reason)
}

func (m *Manager) cancelWatch(current *watch, reason error) error {
	if current == nil {
		return nil
	}
	current.mu.Lock()
	if current.closeErr == nil {
		current.closeErr = reason
	}
	current.mu.Unlock()
	current.cancel()
	<-current.done
	return nil
}

// CancelOwned checks the authenticated owner without claiming stream delivery.
func (m *Manager) CancelOwned(ctx context.Context, id string, principal model.PrincipalRef) error {
	if ctx == nil {
		return ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	current := m.watches[id]
	if current == nil || !samePrincipal(current.request.Principal, principal) {
		m.mu.Unlock()
		return ErrSubscriptionClosed
	}
	m.mu.Unlock()
	return m.cancelWatch(current, ErrSubscriptionClosed)
}

// Close cancels every watch and waits for every delivery loop to release its
// queue and reader copy. It never closes the caller's event service or store.
func (m *Manager) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.stopLifetime()
	<-m.done
	return nil
}

// CloseSession releases all watches owned by a collab session. The manager
// does not close the collab session itself. Passing its real SessionContext
// to Open also releases all watches when that session ends.
func (m *Manager) CloseSession(ctx context.Context, sessionID string) error {
	if ctx == nil || sessionID == "" {
		return ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	sessions := m.session[sessionID]
	watches := make([]*watch, 0, len(sessions))
	for _, current := range sessions {
		watches = append(watches, current)
	}
	m.mu.Unlock()
	for _, current := range watches {
		_ = m.cancelWatch(current, ErrSubscriptionClosed)
	}
	return nil
}

func (m *Manager) WatchCount(sessionID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.session[sessionID])
}

// Attach returns a live subscription for a transport stream after checking the
// original authenticated principal. It is the handoff used by watchhttp after
// event.subscribe has returned its RPC result; a different principal cannot
// siphon another caller's queued bytes.
func (m *Manager) Attach(ctx context.Context, id string, principal model.PrincipalRef) (*Subscription, error) {
	if ctx == nil {
		return nil, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	current := m.watches[id]
	if current == nil {
		m.mu.Unlock()
		return nil, ErrSubscriptionClosed
	}
	if !samePrincipal(current.request.Principal, principal) {
		m.mu.Unlock()
		return nil, ErrSubscriptionClosed
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	defer m.mu.Unlock()
	if current.ctx.Err() != nil || current.closed {
		return nil, ErrSubscriptionClosed
	}
	if current.consumer != nil {
		return nil, ErrAlreadyAttached
	}
	subscription := &Subscription{watch: current}
	current.consumer = subscription
	return subscription, nil
}

func (current *watch) open() error {
	binding, err := current.manager.binding(current.ctx, current.request)
	if err != nil {
		return err
	}
	page, position, err := current.poll(current.ctx, current.request.From, binding)
	if err != nil {
		return err
	}
	current.mu.Lock()
	current.binding, current.cursor, current.position = binding, page.Cursor, position
	current.openingCursor = page.Cursor
	current.mu.Unlock()
	kind := FrameSnapshot
	if page.Reset {
		kind = FrameReset
	}
	frame := current.frame(kind, "", page, position, binding)
	return current.enqueue(frame)
}

func (current *watch) run() {
	defer func() {
		current.closeQueue()
		current.manager.detach(current)
		close(current.done)
	}()
	ticker := time.NewTicker(current.manager.config.Coalesce)
	defer ticker.Stop()
	for {
		select {
		case <-current.ctx.Done():
			return
		case <-ticker.C:
			if err := current.step(); err != nil {
				current.mu.Lock()
				if current.closeErr == nil {
					current.closeErr = err
				}
				current.mu.Unlock()
				current.cancel()
			}
		}
	}
}

func (current *watch) step() error {
	current.mu.Lock()
	if current.halted {
		current.mu.Unlock()
		return nil
	}
	oldCursor, oldBinding := current.cursor, cloneBinding(current.binding)
	current.mu.Unlock()
	binding, err := current.manager.binding(current.ctx, current.request)
	if err != nil {
		return err
	}
	if reason := bindingChange(oldBinding, binding); reason != "" {
		if reason == "incarnation_changed" {
			// Let server/events turn the stale old-incarnation cursor into its
			// mandatory event.gap. Do not manufacture a reset at the old tuple.
			page, position, err := current.poll(current.ctx, oldCursor, oldBinding)
			if err != nil {
				return err
			}
			return current.enqueue(current.frame(FrameReset, current.emittedCursor, page, position, oldBinding))
		}
		return current.replaceWithReset(reason, binding)
	}
	page, position, err := current.poll(current.ctx, oldCursor, oldBinding)
	if err != nil {
		return err
	}
	if len(page.Events) == 0 && !page.Reset {
		current.mu.Lock()
		if !current.halted {
			current.cursor, current.position = page.Cursor, position
		}
		current.mu.Unlock()
		return nil
	}
	current.mu.Lock()
	from := current.emittedCursor
	current.mu.Unlock()
	kind := FrameDelta
	if page.Reset {
		kind = FrameReset
	}
	return current.enqueue(current.frame(kind, from, page, position, oldBinding))
}

func bindingChange(a, b Binding) string {
	switch {
	case !samePrincipal(a.Principal, b.Principal) || !sameFilter(a.Filter, b.Filter):
		return "visibility_changed"
	case a.Incarnation != b.Incarnation:
		return "incarnation_changed"
	case a.Ontology.String() != b.Ontology.String():
		return "ontology_changed"
	case a.PolicyVersion != b.PolicyVersion:
		return "policy_changed"
	default:
		return ""
	}
}

// The event service owns actual log/materializer/authorization semantics. It
// uses event.subscribe internally; translate signed tokens at this boundary so
// a bead.watch token cannot be replayed as event.subscribe (or vice versa).
func (current *watch) poll(ctx context.Context, after string, binding Binding) (serverevents.Page, coreevents.Position, error) {
	service := *current.manager.config.Events
	reader := *service.Reader
	reader.Authorization = current.auth
	service.Reader = &reader
	filter := cloneFilter(binding.Filter)
	if len(filter.Types) > 0 {
		filter.Types = append(filter.Types, "watch.reset", "visibility.lost")
	}
	inner, err := serverevents.BindingFor("event.subscribe", filter, binding.Principal, binding.Ontology, binding.PolicyVersion)
	if err != nil {
		return serverevents.Page{}, coreevents.Position{}, err
	}
	outer, err := current.cursorBinding(binding)
	if err != nil {
		return serverevents.Page{}, coreevents.Position{}, err
	}
	if after != "" {
		position, err := service.Cursors.Decode(after, outer)
		if err != nil {
			return serverevents.Page{}, coreevents.Position{}, err
		}
		after, err = service.Cursors.Encode(position, inner)
		if err != nil {
			return serverevents.Page{}, coreevents.Position{}, err
		}
	}
	page, err := service.Poll(ctx, serverevents.Request{Principal: binding.Principal, Filter: filter, After: after, Limit: current.manager.config.PageLimit, Ontology: binding.Ontology, PolicyVersion: binding.PolicyVersion, SubscriptionID: current.id})
	if err != nil {
		return page, coreevents.Position{}, err
	}
	position, err := service.Cursors.Decode(page.Cursor, inner)
	if err != nil {
		return page, position, err
	}
	page.Cursor, err = service.Cursors.Encode(position, outer)
	if err != nil {
		return page, position, err
	}
	for i := range page.Events {
		event := &page.Events[i]
		token, err := service.Cursors.Encode(coreevents.PositionOf(*event), outer)
		if err != nil {
			return page, position, err
		}
		if event.Durable != nil {
			event.Durable.Cursor = token
		} else {
			event.Control.Cursor = token
			if details, ok := event.Control.Details.(model.WatchResetDetails); ok {
				details.SnapshotCursor = token
				event.Control.Details = details
			}
		}
	}
	return page, position, nil
}

func (current *watch) cursorBinding(binding Binding) (serverevents.CursorBinding, error) {
	return serverevents.BindingFor(string(current.request.Method), binding.Filter, binding.Principal, binding.Ontology, binding.PolicyVersion)
}

func (current *watch) frame(kind FrameKind, from string, page serverevents.Page, position coreevents.Position, binding Binding) Frame {
	frame := Frame{SubscriptionID: current.id, Kind: kind, FromCursor: from, Cursor: page.Cursor, position: position, binding: cloneBinding(binding), created: current.manager.config.Clock()}
	if page.Reset {
		frame.Reason = pageBoundaryReason(page)
	}
	for _, event := range page.Events {
		// One terminal reset belongs to the frame, not inside watch.delta.
		if event.Control != nil && event.Control.Type == model.EventWatchReset {
			continue
		}
		frame.Events = append(frame.Events, event)
	}
	return frame
}

func (current *watch) enqueue(frame Frame) error {
	if err := frame.validate(); err != nil {
		return err
	}
	current.queueMu.Lock()
	defer current.queueMu.Unlock()
	current.mu.Lock()
	defer current.mu.Unlock()
	if err := current.ctx.Err(); err != nil {
		return err
	}
	if current.halted {
		return nil
	}
	current.cursor, current.position = frame.Cursor, frame.position
	select {
	case current.queue <- frame:
		current.emittedCursor = frame.Cursor
		current.halted = frame.Terminal()
		return nil
	default:
		return current.replaceLocked("overflow", current.binding)
	}
}

// queueMu and mu are held. Replacements discard every undelivered payload,
// halt polling and retain just one reset until explicit reopen/cancellation.
func (current *watch) replaceLocked(reason string, binding Binding) error {
	codecBinding, err := current.cursorBinding(binding)
	if err != nil {
		return err
	}
	cursor, err := current.manager.config.Events.Cursors.Encode(current.position, codecBinding)
	if err != nil {
		return err
	}
	reset := Frame{SubscriptionID: current.id, Kind: FrameReset, Cursor: cursor, Reason: reason, position: current.position, binding: cloneBinding(binding), created: current.manager.config.Clock()}
	if err := reset.validate(); err != nil {
		return err
	}
	current.halted, current.replacement, current.cursor = true, &reset, cursor
	for len(current.queue) > 0 {
		<-current.queue
	}
	current.queue <- reset
	return nil
}

func (current *watch) replaceWithReset(reason string, binding Binding) error {
	current.queueMu.Lock()
	defer current.queueMu.Unlock()
	current.mu.Lock()
	defer current.mu.Unlock()
	if current.ctx.Err() != nil {
		return current.ctx.Err()
	}
	if current.replacement != nil {
		return nil
	}
	return current.replaceLocked(reason, binding)
}

func (current *watch) next(ctx context.Context, consumer *Subscription) (Frame, error) {
	if ctx == nil {
		return Frame{}, ErrInvalidRequest
	}
	current.nextMu.Lock()
	defer current.nextMu.Unlock()
	current.mu.Lock()
	if current.consumer == nil {
		current.consumer = consumer
	}
	owned := current.consumer == consumer
	current.mu.Unlock()
	if !owned {
		return Frame{}, ErrAlreadyAttached
	}
	if err := ctx.Err(); err != nil {
		return Frame{}, err
	}
	if err := current.ctx.Err(); err != nil {
		return Frame{}, current.closedError()
	}
	var frame Frame
	select {
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case <-current.ctx.Done():
		return Frame{}, current.closedError()
	case value, ok := <-current.queue:
		if !ok {
			return Frame{}, current.closedError()
		}
		frame = value
	}
	// Never drain payloads after cancellation, even if select chose a ready
	// queue at the same time as the lifetime ended.
	if err := ctx.Err(); err != nil {
		return Frame{}, err
	}
	if current.ctx.Err() != nil {
		return Frame{}, current.closedError()
	}
	checkCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(current.ctx, cancel)
	defer func() { stop(); cancel() }()
	if frame.Kind != FrameReset {
		binding, err := current.manager.binding(checkCtx, current.request)
		if err != nil {
			return Frame{}, current.fail(err)
		}
		reason := bindingChange(frame.binding, binding)
		if reason == "" && current.manager.config.Clock().Sub(frame.created) > current.manager.config.MaxLag {
			reason = "lag"
		}
		if reason != "" {
			if err := current.replaceWithReset(reason, binding); err != nil {
				return Frame{}, current.fail(err)
			}
		} else {
			frame, err = current.revalidate(checkCtx, frame)
			if err != nil {
				return Frame{}, current.fail(err)
			}
			latest, err := current.manager.binding(checkCtx, current.request)
			if err != nil {
				return Frame{}, current.fail(err)
			}
			if reason := bindingChange(binding, latest); reason != "" {
				if err := current.replaceWithReset(reason, latest); err != nil {
					return Frame{}, current.fail(err)
				}
			}
		}
	}
	current.mu.Lock()
	if current.replacement != nil {
		frame = *current.replacement
	}
	current.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Frame{}, err
	}
	if current.ctx.Err() != nil {
		return Frame{}, current.closedError()
	}
	if err := frame.validate(); err != nil {
		return Frame{}, current.fail(err)
	}
	// Only this handoff updates visibility. Polling and revalidation never do.
	if !current.recordVisible(frame) {
		if err := current.replaceWithReset("overflow", frame.binding); err != nil {
			return Frame{}, current.fail(err)
		}
		current.mu.Lock()
		frame = *current.replacement
		current.mu.Unlock()
	}
	if frame.Terminal() {
		current.fail(ErrResetRequired)
	}
	return frame, nil
}

func (current *watch) fail(err error) error {
	current.mu.Lock()
	if current.closeErr == nil {
		current.closeErr = err
	}
	current.mu.Unlock()
	current.cancel()
	return err
}

func (current *watch) closedError() error {
	current.mu.Lock()
	defer current.mu.Unlock()
	if current.closeErr != nil {
		return current.closeErr
	}
	return ErrSubscriptionClosed
}

func (current *watch) revalidate(ctx context.Context, frame Frame) (Frame, error) {
	checked := make([]model.Event, 0, len(frame.Events))
	for _, event := range frame.Events {
		if event.Durable == nil {
			if event.Control.Type == model.EventVisibilityLost {
				current.auth.mu.Lock()
				seen := current.auth.visible[event.Control.Resource]
				current.auth.mu.Unlock()
				if !seen {
					continue
				}
				checked = append(checked, event)
				frame.Reason = "visibility_changed"
				break
			}
			checked = append(checked, event)
			continue
		}
		value := *event.Durable
		notice := value.Type == model.EventDependencyConditionUnmet
		if notice {
			allowed, err := current.noticeReadable(ctx, frame.binding.Principal, value)
			if err != nil {
				return Frame{}, err
			}
			if !allowed {
				continue
			}
		}
		// Give callbacks owned maps and principal values, as the Reader does.
		protected, err := copyEvents([]model.Event{event})
		if err != nil {
			return Frame{}, err
		}
		principal := cloneBinding(frame.binding).Principal
		visibility, err := current.auth.Read(ctx, principal, *protected[0].Durable)
		if err != nil {
			return Frame{}, err
		}
		if notice && (visibility.Redacted || !reflect.DeepEqual(visibility.Before, value.Before) || !reflect.DeepEqual(visibility.After, value.After)) {
			continue
		}
		if visibility.Readable {
			value.Before, value.After = visibility.Before, visibility.After
			checked = append(checked, model.Event{Category: model.EventDurable, Durable: &value})
			continue
		}
		// A notice denial cannot reveal even that the notice existed.
		if notice {
			continue
		}
		if visibility.PreviouslyVisible {
			checked = append(checked, model.Event{Category: model.EventControl, Control: &model.ControlEvent{EventPosition: value.EventPosition, Type: model.EventVisibilityLost, Resource: value.Resource, Details: model.VisibilityLostDetails{Reason: "authorization_changed"}}})
		}
		frame.Reason = "visibility_changed"
		break
	}
	var err error
	frame.Events, err = copyEvents(checked)
	if err != nil {
		return Frame{}, err
	}
	if frame.Terminal() {
		// Do not leave an in-flight poll able to enqueue later payloads.
		current.queueMu.Lock()
		current.mu.Lock()
		current.halted = true
		for len(current.queue) > 0 {
			<-current.queue
		}
		current.mu.Unlock()
		current.queueMu.Unlock()
	}
	return frame, nil
}

// Notices protect the dependent, blocker, pinned ontology and optional
// project. They receive the same complete current checks at delivery as read.
func (current *watch) noticeReadable(ctx context.Context, principal model.PrincipalRef, event model.DurableEvent) (bool, error) {
	if event.Details == nil {
		return false, ErrInvalidRequest
	}
	if err := event.Details.Validate(); err != nil {
		return false, err
	}
	d := event.Details
	resources := []model.AuditResource{{Kind: "bead", Ref: d.Dependent.Ref}, {Kind: "bead", Ref: d.BlockerAfter.Ref}, {Kind: "ontology", Ref: d.Ontology.Ref}}
	if d.Ontology.Project != nil {
		resources = append(resources, model.AuditResource{Kind: "project", Ref: *d.Ontology.Project})
	}
	for _, resource := range resources {
		visibility, err := current.auth.ReadResource(ctx, cloneBinding(Binding{Principal: principal}).Principal, resource)
		if err != nil {
			return false, err
		}
		if !visibility.Readable || visibility.Redacted {
			return false, nil
		}
	}
	return true, nil
}

const maxVisibleResources = 4096

func (current *watch) recordVisible(frame Frame) bool {
	current.auth.mu.Lock()
	defer current.auth.mu.Unlock()
	visible := make(map[string]bool, len(current.auth.visible))
	for key := range current.auth.visible {
		visible[key] = true
	}
	for _, event := range frame.Events {
		if event.Durable != nil {
			visible[event.Durable.Resource] = true
		}
		if event.Control != nil && event.Control.Type == model.EventVisibilityLost {
			delete(visible, event.Control.Resource)
		}
	}
	if len(visible) > maxVisibleResources {
		return false
	}
	current.auth.visible = visible
	return true
}

func (current *watch) closeQueue() {
	current.queueMu.Lock()
	defer current.queueMu.Unlock()
	current.mu.Lock()
	if current.closed {
		current.mu.Unlock()
		return
	}
	current.closed = true
	for len(current.queue) > 0 {
		<-current.queue
	}
	close(current.queue)
	current.mu.Unlock()
}

func (m *Manager) detach(current *watch) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing := m.watches[current.id]; existing != current {
		return
	}
	delete(m.watches, current.id)
	if sessions := m.session[current.session]; sessions != nil {
		delete(sessions, current.id)
		if len(sessions) == 0 {
			delete(m.session, current.session)
		}
	}
}

func (m *Manager) binding(ctx context.Context, request Request) (Binding, error) {
	var binding Binding
	var err error
	if request.Method == MethodViewWatch || request.ViewID != "" {
		return Binding{}, ErrViewCheckpointRequired
	} else if m.config.Bindings != nil {
		binding, err = m.config.Bindings.Current(ctx, request)
		if err != nil {
			return Binding{}, err
		}
	} else {
		binding = Binding{Principal: request.Principal, Filter: cloneFilter(request.Filter), Ontology: request.Ontology, PolicyVersion: request.PolicyVersion}
	}
	if binding.Principal.Kind == "" || binding.Principal.ID == "" {
		binding.Principal = request.Principal
	}
	if request.Method == MethodBeadWatch {
		binding.Filter = coreevents.Filter{Resources: []string{request.Resource}}
	}
	if !samePrincipal(binding.Principal, request.Principal) {
		return Binding{}, ErrBindingChanged
	}
	if request.Method != MethodViewWatch && request.Method != MethodBeadWatch && !sameFilter(binding.Filter, request.Filter) {
		return Binding{}, ErrBindingChanged
	}
	if binding.Ontology.Validate() != nil {
		return Binding{}, ErrInvalidRequest
	}
	head, headErr := m.config.Events.Reader.Log.Head(ctx)
	if headErr != nil {
		return Binding{}, headErr
	}
	binding.Incarnation = head.Incarnation
	if binding.Incarnation == "" {
		return Binding{}, ErrInvalidRequest
	}
	binding.Filter = normalizeFilter(binding.Filter)
	return cloneBinding(binding), nil
}

func samePrincipal(a, b model.PrincipalRef) bool {
	return reflect.DeepEqual(a, b)
}

func sameFilter(a, b coreevents.Filter) bool {
	return reflect.DeepEqual(normalizeFilter(a), normalizeFilter(b))
}

func cloneFilter(filter coreevents.Filter) coreevents.Filter {
	filter.Types = append([]string(nil), filter.Types...)
	filter.Resources = append([]string(nil), filter.Resources...)
	return filter
}

func normalizeFilter(filter coreevents.Filter) coreevents.Filter {
	filter = cloneFilter(filter)
	sort.Strings(filter.Types)
	sort.Strings(filter.Resources)
	filter.Types = compactStrings(filter.Types)
	filter.Resources = compactStrings(filter.Resources)
	return filter
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func pageBoundaryReason(page serverevents.Page) string {
	for _, event := range page.Events {
		if event.Control != nil && event.Control.Type == model.EventGap {
			if details, ok := event.Control.Details.(model.EventGapDetails); ok && details.Reason != "" {
				return details.Reason
			}
		}
		if event.Control != nil && event.Control.Type == model.EventWatchReset {
			if details, ok := event.Control.Details.(model.WatchResetDetails); ok {
				return details.Reason
			}
		}
	}
	for _, event := range page.Events {
		if event.Durable == nil {
			continue
		}
		switch event.Durable.Type {
		case model.EventOntologyApplied:
			return "ontology_changed"
		case model.EventPluginApplied:
			return "policy_changed"
		case model.EventViewSaved:
			return "view_changed"
		case model.EventViewDeleted:
			return "view_deleted"
		}
	}
	return "policy_changed"
}

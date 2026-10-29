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

	mu       sync.Mutex
	cursor   string
	position coreevents.Position
	binding  Binding
	closed   bool
	closeErr error

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
	if config.Events.Reader.Log == nil || config.Events.Reader.Materializer == nil || config.Events.Reader.Authorization == nil {
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

// Open admits and starts one bounded watch. The initial frame is queued before
// the worker starts, so a write racing the snapshot is observed after the
// exact returned cursor rather than being lost.
func (m *Manager) Open(ctx context.Context, request Request) (*Subscription, error) {
	if ctx == nil {
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
	if request.Method == MethodViewWatch && request.ViewID == "" {
		return nil, ErrInvalidRequest
	}
	if request.Method == MethodBeadWatch {
		if request.Resource == "" {
			return nil, ErrInvalidRequest
		}
		if !strings.HasPrefix(request.Resource, "bead/") || len(request.Resource) <= len("bead/") {
			return nil, ErrInvalidRequest
		}
	}
	if request.SessionID == "" {
		// One-shot callers without collab session state share a bounded implicit
		// session. A transport that has real collab uses its session ID here.
		request.SessionID = "anonymous-session"
	}

	m.mu.Lock()
	if m.closed {
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
	if current == nil {
		m.mu.Unlock()
		return nil
	}
	delete(m.watches, id)
	if sessions := m.session[current.session]; sessions != nil {
		delete(sessions, id)
		if len(sessions) == 0 {
			delete(m.session, current.session)
		}
	}
	current.mu.Lock()
	current.closeErr = reason
	current.mu.Unlock()
	current.cancel()
	m.mu.Unlock()
	<-current.done
	return nil
}

// Close cancels every watch and waits for every delivery loop to release its
// queue and reader copy. It never closes the caller's event service or store.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		select {
		case <-m.done:
			return nil
		default:
		}
	}
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
	m.stopLifetime()
	select {
	case <-m.done:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("watch manager close timed out")
	}
	return nil
}

// CloseSession releases all watches owned by a collab session. The manager
// does not import collab; the same stable session ID is the seam consumed by
// the merged PR52 manager.
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
		_ = m.cancel(current.id, ErrSubscriptionClosed)
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
	m.mu.Unlock()
	return &Subscription{watch: current}, nil
}

func (current *watch) open() error {
	binding, err := current.manager.binding(current.ctx, current.request)
	if err != nil {
		return err
	}
	current.mu.Lock()
	current.binding = binding
	current.mu.Unlock()
	page, position, err := current.poll(current.ctx, current.request.From, binding)
	if err != nil {
		return err
	}
	frame := current.snapshotFrame(page, position)
	if err := current.enqueue(frame); err != nil {
		return err
	}
	current.mu.Lock()
	current.cursor = page.Cursor
	current.position = position
	current.mu.Unlock()
	return nil
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
	if current.closed {
		current.mu.Unlock()
		return ErrSubscriptionClosed
	}
	oldCursor, oldBinding := current.cursor, cloneBinding(current.binding)
	request := current.request
	current.mu.Unlock()

	binding, err := current.manager.binding(current.ctx, request)
	if err != nil {
		return err
	}
	if !sameBindingIdentity(oldBinding, binding) {
		reason := "visibility_changed"
		if request.Method == MethodViewWatch {
			reason = "view_changed"
		}
		return current.rebind(binding, reason)
	}
	if oldBinding.Ontology.String() != binding.Ontology.String() {
		return current.rebind(binding, "ontology_changed")
	}
	if oldBinding.PolicyVersion != binding.PolicyVersion {
		return current.rebind(binding, "policy_changed")
	}
	if oldBinding.Incarnation != binding.Incarnation {
		// Keep the old binding for one poll so server/events emits its typed
		// event.gap; a stale cursor is never decoded as the new incarnation.
		page, position, pollErr := current.poll(current.ctx, oldCursor, oldBinding)
		if pollErr != nil {
			return pollErr
		}
		if err := current.emitDelta(oldCursor, page, position); err != nil {
			return err
		}
		current.mu.Lock()
		current.binding = binding
		current.cursor = page.Cursor
		current.position = position
		current.mu.Unlock()
		return nil
	}

	page, position, err := current.poll(current.ctx, oldCursor, oldBinding)
	if err != nil {
		return err
	}
	if err := current.emitDelta(oldCursor, page, position); err != nil {
		return err
	}
	current.mu.Lock()
	current.cursor = page.Cursor
	current.position = position
	current.mu.Unlock()
	if page.Reset {
		reason := pageBoundaryReason(page)
		return current.rebind(binding, reason)
	}
	return nil
}

func (current *watch) rebind(binding Binding, reason string) error {
	if reason == "" {
		reason = "visibility_changed"
	}
	page, position, err := current.poll(current.ctx, "", binding)
	if err != nil {
		return err
	}
	reset := Frame{SubscriptionID: current.id, Kind: FrameReset, Cursor: page.Cursor, Reason: reason, position: position, created: current.manager.config.Clock()}
	if err := current.enqueue(reset); err != nil {
		return err
	}
	snapshot := current.snapshotFrame(page, position)
	if err := current.enqueue(snapshot); err != nil {
		return err
	}
	current.mu.Lock()
	current.binding = binding
	current.cursor = page.Cursor
	current.position = position
	current.mu.Unlock()
	return nil
}

func (current *watch) poll(ctx context.Context, after string, binding Binding) (serverevents.Page, coreevents.Position, error) {
	service := *current.manager.config.Events
	reader := *service.Reader
	reader.Authorization = current.auth
	service.Reader = &reader
	request := serverevents.Request{Principal: binding.Principal, Filter: cloneFilter(binding.Filter), After: after, Limit: current.manager.config.PageLimit, Ontology: binding.Ontology, PolicyVersion: binding.PolicyVersion, SubscriptionID: current.id}
	page, err := service.Poll(ctx, request)
	if err != nil {
		return serverevents.Page{}, coreevents.Position{}, err
	}
	position, err := service.Cursors.Decode(page.Cursor, cursorBindingFor(request))
	if err != nil {
		return serverevents.Page{}, coreevents.Position{}, err
	}
	return page, position, nil
}

func cursorBindingFor(request serverevents.Request) serverevents.CursorBinding {
	binding, _ := serverevents.BindingFor("event.subscribe", request.Filter, request.Principal, request.Ontology, request.PolicyVersion)
	return binding
}

func (current *watch) snapshotFrame(page serverevents.Page, position coreevents.Position) Frame {
	return Frame{SubscriptionID: current.id, Kind: FrameSnapshot, Cursor: page.Cursor, Events: cloneEvents(page.Events), position: position, created: current.manager.config.Clock()}
}

func (current *watch) emitDelta(from string, page serverevents.Page, position coreevents.Position) error {
	if len(page.Events) == 0 && !page.Reset {
		return nil
	}
	reason := ""
	for _, event := range page.Events {
		if event.Control != nil && event.Control.Type == model.EventVisibilityLost {
			reason = "visibility_changed"
		}
	}
	frame := Frame{SubscriptionID: current.id, Kind: FrameDelta, FromCursor: from, Cursor: page.Cursor, Events: cloneEvents(page.Events), Reason: reason, position: position, created: current.manager.config.Clock()}
	return current.enqueue(frame)
}

func (current *watch) enqueue(frame Frame) error {
	if frame.created.IsZero() {
		frame.created = current.manager.config.Clock()
	}
	if err := frame.validate(); err != nil {
		return err
	}
	select {
	case <-current.ctx.Done():
		return current.ctx.Err()
	default:
	}
	current.queueMu.Lock()
	defer current.queueMu.Unlock()
	current.mu.Lock()
	closed := current.closed
	current.mu.Unlock()
	if closed {
		return ErrSubscriptionClosed
	}
	select {
	case current.queue <- frame:
		return nil
	default:
		// The incoming frame is the newest safe checkpoint. Advance the
		// manager cursor before replacing the queue so the reset token and
		// decoded position remain one exact pair.
		current.mu.Lock()
		current.cursor = frame.Cursor
		current.position = frame.position
		current.mu.Unlock()
		return current.enqueueResetLocked("overflow", frame.Cursor, frame.position)
	}
}

func (current *watch) enqueueReset(reason string, position coreevents.Position) error {
	current.mu.Lock()
	cursor := current.cursor
	current.mu.Unlock()
	if cursor == "" {
		return ErrSubscriptionClosed
	}
	return current.enqueueResetAt(reason, cursor, position)
}

func (current *watch) enqueueResetAt(reason, cursor string, position coreevents.Position) error {
	current.queueMu.Lock()
	defer current.queueMu.Unlock()
	return current.enqueueResetLocked(reason, cursor, position)
}

func (current *watch) enqueueResetLocked(reason, cursor string, position coreevents.Position) error {
	reset := Frame{SubscriptionID: current.id, Kind: FrameReset, Cursor: cursor, Reason: reason, position: position, created: current.manager.config.Clock()}
	if err := reset.validate(); err != nil {
		return err
	}
	current.mu.Lock()
	closed := current.closed
	current.mu.Unlock()
	if closed {
		return ErrSubscriptionClosed
	}
	// Discard stale queued frames. A reset is the only admissible recovery
	// after overflow/lag; silently dropping a prefix would make the cursor
	// appear acknowledged while leaving the client with an incomplete stream.
	for {
		select {
		case <-current.queue:
			continue
		default:
		}
		break
	}
	select {
	case current.queue <- reset:
		return nil
	default:
		return fmt.Errorf("watch reset could not enter bounded queue")
	}
}

func (current *watch) next(ctx context.Context) (Frame, error) {
	if ctx == nil {
		return Frame{}, ErrInvalidRequest
	}
	select {
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case frame, ok := <-current.queue:
		if !ok {
			current.mu.Lock()
			err := current.closeErr
			current.mu.Unlock()
			if err == nil {
				err = ErrSubscriptionClosed
			}
			return Frame{}, err
		}
		if current.manager.config.MaxLag > 0 && frame.Kind != FrameReset && current.manager.config.Clock().Sub(frame.created) > current.manager.config.MaxLag {
			return current.lagReset()
		}
		return current.revalidate(ctx, frame)
	}
}

func (current *watch) lagReset() (Frame, error) {
	current.queueMu.Lock()
	defer current.queueMu.Unlock()
	current.mu.Lock()
	if current.closed {
		err := current.closeErr
		current.mu.Unlock()
		if err == nil {
			err = ErrSubscriptionClosed
		}
		return Frame{}, err
	}
	cursor, position := current.cursor, current.position
	current.mu.Unlock()
	for {
		select {
		case <-current.queue:
			continue
		default:
		}
		break
	}
	reset := Frame{SubscriptionID: current.id, Kind: FrameReset, Cursor: cursor, Reason: "lag", position: position, created: current.manager.config.Clock()}
	if err := reset.validate(); err != nil {
		return Frame{}, err
	}
	return reset, nil
}

func (current *watch) revalidate(ctx context.Context, frame Frame) (Frame, error) {
	if frame.Kind == FrameReset || len(frame.Events) == 0 {
		return frame, nil
	}
	current.mu.Lock()
	binding := cloneBinding(current.binding)
	current.mu.Unlock()
	checked := make([]model.Event, 0, len(frame.Events))
	revoked := false
	for _, event := range frame.Events {
		if event.Durable == nil {
			checked = append(checked, event)
			continue
		}
		value := *event.Durable
		visibility, err := current.auth.Read(ctx, binding.Principal, value)
		if err != nil {
			return Frame{}, err
		}
		if visibility.Readable {
			value.Before, value.After = visibility.Before, visibility.After
			checked = append(checked, model.Event{Category: model.EventDurable, Durable: &value})
			continue
		}
		if visibility.PreviouslyVisible {
			revoked = true
			checked = append(checked, model.Event{Category: model.EventControl, Control: &model.ControlEvent{EventPosition: value.EventPosition, Type: model.EventVisibilityLost, Resource: value.Resource, Details: model.VisibilityLostDetails{Reason: "authorization_changed"}}})
		}
	}
	frame.Events = checked
	if revoked {
		frame.Reason = "visibility_changed"
	}
	return frame, nil
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
		if m.config.Views == nil {
			return Binding{}, ErrViewCheckpointRequired
		}
		view, resolveErr := m.config.Views.WatchBinding(ctx, request.Principal, request.ViewID)
		if resolveErr != nil {
			return Binding{}, resolveErr
		}
		binding = Binding{Principal: request.Principal, Filter: cloneFilter(view.Filter), Ontology: view.Ontology, PolicyVersion: view.PolicyVersion, Incarnation: view.Incarnation}
		if request.Method == MethodViewWatch {
			request.Filter = binding.Filter
			request.Ontology = binding.Ontology
			request.PolicyVersion = binding.PolicyVersion
		}
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
	if binding.Incarnation == "" {
		head, headErr := m.config.Events.Reader.Log.Head(ctx)
		if headErr != nil {
			return Binding{}, headErr
		}
		binding.Incarnation = head.Incarnation
	}
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
		if event.Control != nil && event.Control.Type == model.EventWatchReset {
			if details, ok := event.Control.Details.(model.WatchResetDetails); ok {
				return details.Reason
			}
		}
	}
	// server/events exposes boundaries through Page.Reset but omits the marker
	// when the filter excludes watch.reset. Re-read the event type from the
	// materialized payload when possible; ontology/policy are the only hidden
	// boundaries in the admitted slice.
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

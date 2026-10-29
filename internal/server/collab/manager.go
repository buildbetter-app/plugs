package collab

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type entry struct {
	session  Session
	caller   Caller
	owner    string
	project  string
	group    string
	focus    string
	working  []string
	expires  time.Time
	ctx      context.Context
	cancel   context.CancelFunc
	sequence uint64
}

type Manager struct {
	mu       sync.Mutex
	config   Config
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	sessions map[string]*entry
	sequence uint64
	closed   bool
}

func nilValue(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return r.IsNil()
	}
	return false
}

func New(lifetime context.Context, config Config) (*Manager, error) {
	if lifetime == nil || nilValue(config.Service) || nilValue(config.Lease) || config.Lease.Lost() == nil || config.AuthorizationInstalled == nil || (config.Policy != nil && nilValue(config.Policy)) {
		return nil, fmt.Errorf("collab requires a real service, active lease, lifetime and authorization installation boundary")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.IdleTTL == 0 {
		config.IdleTTL = SessionTTL
	}
	if config.IdleTTL < time.Millisecond || config.IdleTTL > SessionTTL {
		return nil, fmt.Errorf("collab idle TTL must be between one millisecond and five minutes")
	}
	limits := []*int{&config.Limits.Sessions, &config.Limits.PerPrincipal, &config.Limits.PerSponsor}
	for i, maximum := range []int{MaxSessions, MaxSessionsPerPrincipal, MaxSessionsPerSponsor} {
		if *limits[i] == 0 {
			*limits[i] = maximum
		}
		if *limits[i] < 1 || *limits[i] > maximum {
			return nil, fmt.Errorf("collab session limit outside bounded ceiling")
		}
	}
	ctx, cancel := context.WithCancel(lifetime)
	m := &Manager{config: config, ctx: ctx, cancel: cancel, done: make(chan struct{}), sessions: map[string]*entry{}}
	go m.reap()
	return m, nil
}

func (m *Manager) reap() {
	defer close(m.done)
	ticker := time.NewTicker(min(time.Second, m.config.IdleTTL))
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
		case <-m.config.Lease.Lost():
		case <-ticker.C:
			m.mu.Lock()
			m.pruneLocked()
			m.mu.Unlock()
			continue
		}
		m.mu.Lock()
		m.shutdownLocked()
		m.mu.Unlock()
		return
	}
}

func (m *Manager) shutdownLocked() {
	m.closed = true
	m.cancel()
	for id, e := range m.sessions {
		e.cancel()
		delete(m.sessions, id)
	}
}

func (m *Manager) pruneLocked() {
	now := m.config.Clock()
	for id, e := range m.sessions {
		if !now.Before(e.expires) {
			e.cancel()
			delete(m.sessions, id)
		}
	}
}

func (m *Manager) checkLocked(ctx context.Context) error {
	select {
	case <-m.config.Lease.Lost():
		m.shutdownLocked()
	default:
	}
	if m.ctx.Err() != nil || m.closed {
		m.shutdownLocked()
		return failure("busy", "collaboration server lifetime ended")
	}
	m.pruneLocked()
	if ctx.Err() != nil {
		return failure("busy", "request was canceled")
	}
	return nil
}

func (m *Manager) prepare(ctx context.Context, caller Caller, project string) (context.Context, context.CancelFunc, Caller, error) {
	by, err := clonePrincipal(caller.Principal)
	if err != nil {
		return nil, nil, Caller{}, err
	}
	caller.Principal = by
	if len(caller.Agent) > MaxResourceBytes || len(project) > MaxResourceBytes {
		return nil, nil, Caller{}, failure("invalid_params", "presence scope exceeds its bound")
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.ctx, cancel)
	cleanup := func() { stop(); cancel() }
	m.mu.Lock()
	err = m.checkLocked(ctx)
	m.mu.Unlock()
	if err == nil {
		err = m.capability(caller.Principal)
	}
	if err != nil {
		cleanup()
		return nil, nil, Caller{}, err
	}
	return ctx, cleanup, caller, nil
}

func (m *Manager) admit(ctx context.Context, by model.PrincipalRef, project string) error {
	if err := m.capability(by); err != nil {
		return err
	}
	if m.config.AuthorizationInstalled() {
		if m.config.Policy == nil {
			return failure("authz_unavailable", "current presence authorization is unavailable")
		}
		by, _ = clonePrincipal(by)
		allowed, err := m.config.Policy.AdmitPresence(ctx, by, project)
		if ctx.Err() != nil {
			return failure("busy", "request was canceled")
		}
		if err != nil {
			return failure("authz_unavailable", "current presence authorization failed")
		}
		if !allowed {
			return failure("forbidden", "presence access is denied")
		}
	}
	return nil
}

func (m *Manager) Open(ctx context.Context, caller Caller, project string, onBehalfOf *string) (Session, error) {
	ctx, cancel, caller, err := m.prepare(ctx, caller, project)
	if err != nil {
		return Session{}, err
	}
	defer cancel()
	if err := m.admit(ctx, caller.Principal, project); err != nil {
		return Session{}, err
	}
	name, group, err := delegation(caller.Principal, onBehalfOf)
	if err != nil {
		return Session{}, err
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return Session{}, failure("internal_error", "session ID generation failed")
	}
	id := hex.EncodeToString(random[:])
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return Session{}, err
	}
	if err := m.capability(caller.Principal); err != nil {
		return Session{}, err
	}
	principal, sponsor := 0, 0
	for _, e := range m.sessions {
		if e.caller.Principal.Kind == caller.Principal.Kind && e.caller.Principal.ID == caller.Principal.ID {
			principal++
		}
		if sponsorIdentity(e.caller) == sponsorIdentity(caller) {
			sponsor++
		}
	}
	if len(m.sessions) >= m.config.Limits.Sessions || principal >= m.config.Limits.PerPrincipal || sponsor >= m.config.Limits.PerSponsor {
		return Session{}, failure("too_many_sessions", "live collaboration session quota reached")
	}
	if _, exists := m.sessions[id]; exists {
		return Session{}, failure("internal_error", "session ID collision")
	}
	now := m.config.Clock().UTC()
	s := Session{SessionID: id, Principal: displayPrincipal(caller.Principal), OpenedAt: now, OnBehalfOf: name}
	sessionCtx, sessionCancel := context.WithCancel(m.ctx)
	m.sequence++
	m.sessions[id] = &entry{session: s, caller: caller, owner: ownership(caller), project: project, group: group, expires: now.Add(m.config.IdleTTL), ctx: sessionCtx, cancel: sessionCancel, sequence: m.sequence}
	return s, nil
}

func sponsorIdentity(c Caller) string {
	p := c.Principal
	if p.Sponsor != nil {
		p = *p.Sponsor
	}
	return principalKey(model.PrincipalRef{Kind: p.Kind, ID: p.ID})
}

func (m *Manager) ownedLocked(caller Caller, id string) (*entry, error) {
	e := m.sessions[id]
	if e == nil || e.owner != ownership(caller) {
		return nil, failure("not_found", "live session was not found")
	}
	return e, nil
}

func (m *Manager) CloseSession(ctx context.Context, caller Caller, id string) error {
	ctx, cancel, caller, err := m.prepare(ctx, caller, "")
	if err != nil {
		return err
	}
	defer cancel()
	m.mu.Lock()
	if err := m.checkLocked(ctx); err != nil {
		m.mu.Unlock()
		return err
	}
	e, err := m.ownedLocked(caller, id)
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if err := m.admit(ctx, caller.Principal, e.project); err != nil {
		if isAbsent(err) {
			return failure("not_found", "live session was not found")
		}
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return err
	}
	current, err := m.ownedLocked(caller, id)
	if err != nil {
		return err
	}
	// Set replaces an entry while preserving the session's lifetime. Retry
	// admission against its current project before closing an updated entry.
	if current != e {
		return failure("busy", "session changed during close admission")
	}
	e.cancel()
	delete(m.sessions, id)
	return nil
}

// SessionContext is for a future watch owner. It proves ownership, renews
// nothing, and cancels on close, expiry, manager shutdown or active lease loss.
func (m *Manager) SessionContext(ctx context.Context, caller Caller, id string) (context.Context, error) {
	ctx, cancel, caller, err := m.prepare(ctx, caller, "")
	if err != nil {
		return nil, err
	}
	defer cancel()
	m.mu.Lock()
	if err := m.checkLocked(ctx); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	e, err := m.ownedLocked(caller, id)
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := m.admit(ctx, caller.Principal, e.project); err != nil {
		if isAbsent(err) {
			return nil, failure("not_found", "live session was not found")
		}
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return nil, err
	}
	current, err := m.ownedLocked(caller, id)
	if err != nil {
		return nil, err
	}
	if current != e {
		return nil, failure("busy", "session changed during context admission")
	}
	return e.ctx, nil
}

func (m *Manager) Set(ctx context.Context, caller Caller, req SetRequest) (Presence, error) {
	ctx, cancel, caller, err := m.prepare(ctx, caller, req.Project)
	if err != nil {
		return Presence{}, err
	}
	defer cancel()
	m.mu.Lock()
	if err = m.checkLocked(ctx); err == nil {
		var current *entry
		current, err = m.ownedLocked(caller, req.SessionID)
		if err == nil {
			copy := *current
			m.mu.Unlock()
			return m.set(ctx, caller, req, copy)
		}
	}
	m.mu.Unlock()
	return Presence{}, err
}

func (m *Manager) set(ctx context.Context, caller Caller, req SetRequest, before entry) (Presence, error) {
	// Owning an ID does not permit moving a session out of a project whose
	// current access has been revoked. Unreadable sessions share not_found.
	if err := m.admit(ctx, caller.Principal, before.project); err != nil {
		if isAbsent(err) {
			return Presence{}, failure("not_found", "live session was not found")
		}
		return Presence{}, err
	}
	if len(req.WorkingOn) > MaxWorkingOn {
		return Presence{}, failure("invalid_params", "workingOn exceeds 64 resources")
	}
	name, group := before.session.OnBehalfOf, before.group
	if req.OnBehalfOf != nil {
		var err error
		name, group, err = delegation(caller.Principal, req.OnBehalfOf)
		if err != nil {
			return Presence{}, err
		}
	}
	project := before.project
	if req.Project != "" {
		project = req.Project
	}
	focus := before.focus
	if req.Focus != nil {
		focus = *req.Focus
	}
	if focus != "" {
		v, visible, err := m.read(ctx, caller.Principal, focus)
		if err != nil {
			return Presence{}, err
		}
		if !visible || (project != "" && v.Bead.Project != project) {
			return Presence{}, failure("not_found", "focus resource was not found")
		}
		project = v.Bead.Project
	}
	working := before.working
	if req.WorkingOn != nil {
		working = slices.Clone(req.WorkingOn)
	}
	working = slices.Clone(working)
	sort.Strings(working)
	working = slices.Compact(working)
	for _, resource := range working {
		v, visible, err := m.read(ctx, caller.Principal, resource)
		if err != nil {
			return Presence{}, err
		}
		if !visible || (project != "" && v.Bead.Project != project) || !holdsClaim(caller, v, m.config.Clock()) {
			return Presence{}, failure("forbidden", "workingOn requires a current caller-owned claim in the presence project")
		}
		project = v.Bead.Project
	}
	if err := m.admit(ctx, caller.Principal, project); err != nil {
		if isAbsent(err) {
			return Presence{}, failure("not_found", "live session was not found")
		}
		return Presence{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return Presence{}, err
	}
	if err := m.capability(caller.Principal); err != nil {
		return Presence{}, err
	}
	current, err := m.ownedLocked(caller, req.SessionID)
	if err != nil {
		return Presence{}, err
	}
	// Another completed update wins; do not overwrite values whose validation
	// this operation did not observe. The caller can retry the full update.
	if current.sequence != before.sequence {
		return Presence{}, failure("busy", "session changed during presence validation")
	}
	updated := before
	updated.project, updated.focus, updated.working, updated.group = project, focus, working, group
	updated.session.OnBehalfOf = name
	updated.expires = m.config.Clock().Add(m.config.IdleTTL)
	m.sequence++
	updated.sequence = m.sequence
	m.sessions[req.SessionID] = &updated
	return Presence{SessionID: req.SessionID, Principal: displayPrincipal(caller.Principal), Focus: focus, WorkingOn: slices.Clone(working), OnBehalfOf: name}, nil
}

// Close ends this manager's lifetime. A new manager starts empty even when it
// uses the same store; no session or presence state reaches storage or dumps.
func (m *Manager) Close() error {
	m.mu.Lock()
	m.shutdownLocked()
	m.mu.Unlock()
	<-m.done
	return nil
}

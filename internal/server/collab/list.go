package collab

import (
	"context"
	"slices"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/service"
)

func (m *Manager) List(ctx context.Context, caller Caller, project string) ([]Presence, error) {
	if project == "" {
		return nil, failure("project_required", "supply a project for presence")
	}
	ctx, cancel, caller, err := m.prepare(ctx, caller, project)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := m.admit(ctx, caller.Principal, project); err != nil {
		return nil, err
	}
	m.mu.Lock()
	if err = m.checkLocked(ctx); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	entries := make([]*entry, 0, len(m.sessions))
	for _, e := range m.sessions {
		if e.project == project {
			entries = append(entries, e)
		}
	}
	m.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].session.OpenedAt.Equal(entries[j].session.OpenedAt) {
			return entries[i].session.OpenedAt.Before(entries[j].session.OpenedAt)
		}
		return entries[i].session.SessionID < entries[j].session.SessionID
	})
	// Reads are current on EVERY list, including focus set before a revocation,
	// moved/deleted resources, expired/reclaimed claims, and grouped members.
	type resourceRead struct {
		value   service.GetResult
		visible bool
	}
	cache := map[string]resourceRead{}
	read := func(resource string) (resourceRead, error) {
		if result, found := cache[resource]; found {
			return result, nil
		}
		v, visible, err := m.read(ctx, caller.Principal, resource)
		result := resourceRead{value: v, visible: visible && v.Bead.Project == project}
		if err == nil {
			cache[resource] = result
		}
		return result, err
	}
	rows := make([]Presence, len(entries))
	for i, e := range entries {
		row := Presence{SessionID: e.session.SessionID, Principal: displayPrincipal(e.caller.Principal), OnBehalfOf: e.session.OnBehalfOf}
		if e.focus != "" {
			v, err := read(e.focus)
			if err != nil {
				return nil, err
			}
			if v.visible {
				row.Focus = e.focus
			}
		}
		for _, resource := range e.working {
			v, err := read(resource)
			if err != nil {
				return nil, err
			}
			if v.visible && holdsClaim(e.caller, v.value, m.config.Clock()) {
				row.WorkingOn = append(row.WorkingOn, resource)
			}
		}
		rows[i] = row
	}
	if err := m.admit(ctx, caller.Principal, project); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(ctx); err != nil {
		return nil, err
	}
	if err := m.capability(caller.Principal); err != nil {
		return nil, err
	}
	out := []Presence{}
	groups := map[string]int{}
	for i, e := range entries {
		if m.sessions[e.session.SessionID] != e {
			// Closing/expiry never leaves a stale row. Concurrent updates cause
			// an explicit retry instead of a response with partially old state.
			if m.sessions[e.session.SessionID] != nil {
				return nil, failure("busy", "session changed during presence read")
			}
			continue
		}
		row := rows[i]
		if e.group == "" {
			out = append(out, row)
			continue
		}
		index, exists := groups[e.group]
		if !exists {
			groups[e.group] = len(out)
			out = append(out, row)
			continue
		}
		group := &out[index]
		if group.Focus == "" {
			group.Focus = row.Focus
		}
		group.WorkingOn = append(group.WorkingOn, row.WorkingOn...)
		sort.Strings(group.WorkingOn)
		group.WorkingOn = slices.Compact(group.WorkingOn)
		if len(group.WorkingOn) > MaxWorkingOn {
			return nil, failure("scope_too_large", "compact presence exceeds 64 distinct claimed resources")
		}
	}
	return out, nil
}

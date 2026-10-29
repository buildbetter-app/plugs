package collab

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// capability prevents the baseline service's unfiltered Get from becoming an
// authorization adapter. Installed policy without the real port always refuses.
func (m *Manager) capability(by model.PrincipalRef) error {
	if m.config.AuthorizationInstalled() {
		if m.config.Policy == nil {
			return failure("authz_unavailable", "current resource authorization is unavailable")
		}
	} else if by.Kind == "agent" || by.Sponsor != nil || len(by.Caveats) != 0 {
		return failure("authz_not_installed", "scoped principals require an authorization plugin")
	}
	return nil
}

func resourceID(resource string) (string, error) {
	id, found := strings.CutPrefix(resource, "bead/")
	if !found || id == "" || len(resource) > MaxResourceBytes {
		return "", failure("invalid_params", "presence supports bounded bead/<id> resources")
	}
	return id, nil
}

func isAbsent(err error) bool {
	var e *model.Error
	return errors.As(err, &e) && (e.Data.Code == "not_found" || e.Data.Code == "forbidden")
}

// read returns no resource on denial/absence. Arbitrary service/policy errors
// refuse the entire response instead of manufacturing an empty/allowed result.
func (m *Manager) read(ctx context.Context, by model.PrincipalRef, resource string) (service.GetResult, bool, error) {
	id, err := resourceID(resource)
	if err != nil {
		return service.GetResult{}, false, err
	}
	if err := m.capability(by); err != nil {
		return service.GetResult{}, false, err
	}
	installed := m.config.AuthorizationInstalled()
	p, err := clonePrincipal(by)
	if err != nil {
		return service.GetResult{}, false, err
	}
	v, err := m.config.Service.Get(ctx, p, service.GetRequest{ID: model.RefByID(id, model.Natural64(0))})
	if ctx.Err() != nil {
		return service.GetResult{}, false, failure("busy", "request was canceled")
	}
	if isAbsent(err) {
		return service.GetResult{}, false, nil
	}
	if err != nil {
		return service.GetResult{}, false, err
	}
	if v.Bead.ID != id || v.Bead.Project == "" {
		return service.GetResult{}, false, failure("internal_error", "invalid service resource result")
	}
	if installed {
		if m.config.Policy == nil {
			return service.GetResult{}, false, failure("authz_unavailable", "current resource authorization is unavailable")
		}
		p, _ = clonePrincipal(by)
		allowed, err := m.config.Policy.CanRead(ctx, p, v.Bead)
		if ctx.Err() != nil {
			return service.GetResult{}, false, failure("busy", "request was canceled")
		}
		if err != nil {
			return service.GetResult{}, false, failure("authz_unavailable", "current resource authorization failed")
		}
		if !allowed {
			return service.GetResult{}, false, nil
		}
	}
	if installed != m.config.AuthorizationInstalled() {
		return service.GetResult{}, false, failure("authz_unavailable", "authorization installation changed during read")
	}
	if err := ctx.Err(); err != nil {
		return service.GetResult{}, false, failure("busy", "request was canceled")
	}
	return v, true, nil
}

func clonePrincipal(p model.PrincipalRef) (model.PrincipalRef, error) {
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > MaxPrincipalBytes {
		return model.PrincipalRef{}, failure("unauthenticated", "invalid or oversized authenticated principal")
	}
	var copy model.PrincipalRef
	if err := json.Unmarshal(raw, &copy); err != nil {
		return model.PrincipalRef{}, failure("unauthenticated", "invalid authenticated principal")
	}
	return copy, nil
}

func principalKey(p model.PrincipalRef) string {
	raw, _ := json.Marshal(p)
	return string(raw)
}

// Public presence never exposes another principal's resource caveats.
func displayPrincipal(p model.PrincipalRef) model.PrincipalRef {
	out := model.PrincipalRef{Kind: p.Kind, ID: p.ID}
	if p.Sponsor != nil {
		sponsor := displayPrincipal(*p.Sponsor)
		out.Sponsor = &sponsor
	}
	return out
}

func ownership(c Caller) string {
	key := principalKey(c.Principal)
	if c.Principal.Kind == "anonymous" {
		label, _ := json.Marshal(c.Agent)
		key += string(label)
	}
	return key
}

func delegation(p model.PrincipalRef, requested *string) (string, string, error) {
	target := p
	if p.Sponsor != nil {
		target = *p.Sponsor
	}
	name := ""
	if p.Sponsor != nil {
		name = target.ID
	}
	if requested != nil {
		name = *requested
	}
	if name == "" {
		return "", "", nil
	}
	if name != target.ID {
		return "", "", failure("forbidden", "onBehalfOf must identify the caller or its authenticated sponsor")
	}
	return name, principalKey(model.PrincipalRef{Kind: target.Kind, ID: target.ID}), nil
}

func holdsClaim(c Caller, v service.GetResult, now time.Time) bool {
	return v.Lease != nil && now.Before(v.Lease.ExpiresAt) &&
		principalKey(v.Lease.Principal) == principalKey(c.Principal) &&
		(c.Principal.Kind != "anonymous" || v.Lease.Agent == c.Agent)
}

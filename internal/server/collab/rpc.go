package collab

import (
	"context"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// Bind registers the collaboration state handlers before rpc.New snapshots the
// registry. The watch adapter binds presence.watch after it has a real watch
// manager and collaboration snapshot source.
func Bind(registry *rpc.Registry, manager *Manager) error {
	if registry == nil || manager == nil {
		return fmt.Errorf("nil collaboration manager or registry")
	}
	names := []string{"session.open", "session.close", "presence.set", "presence.list"}
	existing := map[string]bool{}
	for _, descriptor := range registry.Descriptors() {
		existing[descriptor.Name] = true
	}
	for _, name := range names {
		if existing[name] {
			return fmt.Errorf("duplicate collaboration handler: %s", name)
		}
	}
	for _, name := range names {
		if err := registry.Register(name, manager.handle); err != nil {
			return err
		}
	}
	return nil
}

type params struct {
	SessionID   string    `json:"sessionId"`
	Focus       *string   `json:"focus"`
	WorkingOn   []string  `json:"workingOn"`
	OnBehalfOf  *string   `json:"onBehalfOf"`
	DisplayName *string   `json:"displayName"`
	Project     *string   `json:"project"`
	Meta        wire.Meta `json:"_meta"`
}

func (m *Manager) handle(ctx context.Context, call rpc.Call) (any, error) {
	var p params
	if err := wire.DecodeInto(call.Params, &p); err != nil {
		return nil, failure("invalid_params", "invalid collaboration parameters")
	}
	caller := Caller{Principal: call.Principal, Agent: call.Meta.Agent}
	switch call.Method {
	case "session.open":
		name, err := normalizeDisplayName(p.DisplayName)
		if err != nil {
			return nil, err
		}
		return m.open(ctx, caller, call.Meta.Project, p.OnBehalfOf, name)
	case "session.close":
		if err := m.CloseSession(ctx, caller, p.SessionID); err != nil {
			return nil, err
		}
		return struct {
			Closed bool `json:"closed"`
		}{true}, nil
	case "presence.set":
		return m.Set(ctx, caller, SetRequest{SessionID: p.SessionID, Project: call.Meta.Project, Focus: p.Focus, WorkingOn: p.WorkingOn, OnBehalfOf: p.OnBehalfOf})
	case "presence.list":
		project := call.Meta.Project
		if p.Project != nil {
			project = *p.Project
		}
		rows, err := m.List(ctx, caller, project)
		return struct {
			Presence []Presence `json:"presence"`
		}{rows}, err
	default:
		return nil, failure("method_not_found", "unsupported collaboration method")
	}
}

func normalizeDisplayName(input *string) (string, error) {
	if input == nil {
		return "", nil
	}
	name := strings.TrimSpace(*input)
	if utf8.RuneCountInString(name) > 64 {
		return "", failure("invalid_params", "displayName exceeds 64 Unicode scalar values after trimming")
	}
	for _, r := range name {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return "", failure("invalid_params", "displayName contains a control or format character")
		}
	}
	return name, nil
}

// Package viewrpc binds the read-only A7 view methods to the canonical RPC
// registry. Saved-view writes and CRUD are intentionally left unbound.
package viewrpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

type Service interface {
	Run(context.Context, model.PrincipalRef, viewops.RunRequest) (viewops.Result, error)
	Explain(context.Context, model.PrincipalRef, viewops.ExplainRequest) (viewops.ExplainResult, error)
}

type Config struct {
	CursorKey []byte
}

var names = []string{"view.run", "view.explain"}

// Bind uses a fresh process-local signing key. Hosts that need cursor
// continuity across server restarts should call BindWithConfig with a stable
// secret before rpc.New snapshots the registry.
func Bind(registry *rpc.Registry, service Service) error {
	return BindWithConfig(registry, service, Config{})
}

func BindWithConfig(registry *rpc.Registry, service Service, cfg Config) error {
	if registry == nil || service == nil {
		return fmt.Errorf("view binding requires a registry and service")
	}
	codec, err := NewCursorCodec(cfg.CursorKey)
	if err != nil {
		return err
	}
	bound := map[string]bool{}
	for _, descriptor := range registry.Descriptors() {
		bound[descriptor.Name] = true
	}
	for _, name := range names {
		if bound[name] {
			return fmt.Errorf("duplicate handler: %s", name)
		}
	}
	for _, name := range names {
		method := name
		if err := registry.Register(method, func(ctx context.Context, call rpc.Call) (any, error) {
			return dispatch(ctx, service, codec, call)
		}); err != nil {
			return err
		}
	}
	return nil
}

type params struct {
	CountOnly  bool                 `json:"countOnly,omitempty"`
	Stale      *viewops.StaleFilter `json:"stale,omitempty"`
	ViewID     string               `json:"viewId,omitempty"`
	Definition *definitionWire      `json:"definition,omitempty"`
	Cursor     *string              `json:"cursor,omitempty"`
	AsOf       *asOfWire            `json:"asOf,omitempty"`
	ItemID     string               `json:"itemId,omitempty"`
	Meta       wire.Meta            `json:"_meta,omitempty"`
}

type asOfWire struct {
	Revision    model.Revision `json:"revision"`
	Incarnation string         `json:"incarnation,omitempty"`
}

type filterWire struct {
	Query     string   `json:"query,omitempty"`
	Resources []string `json:"resources,omitempty"`
	Types     []string `json:"types,omitempty"`
	Events    []string `json:"events,omitempty"`
	ViewID    string   `json:"viewId,omitempty"`
}

type definitionWire struct {
	Name         string         `json:"name,omitempty"`
	Filter       *filterWire    `json:"filter"`
	Fields       *[]string      `json:"fields,omitempty"`
	GroupBy      []string       `json:"groupBy,omitempty"`
	Sort         []string       `json:"sort,omitempty"`
	BudgetTokens *model.Natural `json:"budgetTokens,omitempty"`
}

func (d *definitionWire) domain() *viewops.Definition {
	if d == nil {
		return nil
	}
	out := &viewops.Definition{Name: d.Name, GroupBy: append([]string(nil), d.GroupBy...), Sort: append([]string(nil), d.Sort...), BudgetTokens: d.BudgetTokens}
	if d.Filter != nil {
		out.Filter = &viewops.Filter{Query: d.Filter.Query, Resources: append([]string(nil), d.Filter.Resources...), Types: append([]string(nil), d.Filter.Types...), Events: append([]string(nil), d.Filter.Events...), ViewID: d.Filter.ViewID}
	}
	if d.Fields != nil {
		out.Fields = append([]string{}, (*d.Fields)...)
	}
	return out
}

func (a *asOfWire) domain() *viewops.AsOf {
	if a == nil {
		return nil
	}
	return &viewops.AsOf{Revision: a.Revision, Incarnation: a.Incarnation}
}

func dispatch(ctx context.Context, service Service, codec *CursorCodec, call rpc.Call) (any, error) {
	var p params
	if err := wire.DecodeInto(call.Params, &p); err != nil {
		return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	}
	switch call.Method {
	case "view.run":
		if p.CountOnly && p.Cursor != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "countOnly does not accept a cursor"}}
		}
		var cursor *viewops.Cursor
		var err error
		if p.Cursor != nil {
			cursor, err = codec.Decode(*p.Cursor)
			if err != nil {
				return nil, err
			}
		}
		result, err := service.Run(ctx, call.Principal, viewops.RunRequest{CountOnly: p.CountOnly, Stale: p.Stale, ViewID: p.ViewID, Definition: p.Definition.domain(), Cursor: cursor, AsOf: p.AsOf.domain(), Project: call.Meta.Project, Agent: call.Meta.Agent})
		if err != nil {
			return nil, err
		}
		return encodeResult(codec, call, p, result)
	case "view.explain":
		if p.Stale != nil {
			return nil, &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: "stale is only available on view.run"}}
		}
		result, err := service.Explain(ctx, call.Principal, viewops.ExplainRequest{ViewID: p.ViewID, ItemID: p.ItemID, Project: call.Meta.Project, Agent: call.Meta.Agent})
		if err != nil {
			return nil, err
		}
		if result.Reasons == nil {
			result.Reasons = []string{}
		}
		return result, nil
	default:
		return nil, &model.Error{Data: model.ErrorData{Code: "method_not_found"}}
	}
}

func encodeResult(codec *CursorCodec, call rpc.Call, p params, result viewops.Result) (map[string]any, error) {
	items := make([]viewops.Bead, 0, len(result.Items))
	items = append(items, result.Items...)
	out := map[string]any{"items": items, "asOf": map[string]any{"revision": result.AsOf.Revision}}
	if result.Count != nil {
		out["count"] = *result.Count
	}
	if result.Projection != nil {
		// Projection health is response metadata, not part of the signed
		// paging cursor. Preserve the exact status observed with this pinned
		// view snapshot while keeping continuation tokens unchanged.
		out["projection"] = result.Projection
	}
	if result.AsOf.Incarnation != "" {
		out["asOf"].(map[string]any)["incarnation"] = result.AsOf.Incarnation
	}
	var next string
	if result.NextCursor != nil {
		var err error
		next, err = codec.Encode(result.NextCursor)
		if err != nil {
			return nil, fmt.Errorf("sign view cursor: %w", err)
		}
		out["nextCursor"] = next
	}
	if len(result.Groups) != 0 {
		groups := make([]map[string]any, 0, len(result.Groups))
		for _, group := range result.Groups {
			entry := map[string]any{"key": group.Key, "count": group.Count, "items": group.Items}
			if group.Elided != nil {
				refresh := group.Elided.Reason == "authorization"
				if refresh && (group.Key != "total" || result.NextCursor != nil) {
					return nil, fmt.Errorf("authorization elision must be an atomic epic total")
				}
				if !refresh && next == "" {
					return nil, fmt.Errorf("view group elision lacks continuation cursor")
				}
				cursor := next
				if refresh {
					cursor = ""
				}
				continueParams, err := continuationParams(call, p, cursor)
				if err != nil {
					return nil, err
				}
				entry["elided"] = map[string]any{"count": group.Elided.Count, "reason": group.Elided.Reason, "continueWith": map[string]any{"method": "view.run", "params": continueParams}}
			}
			groups = append(groups, entry)
		}
		out["groups"] = groups
	}
	return out, nil
}

func continuationParams(call rpc.Call, p params, cursor string) (map[string]any, error) {
	params := map[string]any{}
	if p.Stale != nil {
		params["stale"] = *p.Stale
	}
	if cursor != "" {
		params["cursor"] = cursor
	}
	if p.ViewID != "" {
		params["viewId"] = p.ViewID
	} else if p.Definition != nil {
		var raw map[string]any
		encoded, err := json.Marshal(p.Definition)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(encoded, &raw); err != nil {
			return nil, err
		}
		// json omitempty cannot carry explicit fields=[] through definitionWire;
		// restore it for the exact #20 contract shape.
		if p.Definition.Fields != nil {
			raw["fields"] = append([]string{}, (*p.Definition.Fields)...)
		}
		params["definition"] = raw
	}
	if call.Meta.Project != "" || call.Meta.Agent != "" {
		meta := map[string]any{}
		if call.Meta.Project != "" {
			meta["project"] = call.Meta.Project
		}
		if call.Meta.Agent != "" {
			meta["agent"] = call.Meta.Agent
		}
		params["_meta"] = meta
	}
	return params, nil
}

package workflow

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// NativeInvoker is supplied by the trusted standard-service adapter. It must
// obtain the current caller and write metadata from its authenticated context,
// and route permanent invocation through the ordinary writer. The plugin
// never invents a principal or an idempotency key.
type NativeInvoker func(context.Context, *Provider, InvokeRequest) (InvokeResult, error)

// NativeLifecycleAdapter is the trusted host boundary for the complete B26
// public producer. The adapter, rather than wire parameters, supplies the
// authenticated principal and write metadata for lifecycle moves.
type NativeLifecycleAdapter struct {
	Principal func(context.Context) model.PrincipalRef
	Invoke    NativeInvoker
	Promote   func(context.Context, *Provider, model.PrincipalRef, PromoteRequest, model.WriteMeta) (PromoteResult, error)
	Demote    func(context.Context, *Provider, model.PrincipalRef, DemoteRequest, model.WriteMeta) (DemoteResult, error)
}

// Native exposes the plugin-owned invocation method for hosts that compose
// extension methods directly. The core composition binds the same Provider to
// standard tools so workflow items are visible through bead.ready/get/comment/
// close rather than requiring plugin-only methods.
func Native(invoke NativeInvoker) sdk.Native {
	return NativeWithLifecycle(NativeLifecycleAdapter{Invoke: invoke})
}

// NativeWithLifecycle exposes invoke plus the typed promotion/demotion methods
// when a composition supplies all trusted callbacks.
func NativeWithLifecycle(adapter NativeLifecycleAdapter) sdk.Native {
	manifest := pluginapi.PluginManifest{
		ID: "buildbetter.workflow", Alias: Alias, Version: "1.0.0", PluginAPI: pluginapi.APIVersion, Runtime: pluginapi.RuntimeNative,
		Requires: pluginapi.Requirements{State: pluginapi.StateDurable},
		Provides: []pluginapi.Provision{{Kind: "methods", Prefix: "x.workflow", Methods: map[string]pluginapi.MethodGrant{
			"invoke":      {Permission: "write", Resource: "params.project"},
			"run.promote": {Permission: "write", Resource: "params.target"},
			"run.demote":  {Permission: "write", Resource: "params.source"},
		}}},
	}
	return sdk.Native{Instance: "default", Manifest: manifest, New: func(host pluginapi.Host) (sdk.Registration, error) {
		if adapter.Invoke == nil {
			return sdk.Registration{}, fail("unauthenticated", "workflow native invocation requires a trusted caller adapter")
		}
		provider, err := New(host.DurableState())
		if err != nil {
			return sdk.Registration{}, err
		}
		methods := []sdk.Method{
			{
				Descriptor: pluginapi.Method{
					Name: "x.workflow.invoke", Permission: "write",
					Params: map[string]any{"type": "object"}, Result: map[string]any{"type": "object"},
					Resource: func(params any) []pluginapi.Ref {
						value, _ := params.(map[string]any)
						project, _ := value["project"].(string)
						return []pluginapi.Ref{pluginapi.RefByID(project, "")}
					},
				},
				Handle: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
					var req InvokeRequest
					if err := json.Unmarshal(params, &req); err != nil {
						return nil, err
					}
					result, err := adapter.Invoke(ctx, provider, req)
					if err != nil {
						return nil, err
					}
					return json.Marshal(result)
				},
			},
		}
		if adapter.Promote != nil {
			methods = append(methods, sdk.Method{
				Descriptor: pluginapi.Method{Name: "x.workflow.run.promote", Permission: "write", Params: map[string]any{"type": "object"}, Result: map[string]any{"type": "object"}},
				Handle: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
					var req PromoteRequest
					if err := json.Unmarshal(params, &req); err != nil {
						return nil, err
					}
					if adapter.Principal == nil {
						return nil, fail("unauthenticated", "workflow promotion requires a trusted caller adapter")
					}
					result, err := adapter.Promote(ctx, provider, adapter.Principal(ctx), req, model.WriteMeta{Method: "x.workflow.run.promote"})
					if err != nil {
						return nil, err
					}
					return json.Marshal(result)
				},
			})
		}
		if adapter.Demote != nil {
			methods = append(methods, sdk.Method{
				Descriptor: pluginapi.Method{Name: "x.workflow.run.demote", Permission: "write", Params: map[string]any{"type": "object"}, Result: map[string]any{"type": "object"}},
				Handle: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
					var req DemoteRequest
					if err := json.Unmarshal(params, &req); err != nil {
						return nil, err
					}
					if adapter.Principal == nil {
						return nil, fail("unauthenticated", "workflow demotion requires a trusted caller adapter")
					}
					result, err := adapter.Demote(ctx, provider, adapter.Principal(ctx), req, model.WriteMeta{Method: "x.workflow.run.demote"})
					if err != nil {
						return nil, err
					}
					return json.Marshal(result)
				},
			})
		}
		return sdk.Registration{Methods: methods}, nil
	}}
}

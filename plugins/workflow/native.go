package workflow

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// Native exposes the plugin-owned invocation method for hosts that compose
// extension methods directly. The core composition binds the same Provider to
// standard tools so workflow items are visible through bead.ready/get/comment/
// close rather than requiring plugin-only methods.
func Native() sdk.Native {
	manifest := pluginapi.PluginManifest{
		ID: "buildbetter.workflow", Alias: Alias, Version: "1.0.0", PluginAPI: pluginapi.APIVersion, Runtime: pluginapi.RuntimeNative,
		Provides: []pluginapi.Provision{{Kind: "methods", Prefix: "x.workflow", Methods: map[string]pluginapi.MethodGrant{
			"invoke": {Permission: "write", Resource: "params.project"},
		}}},
	}
	return sdk.Native{Instance: "default", Manifest: manifest, New: func(host pluginapi.Host) (sdk.Registration, error) {
		provider, err := New(host.DurableState())
		if err != nil {
			return sdk.Registration{}, err
		}
		return sdk.Registration{Methods: []sdk.Method{{Descriptor: pluginapi.Method{
			Name: "x.workflow.invoke", Permission: "write",
			Params: map[string]any{"type": "object"}, Result: map[string]any{"type": "object"},
			Resource: func(params any) []pluginapi.Ref {
				value, _ := params.(map[string]any)
				project, _ := value["project"].(string)
				return []pluginapi.Ref{pluginapi.RefByID(project, "")}
			},
		}, Handle: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var req InvokeRequest
			if err := json.Unmarshal(params, &req); err != nil {
				return nil, err
			}
			result, err := provider.Invoke(ctx, model.PrincipalRef{Kind: "plugin", ID: "buildbetter.workflow/default"}, req, model.WriteMeta{Method: "x.workflow.invoke", IdempotencyKey: "native-workflow-invoke", Project: req.Project})
			if err != nil {
				return nil, err
			}
			return json.Marshal(result)
		}}}}, nil
	}}
}

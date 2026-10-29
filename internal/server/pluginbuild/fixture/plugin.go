package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// PlugNatives is the fixed source-build ABI. The host obtains this symbol
// from the produced plugin rather than manufacturing a registration in core.
func PlugNatives() ([]sdk.Native, error) {
	manifest := pluginapi.PluginManifest{
		ID: "app.example.source", Alias: "source", Version: "1.0.0",
		PluginAPI: "1.0", Runtime: "native",
		Provides: []pluginapi.Provision{
			{Kind: "subscriber", Name: "normalize", Phases: []pluginapi.Phase{pluginapi.PhaseMutate}, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}},
			{Kind: "subscriber", Name: "check", Phases: []pluginapi.Phase{pluginapi.PhaseValidate}, Reads: []string{"fields.title"}},
			{Kind: "methods", Prefix: "x.source", Methods: map[string]pluginapi.MethodGrant{
				"echo": {Permission: "read", Resource: "params.id"},
			}},
		},
	}
	return []sdk.Native{{Manifest: manifest, Instance: "default", New: newSource}}, nil
}

func newSource(host pluginapi.Host) (sdk.Registration, error) {
	descriptor := pluginapi.Method{
		Name: "x.source.echo", Permission: "read",
		Params: map[string]any{"type": "object"}, Result: map[string]any{"type": "object"},
		Resource: func(params any) []pluginapi.Ref {
			value, _ := params.(map[string]any)["id"].(string)
			return []pluginapi.Ref{pluginapi.RefByID(value, "")}
		},
	}
	return sdk.Registration{
		Mutators: map[string]pluginapi.Mutator{
			"normalize": sdk.MutateFunc(func(ctx context.Context, change pluginapi.ProposedChange, _ pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
				if err := host.Call(ctx, "tx.apply", map[string]any{}, nil); err == nil || !strings.Contains(err.Error(), "plugin_host_write_in_sync_call") {
					return nil, fmt.Errorf("hot graph write was not refused: %v", err)
				}
				if response, err := host.HTTP().Get("https://denied.example"); err == nil || !strings.Contains(err.Error(), "plugin_egress_in_hot_path") {
					if response != nil {
						_ = response.Body.Close()
					}
					return nil, fmt.Errorf("hot egress was not refused: %v", err)
				}
				patches := make([]pluginapi.Patch, 0, len(change.Changes))
				for _, item := range change.Changes {
					var body struct {
						Fields map[string]any `json:"fields"`
					}
					if err := json.Unmarshal(item.After, &body); err != nil {
						return nil, err
					}
					title, _ := body.Fields["title"].(string)
					value, err := json.Marshal(strings.ToUpper(title))
					if err != nil {
						return nil, err
					}
					patches = append(patches, pluginapi.Patch{Resource: item.Resource, Paths: map[string]json.RawMessage{"fields.normalized": value}})
				}
				return patches, nil
			}),
		},
		Methods: []sdk.Method{{Descriptor: descriptor, Handle: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var request struct{ State bool }
			if err := json.Unmarshal(params, &request); err != nil {
				return nil, err
			}
			if request.State {
				state := host.State()
				var count int
				if _, err := state.Get(ctx, "count", &count); err != nil {
					return nil, err
				}
				revision, err := state.Revision(ctx, "count")
				if err != nil {
					return nil, err
				}
				if _, err := state.Put(ctx, "count", count+1, revision); err != nil {
					return nil, err
				}
				return json.Marshal(count + 1)
			}
			return append(json.RawMessage(nil), params...), nil
		}}},
		Validators: map[string]pluginapi.Validator{
			"check": sdk.ValidateFunc(func(_ context.Context, change pluginapi.ProposedChange, _ pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
				for _, item := range change.Changes {
					var body struct {
						Fields map[string]any `json:"fields"`
					}
					if err := json.Unmarshal(item.After, &body); err != nil {
						return pluginapi.Verdict{}, err
					}
					if title, _ := body.Fields["title"].(string); title == "reject" {
						return pluginapi.Verdict{Allow: false, Violations: []pluginapi.Violation{{Guard: "source-check", Message: "source validator rejected"}}}, nil
					}
				}
				return pluginapi.Verdict{Allow: true}, nil
			}),
		},
	}, nil
}

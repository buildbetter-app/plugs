// Package fixture supplies compiled-in implementations of the source bundle
// used by pluginmanifest tests. Its only Plug imports are the public API/SDK.
package fixture

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

//go:embed plug-plugin.json
var manifestJSON []byte

func Natives() ([]sdk.Native, error) {
	manifest, err := pluginapi.DecodeManifest(manifestJSON)
	if err != nil {
		return nil, err
	}
	return []sdk.Native{
		{Manifest: manifest.Plugins[0], Instance: "default", New: normalizer},
		{Manifest: manifest.Plugins[1], Instance: "default", New: extension},
	}, nil
}

func normalizer(pluginapi.Host) (sdk.Registration, error) {
	return sdk.Registration{
		Mutators: map[string]pluginapi.Mutator{"title": sdk.MutateFunc(func(_ context.Context, change pluginapi.ProposedChange, _ pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
			patches := []pluginapi.Patch{}
			for _, item := range change.Changes {
				var body struct{ Fields map[string]string }
				if err := json.Unmarshal(item.After, &body); err != nil {
					return nil, err
				}
				value, err := json.Marshal(strings.ToUpper(body.Fields["title"]))
				if err != nil {
					return nil, err
				}
				patches = append(patches, pluginapi.Patch{Resource: item.Resource, Paths: map[string]json.RawMessage{"fields.normalized": value}})
			}
			return patches, nil
		})},
		Validators: map[string]pluginapi.Validator{"check": sdk.ValidateFunc(func(_ context.Context, change pluginapi.ProposedChange, _ pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
			for _, item := range change.Changes {
				var body struct{ Fields map[string]string }
				if err := json.Unmarshal(item.After, &body); err != nil {
					return pluginapi.Verdict{}, err
				}
				if body.Fields["normalized"] == "" {
					return pluginapi.Verdict{Allow: false}, nil
				}
			}
			return pluginapi.Verdict{Allow: true}, nil
		})},
	}, nil
}

func extension(host pluginapi.Host) (sdk.Registration, error) {
	descriptor := func(name string) pluginapi.Method {
		return pluginapi.Method{Name: "x.echo." + name, Permission: "read", Params: map[string]any{"type": "object"}, Result: map[string]any{"type": "object"}, Resource: func(params any) []pluginapi.Ref {
			id, _ := params.(map[string]any)["id"].(string)
			return []pluginapi.Ref{pluginapi.RefByID(id, "")}
		}}
	}
	return sdk.Registration{Methods: []sdk.Method{
		{Descriptor: descriptor("echo"), Handle: func(_ context.Context, params json.RawMessage) (json.RawMessage, error) { return params, nil }},
		{Descriptor: descriptor("access"), Handle: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct{ ID, Action, Method, URL string }
			if err := json.Unmarshal(params, &input); err != nil {
				return nil, err
			}
			switch input.Action {
			case "call":
				var result json.RawMessage
				if err := host.Call(ctx, input.Method, map[string]any{"id": input.ID}, &result); err != nil {
					return nil, err
				}
				return result, nil
			case "state":
				state := host.State()
				var count int
				if _, err := state.Get(ctx, "calls", &count); err != nil {
					return nil, err
				}
				revision, err := state.Revision(ctx, "calls")
				if err != nil {
					return nil, err
				}
				if _, err := state.Put(ctx, "calls", count+1, revision); err != nil {
					return nil, err
				}
				return json.Marshal(map[string]int{"count": count + 1})
			case "egress":
				response, err := host.HTTP().Get(input.URL)
				if err != nil {
					return nil, err
				}
				response.Body.Close()
				return json.RawMessage(`{}`), nil
			default:
				return nil, fmt.Errorf("unsupported fixture action")
			}
		}},
	}}, nil
}

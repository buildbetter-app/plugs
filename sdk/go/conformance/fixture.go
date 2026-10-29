// Package conformance supplies a real compiled-in plugin used by the native
// host's conformance tests. It imports only the public plugin API and SDK.
package conformance

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

type Options struct {
	PatchPath string
	Probe     func(context.Context, pluginapi.Host, pluginapi.ProposedChange, pluginapi.SnapshotReader) error
	After     func(context.Context, pluginapi.Host, pluginapi.CommittedChange, pluginapi.SnapshotReader) error
}

func Native(options Options) sdk.Native {
	return sdk.Native{
		Instance: "default",
		Manifest: pluginapi.PluginManifest{ID: "app.example.fixture", Alias: "fixture", Version: "1.0.0", PluginAPI: pluginapi.APIVersion, Runtime: "native",
			Provides: []pluginapi.Provision{
				{Kind: "subscriber", Name: "normalize", Phases: []pluginapi.Phase{pluginapi.PhaseMutate}, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}, MaxPointReads: 2, MaxQueries: 1},
				{Kind: "subscriber", Name: "check", Phases: []pluginapi.Phase{pluginapi.PhaseValidate}, Reads: []string{"fields.normalized"}},
				{Kind: "subscriber", Name: "observe", Phases: []pluginapi.Phase{pluginapi.PhasePost}, Reads: []string{"fields.normalized"}},
				{Kind: "methods", Prefix: "x.fixture", Methods: map[string]pluginapi.MethodGrant{"echo": {Permission: "read", Resource: "params.id"}}},
			}, Requires: pluginapi.Requirements{Methods: []string{"bead.get", "bead.ready", "tx.apply"}}},
		New: func(host pluginapi.Host) (sdk.Registration, error) {
			return sdk.Registration{
				Methods: []sdk.Method{{Descriptor: pluginapi.Method{Name: "x.fixture.echo", Params: map[string]any{"type": "object"}, Result: map[string]any{"type": "object"}, Permission: "read", Resource: func(params any) []pluginapi.Ref {
					return []pluginapi.Ref{pluginapi.RefByID(params.(map[string]any)["id"].(string), "")}
				}}, Handle: func(_ context.Context, params json.RawMessage) (json.RawMessage, error) { return params, nil }}},
				Mutators: map[string]pluginapi.Mutator{"normalize": sdk.MutateFunc(func(ctx context.Context, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
					if options.Probe != nil {
						if err := options.Probe(ctx, host, c, r); err != nil {
							return nil, err
						}
					}
					patches := make([]pluginapi.Patch, 0, len(c.Changes))
					for _, change := range c.Changes {
						var body struct {
							Fields map[string]any `json:"fields"`
						}
						if err := json.Unmarshal(change.After, &body); err != nil {
							return nil, err
						}
						title, _ := body.Fields["title"].(string)
						value, _ := json.Marshal(strings.ToUpper(title))
						path := options.PatchPath
						if path == "" {
							path = "fields.normalized"
						}
						patches = append(patches, pluginapi.Patch{Resource: change.Resource, Paths: map[string]json.RawMessage{path: value}})
					}
					return patches, nil
				})},
				Validators: map[string]pluginapi.Validator{"check": sdk.ValidateFunc(func(_ context.Context, c pluginapi.ProposedChange, _ pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
					for _, change := range c.Changes {
						var body struct {
							Fields map[string]any `json:"fields"`
						}
						if err := json.Unmarshal(change.After, &body); err != nil {
							return pluginapi.Verdict{}, err
						}
						if body.Fields["normalized"] == "" {
							return pluginapi.Verdict{Allow: false, Violations: []pluginapi.Violation{{Guard: "normalized", Message: "normalized title is empty"}}}, nil
						}
					}
					return pluginapi.Verdict{Allow: true}, nil
				})},
				Posts: map[string]pluginapi.PostSubscriber{"observe": sdk.AfterFunc(func(ctx context.Context, c pluginapi.CommittedChange, r pluginapi.SnapshotReader) error {
					if options.After != nil {
						return options.After(ctx, host, c, r)
					}
					return nil
				})},
			}, nil
		},
	}
}

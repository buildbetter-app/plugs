package sdk

import (
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/pluginapi"
)

type Handler func(context.Context, json.RawMessage) (json.RawMessage, error)

type Method struct {
	Descriptor pluginapi.Method
	Handle     Handler
}

// Registration names callbacks exactly as declared by the manifest. Instances
// receive a separate Host and guest runtime, even when sharing native code. A
// binding may pool those runtimes while sharing its host-side State().
type Registration struct {
	Methods    []Method
	Mutators   map[string]pluginapi.Mutator
	Validators map[string]pluginapi.Validator
	Posts      map[string]pluginapi.PostSubscriber
}

type Native struct {
	Manifest pluginapi.PluginManifest
	Instance string
	New      func(pluginapi.Host) (Registration, error)
}

type MutateFunc func(context.Context, pluginapi.ProposedChange, pluginapi.SnapshotReader) ([]pluginapi.Patch, error)

func (f MutateFunc) Mutate(ctx context.Context, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
	return f(ctx, c, r)
}

type ValidateFunc func(context.Context, pluginapi.ProposedChange, pluginapi.SnapshotReader) (pluginapi.Verdict, error)

func (f ValidateFunc) Validate(ctx context.Context, c pluginapi.ProposedChange, r pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
	return f(ctx, c, r)
}

type AfterFunc func(context.Context, pluginapi.CommittedChange, pluginapi.SnapshotReader) error

func (f AfterFunc) After(ctx context.Context, c pluginapi.CommittedChange, r pluginapi.SnapshotReader) error {
	return f(ctx, c, r)
}

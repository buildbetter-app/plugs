package pluginhost

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

func TestDurableManifestRefusesMemoryCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "durable-manifest", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)

	native := sdk.Native{
		Instance: "default",
		Manifest: pluginapi.PluginManifest{
			ID:        "app.example.durable",
			Alias:     "durable",
			Version:   "1.0.0",
			PluginAPI: pluginapi.APIVersion,
			Runtime:   pluginapi.RuntimeNative,
			Requires:  pluginapi.Requirements{State: pluginapi.StateDurable},
			Provides: []pluginapi.Provision{{
				Kind:   "methods",
				Prefix: "x.durable",
				Methods: map[string]pluginapi.MethodGrant{
					"probe": {Permission: "read", Resource: "params.project"},
				},
			}},
		},
		New: func(pluginapi.Host) (sdk.Registration, error) {
			return sdk.Registration{Methods: []sdk.Method{{
				Descriptor: pluginapi.Method{
					Name:       "x.durable.probe",
					Permission: "read",
					Resource: func(params any) []pluginapi.Ref {
						value, _ := params.(map[string]any)
						project, _ := value["project"].(string)
						return []pluginapi.Ref{pluginapi.RefByID(project, "")}
					},
				},
				Handle: func(context.Context, json.RawMessage) (json.RawMessage, error) {
					return json.RawMessage(`{}`), nil
				},
			}}}, nil
		},
	}

	_, err = New(Config{Natives: []sdk.Native{native}, CheckpointStore: store, CheckpointLease: lease})
	if code(err) != ErrDurableState {
		t.Fatalf("memory checkpoint accepted for durable manifest: %v", err)
	}
}

func TestMemoryDurableAccessorRefusesEveryOperation(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(ctx, "durable-accessor", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	state := NewDurableStateWithPersistence(store, lease, "example", "default", true)
	if state.(interface{ Persistent() bool }).Persistent() {
		t.Fatal("legacy caller flag granted restart durability to memory")
	}
	for name, operation := range map[string]func() error{
		"read":     func() error { var v any; _, _, err := state.Read(ctx, "state", &v); return err },
		"get":      func() error { var v any; _, err := state.Get(ctx, "state", &v); return err },
		"revision": func() error { _, err := state.Revision(ctx, "state"); return err },
		"put": func() error {
			_, err := state.Put(ctx, "state", map[string]any{"value": true}, pluginapi.Natural64(0))
			return err
		},
		"delete": func() error { return state.Delete(ctx, "state", pluginapi.Natural64(0)) },
		"list":   func() error { _, err := state.List(ctx, "state", 10); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := operation(); code(err) != ErrDurableState {
				t.Fatalf("memory durable %s did not refuse with typed capability error: %v", name, err)
			}
		})
	}
	checkpoint, err := store.LoadCheckpoint(ctx, "plugin/example/default/durable")
	if err != nil || checkpoint.Generation != 0 || len(checkpoint.Payload) != 0 {
		t.Fatalf("unavailable capability wrote a memory checkpoint: generation=%d err=%v", checkpoint.Generation, err)
	}
}

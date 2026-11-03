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

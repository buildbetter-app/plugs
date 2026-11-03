package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
	"github.com/buildbetter-app/plugs/sdk/go/conformance"
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

func TestPooledReplacementKeepsNativeInstanceDurableNamespace(t *testing.T) {
	ctx := context.Background()
	store, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "durable.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(ctx, "namespace-replacement", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	persistent, ok := store.(driver.PersistentCheckpointStore)
	if !ok || !persistent.Persistent() {
		t.Fatal("replacement namespace test requires a real persistent checkpoint store")
	}

	for _, test := range []struct {
		instance string
		timeout  bool
	}{
		{instance: "alpha"},
		{instance: "beta", timeout: true},
	} {
		t.Run(test.instance, func(t *testing.T) {
			native := conformance.Native(conformance.Options{})
			native.Instance = test.instance
			if native.Manifest.ID == native.Instance {
				t.Fatal("test manifest ID must differ from native instance")
			}
			host, err := New(Config{Natives: []sdk.Native{native}, Bindings: []Binding{{
				ID: "normalize", Alias: native.Manifest.Alias, Instance: native.Instance,
				Subscriber: "normalize", Phase: pluginapi.PhaseMutate,
				Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"},
			}}, CheckpointStore: persistent, CheckpointLease: lease})
			if err != nil {
				t.Fatal(err)
			}
			pool := host.bindings[0].pool
			state := host.instances[native.Manifest.Alias+"/"+native.Instance].durable
			if _, err := state.Put(ctx, "marker", test.instance, pluginapi.Natural64(0)); err != nil {
				t.Fatal(err)
			}
			callbackTimeout := time.Second
			if test.timeout {
				callbackTimeout = 20 * time.Millisecond
			}
			err = pool.run(ctx, pluginapi.PhaseMutate, callbackTimeout, nil, func(_ *instance, callbackCtx context.Context) error {
				if test.timeout {
					<-callbackCtx.Done()
					return callbackCtx.Err()
				}
				return errors.New("retire this guest")
			})
			if err == nil {
				t.Fatal("failed callback did not retire its pooled member")
			}
			deadline := time.After(2 * time.Second)
			for len(pool.available) != maxRuntimeInstances {
				select {
				case <-deadline:
					t.Fatalf("pool did not refill after retired callback: available=%d", len(pool.available))
				case <-time.After(time.Millisecond):
				}
			}
			members := make([]*instance, 0, maxRuntimeInstances)
			for index := 0; index < maxRuntimeInstances; index++ {
				members = append(members, <-pool.available)
			}
			for index, member := range members {
				var marker string
				found, err := member.durable.Get(ctx, "marker", &marker)
				if err != nil || !found || marker != test.instance {
					t.Fatalf("member %d switched durable namespace after replacement: found=%t marker=%q err=%v", index, found, marker, err)
				}
			}
			for _, member := range members {
				pool.available <- member
			}
		})
	}
}

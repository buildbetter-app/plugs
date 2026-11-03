package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func durableMethodNative(handle func(context.Context, pluginapi.DurableState) error) sdk.Native {
	return sdk.Native{
		Instance: "default",
		Manifest: pluginapi.PluginManifest{
			ID: "app.example.durableprobe", Alias: "durableprobe", Version: "1.0.0",
			PluginAPI: pluginapi.APIVersion, Runtime: pluginapi.RuntimeNative,
			Requires: pluginapi.Requirements{State: pluginapi.StateDurable},
			Provides: []pluginapi.Provision{{Kind: "methods", Prefix: "x.durableprobe", Methods: map[string]pluginapi.MethodGrant{
				"probe": {Permission: "read", Resource: "params.project"},
			}}},
		},
		New: func(host pluginapi.Host) (sdk.Registration, error) {
			// Like workflow.Native, a provider captures its state facade during
			// construction and uses it only from later authenticated callbacks.
			state := host.DurableState()
			if marker, ok := state.(interface{ Persistent() bool }); !ok || !marker.Persistent() {
				return sdk.Registration{}, fmt.Errorf("constructor lost persistent capability metadata")
			}
			return sdk.Registration{Methods: []sdk.Method{{
				Descriptor: pluginapi.Method{
					Name: "x.durableprobe.probe", Permission: "read", Resource: func(any) []pluginapi.Ref {
						return []pluginapi.Ref{pluginapi.RefByID("probe-project", "")}
					},
				},
				Handle: func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
					return json.RawMessage(`{}`), handle(ctx, state)
				},
			}}}, nil
		},
	}
}

func durableProbeStore(t *testing.T) (driver.PersistentCheckpointStore, driver.ActiveLease) {
	t.Helper()
	store, err := (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "invocation.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease, err := store.Lease(context.Background(), "durable-invocation", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	return store.(driver.PersistentCheckpointStore), lease
}

func requireDurableOperationsInactive(t *testing.T, state pluginapi.DurableState, ctx context.Context) {
	t.Helper()
	for name, operation := range map[string]func() error{
		"read":     func() error { var value any; _, _, err := state.Read(ctx, "outside", &value); return err },
		"get":      func() error { var value any; _, err := state.Get(ctx, "outside", &value); return err },
		"revision": func() error { _, err := state.Revision(ctx, "outside"); return err },
		"list":     func() error { _, err := state.List(ctx, "outside", 10); return err },
		"put":      func() error { _, err := state.Put(ctx, "outside", "unauthorized", pluginapi.Natural64(0)); return err },
		"delete":   func() error { return state.Delete(ctx, "outside", pluginapi.Natural64(0)) },
	} {
		if err := operation(); code(err) != ErrInactive {
			t.Errorf("retained %s returned %v; want %s", name, err, ErrInactive)
		}
	}
}

func TestConstructorHeldDurableStateRequiresExactLiveInvocation(t *testing.T) {
	store, lease := durableProbeStore(t)
	type captured struct {
		state   pluginapi.DurableState
		ctx     context.Context
		release chan struct{}
	}
	calls := make(chan captured, 2)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
	defer func() {
		for _, release := range releases {
			close(release)
		}
	}()
	index := 0 // Source extension calls are serialized by the host.
	native := durableMethodNative(func(ctx context.Context, state pluginapi.DurableState) error {
		var previous string
		_, revision, err := state.Read(ctx, "value", &previous)
		if err != nil {
			return err
		}
		if _, err := state.Put(ctx, "value", "accepted", revision); err != nil {
			return err
		}
		release := releases[index]
		index++
		calls <- captured{state, ctx, release}
		<-release
		return nil
	})
	host, err := New(Config{Natives: []sdk.Native{native}, CheckpointStore: store, CheckpointLease: lease})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	invoke := func(ctx context.Context) <-chan error {
		result := make(chan error, 1)
		go func() {
			_, err := host.Invoke(ctx, "x.durableprobe.probe", json.RawMessage(`{"project":"probe-project"}`))
			result <- err
		}()
		return result
	}
	await := func() captured {
		t.Helper()
		select {
		case call := <-calls:
			return call
		case <-time.After(2 * time.Second):
			t.Fatal("durable extension did not start")
			return captured{}
		}
	}
	firstResult := invoke(context.Background())
	first := await()
	requireDurableOperationsInactive(t, first.state, context.Background())
	// Complete the first call without closing the shared release twice.
	first.release <- struct{}{}
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
	requireDurableOperationsInactive(t, first.state, context.WithoutCancel(first.ctx))
	secondCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	secondResult := invoke(secondCtx)
	second := await()
	if second.state != first.state {
		t.Fatal("test did not reuse the constructor-held facade on the same extension instance")
	}
	// An earlier token cannot borrow authority from this later live call.
	requireDurableOperationsInactive(t, first.state, context.WithoutCancel(first.ctx))
	requireDurableOperationsInactive(t, second.state, context.Background())
	cancel()
	select {
	case err := <-secondResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("extension cancellation=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled invocation did not return")
	}
	// The guest is still unwinding and its lease is still held.
	requireDurableOperationsInactive(t, second.state, context.WithoutCancel(second.ctx))
	checkpoint, err := store.LoadCheckpoint(context.Background(), "plugin/durableprobe/default/durable")
	if err != nil || checkpoint.Generation != 2 {
		t.Fatalf("unauthorized operation published state: %+v err=%v", checkpoint, err)
	}
}

type cancelBoundCheckpoint struct {
	driver.PersistentCheckpointStore
	entered chan context.Context
	release chan struct{}
}

func (s *cancelBoundCheckpoint) SaveCheckpoint(ctx context.Context, _ driver.ActiveLease, _ string, _ uint64, _ []byte) (driver.Checkpoint, error) {
	s.entered <- ctx
	select {
	case <-ctx.Done():
		return driver.Checkpoint{}, ctx.Err()
	case <-s.release:
		return driver.Checkpoint{}, errors.New("test cleanup released blocked checkpoint")
	}
}

func TestDurableOperationCannotDetachFromInvocationCancellation(t *testing.T) {
	store, lease := durableProbeStore(t)
	blocked := &cancelBoundCheckpoint{store, make(chan context.Context, 1), make(chan struct{})}
	defer close(blocked.release)
	callbackDone := make(chan error, 1)
	native := durableMethodNative(func(ctx context.Context, state pluginapi.DurableState) error {
		_, err := state.Put(context.WithoutCancel(ctx), "blocked", "must not publish", pluginapi.Natural64(0))
		callbackDone <- err
		return err
	})
	host, err := New(Config{Natives: []sdk.Native{native}, CheckpointStore: blocked, CheckpointLease: lease})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := host.Invoke(ctx, "x.durableprobe.probe", json.RawMessage(`{"project":"probe-project"}`))
		finished <- err
	}()
	var operation context.Context
	select {
	case operation = <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("accepted durable write did not reach checkpoint storage")
	}
	cancel()
	select {
	case <-operation.Done():
	case <-time.After(2 * time.Second):
		t.Error("checkpoint IO escaped the canonical invocation cancellation")
	}
	select {
	case err := <-callbackDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("checkpoint cancellation=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("canceled checkpoint did not unwind")
	}
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Errorf("invocation cancellation=%v", err)
	}
	checkpoint, err := store.LoadCheckpoint(context.Background(), "plugin/durableprobe/default/durable")
	if err != nil || checkpoint.Generation != 0 {
		t.Fatalf("canceled checkpoint was published: %+v err=%v", checkpoint, err)
	}
}

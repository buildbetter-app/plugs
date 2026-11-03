package composition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

type nativeObservedStore struct {
	driver.Store
	driver.PersistentCheckpointStore
	// Preserve the real driver's response finalization capability; callbacks
	// must use the ordinary durable post-outcome receipt path as well.
	driver.ResponseFinalizer
	mu     sync.Mutex
	leases []driver.ActiveLease
	closed atomic.Bool
}

func (s *nativeObservedStore) Close() error {
	s.closed.Store(true)
	return s.Store.Close()
}

func (s *nativeObservedStore) SaveCheckpoint(ctx context.Context, lease driver.ActiveLease, key string, generation uint64, payload []byte) (driver.Checkpoint, error) {
	s.mu.Lock()
	s.leases = append(s.leases, lease)
	s.mu.Unlock()
	return s.PersistentCheckpointStore.SaveCheckpoint(ctx, lease, key, generation, payload)
}

func composedNative() sdk.Native {
	return sdk.Native{
		Instance: "default",
		Manifest: pluginapi.PluginManifest{
			ID: "app.example.composednative", Alias: "composednative", Version: "1.0.0",
			PluginAPI: pluginapi.APIVersion, Runtime: pluginapi.RuntimeNative,
			Requires: pluginapi.Requirements{State: pluginapi.StateDurable},
			Provides: []pluginapi.Provision{
				{Kind: "subscriber", Name: "normalize", Phases: []pluginapi.Phase{pluginapi.PhaseMutate}, Reads: []string{"fields.title"}, Owns: []string{"fields.title"}},
				{Kind: "subscriber", Name: "observe", Phases: []pluginapi.Phase{pluginapi.PhasePost}, Reads: []string{"fields.title"}},
			},
		},
		New: func(host pluginapi.Host) (sdk.Registration, error) {
			return sdk.Registration{
				Mutators: map[string]pluginapi.Mutator{"normalize": sdk.MutateFunc(func(_ context.Context, change pluginapi.ProposedChange, _ pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
					patches := make([]pluginapi.Patch, 0, len(change.Changes))
					for _, value := range change.Changes {
						patches = append(patches, pluginapi.Patch{Resource: value.Resource, Paths: map[string]json.RawMessage{"fields.title": json.RawMessage(`"COMPOSED"`)}})
					}
					return patches, nil
				})},
				Posts: map[string]pluginapi.PostSubscriber{"observe": sdk.AfterFunc(func(ctx context.Context, change pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
					_, err := host.DurableState().Put(ctx, "commit", change.Revision, pluginapi.Natural64(0))
					return err
				})},
			}, nil
		},
	}
}

func TestNativeCompositionUsesOwnedPersistentStoreAndActiveLease(t *testing.T) {
	ctx := context.Background()
	var observed *nativeObservedStore
	config := Config{
		Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "native.db"), Project: "native", Address: DefaultAddress,
		NativePlugins: []sdk.Native{composedNative()},
		NativeBindings: []pluginhost.Binding{
			{ID: "native-mutate", Alias: "composednative", Instance: "default", Subscriber: "normalize", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.title"}},
			{ID: "native-post", Alias: "composednative", Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost, Reads: []string{"fields.title"}},
		},
		StoreFactory: func(ctx context.Context, selected string, config driver.Config) (driver.Store, error) {
			if selected != "sqlite" {
				return nil, fmt.Errorf("unexpected driver %s", selected)
			}
			store, err := (sqlite.Driver{}).Open(ctx, config)
			if err != nil {
				return nil, err
			}
			observed = &nativeObservedStore{Store: store, PersistentCheckpointStore: store.(driver.PersistentCheckpointStore), ResponseFinalizer: store.(driver.ResponseFinalizer)}
			return observed, nil
		},
	}
	f := startRuntime(t, config)
	if f.r.Store != observed {
		t.Fatal("native composition replaced the configured store")
	}
	created, err := f.client.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "owned store and lease"}}, "550e8400-e29b-41d4-a716-446655440001")
	if err != nil {
		t.Fatal(err)
	}
	var commit struct {
		Revision pluginapi.Revision
		Warnings []json.RawMessage
	}
	if err := json.Unmarshal(created.JSON, &commit); err != nil || len(commit.Warnings) != 0 {
		t.Fatalf("native post did not persist its real HTTP commit: %s err=%v", created.JSON, err)
	}
	result := resultObject(t, created)
	ref := result["results"].([]any)[0].(map[string]any)["ref"]
	read, err := f.client.Call(ctx, "bead.get", map[string]any{"id": ref}, "")
	if err != nil {
		t.Fatal(err)
	}
	bead := resultObject(t, read)["bead"].(map[string]any)
	if bead["fields"].(map[string]any)["title"] != "COMPOSED" {
		t.Fatal("configured HTTP write bypassed the native mutator")
	}
	checkpoint, err := observed.LoadCheckpoint(ctx, "plugin/composednative/default/durable")
	if err != nil || checkpoint.Generation != 1 {
		t.Fatalf("same-store native checkpoint=%+v err=%v", checkpoint, err)
	}
	observed.mu.Lock()
	leases := append([]driver.ActiveLease(nil), observed.leases...)
	observed.mu.Unlock()
	if len(leases) != 1 || leases[0] != f.r.Lease {
		t.Fatal("native persistence did not use the exact Runtime active lease")
	}
	oldLease := f.r.Lease
	f.stop(t)
	config.StoreFactory = nil
	restarted := startRuntime(t, config)
	store := restarted.r.Store.(driver.PersistentCheckpointStore)
	var fenced driver.ErrFenced
	if _, err := store.SaveCheckpoint(ctx, oldLease, "plugin/composednative/default/durable", 1, checkpoint.Payload); !errors.As(err, &fenced) {
		t.Fatalf("old native-runtime lease was not fenced after reopen: %v", err)
	}
	persisted, err := store.LoadCheckpoint(ctx, "plugin/composednative/default/durable")
	if err != nil || persisted.Generation != checkpoint.Generation || string(persisted.Payload) != string(checkpoint.Payload) {
		t.Fatal("restart lost native checkpoint or stale lease mutated it")
	}
}

type nonpersistentNativeStore struct {
	*memory.Store
}

func (*nonpersistentNativeStore) Persistent() bool { return false }

func TestNativeCompositionRefusesNonpersistentCapabilityBeforeConstruction(t *testing.T) {
	for _, checkpointOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("checkpoint_only=%t", checkpointOnly), func(t *testing.T) {
			base := memory.New()
			var store driver.Store = &nonpersistentNativeStore{base}
			if checkpointOnly {
				// Expose the checkpoint interface without the stronger capability.
				store = struct {
					driver.Store
					driver.CheckpointStore
				}{base, base}
			}
			var constructed atomic.Bool
			native := composedNative()
			native.New = func(pluginapi.Host) (sdk.Registration, error) {
				constructed.Store(true)
				return sdk.Registration{}, nil
			}
			_, err := Open(context.Background(), Config{
				// A persistent-looking driver name must not manufacture capability.
				Driver: "sqlite", DSN: "unused", NativePlugins: []sdk.Native{native},
				StoreFactory: func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil },
			})
			var diagnostic *pluginhost.Error
			if !errors.As(err, &diagnostic) || diagnostic.Code != pluginhost.ErrDurableState || constructed.Load() {
				t.Fatalf("nonpersistent configured native startup=%v constructed=%t", err, constructed.Load())
			}
			if _, err := base.Meta(context.Background()); err == nil {
				t.Fatal("failed durable native admission left its store open")
			}
		})
	}
}

func TestFailedPersistentNativeConstructionReleasesStoreAndLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed-native.db")
	native := composedNative()
	native.New = func(pluginapi.Host) (sdk.Registration, error) {
		return sdk.Registration{}, errors.New("constructor refused")
	}
	config := Config{Driver: "sqlite", DSN: path, Address: DefaultAddress, NativePlugins: []sdk.Native{native}}
	if _, err := Open(context.Background(), config); err == nil {
		t.Fatal("failed native constructor was admitted")
	}
	config.NativePlugins = nil
	startRuntime(t, config) // Reopening the exact file must acquire a fresh lease.
}

func TestNativeConstructionUsesRuntimeCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	native := composedNative()
	native.NewWithContext = func(ctx context.Context, _ pluginapi.Host) (sdk.Registration, error) {
		close(entered)
		<-ctx.Done()
		return sdk.Registration{}, ctx.Err()
	}
	config := Config{Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "canceled-native.db"), Address: DefaultAddress, NativePlugins: []sdk.Native{native}}
	finished := make(chan error, 1)
	go func() { _, err := Open(ctx, config); finished <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("configured native constructor did not start")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime cancellation was not preserved through native admission: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("configured native construction ignored runtime cancellation")
	}
	config.NativePlugins = nil
	startRuntime(t, config)
}

func TestRuntimeCloseDrainsCanceledNativeCallbackBeforeStoreAndLease(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	native := composedNative()
	native.New = func(pluginapi.Host) (sdk.Registration, error) {
		return sdk.Registration{
			Mutators: map[string]pluginapi.Mutator{"normalize": sdk.MutateFunc(func(context.Context, pluginapi.ProposedChange, pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
				return nil, nil
			})},
			Posts: map[string]pluginapi.PostSubscriber{"observe": sdk.AfterFunc(func(ctx context.Context, _ pluginapi.CommittedChange, _ pluginapi.SnapshotReader) error {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release
				return ctx.Err()
			})},
		}, nil
	}
	var observed *nativeObservedStore
	config := Config{
		Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "native-drain.db"), Project: "native-drain", Address: DefaultAddress,
		NativePlugins:  []sdk.Native{native},
		NativeBindings: []pluginhost.Binding{{ID: "native-post", Alias: "composednative", Instance: "default", Subscriber: "observe", Phase: pluginapi.PhasePost, Reads: []string{"fields.title"}}},
		StoreFactory: func(ctx context.Context, _ string, config driver.Config) (driver.Store, error) {
			store, err := (sqlite.Driver{}).Open(ctx, config)
			if err != nil {
				return nil, err
			}
			observed = &nativeObservedStore{Store: store, PersistentCheckpointStore: store.(driver.PersistentCheckpointStore), ResponseFinalizer: store.(driver.ResponseFinalizer)}
			return observed, nil
		},
	}
	f := startRuntime(t, config)
	requestDone := make(chan error, 1)
	go func() {
		_, err := f.client.Call(context.Background(), "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "drain native"}}, "550e8400-e29b-41d4-a716-446655440001")
		requestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("admitted HTTP write did not enter native callback")
	}
	runtime := f.r
	closeCtx, cancel := context.WithCancel(context.Background())
	cancel()
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close(closeCtx) }()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("forced shutdown did not cancel native callback")
	}
	if observed.closed.Load() {
		t.Error("Runtime.Close closed SQLite while a native callback was still draining")
	}
	select {
	case <-runtime.Lease.Lost():
		t.Error("Runtime.Close released the lease before native callback drain")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-closed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("forced shutdown lost its canceled drain result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Runtime.Close did not join the native callback")
	}
	<-requestDone
	// The expected canceled shutdown result was checked above; prevent the
	// generic fixture cleanup from reporting the same expected result again.
	f.transport.Close()
	<-f.done
	f.r = nil
	if !observed.closed.Load() {
		t.Fatal("native callback drain left SQLite open")
	}
}

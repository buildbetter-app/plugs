package b01readyorder_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

const readyWorkflowCheckpoint = "plugin/workflow/default/durable"

// Faults follow an actual checkpoint read. Persistence is delegated to the
// selected store; the memory backend never acquires a false restart promise.
type readyCheckpointFault struct {
	driver.CheckpointStore
	reads atomic.Uint64
	fault error
}

func (s *readyCheckpointFault) Persistent() bool {
	store, ok := s.CheckpointStore.(driver.PersistentCheckpointStore)
	return ok && store.Persistent()
}

func (s *readyCheckpointFault) LoadCheckpoint(ctx context.Context, key string) (driver.Checkpoint, error) {
	checkpoint, err := s.CheckpointStore.LoadCheckpoint(ctx, key)
	if err != nil {
		return driver.Checkpoint{}, err
	}
	s.reads.Add(1)
	if s.fault != nil {
		return driver.Checkpoint{}, s.fault
	}
	return checkpoint, nil
}

func readyCheckpoint(t *testing.T, f *fixture) driver.Checkpoint {
	t.Helper()
	checkpoint, err := f.store.(driver.CheckpointStore).LoadCheckpoint(f.ctx, readyWorkflowCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func installReadyWorkflow(t *testing.T, f *fixture, checkpoints driver.CheckpointStore) {
	t.Helper()
	provider, err := workflowplugin.New(pluginhost.NewDurableState(checkpoints, f.lease, workflowplugin.Alias, "default"))
	if err != nil {
		t.Fatal(err)
	}
	provider.WithClock(f.clock.Now).WithIdentity(workflowplugin.Alias, "default", f.meta(t).Incarnation)
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(f.onto), pipeline.WithClock(f.clock.Now))
	f.svc = service.New(f.store, f.lease, writer, service.WithOntology(f.onto), service.WithClock(f.clock.Now), service.WithWorkflow(provider))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err := rpc.BindService(registry, f.svc); err != nil {
		t.Fatal(err)
	}
	f.server, err = rpc.New(registry, rpc.Config{DefaultProject: project, Incarnation: f.meta(t).Incarnation, CursorKey: []byte("b01readyorder-disposable-stable-key")})
	if err != nil {
		t.Fatal(err)
	}
}

func TestB01AbsentOptionalWorkflowKeepsCompleteDurablePages(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		f.seed(t, document("pl-first", f.clock.Now(), 0), document("pl-second", f.clock.Now(), 1))
		before := readyCheckpoint(t, f)
		if before.Generation != 0 || len(before.Payload) != 0 {
			t.Fatalf("optional workflow namespace was already present: %+v", before)
		}
		f.unchanged(t, func() { f.pages(t, nil, "pl-first", "pl-second") })
		if after := readyCheckpoint(t, f); !reflect.DeepEqual(after, before) {
			t.Fatal("reading without the optional provider created or changed its namespace")
		}
	})
}

func TestB01ConfiguredWorkflowFailureNeverReturnsPartialDurablePage(t *testing.T) {
	eachStore(t, func(t *testing.T, f *fixture) {
		// Two eligible durable rows ensure limit one cannot bypass the provider
		// by returning an already available durable continuation.
		f.seed(t, document("pl-first", f.clock.Now(), 0), document("pl-second", f.clock.Now(), 1))
		before := readyCheckpoint(t, f)
		checkpoints := &readyCheckpointFault{CheckpointStore: f.store.(driver.CheckpointStore)}
		installReadyWorkflow(t, f, checkpoints)
		if checkpoints.Persistent() {
			t.Run("empty-checkpoint", func(t *testing.T) {
				f.unchanged(t, func() { f.pages(t, nil, "pl-first", "pl-second") })
				if checkpoints.reads.Load() == 0 {
					t.Fatal("configured provider did not read the actual empty checkpoint")
				}
			})
		}
		fault := errors.New("configured workflow checkpoint query failed")
		checkpoints.fault = fault
		priorReads := checkpoints.reads.Load()
		f.unchanged(t, func() {
			page, err := f.svc.Ready(f.ctx, anonymous, service.ReadyRequest{Project: project, Limit: 1})
			if err == nil || len(page.Items) != 0 || page.Next != nil {
				t.Fatalf("provider failure became a partial durable page: %+v %v", page, err)
			}
			if checkpoints.Persistent() {
				if !errors.Is(err, fault) {
					t.Fatalf("configured query error was not preserved: %v", err)
				}
			} else {
				var unavailable *pluginhost.Error
				if !errors.As(err, &unavailable) || unavailable.Code != pluginhost.ErrDurableState {
					t.Fatalf("nonpersistent provider lost its typed capability refusal: %v", err)
				}
			}
			response := f.call(t, "bead.ready", map[string]any{"limit": 1})
			if response.Error == nil || response.Error.Data.Code != "internal_error" || len(response.Result) != 0 {
				t.Fatalf("provider failure became public partial success: %+v", response)
			}
		})
		wantReads := priorReads
		if checkpoints.Persistent() {
			wantReads += 2
		}
		if got := checkpoints.reads.Load(); got != wantReads {
			t.Fatalf("provider failure reached %d checkpoint reads; want %d", got, wantReads)
		}
		if after := readyCheckpoint(t, f); !reflect.DeepEqual(after, before) {
			t.Fatal("provider refusal or empty-namespace read changed its checkpoint")
		}
	})
}

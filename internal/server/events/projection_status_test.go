package events

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	core "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type statusProjector func(context.Context, core.ChangeDelivery) error

func (statusProjector) Apply(context.Context, core.ChangeDelivery) error { return nil }

type statusFailingProjector struct{}

func (statusFailingProjector) Apply(context.Context, core.ChangeDelivery) error {
	return errors.New("projection failed")
}

type statusHead struct{ head core.Head }

func (l statusHead) Head(context.Context) (core.Head, error) { return l.head, nil }
func (statusHead) Scan(context.Context, core.Position, int) ([]model.Changeset, error) {
	return nil, errors.New("status test log scan is not used")
}

func statusRegistry(t *testing.T, projector core.Projector) *core.ProjectionRegistry {
	t.Helper()
	registry := core.NewProjectionRegistry()
	if err := registry.RegisterProjection(core.Binding{ID: "core", Class: core.Projection, Projector: projector}); err != nil {
		t.Fatal(err)
	}
	return registry
}

func statusAdapter(t *testing.T, f *fixture, registry *core.ProjectionRegistry) *ProjectionStatusAdapter {
	t.Helper()
	checkpoints := f.checkpoints(t)
	adapter, err := NewProjectionStatusReader(ProjectionStatusConfig{
		Checkpoints: checkpoints,
		Log:         Log{Store: f.store},
		Lease:       f.lease,
		Registry:    registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func TestProjectionStatusReadsRealCheckpointAfterDeliveryAndFailure(t *testing.T) {
	f := newFixture(t)
	f.commit(t, time.Time{}, bead("pl-status", "status"))
	registry := statusRegistry(t, statusProjector(nil))
	adapter := statusAdapter(t, f, registry)
	initial, err := adapter.Status(f.ctx, "core")
	if err != nil || initial.Position.Revision.String() != "0" || initial.Position.Incarnation != f.incarnation {
		t.Fatalf("initial status=%+v err=%v", initial, err)
	}
	dispatcher := f.dispatcher(t)
	binding, ok := registry.Get("core")
	if !ok {
		t.Fatal("registered projection missing")
	}
	if _, err := dispatcher.Step(f.ctx, binding); err != nil {
		t.Fatal(err)
	}
	status, err := adapter.Status(f.ctx, "core")
	if err != nil || status.Position.Revision.String() != "1" || status.Halted || status.Failure != nil || status.Resume != nil {
		t.Fatalf("delivered status=%+v err=%v", status, err)
	}

	f.commit(t, time.Time{}, bead("pl-status-second", "second"))
	failedRegistry := statusRegistry(t, statusFailingProjector{})
	failedAdapter := statusAdapter(t, f, failedRegistry)
	failedBinding, _ := failedRegistry.Get("core")
	if _, err := dispatcher.Step(f.ctx, failedBinding); err == nil {
		t.Fatal("failed projection unexpectedly succeeded")
	}
	failed, err := failedAdapter.Status(f.ctx, "core")
	if err != nil || !failed.Halted || failed.Failure == nil || failed.Failure.Attempts != 1 || failed.Resume == nil || failed.Resume.Position.Revision.String() != "1" {
		t.Fatalf("failed status=%+v err=%v", failed, err)
	}
	failed.Failure.Reason = "caller mutation"
	reloaded, err := failedAdapter.Status(f.ctx, "core")
	if err != nil || reloaded.Failure == nil || reloaded.Failure.Reason == "caller mutation" {
		t.Fatalf("status snapshot was aliased: %+v err=%v", reloaded, err)
	}
}

func TestProjectionStatusReadsPersistedStallAndRefusesLostLease(t *testing.T) {
	f := newFixture(t)
	registry := statusRegistry(t, statusProjector(nil))
	checkpoints := f.checkpoints(t)
	if _, err := checkpoints.Save(f.ctx, "core", 0, core.State{
		Class:              core.Projection,
		Position:           core.Position{Incarnation: f.incarnation, Revision: "3"},
		Stalled:            true,
		LagAcceptedThrough: "2",
	}); err != nil {
		t.Fatal(err)
	}
	adapter := statusAdapter(t, f, registry)
	status, err := adapter.Status(f.ctx, "core")
	if err != nil || !status.Stalled || status.Resume == nil || status.Resume.Position.Revision.String() != "3" || status.LagAcceptedThrough.String() != "2" {
		t.Fatalf("stalled status=%+v err=%v", status, err)
	}
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	var fenced driver.ErrFenced
	if _, err := adapter.Status(f.ctx, "core"); !errors.As(err, &fenced) {
		t.Fatalf("lost lease status error=%v", err)
	}
}

func TestProjectionStatusRefusesCheckpointFromAnotherIncarnation(t *testing.T) {
	f := newFixture(t)
	registry := statusRegistry(t, statusProjector(nil))
	checkpoints := f.checkpoints(t)
	if _, err := checkpoints.Save(f.ctx, "core", 0, core.State{
		Class:    core.Projection,
		Position: core.Position{Incarnation: "old-incarnation", Revision: "1"},
	}); err != nil {
		t.Fatal(err)
	}
	adapter, err := NewProjectionStatusReader(ProjectionStatusConfig{
		Checkpoints: checkpoints,
		Log:         statusHead{head: core.Head{Incarnation: f.incarnation}},
		Lease:       f.lease,
		Registry:    registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	var changed *driver.ErrIncarnationChanged
	if _, err := adapter.Status(f.ctx, "core"); !errors.As(err, &changed) || changed.From != "old-incarnation" || changed.To != f.incarnation {
		t.Fatalf("incarnation error=%T %v", err, err)
	}
}

func TestProjectionStatusSurvivesSQLiteRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "projection-status.db")
	first, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := first.Lease(ctx, "projection-status-first", driver.LeaseTTL)
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	meta, err := first.Meta(ctx)
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	checkpoints, err := NewCheckpoints(first, lease)
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	if _, err := checkpoints.Save(ctx, "core", 0, core.State{Class: core.Projection, Position: core.Position{Incarnation: meta.Incarnation, Revision: "7"}, Halted: true}); err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondLease, err := second.Lease(ctx, "projection-status-second", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer secondLease.Release(ctx)
	secondCheckpoints, err := NewCheckpoints(second, secondLease)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewProjectionStatusReader(ProjectionStatusConfig{
		Checkpoints: secondCheckpoints,
		Log:         Log{Store: second},
		Lease:       secondLease,
		Registry:    statusRegistry(t, statusProjector(nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := adapter.Status(ctx, "core")
	if err != nil || !status.Halted || status.Position.Revision.String() != "7" || status.Position.Incarnation != meta.Incarnation {
		t.Fatalf("restarted status=%+v err=%v", status, err)
	}
}

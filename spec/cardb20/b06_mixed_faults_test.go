package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// These adapters inject only a read-boundary fault into the real installed
// host checkpoint port. They cannot fabricate membership or a positive report.
type b06CycleState struct {
	pluginapi.DurableState
	fault string
}

func (s b06CycleState) Persistent() bool {
	return s.DurableState.(interface{ Persistent() bool }).Persistent()
}
func (s b06CycleState) WorkflowCheckpointLifetime() driver.CheckpointLifetime {
	return s.DurableState.(interface {
		WorkflowCheckpointLifetime() driver.CheckpointLifetime
	}).WorkflowCheckpointLifetime()
}
func (s b06CycleState) ReadWorkflowRuntimeCheckpoint(ctx context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error) {
	raw, revision, head, namespace, err := s.DurableState.(interface {
		ReadWorkflowRuntimeCheckpoint(context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error)
	}).ReadWorkflowRuntimeCheckpoint(ctx)
	if err != nil {
		return raw, revision, head, namespace, err
	}
	switch s.fault {
	case "unsupported":
		err = driver.ErrUnsupported
	case "head":
		head.Head = model.Natural64(999999)
	case "store":
		head.StoreID = "b06-other-store"
		namespace.StoreID = head.StoreID
	case "incarnation":
		head.Incarnation = "b06-other-incarnation"
		namespace.Incarnation = head.Incarnation
	case "namespace-store":
		namespace.StoreID = "b06-other-store"
	case "namespace-key":
		namespace.Key = "plugin/workflow/other/durable"
	case "malformed":
		raw = json.RawMessage(`{"runs":`)
	case "incomplete":
		raw = json.RawMessage(`{"runs":{},"items":{}}`)
	case "trailing":
		raw = append(raw, []byte(` {}`)...)
	}
	return raw, revision, head, namespace, err
}

type b06LegacyCycleState struct{ pluginapi.DurableState }

func (s b06LegacyCycleState) Persistent() bool {
	return s.DurableState.(interface{ Persistent() bool }).Persistent()
}

type b06SnapshotCloseStore struct {
	driver.WorkflowCheckpointStore
	meta              func(context.Context) (driver.Meta, error)
	fail              bool
	reads             int
	closes            int
	readErr, closeErr error
}

func (s *b06SnapshotCloseStore) Meta(ctx context.Context) (driver.Meta, error) { return s.meta(ctx) }
func (s *b06SnapshotCloseStore) ReadSourcePlaneSnapshot(ctx context.Context, view driver.View, keys []string) (driver.SourcePlaneSnapshot, error) {
	r, err := s.WorkflowCheckpointStore.ReadSourcePlaneSnapshot(ctx, view, keys)
	if err != nil || !s.fail {
		return r, err
	}
	s.reads++
	return &b06SnapshotCloseReader{SourcePlaneSnapshot: r, owner: s}, nil
}

type b06SnapshotCloseReader struct {
	driver.SourcePlaneSnapshot
	owner *b06SnapshotCloseStore
}

func (r *b06SnapshotCloseReader) SourcePlaneCheckpoint(key string) (driver.Checkpoint, error) {
	if r.owner.readErr != nil {
		return driver.Checkpoint{}, r.owner.readErr
	}
	return r.SourcePlaneSnapshot.SourcePlaneCheckpoint(key)
}
func (r *b06SnapshotCloseReader) Close() error {
	r.owner.closes++
	return errors.Join(r.SourcePlaneSnapshot.Close(), r.owner.closeErr)
}

type b06ContentCloseStore struct {
	driver.Store
	closes int
}

func (s *b06ContentCloseStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &b06ContentCloseReader{Reader: r, owner: s}, nil
}

type b06ContentCloseReader struct {
	driver.Reader
	owner *b06ContentCloseStore
}

func (r *b06ContentCloseReader) CycleTopology(ctx context.Context, cursor *driver.Cursor, limit int) (driver.Page[driver.CycleTopologyEdge], error) {
	return r.Reader.(driver.CycleTopologyReader).CycleTopology(ctx, cursor, limit)
}
func (r *b06ContentCloseReader) Close() error {
	r.owner.closes++
	return errors.Join(r.Reader.Close(), errors.New("b06 content close fault"))
}

func b06BindCycleState(f *b06PlugFixture, state pluginapi.DurableState) {
	f.t.Helper()
	provider, err := workflowplugin.New(state)
	if err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	provider.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)
	service.WithWorkflow(provider)(f.domain)
}

func b06MixedReadFaults(f *b06PlugFixture) {
	f.t.Helper()
	ctx := context.Background()
	store, err := driver.RequireWorkflowCheckpointStore(f.store, f.lease)
	if err != nil {
		f.t.Fatal(err)
	}
	state, err := pluginhost.NewWorkflowRuntimeState(store, f.lease)
	if err != nil {
		f.t.Fatal(err)
	}
	// The live driver has a truthful marker. Stripping both markers must
	// still refuse invocation, even when the underlying driver is persistent.
	b06BindCycleState(f, struct{ pluginapi.DurableState }{state})
	f.writes["x.workflow.invoke/"+fmt.Sprint(f.requests+1)] = "x.workflow.invoke"
	beforeMarker := b06MixedImage(f, "before-missing-lifetime-marker")
	refused := f.call("x.workflow.invoke", map[string]any{"template": "b06", "project": "b06", "mode": "ephemeral", "checklist": []map[string]any{{"label": "refused", "title": "refused"}}}, true)
	if refused.Error == nil || refused.Error.Data.Code != "plugin_durable_state_unavailable" || len(refused.Result) != 0 {
		f.t.Fatalf("missing lifetime/persistence marker did not refuse %+v", refused)
	}
	if !bytes.Equal(beforeMarker, b06MixedImage(f, "after-missing-lifetime-marker")) {
		f.t.Fatal("marker refusal published state or receipt")
	}
	for _, fault := range []string{"missing-capability", "unsupported", "head", "store", "incarnation", "namespace-store", "namespace-key", "malformed", "incomplete", "trailing"} {
		f.t.Run("snapshot-"+fault, func(t *testing.T) {
			previous := f.t
			f.t = t
			defer func() { f.t = previous }()
			before := b06MixedImage(f, "before-snapshot-"+fault)
			if fault == "missing-capability" {
				b06BindCycleState(f, b06LegacyCycleState{state})
			} else {
				b06BindCycleState(f, b06CycleState{state, fault})
			}
			code := "write_conflict"
			switch fault {
			case "missing-capability", "unsupported":
				code = "plugin_durable_state_unavailable"
			case "malformed", "incomplete", "trailing":
				code = "internal_error"
			}
			got := f.call("bead.cycles", map[string]any{"includeEphemeral": true, "includeTracks": true}, false)
			if got.Error == nil || string(got.Error.Data.Code) != code || got.Error.Data.Retryable != (code == "write_conflict") || len(got.Result) != 0 {
				t.Fatalf("snapshot %s published report or wrong refusal: %+v want %s", fault, got, code)
			}
			if !bytes.Equal(before, b06MixedImage(f, "after-snapshot-"+fault)) {
				t.Fatal("failed snapshot changed either plane, history or receipts")
			}
		})
	}
	for _, readFailure := range []bool{false, true} {
		label := fmt.Sprintf("joint-close-read-failure-%t", readFailure)
		boundary := &b06SnapshotCloseStore{WorkflowCheckpointStore: store, meta: f.store.Meta, fail: true, closeErr: errors.New("b06 joint close fault")}
		if readFailure {
			boundary.readErr = errors.New("b06 joint read fault")
		}
		wrapped, err := pluginhost.NewWorkflowRuntimeState(boundary, f.lease)
		if err != nil {
			f.t.Fatal(err)
		}
		b06BindCycleState(f, wrapped)
		before := b06MixedImage(f, "before-"+label)
		got := f.call("bead.cycles", map[string]any{"includeEphemeral": true, "includeTracks": true}, false)
		if got.Error == nil || got.Error.Data.Code != "internal_error" || len(got.Result) != 0 || boundary.reads != 1 || boundary.closes != 1 {
			f.t.Fatalf("joint read/close failure published report or wrong lifecycle: %+v reads%d closes%d", got, boundary.reads, boundary.closes)
		}
		// Inspect only the same read-only host port to prove both error causes
		// survive and that none of the failed observation can be consumed.
		raw, revision, head, namespace, err := wrapped.(interface {
			ReadWorkflowRuntimeCheckpoint(context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error)
		}).ReadWorkflowRuntimeCheckpoint(ctx)
		if !errors.Is(err, boundary.closeErr) || readFailure && !errors.Is(err, boundary.readErr) || len(raw) != 0 || revision != "" || head != (driver.CoreSnapshotExpectation{}) || namespace != (driver.CheckpointExpectation{}) {
			f.t.Fatalf("failed host snapshot lost cause or retained usable output: %s %s %+v %+v %v", raw, revision, head, namespace, err)
		}
		if !bytes.Equal(before, b06MixedImage(f, "after-"+label)) {
			f.t.Fatal("failed joint close changed committed state")
		}
	}
	// The ordinary content reader has a separate final close boundary.
	b06BindCycleState(f, state)
	wrappedStore := &b06ContentCloseStore{Store: f.store}
	o := service.NewOntology(nil)
	domain := service.New(wrappedStore, f.lease, pipeline.New(f.store, f.lease, pipeline.WithOntology(o)), service.WithOntology(o))
	provider, err := workflowplugin.New(state)
	if err != nil {
		f.t.Fatal(err)
	}
	service.WithWorkflow(provider)(domain)
	registry, err := rpc.NewRegistry()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := rpc.BindService(registry, domain); err != nil {
		f.t.Fatal(err)
	}
	meta, err := f.store.Meta(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "b06", Incarnation: meta.Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	before := b06MixedImage(f, "before-content-close")
	original := f.server
	f.server = server
	got := f.call("bead.cycles", map[string]any{"includeEphemeral": true, "includeTracks": true}, false)
	f.server = original
	if got.Error == nil || got.Error.Data.Code != "internal_error" || len(got.Result) != 0 || wrappedStore.closes != 1 {
		f.t.Fatalf("content close failure published report: %+v closes%d", got, wrappedStore.closes)
	}
	if !bytes.Equal(before, b06MixedImage(f, "after-content-close")) {
		f.t.Fatal("failed content close changed state")
	}
	b06BindCycleState(f, state)
}

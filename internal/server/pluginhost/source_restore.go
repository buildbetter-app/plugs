package pluginhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

const (
	sourceStateKey  = "workflow/state"
	sourceNamespace = "plugin/workflow/default/durable"
)

// NewSourceRuntimeState selects a restored namespace on the actual store. The
// origin is verified before exposing state and on every subsequent operation.
// A process lifetime does not grant the legacy restart-durable capability.
func NewSourceRuntimeState(ctx context.Context, store driver.WorkflowCheckpointStore, lease driver.ActiveLease, binding driver.SourcePlaneProviderCommitV1) (pluginapi.DurableState, error) {
	if store == nil || lease == nil {
		return nil, driver.ErrUnsupported
	}
	if err := validateSourceProvider(binding.Provider); err != nil {
		return nil, err
	}
	if err := validateSourceOrigin(binding.Origin); err != nil {
		return nil, err
	}
	if !sourceDigest(binding.EnvelopeSHA256) || binding.Origin.LogicalImageSHA256 != binding.Provider.LogicalImageSHA256 || binding.Origin.Lifetime != binding.Provider.Lifetime || store.WorkflowCheckpointLifetime() != binding.Provider.Lifetime {
		return nil, driver.ErrUnsupported
	}
	d := newDurableAccess(store, lease, "workflow", "default")
	d.source = &binding
	if _, _, _, err := d.loadSource(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

func sourceDigest(value string) bool {
	return len(value) == 64 && strings.Trim(value, "0123456789abcdef") == ""
}

func sourceHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validateSourceProvider(p driver.SourcePlaneProviderV1) error {
	if p.Alias != "workflow" || p.Instance != "default" || p.Namespace != sourceNamespace || p.Schema != "workflow/source-state/1" || p.Codec != "workflow/json/1" || p.OriginAdapter != "beads-workflow-origins/1" || p.RuntimeContract != driver.SourcePlaneRuntimeContractV1 || p.ExpectedGeneration != 0 || !sourceDigest(p.LogicalImageSHA256) {
		return driver.ErrUnsupported
	}
	return p.Lifetime.Validate()
}

func validateSourceOrigin(origin driver.SourcePlaneOriginBindingV1) error {
	if origin.Version != driver.SourcePlaneOriginVersionV1 || !sourceDigest(origin.JointPlanDigest) || !sourceDigest(origin.RequestHash) || driver.ValidateCheckpointKey(string(origin.StoreID)) != nil || driver.ValidateCheckpointKey(origin.Incarnation) != nil || origin.Namespace != sourceNamespace || origin.InitialGeneration != 1 || !sourceDigest(origin.LogicalImageSHA256) {
		return driver.ErrUnsupported
	}
	return origin.Lifetime.Validate()
}

func (d *durableAccess) sourceKey(key string) error {
	if d != nil && d.source != nil && key != sourceStateKey {
		return driver.ErrUnsupported
	}
	return nil
}

func (d *durableAccess) sourceMeta(meta driver.Meta) error {
	if d.source != nil && (meta.StoreID != d.source.Origin.StoreID || meta.Incarnation != d.source.Origin.Incarnation) {
		return driver.ErrIncarnationChanged{From: d.source.Origin.Incarnation, To: meta.Incarnation}
	}
	return nil
}

// Publication preparation only validates the previously consumed namespace.
// It may run inside the original writer's held Head, so it must not open a
// second graph/writer snapshot. The original transaction owns every final CAS.
func (d *durableAccess) loadForPreparation(ctx context.Context) (driver.Checkpoint, durableNamespace, error) {
	if d.source == nil {
		return d.load(ctx)
	}
	if err := d.available(ctx); err != nil {
		return driver.Checkpoint{}, durableNamespace{}, err
	}
	checkpoint, err := d.store.LoadCheckpoint(ctx, d.key)
	if err != nil {
		return driver.Checkpoint{}, durableNamespace{}, err
	}
	namespace, err := decodeSourceNamespace(checkpoint)
	if err != nil {
		return driver.Checkpoint{}, durableNamespace{}, err
	}
	if *namespace.Origin != d.source.Origin || (checkpoint.Generation == d.source.Origin.InitialGeneration && sourceHash(checkpoint.Payload) != d.source.EnvelopeSHA256) {
		return driver.Checkpoint{}, durableNamespace{}, driver.ErrCheckpointConflict
	}
	return checkpoint, namespace, nil
}

// SourceRuntimeBinding is metadata from the trusted constructor, not authority
// supplied by a plugin. The namespace itself is checked again by each read/CAS.
func (d *durableAccess) SourceRuntimeBinding() (driver.SourcePlaneProviderCommitV1, error) {
	if d == nil || d.source == nil {
		return driver.SourcePlaneProviderCommitV1{}, driver.ErrUnsupported
	}
	return *d.source, nil
}

func (d *durableStateAccess) SourceRuntimeBinding() (driver.SourcePlaneProviderCommitV1, error) {
	return d.instance.durable.SourceRuntimeBinding()
}

// ReadSourceRuntime is a trusted staging seam. It detaches one logical image
// and its namespace revision from the very same pinned core observation.
// Call before acquiring the ordinary writer's Head, never from inside it.
func (d *durableAccess) ReadSourceRuntime(ctx context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, error) {
	logical, revision, head, _, err := d.ReadSourceRuntimeCheckpoint(ctx)
	return logical, revision, head, err
}

// ReadSourceRuntimeCheckpoint carries both guards from the very same pinned
// image. Staged ordinary guards must not reload the original provider after
// their caller has acquired the writer Head.
func (d *durableAccess) ReadSourceRuntimeCheckpoint(ctx context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error) {
	checkpoint, namespace, head, err := d.loadSource(ctx)
	if err != nil {
		return nil, "", driver.CoreSnapshotExpectation{}, driver.CheckpointExpectation{}, err
	}
	value := namespace.Values[sourceStateKey]
	expectation := driver.CheckpointExpectation{StoreID: head.StoreID, Incarnation: head.Incarnation, Key: d.key, Expected: checkpoint.Generation}
	return append(json.RawMessage(nil), value.Value...), pluginapi.Natural64(value.Revision), head, expectation, nil
}

func (d *durableStateAccess) ReadSourceRuntime(ctx context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, error) {
	ctx, cancel, err := d.context(ctx, false)
	if err != nil {
		return nil, "", driver.CoreSnapshotExpectation{}, err
	}
	defer cancel()
	return d.instance.durable.ReadSourceRuntime(ctx)
}

func (d *durableStateAccess) ReadSourceRuntimeCheckpoint(ctx context.Context) (json.RawMessage, pluginapi.Revision, driver.CoreSnapshotExpectation, driver.CheckpointExpectation, error) {
	ctx, cancel, err := d.context(ctx, false)
	if err != nil {
		return nil, "", driver.CoreSnapshotExpectation{}, driver.CheckpointExpectation{}, err
	}
	defer cancel()
	return d.instance.durable.ReadSourceRuntimeCheckpoint(ctx)
}

func sourceObject(data []byte) error {
	var object map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&object); err != nil || object == nil {
		return fmt.Errorf("source runtime requires a JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("source runtime has trailing JSON")
	}
	return nil
}

func decodeSourceNamespace(checkpoint driver.Checkpoint) (durableNamespace, error) {
	var ns durableNamespace
	if checkpoint.Generation == 0 || len(checkpoint.Payload) == 0 {
		return ns, driver.ErrCheckpointConflict
	}
	decoder := json.NewDecoder(bytes.NewReader(checkpoint.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ns); err != nil {
		return ns, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ns, fmt.Errorf("source namespace has trailing JSON")
	}
	if ns.Origin == nil || len(ns.Values) != 1 {
		return ns, driver.ErrCheckpointConflict
	}
	if err := validateSourceOrigin(*ns.Origin); err != nil {
		return ns, err
	}
	value, ok := ns.Values[sourceStateKey]
	if !ok || value.Revision == 0 || checkpoint.Generation < ns.Origin.InitialGeneration {
		return ns, driver.ErrCheckpointConflict
	}
	if err := sourceObject(value.Value); err != nil {
		return ns, err
	}
	if checkpoint.Generation == ns.Origin.InitialGeneration && (value.Revision != 1 || sourceHash(value.Value) != ns.Origin.LogicalImageSHA256) {
		return ns, driver.ErrCheckpointConflict
	}
	return ns, nil
}

// loadSource pins the graph and namespace together. Even a checkpoint-only
// update uses this graph head for its final CAS; a new incarnation cannot
// replace the namespace underneath a retained process-lifetime facade.
func (d *durableAccess) loadSource(ctx context.Context) (driver.Checkpoint, durableNamespace, driver.CoreSnapshotExpectation, error) {
	var empty durableNamespace
	var head driver.CoreSnapshotExpectation
	if err := d.available(ctx); err != nil {
		return driver.Checkpoint{}, empty, head, err
	}
	store, ok := d.store.(driver.WorkflowCheckpointStore)
	metadata, metaOK := d.store.(interface {
		Meta(context.Context) (driver.Meta, error)
	})
	if !ok || !metaOK || d.source == nil || store.WorkflowCheckpointLifetime() != d.source.Origin.Lifetime {
		return driver.Checkpoint{}, empty, head, driver.ErrUnsupported
	}
	meta, err := metadata.Meta(ctx)
	if err != nil {
		return driver.Checkpoint{}, empty, head, err
	}
	origin := d.source.Origin
	if meta.StoreID != origin.StoreID || meta.Incarnation != origin.Incarnation {
		return driver.Checkpoint{}, empty, head, driver.ErrIncarnationChanged{From: origin.Incarnation, To: meta.Incarnation}
	}
	snapshot, err := store.ReadSourcePlaneSnapshot(ctx, driver.View{Branch: model.Natural64(0), Incarnation: origin.Incarnation}, []string{d.key})
	if err != nil {
		return driver.Checkpoint{}, empty, head, err
	}
	defer snapshot.Close()
	view := snapshot.Snapshot()
	if view.At == nil || view.Incarnation != origin.Incarnation || view.Branch.String() != "0" {
		return driver.Checkpoint{}, empty, head, driver.ErrCheckpointConflict
	}
	head = driver.CoreSnapshotExpectation{StoreID: origin.StoreID, Incarnation: origin.Incarnation, Head: *view.At}
	checkpoint, err := snapshot.SourcePlaneCheckpoint(d.key)
	if err != nil {
		return driver.Checkpoint{}, empty, head, err
	}
	namespace, err := decodeSourceNamespace(checkpoint)
	if err != nil {
		return driver.Checkpoint{}, empty, head, err
	}
	if *namespace.Origin != origin || (checkpoint.Generation == origin.InitialGeneration && sourceHash(checkpoint.Payload) != d.source.EnvelopeSHA256) {
		return driver.Checkpoint{}, empty, head, driver.ErrCheckpointConflict
	}
	if err := d.available(ctx); err != nil {
		return driver.Checkpoint{}, empty, head, err
	}
	return checkpoint, namespace, head, nil
}

// PrepareRestoreCheckpoint is deliberately data-only. The restore owner stages
// this exact mutation in the transaction that publishes newMeta, core records,
// provenance and the receipt. It does not save any checkpoint here.
func (d *durableAccess) PrepareRestoreCheckpoint(ctx context.Context, newMeta driver.Meta, profile driver.RestoreImportProfile, provider driver.SourcePlaneProviderV1, logical []byte) (driver.PreparedSourcePlaneCheckpoint, error) {
	var empty driver.PreparedSourcePlaneCheckpoint
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if d == nil || d.store == nil || d.lease == nil || d.key != sourceNamespace {
		return empty, driver.ErrUnsupported
	}
	select {
	case <-d.lease.Lost():
		return empty, failure(ErrDurable, "storage lease is lost")
	default:
	}
	store, ok := d.store.(driver.WorkflowCheckpointStore)
	metadata, metaOK := d.store.(interface {
		Meta(context.Context) (driver.Meta, error)
	})
	if !ok || !metaOK {
		return empty, driver.ErrUnsupported
	}
	detached, err := profile.Detached()
	if err != nil {
		return empty, err
	}
	if detached.SourcePlanes == nil {
		return empty, driver.ErrUnsupported
	}
	plan, err := detached.SourcePlanes.Plan()
	if err != nil {
		return empty, err
	}
	if err := validateSourceProvider(provider); err != nil {
		return empty, err
	}
	if store.WorkflowCheckpointLifetime() != provider.Lifetime || plan.Target.Lifetime != provider.Lifetime || sourceHash(logical) != provider.LogicalImageSHA256 || uint64(len(logical)) != provider.LogicalImageSize {
		return empty, driver.ErrUnsupported
	}
	if err := sourceObject(logical); err != nil {
		return empty, err
	}
	found := false
	for _, participant := range plan.Providers {
		if participant.Namespace == d.key {
			found = participant == provider
		}
	}
	if !found {
		return empty, driver.ErrUnsupported
	}
	current, err := metadata.Meta(ctx)
	if err != nil {
		return empty, err
	}
	if string(current.StoreID) != plan.Target.StoreID || current.Incarnation != plan.Target.Incarnation || current.Head.String() != fmt.Sprint(plan.Target.Head) || !plan.Target.RequireEmpty {
		return empty, driver.ErrCheckpointConflict
	}
	if newMeta.StoreID != detached.SourceStoreID || string(newMeta.StoreID) != plan.SourceStoreID || driver.ValidateCheckpointKey(newMeta.Incarnation) != nil || newMeta.Incarnation == current.Incarnation || newMeta.Head.String() != "1" || newMeta.HistoryFloor.String() != "1" {
		return empty, driver.ErrIncarnationChanged{From: current.Incarnation, To: newMeta.Incarnation}
	}
	checkpoint, err := store.LoadCheckpoint(ctx, d.key)
	if err != nil {
		return empty, err
	}
	if checkpoint.Generation != 0 || len(checkpoint.Payload) != 0 {
		return empty, driver.ErrCheckpointConflict
	}
	requestHash, err := detached.RequestHash()
	if err != nil {
		return empty, err
	}
	origin := driver.SourcePlaneOriginBindingV1{Version: driver.SourcePlaneOriginVersionV1, JointPlanDigest: detached.SourcePlanes.JointPlanDigest, RequestHash: requestHash, StoreID: newMeta.StoreID, Incarnation: newMeta.Incarnation, Namespace: d.key, InitialGeneration: 1, LogicalImageSHA256: provider.LogicalImageSHA256, Lifetime: provider.Lifetime}
	namespace := durableNamespace{Values: map[string]durableValue{sourceStateKey: {Revision: 1, Value: append(json.RawMessage(nil), logical...)}}, Origin: &origin}
	payload, err := json.Marshal(namespace)
	if err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return driver.PreparedSourcePlaneCheckpoint{Mutation: driver.CheckpointMutation{StoreID: newMeta.StoreID, Incarnation: newMeta.Incarnation, Key: d.key, Expected: 0, Payload: payload}, Binding: driver.SourcePlaneProviderCommitV1{Provider: provider, Origin: origin, EnvelopeSHA256: sourceHash(payload)}}, nil
}

// InspectSourcePlaneOrigin checks the complete envelope while returning the
// current payload, which may legitimately differ from the initial image.
func (d *durableAccess) InspectSourcePlaneOrigin(checkpoint driver.Checkpoint) (driver.SourcePlaneOriginBindingV1, json.RawMessage, error) {
	namespace, err := decodeSourceNamespace(checkpoint)
	if err != nil {
		return driver.SourcePlaneOriginBindingV1{}, nil, err
	}
	return *namespace.Origin, append(json.RawMessage(nil), namespace.Values[sourceStateKey].Value...), nil
}

var _ driver.SourcePlaneCheckpointPreparer = (*durableAccess)(nil)
var _ driver.SourcePlaneOriginReader = (*durableAccess)(nil)

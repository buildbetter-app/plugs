package pluginhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// durableStateAccess is the plugin-facing facade. It can be retained by a
// constructor, but every operation needs the authenticated context of a live
// host-driven call. The raw namespace adapter below remains available only to
// trusted composition roots, which do not execute as plugin callbacks.
type durableStateAccess struct{ instance *instance }

var _ pluginapi.DurableState = (*durableStateAccess)(nil)

// Persistent is capability metadata, safe to inspect during construction.
func (d *durableStateAccess) Persistent() bool { return d.instance.durable.Persistent() }

func (d *durableStateAccess) context(ctx context.Context, write bool) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	i := d.instance
	i.owner.mu.Lock()
	closed := i.owner.closed
	i.owner.mu.Unlock()
	if closed || i.isDisposed() {
		return nil, nil, failure(ErrInactive, "plugin host or instance is closed")
	}
	call, err := i.current()
	if err != nil {
		return nil, nil, err
	}
	if ctx.Value(invocationKey{}) != call {
		return nil, nil, failure(ErrInactive, "durable state requires the current host-driven invocation context")
	}
	if err := call.stateAvailable(write); err != nil {
		return nil, nil, err
	}
	// Always retain the host's deadline/cancellation even when plugin code
	// supplies context.WithoutCancel(callCtx). Caller cancellation may only
	// narrow that lifetime; it can never replace or extend it.
	operation, cancel := context.WithCancel(call.ctx)
	stop := context.AfterFunc(ctx, cancel)
	return operation, func() { stop(); cancel() }, nil
}

func (d *durableStateAccess) Read(ctx context.Context, key string, dst any) (bool, pluginapi.Revision, error) {
	ctx, cancel, err := d.context(ctx, false)
	if err != nil {
		return false, "", err
	}
	defer cancel()
	return d.instance.durable.Read(ctx, key, dst)
}

func (d *durableStateAccess) Get(ctx context.Context, key string, dst any) (bool, error) {
	found, _, err := d.Read(ctx, key, dst)
	return found, err
}

func (d *durableStateAccess) Revision(ctx context.Context, key string) (pluginapi.Revision, error) {
	ctx, cancel, err := d.context(ctx, false)
	if err != nil {
		return "", err
	}
	defer cancel()
	return d.instance.durable.Revision(ctx, key)
}

func (d *durableStateAccess) Put(ctx context.Context, key string, value any, expect pluginapi.Revision) (pluginapi.Revision, error) {
	ctx, cancel, err := d.context(ctx, true)
	if err != nil {
		return "", err
	}
	defer cancel()
	return d.instance.durable.Put(ctx, key, value, expect)
}

// PutAtHead is a trusted workflow bridge, absent from pluginapi.DurableState.
// Native calls retain the same live invocation and cancellation checks.
func (d *durableStateAccess) PutAtHead(ctx context.Context, key string, value any, expect pluginapi.Revision, head driver.CoreSnapshotExpectation) (pluginapi.Revision, error) {
	ctx, cancel, err := d.context(ctx, true)
	if err != nil {
		return "", err
	}
	defer cancel()
	return d.instance.durable.PutAtHead(ctx, key, value, expect, head)
}

func (d *durableStateAccess) Delete(ctx context.Context, key string, expect pluginapi.Revision) error {
	ctx, cancel, err := d.context(ctx, true)
	if err != nil {
		return err
	}
	defer cancel()
	return d.instance.durable.Delete(ctx, key, expect)
}

func (d *durableStateAccess) List(ctx context.Context, prefix string, limit int) ([]pluginapi.DurableEntry, error) {
	ctx, cancel, err := d.context(ctx, false)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return d.instance.durable.List(ctx, prefix, limit)
}

// durableAccess stores one plugin alias/instance namespace in one driver
// checkpoint. CheckpointStore is deliberately the only persistence primitive
// exposed here: plugin code cannot obtain a driver, transaction, SQL handle or
// filesystem path. Each mutation is a per-key revision check plus an outer
// checkpoint CAS, so concurrent runtimes either publish a complete namespace
// or receive a typed conflict.
type durableAccess struct {
	store      driver.CheckpointStore
	lease      driver.ActiveLease
	key        string
	persistent bool
	source     *driver.SourcePlaneProviderCommitV1
}

type durableNamespace struct {
	Values map[string]durableValue            `json:"values"`
	Origin *driver.SourcePlaneOriginBindingV1 `json:"sourceOrigin,omitempty"`
}

type durableValue struct {
	Revision uint64          `json:"revision"`
	Value    json.RawMessage `json:"value,omitempty"`
}

func newDurableAccess(store driver.CheckpointStore, lease driver.ActiveLease, alias, instance string) *durableAccess {
	return newDurableAccessWithPersistence(store, lease, alias, instance, false)
}

func newDurableAccessWithPersistence(store driver.CheckpointStore, lease driver.ActiveLease, alias, instance string, _ bool) *durableAccess {
	// The final argument is retained for the composition seam's source
	// compatibility, but persistence is never caller-asserted. A checkpoint is
	// restart-durable only when the selected driver advertises the stronger
	// capability and reports it as persistent.
	persistent := false
	if capability, ok := store.(driver.PersistentCheckpointStore); ok {
		persistent = capability.Persistent()
	}
	return &durableAccess{store: store, lease: lease, key: "plugin/" + alias + "/" + instance + "/durable", persistent: persistent}
}

// NewDurableState exposes the plugin-scoped capability to a trusted
// composition root. The plugin still receives only the pluginapi interface.
func NewDurableState(store driver.CheckpointStore, lease driver.ActiveLease, alias, instance string) pluginapi.DurableState {
	return newDurableAccess(store, lease, alias, instance)
}

// NewDurableStateWithPersistence is retained for composition source
// compatibility. Its legacy boolean is ignored: the process restart guarantee
// is derived from the selected driver's PersistentCheckpointStore capability.
func NewDurableStateWithPersistence(store driver.CheckpointStore, lease driver.ActiveLease, alias, instance string, persistent bool) pluginapi.DurableState {
	return newDurableAccessWithPersistence(store, lease, alias, instance, persistent)
}

func (d *durableAccess) Persistent() bool { return d != nil && d.persistent }

func (d *durableAccess) available(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d == nil || d.store == nil || d.lease == nil || (!d.persistent && d.source == nil) {
		return failure(ErrDurableState, "persistent durable capability is not configured")
	}
	select {
	case <-d.lease.Lost():
		return failure(ErrDurable, "storage lease is lost")
	default:
	}
	return nil
}

func (d *durableAccess) load(ctx context.Context) (driver.Checkpoint, durableNamespace, error) {
	if d != nil && d.source != nil {
		checkpoint, namespace, _, err := d.loadSource(ctx)
		return checkpoint, namespace, err
	}
	var namespace durableNamespace
	if err := d.available(ctx); err != nil {
		return driver.Checkpoint{}, namespace, err
	}
	checkpoint, err := d.store.LoadCheckpoint(ctx, d.key)
	if err != nil {
		return driver.Checkpoint{}, namespace, err
	}
	namespace.Values = map[string]durableValue{}
	if checkpoint.Generation == 0 {
		return checkpoint, namespace, nil
	}
	if len(checkpoint.Payload) == 0 {
		return driver.Checkpoint{}, namespace, fmt.Errorf("empty durable namespace payload")
	}
	decoder := json.NewDecoder(bytes.NewReader(checkpoint.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&namespace); err != nil {
		return driver.Checkpoint{}, namespace, fmt.Errorf("invalid durable namespace: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return driver.Checkpoint{}, namespace, fmt.Errorf("durable namespace has trailing data")
		}
		return driver.Checkpoint{}, namespace, fmt.Errorf("durable namespace has trailing data")
	}
	if namespace.Values == nil {
		namespace.Values = map[string]durableValue{}
	}
	if namespace.Origin != nil {
		return driver.Checkpoint{}, namespace, failure(ErrDurableState, "source-restored state requires its bound runtime")
	}
	for key, value := range namespace.Values {
		if err := driver.ValidateCheckpointKey(key); err != nil {
			return driver.Checkpoint{}, namespace, err
		}
		if value.Revision == 0 && len(value.Value) != 0 {
			return driver.Checkpoint{}, namespace, fmt.Errorf("durable value %q has a zero revision", key)
		}
	}
	return checkpoint, namespace, nil
}

func (d *durableAccess) Read(ctx context.Context, key string, dst any) (bool, pluginapi.Revision, error) {
	if err := d.sourceKey(key); err != nil {
		return false, "", err
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return false, "", err
	}
	_, namespace, err := d.load(ctx)
	if err != nil {
		return false, "", err
	}
	value, ok := namespace.Values[key]
	revision := pluginapi.Natural64(value.Revision)
	if !ok || len(value.Value) == 0 {
		return false, revision, nil
	}
	if dst == nil {
		return false, revision, failure(ErrDurable, "destination is nil")
	}
	if err := json.Unmarshal(value.Value, dst); err != nil {
		return false, revision, fmt.Errorf("decode durable value %q: %w", key, err)
	}
	return true, revision, nil
}

func (d *durableAccess) Get(ctx context.Context, key string, dst any) (bool, error) {
	found, _, err := d.Read(ctx, key, dst)
	return found, err
}

func (d *durableAccess) Revision(ctx context.Context, key string) (pluginapi.Revision, error) {
	if err := d.sourceKey(key); err != nil {
		return "", err
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return "", err
	}
	_, namespace, err := d.load(ctx)
	if err != nil {
		return "", err
	}
	return pluginapi.Natural64(namespace.Values[key].Revision), nil
}

func (d *durableAccess) Put(ctx context.Context, key string, value any, expect pluginapi.Revision) (pluginapi.Revision, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return d.replace(ctx, key, data, expect)
}

func (d *durableAccess) PutAtHead(ctx context.Context, key string, value any, expect pluginapi.Revision, head driver.CoreSnapshotExpectation) (pluginapi.Revision, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return d.replaceAtHead(ctx, key, data, expect, &head)
}

func (d *durableAccess) Delete(ctx context.Context, key string, expect pluginapi.Revision) error {
	if d != nil && d.source != nil {
		return driver.ErrUnsupported
	}
	_, err := d.replace(ctx, key, nil, expect)
	return err
}

func (d *durableAccess) replace(ctx context.Context, key string, data []byte, expect pluginapi.Revision) (pluginapi.Revision, error) {
	return d.replaceAtHead(ctx, key, data, expect, nil)
}

func (d *durableAccess) replaceAtHead(ctx context.Context, key string, data []byte, expect pluginapi.Revision, head *driver.CoreSnapshotExpectation) (pluginapi.Revision, error) {
	if err := d.sourceKey(key); err != nil {
		return "", err
	}
	if d.source != nil {
		if err := sourceObject(data); err != nil {
			return "", err
		}
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return "", err
	}
	expected, err := expect.Uint64()
	if err != nil {
		return "", err
	}
	var checkpoint driver.Checkpoint
	var namespace durableNamespace
	if d.source != nil {
		var observed driver.CoreSnapshotExpectation
		checkpoint, namespace, observed, err = d.loadSource(ctx)
		if head == nil {
			head = &observed
		} else if *head != observed {
			return "", driver.ErrCheckpointConflict
		}
	} else {
		checkpoint, namespace, err = d.load(ctx)
	}
	if err != nil {
		return "", err
	}
	current := namespace.Values[key]
	if current.Revision != expected {
		return "", driver.ErrCheckpointConflict
	}
	if current.Revision == ^uint64(0) {
		return "", fmt.Errorf("durable key generation exhausted")
	}
	current.Revision++
	current.Value = append([]byte(nil), data...)
	namespace.Values[key] = current
	payload, err := json.Marshal(namespace)
	if err != nil {
		return "", err
	}
	if head == nil {
		_, err = d.store.SaveCheckpoint(ctx, d.lease, d.key, checkpoint.Generation, payload)
	} else if guarded, ok := d.store.(driver.HeadGuardedCheckpointStore); ok {
		_, err = guarded.SaveCheckpointAtHead(ctx, d.lease, d.key, checkpoint.Generation, payload, *head)
	} else {
		err = driver.ErrUnsupported
	}
	if err != nil {
		return "", err
	}
	return pluginapi.Natural64(current.Revision), nil
}

func (d *durableAccess) List(ctx context.Context, prefix string, limit int) ([]pluginapi.DurableEntry, error) {
	if err := d.sourceKey(prefix); err != nil {
		return nil, err
	}
	if err := driver.ValidateCheckpointKey(prefix); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		return nil, failure(ErrDurable, "durable list limit must be between 1 and 1000")
	}
	_, namespace, err := d.load(ctx)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(namespace.Values))
	for key, value := range namespace.Values {
		if len(value.Value) != 0 && len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]pluginapi.DurableEntry, 0, len(keys))
	for _, key := range keys {
		value := namespace.Values[key]
		out = append(out, pluginapi.DurableEntry{Key: key, Revision: pluginapi.Natural64(value.Revision), Value: append([]byte(nil), value.Value...)})
	}
	return out, nil
}

var _ pluginapi.DurableState = (*durableAccess)(nil)

// PrepareCheckpoint is a host-only data preparation seam for coordinated core
// lifecycle writes. It is intentionally absent from pluginapi.DurableState.
// Preparation publishes nothing; the ordinary writer owns fencing and commit.
func (d *durableAccess) PrepareCheckpoint(ctx context.Context, key string, value any, expect pluginapi.Revision) (driver.CheckpointMutation, error) {
	if err := d.sourceKey(key); err != nil {
		return driver.CheckpointMutation{}, err
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return driver.CheckpointMutation{}, err
	}
	expected, err := expect.Uint64()
	if err != nil {
		return driver.CheckpointMutation{}, err
	}
	checkpoint, namespace, err := d.loadForPreparation(ctx)
	if err != nil {
		return driver.CheckpointMutation{}, err
	}
	current := namespace.Values[key]
	if current.Revision != expected {
		return driver.CheckpointMutation{}, driver.ErrCheckpointConflict
	}
	if current.Revision == ^uint64(0) {
		return driver.CheckpointMutation{}, fmt.Errorf("durable key generation exhausted")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return driver.CheckpointMutation{}, err
	}
	if d.source != nil {
		if err := sourceObject(data); err != nil {
			return driver.CheckpointMutation{}, err
		}
	}
	namespace.Values[key] = durableValue{Revision: expected + 1, Value: data}
	payload, err := json.Marshal(namespace)
	if err != nil {
		return driver.CheckpointMutation{}, err
	}
	metadata, ok := d.store.(interface {
		Meta(context.Context) (driver.Meta, error)
	})
	if !ok {
		return driver.CheckpointMutation{}, driver.ErrUnsupported
	}
	meta, err := metadata.Meta(ctx)
	if err != nil {
		return driver.CheckpointMutation{}, err
	}
	if err := d.sourceMeta(meta); err != nil {
		return driver.CheckpointMutation{}, err
	}
	return driver.CheckpointMutation{StoreID: meta.StoreID, Incarnation: meta.Incarnation, Key: d.key, Expected: checkpoint.Generation, Payload: payload}, nil
}

// PrepareCheckpoint retains live invocation/cancellation validation for native
// lifecycle callbacks; it still exposes only data to the trusted writer.
func (d *durableStateAccess) PrepareCheckpoint(ctx context.Context, key string, value any, expect pluginapi.Revision) (driver.CheckpointMutation, error) {
	ctx, cancel, err := d.context(ctx, true)
	if err != nil {
		return driver.CheckpointMutation{}, err
	}
	defer cancel()
	return d.instance.durable.PrepareCheckpoint(ctx, key, value, expect)
}

// PrepareCheckpointExpectation binds a provider read to the complete namespace
// generation. It never writes and is absent from the public plugin capability.
func (d *durableAccess) PrepareCheckpointExpectation(ctx context.Context, key string, expect pluginapi.Revision) (driver.CheckpointExpectation, error) {
	if err := d.sourceKey(key); err != nil {
		return driver.CheckpointExpectation{}, err
	}
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return driver.CheckpointExpectation{}, err
	}
	expected, err := expect.Uint64()
	if err != nil {
		return driver.CheckpointExpectation{}, err
	}
	checkpoint, namespace, err := d.loadForPreparation(ctx)
	if err != nil {
		return driver.CheckpointExpectation{}, err
	}
	if namespace.Values[key].Revision != expected {
		return driver.CheckpointExpectation{}, driver.ErrCheckpointConflict
	}
	metadata, ok := d.store.(interface {
		Meta(context.Context) (driver.Meta, error)
	})
	if !ok {
		return driver.CheckpointExpectation{}, driver.ErrUnsupported
	}
	meta, err := metadata.Meta(ctx)
	if err != nil {
		return driver.CheckpointExpectation{}, err
	}
	if err := d.sourceMeta(meta); err != nil {
		return driver.CheckpointExpectation{}, err
	}
	return driver.CheckpointExpectation{StoreID: meta.StoreID, Incarnation: meta.Incarnation, Key: d.key, Expected: checkpoint.Generation}, nil
}

func (d *durableStateAccess) PrepareCheckpointExpectation(ctx context.Context, key string, expect pluginapi.Revision) (driver.CheckpointExpectation, error) {
	ctx, cancel, err := d.context(ctx, false)
	if err != nil {
		return driver.CheckpointExpectation{}, err
	}
	defer cancel()
	return d.instance.durable.PrepareCheckpointExpectation(ctx, key, expect)
}

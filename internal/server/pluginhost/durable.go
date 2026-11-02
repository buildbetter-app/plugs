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
}

type durableNamespace struct {
	Values map[string]durableValue `json:"values"`
}

type durableValue struct {
	Revision uint64          `json:"revision"`
	Value    json.RawMessage `json:"value,omitempty"`
}

func newDurableAccess(store driver.CheckpointStore, lease driver.ActiveLease, alias, instance string) *durableAccess {
	return newDurableAccessWithPersistence(store, lease, alias, instance, true)
}

func newDurableAccessWithPersistence(store driver.CheckpointStore, lease driver.ActiveLease, alias, instance string, persistent bool) *durableAccess {
	return &durableAccess{store: store, lease: lease, key: "plugin/" + alias + "/" + instance + "/durable", persistent: persistent}
}

// NewDurableState exposes the plugin-scoped capability to a trusted
// composition root. The plugin still receives only the pluginapi interface.
func NewDurableState(store driver.CheckpointStore, lease driver.ActiveLease, alias, instance string) pluginapi.DurableState {
	return newDurableAccess(store, lease, alias, instance)
}

// NewDurableStateWithPersistence is used by composition to make the process
// restart guarantee explicit. The memory driver is intentionally marked false.
func NewDurableStateWithPersistence(store driver.CheckpointStore, lease driver.ActiveLease, alias, instance string, persistent bool) pluginapi.DurableState {
	return newDurableAccessWithPersistence(store, lease, alias, instance, persistent)
}

func (d *durableAccess) Persistent() bool { return d != nil && d.persistent }

func (d *durableAccess) available(ctx context.Context, write bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d == nil || d.store == nil || d.lease == nil {
		return failure(ErrDurable, "durable capability is not configured")
	}
	select {
	case <-d.lease.Lost():
		return failure(ErrDurable, "storage lease is lost")
	default:
	}
	if write {
		return nil
	}
	return nil
}

func (d *durableAccess) load(ctx context.Context) (driver.Checkpoint, durableNamespace, error) {
	var namespace durableNamespace
	if err := d.available(ctx, false); err != nil {
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

func (d *durableAccess) Delete(ctx context.Context, key string, expect pluginapi.Revision) error {
	_, err := d.replace(ctx, key, nil, expect)
	return err
}

func (d *durableAccess) replace(ctx context.Context, key string, data []byte, expect pluginapi.Revision) (pluginapi.Revision, error) {
	if err := driver.ValidateCheckpointKey(key); err != nil {
		return "", err
	}
	expected, err := expect.Uint64()
	if err != nil {
		return "", err
	}
	checkpoint, namespace, err := d.load(ctx)
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
	if _, err := d.store.SaveCheckpoint(ctx, d.lease, d.key, checkpoint.Generation, payload); err != nil {
		return "", err
	}
	return pluginapi.Natural64(current.Revision), nil
}

func (d *durableAccess) List(ctx context.Context, prefix string, limit int) ([]pluginapi.DurableEntry, error) {
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

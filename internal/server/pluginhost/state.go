package pluginhost

import (
	"context"
	"encoding/json"
	"math"
	"sync"

	"github.com/buildbetter-app/plugs/pluginapi"
)

// Native state is per-instance, in-process CAS storage. It is not a durable
// delivery checkpoint; those belong to the storage/event layer.
type stateStore struct {
	mu     sync.Mutex
	values map[string]stateValue
}
type stateValue struct {
	revision uint64
	data     []byte
}

func newStateStore() *stateStore { return &stateStore{values: map[string]stateValue{}} }

type stateAccess struct{ instance *instance }

func (s *stateAccess) available(ctx context.Context, write bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	call, err := s.instance.current()
	if err != nil {
		return err
	}
	if call.hot() {
		if write {
			return failure(ErrHotWrite, "plugin state")
		}
		return failure(ErrRead, "mutable plugin state is unavailable in a hot call")
	}
	return nil
}
func (s *stateAccess) Get(ctx context.Context, key string, dst any) (bool, error) {
	if err := s.available(ctx, false); err != nil {
		return false, err
	}
	s.instance.state.mu.Lock()
	v := s.instance.state.values[key]
	data := append([]byte(nil), v.data...)
	s.instance.state.mu.Unlock()
	if data == nil {
		return false, nil
	}
	return true, decode(data, dst)
}
func (s *stateAccess) Revision(ctx context.Context, key string) (pluginapi.Revision, error) {
	if err := s.available(ctx, false); err != nil {
		return "", err
	}
	s.instance.state.mu.Lock()
	defer s.instance.state.mu.Unlock()
	return pluginapi.Natural64(s.instance.state.values[key].revision), nil
}
func (s *stateAccess) Put(ctx context.Context, key string, value any, expect pluginapi.Revision) (pluginapi.Revision, error) {
	if err := s.available(ctx, true); err != nil {
		return "", err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return s.replace(key, data, expect)
}
func (s *stateAccess) Delete(ctx context.Context, key string, expect pluginapi.Revision) error {
	if err := s.available(ctx, true); err != nil {
		return err
	}
	_, err := s.replace(key, nil, expect)
	return err
}
func (s *stateAccess) replace(key string, data []byte, expect pluginapi.Revision) (pluginapi.Revision, error) {
	if key == "" {
		return "", failure(ErrManifest, "state key is empty")
	}
	expected, err := expect.Uint64()
	if err != nil {
		return "", err
	}
	store := s.instance.state
	store.mu.Lock()
	defer store.mu.Unlock()
	old := store.values[key]
	if old.revision != expected || old.revision == math.MaxUint64 {
		return "", failure("plugin_state_conflict", "compare-and-set failed")
	}
	next := old.revision + 1
	store.values[key] = stateValue{next, append([]byte(nil), data...)}
	return pluginapi.Natural64(next), nil
}

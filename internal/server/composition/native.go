package composition

import (
	"fmt"

	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// newNativeHost is the only composition seam that creates the trusted native
// callback host. Only a store advertising persistent checkpoints receives the
// exact checkpoint store and lease already owned by Runtime. A driver name or
// a process-local checkpoint implementation cannot promise restart durability.
func (r *Runtime) newNativeHost(config Config, authConfig authn.Config) (*pluginhost.Host, error) {
	var checkpoints driver.CheckpointStore
	var durableLease driver.ActiveLease
	if candidate, ok := r.Store.(driver.PersistentCheckpointStore); ok && candidate.Persistent() {
		checkpoints = candidate
		durableLease = r.Lease
	}
	host, err := pluginhost.NewWithContext(r.ctx, pluginhost.Config{
		Natives:                  config.NativePlugins,
		Bindings:                 config.NativeBindings,
		AuthenticationConfigured: authConfig.Provider != nil || authConfig.CoreSessions != nil,
		CheckpointStore:          checkpoints,
		CheckpointLease:          durableLease,
	})
	if err != nil {
		return nil, fmt.Errorf("composition: create native plugin host: %w", err)
	}
	return host, nil
}

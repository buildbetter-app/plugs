package composition

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/pluginbuild"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

// RestoredWASIConfig selects an already source-built artifact and its exact
// stored lock. Open restores bytes from Repository; it never builds or requires
// the source checkout. The artifact's plugins share one canonical host and its
// binding/ownership checks. Mixing this slot with compiled-in callbacks is not
// supported. This host-owned configuration does not install a plugin set.
type RestoredWASIConfig struct {
	Repository *pluginbuild.ArtifactStore
	Lock       pluginbuild.ArtifactLock
	Bindings   []pluginhost.Binding
}

type restoredWASI struct {
	artifact *pluginbuild.Artifact
	bindings []pluginhost.Binding
}

// restoreConfiguredWASI admits the complete lock before storage, constructors
// or listeners can have side effects. Restore compares source identity and
// manifest/output digests with the repository's actual Put-produced lock.
func restoreConfiguredWASI(ctx context.Context, config Config) (*restoredWASI, error) {
	if config.RestoredWASI == nil {
		return nil, nil
	}
	if len(config.NativePlugins) != 0 || len(config.NativeBindings) != 0 {
		return nil, &pluginbuild.Error{Code: pluginbuild.ErrConfig, Detail: "restored WASI and compiled-in callbacks cannot be combined"}
	}
	selected := config.RestoredWASI
	if selected.Lock.Runtime != pluginapi.RuntimeWASI {
		return nil, &pluginbuild.Error{Code: pluginbuild.ErrConfig, Detail: "configured restored artifacts must use wasip1"}
	}
	artifact, err := selected.Repository.Restore(ctx, selected.Lock)
	if err != nil {
		return nil, fmt.Errorf("composition: restore WASI artifact: %w", err)
	}
	bindings := append([]pluginhost.Binding(nil), selected.Bindings...)
	for i := range bindings {
		bindings[i].Reads = append([]string(nil), bindings[i].Reads...)
		bindings[i].Owns = append([]string(nil), bindings[i].Owns...)
	}
	return &restoredWASI{artifact: artifact, bindings: bindings}, nil
}

func (r *Runtime) newRestoredWASIHost(authConfig authn.Config) (*pluginhost.Host, error) {
	var checkpoints driver.CheckpointStore
	var durableLease driver.ActiveLease
	if candidate, ok := r.Store.(driver.PersistentCheckpointStore); ok && candidate.Persistent() {
		checkpoints, durableLease = candidate, r.Lease
	}
	host, err := r.restoredWASI.artifact.Host(r.ctx, pluginhost.Config{
		Bindings:                 r.restoredWASI.bindings,
		AuthenticationConfigured: authConfig.Provider != nil || authConfig.CoreSessions != nil,
		CheckpointStore:          checkpoints,
		CheckpointLease:          durableLease,
	})
	if err != nil {
		return nil, fmt.Errorf("composition: create restored WASI host: %w", err)
	}
	return host, nil
}

func closeRestoredWASI(restored *restoredWASI) error {
	if restored == nil {
		return nil
	}
	return restored.artifact.Close()
}

package pluginmanifest

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

// Natives binds the complete source manifest to compiled-in implementations.
// Every declared plugin needs at least one explicit instance. No undeclared
// plugin, changed alias, instance collision, or difference in API, capabilities,
// phases, field/read budgets, method permissions/resources or egress is allowed.
// Constructors run later under pluginhost.New, which validates the actual
// method and callback registrations against these same public declarations.
// Native code remains trusted compiled-in code; this is not source execution.
func (b *Bundle) Natives(ctx context.Context, implementations []sdk.Native) ([]sdk.Native, error) {
	if err := b.Verify(ctx); err != nil {
		return nil, err
	}
	declared := map[string]pluginapi.PluginManifest{}
	for _, plugin := range b.manifest.Plugins {
		declared[plugin.ID] = plugin
	}
	if err := ValidateNatives(implementations); err != nil {
		return nil, err
	}
	used := map[string]bool{}
	result := make([]sdk.Native, 0, len(implementations))
	for _, native := range implementations {
		plugin, ok := declared[native.Manifest.ID]
		if !ok {
			return nil, refuse("registration_invalid", "compiled plugin is absent from source manifest")
		}
		if !sameManifest(plugin, native.Manifest) {
			return nil, refuse("grant_mismatch", "compiled declaration differs from source manifest for %q", plugin.Alias)
		}
		copy, err := pluginapi.CloneJSON(plugin)
		if err != nil {
			return nil, err
		}
		result = append(result, sdk.Native{Manifest: copy, Instance: native.Instance, New: native.New, NewWithContext: native.NewWithContext})
		used[plugin.ID] = true
	}
	for _, plugin := range b.manifest.Plugins {
		if !used[plugin.ID] {
			return nil, refuse("registration_missing", "source plugin %q has no compiled implementation", plugin.Alias)
		}
	}
	return result, nil
}

// ValidateNatives checks identities across bundles before host composition.
// Identical subscriber plugins may have several instances. Extension methods
// have one global name, so registering them twice refuses as it does in the
// native host. Different plugins may use the same instance name.
func ValidateNatives(natives []sdk.Native) error {
	aliases := map[string]string{}
	plugins := map[string]pluginapi.PluginManifest{}
	instances := map[struct{ alias, instance string }]bool{}
	methods := map[string]bool{}
	for _, native := range natives {
		p := native.Manifest
		if err := p.Validate(); err != nil {
			return refuse("manifest_invalid", "%v", err)
		}
		if !pluginapi.ValidName(native.Instance) || native.New == nil {
			return refuse("registration_invalid", "explicit valid instance and constructor are required")
		}
		if id, ok := aliases[p.Alias]; ok && id != p.ID {
			return refuse("registration_collision", "alias %q belongs to another plugin", p.Alias)
		}
		if previous, ok := plugins[p.ID]; ok && !sameManifest(previous, p) {
			return refuse("registration_collision", "plugin id has different aliases or declarations")
		}
		key := struct{ alias, instance string }{p.Alias, native.Instance}
		if instances[key] {
			return refuse("registration_collision", "duplicate alias/instance %q/%q", p.Alias, native.Instance)
		}
		aliases[p.Alias], plugins[p.ID], instances[key] = p.ID, p, true
		for _, provision := range p.Provides {
			for suffix := range provision.Methods {
				name := provision.Prefix + "." + suffix
				if methods[name] {
					return refuse("registration_collision", "extension method %q has multiple instances", name)
				}
				methods[name] = true
			}
		}
	}
	return nil
}

func sameManifest(a, b pluginapi.PluginManifest) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	return err == nil && bytes.Equal(left, right)
}

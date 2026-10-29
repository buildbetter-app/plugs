package pluginhost

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

type Effect string

const (
	Read  Effect = "read"
	Write Effect = "write"
)

// Dispatcher is supplied by the server registry, never imported by core. It
// must perform normal authorization for the supplied plugin principal.
type Dispatcher interface {
	Effect(string) (Effect, bool)
	Call(context.Context, pluginapi.PrincipalRef, string, json.RawMessage) (json.RawMessage, error)
}

type Config struct {
	Natives    []sdk.Native
	Bindings   []Binding
	Dispatcher Dispatcher
	// Authorize checks a caller's permission to invoke extension methods. With
	// no authorization plugin nil permits, matching core's optional chain.
	Authorize func(context.Context, string, []pluginapi.Ref) error
}

// Binding is an effective ontology binding; its grants may only narrow the
// manifest. Empty Reads/Owns mean no fields, not the manifest's maximum.
type Binding struct {
	ID, Alias, Instance, Subscriber string
	Project, Type                   string
	Phase                           pluginapi.Phase
	Reads, Owns                     []string
	MaxPointReads, MaxQueries       int
	Timeout                         time.Duration
	FailOpen                        bool
}

type Host struct {
	instances  map[string]*instance
	bindings   []*bound
	methods    map[string]registeredMethod
	dispatcher Dispatcher
	authorize  func(context.Context, string, []pluginapi.Ref) error
}

type instance struct {
	manifest     pluginapi.PluginManifest
	name         string
	registration sdk.Registration
	owner        *Host
	gate         chan struct{}
	mu           sync.Mutex
	active       *invocation
	state        *stateStore
}
type bound struct {
	Binding
	instance *instance
}
type registeredMethod struct {
	sdk.Method
	instance *instance
	grant    pluginapi.MethodGrant
}

func New(config Config) (*Host, error) {
	h := &Host{instances: map[string]*instance{}, methods: map[string]registeredMethod{}, dispatcher: config.Dispatcher, authorize: config.Authorize}
	aliases, ids := map[string]string{}, map[string]string{}
	for _, native := range config.Natives {
		if err := native.Manifest.Validate(); err != nil {
			return nil, failure(ErrManifest, "%v", err)
		}
		p := native.Manifest
		if !pluginapi.ValidName(native.Instance) || native.New == nil {
			return nil, failure(ErrManifest, "instance and constructor required")
		}
		if id, ok := aliases[p.Alias]; ok && id != p.ID {
			return nil, failure(ErrManifest, "duplicate alias %s", p.Alias)
		}
		if alias, ok := ids[p.ID]; ok && alias != p.Alias {
			return nil, failure(ErrManifest, "plugin id has multiple aliases")
		}
		aliases[p.Alias], ids[p.ID] = p.ID, p.Alias
		key := p.Alias + "/" + native.Instance
		if h.instances[key] != nil {
			return nil, failure(ErrManifest, "duplicate instance %s", key)
		}
		manifest, err := pluginapi.CloneJSON(p)
		if err != nil {
			return nil, err
		}
		i := &instance{manifest: manifest, name: native.Instance, owner: h, gate: make(chan struct{}, 1), state: newStateStore()}
		registration, err := native.New(&capabilities{instance: i})
		if err != nil {
			return nil, fmt.Errorf("initialize %s: %w", key, err)
		}
		i.registration = registration
		if err := h.register(i); err != nil {
			return nil, err
		}
		h.instances[key] = i
	}
	seen := map[string]bool{}
	for _, binding := range config.Bindings {
		if !pluginapi.ValidName(binding.ID) || seen[binding.ID] {
			return nil, failure(ErrManifest, "binding id must be unique")
		}
		seen[binding.ID] = true
		i := h.instances[binding.Alias+"/"+binding.Instance]
		if i == nil {
			return nil, failure(ErrManifest, "binding %s names missing instance", binding.ID)
		}
		var declaration *pluginapi.Provision
		for _, p := range i.manifest.Provides {
			if p.Kind == "subscriber" && p.Name == binding.Subscriber {
				copy := p
				declaration = &copy
				break
			}
		}
		if declaration == nil || !hasPhase(declaration.Phases, binding.Phase) {
			return nil, failure(ErrManifest, "binding phase not declared")
		}
		if !narrowFields(declaration.Reads, binding.Reads) || !narrowFields(declaration.Owns, binding.Owns) {
			return nil, failure(ErrManifest, "binding widens manifest field grant")
		}
		for _, path := range binding.Owns {
			if !strings.HasPrefix(path, "fields.") || !pluginapi.ValidField(path, false) || strings.Contains(strings.TrimPrefix(path, "fields."), ".") {
				return nil, failure(ErrManifest, "this pipeline supports concrete fields.<name> ownership only")
			}
		}
		if binding.Phase != pluginapi.PhaseMutate && len(binding.Owns) > 0 {
			return nil, failure(ErrManifest, "non-mutator owns fields")
		}
		pointMax, queryMax := readLimits(*declaration)
		if binding.MaxPointReads == 0 {
			binding.MaxPointReads = pointMax
		}
		if binding.MaxQueries == 0 {
			binding.MaxQueries = queryMax
		}
		if binding.MaxPointReads < 0 || binding.MaxPointReads > pointMax || binding.MaxQueries < 0 || binding.MaxQueries > queryMax {
			return nil, failure(ErrManifest, "binding widens read budget")
		}
		if binding.Timeout < 0 || binding.Timeout > time.Second {
			return nil, failure(ErrManifest, "callback timeout exceeds one second")
		}
		if binding.Timeout == 0 {
			binding.Timeout = 500 * time.Millisecond
			if binding.Phase == pluginapi.PhaseMutate {
				binding.Timeout = 250 * time.Millisecond
			}
		}
		binding.Reads = append([]string(nil), binding.Reads...)
		binding.Owns = append([]string(nil), binding.Owns...)
		h.bindings = append(h.bindings, &bound{binding, i})
	}
	ordered, err := order(h.bindings)
	if err != nil {
		return nil, err
	}
	h.bindings = ordered
	return h, nil
}

func (h *Host) register(i *instance) error {
	declared := map[string]pluginapi.MethodGrant{}
	subs := map[string]map[pluginapi.Phase]bool{}
	for _, p := range i.manifest.Provides {
		if p.Kind == "methods" {
			for suffix, grant := range p.Methods {
				declared[p.Prefix+"."+suffix] = grant
			}
		}
		if p.Kind == "subscriber" {
			subs[p.Name] = map[pluginapi.Phase]bool{}
			for _, phase := range p.Phases {
				subs[p.Name][phase] = true
			}
		}
	}
	for _, m := range i.registration.Methods {
		d := m.Descriptor
		grant, ok := declared[d.Name]
		if !ok || h.methods[d.Name].instance != nil || d.Permission != grant.Permission || d.Resource == nil || d.Params == nil || d.Result == nil || m.Handle == nil {
			return failure(ErrMethod, "invalid or undeclared registration %s", d.Name)
		}
		h.methods[d.Name] = registeredMethod{m, i, grant}
		delete(declared, d.Name)
	}
	if len(declared) > 0 {
		return failure(ErrMethod, "manifest declares missing method implementation")
	}
	for name, callback := range i.registration.Mutators {
		if callback == nil || !subs[name][pluginapi.PhaseMutate] {
			return failure(ErrManifest, "undeclared mutator %s", name)
		}
	}
	for name, callback := range i.registration.Validators {
		if callback == nil || !subs[name][pluginapi.PhaseValidate] {
			return failure(ErrManifest, "undeclared validator %s", name)
		}
	}
	for name, callback := range i.registration.Posts {
		if callback == nil || !subs[name][pluginapi.PhasePost] {
			return failure(ErrManifest, "undeclared post subscriber %s", name)
		}
	}
	for name, phases := range subs {
		if phases[pluginapi.PhaseMutate] && i.registration.Mutators[name] == nil || phases[pluginapi.PhaseValidate] && i.registration.Validators[name] == nil || phases[pluginapi.PhasePost] && i.registration.Posts[name] == nil {
			return failure(ErrManifest, "missing callback %s", name)
		}
	}
	return nil
}

func readLimits(p pluginapi.Provision) (int, int) {
	a, b := p.MaxPointReads, p.MaxQueries
	if a == 0 {
		a = 32
	}
	if b == 0 {
		b = 4
	}
	return a, b
}
func hasPhase(phases []pluginapi.Phase, phase pluginapi.Phase) bool {
	for _, p := range phases {
		if p == phase {
			return true
		}
	}
	return false
}
func narrowFields(maximum, fields []string) bool {
	seen := map[string]bool{}
	for _, path := range fields {
		if !pluginapi.ValidField(path, true) || seen[path] {
			return false
		}
		seen[path] = true
		ok := false
		for _, grant := range maximum {
			if pluginapi.AllowsField(grant, path) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}
func overlap(a, b string) bool {
	return pluginapi.AllowsField(a, b) || pluginapi.AllowsField(b, a) || strings.HasPrefix(a, b+".") || strings.HasPrefix(b, a+".")
}
func sameScope(a, b Binding) bool {
	return (a.Project == "" || b.Project == "" || a.Project == b.Project) && (a.Type == "" || b.Type == "" || a.Type == b.Type)
}

func order(bindings []*bound) ([]*bound, error) {
	mutators := []*bound{}
	others := []*bound{}
	for _, b := range bindings {
		if b.Phase == pluginapi.PhaseMutate {
			mutators = append(mutators, b)
		} else {
			others = append(others, b)
		}
	}
	sort.Slice(mutators, func(i, j int) bool { return mutators[i].ID < mutators[j].ID })
	edges := make([]map[int]bool, len(mutators))
	degree := make([]int, len(mutators))
	for a, x := range mutators {
		edges[a] = map[int]bool{}
		for b, y := range mutators {
			if a == b || !sameScope(x.Binding, y.Binding) {
				continue
			}
			for _, own := range x.Owns {
				for _, other := range y.Owns {
					if overlap(own, other) {
						return nil, failure(ErrManifest, "multiple owners: %s and %s", x.ID, y.ID)
					}
				}
				for _, read := range y.Reads {
					if overlap(own, read) && !edges[a][b] {
						edges[a][b] = true
						degree[b]++
					}
				}
			}
		}
	}
	out := []*bound{}
	used := make([]bool, len(mutators))
	for len(out) < len(mutators) {
		next := -1
		for i := range mutators {
			if !used[i] && degree[i] == 0 {
				next = i
				break
			}
		}
		if next < 0 {
			return nil, failure(ErrManifest, "mutator dataflow cycle")
		}
		used[next] = true
		out = append(out, mutators[next])
		for j := range edges[next] {
			degree[j]--
		}
	}
	sort.Slice(others, func(i, j int) bool {
		if others[i].Phase != others[j].Phase {
			return others[i].Phase < others[j].Phase
		}
		return others[i].ID < others[j].ID
	})
	return append(out, others...), nil
}

func (h *Host) BindingOrder() []string {
	out := make([]string, len(h.bindings))
	for j, b := range h.bindings {
		out[j] = b.ID
	}
	return out
}

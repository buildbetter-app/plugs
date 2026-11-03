package pluginhost

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
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
	// CheckpointStore and CheckpointLease are the canonical host-owned
	// persistence capability. DurableStore/DurableLease below are retained as
	// composition vocabulary aliases for this same capability.
	CheckpointStore     driver.CheckpointStore
	CheckpointLease     driver.ActiveLease
	RequireDurableState bool
	// AuthenticationConfigured is trusted host configuration, never a plugin
	// grant or caller-supplied principal. Leave it false in anonymous mode:
	// even declared egress is disabled without an authentication provider.
	AuthenticationConfigured bool
	// Authorize checks a caller's permission to invoke extension methods. With
	// no authorization plugin nil permits, matching core's optional chain.
	Authorize func(context.Context, string, []pluginapi.Ref) error
	// DurableStore and DurableLease enable the plugin-owned unversioned
	// capability. Both must be supplied together; without them DurableState
	// fails closed instead of falling back to process memory.
	DurableStore driver.CheckpointStore
	DurableLease driver.ActiveLease
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
	instances                map[string]*instance
	natives                  map[string]sdk.Native
	bindings                 []*bound
	methods                  map[string]registeredMethod
	dispatcher               Dispatcher
	authorize                func(context.Context, string, []pluginapi.Ref) error
	authenticationConfigured bool
	durableStore             driver.CheckpointStore
	durableLease             driver.ActiveLease
	durablePersistent        bool
}

const (
	// maxRuntimeInstances is the settled per-binding WASI pool size. Each
	// member owns its guest module and linear memory; only the host state store
	// is shared between members.
	maxRuntimeInstances = 4
	// callbackQueueWait is separate from the callback execution budget. A
	// caller that cannot acquire a runtime in this interval gets a typed,
	// retryable pool-busy error instead of spending its execution budget queued.
	callbackQueueWait = 100 * time.Millisecond
)

type instance struct {
	manifest     pluginapi.PluginManifest
	name         string
	registration sdk.Registration
	owner        *Host
	gate         chan struct{}
	mu           sync.Mutex
	active       *invocation
	state        *stateStore
	durable      *durableAccess
	disposed     bool
	disposeFn    func()
}
type bound struct {
	Binding
	pool *instancePool
}

type instancePool struct {
	available chan *instance
	native    sdk.Native
	manifest  pluginapi.PluginManifest
	owner     *Host
	state     *stateStore
	mu        sync.Mutex
	affinity  map[string]*instance
	leased    map[*instance]bool
	notify    chan struct{}
}

func newInstancePool(native sdk.Native, owner *Host, source *instance) (*instancePool, error) {
	pool := &instancePool{
		available: make(chan *instance, maxRuntimeInstances),
		native:    native,
		manifest:  source.manifest,
		owner:     owner,
		state:     source.state,
		affinity:  map[string]*instance{},
		leased:    map[*instance]bool{},
		notify:    make(chan struct{}, 1),
	}
	// The source instance owns extension methods. Each binding gets four fresh
	// callback guests; only the host-side state store crosses that boundary.
	for n := 0; n < maxRuntimeInstances; n++ {
		member, err := newInstance(native, source.manifest, owner, source.state, source.durable)
		if err != nil {
			return nil, err
		}
		if err := validateRegistration(member); err != nil {
			return nil, err
		}
		pool.available <- member
	}
	return pool, nil
}

func (p *instancePool) newMember() (*instance, error) {
	member, err := newInstance(p.native, p.manifest, p.owner, p.state, p.owner.durableFor(p.manifest.Alias, p.native.Instance))
	if err != nil {
		return nil, err
	}
	if err := validateRegistration(member); err != nil {
		member.dispose()
		return nil, err
	}
	return member, nil
}

func (p *instancePool) signal() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// takeFree reassigns an idle affinity member when every unpinned member is
// occupied by another principal. This keeps affinity useful for sequential
// callbacks without pinning a four-member pool to an unbounded number of
// principals.
func (p *instancePool) takeFree(key string) *instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	for owner, member := range p.affinity {
		if p.leased[member] || member.isDisposed() {
			continue
		}
		delete(p.affinity, owner)
		p.leased[member] = true
		if key != "" {
			p.affinity[key] = member
		}
		return member
	}
	return nil
}

func (p *instancePool) acquire(ctx context.Context, wait time.Duration, key string) (*instance, error) {
	queueCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		if key != "" {
			p.mu.Lock()
			member := p.affinity[key]
			if member != nil && !member.isDisposed() && !p.leased[member] {
				p.leased[member] = true
				p.mu.Unlock()
				return member, nil
			}
			p.mu.Unlock()
		}
		if member := p.takeFree(key); member != nil {
			return member, nil
		}
		select {
		case member := <-p.available:
			if member == nil || member.isDisposed() {
				continue
			}
			if key != "" {
				p.mu.Lock()
				if current := p.affinity[key]; current == nil || current.isDisposed() {
					p.affinity[key] = member
				}
				p.leased[member] = true
				p.mu.Unlock()
			}
			return member, nil
		case <-p.notify:
			// A keyed member may have completed while it remains outside the
			// general available channel. Recheck its affinity before waiting.
			continue
		case <-queueCtx.Done():
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, failure(ErrPluginBusy, "plugin runtime pool queue is full")
		}
	}
}

func (p *instancePool) release(member *instance, key string, err error) {
	if err != nil {
		// Dispose before waiting so a trapped or expired WASI call is asked to
		// unwind; the old member is never put back into the pool.
		member.dispose()
		member.waitIdle()
		p.mu.Lock()
		if key != "" && p.affinity[key] == member {
			delete(p.affinity, key)
		}
		delete(p.leased, member)
		p.mu.Unlock()
		fresh, freshErr := p.newMember()
		if freshErr == nil {
			p.available <- fresh
			p.signal()
		}
		return
	}
	member.waitIdle()
	p.mu.Lock()
	delete(p.leased, member)
	keepAffinity := key != "" && p.affinity[key] == member && !member.isDisposed()
	if !keepAffinity {
		for owner, current := range p.affinity {
			if current == member {
				delete(p.affinity, owner)
			}
		}
	}
	p.mu.Unlock()
	if !keepAffinity {
		p.available <- member
	}
	p.signal()
}

// run acquires an idle member with a bounded queue wait, then starts the
// member's callback deadline only after it has been acquired. The selected
// member is retained only while it is healthy. A non-empty affinity key keeps
// a sequential caller on one guest while allowing other keys to use the other
// pool members concurrently.
func (p *instancePool) run(ctx context.Context, phase pluginapi.Phase, timeout time.Duration, reader pluginapi.SnapshotReader, fn func(*instance, context.Context) error, affinity ...string) error {
	wait := callbackQueueWait
	if timeout > 0 && timeout < wait {
		wait = timeout
	}
	key := ""
	if len(affinity) != 0 {
		key = affinity[0]
	}
	member, err := p.acquire(ctx, wait, key)
	if err != nil {
		return err
	}
	err = member.run(ctx, phase, timeout, reader, func(execCtx context.Context) error {
		return fn(member, execCtx)
	})
	// A timed-out native callback may still be unwinding while instance.run
	// returns. Do not advertise that member to another caller until its gate is
	// actually released; failed members are replaced only after that drain.
	go p.release(member, key, err)
	return err
}

type registeredMethod struct {
	sdk.Method
	instance *instance
	grant    pluginapi.MethodGrant
}

func New(config Config) (*Host, error) {
	if config.CheckpointStore != nil && config.DurableStore != nil {
		return nil, failure(ErrDurableState, "checkpoint store supplied more than once")
	}
	if config.CheckpointLease != nil && config.DurableLease != nil {
		return nil, failure(ErrDurableState, "checkpoint lease supplied more than once")
	}
	checkpointStore := config.CheckpointStore
	if checkpointStore == nil {
		checkpointStore = config.DurableStore
	}
	checkpointLease := config.CheckpointLease
	if checkpointLease == nil {
		checkpointLease = config.DurableLease
	}
	hasCheckpoint := checkpointStore != nil || checkpointLease != nil
	if hasCheckpoint && (checkpointStore == nil || checkpointLease == nil) {
		return nil, failure(ErrDurableState, "checkpoint store and active lease must be supplied together")
	}
	persistentCheckpoint := false
	if checkpointStore != nil {
		persistent, ok := checkpointStore.(driver.PersistentCheckpointStore)
		persistentCheckpoint = ok && persistent.Persistent()
	}
	if config.RequireDurableState && !persistentCheckpoint {
		return nil, failure(ErrDurableState, "persistent checkpoint capability is required")
	}
	h := &Host{instances: map[string]*instance{}, natives: map[string]sdk.Native{}, methods: map[string]registeredMethod{}, dispatcher: config.Dispatcher, authorize: config.Authorize, authenticationConfigured: config.AuthenticationConfigured, durableStore: checkpointStore, durableLease: checkpointLease, durablePersistent: persistentCheckpoint}
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
		if p.Requires.State == pluginapi.StateDurable && !persistentCheckpoint {
			return nil, failure(ErrDurableState, "plugin %s requires restart-durable state", p.Alias)
		}
		i, err := newInstance(native, p, h, newStateStore(), h.durableFor(p.Alias, native.Instance))
		if err != nil {
			return nil, fmt.Errorf("initialize %s: %w", key, err)
		}
		if err := h.register(i); err != nil {
			return nil, err
		}
		h.instances[key] = i
		h.natives[key] = native
	}
	seen := map[string]bool{}
	for _, binding := range config.Bindings {
		if binding.Project != "" || binding.Type != "" {
			return nil, failure(ErrManifest, "scoped bindings require ontology routing, which this host does not yet implement")
		}
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
		pool, err := newInstancePool(h.natives[binding.Alias+"/"+binding.Instance], h, i)
		if err != nil {
			return nil, fmt.Errorf("initialize binding %s runtime pool: %w", binding.ID, err)
		}
		h.bindings = append(h.bindings, &bound{Binding: binding, pool: pool})
	}
	ordered, err := order(h.bindings)
	if err != nil {
		return nil, err
	}
	h.bindings = ordered
	return h, nil
}

func (h *Host) register(i *instance) error {
	if err := validateRegistration(i); err != nil {
		return err
	}
	declared := methodGrants(i.manifest)
	for _, m := range i.registration.Methods {
		d := m.Descriptor
		grant := declared[d.Name]
		if h.methods[d.Name].instance != nil {
			return failure(ErrMethod, "invalid or undeclared registration %s", d.Name)
		}
		h.methods[d.Name] = registeredMethod{m, i, grant}
	}
	return nil
}

func newInstance(native sdk.Native, manifest pluginapi.PluginManifest, owner *Host, state *stateStore, durable *durableAccess) (*instance, error) {
	copyManifest, err := pluginapi.CloneJSON(manifest)
	if err != nil {
		return nil, err
	}
	i := &instance{manifest: copyManifest, name: native.Instance, owner: owner, gate: make(chan struct{}, 1), state: state, durable: durable}
	host := &capabilities{instance: i}
	registration, err := native.New(host)
	if err != nil {
		return nil, err
	}
	i.registration = registration
	i.disposeFn = host.onDispose
	return i, nil
}

func (h *Host) durableFor(alias, instance string) *durableAccess {
	if h.durableStore == nil || h.durableLease == nil {
		return newDurableAccessWithPersistence(nil, nil, alias, instance, false)
	}
	return newDurableAccessWithPersistence(h.durableStore, h.durableLease, alias, instance, h.durablePersistent)
}

func (i *instance) isDisposed() bool {
	i.mu.Lock()
	disposed := i.disposed
	i.mu.Unlock()
	return disposed
}

func (i *instance) dispose() {
	i.mu.Lock()
	if i.disposed {
		i.mu.Unlock()
		return
	}
	i.disposed = true
	dispose := i.disposeFn
	i.mu.Unlock()
	if dispose != nil {
		dispose()
	}
}

func methodGrants(manifest pluginapi.PluginManifest) map[string]pluginapi.MethodGrant {
	declared := map[string]pluginapi.MethodGrant{}
	for _, p := range manifest.Provides {
		if p.Kind != "methods" {
			continue
		}
		for suffix, grant := range p.Methods {
			declared[p.Prefix+"."+suffix] = grant
		}
	}
	return declared
}

func validateRegistration(i *instance) error {
	declared := methodGrants(i.manifest)
	subs := map[string]map[pluginapi.Phase]bool{}
	for _, p := range i.manifest.Provides {
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
		if !ok || d.Permission != grant.Permission || d.Resource == nil || d.Params == nil || d.Result == nil || m.Handle == nil {
			return failure(ErrMethod, "invalid or undeclared registration %s", d.Name)
		}
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
			return others[i].Phase == pluginapi.PhaseValidate
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

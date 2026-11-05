package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
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
	// SourcePlanes selects only committed source-restored workflow namespaces.
	// The selected driver and stored origin are verified during admission.
	SourcePlanes []driver.SourcePlaneProviderCommitV1
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
	mu                       sync.Mutex
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
	sourceStates             map[string]*durableAccess
	closed                   bool
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

// affinityState is the ownership record for one principal. The generation
// and done channel make a lease self-identifying: a late callback cannot
// release a newer generation or reopen a retired guest.
type affinityState struct {
	member     *instance
	generation uint64
	leased     bool
	done       chan struct{}
}

type poolLease struct {
	member     *instance
	key        string
	generation uint64
	state      *affinityState
}

type instancePool struct {
	available    chan *instance
	native       sdk.Native
	manifest     pluginapi.PluginManifest
	owner        *Host
	state        *stateStore
	mu           sync.Mutex
	affinity     map[string]*affinityState
	leased       map[*instance]bool
	retiring     map[*instance]bool
	notify       chan struct{}
	generation   uint64
	slots        int
	constructing int
	waiters      int
	constructWG  sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	closed       bool
}

func newInstancePool(ctx context.Context, native sdk.Native, owner *Host, source *instance) (*instancePool, error) {
	poolCtx, cancel := context.WithCancel(context.Background())
	pool := &instancePool{
		available: make(chan *instance, maxRuntimeInstances),
		native:    native,
		manifest:  source.manifest,
		owner:     owner,
		state:     source.state,
		affinity:  map[string]*affinityState{},
		leased:    map[*instance]bool{},
		retiring:  map[*instance]bool{},
		notify:    make(chan struct{}),
		slots:     maxRuntimeInstances,
		ctx:       poolCtx,
		cancel:    cancel,
	}
	// The source instance owns extension methods. Each binding gets four fresh
	// callback guests; only the host-side state store crosses that boundary.
	for n := 0; n < maxRuntimeInstances; n++ {
		member, err := newInstanceWithContext(ctx, native, source.manifest, owner, source.state, source.durable)
		if err != nil {
			pool.close()
			return nil, err
		}
		if err := validateRegistration(member); err != nil {
			member.dispose()
			pool.close()
			return nil, err
		}
		pool.available <- member
	}
	return pool, nil
}

func (p *instancePool) newMember(ctx context.Context) (*instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	member, err := newInstanceWithContext(ctx, p.native, p.manifest, p.owner, p.state, p.owner.durableFor(p.manifest.Alias, p.native.Instance))
	if err != nil {
		return nil, err
	}
	if err := validateRegistration(member); err != nil {
		member.dispose()
		return nil, err
	}
	return member, nil
}

// broadcastLocked wakes every waiter that could have been affected by the
// state transition. Replacing the channel gives each waiter its own
// generation and avoids a single-token wrong-waiter race.
func (p *instancePool) broadcastLocked() {
	previous := p.notify
	p.notify = make(chan struct{})
	close(previous)
}

func (p *instancePool) nextGenerationLocked() uint64 {
	p.generation++
	return p.generation
}

func (p *instancePool) newAffinityLocked(member *instance, leased bool) *affinityState {
	return &affinityState{member: member, generation: p.nextGenerationLocked(), leased: leased, done: make(chan struct{})}
}

func (p *instancePool) newPendingLocked() *affinityState {
	return &affinityState{generation: p.nextGenerationLocked(), done: make(chan struct{})}
}

// takeAvailableLocked consumes only genuinely unbound members. A member that
// was disposed is never returned to a different principal.
func (p *instancePool) takeAvailableLocked() *instance {
	for {
		select {
		case member := <-p.available:
			if member == nil || member.isDisposed() || p.retiring[member] {
				continue
			}
			return member
		default:
			return nil
		}
	}
}

func (p *instancePool) idleAffinityLocked() (string, *instance) {
	owner := ""
	var chosen *instance
	for key, state := range p.affinity {
		if state.member == nil || state.leased || p.retiring[state.member] || state.member.isDisposed() {
			continue
		}
		if chosen == nil || key < owner {
			owner, chosen = key, state.member
		}
	}
	return owner, chosen
}

func (p *instancePool) wait(queueCtx context.Context, done <-chan struct{}) error {
	p.mu.Lock()
	p.waiters++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.waiters--
		p.mu.Unlock()
	}()
	select {
	case <-done:
		return nil
	case <-queueCtx.Done():
		return queueCtx.Err()
	}
}

func closeAffinityLocked(state *affinityState) {
	if state == nil || state.done == nil {
		return
	}
	close(state.done)
	state.done = nil
}

// constructionContext combines the caller's acquisition bound with the
// pool-owned shutdown context. A host close must cancel both foreground
// replacement and background refill without changing the callback budget.
func (p *instancePool) constructionContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(p.ctx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (p *instancePool) scheduleRefill() {
	p.mu.Lock()
	if p.closed || p.slots >= maxRuntimeInstances {
		p.mu.Unlock()
		return
	}
	p.constructWG.Add(1)
	p.mu.Unlock()
	go p.refillSlot()
}

func (p *instancePool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.cancel()
	members := map[*instance]struct{}{}
	for _, state := range p.affinity {
		closeAffinityLocked(state)
		if state.member != nil {
			members[state.member] = struct{}{}
		}
	}
	for member := range p.leased {
		members[member] = struct{}{}
	}
	for member := range p.retiring {
		members[member] = struct{}{}
	}
	for {
		select {
		case member := <-p.available:
			if member != nil {
				members[member] = struct{}{}
			}
		default:
			p.affinity = map[string]*affinityState{}
			p.leased = map[*instance]bool{}
			p.retiring = map[*instance]bool{}
			p.broadcastLocked()
			p.mu.Unlock()
			for member := range members {
				member.dispose()
				member.waitIdle()
			}
			p.constructWG.Wait()
			return
		}
	}
}

func poolBusy(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	return failure(ErrPluginBusy, "plugin runtime pool queue is full")
}

func (p *instancePool) acquire(ctx context.Context, wait time.Duration, key string) (*poolLease, error) {
	queueCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, failure(ErrInactive, "plugin runtime pool is closed")
		}
		if key != "" {
			if state := p.affinity[key]; state != nil {
				if state.member != nil && !state.leased && !state.member.isDisposed() && !p.retiring[state.member] {
					state.leased = true
					p.leased[state.member] = true
					lease := &poolLease{member: state.member, key: key, generation: state.generation, state: state}
					p.mu.Unlock()
					return lease, nil
				}
				// A healthy leased or pending affinity is a hard reservation for
				// this principal. It may not fall through to another guest.
				done := state.done
				p.mu.Unlock()
				if err := p.wait(queueCtx, done); err != nil {
					return nil, poolBusy(ctx)
				}
				continue
			}
		}

		if member := p.takeAvailableLocked(); member != nil {
			var state *affinityState
			if key != "" {
				state = p.newAffinityLocked(member, true)
				p.affinity[key] = state
			}
			p.leased[member] = true
			lease := &poolLease{member: member, key: key, state: state}
			if state != nil {
				lease.generation = state.generation
			}
			p.mu.Unlock()
			return lease, nil
		}

		// If a previous replacement failed, an unused slot can be filled
		// directly. This keeps capacity recoverable without exceeding four
		// live/constructing members.
		if p.slots < maxRuntimeInstances {
			var state *affinityState
			if key != "" {
				state = p.newPendingLocked()
				p.affinity[key] = state
			}
			p.slots++
			p.constructing++
			p.constructWG.Add(1)
			p.mu.Unlock()
			lease, err := p.construct(ctx, key, state, nil, queueCtx)
			if err == nil {
				return lease, nil
			}
			return nil, err
		}

		// All slots are occupied, so an idle affinity may be retired and
		// replaced, but its private guest state is never reassigned.
		owner, member := p.idleAffinityLocked()
		if member != nil {
			oldState := p.affinity[owner]
			delete(p.affinity, owner)
			closeAffinityLocked(oldState)
			p.retiring[member] = true
			var state *affinityState
			if key != "" {
				state = p.newPendingLocked()
				p.affinity[key] = state
			}
			p.constructing++
			p.constructWG.Add(1)
			p.mu.Unlock()
			lease, err := p.construct(ctx, key, state, member, queueCtx)
			if err == nil {
				return lease, nil
			}
			return nil, err
		}

		done := p.notify
		p.mu.Unlock()
		if err := p.wait(queueCtx, done); err != nil {
			return nil, poolBusy(ctx)
		}
	}
}

// construct fills one reserved slot. The old member is retired before the
// replacement slot is counted, so a slow constructor never creates a fifth
// live guest. queueCtx bounds caller-driven construction; a canceled caller
// leaves capacity recovery to the pool's bounded background refill.
func (p *instancePool) construct(ctx context.Context, key string, state *affinityState, old *instance, queueCtx context.Context) (*poolLease, error) {
	defer p.constructWG.Done()
	if old != nil {
		old.dispose()
		old.waitIdle()
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, failure(ErrInactive, "plugin runtime pool is closed")
		}
		delete(p.retiring, old)
		p.slots--
		p.slots++
		p.mu.Unlock()
	}
	constructionCtx, stop := p.constructionContext(queueCtx)
	defer stop()
	if err := constructionCtx.Err(); err != nil {
		if state != nil {
			p.mu.Lock()
			if p.affinity[key] == state {
				delete(p.affinity, key)
				closeAffinityLocked(state)
				p.broadcastLocked()
			}
			p.mu.Unlock()
		}
		p.mu.Lock()
		p.constructing--
		p.slots--
		p.broadcastLocked()
		p.mu.Unlock()
		p.scheduleRefill()
		return nil, poolBusy(ctx)
	}
	fresh, err := p.newMember(constructionCtx)
	if constructionCtx.Err() != nil && (err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		// Only a context-shaped construction result can be reclassified as
		// queue pressure. Preserve real constructor/manifest failures even when
		// the queue deadline races their return; caller and owner cancellation
		// retain their own semantics.
		switch {
		case ctx.Err() != nil:
			err = ctx.Err()
		case p.ctx.Err() != nil:
			err = failure(ErrInactive, "plugin runtime pool is closed")
		default:
			err = poolBusy(ctx)
		}
	}
	if err != nil {
		if fresh != nil {
			fresh.dispose()
		}
		p.mu.Lock()
		p.constructing--
		p.slots--
		if state != nil && p.affinity[key] == state {
			delete(p.affinity, key)
			closeAffinityLocked(state)
		}
		p.broadcastLocked()
		p.mu.Unlock()
		p.scheduleRefill()
		return nil, err
	}

	p.mu.Lock()
	p.constructing--
	if p.closed {
		p.slots--
		p.mu.Unlock()
		fresh.dispose()
		return nil, failure(ErrInactive, "plugin runtime pool is closed")
	}
	if state != nil {
		if p.affinity[key] != state {
			closeAffinityLocked(state)
			p.slots--
			p.broadcastLocked()
			p.mu.Unlock()
			fresh.dispose()
			p.scheduleRefill()
			return nil, failure(ErrPluginBusy, "plugin runtime affinity reservation was superseded")
		}
		state.member = fresh
		state.leased = true
		p.leased[fresh] = true
		closeAffinityLocked(state)
		state.done = make(chan struct{})
		p.broadcastLocked()
		lease := &poolLease{member: fresh, key: key, generation: state.generation, state: state}
		p.mu.Unlock()
		return lease, nil
	}
	p.leased[fresh] = true
	lease := &poolLease{member: fresh}
	p.broadcastLocked()
	p.mu.Unlock()
	return lease, nil
}

func (p *instancePool) refillSlot() {
	defer p.constructWG.Done()
	ctx, cancel := context.WithTimeout(p.ctx, wasmInitTimeout)
	defer cancel()
	p.mu.Lock()
	if p.closed || p.slots >= maxRuntimeInstances {
		p.mu.Unlock()
		return
	}
	p.slots++
	p.constructing++
	p.mu.Unlock()
	fresh, err := p.newMember(ctx)
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		if fresh != nil {
			fresh.dispose()
		}
		p.mu.Lock()
		p.constructing--
		p.slots--
		p.broadcastLocked()
		p.mu.Unlock()
		return
	}
	p.mu.Lock()
	p.constructing--
	if p.closed {
		p.slots--
		p.mu.Unlock()
		fresh.dispose()
		return
	}
	p.available <- fresh
	p.broadcastLocked()
	p.mu.Unlock()
}

func (p *instancePool) fail(lease *poolLease) {
	member := lease.member
	member.dispose()
	p.mu.Lock()
	if p.leased[member] && (lease.state == nil || (p.affinity[lease.key] == lease.state && lease.state.generation == lease.generation)) {
		delete(p.leased, member)
		if lease.state != nil && p.affinity[lease.key] == lease.state {
			delete(p.affinity, lease.key)
			closeAffinityLocked(lease.state)
		}
		p.retiring[member] = true
		p.broadcastLocked()
	}
	p.mu.Unlock()
	go func() {
		member.waitIdle()
		p.refillRetired(member)
	}()
}

func (p *instancePool) refillRetired(member *instance) {
	member.dispose()
	member.waitIdle()
	p.mu.Lock()
	if p.closed || !p.retiring[member] {
		p.mu.Unlock()
		return
	}
	delete(p.retiring, member)
	p.slots--
	p.slots++
	p.constructing++
	p.constructWG.Add(1)
	p.mu.Unlock()
	defer p.constructWG.Done()
	ctx, cancel := context.WithTimeout(p.ctx, wasmInitTimeout)
	defer cancel()
	fresh, err := p.newMember(ctx)
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		if fresh != nil {
			fresh.dispose()
		}
		p.mu.Lock()
		p.constructing--
		p.slots--
		p.broadcastLocked()
		p.mu.Unlock()
		return
	}
	p.mu.Lock()
	p.constructing--
	if p.closed {
		p.slots--
		p.mu.Unlock()
		fresh.dispose()
		return
	}
	p.available <- fresh
	p.broadcastLocked()
	p.mu.Unlock()
}

func (p *instancePool) finish(lease *poolLease) {
	member := lease.member
	member.waitIdle()
	p.mu.Lock()
	if !p.leased[member] {
		p.mu.Unlock()
		return
	}
	delete(p.leased, member)
	if lease.state == nil && !member.isDisposed() && !p.retiring[member] {
		p.available <- member
		p.broadcastLocked()
		p.mu.Unlock()
		return
	}
	keep := lease.state != nil && p.affinity[lease.key] == lease.state && lease.state.generation == lease.generation && !member.isDisposed() && !p.retiring[member]
	if keep {
		lease.state.leased = false
		closeAffinityLocked(lease.state)
		lease.state.done = make(chan struct{})
		p.broadcastLocked()
		p.mu.Unlock()
		return
	}
	p.retiring[member] = true
	p.broadcastLocked()
	p.mu.Unlock()
	go p.refillRetired(member)
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
	lease, err := p.acquire(ctx, wait, key)
	if err != nil {
		return err
	}
	err = lease.member.run(ctx, phase, timeout, reader, func(execCtx context.Context) error {
		return fn(lease.member, execCtx)
	})
	if err != nil {
		// Detach the generation before returning. Draining the callback and
		// constructing its replacement may continue in the background.
		p.fail(lease)
		return err
	}
	// Successful callbacks have released their gate before instance.run
	// returns, so ownership bookkeeping is completed synchronously.
	p.finish(lease)
	return err
}

type registeredMethod struct {
	sdk.Method
	instance *instance
	grant    pluginapi.MethodGrant
}

// Close retires every binding pool and source instance owned by the host. It
// is safe to call repeatedly; pool shutdown cancels queued/refill work before
// disposing members, and source instances are drained before returning.
func (h *Host) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	pools := make([]*instancePool, 0, len(h.bindings))
	for _, binding := range h.bindings {
		if binding != nil && binding.pool != nil {
			pools = append(pools, binding.pool)
		}
	}
	instances := make([]*instance, 0, len(h.instances))
	for _, instance := range h.instances {
		instances = append(instances, instance)
	}
	h.bindings = nil
	h.instances = map[string]*instance{}
	h.natives = map[string]sdk.Native{}
	h.methods = map[string]registeredMethod{}
	h.mu.Unlock()

	for _, pool := range pools {
		pool.close()
	}
	for _, instance := range instances {
		instance.dispose()
		instance.waitIdle()
	}
	return nil
}

func New(config Config) (*Host, error) {
	return NewWithContext(context.Background(), config)
}

// NewWithContext admits a host while retaining the caller's cancellation
// bound through source construction, WASI linking and initial pool members.
// Once admission returns, replacement construction uses the pool's own
// bounded queue and shutdown contexts.
func NewWithContext(ctx context.Context, config Config) (result *Host, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
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
	h.sourceStates = map[string]*durableAccess{}
	for _, binding := range config.SourcePlanes {
		store, ok := checkpointStore.(driver.WorkflowCheckpointStore)
		if !ok {
			underlying, available := checkpointStore.(driver.Store)
			if !available {
				return nil, driver.ErrUnsupported
			}
			store, err = driver.RequireWorkflowCheckpointStore(underlying, checkpointLease)
			if err != nil {
				return nil, err
			}
		}
		state, sourceErr := NewSourceRuntimeState(ctx, store, checkpointLease, binding)
		if sourceErr != nil {
			return nil, sourceErr
		}
		key := binding.Provider.Alias + "/" + binding.Provider.Instance
		if h.sourceStates[key] != nil {
			return nil, failure(ErrDurableState, "duplicate source runtime namespace")
		}
		h.sourceStates[key] = state.(*durableAccess)
	}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
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
		if p.Requires.State == pluginapi.StateDurable && !persistentCheckpoint && h.sourceStates[key] == nil {
			return nil, failure(ErrDurableState, "plugin %s requires restart-durable state", p.Alias)
		}
		i, err := newInstanceWithContext(ctx, native, p, h, newStateStore(), h.durableFor(p.Alias, native.Instance))
		if err != nil {
			return nil, fmt.Errorf("initialize %s: %w", key, err)
		}
		// Publish ownership before registration so a partially registered
		// instance is included in the rollback path if validation fails.
		h.instances[key] = i
		h.natives[key] = native
		if err := h.register(i); err != nil {
			return nil, err
		}
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
		pool, err := newInstancePool(ctx, h.natives[binding.Alias+"/"+binding.Instance], h, i)
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
	return newInstanceWithContext(context.Background(), native, manifest, owner, state, durable)
}

func newInstanceWithContext(ctx context.Context, native sdk.Native, manifest pluginapi.PluginManifest, owner *Host, state *stateStore, durable *durableAccess) (*instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	copyManifest, err := pluginapi.CloneJSON(manifest)
	if err != nil {
		return nil, err
	}
	i := &instance{manifest: copyManifest, name: native.Instance, owner: owner, gate: make(chan struct{}, 1), state: state, durable: durable}
	host := &capabilities{instance: i}
	var registration sdk.Registration
	if native.NewWithContext != nil {
		registration, err = native.NewWithContext(ctx, host)
	} else {
		registration, err = native.New(host)
	}
	if err != nil {
		return nil, err
	}
	i.registration = registration
	i.disposeFn = host.onDispose
	return i, nil
}

func (h *Host) durableFor(alias, instance string) *durableAccess {
	if source := h.sourceStates[alias+"/"+instance]; source != nil {
		return source
	}
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

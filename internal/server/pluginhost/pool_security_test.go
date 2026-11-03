package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
	"github.com/buildbetter-app/plugs/sdk/go/conformance"
)

func securityPool(t *testing.T) *instancePool {
	t.Helper()
	h, err := New(Config{Natives: []sdk.Native{conformance.Native(conformance.Options{})}, Bindings: fixtureBindings()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h.bindings[0].pool
}

func statefulSecurityPool(t *testing.T) *instancePool {
	t.Helper()
	manifest := pluginapi.PluginManifest{
		ID: "app.example.stateful", Alias: "stateful", Version: "1.0.0",
		PluginAPI: pluginapi.APIVersion, Runtime: pluginapi.RuntimeNative,
		Provides: []pluginapi.Provision{{Kind: "subscriber", Name: "mutate", Phases: []pluginapi.Phase{pluginapi.PhaseMutate}, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}}},
	}
	native := sdk.Native{
		Manifest: manifest,
		Instance: "default",
		New: func(pluginapi.Host) (sdk.Registration, error) {
			var calls atomic.Int32
			mutator := sdk.MutateFunc(func(context.Context, pluginapi.ProposedChange, pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
				value := strconv.FormatInt(int64(calls.Add(1)), 10)
				return []pluginapi.Patch{
					{Resource: pluginapi.ResourceRef{Kind: "bead", Ref: pluginapi.RefByID("pl-a", "")}, Paths: map[string]json.RawMessage{"fields.normalized": json.RawMessage(value)}},
				}, nil
			})
			return sdk.Registration{Mutators: map[string]pluginapi.Mutator{"mutate": mutator}}, nil
		},
	}
	h, err := New(Config{
		Natives:  []sdk.Native{native},
		Bindings: []Binding{{ID: "stateful", Alias: "stateful", Instance: "default", Subscriber: "mutate", Phase: pluginapi.PhaseMutate, Reads: []string{"fields.title"}, Owns: []string{"fields.normalized"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h.bindings[0].pool
}

func runStateful(t *testing.T, p *instancePool, key string, after func() error) (int, error) {
	t.Helper()
	count := 0
	err := p.run(context.Background(), pluginapi.PhaseMutate, time.Second, nil, func(member *instance, ctx context.Context) error {
		patches, err := member.registration.Mutators["mutate"].Mutate(ctx, pluginapi.ProposedChange{}, nil)
		if err != nil {
			return err
		}
		if len(patches) != 1 {
			return errors.New("stateful mutator returned no patch")
		}
		count, err = strconv.Atoi(string(patches[0].Paths["fields.normalized"]))
		if err != nil {
			return err
		}
		if after != nil {
			return after()
		}
		return nil
	}, key)
	return count, err
}

func TestPoolSecurityMutableGuestStateAndFreshPrincipalBoundary(t *testing.T) {
	p := statefulSecurityPool(t)
	if count, err := runStateful(t, p, "principal-a", nil); err != nil || count != 1 {
		t.Fatalf("first principal callback=%d err=%v", count, err)
	}
	if count, err := runStateful(t, p, "principal-a", nil); err != nil || count != 2 {
		t.Fatalf("same-principal callback did not retain guest state: count=%d err=%v", count, err)
	}
	if count, err := runStateful(t, p, "principal-b", nil); err != nil || count != 1 {
		t.Fatalf("cross-principal callback inherited guest state: count=%d err=%v", count, err)
	}
	for _, key := range []string{"principal-c", "principal-d"} {
		if count, err := runStateful(t, p, key, nil); err != nil || count != 1 {
			t.Fatalf("fresh principal %s callback=%d err=%v", key, count, err)
		}
	}
	if count, err := runStateful(t, p, "principal-e", nil); err != nil || count != 1 {
		t.Fatalf("fifth principal inherited retired guest state: count=%d err=%v", count, err)
	}
}

func TestPoolSecurityRefusedGuestStateIsNeverReused(t *testing.T) {
	p := statefulSecurityPool(t)
	var refuse atomic.Bool
	count, err := runStateful(t, p, "principal-refused", func() error {
		if !refuse.Swap(true) {
			return errors.New("deliberate callback refusal")
		}
		return nil
	})
	if err == nil || count != 1 {
		t.Fatalf("initial refused callback count=%d err=%v", count, err)
	}
	count, err = runStateful(t, p, "principal-refused", nil)
	if err != nil || count != 1 {
		t.Fatalf("refused guest state was reused: count=%d err=%v", count, err)
	}
}

func TestPoolSecurityOwnerCloseWakesGenerationWaiters(t *testing.T) {
	p := statefulSecurityPool(t)
	lease, err := p.acquire(context.Background(), time.Second, "principal-close")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, waitErr := p.acquire(context.Background(), time.Second, "principal-close")
		result <- waitErr
	}()
	deadline := time.After(time.Second)
	for {
		p.mu.Lock()
		waiting := p.waiters > 0
		p.mu.Unlock()
		if waiting {
			break
		}
		select {
		case <-deadline:
			t.Fatal("same-generation waiter did not register")
		default:
			runtime.Gosched()
		}
	}
	if err := p.owner.Close(); err != nil {
		t.Fatal(err)
	}
	if waitErr := <-result; code(waitErr) != ErrInactive {
		t.Fatalf("owner close did not wake waiter with inactive error: %v", waitErr)
	}
	_ = lease
}

func TestPoolSecurityGenerationSignalsOnlyItsPrincipal(t *testing.T) {
	p := securityPool(t)
	first, err := p.acquire(context.Background(), time.Second, "principal-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.acquire(context.Background(), time.Second, "principal-b")
	if err != nil {
		t.Fatal(err)
	}
	aReady := make(chan *poolLease, 1)
	bReady := make(chan *poolLease, 1)
	go func() {
		lease, waitErr := p.acquire(context.Background(), time.Second, "principal-a")
		if waitErr == nil {
			aReady <- lease
		}
	}()
	go func() {
		lease, waitErr := p.acquire(context.Background(), time.Second, "principal-b")
		if waitErr == nil {
			bReady <- lease
		}
	}()
	deadline := time.After(time.Second)
	for {
		p.mu.Lock()
		waiting := p.waiters == 2
		p.mu.Unlock()
		if waiting {
			break
		}
		select {
		case <-deadline:
			t.Fatal("both principal waiters did not register")
		default:
			runtime.Gosched()
		}
	}
	p.finish(first)
	var nextA *poolLease
	select {
	case nextA = <-aReady:
	case <-time.After(time.Second):
		t.Fatal("principal-a generation was not released")
	}
	select {
	case <-bReady:
		t.Fatal("principal-b waiter was woken by principal-a generation")
	default:
	}
	p.finish(nextA)
	p.finish(second)
	select {
	case nextB := <-bReady:
		p.finish(nextB)
	case <-time.After(time.Second):
		t.Fatal("principal-b generation was not released")
	}
}

func TestHostNewRollsBackSourceAndPartialPool(t *testing.T) {
	manifest := pluginapi.PluginManifest{
		ID: "app.example.rollback", Alias: "rollback", Version: "1.0.0",
		PluginAPI: pluginapi.APIVersion, Runtime: pluginapi.RuntimeNative,
		Provides: []pluginapi.Provision{{Kind: "subscriber", Name: "mutate", Phases: []pluginapi.Phase{pluginapi.PhaseMutate}}},
	}
	var calls, disposed atomic.Int32
	registration := func(host pluginapi.Host) (sdk.Registration, error) {
		if c, ok := host.(*capabilities); ok {
			c.onDispose = func() { disposed.Add(1) }
		}
		return sdk.Registration{Mutators: map[string]pluginapi.Mutator{"mutate": sdk.MutateFunc(func(context.Context, pluginapi.ProposedChange, pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
			return nil, nil
		})}}, nil
	}
	native := sdk.Native{Manifest: manifest, Instance: "default", New: func(host pluginapi.Host) (sdk.Registration, error) {
		return registration(host)
	}, NewWithContext: func(_ context.Context, host pluginapi.Host) (sdk.Registration, error) {
		if calls.Add(1) == 5 {
			return sdk.Registration{}, errors.New("fifth guest construction failed")
		}
		return registration(host)
	}}
	_, err := New(Config{Natives: []sdk.Native{native}, Bindings: []Binding{{ID: "rollback", Alias: "rollback", Instance: "default", Subscriber: "mutate", Phase: pluginapi.PhaseMutate}}})
	if err == nil {
		t.Fatal("partial pool construction unexpectedly succeeded")
	}
	if got := disposed.Load(); got != 4 {
		t.Fatalf("rollback disposed %d completed source/pool instances, want 4", got)
	}
}

func TestPoolSecurityAtomicAffinityAndFreshPrincipalGuests(t *testing.T) {
	p := securityPool(t)
	first, err := p.acquire(context.Background(), time.Second, "principal-a")
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	state := p.affinity["principal-a"]
	if state == nil || !state.leased || state.member != first.member || !p.leased[first.member] {
		p.mu.Unlock()
		t.Fatal("first affinity reservation was not committed before acquire returned")
	}
	p.mu.Unlock()

	// The same principal waits for its own generation even though three clean
	// unbound guests are available. The bounded wait makes this deterministic.
	if _, err := p.acquire(context.Background(), 20*time.Millisecond, "principal-a"); code(err) != ErrPluginBusy {
		t.Fatalf("leased affinity fell through: %v", err)
	}
	p.finish(first)
	second, err := p.acquire(context.Background(), time.Second, "principal-a")
	if err != nil {
		t.Fatal(err)
	}
	if second.member != first.member {
		t.Fatal("same-principal continuation changed guest generation")
	}
	p.finish(second)

	// A different principal takes a clean unbound guest. It never inherits the
	// first principal's private module state.
	other, err := p.acquire(context.Background(), time.Second, "principal-b")
	if err != nil {
		t.Fatal(err)
	}
	if other.member == first.member {
		t.Fatal("different principal reused a private guest")
	}
	p.finish(other)
}

func TestPoolSecurityFifthPrincipalRetiresIdleGuest(t *testing.T) {
	p := securityPool(t)
	keys := []string{"principal-a", "principal-b", "principal-c", "principal-d"}
	members := make(map[string]*instance, len(keys))
	for _, key := range keys {
		lease, err := p.acquire(context.Background(), time.Second, key)
		if err != nil {
			t.Fatal(err)
		}
		members[key] = lease.member
		p.finish(lease)
	}
	fifth, err := p.acquire(context.Background(), time.Second, "principal-e")
	if err != nil {
		t.Fatal(err)
	}
	for key, member := range members {
		if fifth.member == member {
			t.Fatalf("fifth principal inherited %s guest", key)
		}
	}
	p.mu.Lock()
	if p.affinity["principal-a"] != nil || p.slots != maxRuntimeInstances || len(p.leased) != 1 {
		p.mu.Unlock()
		t.Fatalf("fifth-principal retirement state is inconsistent: affinity=%d slots=%d leased=%d", len(p.affinity), p.slots, len(p.leased))
	}
	p.mu.Unlock()
	p.finish(fifth)
}

func TestPoolSecurityFailureDetachesBeforeLateCallbackDrain(t *testing.T) {
	p := securityPool(t)
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- p.run(context.Background(), pluginapi.PhaseMutate, 10*time.Millisecond, nil, func(_ *instance, _ context.Context) error {
			close(started)
			<-release
			return nil
		}, "principal-failing")
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("failure callback did not start")
	}
	err := <-result
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failure did not preserve callback deadline: %v", err)
	}
	p.mu.Lock()
	if p.affinity["principal-failing"] != nil || len(p.leased) != 0 {
		p.mu.Unlock()
		t.Fatal("failed generation remained published after run returned")
	}
	p.mu.Unlock()

	// A clean guest is usable while the failed member is still draining.
	other, err := p.acquire(context.Background(), time.Second, "principal-other")
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	if p.retiring[other.member] {
		p.mu.Unlock()
		t.Fatal("replacement path leased a retiring guest")
	}
	p.mu.Unlock()
	p.finish(other)
	close(release)
}

func TestPoolSecurityCanceledConstructionRespectsFourLiveCeiling(t *testing.T) {
	native := conformance.Native(conformance.Options{})
	original := native.New
	var calls atomic.Int32
	constructionStarted := make(chan struct{})
	native.NewWithContext = func(ctx context.Context, host pluginapi.Host) (sdk.Registration, error) {
		if calls.Add(1) == 6 {
			close(constructionStarted)
			<-ctx.Done()
			return sdk.Registration{}, ctx.Err()
		}
		return original(host)
	}
	h, err := New(Config{Natives: []sdk.Native{native}, Bindings: []Binding{fixtureBindings()[0]}})
	if err != nil {
		t.Fatal(err)
	}
	p := h.bindings[0].pool
	for _, key := range []string{"a", "b", "c", "d"} {
		lease, err := p.acquire(context.Background(), time.Second, key)
		if err != nil {
			t.Fatal(err)
		}
		p.finish(lease)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, acquireErr := p.acquire(ctx, time.Second, "e")
		result <- acquireErr
	}()
	select {
	case <-constructionStarted:
	case <-time.After(time.Second):
		t.Fatal("replacement construction did not start")
	}
	p.mu.Lock()
	if p.slots != maxRuntimeInstances || p.constructing != 1 {
		p.mu.Unlock()
		t.Fatalf("construction exceeded live-slot ceiling: slots=%d constructing=%d", p.slots, p.constructing)
	}
	p.mu.Unlock()
	cancel()
	if acquireErr := <-result; !errors.Is(acquireErr, context.Canceled) {
		t.Fatalf("canceled construction error=%v", acquireErr)
	}
	// The bounded background refill uses the pool context and restores a clean
	// unbound slot after the caller's construction is canceled.
	select {
	case member := <-p.available:
		if member == nil || member.isDisposed() {
			t.Fatal("background refill published an invalid guest")
		}
		p.available <- member
	case <-time.After(time.Second):
		t.Fatal("capacity was not recovered after canceled construction")
	}
}

func TestPoolConstructionPreservesManifestErrorAfterQueueExpiry(t *testing.T) {
	native := conformance.Native(conformance.Options{})
	original := native.New
	var constructions atomic.Int32
	native.NewWithContext = func(ctx context.Context, host pluginapi.Host) (sdk.Registration, error) {
		if constructions.Add(1) != maxRuntimeInstances+2 {
			return original(host)
		}
		<-ctx.Done()
		return sdk.Registration{}, failure(ErrManifest, "constructor manifest failure")
	}
	// Keep one binding in this constructor-race probe so the sixth constructor
	// is the first queue-expiry replacement, rather than an unrelated second
	// pool's initial member. The production pool still retains its four-live
	// ceiling; this test isolates the error-classification boundary.
	h, err := New(Config{Natives: []sdk.Native{native}, Bindings: fixtureBindings()[:1]})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	p := h.bindings[0].pool
	for _, key := range []string{"manifest-a", "manifest-b", "manifest-c", "manifest-d"} {
		lease, err := p.acquire(context.Background(), time.Second, key)
		if err != nil {
			t.Fatal(err)
		}
		p.finish(lease)
	}
	if _, err := p.acquire(context.Background(), 20*time.Millisecond, "manifest-e"); code(err) != ErrManifest {
		t.Fatalf("queue expiry replaced a real constructor failure: %v", err)
	}
}

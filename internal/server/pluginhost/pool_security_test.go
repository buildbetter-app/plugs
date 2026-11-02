package pluginhost

import (
	"context"
	"errors"
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
	return h.bindings[0].pool
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

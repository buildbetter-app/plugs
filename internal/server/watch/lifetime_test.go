package watch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// delayedSessionLifetime keeps the authoritative Done/Err synchronous while
// withholding AfterFunc callbacks, just as a scheduler can delay notification.
// Hiding Value forces context.AfterFunc to use this explicit test-only port.
type delayedSessionLifetime struct {
	context.Context
	notify <-chan struct{}
}

func (c delayedSessionLifetime) Value(any) any { return nil }

func (c delayedSessionLifetime) AfterFunc(fn func()) func() bool {
	return context.AfterFunc(c.Context, func() { <-c.notify; fn() })
}

func newDelayedSessionLifetime(t *testing.T, parent context.Context) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	notify := make(chan struct{})
	t.Cleanup(func() { close(notify); cancel() })
	return delayedSessionLifetime{Context: ctx, notify: notify}, cancel
}

func TestSessionLifetimeRevokesWatchBeforeNotification(t *testing.T) {
	for _, scenario := range []string{"buffered-delivery", "blocked-delivery", "attach", "attach-session", "cleanup"} {
		t.Run(scenario, func(t *testing.T) {
			f := newWatchFixture(t, 8, CoalesceWindow, time.Second)
			lifetime, cancel := newDelayedSessionLifetime(t, f.ctx)
			req := f.request("delayed-" + scenario)
			sub, err := f.manager.Open(lifetime, req)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "blocked-delivery" {
				_ = nextFrame(t, sub, time.Second)
				result := make(chan error, 1)
				go func() { _, err := sub.Next(f.ctx); result <- err }()
				cancel()
				select {
				case err := <-result:
					if !errors.Is(err, ErrSubscriptionClosed) {
						t.Fatalf("blocked delivery after session close: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("blocked delivery waited for asynchronous session notification")
				}
			} else {
				cancel()
				switch scenario {
				case "buffered-delivery":
					if _, err := sub.Next(f.ctx); !errors.Is(err, ErrSubscriptionClosed) {
						t.Fatalf("buffered delivery after session close: %v", err)
					}
				case "attach":
					if _, err := f.manager.Attach(f.ctx, sub.ID(), req.Principal); !errors.Is(err, ErrSubscriptionClosed) {
						t.Fatalf("attachment after session close: %v", err)
					}
				case "attach-session":
					if _, err := f.manager.AttachSession(f.ctx, sub.ID(), req.SessionID, req.Principal); !errors.Is(err, ErrSubscriptionClosed) {
						t.Fatalf("session attachment after session close: %v", err)
					}
				}
			}
			select {
			case <-sub.watch.done:
			case <-time.After(time.Second):
				t.Fatal("watch cleanup waited for asynchronous session notification")
			}
			if count := f.manager.WatchCount(req.SessionID); count != 0 {
				t.Fatalf("closed session retained %d watches", count)
			}
		})
	}
}

func TestSessionLifetimeEndsDuringWatchRegistration(t *testing.T) {
	f := newWatchFixture(t, 8, CoalesceWindow, time.Second)
	lifetime, cancel := newDelayedSessionLifetime(t, f.ctx)
	f.manager.config.IDGenerator = func() (string, error) {
		cancel()
		return "closed-during-registration", nil
	}
	req := f.request("")
	if sub, err := f.manager.Open(lifetime, req); sub != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("registered watch after session close: sub=%v err=%v", sub, err)
	}
	if count := f.manager.WatchCount(req.SessionID); count != 0 {
		t.Fatalf("registration retained %d watches for a closed session", count)
	}
}

func TestSessionLifetimeEndsDuringOpeningRead(t *testing.T) {
	f := newWatchFixture(t, 8, CoalesceWindow, time.Second)
	lifetime, cancel := newDelayedSessionLifetime(t, f.ctx)
	gate := &scanGate{inner: f.manager.config.Events.Reader.Log, started: make(chan struct{}), release: make(chan struct{})}
	f.manager.config.Events.Reader.Log = gate
	var released atomic.Bool
	release := func() {
		if released.CompareAndSwap(false, true) {
			close(gate.release)
		}
	}
	t.Cleanup(release)
	req := f.request("closed-during-opening")
	type openResult struct {
		sub *Subscription
		err error
	}
	result := make(chan openResult, 1)
	go func() {
		sub, err := f.manager.Open(lifetime, req)
		result <- openResult{sub: sub, err: err}
	}()
	select {
	case <-gate.started:
	case <-time.After(time.Second):
		t.Fatal("opening read did not reach the scan gate")
	}
	cancel()
	release()
	select {
	case opened := <-result:
		if opened.sub != nil || !errors.Is(opened.err, ErrSubscriptionClosed) {
			t.Fatalf("opening read survived session close: sub=%v err=%v", opened.sub, opened.err)
		}
	case <-time.After(time.Second):
		t.Fatal("opening read did not finish after session close")
	}
	if count := f.manager.WatchCount(req.SessionID); count != 0 {
		t.Fatalf("opening read retained %d watches for a closed session", count)
	}
}

func TestSessionLifetimeEndsDuringDeliveryRevalidation(t *testing.T) {
	f := newWatchFixture(t, 8, CoalesceWindow, time.Second)
	lifetime, cancel := newDelayedSessionLifetime(t, f.ctx)
	var armed atomic.Bool
	f.manager.config.Bindings = bindingFunc(func(_ context.Context, req Request) (Binding, error) {
		if armed.CompareAndSwap(true, false) {
			cancel()
		}
		return Binding{Principal: req.Principal, Filter: req.Filter, Ontology: req.Ontology, PolicyVersion: req.PolicyVersion}, nil
	})
	sub, err := f.manager.Open(lifetime, f.request("closed-during-revalidation"))
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	if _, err := sub.Next(f.ctx); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("delivery revalidation survived session close: %v", err)
	}
}

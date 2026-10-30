package collabwatch_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	"github.com/buildbetter-app/plugs/internal/server/watch"
)

// presenceHeadOverride presents a takeover incarnation through the same log
// boundary the watch manager reads. Scans still use the real memory log; the
// gap case below deliberately keeps its returned head at revision zero, so no
// historical data is fabricated for the acceptance.
type presenceHeadOverride struct {
	inner coreevents.Log

	mu          sync.RWMutex
	incarnation string
}

func (l *presenceHeadOverride) Head(ctx context.Context) (coreevents.Head, error) {
	head, err := l.inner.Head(ctx)
	if err != nil {
		return head, err
	}
	l.mu.RLock()
	incarnation := l.incarnation
	l.mu.RUnlock()
	if incarnation != "" {
		head.Incarnation = incarnation
		head.Revision = model.Natural64(0)
		head.Floor = model.Natural64(0)
	}
	return head, nil
}

func (l *presenceHeadOverride) Scan(ctx context.Context, after coreevents.Position, limit int) ([]model.Changeset, error) {
	return l.inner.Scan(ctx, after, limit)
}

func (l *presenceHeadOverride) setIncarnation(value string) {
	l.mu.Lock()
	l.incarnation = value
	l.mu.Unlock()
}

func waitForWatchCount(t *testing.T, f *fixture, sessionID string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := f.watch.WatchCount(sessionID); got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("watch count for %s did not settle at %d: got %d", sessionID, want, f.watch.WatchCount(sessionID))
}

func TestPresenceWatchRejectsTamperedAndReboundCursorsWithoutWrites(t *testing.T) {
	f := newFixture(t)
	sessionID := f.session(t, alice)
	subscription := f.presenceWatch(t, alice, sessionID, "")
	initial := nextFrame(t, subscription)
	if initial.Kind != watch.FrameSnapshot || initial.Cursor == "" {
		t.Fatalf("presence initial snapshot=%+v", initial)
	}
	before := durableState(t, f)

	tampered := initial.Cursor + "tampered"
	if code := errorCode(t, f.presenceWatchResponse(t, alice, sessionID, tampered)); code != "cursor_invalid" {
		t.Fatalf("tampered cursor code=%s", code)
	}
	if got := f.watch.WatchCount(sessionID); got != 1 {
		t.Fatalf("tampered cursor retained a watch: count=%d", got)
	}

	if code := errorCode(t, f.subscribeResponse(t, alice, sessionID, initial.Cursor)); code != "cursor_invalid" {
		t.Fatalf("presence cursor replayed as event.subscribe: code=%s", code)
	}
	if got := f.watch.WatchCount(sessionID); got != 1 {
		t.Fatalf("method-bound cursor refusal retained a watch: count=%d", got)
	}

	otherSession := f.session(t, bob)
	if code := errorCode(t, f.presenceWatchResponse(t, bob, otherSession, initial.Cursor)); code != "cursor_invalid" {
		t.Fatalf("principal-bound cursor replay code=%s", code)
	}
	if got := f.watch.WatchCount(otherSession); got != 0 {
		t.Fatalf("principal-bound cursor refusal retained a watch: count=%d", got)
	}
	if after := durableState(t, f); string(before) != string(after) {
		t.Fatal("cursor refusals changed durable rows, history, or head")
	}
}

func TestPresenceWatchExpiryAndCloseCancelDeliveryLifetime(t *testing.T) {
	f := newFixture(t)
	closedSession := f.session(t, alice)
	closedSubscription := f.presenceWatch(t, alice, closedSession, "")
	_ = nextFrame(t, closedSubscription)
	beforeClose := durableState(t, f)
	if _, err := f.callSessionClose(t, alice, closedSession); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := closedSubscription.Next(ctx); !errors.Is(err, watch.ErrSubscriptionClosed) {
		t.Fatalf("session close did not cancel presence watch: %v", err)
	}
	if rows := presenceRows(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-a"})); len(rows) != 0 {
		t.Fatalf("closed session remained visible: %v", rows)
	}
	if afterClose := durableState(t, f); string(beforeClose) != string(afterClose) {
		t.Fatal("session close changed durable rows, history, or head")
	}

	expiredSession := f.session(t, alice)
	expiredSubscription := f.presenceWatch(t, alice, expiredSession, "")
	_ = nextFrame(t, expiredSubscription)
	f.clock.advance(collab.SessionTTL)
	if rows := presenceRows(t, f.call(t, bob, "presence.list", map[string]any{"project": "project-a"})); len(rows) != 0 {
		t.Fatalf("expired session remained visible: %v", rows)
	}
	if _, err := expiredSubscription.Next(ctx); !errors.Is(err, watch.ErrSubscriptionClosed) {
		t.Fatalf("session expiry did not cancel presence watch: %v", err)
	}
	waitForWatchCount(t, f, expiredSession, 0)
}

func TestPresenceWatchIncarnationChangeEmitsEventGap(t *testing.T) {
	log := &presenceHeadOverride{}
	f := newConfiguredFixtureWithLog(t, nil, func(inner coreevents.Log) coreevents.Log {
		log.inner = inner
		return log
	})
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := f.session(t, alice)
	subscription := f.presenceWatch(t, alice, sessionID, "")
	initial := nextFrame(t, subscription)
	if initial.Kind != watch.FrameSnapshot || initial.Cursor == "" {
		t.Fatalf("presence initial snapshot=%+v", initial)
	}
	newIncarnation := "presence-watch-takeover"
	log.setIncarnation(newIncarnation)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	gap, err := subscription.Next(ctx)
	if err != nil {
		t.Fatalf("incarnation change delivery: %v", err)
	}
	if gap.Kind != watch.FrameReset || gap.Reason != "" || len(gap.Events) != 1 || gap.Events[0].Control == nil || gap.Events[0].Control.Type != model.EventGap {
		t.Fatalf("incarnation gap frame=%+v", gap)
	}
	details, ok := gap.Events[0].Control.Details.(model.EventGapDetails)
	if !ok || details.FromIncarnation != meta.Incarnation || details.ToIncarnation != newIncarnation || details.Reason != "failover" {
		t.Fatalf("incarnation gap details=%+v", gap.Events[0].Control.Details)
	}
	if _, err := subscription.Next(ctx); !errors.Is(err, watch.ErrResetRequired) {
		t.Fatalf("incarnation gap stream resumed: %v", err)
	}
}

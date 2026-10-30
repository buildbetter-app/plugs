package mcp

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestNotificationHubClosesSlowStreamAtFixedQueueBound(t *testing.T) {
	hub := newNotificationHub()
	principal := model.PrincipalRef{Kind: "user", ID: "user:slow"}
	key, stream, err := hub.open(principal, "session-slow")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < notificationQueueDepth; i++ {
		if !hub.publish(context.Background(), principal, "session-slow", []byte(`{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}`)) {
			t.Fatalf("queue refused frame %d before reaching its bound", i)
		}
	}
	if hub.publish(context.Background(), principal, "session-slow", []byte(`{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}`)) {
		t.Fatal("full notification queue accepted an additional frame")
	}
	if _, ok := <-stream.queue; !ok {
		t.Fatal("slow stream closed without retaining its bounded queued frames")
	}
	for range stream.queue {
	}
	hub.mu.Lock()
	remaining := len(hub.streams)
	hub.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("slow stream remained registered after overflow: %d", remaining)
	}
	if key == "" {
		t.Fatal("stream key was empty")
	}
}

func TestNotificationHubReconnectReplacesAndCloseReleasesStreams(t *testing.T) {
	hub := newNotificationHub()
	principal := model.PrincipalRef{Kind: "user", ID: "user:reconnect"}
	keyA, streamA, err := hub.open(principal, "session-reconnect")
	if err != nil {
		t.Fatal(err)
	}
	keyB, streamB, err := hub.open(principal, "session-reconnect")
	if err != nil {
		t.Fatal(err)
	}
	if keyA != keyB {
		t.Fatalf("reconnect changed stream key: %q vs %q", keyA, keyB)
	}
	if _, ok := <-streamA.queue; ok {
		t.Fatal("replaced stream remained open")
	}
	hub.close()
	if _, ok := <-streamB.queue; ok {
		t.Fatal("hub close left current stream open")
	}
	if _, _, err := hub.open(principal, "session-new"); err != errNotificationHubClosed {
		t.Fatalf("closed hub accepted a stream: %v", err)
	}
}

func TestNotificationHubCanceledPublishDoesNotQueue(t *testing.T) {
	hub := newNotificationHub()
	principal := model.PrincipalRef{Kind: "user", ID: "user:cancel"}
	_, stream, err := hub.open(principal, "session-cancel")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if hub.publish(ctx, principal, "session-cancel", []byte(`{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}`)) {
		t.Fatal("canceled publication was queued")
	}
	select {
	case <-stream.queue:
		t.Fatal("canceled publication reached the stream")
	default:
	}
	hub.remove(notificationStreamKey(principal, "session-cancel"), stream)
}

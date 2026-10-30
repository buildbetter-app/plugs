package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Notification streams are deliberately ephemeral and bounded. A stream has
// one fixed queue; a full queue closes that stream instead of blocking a
// canonical RPC request or silently dropping a notification. There is no
// replay after disconnect, restart or queue overflow.
const (
	maxNotificationStreams = 64
	notificationQueueDepth = 16
	maxNotificationBytes   = 64 << 10
)

var (
	errNotificationHubClosed = errors.New("mcp: notification hub is closed")
	errNotificationStreamCap = errors.New("mcp: notification stream limit reached")
)

type notificationHub struct {
	mu      sync.Mutex
	streams map[string]*notificationStream
	closed  bool
}

type notificationStream struct {
	mu     sync.Mutex
	queue  chan []byte
	closed bool
}

func newNotificationHub() *notificationHub {
	return &notificationHub{streams: make(map[string]*notificationStream)}
}

func notificationStreamKey(principal model.PrincipalRef, sessionID string) string {
	encoded, _ := json.Marshal(principal)
	return string(encoded) + "\x00" + sessionID
}

func (h *notificationHub) open(principal model.PrincipalRef, sessionID string) (string, *notificationStream, error) {
	if h == nil || sessionID == "" {
		return "", nil, errNotificationHubClosed
	}
	key := notificationStreamKey(principal, sessionID)
	stream := &notificationStream{queue: make(chan []byte, notificationQueueDepth)}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return "", nil, errNotificationHubClosed
	}
	if previous := h.streams[key]; previous == nil && len(h.streams) >= maxNotificationStreams {
		h.mu.Unlock()
		return "", nil, errNotificationStreamCap
	} else {
		h.streams[key] = stream
		h.mu.Unlock()
		// Reconnecting replaces the previous stream for this authenticated
		// session. The pointer check in remove prevents the old request from
		// removing this new stream when it observes its closed queue.
		if previous != nil {
			previous.close()
		}
	}
	return key, stream, nil
}

func (h *notificationHub) remove(key string, stream *notificationStream) {
	if h == nil || stream == nil {
		return
	}
	h.mu.Lock()
	if current := h.streams[key]; current == stream {
		delete(h.streams, key)
	}
	h.mu.Unlock()
	stream.close()
}

// publish enqueues one JSON-RPC notification for the authenticated session.
// It is intentionally non-blocking: a slow consumer is disconnected at the
// fixed queue bound and the caller can fall back to a finite response.
func (h *notificationHub) publish(ctx context.Context, principal model.PrincipalRef, sessionID string, data []byte) bool {
	if h == nil || sessionID == "" || len(data) == 0 || len(data) > maxNotificationBytes {
		return false
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return false
		default:
		}
	}
	key := notificationStreamKey(principal, sessionID)
	h.mu.Lock()
	stream := h.streams[key]
	h.mu.Unlock()
	if stream == nil {
		return false
	}
	if stream.offer(data) {
		return true
	}
	h.remove(key, stream)
	return false
}

func (h *notificationHub) close() {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	streams := make([]*notificationStream, 0, len(h.streams))
	for key, stream := range h.streams {
		streams = append(streams, stream)
		delete(h.streams, key)
	}
	h.mu.Unlock()
	for _, stream := range streams {
		stream.close()
	}
}

func (s *notificationStream) offer(data []byte) bool {
	if s == nil || len(data) == 0 || len(data) > maxNotificationBytes {
		return false
	}
	copyOfData := append([]byte(nil), data...)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.queue <- copyOfData:
		return true
	default:
		// Closing the stream is the explicit backpressure behavior. The
		// notification that filled the bound is not represented as delivered;
		// no replay is promised after the client reconnects.
		s.closed = true
		close(s.queue)
		return false
	}
}

func (s *notificationStream) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.mu.Unlock()
}

package websocket

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
)

const (
	closeNormal      = 1000
	closeGoingAway   = 1001
	closeProtocol    = 1002
	closeUnsupported = 1003
	closePolicy      = 1008
	closeTooLarge    = 1009
	closeTryAgain    = 1013
	closeInternal    = 1011
)

// Handler is reusable and safe for concurrent connections. It owns no store,
// listener, session or watch manager. A composition root mounts it at Path and
// may use Stream for subscriptions opened through the existing watch adapter.
type Handler struct {
	rpc         *rpc.Server
	authn       *authn.Authenticator
	cfg         Config
	mu          sync.Mutex
	closed      bool
	connections map[*activeConnection]struct{}
	active      sync.WaitGroup
	closeOnce   sync.Once
}

type activeConnection struct {
	cancel context.CancelFunc
	conn   net.Conn
}

func New(server *rpc.Server, authenticator *authn.Authenticator, config Config) (*Handler, error) {
	if server == nil || authenticator == nil {
		return nil, fmt.Errorf("websocket: RPC server and authenticator are required")
	}
	config, err := config.defaults()
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(config.Path, "/") || strings.ContainsAny(config.Path, "?#") {
		return nil, fmt.Errorf("websocket: Path must be an absolute path without query markers")
	}
	return &Handler{rpc: server, authn: authenticator, cfg: config, connections: map[*activeConnection]struct{}{}}, nil
}

// Close stops admission, cancels and closes every owned upgrade, and waits for
// its dispatch and writer to return. net/http.Shutdown does not own hijacked
// connections, so a composition must call Close before releasing the store.
// Dispatchers must honor their contexts, as with ordinary HTTP requests.
func (h *Handler) Close() error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		h.mu.Lock()
		h.closed = true
		connections := make([]activeConnection, 0, len(h.connections))
		for connection := range h.connections {
			connections = append(connections, *connection)
		}
		h.mu.Unlock()
		for _, connection := range connections {
			connection.cancel()
			if connection.conn != nil {
				_ = connection.conn.Close()
			}
		}
		h.active.Wait()
	})
	return nil
}

// ServeHTTP performs the HTTP upgrade only after the existing WebSocket
// admission checks have accepted the request. No HTTP marker is required:
// browsers cannot set Plug-RPC on a WebSocket handshake. Once upgraded,
// every JSON message is passed unchanged to rpc.Server.Dispatch.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Register before admission so Close also joins a handshake already in
	// flight. The closed check and Add share the mutex used by Close's snapshot.
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		h.reject(w, http.StatusServiceUnavailable, "busy", "WebSocket transport is closed", nil)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	active := &activeConnection{cancel: cancel}
	h.connections[active] = struct{}{}
	h.active.Add(1)
	h.mu.Unlock()
	defer func() {
		cancel()
		h.mu.Lock()
		delete(h.connections, active)
		h.mu.Unlock()
		h.active.Done()
	}()
	r = r.WithContext(ctx)
	if r.URL.Path != h.cfg.Path || r.URL.EscapedPath() != h.cfg.Path || r.URL.RawQuery != "" || r.URL.ForceQuery {
		h.reject(w, http.StatusNotFound, "not_found", "unknown WebSocket endpoint", nil)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		h.reject(w, http.StatusMethodNotAllowed, "invalid_request", "use GET /rpc/v1/ws", nil)
		return
	}
	_, err := h.authn.AdmitWebSocket(r)
	if err != nil {
		status := http.StatusForbidden
		if errors.Is(err, authn.ErrUnauthenticated) || errors.Is(err, authn.ErrTokenExpired) {
			status = http.StatusUnauthorized
			w.Header().Set("WWW-Authenticate", "Bearer")
		} else if errors.Is(err, authn.ErrUnavailable) {
			status = http.StatusServiceUnavailable
		}
		h.reject(w, status, "", "", err)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		h.reject(w, http.StatusUpgradeRequired, "forbidden", "WebSocket upgrade is unavailable", nil)
		return
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	active.conn = conn
	h.mu.Unlock()
	if err := handshake(rw, r.Header.Get("Sec-WebSocket-Key")); err != nil {
		return
	}
	connection := newConn(conn, rw, h.cfg)
	// Keep the admitted handshake inputs private to this connection. Recheck
	// them immediately before dispatch so queued messages cannot reuse a
	// detached identity after credential revocation or expiry.
	admission := r.Clone(r.Context())
	h.serveConnection(connection, r.Context(), func(ctx context.Context) (authn.Identity, error) {
		return h.authn.AdmitWebSocket(admission.WithContext(ctx))
	})
}

// reject is used only before an HTTP upgrade. Once the socket is upgraded,
// protocol errors are represented by a WebSocket close frame instead.
func (h *Handler) reject(w http.ResponseWriter, status int, code, hint string, err error) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "application/json")
	var body []byte
	if err != nil {
		body = h.rpc.TransportError(err)
	} else {
		body = h.rpc.TransportError(&model.Error{Data: model.ErrorData{Code: model.ErrorCode(code), Hint: hint}})
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

type outbound struct {
	kind MessageType
	data []byte
}

func (h *Handler) serveConnection(connection *Conn, requestContext context.Context, admit func(context.Context) (authn.Identity, error)) {
	ctx, cancel := context.WithCancel(requestContext)
	defer cancel()

	requests := make(chan []byte, h.cfg.MaxPendingReads)
	outgoing := make(chan outbound, h.cfg.MaxPendingWrites)
	dispatchDone := make(chan struct{})
	writerDone := make(chan struct{})
	var terminateOnce sync.Once
	terminate := func(code uint16, reason string) {
		terminateOnce.Do(func() {
			cancel()
			_ = connection.Close(context.Background(), code, reason)
		})
	}

	go func() {
		defer close(writerDone)
		for {
			select {
			case <-ctx.Done():
				return
			case message, ok := <-outgoing:
				if !ok {
					return
				}
				if err := connection.WriteMessage(ctx, message.kind, message.data); err != nil {
					terminate(closeInternal, "write failed")
					return
				}
			}
		}
	}()

	go func() {
		defer close(dispatchDone)
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-requests:
				if !ok {
					return
				}
				callContext, callCancel := context.WithTimeout(ctx, h.cfg.RequestTimeout)
				identity, err := admit(callContext)
				if err != nil {
					callCancel()
					code, reason := uint16(closePolicy), "authentication no longer valid"
					if errors.Is(err, authn.ErrUnavailable) {
						code, reason = closeTryAgain, "authentication unavailable"
					}
					terminate(code, reason)
					return
				}
				callContext = authn.WithIdentity(callContext, identity)
				callContext = rpc.WithPrincipal(callContext, identity.Principal())
				response := h.rpc.Dispatch(callContext, data)
				callCancel()
				if len(response) > 0 {
					if int64(len(response)) > h.cfg.MaxMessageBytes {
						terminate(closeTooLarge, "response exceeds configured limit")
						return
					}
					if !offer(ctx, outgoing, outbound{kind: TextMessage, data: response}) {
						terminate(closeTryAgain, "slow consumer")
						return
					}
					continue
				}
				for _, rejected := range rejectedNotifications(data) {
					if int64(len(rejected)) > h.cfg.MaxMessageBytes {
						terminate(closeTooLarge, "notification exceeds configured limit")
						return
					}
					if !offer(ctx, outgoing, outbound{kind: TextMessage, data: rejected}) {
						terminate(closeTryAgain, "slow consumer")
						return
					}
				}
			}
		}
	}()

	closeCode, closeReason := uint16(closeNormal), ""
	for {
		messageType, data, err := connection.ReadMessage(ctx)
		if err != nil {
			var peerClose *closeError
			switch {
			case errors.As(err, &peerClose):
				closeCode, closeReason = peerClose.code, peerClose.reason
			case errors.Is(err, ErrMessageTooLarge):
				closeCode, closeReason = closeTooLarge, "message exceeds configured limit"
			case errors.Is(err, ErrProtocol):
				closeCode, closeReason = closeProtocol, "protocol error"
			case errors.Is(err, context.Canceled), errors.Is(err, ErrConnectionClosed):
				closeCode, closeReason = closeGoingAway, "connection closed"
			default:
				closeCode, closeReason = closeGoingAway, "connection closed"
			}
			break
		}
		switch messageType {
		case pingMessage:
			if err := connection.writePong(ctx, data); err != nil {
				closeCode, closeReason = closeGoingAway, "connection closed"
				break
			}
			continue
		case pongMessage:
			continue
		case TextMessage, BinaryMessage:
			select {
			case requests <- data:
			default:
				closeCode, closeReason = closeTryAgain, "request queue full"
				break
			}
			if closeCode != closeNormal {
				break
			}
			continue
		default:
			closeCode, closeReason = closeUnsupported, "unsupported message"
		}
		break
	}

	terminate(closeCode, closeReason)
	close(requests)
	<-dispatchDone
	close(outgoing)
	<-writerDone
	_ = connection.closeNetwork()
}

func offer(ctx context.Context, queue chan<- outbound, message outbound) bool {
	select {
	case queue <- message:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

// Stream delivers canonical watch.Frame event.notify payloads on an already
// upgraded connection. The subscription is always closed on return, so a
// disconnect or write deadline releases the watch manager's queue and reader.
// RPC subscription opening remains owned by watchhttp/watch; this helper only
// adapts its transport-neutral frames to WebSocket messages.
func (h *Handler) Stream(ctx context.Context, connection *Conn, subscription *watch.Subscription) error {
	if h == nil || connection == nil || subscription == nil {
		return watch.ErrInvalidRequest
	}
	defer subscription.Close()
	for {
		frame, err := subscription.Next(ctx)
		if err != nil {
			return err
		}
		data, err := json.Marshal(frame)
		if err != nil {
			return err
		}
		notification := map[string]any{
			"jsonrpc": "2.0",
			"method":  "event.notify",
			"params":  map[string]json.RawMessage{"notification": data},
		}
		payload, err := json.Marshal(notification)
		if err != nil {
			return err
		}
		if int64(len(payload)) > h.cfg.MaxMessageBytes {
			return ErrMessageTooLarge
		}
		if err := connection.WriteMessage(ctx, TextMessage, payload); err != nil {
			return err
		}
		if frame.Terminal() {
			return watch.ErrResetRequired
		}
	}
}

func rejectedNotifications(data []byte) [][]byte {
	var values []json.RawMessage
	if len(data) > 0 && data[0] == '[' {
		if json.Unmarshal(data, &values) != nil {
			return nil
		}
	} else {
		values = []json.RawMessage{data}
	}
	var out [][]byte
	for _, value := range values {
		var object map[string]json.RawMessage
		if json.Unmarshal(value, &object) != nil || object["id"] != nil {
			continue
		}
		var method string
		if json.Unmarshal(object["method"], &method) != nil {
			continue
		}
		params, err := json.Marshal(map[string]any{"reason": "notification_no_result", "requestId": method})
		if err != nil {
			continue
		}
		message, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "rpc.rejected", "params": json.RawMessage(params)})
		if err == nil {
			out = append(out, message)
		}
	}
	return out
}

// handshake is kept separate from ServeHTTP so a real socket fixture can
// exercise the exact HTTP upgrade bytes without involving RPC dispatch.
func handshake(rw *bufio.ReadWriter, key string) error {
	if key == "" {
		return ErrProtocol
	}
	accept := websocketAccept(key)
	if _, err := fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept); err != nil {
		return err
	}
	return rw.Flush()
}

var _ http.Handler = (*Handler)(nil)

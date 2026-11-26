package mcp

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// Handler is a bounded MCP streamable HTTP adapter over one canonical RPC
// server. It owns no store, lease, listener, or authorization policy.
type Handler struct {
	rpc           *rpc.Server
	authn         *authn.Authenticator
	cfg           Config
	notifications *notificationHub
	slots         chan struct{}
	activeMu      sync.Mutex
	active        map[string]activeCall
	methodMu      sync.Mutex
	methods       map[string]string
	sessionKey    [32]byte
}

// New constructs an MCP adapter over an existing canonical dispatcher.
func New(server *rpc.Server, authenticator *authn.Authenticator, config Config) (*Handler, error) {
	if server == nil {
		return nil, invalidConfig("RPC server is required")
	}
	if authenticator == nil {
		return nil, invalidConfig("authenticator is required")
	}
	var err error
	config, err = config.defaults()
	if err != nil {
		return nil, err
	}
	h := &Handler{rpc: server, authn: authenticator, cfg: config, notifications: newNotificationHub(), slots: make(chan struct{}, config.MaxConcurrentRequests), active: make(map[string]activeCall), methods: make(map[string]string)}
	h.sessionKey = config.SessionKey
	if h.sessionKey == ([32]byte{}) {
		if _, err := rand.Read(h.sessionKey[:]); err != nil {
			return nil, fmt.Errorf("mcp: session key unavailable: %w", err)
		}
	}
	return h, nil
}

// ServeHTTP serves the MCP streamable HTTP endpoint at /mcp. POST keeps the
// finite request/response behavior; GET attaches one authenticated session to
// the bounded notification stream. MCP protocol failures use JSON-RPC
// envelopes; HTTP admission/framing failures use the shared RPC transport
// error port with a null ID. Dispatch is synchronous and receives the live
// admitted identity plus the request context, so cancellation cannot leave a
// detached write behind.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("MCP-Protocol-Version", h.cfg.ProtocolVersion)
	if r == nil || r.URL == nil || r.URL.Path != Path || r.URL.EscapedPath() != Path || r.URL.RawQuery != "" || r.URL.ForceQuery {
		h.fail(w, http.StatusNotFound, "not_found", "unknown MCP endpoint")
		return
	}
	if r.Method == http.MethodGet {
		h.serveNotificationStream(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		h.fail(w, http.StatusMethodNotAllowed, "invalid_request", "use GET or POST /mcp")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.RequestTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	identity, err := h.authn.AdmitHTTP(r)
	if err != nil {
		status := http.StatusForbidden
		switch {
		case errors.Is(err, authn.ErrUnauthenticated), errors.Is(err, authn.ErrTokenExpired), errors.Is(err, authn.ErrHumanPresenceRequired):
			status = http.StatusUnauthorized
			w.Header().Set("WWW-Authenticate", "Bearer")
		case errors.Is(err, authn.ErrUnavailable):
			status = http.StatusServiceUnavailable
		}
		h.fail(w, status, failureCode(err), "MCP authentication failed")
		return
	}
	ctx = authn.WithIdentity(ctx, identity)
	ctx = WithPrincipal(ctx, identity.Principal())
	sessionID := "connection:" + r.RemoteAddr
	if values := r.Header.Values("MCP-Session-Id"); len(values) != 0 {
		if len(values) != 1 || values[0] == "" {
			h.fail(w, http.StatusBadRequest, "invalid_request", "one MCP session id is required")
			return
		}
		if !h.validSessionID(identity.Principal(), values[0]) {
			h.fail(w, http.StatusNotFound, "not_found", "unknown MCP session")
			return
		}
		sessionID = values[0]
	}
	ctx = WithSessionID(ctx, sessionID)
	r = r.WithContext(ctx)
	if len(r.Header.Values("Cookie")) != 0 {
		h.fail(w, http.StatusForbidden, "forbidden", "cookie credentials are unsupported")
		return
	}
	if len(r.Header.Values("Content-Encoding")) != 0 {
		h.fail(w, http.StatusUnsupportedMediaType, "invalid_request", "content encodings are unsupported")
		return
	}
	if !jsonContentType(r.Header.Values("Content-Type")) {
		h.fail(w, http.StatusUnsupportedMediaType, "invalid_request", "Content-Type must be application/json with optional charset=utf-8")
		return
	}
	if version := r.Header.Get("MCP-Protocol-Version"); version != "" && !h.supportedProtocol(version) {
		h.fail(w, http.StatusBadRequest, "invalid_request", "unsupported MCP protocol version")
		return
	}
	if r.ContentLength > h.cfg.MaxBodyBytes {
		h.fail(w, http.StatusRequestEntityTooLarge, "invalid_request", "MCP request body exceeds the configured limit")
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		h.fail(w, http.StatusTooManyRequests, "rate_limited", "MCP request concurrency limit reached")
		return
	}
	controller := http.NewResponseController(w)
	deadline, hasDeadline := ctx.Deadline()
	readDeadline := time.Now().Add(h.cfg.ReadTimeout)
	if !hasDeadline || readDeadline.Before(deadline) {
		deadline = readDeadline
	}
	if err := controller.SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		h.fail(w, http.StatusServiceUnavailable, "busy", "MCP read deadline unavailable")
		return
	}
	defer controller.SetReadDeadline(time.Time{})
	body := http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes)
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		var timeout net.Error
		if errors.As(err, &tooLarge) {
			h.fail(w, http.StatusRequestEntityTooLarge, "invalid_request", "MCP request body exceeds the configured limit")
		} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
			h.fail(w, http.StatusRequestTimeout, "busy", "MCP body deadline or cancellation reached")
		} else {
			h.fail(w, http.StatusBadRequest, "invalid_request", "unable to read MCP request body")
		}
		return
	}
	if ctx.Err() != nil {
		h.fail(w, http.StatusRequestTimeout, "busy", "request deadline or cancellation reached")
		return
	}
	var initializedSession string
	if r.Header.Get("MCP-Session-Id") == "" && isInitialize(data) {
		initializedSession, err = h.newSessionID(identity.Principal())
		if err != nil {
			h.fail(w, http.StatusServiceUnavailable, "busy", "MCP session unavailable")
			return
		}
		ctx = WithSessionID(ctx, initializedSession)
	}
	response := h.dispatch(ctx, rpc.WithPrincipal(ctx, identity.Principal()), data)
	if len(response) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	// Recheck on ordinary traffic as well as tools/list, so a client can
	// learn that it must refresh its tools from its next call or ping.
	if !isInitialize(data) {
		if _, hash, err := h.visibleMethods(ctx, identity.Principal()); err == nil {
			changed, streamed := h.observeMethods(ctx, identity.Principal(), hash)
			if len(changed) != 0 && h.cfg.OnNotification == nil && !streamed {
				frames := [][]byte{changed}
				appendResponses(&frames, response)
				response = joinResponses(frames)
			}
		}
	}
	if initializedSession != "" {
		var initialized map[string]json.RawMessage
		if json.Unmarshal(response, &initialized) == nil && initialized["result"] != nil {
			w.Header().Set("MCP-Session-Id", initializedSession)
		}
	}
	if frames, ok := notificationFrames(response); ok {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for _, frame := range frames {
			if _, err := fmt.Fprintf(w, "event: message\ndata: %s\n\n", frame); err != nil {
				return
			}
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(response); err != nil {
		// net/http has already committed the status. Keep this failure local to
		// the transport; no arbitrary writer diagnostic is sent to the peer.
		return
	}
}

// Close releases every connected notification stream. It is safe to call
// more than once and is intended for the composition owner during shutdown.
func (h *Handler) Close() error {
	if h != nil && h.notifications != nil {
		h.notifications.close()
	}
	return nil
}

func (h *Handler) serveNotificationStream(w http.ResponseWriter, r *http.Request) {
	identity, err := h.authn.AdmitHTTP(r)
	if err != nil {
		h.writeAdmissionFailure(w, err)
		return
	}
	if len(r.Header.Values("Cookie")) != 0 {
		h.fail(w, http.StatusForbidden, "forbidden", "cookie credentials are unsupported")
		return
	}
	if len(r.Header.Values("Content-Encoding")) != 0 {
		h.fail(w, http.StatusUnsupportedMediaType, "invalid_request", "content encodings are unsupported")
		return
	}
	if version := r.Header.Get("MCP-Protocol-Version"); version != "" && !h.supportedProtocol(version) {
		h.fail(w, http.StatusBadRequest, "invalid_request", "unsupported MCP protocol version")
		return
	}
	if !acceptsEventStream(r.Header.Values("Accept")) {
		h.fail(w, http.StatusNotAcceptable, "invalid_request", "MCP GET requires Accept: text/event-stream")
		return
	}
	sessionValues := r.Header.Values("MCP-Session-Id")
	if len(sessionValues) != 1 || sessionValues[0] == "" {
		h.fail(w, http.StatusBadRequest, "invalid_request", "MCP GET requires one MCP session id")
		return
	}
	sessionID := sessionValues[0]
	principal := identity.Principal()
	if !h.validSessionID(principal, sessionID) {
		h.fail(w, http.StatusNotFound, "not_found", "unknown MCP session")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		h.fail(w, http.StatusNotImplemented, "busy", "MCP notification streaming is unavailable")
		return
	}
	key, stream, err := h.notifications.open(principal, sessionID)
	if err != nil {
		status, code, hint := http.StatusServiceUnavailable, "busy", "MCP notification stream unavailable"
		if errors.Is(err, errNotificationStreamCap) {
			status, code, hint = http.StatusTooManyRequests, "rate_limited", "MCP notification stream limit reached"
		}
		h.fail(w, status, code, hint)
		return
	}
	defer h.notifications.remove(key, stream)

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case frame, ok := <-stream.queue:
			if !ok {
				return
			}
			if err := writeNotificationFrame(w, frame); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (h *Handler) writeAdmissionFailure(w http.ResponseWriter, err error) {
	status := http.StatusForbidden
	switch {
	case errors.Is(err, authn.ErrUnauthenticated), errors.Is(err, authn.ErrTokenExpired), errors.Is(err, authn.ErrHumanPresenceRequired):
		status = http.StatusUnauthorized
		w.Header().Set("WWW-Authenticate", "Bearer")
	case errors.Is(err, authn.ErrUnavailable):
		status = http.StatusServiceUnavailable
	}
	h.fail(w, status, failureCode(err), "MCP authentication failed")
}

func acceptsEventStream(values []string) bool {
	for _, value := range values {
		for _, candidate := range strings.Split(value, ",") {
			mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(candidate))
			if err != nil || mediaType != "text/event-stream" {
				continue
			}
			if quality, ok := params["q"]; ok {
				if parsed, err := strconv.ParseFloat(quality, 64); err == nil && parsed <= 0 {
					continue
				}
			}
			return true
		}
	}
	return false
}

func writeNotificationFrame(w io.Writer, frame []byte) error {
	_, err := fmt.Fprintf(w, "event: message\ndata: %s\n\n", frame)
	return err
}

func (h *Handler) fail(w http.ResponseWriter, status int, code, hint string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("MCP-Protocol-Version", h.cfg.ProtocolVersion)
	w.WriteHeader(status)
	_, _ = w.Write(h.rpc.TransportError(&model.Error{Data: model.ErrorData{Code: model.ErrorCode(code), Hint: hint}}))
}

func failureCode(err error) string {
	for _, code := range []authn.Failure{authn.ErrUnauthenticated, authn.ErrTokenExpired, authn.ErrUnavailable, authn.ErrForbidden, authn.ErrHumanPresenceRequired} {
		if errors.Is(err, code) {
			return string(code)
		}
	}
	return "forbidden"
}

func jsonContentType(values []string) bool {
	if len(values) != 1 {
		return false
	}
	typ, params, err := mime.ParseMediaType(values[0])
	if err != nil || typ != "application/json" {
		return false
	}
	for key, value := range params {
		if key != "charset" || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	return true
}

func (h *Handler) supportedProtocol(version string) bool {
	return version == h.cfg.ProtocolVersion || version == "2025-03-26" || version == "2025-06-18" || version == "2025-11-25"
}

// dispatch translates only the MCP protocol methods. All product calls are
// sent back through the canonical dispatcher with the same principal, params,
// idempotency, warning and error semantics.
func (h *Handler) dispatch(ctx context.Context, ctxWithPrincipal context.Context, data []byte) []byte {
	if err := ctx.Err(); err != nil {
		return h.syntheticError(ctxWithPrincipal, nil, "busy", "request deadline or cancellation reached")
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return h.syntheticError(ctxWithPrincipal, nil, "invalid_request", "MCP request body is empty")
	}
	value, err := wire.DecodeJSON(data)
	if err != nil {
		// The canonical dispatcher owns parse_error semantics and its null ID.
		return h.rpc.Dispatch(ctxWithPrincipal, data)
	}
	if _, ok := value.([]any); ok {
		var batch []json.RawMessage
		if err := json.Unmarshal(data, &batch); err != nil {
			return h.rpc.Dispatch(ctxWithPrincipal, data)
		}
		if len(batch) == 0 || len(batch) > rpc.MaxBatchCalls {
			return h.syntheticError(ctxWithPrincipal, nil, "invalid_request", "batch must contain 1 to 100 calls")
		}
		for _, item := range batch {
			if isInitialize(item) {
				return h.syntheticError(ctxWithPrincipal, nil, "invalid_request", "initialize must be sent as a single request")
			}
		}
		responses := make([][]byte, 0, len(batch))
		for _, item := range batch {
			appendResponses(&responses, h.dispatchOne(ctx, ctxWithPrincipal, item))
		}
		if len(responses) == 0 {
			return nil
		}
		return joinResponses(responses)
	}
	return h.dispatchOne(ctx, ctxWithPrincipal, data)
}

func (h *Handler) dispatchOne(ctx context.Context, ctxWithPrincipal context.Context, data []byte) []byte {
	if err := ctx.Err(); err != nil {
		return h.syntheticError(ctxWithPrincipal, nil, "busy", "request deadline or cancellation reached")
	}
	value, err := wire.DecodeJSON(data)
	if err != nil {
		return h.rpc.Dispatch(ctxWithPrincipal, data)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return h.syntheticError(ctxWithPrincipal, nil, "invalid_request", "MCP request must be an object")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return h.rpc.Dispatch(ctxWithPrincipal, data)
	}
	id, hasID := raw["id"]
	if hasID {
		if idValue, ok := object["id"]; !ok || (idValue != nil && !isJSONRPCID(idValue)) {
			return h.syntheticError(ctxWithPrincipal, nil, "invalid_request", "MCP id must be a string, number or null")
		}
	}
	if isResponse(object, hasID) {
		return nil
	}
	method, ok := object["method"].(string)
	if !ok || object["jsonrpc"] != "2.0" {
		return h.syntheticError(ctxWithPrincipal, idIfPresent(id, hasID), "invalid_request", "expected JSON-RPC 2.0 and a method name")
	}
	for name := range object {
		if name != "jsonrpc" && name != "id" && name != "method" && name != "params" {
			return h.syntheticError(ctxWithPrincipal, idIfPresent(id, hasID), "invalid_request", "unknown MCP request member")
		}
	}
	params := map[string]any{}
	if rawParams, present := raw["params"]; present {
		decoded, err := wire.DecodeJSON(rawParams)
		if err != nil {
			return h.syntheticError(ctxWithPrincipal, idIfPresent(id, hasID), "invalid_params", err.Error())
		}
		var ok bool
		params, ok = decoded.(map[string]any)
		if !ok {
			return h.syntheticError(ctxWithPrincipal, idIfPresent(id, hasID), "invalid_params", "MCP params must be an object")
		}
	}
	if method == "notifications/cancelled" {
		if hasID {
			return h.syntheticError(ctxWithPrincipal, id, "invalid_request", "notifications/cancelled cannot have an id")
		}
		requestID, err := cancelledRequestID(params)
		if err != nil {
			return nil
		}
		h.cancelActive(WithCallerAgent(ctxWithPrincipal, metadataAgent(params)), requestID)
		return nil
	}
	if strings.HasPrefix(method, "notifications/") || method == "notifications/initialized" {
		if !hasID {
			if method == "notifications/initialized" {
				if err := validateInitialized(params); err != nil {
					return nil
				}
			}
			return nil
		}
		return h.syntheticError(ctxWithPrincipal, id, "invalid_request", "notifications cannot have an id")
	}
	if !hasID {
		// MCP requests that can produce a result need an ID. Keep notification
		// behavior aligned with the canonical dispatcher for writes and reads.
		if method == "tools/call" {
			return nil
		}
		return nil
	}
	switch method {
	case "initialize":
		return h.initialize(ctxWithPrincipal, id, params)
	case "ping":
		if len(params) != 0 {
			return h.syntheticError(ctxWithPrincipal, id, "invalid_params", "ping does not accept parameters")
		}
		return mcpResponse(id, map[string]any{})
	case "tools/list":
		return h.toolsList(ctxWithPrincipal, id, params)
	case "tools/call":
		return h.toolsCall(ctxWithPrincipal, id, params)
	default:
		return h.syntheticError(ctxWithPrincipal, id, "method_not_found", "unsupported MCP method")
	}
}

func appendResponses(responses *[][]byte, response []byte) {
	if len(response) == 0 {
		return
	}
	trimmed := bytes.TrimSpace(response)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var nested []json.RawMessage
		if err := json.Unmarshal(trimmed, &nested); err == nil {
			for _, item := range nested {
				appendResponses(responses, item)
			}
			return
		}
	}
	*responses = append(*responses, append([]byte(nil), response...))
}

func joinResponses(responses [][]byte) []byte {
	out := []byte{'['}
	for i, response := range responses {
		if i != 0 {
			out = append(out, ',')
		}
		out = append(out, response...)
	}
	return append(out, ']')
}

func isInitialize(data []byte) bool {
	var request struct {
		Method string `json:"method"`
	}
	return json.Unmarshal(data, &request) == nil && request.Method == "initialize"
}

// Responses to server requests have no reply of their own. This adapter does
// not initiate requests, so well-formed responses with unknown IDs are ignored.
func isResponse(object map[string]any, hasID bool) bool {
	if !hasID || object["jsonrpc"] != "2.0" {
		return false
	}
	if _, hasMethod := object["method"]; hasMethod {
		return false
	}
	_, result := object["result"]
	errorValue, failure := object["error"]
	if result == failure {
		return false
	}
	for key := range object {
		if key != "jsonrpc" && key != "id" && key != "result" && key != "error" {
			return false
		}
	}
	if failure {
		detail, ok := errorValue.(map[string]any)
		if !ok {
			return false
		}
		code, ok := detail["code"].(json.Number)
		if !ok {
			return false
		}
		if _, err := code.Int64(); err != nil {
			return false
		}
		if _, ok := detail["message"].(string); !ok {
			return false
		}
	}
	return true
}

// Streamable HTTP carries server notifications as SSE messages, followed by
// the response. Ordinary batches remain JSON arrays, including a single reply.
func notificationFrames(response []byte) ([]json.RawMessage, bool) {
	var frames []json.RawMessage
	if json.Unmarshal(response, &frames) != nil {
		return nil, false
	}
	found := false
	for i, frame := range frames {
		var message struct {
			Method string `json:"method"`
		}
		if json.Unmarshal(frame, &message) == nil && message.Method == "notifications/tools/list_changed" {
			found = true
		}
		var compact bytes.Buffer
		if json.Compact(&compact, frame) != nil {
			return nil, false
		}
		frames[i] = append(json.RawMessage(nil), compact.Bytes()...)
	}
	return frames, found
}

func idIfPresent(id json.RawMessage, present bool) json.RawMessage {
	if !present {
		return nil
	}
	return id
}

func isJSONRPCID(value any) bool {
	switch value.(type) {
	case string, json.Number:
		return true
	default:
		return value == nil
	}
}

func validateInitialized(params map[string]any) error {
	for key := range params {
		if key != "_meta" {
			return fmt.Errorf("initialized does not accept %q", key)
		}
	}
	return nil
}

func cancelledRequestID(params map[string]any) (string, error) {
	value, ok := params["requestId"]
	if !ok || !isJSONRPCID(value) {
		return "", fmt.Errorf("notifications/cancelled requires requestId")
	}
	for key := range params {
		if key != "requestId" && key != "reason" && key != "_meta" {
			return "", fmt.Errorf("notifications/cancelled does not accept %q", key)
		}
	}
	encoded, err := json.Marshal(value)
	return string(encoded), err
}

func metadataAgent(params map[string]any) string {
	meta, _ := params["_meta"].(map[string]any)
	agent, _ := meta["agent"].(string)
	return agent
}

func (h *Handler) initialize(ctx context.Context, id json.RawMessage, params map[string]any) []byte {
	protocol, ok := params["protocolVersion"].(string)
	if !ok || protocol == "" {
		return h.syntheticError(ctx, id, "invalid_params", "initialize requires protocolVersion")
	}
	if !h.supportedProtocol(protocol) {
		protocol = h.cfg.ProtocolVersion
	}
	capabilities, ok := params["capabilities"]
	if !ok {
		return h.syntheticError(ctx, id, "invalid_params", "initialize requires capabilities")
	}
	if _, ok := capabilities.(map[string]any); !ok {
		return h.syntheticError(ctx, id, "invalid_params", "initialize capabilities must be an object")
	}
	clientInfo, ok := params["clientInfo"]
	if !ok {
		return h.syntheticError(ctx, id, "invalid_params", "initialize requires clientInfo")
	}
	client, ok := clientInfo.(map[string]any)
	if !ok {
		return h.syntheticError(ctx, id, "invalid_params", "initialize clientInfo must be an object")
	}
	for _, name := range []string{"name", "version"} {
		value, ok := client[name].(string)
		if !ok || value == "" {
			return h.syntheticError(ctx, id, "invalid_params", fmt.Sprintf("initialize clientInfo.%s must be a non-empty string", name))
		}
	}
	for key := range params {
		if key != "protocolVersion" && key != "capabilities" && key != "clientInfo" && key != "_meta" {
			return h.syntheticError(ctx, id, "invalid_params", fmt.Sprintf("initialize does not accept %q", key))
		}
	}
	if meta, present := params["_meta"]; present {
		if _, ok := meta.(map[string]any); !ok {
			return h.syntheticError(ctx, id, "invalid_params", "initialize _meta must be an object")
		}
	}
	principal, _ := rpcPrincipal(ctx)
	if _, hash, err := h.visibleMethods(ctx, principal); err == nil {
		h.observeMethods(ctx, principal, hash)
	}
	serverVersion := "0.1.0"
	hello := h.rpc.Dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"server.hello"}`))
	if decoded, err := wire.DecodeJSON(hello); err == nil {
		if response, ok := decoded.(map[string]any); ok {
			if result, ok := response["result"].(map[string]any); ok {
				if value, ok := result["serverVersion"].(string); ok && value != "" {
					serverVersion = value
				}
			}
		}
	}
	result := map[string]any{
		"protocolVersion": protocol,
		"capabilities":    map[string]any{"tools": map[string]any{"listChanged": true}},
		"serverInfo":      map[string]any{"name": h.cfg.ServerName, "version": serverVersion},
	}
	return mcpResponse(id, result)
}

type methodInfo struct {
	Name        string
	Summary     string
	Description string
	AgentCore   bool
	Idempotency string
	Params      []wire.Param
	Schemas     map[string]json.RawMessage
}

func (h *Handler) visibleMethods(ctx context.Context, principal model.PrincipalRef) (map[string]methodInfo, string, error) {
	doc, err := h.rpc.OpenRPC(ctx, principal)
	if err != nil {
		return nil, "", err
	}
	var parsed struct {
		Components struct {
			Schemas map[string]json.RawMessage `json:"schemas"`
		} `json:"components"`
		Methods []struct {
			Name        string       `json:"name"`
			Summary     string       `json:"summary"`
			Description string       `json:"description"`
			AgentCore   bool         `json:"x-agent-core"`
			Idempotency string       `json:"x-idempotency"`
			Params      []wire.Param `json:"params"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(doc, &parsed); err != nil {
		return nil, "", err
	}
	out := make(map[string]methodInfo, len(parsed.Methods))
	for _, method := range parsed.Methods {
		if method.Name == "" || out[method.Name].Name != "" {
			if method.Name != "" {
				return nil, "", fmt.Errorf("duplicate visible MCP method %q", method.Name)
			}
			continue
		}
		out[method.Name] = methodInfo{Name: method.Name, Summary: method.Summary, Description: method.Description, AgentCore: method.AgentCore, Idempotency: method.Idempotency, Params: method.Params, Schemas: parsed.Components.Schemas}
	}
	names := make([]string, 0, len(out))
	for name := range out {
		names = append(names, name)
	}
	sort.Strings(names)
	encoded, _ := json.Marshal(names)
	hash := sha256.Sum256(encoded)
	return out, hex.EncodeToString(hash[:]), nil
}

func (h *Handler) observeMethods(ctx context.Context, principal model.PrincipalRef, hash string) ([]byte, bool) {
	keyBytes, _ := json.Marshal(principal)
	key := string(keyBytes) + "\x00" + sessionIDFromContext(ctx) + "\x00" + callerAgentFromContext(ctx)
	h.methodMu.Lock()
	previous, seen := h.methods[key]
	h.methods[key] = hash
	h.methodMu.Unlock()
	if !seen || previous == hash {
		return nil, false
	}
	notification := toolsListChangedNotification()
	streamed := h.notifications.publish(ctx, principal, sessionIDFromContext(ctx), notification)
	if h.cfg.OnNotification != nil {
		h.cfg.OnNotification(ctx, principal, sessionIDFromContext(ctx), append([]byte(nil), notification...))
	}
	return notification, streamed
}

func toolsListChangedNotification() []byte {
	return []byte(`{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}`)
}

func (h *Handler) toolsList(ctx context.Context, id json.RawMessage, params map[string]any) []byte {
	for key, value := range params {
		if key != "cursor" && key != "_meta" {
			return h.syntheticError(ctx, id, "invalid_params", fmt.Sprintf("tools/list does not accept %q", key))
		}
		if key == "cursor" {
			if cursor, ok := value.(string); !ok || cursor != "" {
				return h.syntheticError(ctx, id, "invalid_params", "MCP tool pagination is unsupported")
			}
		}
	}
	principal, _ := rpcPrincipal(ctx)
	methods, hash, err := h.visibleMethods(ctx, principal)
	if err != nil {
		return h.syntheticError(ctx, id, "internal_error", "unable to discover MCP tools")
	}
	changed, streamed := h.observeMethods(ctx, principal, hash)
	tools := make([]map[string]any, 0, len(methods)+2)
	names := make([]string, 0, len(methods))
	for name, method := range methods {
		if method.AgentCore {
			names = append(names, toolName(name))
		}
	}
	sort.Strings(names)
	byTool := make(map[string]methodInfo, len(methods))
	for name, method := range methods {
		if method.AgentCore {
			byTool[toolName(name)] = method
		}
	}
	for _, name := range names {
		tools = append(tools, toolDefinition(byTool[name]))
	}
	// These two transport tools are intentionally fixed seams. They invoke
	// only methods already present in the canonical visible registry.
	tools = append(tools, plugCallDefinition(), plugDescribeDefinition())
	sort.Slice(tools, func(i, j int) bool { return tools[i]["name"].(string) < tools[j]["name"].(string) })
	response := mcpResponse(id, map[string]any{"tools": tools})
	if len(changed) != 0 && h.cfg.OnNotification == nil && !streamed {
		return joinResponses([][]byte{changed, response})
	}
	return response
}

func toolDefinition(method methodInfo) map[string]any {
	description := method.Description
	if description == "" {
		description = method.Summary
	}
	if description == "" {
		description = method.Name
	}
	properties := map[string]any{}
	definitions := map[string]any{}
	required := []string{}
	for _, param := range method.Params {
		var schema any
		_ = json.Unmarshal(param.Schema, &schema)
		properties[param.Name] = toolSchema(schema, method.Schemas, definitions)
		if param.Required {
			required = append(required, param.Name)
		}
	}
	schema := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) != 0 {
		schema["required"] = required
	}
	if len(definitions) != 0 {
		schema["$defs"] = definitions
	}
	return map[string]any{"name": toolName(method.Name), "description": description, "inputSchema": schema}
}

// An MCP tool is a standalone JSON Schema, so copy the transitive component
// definitions it references rather than leaving OpenRPC-only pointers behind.
func toolSchema(value any, components map[string]json.RawMessage, definitions map[string]any) any {
	switch value := value.(type) {
	case map[string]any:
		if ref, ok := value["$ref"].(string); ok {
			if name, local := strings.CutPrefix(ref, "#/components/schemas/"); local {
				value["$ref"] = "#/$defs/" + name
				if _, exists := definitions[name]; !exists {
					definitions[name] = nil // mark before descending into recursive schemas
					var definition any
					_ = json.Unmarshal(components[name], &definition)
					definitions[name] = toolSchema(definition, components, definitions)
				}
			}
		}
		for name, nested := range value {
			value[name] = toolSchema(nested, components, definitions)
		}
	case []any:
		for i, nested := range value {
			value[i] = toolSchema(nested, components, definitions)
		}
	}
	return value
}

func plugCallDefinition() map[string]any {
	return map[string]any{
		"name":        "plug_call",
		"description": "Call any visible canonical Plug method by name.",
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{
			"method": map[string]any{"type": "string"},
			"params": map[string]any{"type": "object", "additionalProperties": true},
		}, "required": []string{"method"}, "additionalProperties": false},
	}
}

func plugDescribeDefinition() map[string]any {
	return map[string]any{
		"name":        "plug_describe",
		"description": "Describe visible canonical Plug methods and errors.",
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{
			"method": map[string]any{"type": "string"},
		}, "additionalProperties": false},
	}
}

func (h *Handler) toolsCall(ctx context.Context, id json.RawMessage, params map[string]any) []byte {
	name, ok := params["name"].(string)
	if !ok || name == "" {
		return h.syntheticError(ctx, id, "invalid_params", "tools/call requires a tool name")
	}
	for key := range params {
		if key != "name" && key != "arguments" && key != "_meta" {
			return h.syntheticError(ctx, id, "invalid_params", fmt.Sprintf("tools/call does not accept %q", key))
		}
	}
	arguments := map[string]any{}
	if raw, present := params["arguments"]; present {
		var ok bool
		arguments, ok = raw.(map[string]any)
		if !ok {
			return h.syntheticError(ctx, id, "invalid_params", "tool arguments must be an object")
		}
	}
	if meta, present := params["_meta"]; present {
		metaMap, ok := meta.(map[string]any)
		if !ok {
			return h.syntheticError(ctx, id, "invalid_params", "MCP _meta must be an object")
		}
		existing, err := objectMetadata(arguments)
		if err != nil {
			return h.syntheticError(ctx, id, "invalid_params", err.Error())
		}
		merged, err := mergeMetadata(metaMap, existing)
		if err != nil {
			return h.syntheticError(ctx, id, "invalid_params", err.Error())
		}
		arguments["_meta"] = merged
	}
	principal, _ := rpcPrincipal(ctx)
	methods, _, err := h.visibleMethods(ctx, principal)
	if err != nil {
		return h.toolError(id, map[string]any{"code": "internal_error", "message": "unable to discover MCP tools"})
	}
	methodName := ""
	switch name {
	case "plug_call":
		outerMeta, err := objectMetadata(arguments)
		if err != nil {
			return h.syntheticError(ctx, id, "invalid_params", err.Error())
		}
		for key := range arguments {
			if key != "method" && key != "params" && key != "_meta" {
				return h.syntheticError(ctx, id, "invalid_params", fmt.Sprintf("plug_call does not accept %q", key))
			}
		}
		candidate, ok := arguments["method"].(string)
		if !ok || candidate == "" {
			return h.syntheticError(ctx, id, "invalid_params", "plug_call requires method")
		}
		methodName = candidate
		if nested, ok := arguments["params"].(map[string]any); ok {
			arguments = nested
			nestedMeta, err := objectMetadata(arguments)
			if err != nil {
				return h.syntheticError(ctx, id, "invalid_params", err.Error())
			}
			merged, err := mergeMetadata(outerMeta, nestedMeta)
			if err != nil {
				return h.syntheticError(ctx, id, "invalid_params", err.Error())
			}
			if merged != nil {
				arguments["_meta"] = merged
			}
		} else if _, present := arguments["params"]; present {
			return h.syntheticError(ctx, id, "invalid_params", "plug_call params must be an object")
		} else {
			arguments = map[string]any{}
			if outerMeta != nil {
				arguments["_meta"] = outerMeta
			}
		}
	case "plug_describe":
		methodName = "rpc.describe"
	default:
		for canonical, method := range methods {
			if method.AgentCore && toolName(canonical) == name {
				methodName = canonical
				break
			}
		}
		if methodName == "" {
			return h.syntheticError(ctx, id, "method_not_found", "unknown MCP tool")
		}
	}
	if _, exists := methods[methodName]; !exists {
		return h.syntheticError(ctx, id, "method_not_found", "canonical method is not visible or bound")
	}
	if info := methods[methodName]; info.Idempotency == "required" {
		if err := ensureIdempotency(arguments); err != nil {
			return h.syntheticError(ctx, id, "invalid_params", err.Error())
		}
	}
	request := canonicalRequest(id, methodName, arguments)
	callCtx, finish, err := h.beginActive(WithCallerAgent(ctx, metadataAgent(arguments)), id)
	if err != nil {
		return h.syntheticError(ctx, id, "invalid_request", err.Error())
	}
	defer finish()
	response := h.rpc.Dispatch(callCtx, request)
	return h.toolResponse(id, response)
}

func (h *Handler) activeKey(ctx context.Context, id json.RawMessage) string {
	principal, _ := rpcPrincipal(ctx)
	encoded, _ := json.Marshal([]any{principal, sessionIDFromContext(ctx), callerAgentFromContext(ctx), normalizedID(id)})
	return string(encoded)
}

type activeCall struct {
	cancel context.CancelFunc
	token  *byte
}

func (h *Handler) beginActive(ctx context.Context, id json.RawMessage) (context.Context, func(), error) {
	callCtx, cancel := context.WithCancel(ctx)
	key := h.activeKey(ctx, id)
	token := new(byte)
	h.activeMu.Lock()
	if _, exists := h.active[key]; exists {
		h.activeMu.Unlock()
		cancel()
		return nil, nil, fmt.Errorf("request id is already active in this MCP session")
	}
	h.active[key] = activeCall{cancel: cancel, token: token}
	h.activeMu.Unlock()
	return callCtx, func() {
		h.activeMu.Lock()
		if current, ok := h.active[key]; ok && current.token == token {
			delete(h.active, key)
		}
		h.activeMu.Unlock()
		cancel()
	}, nil
}

func (h *Handler) cancelActive(ctx context.Context, requestID string) {
	key := h.activeKey(ctx, json.RawMessage(requestID))
	h.activeMu.Lock()
	call := h.active[key]
	h.activeMu.Unlock()
	if call.cancel != nil {
		call.cancel()
	}
}

func ensureIdempotency(arguments map[string]any) error {
	meta, err := objectMetadata(arguments)
	if err != nil {
		return err
	}
	if meta == nil {
		meta = map[string]any{}
		arguments["_meta"] = meta
	}
	if key, present := meta["idempotencyKey"]; present {
		value, ok := key.(string)
		if !ok || value == "" {
			return fmt.Errorf("idempotencyKey must be a string")
		}
		if err := wire.ValidateIdempotencyKey(value); err != nil {
			return err
		}
		return nil
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Errorf("unable to create idempotency key")
	}
	meta["idempotencyKey"] = hex.EncodeToString(random[:])
	return nil
}

func objectMetadata(params map[string]any) (map[string]any, error) {
	value, present := params["_meta"]
	if !present {
		return nil, nil
	}
	meta, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("_meta must be an object")
	}
	return meta, nil
}

// mergeMetadata preserves caller metadata at either layer and refuses
// conflicting duplicates instead of silently replacing an idempotency key.
func mergeMetadata(outer, nested map[string]any) (map[string]any, error) {
	if outer == nil && nested == nil {
		return nil, nil
	}
	merged := make(map[string]any, len(outer)+len(nested))
	for key, value := range nested {
		merged[key] = value
	}
	for key, value := range outer {
		if previous, exists := merged[key]; exists {
			before, _ := json.Marshal(previous)
			after, _ := json.Marshal(value)
			if !bytes.Equal(before, after) {
				return nil, fmt.Errorf("conflicting _meta.%s", key)
			}
		}
		merged[key] = value
	}
	return merged, nil
}

func canonicalRequest(id json.RawMessage, method string, params map[string]any) []byte {
	request := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(id), "method": method, "params": params}
	data, err := json.Marshal(request)
	if err != nil {
		return []byte(`{"jsonrpc":"2.0","id":null,"method":"server.hello"}`)
	}
	return data
}

func (h *Handler) toolResponse(id json.RawMessage, response []byte) []byte {
	if len(response) == 0 {
		return nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response, &envelope); err != nil {
		return h.toolError(id, map[string]any{"code": "internal_error", "message": "canonical dispatcher returned invalid JSON"})
	}
	meta := envelope["_meta"]
	result := map[string]any{}
	if len(meta) != 0 && !bytes.Equal(bytes.TrimSpace(meta), []byte("null")) {
		result["_meta"] = meta
	}
	if raw, ok := envelope["result"]; ok {
		result["content"] = []map[string]any{{"type": "text", "text": string(raw)}}
		return mcpResponse(id, result)
	}
	if raw, ok := envelope["error"]; ok {
		result["content"] = []map[string]any{{"type": "text", "text": string(raw)}}
		result["isError"] = true
		return mcpResponse(id, result)
	}
	return h.toolError(id, map[string]any{"code": "internal_error", "message": "canonical dispatcher returned no result or error"})
}

func (h *Handler) toolError(id json.RawMessage, errorValue map[string]any) []byte {
	encoded, _ := json.Marshal(errorValue)
	return mcpResponse(id, map[string]any{"content": []map[string]any{{"type": "text", "text": string(encoded)}}, "isError": true})
}

func mcpResponse(id json.RawMessage, result any) []byte {
	if len(id) == 0 {
		id = []byte("null")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		encoded = []byte(`{"content":[{"type":"text","text":"internal error"}],"isError":true}`)
	}
	out := []byte(`{"jsonrpc":"2.0","id":`)
	out = append(out, id...)
	out = append(out, []byte(`,"result":`)...)
	out = append(out, encoded...)
	return append(out, '}')
}

func (h *Handler) syntheticError(ctx context.Context, id json.RawMessage, code, hint string) []byte {
	if code == "parse_error" {
		return h.rpc.Dispatch(ctx, []byte("{"))
	}
	// Keep protocol errors on the canonical catalog/error port. The adapter
	// must not manufacture an invalid canonical request to obtain a response:
	// that would lose the requested code (for example, busy becomes
	// invalid_params) and would couple MCP diagnostics to unrelated schemas.
	response := h.rpc.TransportError(&model.Error{Data: model.ErrorData{Code: model.ErrorCode(code), Hint: hint}})
	if len(id) != 0 {
		return replaceResponseID(response, id)
	}
	return response
}

func replaceResponseID(response, id []byte) []byte {
	if len(response) == 0 || len(id) == 0 {
		return response
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(response, &raw) != nil {
		return response
	}
	raw["id"] = append(json.RawMessage(nil), id...)
	encoded, err := json.Marshal(raw)
	if err != nil {
		return response
	}
	return encoded
}

func rpcPrincipal(ctx context.Context) (model.PrincipalRef, bool) {
	// A canonical RPC call always receives its principal through the context,
	// but rpc intentionally keeps that key private. The MCP adapter obtains the
	// admitted principal from its own context bridge instead.
	if identity, ok := ctx.Value(mcpPrincipalKey{}).(model.PrincipalRef); ok {
		return identity, true
	}
	return model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, true
}

type mcpPrincipalKey struct{}
type mcpSessionKey struct{}
type mcpCallerAgentKey struct{}

func (h *Handler) sessionMAC(principal model.PrincipalRef, nonce []byte) []byte {
	encoded, _ := json.Marshal(principal)
	mac := hmac.New(sha256.New, h.sessionKey[:])
	_, _ = mac.Write(encoded)
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(nonce)
	return mac.Sum(nil)
}

// Session IDs are random and authenticated for the signing key and principal.
// A new process key invalidates these scopes and clients initialize again.
func (h *Handler) newSessionID(principal model.PrincipalRef) (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(append(nonce[:], h.sessionMAC(principal, nonce[:])...)), nil
}

func (h *Handler) validSessionID(principal model.PrincipalRef, sessionID string) bool {
	if len(sessionID) != base64.RawURLEncoding.EncodedLen(16+sha256.Size) {
		return false
	}
	value, err := base64.RawURLEncoding.DecodeString(sessionID)
	return err == nil && len(value) == 16+sha256.Size && hmac.Equal(value[16:], h.sessionMAC(principal, value[:16]))
}

// Normalize strings and decimal IDs without float rounding or expanding huge
// exponents. A cancellation for 1e2 must find the same call as ID 100.0.
func normalizedID(id json.RawMessage) string {
	value, err := wire.DecodeJSON(id)
	if err != nil {
		return string(id)
	}
	switch value := value.(type) {
	case string:
		return "s:" + value
	case json.Number:
		coefficient, exponentText, _ := strings.Cut(strings.ToLower(value.String()), "e")
		exponent := new(big.Int)
		if exponentText != "" {
			exponent.SetString(exponentText, 10)
		}
		negative := strings.HasPrefix(coefficient, "-")
		coefficient = strings.TrimPrefix(coefficient, "-")
		whole, fraction, _ := strings.Cut(coefficient, ".")
		digits := strings.TrimLeft(whole+fraction, "0")
		if digits == "" {
			return "n:0"
		}
		significant := strings.TrimRight(digits, "0")
		exponent.Add(exponent, big.NewInt(int64(len(digits)-len(significant)-len(fraction))))
		if negative {
			significant = "-" + significant
		}
		return "n:" + significant + "e" + exponent.String()
	default:
		return "null"
	}
}

func sessionIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(mcpSessionKey{}).(string)
	return value
}

func callerAgentFromContext(ctx context.Context) string {
	value, _ := ctx.Value(mcpCallerAgentKey{}).(string)
	return value
}

// WithPrincipal is useful for in-process MCP fixtures and trusted composition
// roots. HTTP callers should use Handler.ServeHTTP, which stamps the admitted
// principal automatically.
func WithPrincipal(ctx context.Context, principal model.PrincipalRef) context.Context {
	ctx = context.WithValue(ctx, mcpPrincipalKey{}, principal)
	return rpc.WithPrincipal(ctx, principal)
}

// WithSessionID scopes cancellation and tool-list notifications to one MCP
// session. HTTP callers get this automatically from MCP-Session-Id; trusted
// in-process composition can use this helper when it multiplexes sessions.
func WithSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, mcpSessionKey{}, sessionID)
}

// WithCallerAgent scopes anonymous cancellation state by the caller's
// unverified agent label, when supplied.
func WithCallerAgent(ctx context.Context, agent string) context.Context {
	return context.WithValue(ctx, mcpCallerAgentKey{}, agent)
}

func toolName(method string) string {
	var b strings.Builder
	for _, r := range method {
		if r == '.' {
			b.WriteByte('_')
			continue
		}
		if unicode.IsUpper(r) {
			b.WriteByte('_')
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return strings.TrimPrefix(b.String(), "_")
}

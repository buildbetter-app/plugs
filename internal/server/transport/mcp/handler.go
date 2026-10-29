package mcp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"sort"
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
	rpc      *rpc.Server
	authn    *authn.Authenticator
	cfg      Config
	slots    chan struct{}
	activeMu sync.Mutex
	active   map[string]activeCall
	methodMu sync.Mutex
	methods  map[string]string
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
	return &Handler{rpc: server, authn: authenticator, cfg: config, slots: make(chan struct{}, config.MaxConcurrentRequests), active: make(map[string]activeCall), methods: make(map[string]string)}, nil
}

// ServeHTTP serves exactly POST /mcp. MCP protocol failures use JSON-RPC
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
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		h.fail(w, http.StatusMethodNotAllowed, "invalid_request", "use POST /mcp")
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
	ctx = WithSessionID(ctx, requestSessionID(r))
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
	response := h.dispatch(ctx, rpc.WithPrincipal(ctx, identity.Principal()), data)
	if len(response) == 0 {
		w.WriteHeader(http.StatusAccepted)
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
	if len(responses) == 1 {
		return responses[0]
	}
	out := []byte{'['}
	for i, response := range responses {
		if i != 0 {
			out = append(out, ',')
		}
		out = append(out, response...)
	}
	return append(out, ']')
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
		return h.syntheticError(ctx, id, "invalid_params", "unsupported protocolVersion")
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
	version := h.cfg.ProtocolVersion
	version = protocol
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
		"protocolVersion": version,
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
}

func (h *Handler) visibleMethods(ctx context.Context, principal model.PrincipalRef) (map[string]methodInfo, string, error) {
	doc, err := h.rpc.OpenRPC(ctx, principal)
	if err != nil {
		return nil, "", err
	}
	var parsed struct {
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
		out[method.Name] = methodInfo{Name: method.Name, Summary: method.Summary, Description: method.Description, AgentCore: method.AgentCore, Idempotency: method.Idempotency, Params: method.Params}
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

func (h *Handler) observeMethods(ctx context.Context, principal model.PrincipalRef, hash string) []byte {
	keyBytes, _ := json.Marshal(principal)
	key := string(keyBytes) + "\x00" + sessionIDFromContext(ctx) + "\x00" + callerAgentFromContext(ctx)
	h.methodMu.Lock()
	previous, seen := h.methods[key]
	h.methods[key] = hash
	h.methodMu.Unlock()
	if !seen || previous == hash {
		return nil
	}
	notification := toolsListChangedNotification()
	if h.cfg.OnNotification != nil {
		h.cfg.OnNotification(ctx, principal, append([]byte(nil), notification...))
	}
	return notification
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
	changed := h.observeMethods(ctx, principal, hash)
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
	if len(changed) != 0 && h.cfg.OnNotification == nil {
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
	required := []string{}
	for _, param := range method.Params {
		properties[param.Name] = json.RawMessage(param.Schema)
		if param.Required {
			required = append(required, param.Name)
		}
	}
	schema := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) != 0 {
		schema["required"] = required
	}
	return map[string]any{"name": toolName(method.Name), "description": description, "inputSchema": schema}
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
		if existing, ok := arguments["_meta"].(map[string]any); ok {
			arguments["_meta"] = mergeMetadata(metaMap, existing)
		} else {
			arguments["_meta"] = mergeMetadata(metaMap, nil)
		}
	}
	principal, _ := rpcPrincipal(ctx)
	methods, _, err := h.visibleMethods(ctx, principal)
	if err != nil {
		return h.toolError(id, map[string]any{"code": "internal_error", "message": "unable to discover MCP tools"})
	}
	methodName := ""
	switch name {
	case "plug_call":
		outerMeta, _ := arguments["_meta"].(map[string]any)
		candidate, ok := arguments["method"].(string)
		if !ok || candidate == "" {
			return h.syntheticError(ctx, id, "invalid_params", "plug_call requires method")
		}
		methodName = candidate
		if nested, ok := arguments["params"].(map[string]any); ok {
			arguments = nested
			if nestedMeta, present := arguments["_meta"].(map[string]any); present {
				arguments["_meta"] = mergeMetadata(outerMeta, nestedMeta)
			} else if outerMeta != nil {
				arguments["_meta"] = mergeMetadata(outerMeta, nil)
			}
		} else if _, present := arguments["params"]; present {
			return h.syntheticError(ctx, id, "invalid_params", "plug_call params must be an object")
		} else {
			arguments = map[string]any{}
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
	callCtx, finish := h.beginActive(WithCallerAgent(ctx, metadataAgent(arguments)), id)
	defer finish()
	response := h.rpc.Dispatch(callCtx, request)
	return h.toolResponse(id, response)
}

func (h *Handler) activeKey(ctx context.Context, id json.RawMessage) string {
	principal, _ := rpcPrincipal(ctx)
	encoded, _ := json.Marshal(principal)
	return string(encoded) + "\x00" + sessionIDFromContext(ctx) + "\x00" + callerAgentFromContext(ctx) + "\x00" + string(id)
}

type activeCall struct {
	cancel context.CancelFunc
	token  *struct{}
}

func (h *Handler) beginActive(ctx context.Context, id json.RawMessage) (context.Context, func()) {
	callCtx, cancel := context.WithCancel(ctx)
	key := h.activeKey(ctx, id)
	token := &struct{}{}
	h.activeMu.Lock()
	h.active[key] = activeCall{cancel: cancel, token: token}
	h.activeMu.Unlock()
	return callCtx, func() {
		h.activeMu.Lock()
		if current, ok := h.active[key]; ok && current.token == token {
			delete(h.active, key)
		}
		h.activeMu.Unlock()
		cancel()
	}
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
	meta, ok := arguments["_meta"].(map[string]any)
	if !ok {
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

// mergeMetadata keeps the outer MCP metadata authoritative when a plug_call
// also carries canonical metadata in its nested params. This preserves caller
// idempotency, project, deadline and agent labels across both tool shapes.
func mergeMetadata(outer, nested map[string]any) map[string]any {
	merged := make(map[string]any, len(outer)+len(nested))
	for key, value := range nested {
		merged[key] = value
	}
	for key, value := range outer {
		merged[key] = value
	}
	return merged
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
	if raw, ok := envelope["result"]; ok {
		return mcpResponseWithMeta(id, map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}}, meta)
	}
	if raw, ok := envelope["error"]; ok {
		return mcpResponseWithMeta(id, map[string]any{"content": []map[string]any{{"type": "text", "text": string(raw)}}, "isError": true}, meta)
	}
	return h.toolError(id, map[string]any{"code": "internal_error", "message": "canonical dispatcher returned no result or error"})
}

func (h *Handler) toolError(id json.RawMessage, errorValue map[string]any) []byte {
	encoded, _ := json.Marshal(errorValue)
	return mcpResponse(id, map[string]any{"content": []map[string]any{{"type": "text", "text": string(encoded)}}, "isError": true})
}

func mcpResponse(id json.RawMessage, result any) []byte {
	return mcpResponseWithMeta(id, result, nil)
}

func mcpResponseWithMeta(id json.RawMessage, result any, meta json.RawMessage) []byte {
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
	if len(meta) != 0 && !bytes.Equal(bytes.TrimSpace(meta), []byte("null")) && json.Valid(meta) {
		out = append(out, []byte(`,"_meta":`)...)
		out = append(out, meta...)
	}
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

func requestSessionID(r *http.Request) string {
	if r == nil {
		return ""
	}
	values := r.Header.Values("MCP-Session-Id")
	if len(values) != 1 {
		return ""
	}
	return values[0]
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

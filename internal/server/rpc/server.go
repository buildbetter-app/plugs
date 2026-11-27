package rpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const MaxBatchCalls = 100

// Config describes the actual server/store. Transport authentication stamps a
// Principal through WithPrincipal. Visible is an optional discovery policy;
// handlers/service remain responsible for authorizing actual resource access.
type Config struct {
	ServerVersion    string
	ProtocolVersion  string
	OntologyRevision model.Revision
	DefaultProject   string
	AuthMode         string
	Incarnation      string
	// CurrentIncarnation reads committed store metadata for each paging/hello
	// call. An installed provider must succeed; only an absent provider uses
	// the startup Incarnation. It must be safe for concurrent requests.
	CurrentIncarnation  func(context.Context) (string, error)
	Visible             func(context.Context, model.PrincipalRef, wire.MethodDesc) (bool, error)
	DroppedNotification func(context.Context, string)
	// GraphWriteCapacity bounds the active ordinary graph RPC and its waiters.
	// Zero disables queuing.
	GraphWriteCapacity int
	// CursorKey is an optional >=32-byte signing secret. Supplying a stable key
	// preserves cursors across server restarts; nil generates a per-server key.
	CursorKey []byte
}

type Server struct {
	registry   *Registry
	config     Config
	paging     *pageCodec
	graphWrite chan struct{}
	graphSlots chan struct{}
}
type principalKey struct{}

func WithPrincipal(ctx context.Context, by model.PrincipalRef) context.Context {
	return context.WithValue(ctx, principalKey{}, by)
}
func principal(ctx context.Context) (model.PrincipalRef, bool) {
	p, ok := ctx.Value(principalKey{}).(model.PrincipalRef)
	return p, ok
}

func New(registry *Registry, config Config) (*Server, error) {
	if registry == nil {
		return nil, fmt.Errorf("nil RPC registry")
	}
	if config.Incarnation == "" {
		return nil, fmt.Errorf("RPC requires store incarnation")
	}
	if config.ServerVersion == "" {
		config.ServerVersion = "0.1.0"
	}
	if config.ProtocolVersion == "" {
		config.ProtocolVersion = "1"
	}
	if config.AuthMode == "" {
		config.AuthMode = "anonymous"
	}
	if config.AuthMode != "anonymous" && config.AuthMode != "authenticated" {
		return nil, fmt.Errorf("unknown auth mode %q", config.AuthMode)
	}
	if config.GraphWriteCapacity < 0 {
		return nil, errors.New("RPC graph write capacity must not be negative")
	}
	if err := config.OntologyRevision.Validate(); err != nil {
		return nil, err
	}
	paging, err := newPageCodec(config)
	if err != nil {
		return nil, err
	}
	s := &Server{registry: registry.snapshot(), config: config, paging: paging}
	if config.GraphWriteCapacity > 0 {
		s.graphWrite = make(chan struct{}, 1)
		s.graphSlots = make(chan struct{}, config.GraphWriteCapacity)
	}
	for name, handler := range map[string]Handler{"server.hello": s.hello, "rpc.methods": s.methods, "rpc.describe": s.describe, "rpc.discover": s.discover} {
		if err := s.registry.Register(name, handler); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Dispatch processes a complete message. nil means that the message contained
// only notifications. Batches execute sequentially and responses retain order.
// IDs remain raw JSON bytes, including large/exponent numbers and escapes.
func (s *Server) Dispatch(ctx context.Context, data []byte) []byte {
	data = bytes.TrimSpace(data)
	if !json.Valid(data) {
		return s.failure(nil, "parse_error", "", nil)
	}
	if len(data) > 0 && data[0] == '[' {
		var batch []json.RawMessage
		_ = json.Unmarshal(data, &batch)
		if len(batch) == 0 || len(batch) > MaxBatchCalls {
			return s.failure(nil, "invalid_request", "batch must contain 1 to 100 calls", nil)
		}
		responses := make([][]byte, 0, len(batch))
		for _, item := range batch {
			if response := s.dispatchOne(ctx, item); len(response) > 0 {
				responses = append(responses, response)
			}
		}
		if len(responses) == 0 {
			return nil
		}
		out := []byte{'['}
		out = append(out, bytes.Join(responses, []byte{','})...)
		return append(out, ']')
	}
	return s.dispatchOne(ctx, data)
}

func (s *Server) dispatchOne(ctx context.Context, data []byte) []byte {
	value, err := wire.DecodeJSON(data)
	if err != nil {
		return s.failure(nil, "invalid_request", "", nil)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return s.failure(nil, "invalid_request", "request must be an object", nil)
	}
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(data, &raw)
	id, idPresent := raw["id"]
	if idPresent {
		switch object["id"].(type) {
		case string, json.Number, nil:
		default:
			return s.failure(nil, "invalid_request", "id must be a string, number or null", nil)
		}
	}
	method, methodOK := object["method"].(string)
	if object["jsonrpc"] != "2.0" || !methodOK {
		return s.failure(nil, "invalid_request", "expected jsonrpc 2.0 and a method name", nil)
	}
	for name := range object {
		if name != "jsonrpc" && name != "method" && name != "params" && name != "id" {
			return s.failure(id, "invalid_request", "unknown request member", nil)
		}
	}
	respond := func(code, hint string, details map[string]any) []byte {
		if !idPresent {
			return nil
		}
		return s.failure(id, code, hint, details)
	}
	bound, exists := s.registry.bindings[method]
	if !exists {
		return respond("method_not_found", "method has no bound handler", nil)
	}
	if !idPresent && bound.desc.Idempotency == "required" {
		if s.config.DroppedNotification != nil {
			s.config.DroppedNotification(ctx, method)
		} else {
			slog.WarnContext(ctx, "dropped write notification", "method", method)
		}
		return nil
	}
	params, exists := raw["params"]
	if !exists {
		params = json.RawMessage(`{}`)
	}
	if err = s.registry.catalog.ValidateParams(bound.desc, params); err != nil {
		return respond("invalid_params", err.Error(), nil)
	}
	var envelope struct {
		Meta wire.Meta `json:"_meta"`
	}
	// Params have already been schema-checked; this decode extracts only metadata.
	_ = json.Unmarshal(params, &envelope)
	meta := envelope.Meta
	if bound.desc.Idempotency == "required" {
		if err = wire.ValidateIdempotencyKey(meta.IdempotencyKey); err != nil {
			return respond("invalid_params", err.Error(), nil)
		}
	}
	if meta.Project == "" {
		meta.Project = s.config.DefaultProject
	}
	by, authenticated := principal(ctx)
	if !authenticated {
		if s.config.AuthMode == "authenticated" && method != "server.hello" && method != "rpc.discover" {
			return respond("unauthenticated", "", nil)
		}
		by = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	}
	if _, err = json.Marshal(by); err != nil {
		return respond("unauthenticated", "invalid authenticated principal", nil)
	}
	if !meta.DeadlineMS.IsZero() {
		ms, convErr := meta.DeadlineMS.Uint64()
		if convErr == nil && ms <= uint64((time.Duration(1<<63-1))/time.Millisecond) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
			defer cancel()
		}
	}
	if err = ctx.Err(); err != nil {
		return respond("busy", "request deadline or cancellation reached", nil)
	}
	paging := s.paging
	if s.config.CurrentIncarnation != nil && (method == "bead.blocked" || method == "bead.ready" || method == "bead.get" && selectsCommentPage(params) || method == "history.query" || method == "server.hello" || method == "memory.list") {
		incarnation, lookupErr := s.config.CurrentIncarnation(ctx)
		if lookupErr == nil && incarnation == "" {
			lookupErr = fmt.Errorf("current store incarnation is empty")
		}
		if lookupErr != nil {
			if !idPresent {
				return nil
			}
			return s.errorResponse(id, s.registry.catalog.MapError(lookupErr))
		}
		// Keep the signing key and ontology, but never mutate shared codec
		// state. Binding, decoding, service validation and encoding all use
		// this one request's committed incarnation snapshot.
		copy := *s.paging
		copy.incarnation = incarnation
		paging = &copy
	}
	write := bound.desc.Effect == "write"
	var queueWait time.Duration
	if s.graphWrite != nil && write && method != "bead.heartbeat" && (strings.HasPrefix(method, "bead.") || method == "tx.apply") {
		select {
		case s.graphSlots <- struct{}{}:
			defer func() { <-s.graphSlots }()
		default:
			return respond("busy", "graph write queue is full", nil)
		}
		queuedAt := time.Now()
		select {
		case s.graphWrite <- struct{}{}:
			defer func() { <-s.graphWrite }()
			queueWait = time.Since(queuedAt)
		case <-ctx.Done():
			return respond("busy", "graph write admission deadline or cancellation reached", nil)
		}
		if ctx.Err() != nil {
			return respond("busy", "graph write admission deadline or cancellation reached", nil)
		}
	}
	if write {
		ctx = driver.WithPublicationTiming(ctx, queueWait)
	}
	result, callErr := invoke(driver.WithPublicationMethod(ctx, method), bound.handler, Call{Method: method, Params: params, Meta: meta, Principal: by, paging: paging})
	if !idPresent {
		return nil
	}
	if callErr != nil {
		if uncertain, ok := driver.Indeterminate(callErr); ok {
			details := map[string]any{"outcome": "unknown"}
			if uncertain.IdempotencyKey != "" {
				details["idempotencyKey"] = uncertain.IdempotencyKey
			}
			return respond("busy", "Commit outcome is unknown. Retry the same request and idempotency key.", details)
		}
		if (write || strings.HasPrefix(method, "admin.restore.")) && (errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded)) {
			// A deadline joined during cleanup does not replace a typed refusal
			// from the operation. Only unclassified interruption becomes busy.
			var domainErr *model.Error
			var wireErr *wire.Error
			if !errors.As(callErr, &domainErr) && !errors.As(callErr, &wireErr) {
				return respond("busy", "request deadline or cancellation reached", nil)
			}
		}
		mapped := s.registry.catalog.MapError(callErr)
		if strings.HasPrefix(method, "admin.restore.") && mapped.Data.Code == "internal_error" {
			if mapped.Data.TraceID == "" {
				mapped.Data.TraceID = newTraceID()
			}
			slog.ErrorContext(ctx, "admin restore internal error", "method", method, "traceId", mapped.Data.TraceID, "error", callErr)
		}
		return s.errorResponse(id, mapped)
	}
	replayed := false
	if outcome, ok := result.(Outcome); ok {
		result, replayed = outcome.Result, outcome.Replayed
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return s.failure(id, "internal_error", "", nil)
	}
	if err = s.registry.catalog.ValidateResult(bound.desc, encoded); err != nil {
		return s.failure(id, "internal_error", "handler returned an invalid result", nil)
	}
	return response(id, "result", encoded, replayed)
}

func invoke(ctx context.Context, handler Handler, call Call) (result any, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = fmt.Errorf("handler panic")
		}
	}()
	return handler(ctx, call)
}

func response(id []byte, member string, payload []byte, replayed bool) []byte {
	if len(id) == 0 {
		id = []byte("null")
	}
	out := []byte(`{"jsonrpc":"2.0","id":`)
	out = append(out, id...)
	out = append(out, []byte(`,"`+member+`":`)...)
	out = append(out, payload...)
	if replayed {
		out = append(out, `,"_meta":{"replayed":true}`...)
	}
	return append(out, '}')
}
func (s *Server) failure(id []byte, code, hint string, details map[string]any) []byte {
	return s.errorResponse(id, s.registry.catalog.NewError(code, hint, details))
}

// TransportError encodes an admission or framing failure before a request ID
// has been admitted. It uses the dispatcher catalog and trace handling, with
// an id:null envelope. Arbitrary errors are sanitized as internal_error.
func (s *Server) TransportError(err error) []byte {
	return s.errorResponse(nil, s.registry.catalog.MapError(err))
}

func (s *Server) errorResponse(id []byte, e *wire.Error) []byte {
	if s.registry.catalog.ValidateError(e) != nil {
		e = s.registry.catalog.NewError("internal_error", "", nil)
	}
	if e.Data.TraceID == "" {
		e.Data.TraceID = newTraceID()
	}
	encoded, err := json.Marshal(e)
	if err != nil {
		encoded = []byte(`{"code":-32603,"message":"internal error","data":{"code":"internal_error","retryable":false}}`)
	}
	return response(id, "error", encoded, false)
}

func newTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}

func (s *Server) visible(ctx context.Context, by model.PrincipalRef) ([]wire.MethodDesc, []string, string, error) {
	descriptors := []wire.MethodDesc{}
	names := []string{}
	for _, d := range s.registry.Descriptors() {
		resourceFree := d.Name == "server.hello" || d.Name == "rpc.methods" || d.Name == "rpc.describe" || d.Name == "rpc.discover" || d.Permission == "none"
		if !resourceFree && s.config.Visible != nil {
			allowed, err := s.config.Visible(ctx, by, d)
			if err != nil {
				return nil, nil, "", err
			}
			if !allowed {
				continue
			}
		}
		descriptors = append(descriptors, d)
		names = append(names, d.Name)
	}
	encoded, _ := json.Marshal(names)
	sum := sha256.Sum256(encoded)
	return descriptors, names, hex.EncodeToString(sum[:]), nil
}

func (s *Server) hello(ctx context.Context, c Call) (any, error) {
	_, _, hash, err := s.visible(ctx, c.Principal)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"serverVersion": s.config.ServerVersion, "protocolVersion": s.config.ProtocolVersion, "methodsHash": hash, "ontologyRevision": s.config.OntologyRevision, "authMode": s.config.AuthMode, "incarnation": c.paging.incarnation}
	if c.Meta.Project != "" {
		result["defaultProject"] = c.Meta.Project
	} else if s.config.DefaultProject != "" {
		result["defaultProject"] = s.config.DefaultProject
	}
	return result, nil
}
func (s *Server) methods(ctx context.Context, c Call) (any, error) {
	_, names, hash, err := s.visible(ctx, c.Principal)
	return map[string]any{"methods": names, "methodsHash": hash}, err
}
func (s *Server) describe(ctx context.Context, c Call) (any, error) {
	var params struct {
		Method *string `json:"method"`
	}
	_ = json.Unmarshal(c.Params, &params)
	descriptors, _, hash, err := s.visible(ctx, c.Principal)
	if err != nil {
		return nil, err
	}
	if params.Method != nil {
		for _, d := range descriptors {
			if d.Name == *params.Method {
				return map[string]any{"method": d, "errors": s.registry.catalog.ErrorCatalog(), "components": s.registry.catalog.ComponentsJSON()}, nil
			}
		}
		return nil, s.registry.catalog.NewError("method_not_found", "", nil)
	}
	return map[string]any{"methods": descriptors, "methodsHash": hash, "errors": s.registry.catalog.ErrorCatalog(), "components": s.registry.catalog.ComponentsJSON()}, nil
}
func (s *Server) discover(ctx context.Context, c Call) (any, error) {
	descriptors, _, hash, err := s.visible(ctx, c.Principal)
	if err != nil {
		return nil, err
	}
	doc, err := s.registry.catalog.GenerateOpenRPC(descriptors)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	_ = json.Unmarshal(doc, &result)
	result["methodsHash"] = hash
	// Discovery's bounded result contains the version, methods and hash. Full
	// component definitions are available from OpenRPC for generated clients.
	return map[string]any{"openrpc": result["openrpc"], "methods": result["methods"], "methodsHash": hash}, nil
}

// OpenRPC returns a complete generated schema document for this principal.
func (s *Server) OpenRPC(ctx context.Context, by model.PrincipalRef) ([]byte, error) {
	descriptors, _, _, err := s.visible(ctx, by)
	if err != nil {
		return nil, err
	}
	return s.registry.catalog.GenerateOpenRPC(descriptors)
}

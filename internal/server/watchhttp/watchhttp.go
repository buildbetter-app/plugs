// Package watchhttp is the explicit transport boundary for the watch manager.
// It binds the real manager to the existing RPC registry and exposes a small
// NDJSON stream endpoint. The package owns no event data, authorization policy
// or session lifecycle; those are supplied by the composition root.
package watchhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
)

const (
	// DefaultPath is intentionally separate from /rpc/v1. The shared JSON-RPC
	// transport remains unchanged; a composition can mount this handler beside
	// it when it wants long-lived delivered bytes.
	DefaultPath = "/watch/v1"
	// DefaultMaxRequestBytes bounds the small stream-attach request. Event
	// frames are bounded by watch.Manager's queue and the model codec.
	DefaultMaxRequestBytes = 64 << 10
)

var (
	ErrPrincipalRequired = errors.New("watch stream requires an authenticated principal")
)

type contextKey uint8

const (
	principalKey contextKey = iota + 1
	sessionKey
	lifetimeKey
)

// WithPrincipal and WithSession are trusted host composition seams. Never
// populate them from client-asserted metadata. The lifetime must be the actual
// connection/session context; an RPC request ends before stream attachment.
func WithPrincipal(ctx context.Context, principal model.PrincipalRef) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

func Principal(ctx context.Context) (model.PrincipalRef, bool) {
	if ctx == nil {
		return model.PrincipalRef{}, false
	}
	principal, ok := ctx.Value(principalKey).(model.PrincipalRef)
	return principal, ok
}

func WithSession(ctx context.Context, id string, lifetime context.Context) context.Context {
	ctx = context.WithValue(ctx, sessionKey, id)
	return context.WithValue(ctx, lifetimeKey, lifetime)
}

func SessionID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(sessionKey).(string)
	return id
}

// Adapter binds a real watch.Manager to RPC and NDJSON. New rejects a nil
// manager. The host owns authentication, authorization and session wiring.
type Adapter struct {
	manager       *watch.Manager
	path          string
	maxBody       int64
	ontology      model.Revision
	policyVersion string
}

// Ontology and PolicyVersion are trusted immutable opening generations when
// the manager has no dynamic BindingSource. Mutable installations supply one.
type Config struct {
	Ontology      model.Revision
	PolicyVersion string
	Path          string
	MaxBody       int64
}

func New(manager *watch.Manager, config Config) (*Adapter, error) {
	if manager == nil {
		return nil, fmt.Errorf("watchhttp: manager is required")
	}
	if config.Path == "" {
		config.Path = DefaultPath
	}
	if !strings.HasPrefix(config.Path, "/") || strings.ContainsAny(config.Path, "?#") {
		return nil, fmt.Errorf("watchhttp: path must be an absolute path")
	}
	if config.MaxBody == 0 {
		config.MaxBody = DefaultMaxRequestBytes
	}
	if config.MaxBody < 1 || config.MaxBody > 1<<20 {
		return nil, fmt.Errorf("watchhttp: max body must be between 1 and 1048576 bytes")
	}
	if err := config.Ontology.Validate(); err != nil {
		if config.Ontology.IsZero() {
			config.Ontology = model.Natural64(1)
		} else {
			return nil, err
		}
	}
	return &Adapter{manager: manager, path: config.Path, maxBody: config.MaxBody, ontology: config.Ontology, policyVersion: config.PolicyVersion}, nil
}

// Register installs subscription/unsubscription and explicit view refusal. Registration is kept in
// this package so the shared RPC transport and catalog remain untouched; the
// registry still validates each handler against its existing OpenRPC schema.
func (a *Adapter) Register(registry *rpc.Registry) error {
	if a == nil || a.manager == nil || registry == nil {
		return fmt.Errorf("watchhttp: manager and registry are required")
	}
	if err := registry.Register("event.subscribe", a.subscribe); err != nil {
		return err
	}
	if err := registry.Register("event.unsubscribe", a.unsubscribe); err != nil {
		return err
	}
	if err := registry.Register("view.watch", a.viewWatch); err != nil {
		return err
	}
	return registry.Register("bead.watch", a.beadWatch)
}

type filterParams struct {
	Query     string   `json:"query,omitempty"`
	Resources []string `json:"resources,omitempty"`
	Types     []string `json:"types,omitempty"`
	Events    []string `json:"events,omitempty"`
	ViewID    string   `json:"viewId,omitempty"`
}

type subscribeParams struct {
	Meta   wire.Meta    `json:"_meta"`
	Filter filterParams `json:"filter"`
	From   string       `json:"from,omitempty"`
	ViewID string       `json:"viewId,omitempty"`
}

type viewWatchParams struct {
	Meta   wire.Meta `json:"_meta"`
	ViewID string    `json:"viewId"`
	From   string    `json:"from,omitempty"`
}

type beadWatchParams struct {
	Meta wire.Meta `json:"_meta"`
	ID   model.Ref `json:"id"`
	From string    `json:"from,omitempty"`
}

type unsubscribeParams struct {
	Meta           wire.Meta `json:"_meta"`
	SubscriptionID string    `json:"subscriptionId"`
}

func (a *Adapter) subscribe(ctx context.Context, call rpc.Call) (any, error) {
	var params subscribeParams
	if err := decode(call.Params, &params); err != nil {
		return nil, invalidParams(err)
	}
	filter, viewID, err := convertFilter(params.Filter, params.ViewID)
	if err != nil {
		return nil, invalidParams(err)
	}
	return a.open(ctx, call.Principal, watch.Request{Method: watch.MethodEventSubscribe, Filter: filter, ViewID: viewID, From: params.From})
}

func (a *Adapter) viewWatch(ctx context.Context, call rpc.Call) (any, error) {
	var params viewWatchParams
	if err := decode(call.Params, &params); err != nil {
		return nil, invalidParams(err)
	}
	if params.ViewID == "" {
		return nil, invalidParams(errors.New("viewId is required"))
	}
	return nil, mapError(watch.ErrViewCheckpointRequired)
}

func (a *Adapter) beadWatch(ctx context.Context, call rpc.Call) (any, error) {
	var params beadWatchParams
	if err := decode(call.Params, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.ID.Validate(); err != nil || !params.ID.IsID() || params.ID.ID == "" || !params.ID.Branch.IsZero() {
		return nil, invalidParams(errors.New("bead.watch requires an ID reference without a branch"))
	}
	return a.open(ctx, call.Principal, watch.Request{Method: watch.MethodBeadWatch, Resource: "bead/" + params.ID.ID, From: params.From})
}

func (a *Adapter) open(ctx context.Context, principal model.PrincipalRef, request watch.Request) (any, error) {
	request.Principal = principal
	request.Ontology, request.PolicyVersion = a.ontology, a.policyVersion
	request.SessionID = SessionID(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lifetime, ok := ctx.Value(lifetimeKey).(context.Context)
	if !ok || lifetime == nil || request.SessionID == "" {
		return nil, invalidParams(errors.New("watch requires a trusted connection/session lifetime"))
	}
	subscription, err := a.manager.Open(lifetime, request)
	if err != nil {
		return nil, mapError(err)
	}
	return map[string]any{"subscriptionId": subscription.ID(), "cursor": subscription.Cursor()}, nil
}

func (a *Adapter) unsubscribe(ctx context.Context, call rpc.Call) (any, error) {
	var params unsubscribeParams
	if err := decode(call.Params, &params); err != nil {
		return nil, invalidParams(err)
	}
	if params.SubscriptionID == "" {
		return nil, invalidParams(errors.New("subscriptionId is required"))
	}
	if err := a.manager.CancelOwned(ctx, params.SubscriptionID, call.Principal); err != nil {
		return nil, mapError(err)
	}
	return map[string]any{"unsubscribed": true}, nil
}

func convertFilter(value filterParams, viewID string) (coreevents.Filter, string, error) {
	if value.Query != "" {
		return coreevents.Filter{}, "", errors.New("watch query filters require the real views service")
	}
	if len(value.Types) != 0 {
		return coreevents.Filter{}, "", errors.New("bead-type filters require the real views service")
	}
	if viewID == "" {
		viewID = value.ViewID
	}
	if viewID != "" || value.ViewID != "" {
		return coreevents.Filter{}, "", watch.ErrViewCheckpointRequired
	}
	return coreevents.Filter{Types: append([]string(nil), value.Events...), Resources: append([]string(nil), value.Resources...)}, "", nil
}

func decode(data []byte, dst any) error {
	if len(data) == 0 {
		return errors.New("params are required")
	}
	return wire.DecodeInto(data, dst)
}

func invalidParams(err error) error {
	return &model.Error{Message: err.Error(), Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
}

func mapError(err error) error {
	switch {
	case errors.Is(err, watch.ErrTooManyWatches):
		return &model.Error{Data: model.ErrorData{Code: "too_many_sessions"}}
	case errors.Is(err, watch.ErrSubscriptionClosed):
		return &model.Error{Data: model.ErrorData{Code: "not_found"}}
	case errors.Is(err, coreevents.ErrCursorInvalid):
		return &model.Error{Data: model.ErrorData{Code: "cursor_invalid"}}
	case errors.Is(err, watch.ErrUnsupportedMethod), errors.Is(err, watch.ErrInvalidRequest), errors.Is(err, watch.ErrAlreadyAttached), errors.Is(err, watch.ErrDuplicateSubscription), errors.Is(err, watch.ErrBindingChanged), errors.Is(err, watch.ErrViewCheckpointRequired):
		return &model.Error{Data: model.ErrorData{Code: "invalid_params", Hint: err.Error()}}
	default:
		return err
	}
}

type streamRequest struct {
	SubscriptionID string `json:"subscriptionId"`
}

// ServeHTTP attaches an already-open subscription and writes one canonical
// event.notify object per line. Authentication and the stable session are
// supplied through WithPrincipal/WithSession by the caller; this handler
// never trusts body metadata for either value.
func (a *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a == nil || a.manager == nil {
		http.Error(w, "watch manager unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.URL.Path != a.path || r.URL.RawQuery != "" || r.URL.ForceQuery {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	principal, ok := Principal(r.Context())
	if !ok || principal.Kind == "" || principal.ID == "" {
		http.Error(w, ErrPrincipalRequired.Error(), http.StatusUnauthorized)
		return
	}
	body := http.MaxBytesReader(w, r.Body, a.maxBody)
	defer body.Close()
	var request streamRequest
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.SubscriptionID == "" {
		http.Error(w, "invalid subscription request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		http.Error(w, "invalid subscription request", http.StatusBadRequest)
		return
	}
	subscription, err := a.manager.Attach(r.Context(), request.SubscriptionID, principal)
	if err != nil {
		http.Error(w, "subscription not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	_ = a.Stream(r.Context(), w, subscription)
}

// Stream consumes a subscription and writes canonical event.notify lines. It
// owns the subscription for the duration of the stream and always closes it,
// including when the client disconnects or the writer fails.
func (a *Adapter) Stream(ctx context.Context, dst io.Writer, subscription *watch.Subscription) error {
	if ctx == nil || dst == nil || subscription == nil {
		return watch.ErrInvalidRequest
	}
	defer subscription.Close()
	encoder := json.NewEncoder(dst)
	for {
		frame, err := subscription.Next(ctx)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := encoder.Encode(frame); err != nil {
			return err
		}
		if flusher, ok := dst.(interface{ Flush() }); ok {
			flusher.Flush()
		}
		if frame.Terminal() {
			return watch.ErrResetRequired
		}
	}
}

package httpjson

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
)

// Handler is reusable and safe for concurrent requests. It owns no store,
// lease or listener. The composition root must configure matching RPC auth
// mode and actual authorization before enabling scoped credentials in authn.
type Handler struct {
	rpc   *rpc.Server
	authn *authn.Authenticator
	cfg   Config
	slots chan struct{}
}

func New(server *rpc.Server, authenticator *authn.Authenticator, config Config) (*Handler, error) {
	if server == nil || authenticator == nil {
		return nil, fmt.Errorf("httpjson: RPC server and authenticator are required")
	}
	config, err := config.defaults()
	if err != nil {
		return nil, err
	}
	return &Handler{rpc: server, authn: authenticator, cfg: config, slots: make(chan struct{}, config.MaxConcurrentRequests)}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path != Path || r.URL.EscapedPath() != Path || r.URL.RawQuery != "" || r.URL.ForceQuery {
		h.fail(w, http.StatusNotFound, "not_found", "unknown HTTP endpoint")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		h.fail(w, http.StatusMethodNotAllowed, "invalid_request", "use POST /rpc/v1")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.RequestTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	identity, err := h.authn.AdmitHTTP(r)
	if err != nil {
		status := http.StatusForbidden
		switch {
		case errors.Is(err, authn.ErrUnauthenticated), errors.Is(err, authn.ErrTokenExpired):
			status = http.StatusUnauthorized
			w.Header().Set("WWW-Authenticate", "Bearer")
		case errors.Is(err, authn.ErrUnavailable):
			status = http.StatusServiceUnavailable
		}
		h.respond(w, status, h.rpc.TransportError(err))
		return
	}
	// Cookies and compressed bodies have no credential/decoding semantics in
	// this transport. No CORS response headers or preflight path are installed.
	if len(r.Header.Values("Cookie")) != 0 {
		h.fail(w, http.StatusForbidden, "forbidden", "cookie credentials are unsupported")
		return
	}
	if len(r.Header.Values("Content-Encoding")) != 0 {
		h.fail(w, http.StatusUnsupportedMediaType, "invalid_request", "content encodings are unsupported")
		return
	}
	contentTypes := r.Header.Values("Content-Type")
	if len(contentTypes) != 1 || !jsonContentType(contentTypes[0]) {
		h.fail(w, http.StatusUnsupportedMediaType, "invalid_request", "Content-Type must be application/json with optional charset=utf-8")
		return
	}
	if r.ContentLength > h.cfg.MaxBodyBytes {
		h.fail(w, http.StatusRequestEntityTooLarge, "invalid_request", "HTTP request body exceeds the configured limit")
		return
	}
	if ctx.Err() != nil {
		h.fail(w, http.StatusRequestTimeout, "busy", "request deadline or cancellation reached")
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		h.fail(w, http.StatusTooManyRequests, "rate_limited", "HTTP request concurrency limit reached")
		return
	}
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	if err := controller.SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		h.fail(w, http.StatusServiceUnavailable, "busy", "HTTP read deadline unavailable")
		return
	}
	defer controller.SetReadDeadline(time.Time{})
	body := http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes)
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		var timeout net.Error
		switch {
		case errors.As(err, &tooLarge):
			h.fail(w, http.StatusRequestEntityTooLarge, "invalid_request", "HTTP request body exceeds the configured limit")
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()):
			h.fail(w, http.StatusRequestTimeout, "busy", "HTTP body deadline or cancellation reached")
		default:
			h.fail(w, http.StatusBadRequest, "invalid_request", "unable to read HTTP request body")
		}
		return
	}
	if ctx.Err() != nil {
		h.fail(w, http.StatusRequestTimeout, "busy", "request deadline or cancellation reached")
		return
	}
	response := h.rpc.Dispatch(rpc.WithPrincipal(ctx, identity.Principal()), data)
	if len(response) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.respond(w, http.StatusOK, response)
}

func jsonContentType(raw string) bool {
	typ, params, err := mime.ParseMediaType(raw)
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

func (h *Handler) fail(w http.ResponseWriter, status int, code, hint string) {
	h.respond(w, status, h.rpc.TransportError(&model.Error{Data: model.ErrorData{Code: model.ErrorCode(code), Hint: hint}}))
}

func (h *Handler) respond(w http.ResponseWriter, status int, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

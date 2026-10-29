package authnoidc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

const (
	LoginPath    = "/auth/oidc/login"
	CallbackPath = "/auth/oidc/callback"
)

type loginResponse struct {
	AuthorizationURL string    `json:"authorizationUrl"`
	State            string    `json:"state"`
	Binding          string    `json:"binding"`
	ExpiresAt        time.Time `json:"expiresAt"`
}

type callbackRequest struct {
	State   string `json:"state"`
	Binding string `json:"binding"`
	Code    string `json:"code"`
}

type sessionResponse struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// HTTPHandler composes the bounded login/callback adapter with the existing
// RPC handler. It owns no cookies or credentials; the initiating public client
// keeps Binding and receives the opaque core-session bearer after callback.
// Foreign origins are admitted only when the provider's operator allowlist
// contains the exact origin. The wrapped RPC authenticator must also be built
// with authn.Config{CrossOriginAuthorizer: p.AllowOrigin}.
func (p *Provider) HTTPHandler(next http.Handler) (http.Handler, error) {
	if p == nil || next == nil {
		return nil, fmt.Errorf("authnoidc: provider and next HTTP handler are required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin, ok := requestOrigin(r)
		if !ok {
			if isLoginPath(r.URL.Path) || (r.Method == http.MethodOptions && isAuthPath(r.URL.Path)) {
				writeHTTPError(w, http.StatusForbidden, authn.ErrForbidden)
				return
			}
			// Let the wrapped RPC transport classify malformed Origin headers in
			// its normal JSON-RPC error envelope.
			next.ServeHTTP(w, r)
			return
		}
		// Keep browser login/callback failures as ordinary HTTP errors, while
		// allowing the wrapped RPC adapter to produce its normal JSON-RPC
		// envelope for a disallowed foreign Origin.
		if isLoginPath(r.URL.Path) && origin != "" && !p.OriginAllowed(origin) && !sameRequestOrigin(r, origin) {
			writeHTTPError(w, http.StatusForbidden, authn.ErrForbidden)
			return
		}
		if origin != "" && p.OriginAllowed(origin) {
			setCORS(w, origin)
		}
		if r.Method == http.MethodOptions && isAuthPath(r.URL.Path) {
			if origin == "" || !p.OriginAllowed(origin) {
				writeHTTPError(w, http.StatusForbidden, authn.ErrForbidden)
				return
			}
			handlePreflight(w, r)
			return
		}
		switch r.URL.Path {
		case LoginPath:
			p.handleLogin(w, r, origin)
		case CallbackPath:
			p.handleCallback(w, r, origin)
		default:
			next.ServeHTTP(w, r)
		}
	}), nil
}

func (p *Provider) handleLogin(w http.ResponseWriter, r *http.Request, origin string) {
	if r.Method != http.MethodGet || r.URL.RawQuery != "" || hasCredentialHeaders(r) {
		writeHTTPError(w, http.StatusForbidden, authn.ErrForbidden)
		return
	}
	boundOrigin := ""
	if origin != "" && p.OriginAllowed(origin) {
		boundOrigin = origin
	}
	login, err := p.BeginForOrigin(r.Context(), boundOrigin)
	if err != nil {
		writeHTTPError(w, statusForAuthn(err), err)
		return
	}
	writeJSON(w, http.StatusOK, loginResponse{AuthorizationURL: login.AuthorizationURL, State: login.State, Binding: login.Binding, ExpiresAt: login.ExpiresAt})
}

func (p *Provider) handleCallback(w http.ResponseWriter, r *http.Request, origin string) {
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || hasCredentialHeaders(r) {
		writeHTTPError(w, http.StatusForbidden, authn.ErrForbidden)
		return
	}
	contentType := r.Header.Values("Content-Type")
	if len(contentType) != 1 {
		writeHTTPError(w, http.StatusUnsupportedMediaType, authn.ErrUnauthenticated)
		return
	}
	typ, params, err := mime.ParseMediaType(contentType[0])
	if err != nil || typ != "application/json" || (len(params) != 0 && !(len(params) == 1 && strings.EqualFold(params["charset"], "utf-8"))) {
		writeHTTPError(w, http.StatusUnsupportedMediaType, authn.ErrUnauthenticated)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
	if err != nil || len(data) == 0 || len(data) > 16<<10 {
		writeHTTPError(w, http.StatusBadRequest, authn.ErrUnauthenticated)
		return
	}
	var input callbackRequest
	if err := wire.DecodeInto(data, &input); err != nil || input.State == "" || input.Binding == "" || input.Code == "" {
		writeHTTPError(w, http.StatusBadRequest, authn.ErrUnauthenticated)
		return
	}
	session, err := p.CompleteForOrigin(r.Context(), origin, input.State, input.Binding, input.Code)
	if err != nil {
		writeHTTPError(w, statusForAuthn(err), err)
		return
	}
	writeJSON(w, http.StatusOK, sessionResponse{AccessToken: session.Token, TokenType: "Bearer", ExpiresAt: session.ExpiresAt})
}

func hasCredentialHeaders(r *http.Request) bool {
	return len(r.Header.Values("Cookie")) != 0 || len(r.Header.Values("Authorization")) != 0 || len(r.Header.Values("Content-Encoding")) != 0
}

func requestOrigin(r *http.Request) (string, bool) {
	values := r.Header.Values("Origin")
	if len(values) > 1 {
		return "", false
	}
	if len(values) == 0 {
		return "", true
	}
	if values[0] == "" {
		return "", false
	}
	origin, ok := normalizeOrigin(values[0])
	return origin, ok
}

func sameRequestOrigin(r *http.Request, origin string) bool {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	expected, ok := normalizeOrigin(scheme + "://" + r.Host)
	return ok && expected == origin
}

func isAuthPath(path string) bool {
	return path == LoginPath || path == CallbackPath || path == "/rpc/v1"
}

func isLoginPath(path string) bool {
	return path == LoginPath || path == CallbackPath
}

func handlePreflight(w http.ResponseWriter, r *http.Request) {
	methods := r.Header.Values("Access-Control-Request-Method")
	if len(methods) != 1 || (methods[0] != http.MethodGet && methods[0] != http.MethodPost) {
		writeHTTPError(w, http.StatusForbidden, authn.ErrForbidden)
		return
	}
	for _, line := range r.Header.Values("Access-Control-Request-Headers") {
		for _, raw := range strings.Split(line, ",") {
			name := strings.ToLower(strings.TrimSpace(raw))
			if name != "authorization" && name != "content-type" && name != "plug-rpc" {
				writeHTTPError(w, http.StatusForbidden, authn.ErrForbidden)
				return
			}
		}
	}
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Plug-RPC")
	w.Header().Set("Access-Control-Max-Age", "300")
	w.WriteHeader(http.StatusNoContent)
}

func setCORS(w http.ResponseWriter, origin string) {
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Add("Vary", "Origin")
}

func statusForAuthn(err error) int {
	switch {
	case errors.Is(err, authn.ErrUnavailable):
		return http.StatusServiceUnavailable
	case errors.Is(err, authn.ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, authn.ErrUnauthenticated), errors.Is(err, authn.ErrTokenExpired):
		return http.StatusUnauthorized
	default:
		return http.StatusUnauthorized
	}
}

func canonicalFailure(err error) string {
	for _, candidate := range []authn.Failure{authn.ErrUnauthenticated, authn.ErrTokenExpired, authn.ErrUnavailable, authn.ErrForbidden} {
		if errors.Is(err, candidate) {
			return string(candidate)
		}
	}
	return string(authn.ErrUnauthenticated)
}

func writeHTTPError(w http.ResponseWriter, status int, code error) {
	writeJSON(w, status, map[string]string{"error": canonicalFailure(code)})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// CarryIdentity wraps a custom admitted HTTP handler to preserve the opaque
// identity for D32 consumers through authn.RequireInteractiveUser(ctx, by).
// The standard httpjson.Handler carries this identity itself; this helper is
// retained for custom compositions; it adds no cookies or transport
// exemptions.
func CarryIdentity(a *authn.Authenticator, admitted http.Handler) (http.Handler, error) {
	if a == nil || admitted == nil {
		return nil, fmt.Errorf("authnoidc: authenticator and admitted HTTP handler are required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := a.AdmitHTTP(r)
		if err == nil {
			r = r.WithContext(authn.WithIdentity(r.Context(), identity))
		}
		admitted.ServeHTTP(w, r)
	}), nil
}

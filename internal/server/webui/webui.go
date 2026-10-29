// Package webui serves Plug's embedded, read-only shared-server web UI.
//
// The handler owns no RPC server, store, listener, authentication provider or
// browser session. A composition root mounts it beside the admitted RPC
// transport and supplies the path and project that the browser must use. The
// UI never manufactures graph data: every row is returned by the configured
// RPC endpoint, and capabilities that are not advertised remain unavailable.
package webui

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strings"
	"unicode"

	"github.com/buildbetter-app/plugs/internal/server/authn"
)

// Config is the explicit composition boundary between the shared server and
// the embedded UI. RPCPath must point at the same admitted HTTP/RPC handler
// mounted by the composition root; it is never proxied or simulated here.
type Config struct {
	MountPath string
	RPCPath   string
	Project   string
	MaxRows   int

	// BlockedViewID and BlockedViewDefinition are optional. They are used only
	// when the server advertises view.run/view.explain. The server remains the
	// authority for validating the definition and its authorization.
	BlockedViewID         string
	BlockedViewDefinition json.RawMessage
}

type browserConfig struct {
	RPCPath               string          `json:"rpcPath"`
	Project               string          `json:"project"`
	MaxRows               int             `json:"maxRows"`
	BlockedViewID         string          `json:"blockedViewId,omitempty"`
	BlockedViewDefinition json.RawMessage `json:"blockedViewDefinition,omitempty"`
}

// Handler serves only files below Config.MountPath. It is safe for concurrent
// requests and has no mutable request-specific state.
type Handler struct {
	cfg    Config
	config []byte
	assets fs.FS
	admit  *authn.Authenticator
}

//go:embed static/index.html static/app.js static/styles.css static/favicon.svg
var embedded embed.FS

const contentSecurityPolicy = "default-src 'self'; base-uri 'none'; object-src 'none'; frame-src 'none'; frame-ancestors 'none'; form-action 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; font-src 'self'"

// New validates the explicit browser/RPC composition and returns an embedded
// handler. Unsupported UI methods are represented in the browser as an
// unavailable state; New never accepts a fake data provider.
func New(config Config) (*Handler, error) {
	var err error
	config.MountPath, err = mountPath(config.MountPath)
	if err != nil {
		return nil, err
	}
	config.RPCPath, err = rpcPath(config.RPCPath)
	if err != nil {
		return nil, err
	}
	if config.Project == "" {
		config.Project = "default"
	}
	if err := validText(config.Project, "project", 200); err != nil {
		return nil, err
	}
	if config.MaxRows == 0 {
		config.MaxRows = 25
	}
	if config.MaxRows < 1 || config.MaxRows > 100 {
		return nil, fmt.Errorf("webui: max rows must be between 1 and 100")
	}
	if err := validText(config.BlockedViewID, "blocked view id", 200); err != nil {
		return nil, err
	}
	if len(config.BlockedViewDefinition) > 16<<10 {
		return nil, fmt.Errorf("webui: blocked view definition exceeds 16 KiB")
	}
	if len(config.BlockedViewDefinition) != 0 {
		var definition map[string]json.RawMessage
		if err := json.Unmarshal(config.BlockedViewDefinition, &definition); err != nil || definition == nil {
			return nil, fmt.Errorf("webui: blocked view definition must be a JSON object")
		}
		config.BlockedViewDefinition = append(json.RawMessage(nil), config.BlockedViewDefinition...)
	}

	encoded, err := json.Marshal(browserConfig{
		RPCPath:               config.RPCPath,
		Project:               config.Project,
		MaxRows:               config.MaxRows,
		BlockedViewID:         config.BlockedViewID,
		BlockedViewDefinition: config.BlockedViewDefinition,
	})
	if err != nil {
		return nil, fmt.Errorf("webui: encode browser config: %w", err)
	}
	admit, err := authn.New(authn.Config{})
	if err != nil {
		return nil, fmt.Errorf("webui: anonymous admission: %w", err)
	}
	return &Handler{cfg: config, config: encoded, assets: embedded, admit: admit}, nil
}

func mountPath(raw string) (string, error) {
	if raw == "" {
		raw = "/ui/"
	}
	if !strings.HasPrefix(raw, "/") || !strings.HasSuffix(raw, "/") ||
		!plainPath(raw) || raw != path.Clean(raw)+"/" || raw == "/" {
		return "", fmt.Errorf("webui: mount path must be a clean absolute path ending in /")
	}
	return raw, nil
}

func rpcPath(raw string) (string, error) {
	if raw == "" {
		raw = "/rpc/v1"
	}
	if !strings.HasPrefix(raw, "/") || strings.HasSuffix(raw, "/") ||
		!plainPath(raw) || raw != path.Clean(raw) || raw == "/" {
		return "", fmt.Errorf("webui: RPC path must be a clean absolute path")
	}
	return raw, nil
}

func plainPath(raw string) bool {
	return !strings.ContainsAny(raw, "\\%") && (&url.URL{Path: raw}).EscapedPath() == raw
}

func validText(value, name string, max int) error {
	if len(value) > max || strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("webui: invalid %s", name)
	}
	return nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.setHeaders(w)
	if r == nil {
		h.writeError(w, false, http.StatusBadRequest, "invalid request")
		return
	}
	// Static navigations cannot send Plug-RPC. Reuse the existing anonymous
	// Host/peer/local-listener/Origin checks with an internal sentinel; this
	// grants no RPC bypass and does not alter the caller's request or headers.
	if len(r.Header.Values("Cookie")) != 0 || len(r.Header.Values("Authorization")) != 0 {
		h.writeError(w, r.Method != http.MethodHead, http.StatusForbidden, "browser sessions and credentials are not configured")
		return
	}
	probe := r.Clone(r.Context())
	probe.Header = r.Header.Clone()
	probe.Header.Set("Plug-RPC", "1")
	if _, err := h.admit.AdmitHTTP(probe); err != nil {
		h.writeError(w, r.Method != http.MethodHead, http.StatusForbidden, "the anonymous UI requires same-origin loopback access")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		h.writeError(w, true, http.StatusMethodNotAllowed, "the UI is read-only")
		return
	}

	root := strings.TrimSuffix(h.cfg.MountPath, "/")
	if r.URL.EscapedPath() != r.URL.Path {
		h.writeError(w, r.Method != http.MethodHead, http.StatusNotFound, "UI endpoint not found")
		return
	}
	if r.URL.Path == root {
		http.Redirect(w, r, h.cfg.MountPath, http.StatusPermanentRedirect)
		return
	}
	if !strings.HasPrefix(r.URL.Path, h.cfg.MountPath) {
		h.writeError(w, r.Method == http.MethodGet, http.StatusNotFound, "UI endpoint not found")
		return
	}
	asset := strings.TrimPrefix(r.URL.Path, h.cfg.MountPath)
	if asset == "" {
		asset = "index.html"
	}
	if asset == "config.json" {
		h.writeBytes(w, r.Method == http.MethodGet, http.StatusOK, "application/json; charset=utf-8", h.config)
		return
	}
	if asset != "index.html" && asset != "app.js" && asset != "styles.css" && asset != "favicon.svg" {
		h.writeError(w, r.Method == http.MethodGet, http.StatusNotFound, "UI asset not found")
		return
	}
	data, err := fs.ReadFile(h.assets, "static/"+asset)
	if err != nil {
		h.writeError(w, r.Method == http.MethodGet, http.StatusNotFound, "UI asset not found")
		return
	}
	contentType := map[string]string{
		"index.html":  "text/html; charset=utf-8",
		"app.js":      "text/javascript; charset=utf-8",
		"styles.css":  "text/css; charset=utf-8",
		"favicon.svg": "image/svg+xml",
	}[asset]
	h.writeBytes(w, r.Method == http.MethodGet, http.StatusOK, contentType, data)
}

func (h *Handler) setHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", contentSecurityPolicy)
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
	w.Header().Set("Permissions-Policy", "camera=(), geolocation=(), microphone=()")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
}

func (h *Handler) writeBytes(w http.ResponseWriter, body bool, status int, contentType string, data []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(status)
	// net/http does not discard bodies for a custom HEAD handler, so suppress it
	// explicitly while retaining the accurate Content-Length header.
	if body && status != http.StatusNoContent {
		_, _ = w.Write(data)
	}
}

func (h *Handler) writeError(w http.ResponseWriter, body bool, status int, message string) {
	data := []byte(message + "\n")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(status)
	if body {
		_, _ = w.Write(data)
	}
}

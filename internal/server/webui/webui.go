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
	"sort"
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
	Catalog   Catalog
	Frames    []FrameBundle

	// HomeViewID selects an admitted saved view. Its server-owned definition
	// must supply a row budget no larger than MaxRows. Without an ID, view.run
	// uses a bounded inline definition and cannot open a saved-view watch.
	HomeViewID string
	// WatchPath is optional and must name the existing same-origin NDJSON
	// adapter. The host still owns view resolution, read policy and the trusted
	// collaboration-session bridge; this handler supplies none of them.
	WatchPath string

	// BlockedViewID and BlockedViewDefinition are optional. They are used only
	// when the server advertises view.run/view.explain. The server remains the
	// authority for validating the definition and its authorization.
	BlockedViewID         string
	BlockedViewDefinition json.RawMessage
}

type browserConfig struct {
	RPCPath               string            `json:"rpcPath"`
	Project               string            `json:"project"`
	MaxRows               int               `json:"maxRows"`
	HomeViewID            string            `json:"homeViewId,omitempty"`
	WatchPath             string            `json:"watchPath,omitempty"`
	BlockedViewID         string            `json:"blockedViewId,omitempty"`
	BlockedViewDefinition json.RawMessage   `json:"blockedViewDefinition,omitempty"`
	Catalog               Catalog           `json:"catalog"`
	Frames                []frameDescriptor `json:"frames,omitempty"`
}

type frameDescriptor struct {
	ID      string   `json:"id"`
	Title   string   `json:"title"`
	Digest  string   `json:"digest"`
	Src     string   `json:"src"`
	Methods []string `json:"methods"`
	Project string   `json:"project"`
}

// Handler serves only files below Config.MountPath. It is safe for concurrent
// requests and has no mutable request-specific state.
type Handler struct {
	cfg    Config
	config []byte
	assets fs.FS
	admit  *authn.Authenticator
	frames map[string]frameBundle
}

//go:embed static/index.html static/app.js static/styles.css static/favicon.svg
var embedded embed.FS

const contentSecurityPolicy = "default-src 'self'; base-uri 'none'; object-src 'none'; frame-src 'self'; frame-ancestors 'none'; form-action 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; font-src 'self'"

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
	if err := validText(config.HomeViewID, "home view id", 200); err != nil {
		return nil, err
	}
	if config.WatchPath != "" {
		if config.WatchPath, err = rpcPath(config.WatchPath); err != nil {
			return nil, fmt.Errorf("webui: watch path: %w", err)
		}
		if config.HomeViewID == "" {
			return nil, fmt.Errorf("webui: watch path requires a saved home view")
		}
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
	if config.Catalog.Panels == nil && config.Catalog.Columns == nil && config.Catalog.Buttons == nil {
		config.Catalog = DefaultCatalog()
	}
	if err := config.Catalog.Validate(); err != nil {
		return nil, err
	}
	bundles := append([]FrameBundle(nil), config.Frames...)
	sort.SliceStable(bundles, func(i, j int) bool { return bundles[i].ID < bundles[j].ID })
	frames := make(map[string]frameBundle, len(bundles))
	descriptors := make([]frameDescriptor, 0, len(bundles))
	for _, bundle := range bundles {
		if _, exists := frames[bundle.ID]; exists {
			return nil, fmt.Errorf("webui: duplicate frame %q", bundle.ID)
		}
		prepared, err := prepareFrameBundle(bundle, config.Project)
		if err != nil {
			return nil, err
		}
		frames[bundle.ID] = prepared
		grant := prepared.bridge.Grant()
		descriptors = append(descriptors, frameDescriptor{
			ID: bundle.ID, Title: prepared.Title, Digest: prepared.Digest,
			Src:     config.MountPath + "frames/" + bundle.ID + "/index.html",
			Methods: grant.Methods, Project: grant.Project,
		})
	}

	encoded, err := json.Marshal(browserConfig{
		RPCPath:               config.RPCPath,
		Project:               config.Project,
		MaxRows:               config.MaxRows,
		HomeViewID:            config.HomeViewID,
		WatchPath:             config.WatchPath,
		BlockedViewID:         config.BlockedViewID,
		BlockedViewDefinition: config.BlockedViewDefinition,
		Catalog:               config.Catalog.clone(),
		Frames:                descriptors,
	})
	if err != nil {
		return nil, fmt.Errorf("webui: encode browser config: %w", err)
	}
	admit, err := authn.New(authn.Config{})
	if err != nil {
		return nil, fmt.Errorf("webui: anonymous admission: %w", err)
	}
	config.Catalog = config.Catalog.clone()
	config.Frames = bundles
	return &Handler{cfg: config, config: encoded, assets: embedded, admit: admit, frames: frames}, nil
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
	// Sandboxed frames have an opaque origin and therefore send `Origin: null`.
	// Their assets are still loopback-only, have no credentials, and carry a
	// response CSP that prevents embedding outside this host. Do not weaken
	// normal page/RPC origin admission for any other route.
	frameRequest := r.URL != nil && strings.HasPrefix(r.URL.Path, h.cfg.MountPath+"frames/")
	if frameRequest && len(r.Header.Values("Origin")) == 1 && r.Header.Get("Origin") == "null" {
		probe.Header.Del("Origin")
	}
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
	if strings.HasPrefix(asset, "frames/") {
		h.serveFrame(w, r, strings.TrimPrefix(asset, "frames/"))
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

func (h *Handler) serveFrame(w http.ResponseWriter, r *http.Request, asset string) {
	parts := strings.SplitN(asset, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		h.writeError(w, r.Method == http.MethodGet, http.StatusNotFound, "UI frame asset not found")
		return
	}
	bundle, ok := h.frames[parts[0]]
	if !ok {
		h.writeError(w, r.Method == http.MethodGet, http.StatusNotFound, "UI frame asset not found")
		return
	}
	data, ok := bundle.Assets[parts[1]]
	if !ok {
		h.writeError(w, r.Method == http.MethodGet, http.StatusNotFound, "UI frame asset not found")
		return
	}
	contentType := frameContentType(parts[1])
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	// Host and actual listener/peer posture were checked before serving any
	// asset. The bundle path consists only of validated configuration values.
	bundleURL := scheme + "://" + r.Host + h.cfg.MountPath + "frames/" + bundle.ID + "/"
	w.Header().Set("Content-Security-Policy", frameContentSecurityPolicy(bundleURL))
	// The frame has an opaque origin, so same-origin CORP would block its own
	// external scripts. These are public, digest-checked static assets with no
	// credentials; all host assets retain their same-origin policy. Modules
	// additionally require CORS for the opaque origin, never credentials.
	w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
	w.Header().Add("Vary", "Origin")
	if r.Header.Get("Origin") == "null" {
		w.Header().Set("Access-Control-Allow-Origin", "null")
	}
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
	w.Header().Set("Content-Type", contentType)
	h.writeBytes(w, r.Method == http.MethodGet, http.StatusOK, contentType, data)
}

func frameContentType(name string) string {
	switch path.Ext(name) {
	case ".html":
		return "text/html; charset=utf-8"
	case ".js":
		return "text/javascript; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".svg":
		return "image/svg+xml"
	case ".json":
		return "application/json; charset=utf-8"
	default:
		return "application/octet-stream"
	}
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

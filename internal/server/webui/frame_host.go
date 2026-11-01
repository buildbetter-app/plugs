package webui

import (
	"fmt"
	"html"
	"net/http"
	"strings"
)

// Each trusted wrapper owns exactly one opaque plugin browsing context. Its
// frame-src policy applies to every navigation of that child, so a different
// document cannot inherit the child's stable WindowProxy and bridge grant.
// The plugin's pinned files are served unchanged by serveFrame.
func (h *Handler) serveFrameHost(w http.ResponseWriter, r *http.Request, asset string) {
	parts := strings.Split(asset, "/")
	if len(parts) != 2 || parts[1] != "index.html" {
		h.writeError(w, r.Method == http.MethodGet, http.StatusNotFound, "UI frame host not found")
		return
	}
	bundle, ok := h.frames[parts[0]]
	if !ok {
		h.writeError(w, r.Method == http.MethodGet, http.StatusNotFound, "UI frame host not found")
		return
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	base := scheme + "://" + r.Host + h.cfg.MountPath
	entry := base + "frames/" + bundle.ID + "/index.html"
	script := base + "frame-host.js"
	w.Header().Set("Content-Security-Policy", fmt.Sprintf("default-src 'none'; base-uri 'none'; object-src 'none'; frame-src %s; frame-ancestors 'self'; form-action 'none'; script-src %s; connect-src 'none'", entry, script))
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
	body := fmt.Sprintf(`<!doctype html><html><head><meta charset="utf-8"><title>Plugin frame host</title></head><body data-entry="%s"><script src="%s"></script></body></html>`, html.EscapeString(entry), html.EscapeString(script))
	h.writeBytes(w, r.Method == http.MethodGet, http.StatusOK, "text/html; charset=utf-8", []byte(body))
}

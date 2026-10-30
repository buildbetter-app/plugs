package authn

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

// Listen creates a listener without DNS resolution. Anonymous listeners always
// bind a literal loopback address (localhost is pinned to 127.0.0.1), with no
// override. Network listeners require a provider and directly configured TLS.
// Trusted-proxy termination is deliberately unsupported in this slice.
func (a *Authenticator) Listen(ctx context.Context, address string, tlsConfig *tls.Config) (net.Listener, error) {
	address, loopback, err := listenAddress(address)
	if err != nil || (!loopback && (!a.configured() || tlsConfig == nil)) {
		return nil, ErrForbidden
	}
	if !a.configured() && a.hasAuthorization() {
		return nil, ErrAuthzRequiresAuthn
	}
	if tlsConfig != nil {
		if len(tlsConfig.Certificates) == 0 && tlsConfig.GetCertificate == nil {
			return nil, ErrForbidden
		}
		tlsConfig = tlsConfig.Clone()
		if tlsConfig.MinVersion < tls.VersionTLS12 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
	}
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		return tls.NewListener(listener, tlsConfig), nil
	}
	return listener, nil
}

func listenAddress(address string) (string, bool, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || !validPort(port, true) {
		return "", false, ErrForbidden
	}
	if strings.EqualFold(host, "localhost") {
		host = "127.0.0.1"
	}
	if host == "" {
		return net.JoinHostPort(host, port), false, nil
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" {
		return "", false, ErrForbidden
	}
	return net.JoinHostPort(ip.String(), port), ip.IsLoopback(), nil
}

// AdmitHTTP is for RPC HTTP requests, including /mcp. It requires Plug-RPC: 1
// even when no Origin header is present. Browser origins, when present, must
// match this request's authority and actual TLS scheme, or be admitted by the
// configured core-session cross-origin authorizer.
func (a *Authenticator) AdmitHTTP(r *http.Request) (Identity, error) {
	if r == nil || !oneHeader(r.Header, "Plug-RPC", "1") {
		return Identity{}, ErrForbidden
	}
	return a.admitRequest(r)
}

// AdmitHTTPTransport applies the network and HTTP-marker admission contract
// without requiring a bearer credential. Public login/callback handlers use
// it before processing an unauthenticated request; they must apply their own
// credential and origin policy before issuing anything. It grants no identity
// or cross-origin bearer authority. Browser preflights must request the marker
// on the subsequent GET/POST, since the preflight itself cannot carry it.
// RPC transports must use AdmitHTTP, which always requires the marker itself.
func (a *Authenticator) AdmitHTTPTransport(r *http.Request) error {
	if a == nil {
		return ErrUnavailable
	}
	if r == nil {
		return ErrForbidden
	}
	if r.Method == http.MethodOptions {
		if !requestsHTTPMarker(r.Header) {
			return ErrForbidden
		}
	} else if !oneHeader(r.Header, "Plug-RPC", "1") {
		return ErrForbidden
	}
	return a.checkHTTPPosture(r)
}

func requestsHTTPMarker(header http.Header) bool {
	methods := header.Values("Access-Control-Request-Method")
	if len(methods) != 1 || (methods[0] != http.MethodGet && methods[0] != http.MethodPost) {
		return false
	}
	for _, line := range header.Values("Access-Control-Request-Headers") {
		for _, name := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(name), "Plug-RPC") {
				return true
			}
		}
	}
	return false
}

// AdmitWebSocket applies the same Host/Origin rules at the WebSocket endpoint.
// Browsers cannot send Plug-RPC on a handshake. The HTTP RPC endpoint must use
// AdmitHTTP instead; merely adding Upgrade headers never exempts HTTP requests.
func (a *Authenticator) AdmitWebSocket(r *http.Request) (Identity, error) {
	if r == nil || r.Method != http.MethodGet ||
		!oneHeaderFold(r.Header, "Upgrade", "websocket") || !connectionUpgrade(r.Header) ||
		!oneHeader(r.Header, "Sec-WebSocket-Version", "13") {
		return Identity{}, ErrForbidden
	}
	keys := r.Header.Values("Sec-WebSocket-Key")
	if len(keys) != 1 {
		return Identity{}, ErrForbidden
	}
	key, err := base64.StdEncoding.DecodeString(keys[0])
	if err != nil || len(key) != 16 {
		return Identity{}, ErrForbidden
	}
	return a.admitRequest(r)
}

func (a *Authenticator) admitRequest(r *http.Request) (Identity, error) {
	if err := a.checkHTTPPosture(r); err != nil {
		return Identity{}, err
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	origins := r.Header.Values("Origin")
	token, err := bearerToken(r.Header)
	if err != nil {
		return Identity{}, err
	}
	if len(origins) > 1 {
		return Identity{}, ErrForbidden
	}
	if len(origins) == 1 && !sameOrigin(origins[0], scheme, r.Host) {
		if err := a.allowCrossOrigin(r.Context(), origins[0], token); err != nil {
			return Identity{}, err
		}
	}
	return a.Admit(r.Context(), token)
}

func (a *Authenticator) checkHTTPPosture(r *http.Request) error {
	host, _, err := authority(r.Host)
	if err != nil {
		return ErrForbidden
	}
	peer, err := peerIP(r.RemoteAddr)
	if err != nil {
		return ErrForbidden
	}
	loopback := !a.configured() || isLoopbackHost(host)
	if local, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok {
		localIP, err := peerIP(local.String())
		if err != nil || (!a.configured() && !localIP.IsLoopback()) {
			return ErrForbidden
		}
		loopback = loopback || localIP.IsLoopback()
	}
	if loopback && (!isLoopbackHost(host) || !peer.IsLoopback()) {
		return ErrForbidden
	}
	if !loopback && r.TLS == nil {
		return ErrForbidden
	}
	return nil
}

func (a *Authenticator) allowCrossOrigin(ctx context.Context, origin, token string) error {
	if !strings.HasPrefix(token, CoreSessionPrefix) || a.cross == nil {
		return ErrForbidden
	}
	if err := a.cross(ctx, origin, token); err != nil {
		switch {
		case errors.Is(err, ErrTokenExpired):
			return ErrTokenExpired
		case errors.Is(err, ErrUnavailable):
			return ErrUnavailable
		default:
			return ErrForbidden
		}
	}
	return nil
}

func bearerToken(h http.Header) (string, error) {
	values := h.Values("Authorization")
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 {
		return "", ErrUnauthenticated
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || len(token) > 4096 ||
		strings.ContainsAny(token, " \t\r\n,") {
		return "", ErrUnauthenticated
	}
	return token, nil
}

func sameOrigin(raw, scheme, requestHost string) bool {
	host, port, err := authority(requestHost)
	if err != nil {
		return false
	}
	if strings.ContainsAny(raw, "?#") {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != scheme || u.User != nil || u.Path != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return false
	}
	originHost, originPort, err := authority(u.Host)
	return err == nil && originHost == host && effectivePort(originPort, scheme) == effectivePort(port, scheme)
}

// authority accepts an ordinary DNS name or bracketed IP authority, without
// looking it up. Loopback admission separately requires localhost or literal IP.
func authority(raw string) (string, string, error) {
	if raw == "" || strings.ContainsAny(raw, "/\\?#@% \t\r\n") {
		return "", "", ErrForbidden
	}
	u, err := url.Parse("http://" + raw)
	if err != nil || u.Host != raw || u.User != nil {
		return "", "", ErrForbidden
	}
	host, port := strings.ToLower(u.Hostname()), u.Port()
	if host == "" || (port != "" && !validPort(port, false)) || strings.HasSuffix(raw, ":") {
		return "", "", ErrForbidden
	}
	if strings.HasPrefix(raw, "[") {
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is6() {
			return "", "", ErrForbidden
		}
		host = ip.String()
	} else if strings.ContainsAny(raw, "[]") || strings.Contains(host, ":") {
		return "", "", ErrForbidden
	}
	return host, port, nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.Zone() == "" && ip.IsLoopback()
}

func peerIP(address string) (netip.Addr, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || !validPort(port, false) {
		return netip.Addr{}, ErrForbidden
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" {
		return netip.Addr{}, ErrForbidden
	}
	return ip, nil
}

func validPort(port string, allowZero bool) bool {
	if port == "" {
		return false
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	n, err := strconv.Atoi(port)
	return err == nil && n <= 65535 && (n > 0 || allowZero)
}

func effectivePort(port, scheme string) string {
	if port != "" {
		n, _ := strconv.Atoi(port)
		return strconv.Itoa(n)
	}
	if scheme == "https" {
		return "443"
	}
	return "80"
}

func oneHeader(h http.Header, name, value string) bool {
	values := h.Values(name)
	return len(values) == 1 && values[0] == value
}

func oneHeaderFold(h http.Header, name, value string) bool {
	values := h.Values(name)
	return len(values) == 1 && strings.EqualFold(values[0], value)
}

func connectionUpgrade(h http.Header) bool {
	for _, line := range h.Values("Connection") {
		for _, token := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

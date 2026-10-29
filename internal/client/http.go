package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

// HTTPConfig selects a server origin (or its exact /rpc/v1 endpoint) and an
// optional bearer credential. Zero bounds select 40 seconds and 8 MiB. HTTP is
// permitted only for literal loopback addresses or localhost; HTTPS verifies
// the server using the system trust roots. Credentials never belong in URLs.
type HTTPConfig struct {
	Server           string
	Token            string
	Timeout          time.Duration
	MaxResponseBytes int64
}

// HTTPTransport carries the same bytes as the in-process transport. It has no
// cookies, redirects or application retries. Close releases idle connections;
// callers cancel active work through the RoundTrip context.
type HTTPTransport struct {
	endpoint string
	token    string
	limit    int64
	client   *http.Client
	catalog  *wire.Catalog
}

func NewHTTP(config HTTPConfig) (*HTTPTransport, error) {
	endpoint, err := rpcEndpoint(config.Server)
	if err != nil {
		return nil, err
	}
	if len(config.Token) > 4096 || strings.IndexFunc(config.Token, func(r rune) bool {
		// RFC 6750 b64token; this also excludes header injection and whitespace.
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("-._~+/=", r))
	}) >= 0 {
		return nil, fmt.Errorf("PLUG_TOKEN must be a valid bearer token of at most 4096 bytes")
	}
	if config.Timeout < 0 || config.MaxResponseBytes < 0 || config.MaxResponseBytes == math.MaxInt64 {
		return nil, fmt.Errorf("HTTP timeout and response limit must be positive")
	}
	if config.Timeout == 0 {
		config.Timeout = 40 * time.Second
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = 8 << 20
	}
	catalog, err := wire.DefaultCatalog()
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy:                  http.ProxyFromEnvironment,
		DialContext:            (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           10,
		MaxIdleConnsPerHost:    2,
		IdleConnTimeout:        30 * time.Second,
		TLSHandshakeTimeout:    10 * time.Second,
		ResponseHeaderTimeout:  config.Timeout,
		MaxResponseHeaderBytes: 32 << 10,
		DisableCompression:     true,
	}
	return &HTTPTransport{
		endpoint: endpoint, token: config.Token, limit: config.MaxResponseBytes, catalog: catalog,
		client: &http.Client{
			Transport: transport, Timeout: config.Timeout,
			// Refuse even same-origin redirects: POST rewriting and replay of a
			// possibly committed write are both outside this transport's contract.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func rpcEndpoint(raw string) (string, error) {
	invalid := fmt.Errorf("server must be an HTTP(S) origin or exact /rpc/v1 URL without credentials, query or fragment")
	if raw == "" || raw != strings.TrimSpace(raw) || strings.ContainsAny(raw, "?#\\") {
		return "", invalid
	}
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || u.User != nil || u.Host == "" || u.RawPath != "" ||
		(u.Scheme != "http" && u.Scheme != "https") ||
		(u.Path != "" && u.Path != "/" && u.Path != "/rpc/v1") {
		return "", invalid
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || strings.ContainsAny(u.Host, "% \t\r\n") || strings.HasSuffix(u.Host, ":") {
		return "", invalid
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", invalid
		}
	}
	ip, ipErr := netip.ParseAddr(host)
	if strings.ContainsAny(u.Host, "[]") && (ipErr != nil || !ip.Is6()) {
		return "", invalid
	}
	if u.Scheme == "http" && host != "localhost" && (ipErr != nil || !ip.IsLoopback()) {
		return "", fmt.Errorf("non-loopback servers require HTTPS")
	}
	u.Path = "/rpc/v1"
	return u.String(), nil
}

func (h *HTTPTransport) Close() { h.client.CloseIdleConnections() }

func (h *HTTPTransport) RoundTrip(ctx context.Context, data []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("unable to construct remote HTTP request")
	}
	// A non-rewindable POST prevents net/http from automatically replaying a
	// body after connection loss. The JSON-RPC key is not an HTTP retry header.
	r.GetBody = nil
	r.Header.Set("Plug-RPC", "1")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json")
	if h.token != "" {
		r.Header.Set("Authorization", "Bearer "+h.token)
	}
	response, err := h.client.Do(r)
	if err != nil {
		return nil, safeHTTPFailure(ctx, err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		return nil, fmt.Errorf("remote HTTP server returned status %d; redirects are disabled", response.StatusCode)
	}
	if response.ContentLength > h.limit {
		return nil, fmt.Errorf("remote HTTP response exceeds the configured byte limit")
	}
	if len(response.Header.Values("Content-Encoding")) != 0 {
		return nil, fmt.Errorf("remote HTTP response content encoding is unsupported")
	}
	if response.StatusCode != http.StatusNoContent && !jsonMedia(response.Header.Values("Content-Type")) {
		return nil, fmt.Errorf("remote HTTP server returned status %d with unsupported media type", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, h.limit+1))
	if err != nil {
		return nil, safeHTTPFailure(ctx, err)
	}
	if int64(len(body)) > h.limit {
		return nil, fmt.Errorf("remote HTTP response exceeds the configured byte limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if h.reflectsCredential(body) {
		return nil, fmt.Errorf("remote HTTP error contained a credential")
	}
	if response.StatusCode == http.StatusNoContent {
		return nil, nil // Notification framing; Client.Call still requires a reply.
	}
	if response.StatusCode != http.StatusOK {
		if remote := h.admissionError(body); remote != nil {
			return nil, remote
		}
		return nil, fmt.Errorf("remote HTTP server returned status %d", response.StatusCode)
	}
	return body, nil
}

func safeHTTPFailure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var timeout net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout() {
		return fmt.Errorf("remote HTTP request timed out: %w", context.DeadlineExceeded)
	}
	// url.Error, response headers and arbitrary peer bodies may contain
	// credentials. Never include their text or unwrap them into caller errors.
	return fmt.Errorf("remote HTTP request failed")
}

func jsonMedia(values []string) bool {
	if len(values) != 1 {
		return false
	}
	media, params, err := mime.ParseMediaType(values[0])
	if err != nil || media != "application/json" {
		return false
	}
	for key, value := range params {
		if key != "charset" || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	return true
}

func (h *HTTPTransport) reflectsCredential(body []byte) bool {
	if h.token == "" {
		return false
	}
	var envelope struct{ Error json.RawMessage }
	if json.Unmarshal(body, &envelope) != nil || len(envelope.Error) == 0 {
		return false
	}
	// Decode escapes before inspecting only the error, never a successful
	// result. A peer reflecting Authorization must not turn it into CLI output.
	value, err := wire.DecodeJSON(envelope.Error)
	if err != nil {
		return false
	}
	plain, _ := json.Marshal(value)
	return bytes.Contains(plain, []byte(h.token))
}

func (h *HTTPTransport) admissionError(body []byte) *wire.Error {
	value, err := wire.DecodeJSON(body)
	if err != nil {
		return nil
	}
	envelope, ok := value.(map[string]any)
	_, hasID := envelope["id"]
	if !ok || len(envelope) != 3 || !hasID || envelope["jsonrpc"] != "2.0" || envelope["id"] != nil || envelope["error"] == nil {
		return nil
	}
	raw, _ := json.Marshal(envelope["error"])
	remote, err := decodeRemoteError(h.catalog, raw)
	if err != nil {
		return nil
	}
	return remote
}

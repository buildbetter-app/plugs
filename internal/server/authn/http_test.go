package authn

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func localRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://localhost:8888/rpc", nil)
	r.RemoteAddr = "127.0.0.1:41000"
	r.Header.Set("Plug-RPC", "1")
	return r
}

func TestAnonymousHTTPAdmission(t *testing.T) {
	a := newAuthenticator(t, Config{})
	for _, test := range []struct {
		name   string
		change func(*http.Request)
		want   error
	}{
		{"CLI without origin", func(*http.Request) {}, nil},
		{"same origin", func(r *http.Request) { r.Header.Set("Origin", "http://localhost:8888") }, nil},
		{"case insensitive host", func(r *http.Request) { r.Host = "LOCALHOST:8888"; r.Header.Set("Origin", "http://localhost:8888") }, nil},
		{"IPv4 loopback", func(r *http.Request) { r.Host = "127.0.0.1:8888"; r.Header.Set("Origin", "http://127.0.0.1:8888") }, nil},
		{"IPv6 loopback", func(r *http.Request) {
			r.Host = "[::1]:8888"
			r.RemoteAddr = "[::1]:41000"
			r.Header.Set("Origin", "http://[::1]:8888")
		}, nil},
		{"default port", func(r *http.Request) { r.Host = "localhost"; r.Header.Set("Origin", "http://localhost:80") }, nil},
		{"missing marker", func(r *http.Request) { r.Header.Del("Plug-RPC") }, ErrForbidden},
		{"wrong marker", func(r *http.Request) { r.Header.Set("Plug-RPC", "true") }, ErrForbidden},
		{"duplicate marker", func(r *http.Request) { r.Header.Add("Plug-RPC", "1") }, ErrForbidden},
		{"rebound host", func(r *http.Request) { r.Host = "example.invalid:8888" }, ErrForbidden},
		{"host suffix", func(r *http.Request) { r.Host = "localhost.example.invalid:8888" }, ErrForbidden},
		{"trailing dot", func(r *http.Request) { r.Host = "localhost.:8888" }, ErrForbidden},
		{"shorthand IPv4", func(r *http.Request) { r.Host = "127.1:8888" }, ErrForbidden},
		{"missing host", func(r *http.Request) { r.Host = "" }, ErrForbidden},
		{"bad port", func(r *http.Request) { r.Host = "localhost:65536" }, ErrForbidden},
		{"userinfo", func(r *http.Request) { r.Host = "bad@localhost:8888" }, ErrForbidden},
		{"foreign origin", func(r *http.Request) { r.Header.Set("Origin", "http://example.invalid") }, ErrForbidden},
		{"loopback alias origin", func(r *http.Request) { r.Header.Set("Origin", "http://127.0.0.1:8888") }, ErrForbidden},
		{"different port", func(r *http.Request) { r.Header.Set("Origin", "http://localhost:8889") }, ErrForbidden},
		{"different scheme", func(r *http.Request) { r.Header.Set("Origin", "https://localhost:8888") }, ErrForbidden},
		{"opaque origin", func(r *http.Request) { r.Header.Set("Origin", "null") }, ErrForbidden},
		{"empty origin", func(r *http.Request) { r.Header.Set("Origin", "") }, ErrForbidden},
		{"origin path", func(r *http.Request) { r.Header.Set("Origin", "http://localhost:8888/path") }, ErrForbidden},
		{"origin query", func(r *http.Request) { r.Header.Set("Origin", "http://localhost:8888?") }, ErrForbidden},
		{"origin fragment", func(r *http.Request) { r.Header.Set("Origin", "http://localhost:8888#") }, ErrForbidden},
		{"duplicate origin", func(r *http.Request) {
			r.Header.Add("Origin", "http://localhost:8888")
			r.Header.Add("Origin", "http://localhost:8888")
		}, ErrForbidden},
		{"nonloopback peer", func(r *http.Request) { r.RemoteAddr = "192.0.2.2:41000" }, ErrForbidden},
		{"forwarded peer ignored", func(r *http.Request) {
			r.RemoteAddr = "192.0.2.2:41000"
			r.Header.Set("X-Forwarded-For", "127.0.0.1")
			r.Header.Set("X-Forwarded-Host", "localhost:8888")
		}, ErrForbidden},
		{"host peer is not resolved", func(r *http.Request) { r.RemoteAddr = "localhost:41000" }, ErrForbidden},
		{"nonloopback listener", func(r *http.Request) {
			*r = *r.WithContext(context.WithValue(r.Context(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.IPv4zero, Port: 8888}))
		}, ErrForbidden},
		{"empty credential", func(r *http.Request) { r.Header.Set("Authorization", "") }, ErrUnauthenticated},
		{"bad scheme", func(r *http.Request) { r.Header.Set("Authorization", "Basic unsupported") }, ErrUnauthenticated},
		{"empty bearer", func(r *http.Request) { r.Header.Set("Authorization", "Bearer ") }, ErrUnauthenticated},
		{"supplied bearer without provider", func(r *http.Request) { r.Header.Set("Authorization", "Bearer unsupported") }, ErrUnauthenticated},
		{"multiple credentials", func(r *http.Request) {
			r.Header.Add("Authorization", "Bearer unsupported")
			r.Header.Add("Authorization", "Bearer unsupported")
		}, ErrUnauthenticated},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := localRequest()
			test.change(r)
			i, err := a.AdmitHTTP(r)
			if test.want != nil {
				assertDenied(t, i, err, test.want)
			} else if err != nil || i.Principal().ID != "anonymous" {
				t.Fatalf("local request was refused: %v", err)
			}
		})
	}
}

func websocketRequest() *http.Request {
	r := localRequest()
	r.Method = http.MethodGet
	r.Header.Del("Plug-RPC")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Connection", "keep-alive, Upgrade")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(make([]byte, 16)))
	r.Header.Set("Origin", "http://localhost:8888")
	return r
}

func TestWebSocketAdmissionChecksOriginAndKeepsHTTPMarker(t *testing.T) {
	a := newAuthenticator(t, Config{})
	r := websocketRequest()
	i, err := a.AdmitWebSocket(r)
	if err != nil || i.Principal().ID != "anonymous" {
		t.Fatal("valid loopback WebSocket handshake was refused")
	}
	i, err = a.AdmitHTTP(r)
	assertDenied(t, i, err, ErrForbidden)
	r.Header.Set("Origin", "http://example.invalid")
	i, err = a.AdmitWebSocket(r)
	assertDenied(t, i, err, ErrForbidden)
	r = websocketRequest()
	r.Host = "example.invalid:8888"
	i, err = a.AdmitWebSocket(r)
	assertDenied(t, i, err, ErrForbidden)
	r = websocketRequest()
	r.Header.Del("Connection")
	i, err = a.AdmitWebSocket(r)
	assertDenied(t, i, err, ErrForbidden)
}

func TestListenerPostureWithoutExternalConnections(t *testing.T) {
	a := newAuthenticator(t, Config{})
	for _, address := range []string{":0", "0.0.0.0:0", "[::]:0", "192.0.2.1:0", "example.invalid:0", "localhost", "localhost:65536"} {
		listener, err := a.Listen(context.Background(), address, nil)
		if listener != nil {
			listener.Close()
			t.Fatal("nonloopback anonymous listener was created")
		}
		if !errors.Is(err, ErrForbidden) {
			t.Fatal("nonloopback/invalid listener did not fail at admission")
		}
	}
	// TLS never overrides anonymous loopback binding. No socket is opened here.
	listener, err := a.Listen(context.Background(), "0.0.0.0:0", &tls.Config{})
	if listener != nil || !errors.Is(err, ErrForbidden) {
		t.Fatal("TLS became an anonymous network override")
	}
	listener, err = a.Listen(context.Background(), "localhost:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().(*net.TCPAddr)
	if !addr.IP.IsLoopback() || addr.Port == 0 {
		t.Fatal("localhost did not bind an actual loopback socket")
	}
}

func TestListenAddressIsLiteralAndIPv6Aware(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "127.0.0.2:8080", "[::1]:0", "localhost:80"} {
		_, loopback, err := listenAddress(address)
		if err != nil || !loopback {
			t.Fatal("literal loopback address refused")
		}
	}
	for _, address := range []string{"[::1%lo]:0", "localhost.:0", "127.1:0", "localhost:+80"} {
		if _, _, err := listenAddress(address); err == nil {
			t.Fatal("nonliteral or malformed address accepted")
		}
	}
}

package pluginhost

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const maxEgressBytes = 10 << 20

type egressTransport struct{ instance *instance }

func (t *egressTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	call, err := t.instance.current()
	if err != nil {
		return nil, err
	}
	if call.hot() {
		return nil, failure(ErrHotEgress, "HTTP is unavailable during a synchronous pre call")
	}
	if !t.instance.owner.authenticationConfigured {
		return nil, failure(ErrEgress, "HTTP is unavailable in anonymous mode")
	}
	if request.URL == nil || request.URL.Scheme != "https" || request.URL.User != nil {
		return nil, failure(ErrEgress, "only credential-free HTTPS URLs are supported")
	}
	host := strings.ToLower(request.URL.Hostname())
	port := request.URL.Port()
	if port == "" {
		port = "443"
	}
	endpoint := net.JoinHostPort(host, port)
	if !contains(t.instance.manifest.Requires.Egress, endpoint) {
		return nil, failure(ErrEgress, "host is not declared")
	}
	if request.ContentLength > maxEgressBytes {
		return nil, failure(ErrEgress, "request exceeds byte limit")
	}
	// DNS is resolved by the transport and pinned for this request. Every IP
	// must be public; redirects reenter RoundTrip and repeat all checks.
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: time.Second, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != endpoint {
			return nil, failure(ErrEgress, "unexpected dial target")
		}
		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(addresses) == 0 {
			return nil, failure(ErrEgress, "empty DNS answer")
		}
		for _, address := range addresses {
			if !publicIP(address.IP) {
				return nil, failure(ErrEgress, "private or non-public target")
			}
		}
		dialer := net.Dialer{Timeout: time.Second}
		var last error
		for _, address := range addresses {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(address.IP.String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
		}
		return nil, last
	}}
	copy := request.Clone(call.ctx)
	if request.Body != nil {
		copy.Body = &cappedBody{source: request.Body, remaining: maxEgressBytes}
	}
	response, err := transport.RoundTrip(copy)
	if err != nil {
		return nil, err
	}
	if response.ContentLength > maxEgressBytes {
		response.Body.Close()
		return nil, failure(ErrEgress, "response exceeds byte limit")
	}
	response.Body = &cappedBody{source: response.Body, remaining: maxEgressBytes}
	return response, nil
}

func publicIP(ip net.IP) bool {
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsMulticast() && !ip.IsUnspecified()
}

type cappedBody struct {
	source    io.ReadCloser
	remaining int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.remaining < 0 {
		return 0, fmt.Errorf("plugin egress byte limit exceeded")
	}
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.source.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		return 0, fmt.Errorf("plugin egress byte limit exceeded")
	}
	return n, err
}
func (b *cappedBody) Close() error { return b.source.Close() }

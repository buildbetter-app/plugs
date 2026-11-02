package httpjson

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
)

// Server owns the listener and request contexts, never the RPC service's store
// or lease. Call Serve, then Shutdown or Close and wait for Serve to return
// before closing application resources. Serve uses net/http's ErrServerClosed.
type Server struct {
	server   *http.Server
	listener net.Listener
	cancel   context.CancelFunc
}

// Listen binds only the explicit address via the authenticator's unchanged
// posture checks. Anonymous mode has no non-loopback override. Network TLS is
// terminated here; forwarded headers and trusted proxies confer no authority.
// The caller starts Serve and owns shutdown. ctx is the request lifetime root;
// canceling it cancels handlers, while Shutdown/Close also release sockets.
func (h *Handler) Listen(ctx context.Context, address string, tlsConfig *tls.Config) (*Server, error) {
	return h.ListenHandler(ctx, address, tlsConfig, h)
}

// ListenHandler is the shared-server composition seam. It keeps the admitted
// JSON-RPC handler as the authority for /rpc/v1 while allowing a composition
// root to mount sibling, already-admitted handlers such as the embedded UI or
// a watch stream on the same listener. The supplied root must route requests
// deliberately; this method never widens HTTP admission.
func (h *Handler) ListenHandler(ctx context.Context, address string, tlsConfig *tls.Config, root http.Handler) (*Server, error) {
	if root == nil {
		return nil, errors.New("httpjson: root handler is required")
	}
	listener, err := h.authn.Listen(ctx, address, tlsConfig)
	if err != nil {
		return nil, err
	}
	base, cancel := context.WithCancel(ctx)
	server := &http.Server{
		Handler:           root,
		ReadHeaderTimeout: h.cfg.ReadHeaderTimeout,
		ReadTimeout:       h.cfg.ReadTimeout,
		WriteTimeout:      h.cfg.WriteTimeout,
		IdleTimeout:       h.cfg.IdleTimeout,
		MaxHeaderBytes:    h.cfg.MaxHeaderBytes,
		BaseContext:       func(net.Listener) context.Context { return base },
		// The transport does not log request bodies or credentials. The
		// composition root may install safe operational metrics separately.
		ErrorLog: log.New(io.Discard, "", 0),
	}
	return &Server{server: server, listener: listener, cancel: cancel}, nil
}

func (s *Server) Addr() net.Addr { return s.listener.Addr() }
func (s *Server) Serve() error   { return s.server.Serve(s.listener) }

// Shutdown drains active handlers. If its budget expires it cancels active
// work and closes connections; the returned error preserves the missed drain.
// Also closes a listener that has not yet entered Serve.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	if err != nil {
		_ = s.Close()
		return err
	}
	s.cancel()
	return closeListener(s.listener)
}

func (s *Server) Close() error {
	// Close active HTTP connections before canceling request roots. Otherwise a
	// cooperative handler can observe cancellation, publish an RPC response,
	// and win the race with the forced connection close.
	err := errors.Join(s.server.Close(), closeListener(s.listener))
	s.cancel()
	return err
}

func closeListener(listener net.Listener) error {
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

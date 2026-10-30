// Package composition owns the Plug server lifetime. It is the only package
// that opens a store, acquires its active lease, and wires the core service to
// transports. Clients and tests may borrow the returned components, but they
// do not own any of the underlying resources.
package composition

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	servertsevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/server/webui"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const (
	// DefaultAddress is deliberately loopback-only and ephemeral. Operators
	// that need a stable endpoint put an explicit address in config.yaml.
	DefaultAddress = "127.0.0.1:0"
	DefaultProject = "default"
)

// StoreFactory is a test seam for a real driver.Store. Production uses the
// driver selected by Driver and never substitutes an in-memory store for a
// persistent configuration.
type StoreFactory func(context.Context, string, driver.Config) (driver.Store, error)

// Config describes one server composition. Driver and DSN are intentionally
// separate so an invalid combination can be refused before any listener is
// opened. UI is enabled for shared servers and disabled by in-process callers.
type Config struct {
	Driver       string
	DSN          string
	Project      string
	Address      string
	UI           bool
	AuthMode     string
	Auth         authn.Config
	HTTP         httpjson.Config
	CursorKey    []byte
	LeaseHolder  string
	StoreFactory StoreFactory
}

// Runtime is the composition owner. HTTP, sessions and watches are optional
// handles into the same lifetime; callers must close Runtime exactly once.
type Runtime struct {
	Store         driver.Store
	Lease         driver.ActiveLease
	Service       *service.Service
	RPC           *rpc.Server
	Authenticator *authn.Authenticator
	HTTPHandler   *httpjson.Handler
	UIHandler     *webui.Handler
	WatchHTTP     *watchhttp.Adapter
	Sessions      *collab.Manager
	Watches       *watch.Manager

	ctx       context.Context
	cancel    context.CancelFunc
	renewDone chan struct{}
	httpMu    sync.Mutex
	http      *httpjson.Server
	closed    bool
	once      sync.Once
	closeErr  error
}

// Open builds one core, registry, collaboration manager and watch manager.
// It does not bind a listener; Listen is the explicit network boundary.
func Open(ctx context.Context, config Config) (*Runtime, error) {
	if ctx == nil {
		return nil, errors.New("composition: context is required")
	}
	config, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	store, err := openStore(ctx, config)
	if err != nil {
		return nil, err
	}
	leaseCtx, leaseCancel := context.WithTimeout(ctx, 3*time.Second)
	lease, err := store.Lease(leaseCtx, config.LeaseHolder, driver.LeaseTTL)
	leaseCancel()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("composition: acquire active lease: %w", err)
	}

	lifetime, cancel := context.WithCancel(ctx)
	r := &Runtime{Store: store, Lease: lease, ctx: lifetime, cancel: cancel, renewDone: make(chan struct{})}
	// Start renewal before binding optional subsystems so a failed bind can
	// still be canceled and joined by Close without leaking the lease.
	go r.renew(config.LeaseHolder)
	if err := r.bind(config); err != nil {
		_ = r.Close(context.Background())
		return nil, err
	}
	return r, nil
}

func normalizeConfig(config Config) (Config, error) {
	config.Driver = strings.ToLower(strings.TrimSpace(config.Driver))
	if config.Driver == "" {
		config.Driver = "memory"
	}
	if config.Project == "" {
		config.Project = DefaultProject
	}
	if strings.TrimSpace(config.Project) != config.Project || strings.ContainsAny(config.Project, "\r\n\x00") {
		return Config{}, errors.New("composition: project must be a single nonempty line")
	}
	if config.Address == "" {
		config.Address = DefaultAddress
	}
	if err := validateAddress(config.Address); err != nil {
		return Config{}, err
	}
	if config.AuthMode == "" {
		config.AuthMode = "anonymous"
	}
	if config.AuthMode != "anonymous" && config.AuthMode != "authenticated" {
		return Config{}, fmt.Errorf("composition: unsupported authentication mode %q", config.AuthMode)
	}
	if config.AuthMode == "anonymous" && !loopbackAddress(config.Address) {
		return Config{}, errors.New("composition: anonymous server must bind a loopback address")
	}
	if config.AuthMode == "authenticated" && config.Auth.Provider == nil && config.Auth.CoreSessions == nil {
		return Config{}, errors.New("composition: authenticated mode requires an authentication provider")
	}
	if len(config.CursorKey) != 0 && len(config.CursorKey) < 32 {
		return Config{}, errors.New("composition: cursor key must contain at least 32 bytes")
	}
	switch config.Driver {
	case "memory":
		if config.DSN != "" {
			return Config{}, errors.New("composition: memory storage does not accept a DSN")
		}
	case "sqlite":
		if strings.TrimSpace(config.DSN) == "" {
			return Config{}, errors.New("composition: sqlite storage requires a file DSN")
		}
	case "postgres", "postgresql":
		if strings.TrimSpace(config.DSN) == "" {
			return Config{}, errors.New("composition: postgres storage requires a DSN")
		}
		config.Driver = "postgres"
	default:
		return Config{}, fmt.Errorf("composition: unsupported storage driver %q", config.Driver)
	}
	if config.LeaseHolder == "" {
		config.LeaseHolder = "plug-server-" + randomID()
	}
	return config, nil
}

func openStore(ctx context.Context, config Config) (driver.Store, error) {
	if config.StoreFactory != nil {
		store, err := config.StoreFactory(ctx, config.Driver, driver.Config{DSN: config.DSN})
		if err != nil {
			return nil, err
		}
		if store == nil {
			return nil, errors.New("composition: storage factory returned a nil store")
		}
		return store, nil
	}
	switch config.Driver {
	case "memory":
		return (memory.Driver{}).Open(ctx, driver.Config{})
	case "sqlite":
		return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: config.DSN})
	case "postgres":
		return (postgres.Driver{}).Open(ctx, driver.Config{DSN: config.DSN})
	default:
		return nil, fmt.Errorf("composition: unsupported storage driver %q", config.Driver)
	}
}

func (r *Runtime) bind(config Config) error {
	meta, err := r.Store.Meta(r.ctx)
	if err != nil {
		return fmt.Errorf("composition: read store metadata: %w", err)
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(r.Store, r.Lease, pipeline.WithOntology(ontology))
	r.Service = service.New(r.Store, r.Lease, writer, service.WithOntology(ontology))
	registry, err := rpc.NewRegistry()
	if err != nil {
		return fmt.Errorf("composition: create RPC registry: %w", err)
	}
	if err := rpc.BindService(registry, r.Service); err != nil {
		return fmt.Errorf("composition: bind service: %w", err)
	}

	// Collaboration and watches share the same lease and lifetime as the core.
	r.Sessions, err = collab.New(r.ctx, collab.Config{
		Service: r.Service, Lease: r.Lease, AuthorizationInstalled: func() bool { return false },
	})
	if err != nil {
		return fmt.Errorf("composition: create collaboration manager: %w", err)
	}
	if err := collab.Bind(registry, r.Sessions); err != nil {
		return fmt.Errorf("composition: bind collaboration: %w", err)
	}

	key := append([]byte(nil), config.CursorKey...)
	if len(key) == 0 {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return fmt.Errorf("composition: generate cursor key: %w", err)
		}
	}
	cursors, err := servertsevents.NewCursorCodec(key)
	if err != nil {
		return fmt.Errorf("composition: create event cursor codec: %w", err)
	}
	eventReader := &coreevents.Reader{
		Log:           servertsevents.Log{Store: r.Store},
		Materializer:  servertsevents.HistoryMaterializer{Store: r.Store},
		Authorization: coreevents.AllowAll{},
	}
	eventService := &servertsevents.Service{Reader: eventReader, Cursors: cursors}
	r.Watches, err = watch.New(r.ctx, watch.Config{Events: eventService, Presence: r.Sessions})
	if err != nil {
		return fmt.Errorf("composition: create watch manager: %w", err)
	}
	watchAdapter, err := watchhttp.New(r.Watches, watchhttp.Config{Ontology: model.Natural64(1)})
	if err != nil {
		return fmt.Errorf("composition: create watch adapter: %w", err)
	}
	if err := watchAdapter.Register(registry); err != nil {
		return fmt.Errorf("composition: bind watches: %w", err)
	}
	r.WatchHTTP = watchAdapter

	r.Authenticator, err = authn.New(config.Auth)
	if err != nil {
		return fmt.Errorf("composition: create authenticator: %w", err)
	}
	r.RPC, err = rpc.New(registry, rpc.Config{
		Incarnation: meta.Incarnation, DefaultProject: config.Project,
		AuthMode: config.AuthMode, OntologyRevision: model.Natural64(1), CursorKey: key,
	})
	if err != nil {
		return fmt.Errorf("composition: create RPC server: %w", err)
	}
	httpConfig := config.HTTP
	httpConfig.SessionBridge = r.bridgeRPCSession
	r.HTTPHandler, err = httpjson.New(r.RPC, r.Authenticator, httpConfig)
	if err != nil {
		return fmt.Errorf("composition: create HTTP handler: %w", err)
	}
	if config.UI {
		r.UIHandler, err = webui.New(webui.Config{Project: config.Project, RPCPath: httpjson.Path})
		if err != nil {
			return fmt.Errorf("composition: create web UI: %w", err)
		}
	}
	return nil
}

// bridgeRPCSession preserves a collaboration session beyond the short-lived
// HTTP RPC request. The watch adapter receives this trusted lifetime through
// its context; an unknown or foreign session is left unbridged so the RPC
// watch handler fails closed.
func (r *Runtime) bridgeRPCSession(ctx context.Context, principal model.PrincipalRef, sessionID, agent string) (context.Context, error) {
	if r == nil || r.Sessions == nil || sessionID == "" {
		return ctx, errors.New("composition: collaboration session is unavailable")
	}
	lifetime, err := r.Sessions.SessionContext(ctx, collab.Caller{Principal: principal, Agent: agent}, sessionID)
	if err != nil {
		return ctx, err
	}
	return watchhttp.WithSession(ctx, sessionID, lifetime), nil
}

// Listen mounts the already-composed handlers on one admitted listener. The
// root handler is deliberately small: every route has an owner and unknown
// paths return a normal 404 from net/http.
func (r *Runtime) Listen(ctx context.Context, address string, tlsConfig *tls.Config) (*httpjson.Server, error) {
	if r == nil || r.HTTPHandler == nil || r.Authenticator == nil {
		return nil, errors.New("composition: runtime is not initialized")
	}
	if ctx == nil {
		return nil, errors.New("composition: listen context is required")
	}
	if address == "" {
		address = DefaultAddress
	}
	if err := validateAddress(address); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle(httpjson.Path, r.HTTPHandler)
	if r.UIHandler != nil {
		mux.Handle("/ui/", r.UIHandler)
	}
	if r.WatchHTTP != nil {
		mux.Handle(watchhttp.DefaultPath, r.watchStreamHandler())
	}
	server, err := r.HTTPHandler.ListenHandler(ctx, address, tlsConfig, mux)
	if err != nil {
		return nil, err
	}
	r.httpMu.Lock()
	defer r.httpMu.Unlock()
	if r.closed {
		_ = server.Close()
		return nil, errors.New("composition: runtime is closed")
	}
	if r.http != nil {
		_ = server.Close()
		return nil, errors.New("composition: listener already started")
	}
	r.http = server
	return server, nil
}

// watchStreamHandler bridges the explicit HTTP stream to a collaboration
// session. The session ID and anonymous agent label are transport metadata;
// the principal is always stamped by the authenticator. A missing or foreign
// session fails closed in watchhttp rather than creating an ephemeral owner.
func (r *Runtime) watchStreamHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		identity, err := r.Authenticator.AdmitHTTP(req)
		if err != nil {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		sessionValues := req.Header.Values("Plug-Session")
		agentValues := req.Header.Values("Plug-Agent")
		if len(sessionValues) != 1 || sessionValues[0] == "" || len(agentValues) > 1 {
			http.Error(w, "session is required", http.StatusBadRequest)
			return
		}
		caller := collab.Caller{Principal: identity.Principal()}
		if len(agentValues) == 1 {
			caller.Agent = agentValues[0]
		}
		lifetime, err := r.Sessions.SessionContext(req.Context(), caller, sessionValues[0])
		if err != nil {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}
		ctx := watchhttp.WithPrincipal(req.Context(), identity.Principal())
		ctx = watchhttp.WithSession(ctx, sessionValues[0], lifetime)
		r.WatchHTTP.ServeHTTP(w, req.WithContext(ctx))
	})
}

// HTTPServer returns the owned listener, if Listen was called.
func (r *Runtime) HTTPServer() *httpjson.Server {
	if r == nil {
		return nil
	}
	r.httpMu.Lock()
	defer r.httpMu.Unlock()
	return r.http
}

func (r *Runtime) renew(holder string) {
	defer close(r.renewDone)
	ticker := time.NewTicker(driver.LeaseRenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.Lease.Lost():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(r.ctx, time.Second)
			err := r.Lease.Renew(ctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

// Close drains HTTP before canceling collaboration/watch work, releases the
// lease, and finally closes the store. Each step is idempotent through the
// single owner, so failed composition paths cannot leak a lease or store.
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.once.Do(func() {
		r.httpMu.Lock()
		r.closed = true
		httpServer := r.http
		r.http = nil
		r.httpMu.Unlock()
		if httpServer != nil {
			r.closeErr = errors.Join(r.closeErr, httpServer.Shutdown(ctx))
		}
		if r.Watches != nil {
			r.closeErr = errors.Join(r.closeErr, r.Watches.Close())
		}
		if r.Sessions != nil {
			r.closeErr = errors.Join(r.closeErr, r.Sessions.Close())
		}
		r.cancel()
		<-r.renewDone
		leaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		r.closeErr = errors.Join(r.closeErr, r.Lease.Release(leaseCtx))
		cancel()
		r.closeErr = errors.Join(r.closeErr, r.Store.Close())
	})
	return r.closeErr
}

func randomID() string {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "random"
	}
	return hex.EncodeToString(value[:])
}

func validateAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return errors.New("composition: listen address must be a literal host and port")
	}
	if strings.EqualFold(host, "localhost") {
		host = "127.0.0.1"
	}
	if host == "" {
		return errors.New("composition: listen address must bind an explicit host")
	}
	if strings.ContainsAny(host, "\r\n\x00") {
		return errors.New("composition: listen address contains invalid characters")
	}
	if _, err := netip.ParseAddr(host); err != nil {
		return errors.New("composition: listen address must use a literal IP")
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		if strings.IndexFunc(port, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return errors.New("composition: listen port must be numeric")
		}
		return errors.New("composition: listen port is out of range")
	}
	return nil
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}

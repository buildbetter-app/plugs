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
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/plugins/authnoidc"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/collab"
	servertsevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/server/ontologyrpc"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
	"github.com/buildbetter-app/plugs/internal/server/transport/websocket"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/server/watch"
	"github.com/buildbetter-app/plugs/internal/server/watchhttp"
	"github.com/buildbetter-app/plugs/internal/server/webui"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
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

// OIDCFactory constructs the existing OIDC provider from trusted host
// configuration. Composition supplies the selected store's durable session
// capability in the copied authnoidc.Config before calling the factory.
// Production uses authnoidc.New; the seam keeps tests from replacing the
// provider or its session semantics with a parallel implementation.
type OIDCFactory func(authnoidc.Config) (*authnoidc.Provider, error)

// OntologyConfig installs the durable ontology source used by the composed
// runtime. Source is resolved per project and is read again by ontologyops
// through the same store snapshot for every decision. The optional callbacks
// are trusted host seams; callers cannot supply an ontology document, digest,
// authorization decision or validating-pre result over RPC.
//
// A non-nil configuration enables the canonical ontology RPC surface,
// with protected ontology.apply through the Runtime-owned Writer only when no
// authorization policy is configured. Installed policy requires a current read
// adapter and keeps unsupported protected write authorization unavailable. A nil
// ValidatingPre is converted to ontologyops.NoValidatingPre only when the
// actual host inventory contains no validating-pre bindings.
type OntologyConfig struct {
	Source        service.OntologySourceLocator
	AuthorizeRead ontologyops.AuthorizeRead
	ValidatingPre ontologymigration.ValidatingPre
	Authorizer    port.Authorizer
	EventContext  port.EventContextSource
}

// EventAuthorization is the trusted current-read capability for both ordinary
// event images and every resource referenced by a condition notice. It cannot
// be supplied by RPC or inferred from an ordinary read callback.
type EventAuthorization interface {
	coreevents.Authorization
	coreevents.ResourceAuthorization
}

// Config describes one server composition. Driver and DSN are intentionally
// separate so an invalid combination can be refused before any listener is
// opened. UI is enabled for shared servers and disabled by in-process callers.
type Config struct {
	Driver  string
	DSN     string
	Project string
	Address string
	UI      bool
	// RollbackWindow refuses newly created user-content kinds without an
	// actual canonical reverse Beads mapping. It defaults to false and is a
	// prepared switch-day control; it is never enabled by a live action here.
	RollbackWindow bool
	// ReverseConverter owns executable reverse mappings. The same instance is
	// used by Runtime.ConvertReverse and both write guards.
	// Nil selects canonical built-ins; YAML/RPC cannot register a mapping.
	ReverseConverter *reverseconverter.Converter
	AuthMode         string
	Auth             authn.Config
	// OIDC enables the existing OIDC provider. Its SessionStore is owned by
	// composition and is replaced with the selected SQLite/PostgreSQL store;
	// the typed config is not parsed from .plug/config.yaml yet.
	OIDC         *authnoidc.Config
	OIDCFactory  OIDCFactory
	HTTP         httpjson.Config
	CursorKey    []byte
	LeaseHolder  string
	StoreFactory StoreFactory
	// AbortSQLiteStalledCommit is a process-owned handler for an uncertain
	// native COMMIT that cannot be joined. It must exit for supervisor restart.
	AbortSQLiteStalledCommit func(error)
	// RestorePresence is the trusted D32 verifier for existing admin.restore.*
	// and the host-private converter import. Nil deliberately fails closed.
	RestorePresence restore.PresenceVerifier
	// RestoreImportPlanner is converter-owned inspection of the exact archive;
	// composition never derives or accepts a caller-supplied derived digest.
	RestoreImportPlanner RestoreImportPlanner
	RestoreSourcePlanner RestoreSourcePlanner
	// NativeImportOntology enables the canonical installed native catalog at
	// converterroundtrip.ImportedOntologySourceID. An empty target may start
	// without that record; only a successful native import installs the resolver.
	// ontology.get uses the same protected source before and after restart.
	NativeImportOntology bool
	// NativePlugins and NativeBindings configure trusted compiled-in callbacks
	// at the same composition root as the store, lease and Writer. They are
	// host configuration, never client-supplied code or grants.
	NativePlugins  []sdk.Native
	NativeBindings []pluginhost.Binding
	// UIFrames is the trusted, digest-pinned inventory served by the same
	// listener and RPC composition. The UI validates every bundle and grant;
	// this field accepts no caller-supplied installation or credential.
	UIFrames []webui.FrameBundle
	// RestoredWASI loads an existing source-built lock through ArtifactStore.
	// It is an alternative to NativePlugins/NativeBindings, not an installer.
	RestoredWASI *RestoredWASIConfig
	// Compaction supplies a host-owned summary and trusted D32 verifier. Nil
	// keeps summary generation and reversal unavailable; there is no egress.
	Compaction *service.CompactionConfig

	// ViewReader and Readiness are optional host-owned read projections. When
	// ViewReader is present, view.run/view.explain and view.watch are composed
	// over the same store, lease and projection status source.
	ViewReader         viewops.ViewReader
	Readiness          viewops.ReadinessReader
	AuthorizeRead      viewops.AuthorizeRead
	AuthorizeReadBatch viewops.AuthorizeReadBatch
	// EventAuthorization evaluates current policy for list/watch/dispatch.
	// A configured policy without this capability does not expose event.list.
	EventAuthorization EventAuthorization
	// EpicProgress opts into the separately authorized, count-only parent view.
	// Nil keeps the capability unavailable; no hierarchy write is enabled here.
	EpicProgress *viewops.EpicProgressConfig
	// ParentProjection admits current-authority ordinary parent disclosure.
	ParentProjection *viewops.ParentProjectionConfig
	// DirectBlockerOntology is an optional protected ontology source locator.
	// When absent, view.explain omits directBlockers rather than guessing an ID
	// or falling back to the process-local NewOntology resolver.
	DirectBlockerOntology service.OntologySourceLocator
	// Ontology selects the installed protected ontology for the complete
	// runtime. When absent, the legacy process-local default remains in use and
	// ontology management methods are not advertised.
	Ontology *OntologyConfig
	// StaleOntologySource is an optional protected ontology source for read-only
	// age/status selection. Without it, view.run uses the same immutable
	// built-in ontology instance installed on the Writer and Service.
	StaleOntologySource service.OntologySourceLocator

	// ProjectionBindings are the effective projection bindings owned by this
	// composition. Their durable state is read through the checkpoint adapter;
	// no process-local status is inferred when the list is empty.
	ProjectionBindings []coreevents.Binding
	ProjectionRegistry *coreevents.ProjectionRegistry
	ProjectionBinding  string
}

// Runtime is the composition owner. HTTP, sessions and watches are optional
// handles into the same lifetime; callers must close Runtime exactly once.
type Runtime struct {
	Store                driver.Store
	Lease                driver.ActiveLease
	Service              *service.Service
	RPC                  *rpc.Server
	Metrics              *pipeline.Collector
	Authenticator        *authn.Authenticator
	OIDC                 *authnoidc.Provider
	NativeHost           *pluginhost.Host
	HTTPHandler          *httpjson.Handler
	UIHandler            *webui.Handler
	WatchHTTP            *watchhttp.Adapter
	Sessions             *collab.Manager
	Watches              *watch.Manager
	Views                *viewops.Service
	Projections          *coreevents.ProjectionRegistry
	Dispatcher           *coreevents.Dispatcher
	ProjectionStatus     coreevents.ProjectionStatusReader
	reverseConverter     *reverseconverter.Converter
	restoreImport        *restoreImportAdapter
	restoredWASI         *restoredWASI
	ontologyRuntime      *service.Ontology
	nativeImportOntology bool
	sourceMarker         sourceWorkflowMarker

	config    Config
	ctx       context.Context
	cancel    context.CancelFunc
	renewDone chan struct{}
	renewed   atomic.Bool
	signing   runtimeSigningKeys
	mcp       http.Handler
	websocket *websocket.Handler
	httpMu    sync.Mutex
	http      *runtimeListener
	closed    bool
	once      sync.Once
	closeErr  error
}

type runtimeSigningKeys struct {
	cursor []byte
	mcp    [32]byte
}

// LeaseRenewed reports whether this runtime has completed a successful renewal.
func (r *Runtime) LeaseRenewed() bool { return r.renewed.Load() }

// ConvertReverse uses the same immutable converter that controls this runtime's
// write admission. The default converter refuses native workflow content.
func (r *Runtime) ConvertReverse(dump reverseconverter.Dump) (reverseconverter.Result, error) {
	return r.reverseConverter.Convert(dump)
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
	signing := runtimeSigningKeys{cursor: append([]byte(nil), config.CursorKey...)}
	if len(signing.cursor) == 0 {
		signing.cursor = make([]byte, 32)
		if _, err := rand.Read(signing.cursor); err != nil {
			return nil, fmt.Errorf("composition: generate cursor key: %w", err)
		}
	}
	if _, err := rand.Read(signing.mcp[:]); err != nil {
		return nil, fmt.Errorf("composition: generate MCP session key: %w", err)
	}
	restored, err := restoreConfiguredWASI(ctx, config)
	if err != nil {
		return nil, err
	}
	// Until Runtime takes ownership, every startup failure removes the
	// disposable restored bytes without opening a guest.
	defer func() { _ = closeRestoredWASI(restored) }()
	store, err := openStore(ctx, config)
	if err != nil {
		return nil, err
	}
	// A killed predecessor can leave a durable heartbeat with a full TTL.
	leaseCtx, leaseCancel := context.WithTimeout(ctx, driver.LeaseTTL+5*time.Second)
	lease, err := store.Lease(leaseCtx, config.LeaseHolder, driver.LeaseTTL)
	leaseCancel()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("composition: acquire active lease: %w", err)
	}
	r, err := bindRuntime(ctx, config, store, lease, restored, signing, nil)
	restored = nil // bindRuntime owns the artifact even on failure.
	return r, err
}

func bindRuntime(ctx context.Context, config Config, store driver.Store, lease driver.ActiveLease, restored *restoredWASI, signing runtimeSigningKeys, previous *Runtime) (*Runtime, error) {
	lifetime, cancel := context.WithCancel(ctx)
	r := &Runtime{Store: store, Lease: lease, config: config, ctx: lifetime, cancel: cancel, renewDone: make(chan struct{}), restoredWASI: restored, signing: signing}
	// Start renewal before binding optional subsystems so a failed bind can
	// still be canceled and joined by Close without leaking the lease.
	go r.renew()
	if err := r.bind(config, previous); err != nil {
		_ = r.Close(context.Background())
		return nil, err
	}
	return r, nil
}

// Reacquire retires a fenced runtime and binds a fresh storage epoch over the
// open store. A running listener reports busy until ownership moves to the
// replacement runtime. The caller owns both lifecycle operations.
func (r *Runtime) Reacquire(ctx context.Context) (*Runtime, error) {
	select {
	case <-r.Lease.Lost():
	default:
		return nil, errors.New("composition: active lease is still live")
	}
	store := r.Store
	r.httpMu.Lock()
	listener := r.http
	closed := r.closed
	r.httpMu.Unlock()
	if closed {
		return nil, errors.New("composition: runtime is closed")
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
		if listener != nil {
			_ = listener.Close()
		}
	}()
	if guard, ok := r.Lease.(interface{ WaitForCommit(context.Context) error }); ok {
		if err := guard.WaitForCommit(ctx); err != nil {
			return nil, err
		}
	}
	err := r.shutdown(ctx, false)
	if err != nil {
		slog.WarnContext(ctx, "retired fenced runtime with cleanup errors", "error_type", fmt.Sprintf("%T", err))
	}
	config := r.config
	restored, err := restoreConfiguredWASI(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("composition: restore artifact for fresh lease: %w", err)
	}
	defer func() { _ = closeRestoredWASI(restored) }()

	// A native writer can briefly prevent even opening the acquisition
	// transaction. Retry without reopening the database or refreshing the
	// fenced lease. Bound recovery so a permanently broken store can exit.
	recoveryCtx, stopRecovery := context.WithTimeout(ctx, 30*time.Second)
	defer stopRecovery()
	for {
		lease, err := store.Lease(recoveryCtx, config.LeaseHolder, driver.LeaseTTL)
		if err == nil {
			retireCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_ = r.Lease.Release(retireCtx)
			cancel()
			next, bindErr := bindRuntime(ctx, config, store, lease, restored, r.signing, r)
			store, restored = nil, nil // bindRuntime owns both even on failure.
			if bindErr == nil && listener != nil {
				next.http = listener
				listener.route.Store(next.httpRoute())
				listener = nil
			}
			return next, bindErr
		}
		select {
		case <-recoveryCtx.Done():
			return nil, fmt.Errorf("composition: reacquire active lease: %w", recoveryCtx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// runtimeEventProvider adapts the already-composed durable event service to
// the transport-neutral core service port. It is populated before the RPC
// registry is snapshotted; a nil target is therefore a composition bug, not a
// caller-controlled fallback.
type runtimeEventProvider struct {
	events *servertsevents.Service
}

func (p *runtimeEventProvider) List(ctx context.Context, req service.EventProviderRequest) (service.EventPage, error) {
	if p == nil || p.events == nil {
		return service.EventPage{}, errors.New("composition: durable event reader is not configured")
	}
	page, err := p.events.List(ctx, servertsevents.Request{
		Principal: req.Principal,
		Filter: coreevents.Filter{
			Types:     append([]string(nil), req.Filter.Types...),
			Resources: append([]string(nil), req.Filter.Resources...),
		},
		After: req.After,
		Limit: req.Limit,
	})
	if err != nil {
		return service.EventPage{}, err
	}
	return service.EventPage{Events: page.Events, Cursor: page.Cursor}, nil
}

// runtimeEventAuthorization permits the no-policy fallback only while the
// host has no configured or installed authority. The callback is checked on
// every read so installing a chain cannot retain an earlier AllowAll decision.
type runtimeEventAuthorization struct {
	trusted  EventAuthorization
	required func() bool
}

func (a runtimeEventAuthorization) Read(ctx context.Context, by model.PrincipalRef, event model.DurableEvent) (coreevents.Visibility, error) {
	if a.trusted != nil {
		return a.trusted.Read(ctx, by, event)
	}
	if a.required() {
		return coreevents.Visibility{}, coreevents.ErrResourceAuthorization
	}
	return (coreevents.AllowAll{}).Read(ctx, by, event)
}

func (a runtimeEventAuthorization) ReadResource(ctx context.Context, by model.PrincipalRef, resource model.AuditResource) (coreevents.ResourceVisibility, error) {
	if a.trusted != nil {
		return a.trusted.ReadResource(ctx, by, resource)
	}
	if a.required() {
		return coreevents.ResourceVisibility{}, coreevents.ErrResourceAuthorization
	}
	return (coreevents.AllowAll{}).ReadResource(ctx, by, resource)
}

func eventAuthorizationRequired(config Config, authConfig authn.Config) bool {
	return config.AuthorizeRead != nil || config.AuthorizeReadBatch != nil ||
		(authConfig.AuthorizationInstalled != nil && authConfig.AuthorizationInstalled()) ||
		(config.Ontology != nil && (config.Ontology.AuthorizeRead != nil || config.Ontology.Authorizer != nil))
}

// loadConfiguredOntology proves the configured source before any service,
// writer or registry is admitted. It intentionally reads at the store head;
// a source from an older snapshot would let startup construct a resolver that
// disagrees with the durable rows and audit pins it is meant to guard.
func loadConfiguredOntology(ctx context.Context, store driver.Store, meta driver.Meta, project string, config *OntologyConfig) (service.OntologySourceLocator, ontology.Snapshot, error) {
	if config == nil || config.Source == nil {
		return nil, ontology.Snapshot{}, errors.New("composition: configured ontology requires a protected source locator")
	}
	ref, err := config.Source(project)
	if err != nil {
		return nil, ontology.Snapshot{}, fmt.Errorf("composition: select installed ontology source: %w", err)
	}
	if err := validateOntologySourceRef(ref); err != nil {
		return nil, ontology.Snapshot{}, err
	}
	at := meta.Head
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		return nil, ontology.Snapshot{}, fmt.Errorf("composition: read installed ontology source at store head: %w", err)
	}
	rows, readErr := reader.Get(ctx, []model.Ref{ref})
	closeErr := reader.Close()
	if readErr != nil {
		return nil, ontology.Snapshot{}, fmt.Errorf("composition: read installed ontology source: %w", readErr)
	}
	if closeErr != nil {
		return nil, ontology.Snapshot{}, fmt.Errorf("composition: close installed ontology snapshot: %w", closeErr)
	}
	if len(rows) != 1 || rows[0].Ref.ID != ref.ID || rows[0].Ref.Branch.String() != ref.Branch.String() || rows[0].Kind != "ontology" || rows[0].Revision.Validate() != nil {
		return nil, ontology.Snapshot{}, errors.New("composition: installed ontology source is absent, malformed or has the wrong kind")
	}
	source, err := service.DecodeOntologySource(rows[0].Body)
	if err != nil {
		return nil, ontology.Snapshot{}, fmt.Errorf("composition: decode installed ontology source: %w", err)
	}
	compiled, problems := ontology.Compile(source.Base, source.Overlays, source.Plugins)
	if len(problems) != 0 {
		return nil, ontology.Snapshot{}, fmt.Errorf("composition: compile installed ontology source: %d problem(s)", len(problems))
	}
	// Force the selected effective overlay through the same detached snapshot
	// before publishing it to the Writer and Service. Document() is a deep
	// detached value and therefore cannot expose a partially compiled map.
	_ = compiled.Effective(project).Document()
	return config.Source, compiled, nil
}

// Configuration selects a protected identity, never an ontology document or
// authorization decision. Runtime startup independently verifies its durable
// kind, revision, decoder and compiler before admitting requests.
func validateOntologySourceRef(ref model.Ref) error {
	if err := ref.Validate(); err != nil || !ref.IsID() || !ref.Branch.IsZero() ||
		strings.ContainsAny(ref.ID, " \t\r\n\x00") ||
		(ref.ID != "ontology" && !strings.HasPrefix(ref.ID, "ontology/")) {
		return errors.New("composition: ontology source locator must select a protected branch-zero ontology ID")
	}
	return nil
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
		if config.OIDC != nil {
			config.AuthMode = "authenticated"
		} else {
			config.AuthMode = "anonymous"
		}
	}
	if config.AuthMode != "anonymous" && config.AuthMode != "authenticated" {
		return Config{}, fmt.Errorf("composition: unsupported authentication mode %q", config.AuthMode)
	}
	if config.OIDC != nil && config.AuthMode != "authenticated" {
		return Config{}, errors.New("composition: OIDC authentication requires authenticated mode")
	}
	if config.OIDCFactory != nil && config.OIDC == nil {
		return Config{}, errors.New("composition: OIDC factory requires OIDC configuration")
	}
	if config.AuthMode == "anonymous" && !loopbackAddress(config.Address) {
		return Config{}, errors.New("composition: anonymous server must bind a loopback address")
	}
	if config.AuthMode == "authenticated" && config.Auth.Provider == nil && config.Auth.CoreSessions == nil && config.OIDC == nil {
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
		return (sqlite.Driver{AbortStalledCommit: config.AbortSQLiteStalledCommit}).Open(ctx, driver.Config{DSN: config.DSN})
	case "postgres":
		return (postgres.Driver{}).Open(ctx, driver.Config{DSN: config.DSN})
	default:
		return nil, fmt.Errorf("composition: unsupported storage driver %q", config.Driver)
	}
}

func (r *Runtime) bind(config Config, previous *Runtime) error {
	// Renewal is already running. Complete the current-image upgrade before
	// any request is admitted; ordinary writes retain their two-second pre phase.
	if err := pipeline.BuildSourceIndexes(r.ctx, r.Store, r.Lease); err != nil {
		return fmt.Errorf("composition: activate graph source indexes: %w", err)
	}
	meta, err := r.Store.Meta(r.ctx)
	if err != nil {
		return fmt.Errorf("composition: read store metadata: %w", err)
	}
	authConfig, oidc, err := composeAuth(config, r.Store)
	if err != nil {
		return err
	}
	r.OIDC = oidc
	if config.EpicProgress != nil && config.EpicProgress.AnonymousOnly &&
		(config.AuthMode == "authenticated" || authConfig.Provider != nil || authConfig.CoreSessions != nil ||
			(authConfig.AuthorizationInstalled != nil && authConfig.AuthorizationInstalled())) {
		return fmt.Errorf("composition: anonymous epic progress requires an explicit anonymous host without authentication or authorization plugins")
	}
	var ontologyLocator service.OntologySourceLocator
	var ontologySnapshot ontology.Snapshot
	var ontologyReadAuthorize ontologyops.AuthorizeRead
	var ontologyApplyEnabled bool
	if config.NativeImportOntology {
		installed := OntologyConfig{}
		if config.Ontology != nil {
			installed = *config.Ontology
		}
		installed.Source = service.StaticOntologySource(model.RefByID(converterroundtrip.ImportedOntologySourceID, model.Natural64(0)))
		config.Ontology = &installed
		r.nativeImportOntology = true
	}
	if config.Ontology != nil {
		readAuthorize := config.Ontology.AuthorizeRead
		if readAuthorize == nil && config.AuthorizeRead != nil {
			readAuthorize = ontologyops.AuthorizeRead(config.AuthorizeRead)
		}
		installed := func() bool {
			return config.Ontology.Authorizer != nil ||
				(authConfig.AuthorizationInstalled != nil && authConfig.AuthorizationInstalled())
		}
		if readAuthorize == nil && (installed() || config.AuthorizeReadBatch != nil || config.EventAuthorization != nil) {
			return errors.New("composition: installed ontology with configured authorization requires a current read authorizer")
		}
		// A nil callback is the canonical absence of an authorization chain in
		// ontologyops migration facts. Do not manufacture an installed chain for
		// an explicit no-policy host merely to guard a nonexistent callback.
		if readAuthorize != nil || authConfig.AuthorizationInstalled != nil || config.Ontology.Authorizer != nil {
			ontologyReadAuthorize = func(ctx context.Context, by model.PrincipalRef, resource model.ResourceRef, project string) error {
				if readAuthorize != nil {
					return readAuthorize(ctx, by, resource, project)
				}
				if installed() {
					return &model.Error{Data: model.ErrorData{Code: "forbidden", Hint: "current ontology read authorization is unavailable"}}
				}
				return nil
			}
		}
		// ApplyProtected has no installed-authorizer revalidation capability.
		// Ordinary Writer authorization cannot grant this distinct operation.
		// A declared dynamic chain also keeps apply unavailable from startup.
		ontologyApplyEnabled = authConfig.AuthorizationInstalled == nil &&
			config.Ontology.Authorizer == nil && readAuthorize == nil && config.AuthorizeReadBatch == nil && config.EventAuthorization == nil
		if config.NativeImportOntology && meta.Head.IsZero() {
			ontologyLocator = config.Ontology.Source
		} else {
			ontologyLocator, ontologySnapshot, err = loadConfiguredOntology(r.ctx, r.Store, meta, config.Project, config.Ontology)
			if err != nil {
				return err
			}
		}
	}
	var ontologyRuntime *service.Ontology
	if config.Ontology == nil || config.NativeImportOntology && meta.Head.IsZero() {
		ontologyRuntime = service.NewOntology(nil)
	} else {
		ontologyRuntime = service.NewOntology(ontologySnapshot.Effective)
	}
	ontology := ontologyRuntime
	r.ontologyRuntime = ontologyRuntime
	eventProvider := &runtimeEventProvider{}
	nativeValidatorCount := 0
	r.reverseConverter = config.ReverseConverter
	writerOptions := []pipeline.Option{
		pipeline.WithOntology(ontology),
		pipeline.WithRollbackWindow(config.RollbackWindow),
		pipeline.WithReverseConverter(r.reverseConverter),
	}
	if config.Ontology != nil {
		writerOptions = append(writerOptions, pipeline.WithOntologyAudit(service.NewOntologyAudit(ontologyLocator)))
		if config.Ontology.Authorizer != nil {
			if config.Ontology.EventContext == nil {
				return errors.New("composition: configured ontology authorizer requires trusted event context")
			}
			writerOptions = append(writerOptions, pipeline.WithAuthorizer(config.Ontology.Authorizer))
		}
		if config.Ontology.EventContext != nil {
			writerOptions = append(writerOptions, pipeline.WithEventContext(config.Ontology.EventContext))
		}
	}
	if r.restoredWASI != nil || len(config.NativePlugins) != 0 || len(config.NativeBindings) != 0 {
		var nativeHost *pluginhost.Host
		var hostErr error
		if r.restoredWASI != nil {
			nativeHost, hostErr = r.newRestoredWASIHost(authConfig)
		} else {
			nativeHost, hostErr = r.newNativeHost(config, authConfig)
		}
		if hostErr != nil {
			return hostErr
		}
		r.NativeHost = nativeHost
		nativeValidatorCount = len(nativeHost.Validators())
		writerOptions = append(writerOptions,
			pipeline.WithMutators(nativeHost.Mutators()...),
			pipeline.WithValidators(nativeHost.Validators()...),
			pipeline.WithPostSubscribers(nativeHost.Posts()...),
		)
	}
	writer, metrics := newPipelineWriter(r.Store, r.Lease, writerOptions...)
	r.Metrics = metrics
	eventAuthorization := runtimeEventAuthorization{
		trusted:  config.EventAuthorization,
		required: func() bool { return eventAuthorizationRequired(config, authConfig) },
	}
	serviceOptions := []service.Option{
		service.WithOntology(ontology),
		service.WithRollbackWindow(config.RollbackWindow),
		service.WithReverseConverter(r.reverseConverter),
	}
	if config.EventAuthorization != nil || !eventAuthorization.required() {
		serviceOptions = append(serviceOptions, service.WithEventProvider(eventProvider))
	}
	if checkpoints, checkpointErr := driver.RequireWorkflowCheckpointStore(r.Store, r.Lease); checkpointErr == nil {
		state, stateErr := pluginhost.NewWorkflowRuntimeState(checkpoints, r.Lease)
		if stateErr != nil {
			return fmt.Errorf("composition: initialize workflow checkpoint state: %w", stateErr)
		}
		workflow, workflowErr := workflowplugin.New(state)
		if workflowErr != nil {
			return fmt.Errorf("composition: initialize workflow plugin: %w", workflowErr)
		}
		serviceOptions = append(serviceOptions, service.WithWorkflow(workflow.WithIdentity(workflowplugin.Alias, "default", meta.Incarnation)))
	}
	if config.Compaction != nil {
		serviceOptions = append(serviceOptions, service.WithCompaction(*config.Compaction))
	}
	r.Service = service.New(r.Store, r.Lease, writer, serviceOptions...)
	if err := r.bindSourceWorkflow(r.ctx, meta, previous); err != nil {
		return fmt.Errorf("composition: recover source workflow: %w", err)
	}
	var ontologyService *ontologyops.Service
	if config.Ontology != nil {
		pre := config.Ontology.ValidatingPre
		if pre == nil {
			if nativeValidatorCount != 0 {
				return fmt.Errorf("composition: configured ontology has %d native validating-pre binding(s); supply the actual ValidatingPre inventory", nativeValidatorCount)
			}
			pre = ontologyops.NoValidatingPre()
		}
		var protectedApplier pipeline.ProtectedApplier
		if ontologyApplyEnabled {
			protectedApplier = writer
		}
		ontologyService, err = ontologyops.New(r.Store, r.Lease, ontologyops.Config{
			Source: ontologyLocator, AuthorizeRead: ontologyReadAuthorize, ValidatingPre: pre,
			ProtectedApplier: protectedApplier, OntologyRefresher: ontology,
		})
		if err != nil {
			return fmt.Errorf("composition: create ontology service: %w", err)
		}
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		return fmt.Errorf("composition: create RPC registry: %w", err)
	}
	if err := rpc.BindService(registry, r.Service); err != nil {
		return fmt.Errorf("composition: bind service: %w", err)
	}
	if ontologyService != nil {
		if err := ontologyrpc.Bind(registry, ontologyService); err != nil {
			return fmt.Errorf("composition: bind ontology: %w", err)
		}
	}
	planner := config.RestoreImportPlanner
	if planner == nil {
		// The production default is the converter owner's read-only exact
		// inspection helper. It supplies no authority and opens no target.
		planner = func(ctx context.Context, input io.Reader) (converterroundtrip.ImportInspection, error) {
			return converterroundtrip.InspectImport(ctx, input)
		}
	}
	r.restoreImport = newRestoreImportAdapter(r.Store, r.Lease, config.AuthMode, config.RestorePresence, planner)
	r.restoreImport.sourcePlanner = config.RestoreSourcePlanner
	r.restoreImport.installSource = r.installSourceWorkflow
	r.restoreImport.nativeOntology = config.NativeImportOntology
	manager := restore.NewManager(r.Store, r.Lease, config.RestorePresence)
	manager.SetImportHook(r.restoreImport.Import)
	if err := rpc.BindAdmin(registry, manager); err != nil {
		return fmt.Errorf("composition: bind admin: %w", err)
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

	key := r.signing.cursor
	cursors, err := servertsevents.NewCursorCodec(key)
	if err != nil {
		return fmt.Errorf("composition: create event cursor codec: %w", err)
	}
	eventReader := &coreevents.Reader{
		Log:           servertsevents.Log{Store: r.Store},
		Materializer:  servertsevents.HistoryMaterializer{Store: r.Store},
		Authorization: eventAuthorization,
	}
	eventService := &servertsevents.Service{Reader: eventReader, Cursors: cursors}
	eventProvider.events = eventService
	checkpoints, err := servertsevents.NewCheckpoints(r.Store, r.Lease)
	if err != nil {
		return fmt.Errorf("composition: create durable checkpoints: %w", err)
	}
	projectionRegistry := config.ProjectionRegistry
	if projectionRegistry == nil {
		projectionRegistry = coreevents.NewProjectionRegistry()
	}
	for _, binding := range config.ProjectionBindings {
		if err := projectionRegistry.RegisterProjection(binding); err != nil {
			return fmt.Errorf("composition: register projection %q: %w", binding.ID, err)
		}
	}
	r.Projections = projectionRegistry
	r.Dispatcher = &coreevents.Dispatcher{Reader: eventReader, Checkpoints: checkpoints, Active: r.Lease}
	if len(projectionRegistry.Bindings()) != 0 {
		status, statusErr := servertsevents.NewProjectionStatusReader(servertsevents.ProjectionStatusConfig{
			Checkpoints: checkpoints,
			Log:         servertsevents.Log{Store: r.Store},
			Lease:       r.Lease,
			Registry:    projectionRegistry,
		})
		if statusErr != nil {
			return fmt.Errorf("composition: create projection status: %w", statusErr)
		}
		r.ProjectionStatus = status
	}
	viewBinding := config.ProjectionBinding
	if viewBinding == "" {
		viewBinding = "core"
	}
	staleOntologySource := config.StaleOntologySource
	if staleOntologySource == nil {
		staleOntologySource = config.DirectBlockerOntology
	}
	if staleOntologySource == nil && config.EpicProgress != nil {
		staleOntologySource = config.EpicProgress.OntologySource
	}
	var directBlockers viewops.DirectBlockerSource
	if config.DirectBlockerOntology != nil {
		directBlockers = func(ctx context.Context, reader driver.Reader, dependent model.Ref, limit int) (viewops.DirectBlockerVerdict, error) {
			verdict, err := service.ReadDirectBlockers(ctx, reader, service.DirectBlockerRequest{
				Dependent: dependent,
				Ontology:  config.DirectBlockerOntology,
				Limit:     limit,
			})
			if err != nil {
				return viewops.DirectBlockerVerdict{}, err
			}
			out := viewops.DirectBlockerVerdict{Edges: make([]viewops.DirectBlockerEdge, 0, len(verdict.Edges))}
			for _, edge := range verdict.Edges {
				out.Edges = append(out.Edges, viewops.DirectBlockerEdge{
					Blocker:    edge.Blocker,
					TargetKind: edge.TargetKind,
					Kind:       edge.Kind,
					State:      viewops.DirectBlockerState(edge.State),
					Reason:     edge.Reason,
				})
			}
			return out, nil
		}
	}
	r.Views, err = viewops.New(r.Store, r.Lease, viewops.Config{
		Views:                config.ViewReader,
		Readiness:            config.Readiness,
		AuthorizeRead:        config.AuthorizeRead,
		AuthorizeReadBatch:   config.AuthorizeReadBatch,
		EpicProgress:         config.EpicProgress,
		ParentProjection:     config.ParentProjection,
		DirectBlockers:       directBlockers,
		StaleOntologySource:  staleOntologySource,
		StaleDefaultOntology: ontology,
		ProjectionStatus:     r.ProjectionStatus,
		ProjectionBinding:    viewBinding,
		OntologyRevision:     model.Natural64(1),
	})
	if err != nil {
		return fmt.Errorf("composition: create view service: %w", err)
	}
	if err := viewrpc.BindWithConfig(registry, r.Views, viewrpc.Config{CursorKey: key}); err != nil {
		return fmt.Errorf("composition: bind views: %w", err)
	}
	r.Watches, err = watch.New(r.ctx, watch.Config{Events: eventService, Views: r.Views, Presence: r.Sessions})
	if err != nil {
		return fmt.Errorf("composition: create watch manager: %w", err)
	}
	watchAdapter, err := watchhttp.New(r.Watches, watchhttp.Config{Ontology: model.Natural64(1), Views: r.Views})
	if err != nil {
		return fmt.Errorf("composition: create watch adapter: %w", err)
	}
	if err := watchAdapter.Register(registry); err != nil {
		return fmt.Errorf("composition: bind watches: %w", err)
	}
	r.WatchHTTP = watchAdapter

	r.Authenticator, err = authn.New(authConfig)
	if err != nil {
		return fmt.Errorf("composition: create authenticator: %w", err)
	}
	// Up to one active graph write and sixteen waiters across transports.
	// Keep a read slot free of graph writes at lower configured HTTP limits.
	const graphWriteCapacity = 17
	writeCapacity := graphWriteCapacity
	if limit := config.HTTP.MaxConcurrentRequests; limit > 0 {
		writeCapacity = min(writeCapacity, max(1, limit-1))
	}
	r.RPC, err = rpc.New(registry, rpc.Config{
		Incarnation: meta.Incarnation, DefaultProject: config.Project,
		AuthMode: config.AuthMode, OntologyRevision: model.Natural64(1), CursorKey: key,
		GraphWriteCapacity: writeCapacity,
		CurrentIncarnation: func(ctx context.Context) (string, error) {
			current, err := r.Store.Meta(ctx)
			return current.Incarnation, err
		},
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
	r.mcp, err = newMCPHandler(r.RPC, r.Authenticator, r.signing.mcp)
	if err != nil {
		return fmt.Errorf("composition: create MCP handler: %w", err)
	}
	r.websocket, err = websocket.New(r.RPC, r.Authenticator, websocket.Config{})
	if err != nil {
		return fmt.Errorf("composition: create WebSocket handler: %w", err)
	}
	if config.UI {
		r.UIHandler, err = webui.New(webui.Config{Project: config.Project, RPCPath: httpjson.Path, Frames: config.UIFrames})
		if err != nil {
			return fmt.Errorf("composition: create web UI: %w", err)
		}
	}
	return nil
}

func composeAuth(config Config, store driver.Store) (authn.Config, *authnoidc.Provider, error) {
	authConfig := config.Auth
	if config.OIDC == nil {
		return authConfig, nil, nil
	}
	if authConfig.CoreSessions != nil {
		return authn.Config{}, nil, errors.New("composition: OIDC configuration conflicts with an existing core-session source")
	}
	sessionStore, ok := store.(driver.CoreSessionStore)
	if !ok || sessionStore == nil {
		return authn.Config{}, nil, errors.New("composition: OIDC authentication requires a durable SQLite or PostgreSQL core-session store")
	}
	oidcConfig := *config.OIDC
	// The selected store is the only durable-session authority for this
	// composition. Do not retain an independently supplied store from the
	// caller's copy, which could make restart/revocation use another database.
	oidcConfig.SessionStore = sessionStore
	factory := config.OIDCFactory
	if factory == nil {
		factory = authnoidc.New
	}
	provider, err := factory(oidcConfig)
	if err != nil {
		return authn.Config{}, nil, fmt.Errorf("composition: create OIDC provider: %w", err)
	}
	if provider == nil {
		return authn.Config{}, nil, errors.New("composition: OIDC factory returned a nil provider")
	}
	authConfig.CoreSessions = provider
	return authConfig, provider, nil
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
	if r == nil || r.HTTPHandler == nil || r.mcp == nil || r.websocket == nil || r.Authenticator == nil {
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
	listener := &runtimeListener{}
	listener.route.Store(r.httpRoute())
	server, err := r.HTTPHandler.ListenHandler(ctx, address, tlsConfig, listener)
	if err != nil {
		return nil, err
	}
	listener.Server = server
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
	r.http = listener
	return server, nil
}

type runtimeListener struct {
	*httpjson.Server
	route atomic.Pointer[runtimeRoute]
}

type runtimeRoute struct {
	runtime *Runtime
	mux     *http.ServeMux
}

func (r *Runtime) httpRoute() *runtimeRoute {
	mux := http.NewServeMux()
	mux.Handle(httpjson.Path, r.HTTPHandler)
	mux.Handle(mcp.Path, r.mcp)
	mux.Handle(websocket.Path, r.websocket)
	if r.UIHandler != nil {
		mux.Handle("/ui/", r.UIHandler)
	}
	if r.WatchHTTP != nil {
		mux.Handle(watchhttp.DefaultPath, r.watchStreamHandler())
	}
	return &runtimeRoute{runtime: r, mux: mux}
}

func (l *runtimeListener) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	route := l.route.Load()
	r := route.runtime
	select {
	case <-r.Lease.Lost():
		const hint = "active storage lease was lost, reacquiring"
		w.Header().Set("Retry-After", "1")
		if req.URL.Path != httpjson.Path {
			http.Error(w, hint, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write(r.RPC.TransportError(&model.Error{Data: model.ErrorData{Code: "busy", Hint: hint}}))
	default:
		// Already admitted requests remain bound to their original epoch. On
		// retirement, cancel them without closing the shared listener.
		ctx, cancel := context.WithCancel(req.Context())
		stop := context.AfterFunc(r.ctx, cancel)
		defer cancel()
		defer stop()
		route.mux.ServeHTTP(w, req.WithContext(ctx))
	}
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
	if r.http == nil {
		return nil
	}
	return r.http.Server
}

func (r *Runtime) renew() {
	defer close(r.renewDone)
	lastSuccess := time.Now()
	var lastFailure error
	defer func() {
		select {
		case <-r.Lease.Lost():
			attrs := []any{"epoch", r.Lease.Epoch(), "time_since_last_heartbeat", time.Since(lastSuccess)}
			if lease, ok := r.Lease.(interface{ LossReason() string }); ok {
				attrs = append(attrs, "reason", lease.LossReason())
			}
			if lastFailure != nil {
				attrs = append(attrs, leaseRenewalErrorAttrs(lastFailure)...)
			}
			slog.WarnContext(r.ctx, "active storage lease ended", attrs...)
		default:
		}
	}()
	timer := time.NewTimer(driver.LeaseRenewEvery)
	defer timer.Stop()
	const retryDelay = 250 * time.Millisecond
	// Leave a full renewal interval between a heartbeat timeout and the
	// driver's lease deadline. A short request-side timeout can otherwise turn
	// transient SQL/lock contention into an avoidable lease loss.
	renewTimeout := driver.LeaseTTL - 2*driver.LeaseRenewEvery
	if renewTimeout <= 0 {
		renewTimeout = driver.LeaseRenewEvery
	}
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.Lease.Lost():
			return
		case <-timer.C:
			started := time.Now()
			ctx, cancel := context.WithTimeout(r.ctx, renewTimeout)
			err := r.Lease.Renew(ctx)
			cancel()
			if err == nil {
				r.renewed.Store(true)
				lastSuccess = started
				lastFailure = nil
				timer.Reset(time.Until(started.Add(driver.LeaseRenewEvery)))
				continue
			}
			lost := false
			select {
			case <-r.Lease.Lost():
				lost = true
			default:
			}
			if r.ctx.Err() != nil && !lost {
				return
			}
			if lastFailure == nil || lost {
				attrs := append([]any{"epoch", r.Lease.Epoch(), "attempt_duration", time.Since(started)}, leaseRenewalErrorAttrs(err)...)
				slog.WarnContext(r.ctx, "active storage lease renewal failed", attrs...)
			}
			lastFailure = err
			if code, _, _ := driver.ErrorMapping(err); lost || code == "lease_not_held" {
				return
			}
			// Retry the same lease. The driver owns expiry and fencing, and
			// its Lost channel is terminal.
			timer.Reset(retryDelay)
		}
	}
}

// Driver error text can contain connection details or file paths.
func leaseRenewalErrorAttrs(err error) []any {
	attrs := []any{"error_type", fmt.Sprintf("%T", err)}
	var native interface{ Code() int }
	var sqlState interface{ SQLState() string }
	var path *os.PathError
	var link *os.LinkError
	var errno syscall.Errno
	switch {
	case errors.As(err, &native):
		attrs = append(attrs, "error_code", native.Code())
	case errors.As(err, &sqlState):
		attrs = append(attrs, "sql_state", sqlState.SQLState())
	case errors.As(err, &path):
		attrs = append(attrs, "operation", path.Op)
	case errors.As(err, &link):
		attrs = append(attrs, "operation", link.Op)
	case errors.Is(err, context.DeadlineExceeded):
		attrs = append(attrs, "renewal_error", "attempt deadline exceeded")
	case errors.Is(err, context.Canceled):
		attrs = append(attrs, "renewal_error", "attempt canceled")
	}
	if errors.As(err, &errno) {
		attrs = append(attrs, "errno", int(errno))
	}
	if code, details, ok := driver.ErrorMapping(err); ok {
		attrs = append(attrs, "driver_code", code)
		if code == "lease_not_held" {
			attrs = append(attrs, "expected_epoch", details["expectedEpoch"], "observed_epoch", details["observedEpoch"])
		}
	}
	return attrs
}

// Close releases MCP streams and WebSockets, drains HTTP, cancels collaboration/watch work,
// lease, and finally closes the store. Each step is idempotent through the
// single owner, so failed composition paths cannot leak a lease or store.
func (r *Runtime) Close(ctx context.Context) error {
	return r.shutdown(ctx, true)
}

func (r *Runtime) shutdown(ctx context.Context, closeStore bool) error {
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
		r.closeErr = errors.Join(r.closeErr, closeMCPHandler(r.mcp))
		if r.websocket != nil {
			r.closeErr = errors.Join(r.closeErr, r.websocket.Close())
		}
		if closeStore && httpServer != nil {
			r.closeErr = errors.Join(r.closeErr, httpServer.Shutdown(ctx))
		}
		if r.Watches != nil {
			r.closeErr = errors.Join(r.closeErr, r.Watches.Close())
		}
		if r.Sessions != nil {
			r.closeErr = errors.Join(r.closeErr, r.Sessions.Close())
		}
		// Constructors receive the runtime lifetime. Cancel it before joining
		// callbacks that may retain that context, while the lease and store are
		// still available for their cleanup.
		r.cancel()
		if r.NativeHost != nil {
			r.closeErr = errors.Join(r.closeErr, r.NativeHost.Close())
		}
		r.closeErr = errors.Join(r.closeErr, closeRestoredWASI(r.restoredWASI))
		<-r.renewDone
		if closeStore {
			leaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			r.closeErr = errors.Join(r.closeErr, r.Lease.Release(leaseCtx))
			cancel()
			r.closeErr = errors.Join(r.closeErr, r.Store.Close())
		}
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

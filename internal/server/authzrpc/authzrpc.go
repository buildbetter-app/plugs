// Package authzrpc composes the adopted fixed-permission chain with the real
// service, writer and RPC transport. It owns no policy language or storage
// transaction; all decisions go through core/authz and plugins/authzfixed.
package authzrpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Snapshot is the read-only portion of both a pipeline snapshot and a driver
// reader. FactsSource receives it so a host can load authoritative facts from
// its own records without receiving a transaction handle.
type Snapshot interface {
	Get(context.Context, []model.Ref) ([]driver.Record, error)
	Observe(context.Context, []driver.LockKey) ([]driver.LockState, error)
}

// FactsSource loads the current fixed-permission source for a pinned snapshot.
// A source failure is an authorization failure; it is never treated as an
// empty grant set. Implementations must return an independently owned value.
type FactsSource interface {
	Load(context.Context, Snapshot) (authzfixed.Facts, error)
}

// StaticFacts is a trusted host source useful for an installation whose
// control records are already loaded by the composition root. It validates and
// clones on every request, so callers cannot mutate the installed source while
// a decision is running.
type StaticFacts struct {
	Facts authzfixed.Facts
}

func (s StaticFacts) Load(_ context.Context, _ Snapshot) (authzfixed.Facts, error) {
	return cloneFacts(s.Facts)
}

// CallerSource binds an authenticated principal to the host's verified
// admission context. It is deliberately separate from RPC parameters: a
// client cannot provide VerificationID or CapturedEpoch.
type CallerSource func(context.Context, model.PrincipalRef) (authz.Caller, error)

// DefaultCallerSource creates a stable host admission binding for installations
// whose authentication provider has no separate session receipt API. The
// binding is still checked against the exact principal on every decision.
func DefaultCallerSource(epoch model.Revision) CallerSource {
	if epoch.IsZero() {
		epoch = model.Natural64(1)
	}
	return func(_ context.Context, principal model.PrincipalRef) (authz.Caller, error) {
		data, err := json.Marshal(principal)
		if err != nil {
			return authz.Caller{}, err
		}
		digest := sha256.Sum256(data)
		return authz.Caller{Principal: principal, VerificationID: "rpc-" + hex.EncodeToString(digest[:]), CapturedEpoch: epoch}, nil
	}
}

// Config is trusted composition-root state. Store and Lease are borrowed and
// remain owned by the caller. Provider is an existing authentication provider;
// this package never provisions credentials or accepts an actor field.
type Config struct {
	Store          driver.Store
	Lease          driver.ActiveLease
	Provider       authn.Provider
	Facts          FactsSource
	Namespace      authz.Namespace
	Caller         CallerSource
	DefaultProject string
	HTTP           httpjson.Config
	// Audit is optional because the protected ontology source is a separate
	// host seam. When supplied, the trusted event context is installed with it.
	Audit port.OntologyAudit
}

// Composition owns the installed chain and is useful to hosts that need the
// checker for health or tests. The returned HTTP handler is the real server
// boundary; no in-process shortcut is added.
type Composition struct {
	Handler   *httpjson.Handler
	Checker   *Checker
	Service   *GuardedService
	Chain     authz.Chain
	Namespace authz.Namespace
}

func New(ctx context.Context, config Config) (*Composition, error) {
	if nilValue(config.Store) || nilValue(config.Lease) || config.Provider == nil || config.Facts == nil {
		return nil, fmt.Errorf("authzrpc: store, lease, provider and facts are required")
	}
	meta, err := config.Store.Meta(ctx)
	if err != nil {
		return nil, err
	}
	ns := config.Namespace
	if ns.StoreID == "" {
		ns.StoreID = string(meta.StoreID)
	}
	if ns.Incarnation == "" {
		ns.Incarnation = meta.Incarnation
	}
	if ns.InstallationID == "" {
		ns.InstallationID = "fixed"
	}
	if ns.Authority.ID == "" {
		ns.Authority = model.RefByID("authority", model.Natural64(0))
	}
	if err := ns.Validate(); err != nil {
		return nil, fmt.Errorf("authzrpc: invalid namespace: %w", err)
	}
	if ns.StoreID != string(meta.StoreID) || ns.Incarnation != meta.Incarnation {
		return nil, fmt.Errorf("authzrpc: namespace does not match store")
	}
	caller := config.Caller
	if caller == nil {
		caller = DefaultCallerSource(model.Natural64(1))
	}
	rules, err := authzfixed.New(ns)
	if err != nil {
		return nil, err
	}
	chain := authz.Chain{Namespace: ns, Rules: []authz.RuleSet{rules}}
	checker := &Checker{chain: chain, source: config.Facts, caller: caller, store: config.Store}
	ontology := service.NewOntology(nil)
	options := []pipeline.Option{pipeline.WithOntology(ontology), pipeline.WithAuthorizer(checker), pipeline.WithEventContext(checker)}
	if config.Audit != nil {
		options = append(options, pipeline.WithOntologyAudit(config.Audit))
	}
	writer := pipeline.New(config.Store, config.Lease, options...)
	domain := service.New(config.Store, config.Lease, writer, service.WithOntology(ontology))
	guarded := &GuardedService{delegate: domain, checker: checker}
	registry, err := rpc.NewRegistry()
	if err != nil {
		return nil, err
	}
	if err := rpc.BindService(registry, guarded); err != nil {
		return nil, err
	}
	server, err := rpc.New(registry, rpc.Config{
		Incarnation: meta.Incarnation, DefaultProject: config.DefaultProject,
		AuthMode: "authenticated", OntologyRevision: model.Natural64(1),
		Visible: checker.Visible,
	})
	if err != nil {
		return nil, err
	}
	admission, err := authn.New(authn.Config{Provider: config.Provider, AuthorizationInstalled: func() bool { return true }})
	if err != nil {
		return nil, err
	}
	handler, err := httpjson.New(server, admission, config.HTTP)
	if err != nil {
		return nil, err
	}
	return &Composition{Handler: handler, Checker: checker, Service: guarded, Chain: chain, Namespace: ns}, nil
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func cloneFacts(in authzfixed.Facts) (authzfixed.Facts, error) {
	if err := in.Validate(); err != nil {
		return authzfixed.Facts{}, err
	}
	out := in
	out.Teams = append([]authzfixed.Team(nil), in.Teams...)
	out.Projects = append([]authzfixed.Project(nil), in.Projects...)
	out.Grants = append([]authz.Grant(nil), in.Grants...)
	return out, nil
}

// MutableFacts is a small host-owned source for an installed store. It is
// deliberately explicit: updates replace the complete validated source and do
// not expose a grant mutation RPC or a permission cache.
type MutableFacts struct {
	mu    sync.RWMutex
	facts authzfixed.Facts
}

func NewMutableFacts(facts authzfixed.Facts) (*MutableFacts, error) {
	copy, err := cloneFacts(facts)
	if err != nil {
		return nil, err
	}
	return &MutableFacts{facts: copy}, nil
}
func (s *MutableFacts) Load(_ context.Context, _ Snapshot) (authzfixed.Facts, error) {
	if s == nil {
		return authzfixed.Facts{}, errors.New("authorization facts source is nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneFacts(s.facts)
}
func (s *MutableFacts) Replace(facts authzfixed.Facts) error {
	copy, err := cloneFacts(facts)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.facts = copy
	s.mu.Unlock()
	return nil
}

func samePrincipal(a, b model.PrincipalRef) bool {
	x, err := json.Marshal(a)
	if err != nil {
		return false
	}
	y, err := json.Marshal(b)
	return err == nil && string(x) == string(y)
}

func methodAction(method string) (authz.Action, authz.ResourceKind) {
	switch method {
	case "bead.get", "bead.ready", "bead.list", "view.run", "history.query":
		if method == "history.query" {
			return authz.Read, authz.History
		}
		return authz.Read, authz.Bead
	case "bead.claim", "bead.claimNext", "bead.update":
		return authz.Modify, authz.Bead
	default:
		return authz.Modify, authz.Bead
	}
}

func forbidden(hint string) error {
	return &model.Error{Message: "authorization denied", Data: model.ErrorData{Code: "forbidden", Hint: hint}}
}

func notFound() error {
	return &model.Error{Message: "bead was not found", Data: model.ErrorData{Code: "not_found"}}
}

func authzError(err error) error {
	var unavailable *authz.Error
	if errors.As(err, &unavailable) {
		if unavailable.Code == "authz_unavailable" {
			return &model.Error{Message: unavailable.Error(), Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
		}
		if unavailable.Code == "authz_not_installed" {
			return &model.Error{Message: unavailable.Error(), Data: model.ErrorData{Code: "authz_not_installed"}}
		}
	}
	return err
}

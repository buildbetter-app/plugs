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
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/claimrpc"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
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

func (s StaticFacts) Load(ctx context.Context, snapshot Snapshot) (authzfixed.Facts, error) {
	facts, err := cloneFacts(s.Facts)
	if err != nil {
		return authzfixed.Facts{}, err
	}
	if _, err := observeACL(ctx, snapshot, facts.Namespace); err != nil {
		return authzfixed.Facts{}, err
	}
	return facts, nil
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
	// Mutators optionally installs trusted host bindings on the same Writer as
	// the fixed checker and event context. Callback resources remain caller-owned.
	Mutators []pipeline.MutatorBinding
	// Hierarchy opts into the bounded EP2 create/reparent authorization adapter.
	// It does not implement hierarchy storage/RPC or relax ordinary #18 writes.
	// Keep false until the host installs the separately reviewed hierarchy Writer.
	Hierarchy bool
	// Workflow is the optional durable plugin provider. When installed, the
	// guarded service exposes x.workflow.invoke and standard-tool routing while
	// preserving the checker for core beads.
	Workflow *workflowplugin.Provider
	// LifecyclePolicy opts into the existing exact User-close authority. The
	// host owns this protected policy record; public callers cannot select it.
	LifecyclePolicy *LifecyclePolicyConfig
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
	return newWithClock(ctx, config, time.Now)
}

// newWithClock keeps controlled time at construction, outside RPC and server
// configuration. The test build exposes this seam for real-store assertions.
func newWithClock(ctx context.Context, config Config, now func() time.Time) (*Composition, error) {
	if nilValue(config.Store) || nilValue(config.Lease) || config.Provider == nil || config.Facts == nil || now == nil {
		return nil, fmt.Errorf("authzrpc: store, lease, provider and facts are required")
	}
	var checker *Checker
	ontologyOptions := []service.OntologyOption{}
	if config.LifecyclePolicy != nil {
		ontologyOptions = append(ontologyOptions, service.WithTransitionPermissionObserver(func(ctx context.Context, q service.TransitionPermissionRequest, reader port.SnapshotReader) (service.TransitionPermissionObservation, error) {
			return checker.ObserveTransitionPermission(ctx, q, reader)
		}))
	}
	ontology := service.NewOntology(nil, ontologyOptions...)
	checker, err := newCheckerWithClock(ctx, config.Store, config.Lease, ontology, AuthorityConfig{
		Facts: config.Facts, Namespace: config.Namespace, Caller: config.Caller,
		Hierarchy: config.Hierarchy, LifecyclePolicy: config.LifecyclePolicy,
	}, now)
	if err != nil {
		return nil, err
	}
	meta, err := config.Store.Meta(ctx)
	if err != nil {
		return nil, err
	}
	ns, chain := checker.chain.Namespace, checker.chain
	options := []pipeline.Option{pipeline.WithOntology(ontology), pipeline.WithAuthorizer(checker), pipeline.WithEventContext(checker), pipeline.WithClock(now)}
	if config.Audit != nil {
		options = append(options, pipeline.WithOntologyAudit(config.Audit))
	}
	if len(config.Mutators) != 0 {
		options = append(options, pipeline.WithMutators(config.Mutators...))
	}
	writer := pipeline.New(config.Store, config.Lease, options...)
	serviceOptions := []service.Option{
		service.WithOntology(ontology),
		service.WithClock(now),
		service.WithAuthorizationInstalled(func() bool { return true }),
	}
	if config.Workflow != nil {
		serviceOptions = append(serviceOptions, service.WithWorkflow(config.Workflow))
	}
	domain := service.New(config.Store, config.Lease, writer, serviceOptions...)
	guarded := &GuardedService{
		delegate:        domain,
		checker:         checker,
		lease:           config.Lease,
		commitEmpty:     domain.CommitClaimNextEmpty,
		claimNextPage:   domain.ClaimNextReadyPage,
		readyCandidates: domain.ReadyCandidates,
	}
	registry, err := rpc.NewRegistry()
	if err != nil {
		return nil, err
	}
	if err := rpc.BindService(registry, guarded); err != nil {
		return nil, err
	}
	if err := claimrpc.Bind(registry, guarded); err != nil {
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
	mu           sync.RWMutex
	facts        authzfixed.Facts
	store        driver.Store
	lease        driver.ActiveLease
	namespace    authz.Namespace
	factRevision model.Revision
	bound        bool
}

func NewMutableFacts(facts authzfixed.Facts) (*MutableFacts, error) {
	copy, err := cloneFacts(facts)
	if err != nil {
		return nil, err
	}
	return &MutableFacts{facts: copy}, nil
}
func (s *MutableFacts) Load(ctx context.Context, snapshot Snapshot) (authzfixed.Facts, error) {
	if s == nil {
		return authzfixed.Facts{}, errors.New("authorization facts source is nil")
	}
	s.mu.RLock()
	facts, err := cloneFacts(s.facts)
	expected, bound, namespace := s.factRevision, s.bound, s.namespace
	s.mu.RUnlock()
	if err != nil {
		return authzfixed.Facts{}, err
	}
	if !bound {
		namespace = facts.Namespace
	}
	state, err := observeACL(ctx, snapshot, namespace)
	if err != nil {
		return authzfixed.Facts{}, err
	}
	if bound && state.Revision.String() != expected.String() {
		return authzfixed.Facts{}, fmt.Errorf("authorization facts changed after the pinned snapshot")
	}
	return facts, nil
}
func (s *MutableFacts) Replace(facts authzfixed.Facts) error {
	copy, err := cloneFacts(facts)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.bound {
		s.facts = copy
		return nil
	}
	// Once installed, authority and existing name/Ref bindings are immutable.
	// Validate before opening a write transaction so a refused replacement
	// neither advances the ACL generation nor poisons the installed source.
	if err := authzfixed.ValidateRegistryUpdate(s.facts, copy); err != nil {
		return err
	}
	ctx := context.Background()
	fact, err := driver.NewReadFact(authz.ACLDependency(s.namespace).Key)
	if err != nil {
		return err
	}
	tx, err := s.store.Write(ctx, s.lease)
	if err != nil {
		return err
	}
	defer tx.Close()
	defer tx.Rollback(context.Background())
	lock := driver.LockKey{Kind: driver.LockReadFact, Fact: fact}
	states, err := tx.Lock(ctx, []driver.LockRequest{{Key: lock, Mode: driver.LockUpdate}})
	if err != nil {
		return err
	}
	if len(states) != 1 {
		return errors.New("authorization fact lock was not acquired")
	}
	if _, err := tx.Head(ctx); err != nil {
		return err
	}
	if err := tx.BumpReadFacts(ctx, []driver.ReadFact{fact}); err != nil {
		return err
	}
	entry, err := tx.Commit(ctx, driver.ChangesetHeader{
		Incarnation: s.namespace.Incarnation,
		Kind:        model.ChangesetDerived,
		Time:        time.Now().UTC(),
		Principal:   model.PrincipalRef{Kind: "system", ID: "authzrpc"},
		LockKeys:    []driver.LockKey{lock},
	})
	if err != nil {
		return err
	}
	s.factRevision = entry.Revision
	s.facts = copy
	return nil
}

func (s *MutableFacts) bind(ctx context.Context, store driver.Store, lease driver.ActiveLease, namespace authz.Namespace) error {
	if s == nil || nilValue(store) || nilValue(lease) {
		return errors.New("authorization facts source cannot be bound")
	}
	if err := namespace.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.facts.Namespace != namespace {
		return fmt.Errorf("authorization facts namespace does not match composition")
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		return err
	}
	if namespace.StoreID != string(meta.StoreID) || namespace.Incarnation != meta.Incarnation {
		return fmt.Errorf("authorization facts namespace does not match store")
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return err
	}
	state, err := observeACL(ctx, reader, namespace)
	_ = reader.Close()
	if err != nil {
		return err
	}
	s.store, s.lease, s.namespace, s.factRevision, s.bound = store, lease, namespace, state.Revision, true
	return nil
}

func observeACL(ctx context.Context, snapshot Snapshot, namespace authz.Namespace) (driver.LockState, error) {
	if snapshot == nil {
		return driver.LockState{}, errors.New("authorization snapshot is unavailable")
	}
	dep := authz.ACLDependency(namespace)
	fact, err := driver.NewReadFact(dep.Key)
	if err != nil {
		return driver.LockState{}, err
	}
	states, err := snapshot.Observe(ctx, []driver.LockKey{{Kind: driver.LockReadFact, Fact: fact}})
	if err != nil {
		return driver.LockState{}, err
	}
	if len(states) != 1 {
		return driver.LockState{}, errors.New("authorization fact observation was incomplete")
	}
	return states[0], nil
}

func samePrincipal(a, b model.PrincipalRef) bool {
	x, err := json.Marshal(a)
	if err != nil {
		return false
	}
	y, err := json.Marshal(b)
	if err != nil {
		return false
	}
	// Native caveats can contain typed Refs while authenticated/frozen values
	// contain JSON objects. Compare their normalized values, not object member
	// order in the encoded bytes. PrincipalRef's strict codec preserves exact
	// JSON numbers, so normalization cannot round distinct caveat values.
	var left, right model.PrincipalRef
	if json.Unmarshal(x, &left) != nil || json.Unmarshal(y, &right) != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
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

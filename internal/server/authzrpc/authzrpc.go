// Package authzrpc composes the adopted fixed-permission chain with the real
// service, writer and RPC transport. It owns no policy language or storage
// transaction; all decisions go through core/authz and plugins/authzfixed.
package authzrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
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
	// WriterObserver receives the same bounded, non-durable phase telemetry as
	// pipeline.WithObserver. It must be concurrency-safe and return without I/O.
	WriterObserver pipeline.Observer
	// Hierarchy opts into the bounded EP2 create/reparent authorization adapter.
	// It does not implement hierarchy storage/RPC or relax ordinary #18 writes.
	// Keep false until the host installs the separately reviewed hierarchy Writer.
	Hierarchy bool
	// Workflow is the optional durable plugin provider. When installed, the
	// guarded service exposes x.workflow.invoke and standard-tool routing while
	// preserving the checker for core beads.
	Workflow *workflowplugin.Provider
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
	if nilValue(config.Store) || nilValue(config.Lease) || config.Provider == nil || nilValue(config.Facts) || now == nil {
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
	checker := &Checker{chain: chain, source: config.Facts, caller: caller, store: config.Store, lease: config.Lease, hierarchy: config.Hierarchy, now: now}
	if mutable, ok := config.Facts.(*MutableFacts); ok {
		if err := mutable.bind(ctx, config.Store, config.Lease, ns); err != nil {
			return nil, err
		}
	}
	ontology := service.NewOntology(nil)
	options := []pipeline.Option{pipeline.WithOntology(ontology), pipeline.WithAuthorizer(checker), pipeline.WithEventContext(checker), pipeline.WithClock(now)}
	if config.WriterObserver != nil {
		options = append(options, pipeline.WithObserver(config.WriterObserver))
	}
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
	mu             sync.RWMutex
	facts          authzfixed.Facts
	store          driver.Store
	lease          driver.ActiveLease
	namespace      authz.Namespace
	factRevision   model.Revision
	sourceRevision model.Revision
	sourceDigest   string
	sourcePresent  bool
	bound          bool
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
	sourceRevision, sourceDigest, sourcePresent := s.sourceRevision, s.sourceDigest, s.sourcePresent
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
	if bound {
		source, err := observeFactsSource(ctx, snapshot, namespace)
		if err != nil {
			return authzfixed.Facts{}, err
		}
		if source.Present != sourcePresent || source.Revision.String() != sourceRevision.String() || sourcePresent && (source.Derived == nil || driver.SelectionDigest(source.Derived.Value) != sourceDigest) {
			return authzfixed.Facts{}, errors.New("authorization source binding changed after the pinned snapshot")
		}
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
	revision, digest, err := publishFacts(context.Background(), s.store, s.lease, s.facts, copy, !s.sourcePresent)
	if err != nil {
		return err
	}
	// Only an acknowledged commit installs process-local facts. A lost commit
	// acknowledgement leaves the old binding, which Load refuses against the
	// genuinely advanced durable source/ACL until a legitimate Replace.
	s.factRevision, s.sourceRevision, s.sourceDigest = revision, revision, digest
	s.sourcePresent = true
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
	if err != nil {
		_ = reader.Close()
		return err
	}
	source, err := observeFactsSource(ctx, reader, namespace)
	_ = reader.Close()
	if err != nil {
		return err
	}
	body, err := authzfixed.FactsSourceBytes(s.facts)
	if err != nil {
		return err
	}
	digest := driver.SelectionDigest(body)
	revision, sourceRevision := state.Revision, source.Revision
	if source.Present {
		if source.Derived == nil || !bytes.Equal(source.Derived.Value, body) || source.Revision.String() != state.Revision.String() {
			return errors.New("installed authorization source does not match durable source binding")
		}
	} else {
		// Construction binds the ordinary host facts without writing. Absent
		// durable authority cannot certify optional selection; only an explicit
		// Replace may atomically establish its source, domains and ACL generation.
		digest = ""
	}
	s.store, s.lease, s.namespace, s.factRevision, s.bound = store, lease, namespace, revision, true
	s.sourceRevision, s.sourceDigest = sourceRevision, digest
	s.sourcePresent = source.Present
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

func observeFactsSource(ctx context.Context, snapshot Snapshot, namespace authz.Namespace) (driver.LockState, error) {
	rows, err := authzfixed.SelectionRows(namespace)
	if err != nil {
		return driver.LockState{}, err
	}
	states, err := snapshot.Observe(ctx, []driver.LockKey{{Kind: driver.LockDerived, Derived: &rows.Source}})
	if err != nil {
		return driver.LockState{}, err
	}
	if len(states) != 1 {
		return driver.LockState{}, errors.New("authorization source observation was incomplete")
	}
	return states[0], nil
}

func publishFacts(ctx context.Context, store driver.Store, lease driver.ActiveLease, before, after authzfixed.Facts, initial bool) (model.Revision, string, error) {
	body, err := authzfixed.FactsSourceBytes(after)
	if err != nil {
		return model.Natural64(0), "", err
	}
	keys, err := authzfixed.SelectionRows(after.Namespace)
	if err != nil {
		return model.Natural64(0), "", err
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: after.Namespace.Incarnation})
	if err != nil {
		return model.Natural64(0), "", err
	}
	expectedSource, err := observeFactsSource(ctx, reader, after.Namespace)
	if err != nil {
		_ = reader.Close()
		return model.Natural64(0), "", err
	}
	acl, err := observeACL(ctx, reader, after.Namespace)
	_ = reader.Close()
	if err != nil {
		return model.Natural64(0), "", err
	}
	if initial {
		if expectedSource.Present || !expectedSource.Revision.IsZero() {
			return model.Natural64(0), "", errors.New("authorization source changed during installation")
		}
	} else {
		if !expectedSource.Present || expectedSource.Derived == nil || expectedSource.Revision.String() != acl.Revision.String() {
			return model.Natural64(0), "", errors.New("durable authorization source binding is unavailable")
		}
		var durable authzfixed.Facts
		if err := json.Unmarshal(expectedSource.Derived.Value, &durable); err != nil {
			return model.Natural64(0), "", err
		}
		canonical, err := authzfixed.FactsSourceBytes(durable)
		if err != nil || !bytes.Equal(canonical, expectedSource.Derived.Value) {
			return model.Natural64(0), "", errors.New("durable authorization source is not canonical")
		}
		if err := authzfixed.ValidateRegistryUpdate(durable, after); err != nil {
			return model.Natural64(0), "", err
		}
		before = durable
	}
	fact, err := driver.NewReadFact(authz.ACLDependency(after.Namespace).Key)
	if err != nil {
		return model.Natural64(0), "", err
	}
	byKey := map[string]driver.LockRequest{}
	add := func(key driver.LockKey) error {
		encoded, err := driver.EncodeLockKey(key)
		if err == nil {
			byKey[encoded] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
		}
		return err
	}
	if err := add(driver.LockKey{Kind: driver.LockReadFact, Fact: fact}); err != nil {
		return model.Natural64(0), "", err
	}
	for _, key := range []driver.DerivedRowKey{keys.Source, keys.Grants, keys.Projects, keys.Teams} {
		key := key
		if err := add(driver.LockKey{Kind: driver.LockDerived, Derived: &key}); err != nil {
			return model.Natural64(0), "", err
		}
	}
	sentinels := map[driver.Sentinel]bool{keys.AttributeScope: true}
	// Declare old AND new semantic grant scopes, including explicit empty
	// domains, before Head. Opaque scope components retain all identity kinds.
	for _, facts := range []authzfixed.Facts{before, after} {
		scopes := []authz.Scope{{Kind: authz.AuthorityScope, Ref: facts.Namespace.Authority}}
		for _, grant := range facts.Grants {
			scopes = append(scopes, grant.Scope)
		}
		for _, project := range facts.Projects {
			scopes = append(scopes, authz.Scope{Kind: authz.ProjectScope, Ref: project.Ref})
		}
		for _, team := range facts.Teams {
			scopes = append(scopes, authz.Scope{Kind: authz.TeamScope, Ref: team.Ref})
		}
		for _, scope := range scopes {
			dep, err := authz.GrantDependency(facts.Namespace, scope)
			if err != nil {
				return model.Natural64(0), "", err
			}
			sentinel, err := driver.NewSentinel(dep.Key)
			if err != nil {
				return model.Natural64(0), "", err
			}
			sentinels[sentinel] = true
		}
	}
	for sentinel := range sentinels {
		if err := add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}); err != nil {
			return model.Natural64(0), "", err
		}
	}
	ordered := make([]string, 0, len(byKey))
	for key := range byKey {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	locks, lockKeys := make(driver.LockSet, 0, len(ordered)), make([]driver.LockKey, 0, len(ordered))
	for _, key := range ordered {
		locks = append(locks, byKey[key])
		lockKeys = append(lockKeys, byKey[key].Key)
	}
	if err := driver.ValidateLockSet(locks); err != nil {
		return model.Natural64(0), "", err
	}
	tx, err := store.Write(ctx, lease)
	if err != nil {
		return model.Natural64(0), "", err
	}
	defer tx.Close()
	defer tx.Rollback(context.WithoutCancel(ctx))
	states, err := tx.Lock(ctx, locks)
	if err != nil {
		return model.Natural64(0), "", err
	}
	if len(states) != len(locks) {
		return model.Natural64(0), "", errors.New("authorization publication lock census was incomplete")
	}
	var observed model.Revision
	for _, state := range states {
		if state.Key.Kind == driver.LockReadFact && state.Key.Fact == fact && (state.Present != acl.Present || state.Revision.String() != acl.Revision.String()) {
			return model.Natural64(0), "", errors.New("authorization generation changed during publication")
		}
		if state.Key.Kind == driver.LockDerived && state.Key.Derived != nil && *state.Key.Derived == keys.Source {
			if state.Present != expectedSource.Present || state.Revision.String() != expectedSource.Revision.String() || state.Present && (state.Derived == nil || expectedSource.Derived == nil || !bytes.Equal(state.Derived.Value, expectedSource.Derived.Value)) {
				return model.Natural64(0), "", errors.New("authorization source changed during publication")
			}
			observed = state.Revision
		}
	}
	if _, err := tx.Head(ctx); err != nil {
		return model.Natural64(0), "", err
	}
	root := driver.DerivedRow{Key: keys.Source.Key, Value: body}
	if err := tx.PutDerived(ctx, keys.Source.Table, nil, []driver.DerivedRow{root}); err != nil {
		return model.Natural64(0), "", err
	}
	digest := driver.SelectionDigest(body)
	source := driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &keys.Source}, Revision: observed, Present: true, Digest: digest}
	rows, err := authzfixed.SelectionProjection(after, source)
	if err != nil {
		return model.Natural64(0), "", err
	}
	for i, key := range []driver.DerivedRowKey{keys.Grants, keys.Projects, keys.Teams} {
		row := rows[i]
		err := pipeline.PutSelectionDerived(ctx, tx, key.Table, nil, []driver.DerivedRow{row})
		if err != nil {
			return model.Natural64(0), "", err
		}
	}
	bumps := make([]driver.Sentinel, 0, len(sentinels))
	for sentinel := range sentinels {
		bumps = append(bumps, sentinel)
	}
	sort.Slice(bumps, func(i, j int) bool { return bumps[i] < bumps[j] })
	if err := tx.BumpSentinels(ctx, bumps); err != nil {
		return model.Natural64(0), "", err
	}
	if err := tx.BumpReadFacts(ctx, []driver.ReadFact{fact}); err != nil {
		return model.Natural64(0), "", err
	}
	entry, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: after.Namespace.Incarnation, Kind: model.ChangesetDerived, Time: time.Now().UTC(), Principal: model.PrincipalRef{Kind: "system", ID: "authzrpc"}, LockKeys: lockKeys})
	if err != nil {
		return model.Natural64(0), "", err
	}
	return entry.Revision, digest, nil
}

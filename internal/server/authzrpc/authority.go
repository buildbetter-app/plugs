package authzrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// AuthorityConfig is trusted host installation state. The production runtime
// supplies its own store, lease and shared ontology after opening storage; a
// checker from an earlier runtime is never retained across SQL reopen.
type AuthorityConfig struct {
	Facts           FactsSource
	Namespace       authz.Namespace
	Caller          CallerSource
	Hierarchy       bool
	LifecyclePolicy *LifecyclePolicyConfig
}

// NewChecker constructs the actual fixed chain and optional exact lifecycle
// authority for a host-owned Writer. It does not open a store or grant RPC
// callers control of policy, principal context or the ontology.
func NewChecker(ctx context.Context, store driver.Store, lease driver.ActiveLease, ontology *service.Ontology, config AuthorityConfig) (*Checker, error) {
	return newCheckerWithClock(ctx, store, lease, ontology, config, time.Now)
}

func newCheckerWithClock(ctx context.Context, store driver.Store, lease driver.ActiveLease, ontology *service.Ontology, config AuthorityConfig, now func() time.Time) (*Checker, error) {
	if nilValue(store) || nilValue(lease) || nilValue(config.Facts) || ontology == nil || now == nil {
		return nil, fmt.Errorf("authzrpc: authority requires store, lease, ontology and facts")
	}
	meta, err := store.Meta(ctx)
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
	if config.LifecyclePolicy != nil {
		if err := authz.CanonicalRef(config.LifecyclePolicy.Ref); err != nil {
			return nil, fmt.Errorf("authzrpc: invalid lifecycle policy reference: %w", err)
		}
		chain.Rules = append(chain.Rules, &lifecycleRuleSet{namespace: ns, ref: config.LifecyclePolicy.Ref})
	}
	checker := &Checker{chain: chain, source: config.Facts, caller: caller, store: store, lease: lease, hierarchy: config.Hierarchy, now: now, ontology: ontology}
	checker.lifecycle = config.LifecyclePolicy != nil
	if mutable, ok := config.Facts.(*MutableFacts); ok {
		if err := mutable.bind(ctx, store, lease, ns); err != nil {
			return nil, err
		}
	}

	return checker, nil
}

// GuardService attaches the same checker used by the Writer to the real read
// boundary. The caller still owns the domain service and its lifetime.
func (c *Checker) GuardService(domain *service.Service) *GuardedService {
	return &GuardedService{delegate: domain, checker: c, lease: c.lease,
		commitEmpty: domain.CommitClaimNextEmpty, claimNextPage: domain.ClaimNextReadyPage,
		readyCandidates: domain.ReadyCandidates}
}
